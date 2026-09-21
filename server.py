#!/usr/bin/env python3

import asyncio
import json
import logging
import ssl
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import websockets

from db import (
    authenticate_device,
    insert_event,
    update_last_seen,
)


# ============================================================
# CONFIGURATION
# ============================================================

HOST = "0.0.0.0"
PORT = 8766
HTTP_PORT = 8765
HTTP_STREAM_PATH = "/video"

WEBSOCKET_PATH = "/parking"

SERVER_CERT = Path("certs/server.crt")
SERVER_KEY = Path("certs/server.key")

DATA_DIR = Path("data")
FRAME_DIR = DATA_DIR / "frames"

AUTH_TIMEOUT = 10

PING_INTERVAL = 15

MAX_MESSAGE_SIZE = 5 * 1024 * 1024


# ============================================================
# DIRECTORIES
# ============================================================

DATA_DIR.mkdir(parents=True, exist_ok=True)
FRAME_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

logger = logging.getLogger("parking-host")


# ============================================================
# GLOBAL STATE
# ============================================================

# device_id -> websocket
connected_devices = {}

# device_id -> device information
device_sessions = {}

# device_id -> latest video metadata
latest_frames = {}

# device_id -> latest frame bytes and sequence number
latest_stream_frames = {}

# device_id -> notifies waiting HTTP stream clients
frame_update_conditions = {}

# websocket -> pending video metadata
# The ESP32 sends:
#
#   TEXT video_frame metadata
#   BINARY JPEG
#
# We temporarily associate the metadata with the connection.
pending_video_metadata = {}


# ============================================================
# TIME
# ============================================================

def utc_timestamp():
    """
    Return an ISO-8601 UTC timestamp.
    """

    return (
        datetime.now(timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def parse_timestamp(value):
    """Parse an ISO-8601 timestamp received from an ESP32."""

    if not value:
        return None

    try:
        return datetime.fromisoformat(
            value.replace("Z", "+00:00")
        )
    except (TypeError, ValueError):
        logger.warning(
            "Invalid device timestamp: %r",
            value,
        )
        return None


def get_frame_condition(device_id):
    condition = frame_update_conditions.get(device_id)

    if condition is None:
        condition = asyncio.Condition()
        frame_update_conditions[device_id] = condition

    return condition


# ============================================================
# AUTHENTICATION
# ============================================================

async def authenticate(websocket):
    """
    Wait for the first message from the ESP32 and authenticate
    the device against PostgreSQL.

    Expected:

    {
        "type": "auth",
        "device_id": "...",
        "token": "..."
    }
    """

    try:
        raw_message = await asyncio.wait_for(
            websocket.recv(),
            timeout=AUTH_TIMEOUT,
        )

    except asyncio.TimeoutError:
        logger.warning("Authentication timeout")
        await websocket.close(
            code=4001,
            reason="Authentication timeout",
        )
        return None

    except Exception as exc:
        logger.warning(
            "Failed to receive authentication: %s",
            exc,
        )
        return None

    if isinstance(raw_message, bytes):
        logger.warning(
            "Binary message received before authentication"
        )
        await websocket.close(
            code=4002,
            reason="Authentication required",
        )
        return None

    try:
        message = json.loads(raw_message)
    except json.JSONDecodeError:
        logger.warning("Invalid JSON authentication message")
        await websocket.close(
            code=4003,
            reason="Invalid authentication",
        )
        return None

    if message.get("type") != "auth":
        logger.warning("First message was not authentication")
        await websocket.close(
            code=4004,
            reason="Authentication required",
        )
        return None

    device_id = message.get("device_id")
    token = message.get("token")

    if not device_id or not token:
        logger.warning(
            "Authentication missing device_id or token"
        )
        await websocket.close(
            code=4005,
            reason="Invalid authentication",
        )
        return None

    try:
        device = authenticate_device(
            device_id,
            token,
        )
    except Exception as exc:
        logger.exception(
            "Database authentication error for %s: %s",
            device_id,
            exc,
        )
        await websocket.send(
            json.dumps({
                "type": "auth_failed",
                "reason": "server_error",
            })
        )
        await websocket.close(
            code=4500,
            reason="Authentication service error",
        )
        return None

    if device is None:
        logger.warning(
            "Authentication failed for device: %s",
            device_id,
        )
        await websocket.send(
            json.dumps({
                "type": "auth_failed",
                "reason": "invalid_credentials",
            })
        )
        await websocket.close(
            code=4008,
            reason="Authentication failed",
        )
        return None

    logger.info(
        "Device authenticated: %s (%s)",
        device["device_id"],
        device.get("name") or "unnamed",
    )

    old_websocket = connected_devices.get(device_id)

    if old_websocket is not None:
        logger.warning(
            "Device %s already connected. "
            "Closing previous connection.",
            device_id,
        )
        try:
            await old_websocket.close(
                code=4010,
                reason="New connection established",
            )
        except Exception:
            pass

    connected_devices[device_id] = websocket

    now = utc_timestamp()

    device_sessions[device_id] = {
        "device_id": device["device_id"],
        "name": device.get("name"),
        "spot": device.get("spot"),
        "connected_at": now,
        "last_seen": now,
    }

    try:
        update_last_seen(device_id)
    except Exception as exc:
        logger.error(
            "Failed to update last_seen for %s: %s",
            device_id,
            exc,
        )

    await websocket.send(
        json.dumps({
            "type": "auth_ok",
            "device_id": device_id,
            "server_time": now,
        })
    )

    return device_id


# ============================================================
# PATH VALIDATION
# ============================================================

def get_websocket_path(websocket):
    """
    Obtain the requested WebSocket path.

    Modern versions of websockets expose it through
    websocket.request.path.
    """

    try:

        request = websocket.request

        if request is not None:

            return request.path

    except AttributeError:
        pass

    # Compatibility fallback for older versions.
    try:

        return websocket.path

    except AttributeError:

        return None


# ============================================================
# SENSOR HANDLING
# ============================================================

async def handle_sensor_message(
    websocket,
    device_id,
    message,
):
    """Store a sensor event in PostgreSQL."""

    server_time = utc_timestamp()
    message["server_timestamp"] = server_time

    device_sessions[device_id]["last_seen"] = server_time

    try:
        event_id = insert_event(
            device_id=device_id,
            event_type="sensor",
            event_time=parse_timestamp(message.get("timestamp")),
            payload=message,
        )

        update_last_seen(device_id)

        logger.info(
            "Sensor from %s stored as event %s: %s",
            device_id,
            event_id,
            json.dumps(message),
        )

    except Exception as exc:
        logger.exception(
            "Failed to store sensor event from %s: %s",
            device_id,
            exc,
        )


# ============================================================
# STATUS HANDLING
# ============================================================

async def handle_status_message(
    websocket,
    device_id,
    message,
):
    """Store a device status event in PostgreSQL."""

    server_time = utc_timestamp()
    message["server_timestamp"] = server_time

    device_sessions[device_id]["last_seen"] = server_time

    try:
        event_id = insert_event(
            device_id=device_id,
            event_type="status",
            event_time=parse_timestamp(message.get("timestamp")),
            payload=message,
        )

        update_last_seen(device_id)

        logger.info(
            "Status from %s stored as event %s | "
            "RSSI=%s | IP=%s | IR=%s | video=%s",
            device_id,
            event_id,
            message.get("wifi", {}).get("rssi"),
            message.get("wifi", {}).get("ip"),
            message.get("sensor", {}).get("ir"),
            message.get("video", {}).get("source"),
        )

    except Exception as exc:
        logger.exception(
            "Failed to store status event from %s: %s",
            device_id,
            exc,
        )


# ============================================================
# VIDEO METADATA
# ============================================================

async def handle_video_metadata(
    websocket,
    device_id,
    message,
):
    """
    Receive metadata for the next JPEG frame.

    The ESP32 sends:

        TEXT:
        {
            "type": "video_frame",
            ...
        }

        BINARY:
        <JPEG>

    """

    source = message.get(
        "source",
        "unknown",
    )

    frame_id = message.get(
        "frame_id",
    )

    pending_video_metadata[websocket] = message

    device_sessions[device_id]["last_seen"] = utc_timestamp()

    logger.debug(
        "Video metadata from %s | source=%s | frame=%s",
        device_id,
        source,
        frame_id,
    )


# ============================================================
# VIDEO BINARY FRAME
# ============================================================

async def handle_video_frame(
    websocket,
    device_id,
    binary_data,
):
    """
    Handle a binary JPEG frame.
    """

    # --------------------------------------------------------
    # Validate JPEG
    # --------------------------------------------------------

    if len(binary_data) < 4:

        logger.warning(
            "Ignoring very small binary frame from %s",
            device_id,
        )

        return

    # JPEG SOI marker
    if binary_data[0:2] != b"\xff\xd8":

        logger.warning(
            "Binary frame from %s is not a JPEG",
            device_id,
        )

        return

    # --------------------------------------------------------
    # Get metadata
    # --------------------------------------------------------

    metadata = pending_video_metadata.pop(
        websocket,
        None,
    )

    if metadata is None:

        logger.warning(
            "JPEG received from %s without metadata",
            device_id,
        )

        metadata = {
            "type": "video_frame",
            "source": "unknown",
        }

    source = metadata.get(
        "source",
        "unknown",
    )

    frame_id = metadata.get(
        "frame_id",
        0,
    )

    timestamp = metadata.get(
        "timestamp",
        utc_timestamp(),
    )

    # --------------------------------------------------------
    # Save latest frame
    # --------------------------------------------------------

    frame_path = (
        FRAME_DIR
        / f"{device_id}.jpg"
    )

    try:

        with open(
            frame_path,
            "wb",
        ) as file:

            file.write(binary_data)

    except Exception as exc:

        logger.error(
            "Failed to save JPEG from %s: %s",
            device_id,
            exc,
        )

        return

    # --------------------------------------------------------
    # Update state
    # --------------------------------------------------------

    frame_info = {
        "device_id": device_id,
        "source": source,
        "frame_id": frame_id,
        "timestamp": timestamp,
        "server_timestamp": utc_timestamp(),
        "size": len(binary_data),
        "path": str(frame_path),
    }

    latest_frames[device_id] = frame_info

    device_sessions[device_id]["last_seen"] = (
        utc_timestamp()
    )

    previous_stream_frame = latest_stream_frames.get(
        device_id
    )

    next_sequence = (
        1
        if previous_stream_frame is None
        else previous_stream_frame["sequence"] + 1
    )

    latest_stream_frames[device_id] = {
        "sequence": next_sequence,
        "jpeg": binary_data,
    }

    frame_condition = get_frame_condition(device_id)

    async with frame_condition:
        frame_condition.notify_all()

    logger.info(
        "JPEG frame from %s | source=%s | "
        "frame=%s | size=%d bytes",
        device_id,
        source,
        frame_id,
        len(binary_data),
    )


# ============================================================
# VIDEO STATUS
# ============================================================

async def handle_video_status(
    websocket,
    device_id,
    message,
):
    """Store a video-source status event in PostgreSQL."""

    server_time = utc_timestamp()
    message["server_timestamp"] = server_time

    device_sessions[device_id]["last_seen"] = server_time

    source = message.get("source", "unknown")
    camera_enabled = message.get("camera_enabled", False)
    camera_initialized = message.get(
        "camera_initialized",
        False,
    )

    try:
        event_id = insert_event(
            device_id=device_id,
            event_type="video_status",
            event_time=parse_timestamp(message.get("timestamp")),
            payload=message,
        )

        update_last_seen(device_id)

        logger.info(
            "Video status from %s stored as event %s | "
            "source=%s | camera_enabled=%s | "
            "camera_initialized=%s",
            device_id,
            event_id,
            source,
            camera_enabled,
            camera_initialized,
        )

    except Exception as exc:
        logger.exception(
            "Failed to store video status from %s: %s",
            device_id,
            exc,
        )


# ============================================================
# HTTP MJPEG STREAM
# ============================================================

async def send_http_response(
    writer,
    status_code,
    reason,
    body,
):
    if isinstance(body, str):
        body = body.encode("utf-8")

    headers = [
        f"HTTP/1.1 {status_code} {reason}",
        "Content-Type: text/plain; charset=utf-8",
        f"Content-Length: {len(body)}",
        "Connection: close",
        "",
        "",
    ]

    writer.write(
        "\r\n".join(headers).encode("ascii")
    )
    writer.write(body)
    await writer.drain()


async def handle_http_stream_client(
    reader,
    writer,
):
    client_address = writer.get_extra_info("peername")

    try:
        request_line = await reader.readline()

        if not request_line:
            return

        try:
            request_text = request_line.decode(
                "ascii"
            ).strip()
        except UnicodeDecodeError:
            await send_http_response(
                writer,
                400,
                "Bad Request",
                "Invalid request line",
            )
            return

        parts = request_text.split(" ")

        if len(parts) != 3:
            await send_http_response(
                writer,
                400,
                "Bad Request",
                "Malformed request line",
            )
            return

        method, target, _ = parts

        while True:
            header_line = await reader.readline()

            if not header_line:
                break

            if header_line in (b"\r\n", b"\n"):
                break

        if method != "GET":
            await send_http_response(
                writer,
                405,
                "Method Not Allowed",
                "Only GET is supported",
            )
            return

        parsed_target = urlsplit(target)

        if parsed_target.path != HTTP_STREAM_PATH:
            await send_http_response(
                writer,
                404,
                "Not Found",
                "Unknown endpoint",
            )
            return

        query_params = parse_qs(
            parsed_target.query
        )

        device_id = query_params.get(
            "device_id",
            [None],
        )[0]

        if not device_id:
            await send_http_response(
                writer,
                400,
                "Bad Request",
                "Missing required query parameter: device_id",
            )
            return

        boundary = "frame"
        headers = [
            "HTTP/1.1 200 OK",
            "Cache-Control: no-cache",
            "Pragma: no-cache",
            "Connection: close",
            (
                "Content-Type: "
                f"multipart/x-mixed-replace; boundary={boundary}"
            ),
            "",
            "",
        ]

        writer.write(
            "\r\n".join(headers).encode("ascii")
        )
        await writer.drain()

        logger.info(
            "HTTP stream connected from %s for %s",
            client_address,
            device_id,
        )

        last_sequence = 0

        while True:
            latest_frame = latest_stream_frames.get(
                device_id
            )

            if (
                latest_frame is None
                or latest_frame["sequence"] <= last_sequence
            ):
                condition = get_frame_condition(
                    device_id
                )

                async with condition:
                    latest_frame = latest_stream_frames.get(
                        device_id
                    )
                    if (
                        latest_frame is None
                        or latest_frame["sequence"] <= last_sequence
                    ):
                        await condition.wait()
                continue

            jpeg_data = latest_frame["jpeg"]
            last_sequence = latest_frame["sequence"]

            part_headers = [
                f"--{boundary}",
                "Content-Type: image/jpeg",
                f"Content-Length: {len(jpeg_data)}",
                f"X-Frame-Sequence: {last_sequence}",
                "",
                "",
            ]

            writer.write(
                "\r\n".join(part_headers).encode("ascii")
            )
            writer.write(jpeg_data)
            writer.write(b"\r\n")
            await writer.drain()

    except asyncio.CancelledError:
        raise
    except (
        ConnectionResetError,
        BrokenPipeError,
    ):
        pass
    except Exception as exc:
        logger.warning(
            "HTTP stream error for %s: %s",
            client_address,
            exc,
        )
    finally:
        logger.info(
            "HTTP stream disconnected from %s",
            client_address,
        )

        writer.close()

        try:
            await writer.wait_closed()
        except Exception:
            pass


# ============================================================
# SEND COMMAND TO DEVICE
# ============================================================

async def send_to_device(
    device_id,
    message,
):
    """
    Send a JSON command to an authenticated ESP32.
    """

    websocket = connected_devices.get(
        device_id
    )

    if websocket is None:

        logger.warning(
            "Device %s is not connected",
            device_id,
        )

        return False

    try:

        await websocket.send(
            json.dumps(message)
        )

        return True

    except Exception as exc:

        logger.error(
            "Failed to send command to %s: %s",
            device_id,
            exc,
        )

        return False


# ============================================================
# VIDEO SOURCE CONTROL
# ============================================================

async def set_video_source(
    device_id,
    source,
):
    """
    Change the requested video source.

    Valid values:

        camera
        placeholder
    """

    if source not in (
        "camera",
        "placeholder",
    ):

        raise ValueError(
            "Video source must be "
            "'camera' or 'placeholder'"
        )

    return await send_to_device(
        device_id,
        {
            "type": "set_video_source",
            "source": source,
        },
    )


# ============================================================
# GET SENSOR
# ============================================================

async def request_sensor(device_id):

    return await send_to_device(
        device_id,
        {
            "type": "get_sensor",
        },
    )


# ============================================================
# CLIENT HANDLER
# ============================================================

async def handle_client(websocket):
    """
    Main handler for every WSS connection.
    """

    remote_address = websocket.remote_address

    logger.info(
        "Incoming WSS connection from %s",
        remote_address,
    )

    # --------------------------------------------------------
    # Check WebSocket path
    # --------------------------------------------------------

    path = get_websocket_path(websocket)

    if path is not None:

        if path != WEBSOCKET_PATH:

            logger.warning(
                "Rejected connection from %s: "
                "invalid path %s",
                remote_address,
                path,
            )

            await websocket.close(
                code=4000,
                reason="Invalid WebSocket path",
            )

            return

    device_id = None

    try:

        # ----------------------------------------------------
        # Authenticate
        # ----------------------------------------------------

        device_id = await authenticate(
            websocket
        )

        if device_id is None:
            return

        logger.info(
            "WSS session established for %s",
            device_id,
        )

        # ----------------------------------------------------
        # Main receive loop
        # ----------------------------------------------------

        async for message in websocket:

            # =================================================
            # TEXT MESSAGE
            # =================================================

            if isinstance(message, str):

                try:

                    data = json.loads(message)

                except json.JSONDecodeError:

                    logger.warning(
                        "Invalid JSON from %s: %s",
                        device_id,
                        message,
                    )

                    continue

                message_type = data.get(
                    "type",
                    "",
                )

                # ---------------------------------------------
                # SENSOR
                # ---------------------------------------------

                if message_type == "sensor":

                    await handle_sensor_message(
                        websocket,
                        device_id,
                        data,
                    )

                # ---------------------------------------------
                # STATUS
                # ---------------------------------------------

                elif message_type == "status":

                    await handle_status_message(
                        websocket,
                        device_id,
                        data,
                    )

                # ---------------------------------------------
                # VIDEO FRAME METADATA
                # ---------------------------------------------

                elif message_type == "video_frame":

                    await handle_video_metadata(
                        websocket,
                        device_id,
                        data,
                    )

                # ---------------------------------------------
                # VIDEO STATUS
                # ---------------------------------------------

                elif message_type == "video_status":

                    await handle_video_status(
                        websocket,
                        device_id,
                        data,
                    )

                # ---------------------------------------------
                # PONG
                # ---------------------------------------------

                elif message_type == "pong":

                    device_sessions[device_id][
                        "last_seen"
                    ] = utc_timestamp()

                    logger.debug(
                        "Pong from %s",
                        device_id,
                    )

                # ---------------------------------------------
                # UNKNOWN MESSAGE
                # ---------------------------------------------

                else:

                    logger.info(
                        "Unknown message from %s: %s",
                        device_id,
                        message_type,
                    )

            # =================================================
            # BINARY MESSAGE
            # =================================================

            elif isinstance(message, bytes):

                await handle_video_frame(
                    websocket,
                    device_id,
                    message,
                )

    except websockets.exceptions.ConnectionClosed as exc:

        logger.info(
            "Connection closed for %s: "
            "code=%s reason=%s",
            device_id or "unauthenticated",
            exc.code,
            exc.reason,
        )

    except Exception as exc:

        logger.exception(
            "Unexpected error for %s: %s",
            device_id or "unauthenticated",
            exc,
        )

    finally:

        # ----------------------------------------------------
        # Cleanup connection
        # ----------------------------------------------------

        if device_id is not None:

            if connected_devices.get(device_id) is websocket:

                del connected_devices[device_id]

            pending_video_metadata.pop(
                websocket,
                None,
            )

            if device_id in device_sessions:

                device_sessions[device_id][
                    "disconnected_at"
                ] = utc_timestamp()

            logger.info(
                "Device disconnected: %s",
                device_id,
            )


# ============================================================
# SERVER MONITOR
# ============================================================

async def monitor_devices():

    while True:

        await asyncio.sleep(30)

        if not connected_devices:

            logger.info(
                "No ESP32 devices connected"
            )

            continue

        logger.info(
            "Connected devices: %d",
            len(connected_devices),
        )

        for device_id in connected_devices:

            session = device_sessions.get(
                device_id,
                {},
            )

            logger.info(
                "  %s | spot=%s | last_seen=%s",
                device_id,
                session.get("spot"),
                session.get("last_seen"),
            )


# ============================================================
# DATABASE
# ============================================================

def check_database():
    """Verify that PostgreSQL is reachable before starting WSS."""

    try:
        # Importing here keeps database startup errors explicit.
        from db import get_connection

        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()

        logger.info("PostgreSQL connection OK")

    except Exception as exc:
        raise RuntimeError(
            f"PostgreSQL connection failed: {exc}"
        ) from exc


# ============================================================
# TLS
# ============================================================

def create_ssl_context():

    if not SERVER_CERT.exists():

        raise FileNotFoundError(
            f"Server certificate not found: "
            f"{SERVER_CERT}"
        )

    if not SERVER_KEY.exists():

        raise FileNotFoundError(
            f"Server private key not found: "
            f"{SERVER_KEY}"
        )

    ssl_context = ssl.SSLContext(
        ssl.PROTOCOL_TLS_SERVER
    )

    # Require TLS 1.2 or newer.
    ssl_context.minimum_version = (
        ssl.TLSVersion.TLSv1_2
    )

    ssl_context.load_cert_chain(
        certfile=str(SERVER_CERT),
        keyfile=str(SERVER_KEY),
    )

    return ssl_context


# ============================================================
# MAIN
# ============================================================

async def main():

    logger.info(
        "============================================"
    )

    logger.info(
        "       PARKING IoT WSS SERVER"
    )

    logger.info(
        "============================================"
    )

    logger.info(
        "Listening on %s:%d",
        HOST,
        PORT,
    )

    logger.info(
        "WebSocket path: %s",
        WEBSOCKET_PATH,
    )

    logger.info(
        "HTTP stream endpoint: %s:%d%s",
        HOST,
        HTTP_PORT,
        HTTP_STREAM_PATH,
    )

    logger.info(
        "Frame directory: %s",
        FRAME_DIR,
    )

    # --------------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------------

    check_database()

    # --------------------------------------------------------
    # TLS
    # --------------------------------------------------------

    ssl_context = create_ssl_context()

    logger.info(
        "TLS certificate loaded"
    )

    # --------------------------------------------------------
    # Start WSS server
    # --------------------------------------------------------

    async with websockets.serve(
        handle_client,
        HOST,
        PORT,
        ssl=ssl_context,
        ping_interval=20,
        ping_timeout=10,
        max_size=MAX_MESSAGE_SIZE,
    ):

        logger.info(
            "WSS server started successfully"
        )

        http_server = await asyncio.start_server(
            handle_http_stream_client,
            HOST,
            HTTP_PORT,
        )

        logger.info(
            "HTTP stream server started on %s:%d",
            HOST,
            HTTP_PORT,
        )

        logger.info(
            "Waiting for ESP32 devices..."
        )

        # Run monitoring task.
        monitor_task = asyncio.create_task(
            monitor_devices()
        )

        try:

            await asyncio.Future()

        finally:

            monitor_task.cancel()

            try:
                await monitor_task
            except asyncio.CancelledError:
                pass

            http_server.close()
            await http_server.wait_closed()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":

    try:

        asyncio.run(main())

    except KeyboardInterrupt:

        logger.info(
            "Server stopped by user"
        )

    except Exception as exc:

        logger.exception(
            "Server terminated: %s",
            exc,
        )
