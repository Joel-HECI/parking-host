#!/usr/bin/env python3

import asyncio
import json
import logging
import ssl
from datetime import datetime, timezone
from pathlib import Path

import websockets


# ============================================================
# CONFIGURATION
# ============================================================

HOST = "0.0.0.0"
PORT = 8766

WEBSOCKET_PATH = "/parking"

SERVER_CERT = Path("certs/server.crt")
SERVER_KEY = Path("certs/server.key")

DEVICES_FILE = Path("devices.json")

DATA_DIR = Path("data")
SENSOR_LOG = DATA_DIR / "sensors.jsonl"
VIDEO_LOG = DATA_DIR / "video.jsonl"

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

# websocket -> pending video metadata
# The ESP32 sends:
#
#   TEXT video_frame metadata
#   BINARY JPEG
#
# We temporarily associate the metadata with the connection.
pending_video_metadata = {}


# ============================================================
# DEVICE REGISTRY
# ============================================================

def load_devices():
    """
    Load registered ESP32 devices from devices.json.
    """

    if not DEVICES_FILE.exists():

        logger.error(
            "Device registry not found: %s",
            DEVICES_FILE,
        )

        return {}

    try:

        with open(
            DEVICES_FILE,
            "r",
            encoding="utf-8",
        ) as file:

            devices = json.load(file)

        if not isinstance(devices, dict):

            logger.error(
                "devices.json must contain a JSON object"
            )

            return {}

        return devices

    except Exception as exc:

        logger.error(
            "Failed to load devices.json: %s",
            exc,
        )

        return {}


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


# ============================================================
# JSONL LOGGING
# ============================================================

def append_jsonl(path: Path, data: dict):
    """
    Append a JSON object to a JSON Lines file.
    """

    try:

        with open(
            path,
            "a",
            encoding="utf-8",
        ) as file:

            file.write(
                json.dumps(
                    data,
                    separators=(",", ":"),
                )
            )

            file.write("\n")

    except Exception as exc:

        logger.error(
            "Failed to write %s: %s",
            path,
            exc,
        )


# ============================================================
# AUTHENTICATION
# ============================================================

async def authenticate(websocket):
    """
    Wait for the first message from the ESP32.

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

        logger.warning(
            "Authentication timeout"
        )

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

        logger.warning(
            "Invalid JSON authentication message"
        )

        await websocket.close(
            code=4003,
            reason="Invalid authentication",
        )

        return None

    if message.get("type") != "auth":

        logger.warning(
            "First message was not authentication"
        )

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

    devices = load_devices()

    device = devices.get(device_id)

    if device is None:

        logger.warning(
            "Unknown device attempted authentication: %s",
            device_id,
        )

        await websocket.send(
            json.dumps(
                {
                    "type": "auth_failed",
                    "reason": "unknown_device",
                }
            )
        )

        await websocket.close(
            code=4006,
            reason="Unknown device",
        )

        return None

    if not device.get("enabled", False):

        logger.warning(
            "Disabled device attempted authentication: %s",
            device_id,
        )

        await websocket.send(
            json.dumps(
                {
                    "type": "auth_failed",
                    "reason": "device_disabled",
                }
            )
        )

        await websocket.close(
            code=4007,
            reason="Device disabled",
        )

        return None

    expected_token = device.get("token")

    if token != expected_token:

        logger.warning(
            "Invalid token for device: %s",
            device_id,
        )

        await websocket.send(
            json.dumps(
                {
                    "type": "auth_failed",
                    "reason": "invalid_token",
                }
            )
        )

        await websocket.close(
            code=4008,
            reason="Invalid token",
        )

        return None

    # --------------------------------------------------------
    # Authentication successful
    # --------------------------------------------------------

    logger.info(
        "Device authenticated: %s (%s)",
        device_id,
        device.get("name", "unnamed"),
    )

    # Disconnect an existing connection from the same device.
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

    device_sessions[device_id] = {
        "device_id": device_id,
        "name": device.get("name"),
        "spot": device.get("spot"),
        "connected_at": utc_timestamp(),
        "last_seen": utc_timestamp(),
    }

    await websocket.send(
        json.dumps(
            {
                "type": "auth_ok",
                "device_id": device_id,
                "server_time": utc_timestamp(),
            }
        )
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
    """
    Handle sensor messages from an ESP32.
    """

    device_sessions[device_id]["last_seen"] = utc_timestamp()

    message["server_timestamp"] = utc_timestamp()

    logger.info(
        "Sensor from %s: %s",
        device_id,
        json.dumps(message),
    )

    append_jsonl(
        SENSOR_LOG,
        message,
    )


# ============================================================
# STATUS HANDLING
# ============================================================

async def handle_status_message(
    websocket,
    device_id,
    message,
):
    """
    Handle periodic ESP32 status messages.
    """

    device_sessions[device_id]["last_seen"] = utc_timestamp()

    logger.info(
        "Status from %s | RSSI=%s | IP=%s | IR=%s | video=%s",
        device_id,
        message.get("wifi", {}).get("rssi"),
        message.get("wifi", {}).get("ip"),
        message.get("sensor", {}).get("ir"),
        message.get("video", {}).get("source"),
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

    # --------------------------------------------------------
    # Log video frame
    # --------------------------------------------------------

    append_jsonl(
        VIDEO_LOG,
        frame_info,
    )

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
    """
    Handle video_status messages from ESP32.
    """

    source = message.get(
        "source",
        "unknown",
    )

    camera_enabled = message.get(
        "camera_enabled",
        False,
    )

    camera_initialized = message.get(
        "camera_initialized",
        False,
    )

    logger.info(
        "Video status from %s | "
        "source=%s | "
        "camera_enabled=%s | "
        "camera_initialized=%s",
        device_id,
        source,
        camera_enabled,
        camera_initialized,
    )


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
        "Frame directory: %s",
        FRAME_DIR,
    )

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
