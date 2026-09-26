#!/usr/bin/env python3

import asyncio
import json
import logging
import ssl
import threading
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote, urlparse

import websockets
from PIL import Image

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

# Development HTTP server for the converted JPEG frames.
HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8080

WEBSOCKET_PATH = "/parking"

SERVER_CERT = Path("certs/server.crt")
SERVER_KEY = Path("certs/server.key")

DATA_DIR = Path("data")
FRAME_DIR = DATA_DIR / "frames"

AUTH_TIMEOUT = 10

PING_INTERVAL = 15

MAX_MESSAGE_SIZE = 5 * 1024 * 1024

# RHYX M21-45 / ESP32 camera RGB565 byte order.
# Espressif documents the camera framebuffer RGB565 output as MSB-first
# (big-endian). The converter below swaps each 16-bit pixel into the
# little-endian representation expected by Pillow's BGR;16 decoder.
RGB565_BYTE_ORDER = "big"
JPEG_QUALITY = 85


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

# Background HTTP server used to serve the latest JPEG frames.
http_server = None


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
# RGB565 -> JPEG
# ============================================================

def rgb565_to_jpeg(
    binary_data: bytes,
    width: int,
    height: int,
    byte_order: str = RGB565_BYTE_ORDER,
    quality: int = JPEG_QUALITY,
) -> bytes:
    """
    Convert a raw RGB565 framebuffer to JPEG.

    The ESP32 sends two bytes per pixel. The RHYX M21-45 stream is
    expected to be RGB565, so the host converts it to RGB and then
    encodes it as JPEG for browsers and other HTTP clients.
    """

    if width <= 0 or height <= 0:
        raise ValueError("RGB565 frame dimensions must be positive")

    expected_size = width * height * 2

    if len(binary_data) != expected_size:
        raise ValueError(
            f"Invalid RGB565 frame size: received {len(binary_data)} "
            f"bytes, expected {expected_size} for {width}x{height}"
        )

    if byte_order not in ("big", "little"):
        raise ValueError(
            "RGB565 byte order must be 'big' or 'little'"
        )

    # Pillow's BGR;16 decoder expects little-endian 16-bit words.
    # The ESP32 camera RGB565 framebuffer is MSB-first by default, so
    # swap each 16-bit pixel when receiving big-endian RGB565.
    if byte_order == "big":
        pixel_data = bytearray(binary_data)
        pixel_data[0::2], pixel_data[1::2] = (
            pixel_data[1::2],
            pixel_data[0::2],
        )
        pixel_data = bytes(pixel_data)
    else:
        pixel_data = binary_data

    image = Image.frombytes(
        "RGB",
        (width, height),
        pixel_data,
        "raw",
        "BGR;16",
    )

    output = bytearray()

    # Pillow requires a file-like object for JPEG encoding.
    from io import BytesIO

    buffer = BytesIO()

    image.save(
        buffer,
        format="JPEG",
        quality=quality,
        optimize=True,
    )

    return buffer.getvalue()


# ============================================================
# HTTP JPEG SERVER
# ============================================================

class FrameHTTPHandler(BaseHTTPRequestHandler):
    """
    Development HTTP endpoint for the latest JPEG frames.

    Example:

        http://HOST:8080/frames/PARKING-ESP32-001.jpg
    """

    server_version = "ParkingFrameServer/1.0"

    def _send_frame(self, send_body=True):
        parsed = urlparse(self.path)

        if not parsed.path.startswith("/frames/"):
            self.send_error(
                HTTPStatus.NOT_FOUND,
                "Use /frames/<device_id>.jpg",
            )
            return

        filename = unquote(
            parsed.path[len("/frames/"):]
        )

        # Only serve a single filename. This prevents path traversal.
        if (
            not filename
            or filename in (".", "..")
            or Path(filename).name != filename
            or not filename.lower().endswith(".jpg")
        ):
            self.send_error(
                HTTPStatus.BAD_REQUEST,
                "Invalid frame filename",
            )
            return

        frame_path = FRAME_DIR / filename

        if not frame_path.is_file():
            self.send_error(
                HTTPStatus.NOT_FOUND,
                "Frame not available",
            )
            return

        try:
            data = frame_path.read_bytes()
        except OSError:
            self.send_error(
                HTTPStatus.INTERNAL_SERVER_ERROR,
                "Failed to read frame",
            )
            return

        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "image/jpeg")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
        self.send_header("Pragma", "no-cache")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        if send_body:
            self.wfile.write(data)

    def do_GET(self):
        self._send_frame(send_body=True)

    def do_HEAD(self):
        self._send_frame(send_body=False)

    def log_message(self, format, *args):
        logger.info(
            "HTTP %s - %s",
            self.address_string(),
            format % args,
        )


def start_http_server():
    """Start the development JPEG HTTP server in a background thread."""

    global http_server

    http_server = ThreadingHTTPServer(
        (HTTP_HOST, HTTP_PORT),
        FrameHTTPHandler,
    )

    thread = threading.Thread(
        target=http_server.serve_forever,
        name="jpeg-http-server",
        daemon=True,
    )

    thread.start()

    logger.info(
        "JPEG HTTP server listening on http://%s:%d",
        HTTP_HOST,
        HTTP_PORT,
    )


def stop_http_server():
    """Stop the development JPEG HTTP server."""

    global http_server

    if http_server is not None:
        http_server.shutdown()
        http_server.server_close()
        http_server = None


# ============================================================
# VIDEO METADATA
# ============================================================

async def handle_video_metadata(
    websocket,
    device_id,
    message,
):
    """
    Receive metadata for the next video frame.

    The ESP32 sends:

        TEXT:
        {
            "type": "video_frame",
            "format": "rgb565",
            "width": 320,
            "height": 240,
            ...
        }

        BINARY:
        <raw RGB565>

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
    Handle a binary video frame.

    Supported formats:

        jpeg   -> stored directly as JPEG
        rgb565 -> converted to JPEG on the host, then stored
    """

    # --------------------------------------------------------
    # Get metadata
    # --------------------------------------------------------

    metadata = pending_video_metadata.pop(
        websocket,
        None,
    )

    if metadata is None:

        logger.warning(
            "Binary frame received from %s without metadata",
            device_id,
        )

        return

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

    frame_format = str(
        metadata.get(
            "format",
            "jpeg",
        )
    ).lower()

    # --------------------------------------------------------
    # Convert / validate frame
    # --------------------------------------------------------

    if frame_format == "rgb565":

        try:
            width = int(metadata["width"])
            height = int(metadata["height"])

            byte_order = str(
                metadata.get(
                    "byte_order",
                    RGB565_BYTE_ORDER,
                )
            ).lower()

            jpeg_data = rgb565_to_jpeg(
                binary_data,
                width,
                height,
                byte_order=byte_order,
                quality=JPEG_QUALITY,
            )

        except (KeyError, TypeError, ValueError) as exc:

            logger.warning(
                "Invalid RGB565 frame from %s: %s",
                device_id,
                exc,
            )

            return

        output_format = "rgb565"
        raw_size = len(binary_data)

    elif frame_format == "jpeg":

        # ----------------------------------------------------
        # Validate JPEG SOI marker.
        # ----------------------------------------------------

        if len(binary_data) < 4 or binary_data[0:2] != b"\xff\xd8":

            logger.warning(
                "Binary frame from %s is not a valid JPEG",
                device_id,
            )

            return

        jpeg_data = binary_data
        output_format = "jpeg"
        raw_size = len(binary_data)

        width = metadata.get("width")
        height = metadata.get("height")

    else:

        logger.warning(
            "Unsupported video format from %s: %s",
            device_id,
            frame_format,
        )

        return

    # --------------------------------------------------------
    # Save latest frame as JPEG
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

            file.write(jpeg_data)

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

    server_timestamp = utc_timestamp()

    frame_info = {
        "device_id": device_id,
        "source": source,
        "format": output_format,
        "frame_id": frame_id,
        "timestamp": timestamp,
        "server_timestamp": server_timestamp,
        "raw_size": raw_size,
        "jpeg_size": len(jpeg_data),
        "path": str(frame_path),
    }

    if width is not None:
        frame_info["width"] = width

    if height is not None:
        frame_info["height"] = height

    if frame_format == "rgb565":
        frame_info["rgb565_byte_order"] = metadata.get(
            "byte_order",
            RGB565_BYTE_ORDER,
        )

    latest_frames[device_id] = frame_info

    device_sessions[device_id]["last_seen"] = server_timestamp

    logger.info(
        "Video frame from %s | source=%s | format=%s | "
        "frame=%s | raw=%d bytes | JPEG=%d bytes",
        device_id,
        source,
        frame_format,
        frame_id,
        raw_size,
        len(jpeg_data),
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
        "Frame directory: %s",
        FRAME_DIR,
    )

    logger.info(
        "JPEG quality: %d",
        JPEG_QUALITY,
    )

    logger.info(
        "RGB565 byte order: %s",
        RGB565_BYTE_ORDER,
    )

    # --------------------------------------------------------
    # PostgreSQL
    # --------------------------------------------------------

    check_database()

    # --------------------------------------------------------
    # HTTP JPEG server
    # --------------------------------------------------------

    start_http_server()

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

            stop_http_server()


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
