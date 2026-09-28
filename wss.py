#!/usr/bin/env python3
"""WebSocket/TLS server logic for the parking host."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from functools import partial
from typing import Any

import websockets
from PIL import Image

from db import authenticate_device, insert_event, update_last_seen
from time_utils import utc_now_timestamp

logger = logging.getLogger("parking-host")

AUTH_TIMEOUT = 10
PING_INTERVAL = 15
MAX_MESSAGE_SIZE = 5 * 1024 * 1024
RGB565_BYTE_ORDER = "big"
JPEG_QUALITY = 85


def utc_timestamp() -> str:
    return utc_now_timestamp()


def parse_timestamp(value: Any):
    if not value:
        return None

    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        logger.warning("Invalid device timestamp: %r", value)
        return None


async def authenticate(state, websocket):
    try:
        raw_message = await asyncio.wait_for(websocket.recv(), timeout=AUTH_TIMEOUT)
    except asyncio.TimeoutError:
        logger.warning("Authentication timeout")
        await websocket.close(code=4001, reason="Authentication timeout")
        return None
    except Exception as exc:
        logger.warning("Failed to receive authentication: %s", exc)
        return None

    if isinstance(raw_message, bytes):
        logger.warning("Binary message received before authentication")
        await websocket.close(code=4002, reason="Authentication required")
        return None

    try:
        message = json.loads(raw_message)
    except json.JSONDecodeError:
        logger.warning("Invalid JSON authentication message")
        await websocket.close(code=4003, reason="Invalid authentication")
        return None

    if message.get("type") != "auth":
        logger.warning("First message was not authentication")
        await websocket.close(code=4004, reason="Authentication required")
        return None

    device_id = message.get("device_id")
    token = message.get("token")

    if not device_id or not token:
        logger.warning("Authentication missing device_id or token")
        await websocket.close(code=4005, reason="Invalid authentication")
        return None

    try:
        device = authenticate_device(device_id, token)
    except Exception as exc:
        logger.exception("Database authentication error for %s: %s", device_id, exc)
        await websocket.send(json.dumps({"type": "auth_failed", "reason": "server_error"}))
        await websocket.close(code=4500, reason="Authentication service error")
        return None

    if device is None:
        logger.warning("Authentication failed for device: %s", device_id)
        await websocket.send(json.dumps({"type": "auth_failed", "reason": "invalid_credentials"}))
        await websocket.close(code=4008, reason="Authentication failed")
        return None

    logger.info("Device authenticated: %s (%s)", device["device_id"], device.get("name") or "unnamed")

    old_websocket = state.connected_devices.get(device_id)
    if old_websocket is not None:
        logger.warning("Device %s already connected. Closing previous connection.", device_id)
        try:
            await old_websocket.close(code=4010, reason="New connection established")
        except Exception:
            pass

    state.connected_devices[device_id] = websocket

    now = utc_timestamp()
    state.device_sessions[device_id] = {
        "device_id": device["device_id"],
        "name": device.get("name"),
        "spot": device.get("spot"),
        "connected_at": now,
        "last_seen": now,
    }

    try:
        update_last_seen(device_id)
    except Exception as exc:
        logger.error("Failed to update last_seen for %s: %s", device_id, exc)

    await websocket.send(json.dumps({"type": "auth_ok", "device_id": device_id, "server_time": now}))
    return device_id


def get_websocket_path(websocket):
    try:
        request = websocket.request
        if request is not None:
            return request.path
    except AttributeError:
        pass

    try:
        return websocket.path
    except AttributeError:
        return None


async def handle_sensor_message(state, websocket, device_id, message):
    server_time = utc_timestamp()
    message["server_timestamp"] = server_time
    state.device_sessions[device_id]["last_seen"] = server_time

    sensor_block = message.get("sensor") if isinstance(message.get("sensor"), dict) else {}
    ir_value = message.get("ir")
    if ir_value is None:
        ir_value = sensor_block.get("value")
    if ir_value is None:
        ir_value = sensor_block.get("ir")

    ir_active = not bool(ir_value)
    if isinstance(ir_value, str):
        ir_active = ir_value.strip().lower() in {"0", "false", "off", "low", "clear", "inactive", "released"}
    elif isinstance(ir_value, (int, float)):
        ir_active = ir_value == 0

    state.device_sessions[device_id]["latest_sensor"] = {
        "timestamp": server_time,
        "message": message,
        "ir": ir_value,
        "ir_active": ir_active,
    }

    try:
        event_id = insert_event(
            device_id=device_id,
            event_type="sensor",
            event_time=parse_timestamp(message.get("timestamp")),
            payload=message,
        )
        update_last_seen(device_id)
        logger.info("Sensor from %s stored as event %s: %s", device_id, event_id, json.dumps(message))
    except Exception as exc:
        logger.exception("Failed to store sensor event from %s: %s", device_id, exc)


async def handle_status_message(state, websocket, device_id, message):
    server_time = utc_timestamp()
    message["server_timestamp"] = server_time
    state.device_sessions[device_id]["last_seen"] = server_time

    try:
        event_id = insert_event(
            device_id=device_id,
            event_type="status",
            event_time=parse_timestamp(message.get("timestamp")),
            payload=message,
        )
        update_last_seen(device_id)
        logger.info(
            "Status from %s stored as event %s | RSSI=%s | IP=%s | IR=%s | video=%s",
            device_id,
            event_id,
            message.get("wifi", {}).get("rssi"),
            message.get("wifi", {}).get("ip"),
            message.get("sensor", {}).get("value"),
            message.get("video", {}).get("source"),
        )
    except Exception as exc:
        logger.exception("Failed to store status event from %s: %s", device_id, exc)


def rgb565_to_jpeg(binary_data: bytes, width: int, height: int, byte_order: str = RGB565_BYTE_ORDER, quality: int = JPEG_QUALITY) -> bytes:
    if width <= 0 or height <= 0:
        raise ValueError("RGB565 frame dimensions must be positive")

    expected_size = width * height * 2
    if len(binary_data) != expected_size:
        raise ValueError(f"Invalid RGB565 frame size: received {len(binary_data)} bytes, expected {expected_size} for {width}x{height}")

    if byte_order not in ("big", "little"):
        raise ValueError("RGB565 byte order must be 'big' or 'little'")

    if byte_order == "big":
        pixel_data = bytearray(binary_data)
        pixel_data[0::2], pixel_data[1::2] = pixel_data[1::2], pixel_data[0::2]
        pixel_data = bytes(pixel_data)
    else:
        pixel_data = binary_data

    image = Image.frombytes("RGB", (width, height), pixel_data, "raw", "BGR;16")

    from io import BytesIO

    buffer = BytesIO()
    image.save(buffer, format="JPEG", quality=quality, optimize=True)
    return buffer.getvalue()


async def handle_video_metadata(state, websocket, device_id, message):
    source = message.get("source", "unknown")
    frame_id = message.get("frame_id")
    state.pending_video_metadata[websocket] = message
    state.device_sessions[device_id]["last_seen"] = utc_timestamp()
    logger.debug("Video metadata from %s | source=%s | frame=%s", device_id, source, frame_id)


async def handle_video_frame(state, websocket, device_id, binary_data):
    metadata = state.pending_video_metadata.pop(websocket, None)
    if metadata is None:
        logger.warning("Binary frame received from %s without metadata", device_id)
        return

    source = metadata.get("source", "unknown")
    frame_id = metadata.get("frame_id", 0)
    timestamp = metadata.get("timestamp", utc_timestamp())
    frame_format = str(metadata.get("format", "jpeg")).lower()
    width = None
    height = None

    if frame_format == "rgb565":
        try:
            width = int(metadata["width"])
            height = int(metadata["height"])
            byte_order = str(metadata.get("byte_order", RGB565_BYTE_ORDER)).lower()
            jpeg_data = rgb565_to_jpeg(binary_data, width, height, byte_order=byte_order, quality=JPEG_QUALITY)
        except (KeyError, TypeError, ValueError) as exc:
            logger.warning("Invalid RGB565 frame from %s: %s", device_id, exc)
            return
        output_format = "rgb565"
        raw_size = len(binary_data)
    elif frame_format == "jpeg":
        if len(binary_data) < 4 or binary_data[0:2] != b"\xff\xd8":
            logger.warning("Binary frame from %s is not a valid JPEG", device_id)
            return
        jpeg_data = binary_data
        output_format = "jpeg"
        raw_size = len(binary_data)
        width = metadata.get("width")
        height = metadata.get("height")
    else:
        logger.warning("Unsupported video format from %s: %s", device_id, frame_format)
        return

    frame_path = state.frame_dir / f"{device_id}.jpg"
    try:
        with open(frame_path, "wb") as file:
            file.write(jpeg_data)
    except Exception as exc:
        logger.error("Failed to save JPEG from %s: %s", device_id, exc)
        return

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
        "jpeg_bytes": jpeg_data,
        "updated_at": time.time(),
    }

    if width is not None:
        frame_info["width"] = width
    if height is not None:
        frame_info["height"] = height
    if frame_format == "rgb565":
        frame_info["rgb565_byte_order"] = metadata.get("byte_order", RGB565_BYTE_ORDER)

    state.latest_frames[device_id] = frame_info
    state.device_sessions[device_id]["last_seen"] = server_timestamp

    logger.info(
        "Video frame from %s | source=%s | format=%s | frame=%s | raw=%d bytes | JPEG=%d bytes",
        device_id,
        source,
        frame_format,
        frame_id,
        raw_size,
        len(jpeg_data),
    )


async def handle_video_status(state, websocket, device_id, message):
    server_time = utc_timestamp()
    message["server_timestamp"] = server_time
    state.device_sessions[device_id]["last_seen"] = server_time

    source = message.get("source", "unknown")
    camera_enabled = message.get("camera_enabled", False)
    camera_initialized = message.get("camera_initialized", False)

    try:
        event_id = insert_event(
            device_id=device_id,
            event_type="video_status",
            event_time=parse_timestamp(message.get("timestamp")),
            payload=message,
        )
        update_last_seen(device_id)
        logger.info(
            "Video status from %s stored as event %s | source=%s | camera_enabled=%s | camera_initialized=%s",
            device_id,
            event_id,
            source,
            camera_enabled,
            camera_initialized,
        )
    except Exception as exc:
        logger.exception("Failed to store video status from %s: %s", device_id, exc)


async def send_to_device(state, device_id, message):
    websocket = state.connected_devices.get(device_id)
    if websocket is None:
        logger.warning("Device %s is not connected", device_id)
        return False

    try:
        await websocket.send(json.dumps(message))
        return True
    except Exception as exc:
        logger.error("Failed to send command to %s: %s", device_id, exc)
        return False


async def set_video_source(state, device_id, source):
    if source not in ("camera", "placeholder"):
        raise ValueError("Video source must be 'camera' or 'placeholder'")

    return await send_to_device(state, device_id, {"type": "set_video_source", "source": source})


async def request_sensor(state, device_id):
    return await send_to_device(state, device_id, {"type": "get_sensor"})


async def handle_client(state, websocket):
    remote_address = websocket.remote_address
    logger.info("Incoming WSS connection from %s", remote_address)

    path = get_websocket_path(websocket)
    if path is not None and path != state.websocket_path:
        logger.warning("Rejected connection from %s: invalid path %s", remote_address, path)
        await websocket.close(code=4000, reason="Invalid WebSocket path")
        return

    device_id = None

    try:
        device_id = await authenticate(state, websocket)
        if device_id is None:
            return

        logger.info("WSS session established for %s", device_id)

        async for message in websocket:
            if isinstance(message, str):
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    logger.warning("Invalid JSON from %s: %s", device_id, message)
                    continue

                message_type = data.get("type", "")

                if message_type == "sensor":
                    await handle_sensor_message(state, websocket, device_id, data)
                elif message_type == "status":
                    await handle_status_message(state, websocket, device_id, data)
                elif message_type == "video_frame":
                    await handle_video_metadata(state, websocket, device_id, data)
                elif message_type == "video_status":
                    await handle_video_status(state, websocket, device_id, data)
                elif message_type == "pong":
                    state.device_sessions[device_id]["last_seen"] = utc_timestamp()
                    logger.debug("Pong from %s", device_id)
                else:
                    logger.info("Unknown message from %s: %s", device_id, message_type)
            elif isinstance(message, bytes):
                await handle_video_frame(state, websocket, device_id, message)
    except websockets.exceptions.ConnectionClosed as exc:
        logger.info("Connection closed for %s: code=%s reason=%s", device_id or "unauthenticated", exc.code, exc.reason)
    except Exception as exc:
        logger.exception("Unexpected error for %s: %s", device_id or "unauthenticated", exc)
    finally:
        if device_id is not None:
            if state.connected_devices.get(device_id) is websocket:
                del state.connected_devices[device_id]
            state.pending_video_metadata.pop(websocket, None)
            if device_id in state.device_sessions:
                state.device_sessions[device_id]["disconnected_at"] = utc_timestamp()
            logger.info("Device disconnected: %s", device_id)


async def monitor_devices(state):
    while True:
        await asyncio.sleep(30)
        if not state.connected_devices:
            logger.info("No ESP32 devices connected")
            continue

        logger.info("Connected devices: %d", len(state.connected_devices))
        for device_id in state.connected_devices:
            session = state.device_sessions.get(device_id, {})
            logger.info("  %s | spot=%s | last_seen=%s", device_id, session.get("spot"), session.get("last_seen"))


def serve(state, host, port, ssl_context):
    return websockets.serve(
        partial(handle_client, state),
        host,
        port,
        ssl=ssl_context,
        ping_interval=20,
        ping_timeout=10,
        max_size=MAX_MESSAGE_SIZE,
    )
