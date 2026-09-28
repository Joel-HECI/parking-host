#!/usr/bin/env python3
"""HTTPS video, frame, and database browser server for the parking host."""

from __future__ import annotations

import json
import logging
import threading
import time
from datetime import date, datetime, time as dtime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from db import delete_device, list_devices, list_events, register_device
from time_utils import format_utc_timestamp

logger = logging.getLogger("parking-host")

HTTP_STREAM_BOUNDARY = "parking-stream"
HTTP_STREAM_TIMEOUT = 30.0


def _device_id_from_path(path: str):
    device_id = unquote(path.strip("/"))
    if not device_id or device_id in (".", ".."):
        return None
    if "/" in device_id or "\\" in device_id:
        return None
    return device_id


def _build_handler(state):
    class ParkingHTTPHandler(BaseHTTPRequestHandler):
        server_version = "ParkingFrameServer/3.0"
        protocol_version = "HTTP/1.1"

        @staticmethod
        def _json_default(value):
            if isinstance(value, (datetime, date, dtime)):
                return format_utc_timestamp(value)
            return str(value)

        def _send_json(self, payload, status=HTTPStatus.OK):
            body = json.dumps(
                payload,
                separators=(",", ":"),
                default=self._json_default,
            ).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def _send_file(self, file_path: Path, content_type: str, status=HTTPStatus.OK):
            try:
                body = file_path.read_bytes()
            except OSError:
                self.send_error(HTTPStatus.NOT_FOUND, "File not found")
                return

            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(body)

        def _serve_index(self):
            self._send_file(state.web_dir / "index.html", "text/html; charset=utf-8")

        def _serve_db(self):
            self._send_file(state.web_dir / "db.html", "text/html; charset=utf-8")

        def _serve_static(self, path: str):
            assets = {
                "/styles.css": (state.web_dir / "styles.css", "text/css; charset=utf-8"),
                "/app.js": (state.web_dir / "app.js", "application/javascript; charset=utf-8"),
                "/db.css": (state.web_dir / "db.css", "text/css; charset=utf-8"),
                "/db.js": (state.web_dir / "db.js", "application/javascript; charset=utf-8"),
            }
            for prefix, (file_path, content_type) in assets.items():
                if path.startswith(prefix):
                    self._send_file(file_path, content_type)
                    return
            self.send_error(HTTPStatus.NOT_FOUND, "Unknown static asset")

        def _send_frame(self, send_body=True):
            parsed = urlparse(self.path)
            path = parsed.path
            if not path.startswith("/frames/"):
                self.send_error(HTTPStatus.NOT_FOUND, "Use /frames/<device_id>.jpg")
                return

            device_id = _device_id_from_path(path[len("/frames/"):])
            if not device_id:
                self.send_error(HTTPStatus.BAD_REQUEST, "Invalid frame filename")
                return

            frame_data = get_cached_frame_bytes(state, device_id)
            if not frame_data:
                self.send_error(HTTPStatus.NOT_FOUND, "Frame not available")
                return

            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "image/jpeg")
            self.send_header("Content-Length", str(len(frame_data)))
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            if send_body:
                self.wfile.write(frame_data)

        def _stream_mjpeg(self, device_id: str):
            boundary = f"--{HTTP_STREAM_BOUNDARY}".encode("ascii")
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", f"multipart/x-mixed-replace; boundary={HTTP_STREAM_BOUNDARY}")
            self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
            self.send_header("Pragma", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.send_header("Strict-Transport-Security", "max-age=31536000; includeSubDomains")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            self.wfile.write(b"\r\n")
            self.wfile.flush()

            start_time = time.monotonic()
            last_frame = None
            while True:
                if self.connection is None:
                    break

                frame_data = get_cached_frame_bytes(state, device_id)
                if frame_data:
                    last_frame = frame_data

                if last_frame:
                    payload = (
                        boundary + b"\r\n"
                        + b"Content-Type: image/jpeg\r\n"
                        + f"Content-Length: {len(last_frame)}\r\n\r\n".encode("ascii")
                        + last_frame
                        + b"\r\n"
                    )
                    try:
                        self.wfile.write(payload)
                        self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError, OSError):
                        logger.info("MJPEG client disconnected for %s", device_id)
                        break

                if time.monotonic() - start_time >= HTTP_STREAM_TIMEOUT:
                    logger.info("MJPEG stream timed out for %s", device_id)
                    break

                time.sleep(0.1)

        def _api_devices(self):
            self._send_json({"devices": get_device_summaries(state)})

        def _api_db_devices(self):
            if self.command == "POST":
                content_length = int(self.headers.get("Content-Length", "0"))
                raw_body = self.rfile.read(content_length) if content_length > 0 else b"{}"
                try:
                    payload = json.loads(raw_body.decode("utf-8"))
                except json.JSONDecodeError:
                    self.send_error(HTTPStatus.BAD_REQUEST, "Invalid JSON body")
                    return

                device_id = str(payload.get("device_id", "")).strip()
                token = str(payload.get("token", "")).strip()
                if not device_id or not token:
                    self.send_error(HTTPStatus.BAD_REQUEST, "device_id and token are required")
                    return

                try:
                    device_pk = register_device(
                        device_id=device_id,
                        token=token,
                        name=payload.get("name"),
                        spot=payload.get("spot"),
                        metadata=payload.get("metadata") or {},
                    )
                except Exception as exc:
                    logger.exception("Failed to register device %s: %s", device_id, exc)
                    self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "Failed to register device")
                    return

                self._send_json({"ok": True, "device_pk": device_pk}, status=HTTPStatus.CREATED)
                return

            self._send_json({"devices": list_devices()})

        def _api_db_device_delete(self, device_id: str):
            try:
                deleted_pk = delete_device(device_id)
            except Exception as exc:
                logger.exception("Failed to delete device %s: %s", device_id, exc)
                self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR, "Failed to delete device")
                return

            if deleted_pk is None:
                self.send_error(HTTPStatus.NOT_FOUND, "Device not found")
                return

            self._send_json({"ok": True, "deleted": device_id, "device_pk": deleted_pk})

        def _api_db_events(self):
            parsed = urlparse(self.path)
            query = parse_qs(parsed.query)
            event_type = query.get("event_type", [None])[0]
            device_id = query.get("device_id", [None])[0]

            def _parse_int(name: str, default: int):
                raw = query.get(name, [default])[0]
                try:
                    return max(0, int(raw))
                except (TypeError, ValueError):
                    return default

            limit = min(_parse_int("limit", 100), 500)
            offset = _parse_int("offset", 0)
            self._send_json({
                "events": list_events(
                    event_type=event_type,
                    device_id=device_id,
                    limit=limit,
                    offset=offset,
                ),
                "limit": limit,
                "offset": offset,
            })

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path

            if path == "/" or path == "/index.html":
                self._serve_index()
                return

            if path == "/db" or path == "/db.html":
                self._serve_db()
                return

            if path == "/api/devices":
                self._api_devices()
                return

            if path == "/api/db/devices":
                self._api_db_devices()
                return

            if path == "/api/db/events":
                self._api_db_events()
                return

            if path.startswith("/styles.css") or path.startswith("/app.js") or path.startswith("/db.css") or path.startswith("/db.js"):
                self._serve_static(path)
                return

            if path.startswith("/video/"):
                device_id = _device_id_from_path(path[len("/video/") :])
                if device_id is None or not device_id.endswith(".mjpg"):
                    self.send_error(HTTPStatus.BAD_REQUEST, "Use /video/<device_id>.mjpg")
                    return

                device_id = device_id[:-5]
                if not device_id:
                    self.send_error(HTTPStatus.BAD_REQUEST, "Invalid device identifier")
                    return

                self._stream_mjpeg(device_id)
                return

            if path.startswith("/frames/"):
                self._send_frame(send_body=True)
                return

            self.send_error(HTTPStatus.NOT_FOUND, "Unknown HTTP route")

        def do_POST(self):
            parsed = urlparse(self.path)
            path = parsed.path

            if path == "/api/db/devices":
                self._api_db_devices()
                return

            self.send_error(HTTPStatus.NOT_FOUND, "Unknown HTTP route")

        def do_DELETE(self):
            parsed = urlparse(self.path)
            path = parsed.path

            if path.startswith("/api/db/devices/"):
                device_id = _device_id_from_path(path[len("/api/db/devices/"):])
                if not device_id:
                    self.send_error(HTTPStatus.BAD_REQUEST, "Invalid device identifier")
                    return
                self._api_db_device_delete(device_id)
                return

            self.send_error(HTTPStatus.NOT_FOUND, "Unknown HTTP route")

        def do_HEAD(self):
            parsed = urlparse(self.path)
            path = parsed.path

            if path == "/" or path == "/index.html":
                self._send_file(state.web_dir / "index.html", "text/html; charset=utf-8")
                return

            if path == "/db" or path == "/db.html":
                self._send_file(state.web_dir / "db.html", "text/html; charset=utf-8")
                return

            if path == "/api/devices":
                self._api_devices()
                return

            if path == "/api/db/devices":
                self._api_db_devices()
                return

            if path == "/api/db/events":
                self._api_db_events()
                return

            if path.startswith("/styles.css") or path.startswith("/app.js") or path.startswith("/db.css") or path.startswith("/db.js"):
                self._serve_static(path)
                return

            if path.startswith("/frames/"):
                self._send_frame(send_body=False)
                return

            if path.startswith("/video/"):
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", f"multipart/x-mixed-replace; boundary={HTTP_STREAM_BOUNDARY}")
                self.send_header("Cache-Control", "no-cache, no-store, must-revalidate")
                self.send_header("Pragma", "no-cache")
                self.send_header("Access-Control-Allow-Origin", "*")
                self.end_headers()
                return

            self.send_error(HTTPStatus.NOT_FOUND, "Unknown HTTP route")

        def log_message(self, format, *args):
            logger.info("HTTP %s - %s", self.address_string(), format % args)

    return ParkingHTTPHandler


def get_cached_frame_bytes(state, device_id: str):
    with state.http_cache_lock:
        info = state.latest_frames.get(device_id)
        if info is not None:
            jpeg_bytes = info.get("jpeg_bytes")
            if jpeg_bytes:
                return jpeg_bytes

        frame_path = state.frame_dir / f"{device_id}.jpg"
        try:
            if frame_path.is_file():
                return frame_path.read_bytes()
        except OSError:
            logger.warning("Failed to read frame cache for device %s", device_id)

        return None


def get_all_device_ids(state):
    ids = set(state.latest_frames.keys())
    try:
        for path in state.frame_dir.glob("*.jpg"):
            ids.add(path.stem)
    except OSError:
        pass
    return sorted(ids)


def get_device_summaries(state):
    device_ids = set(get_all_device_ids(state))
    device_ids.update(state.connected_devices.keys())
    device_ids.update(state.device_sessions.keys())

    summaries = []
    for device_id in sorted(device_ids):
        session = state.device_sessions.get(device_id, {})
        latest_sensor = session.get("latest_sensor", {})
        summaries.append({
            "device_id": device_id,
            "name": session.get("name"),
            "spot": session.get("spot"),
            "last_seen": session.get("last_seen"),
            "connected_at": session.get("connected_at"),
            "disconnected_at": session.get("disconnected_at"),
            "ir": latest_sensor.get("ir"),
            "ir_active": latest_sensor.get("ir_active"),
            "sensor": latest_sensor.get("message"),
        })

    return summaries


def start_http_server(state, ssl_context):
    handler = _build_handler(state)
    http_server = ThreadingHTTPServer((state.http_host, state.http_port), handler)
    http_server.socket = ssl_context.wrap_socket(http_server.socket, server_side=True)

    thread = threading.Thread(target=http_server.serve_forever, name="https-video-server", daemon=True)
    thread.start()

    state.http_server = http_server
    logger.info("HTTPS server listening on https://%s:%d", state.http_host, state.http_port)


def stop_http_server(state):
    if state.http_server is not None:
        state.http_server.shutdown()
        state.http_server.server_close()
        state.http_server = None
