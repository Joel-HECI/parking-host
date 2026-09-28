#!/usr/bin/env python3
"""Entrypoint for the parking host."""

from __future__ import annotations

import asyncio
import logging
import ssl
import threading
from dataclasses import dataclass, field
from pathlib import Path

from db import get_connection
from https import start_http_server, stop_http_server
from wss import monitor_devices, serve

HOST = "0.0.0.0"
PORT = 8766
HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8080
WEBSOCKET_PATH = "/parking"
SERVER_CERT = Path("certs/server.crt")
SERVER_KEY = Path("certs/server.key")
DATA_DIR = Path("data")
FRAME_DIR = DATA_DIR / "frames"
WEB_DIR = Path("web")

DATA_DIR.mkdir(parents=True, exist_ok=True)
FRAME_DIR.mkdir(parents=True, exist_ok=True)
WEB_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("parking-host")


@dataclass
class AppState:
    websocket_path: str = WEBSOCKET_PATH
    http_host: str = HTTP_HOST
    http_port: int = HTTP_PORT
    frame_dir: Path = FRAME_DIR
    web_dir: Path = WEB_DIR
    connected_devices: dict = field(default_factory=dict)
    device_sessions: dict = field(default_factory=dict)
    latest_frames: dict = field(default_factory=dict)
    pending_video_metadata: dict = field(default_factory=dict)
    http_cache_lock: threading.Lock = field(default_factory=threading.Lock)
    http_server: object | None = None


state = AppState()


def utc_timestamp():
    from wss import utc_timestamp as _utc_timestamp

    return _utc_timestamp()


def check_database():
    try:
        with get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        logger.info("PostgreSQL connection OK")
    except Exception as exc:
        raise RuntimeError(f"PostgreSQL connection failed: {exc}") from exc


def create_ssl_context():
    if not SERVER_CERT.exists():
        raise FileNotFoundError(f"Server certificate not found: {SERVER_CERT}")
    if not SERVER_KEY.exists():
        raise FileNotFoundError(f"Server private key not found: {SERVER_KEY}")

    ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ssl_context.minimum_version = ssl.TLSVersion.TLSv1_2
    ssl_context.load_cert_chain(certfile=str(SERVER_CERT), keyfile=str(SERVER_KEY))
    return ssl_context


async def main():
    logger.info("============================================")
    logger.info("       PARKING IoT WSS SERVER")
    logger.info("============================================")
    logger.info("Listening on %s:%d", HOST, PORT)
    logger.info("WebSocket path: %s", WEBSOCKET_PATH)
    logger.info("Frame directory: %s", FRAME_DIR)

    check_database()

    ssl_context = create_ssl_context()
    logger.info("TLS certificate loaded")

    start_http_server(state, ssl_context)

    async with serve(state, HOST, PORT, ssl_context):
        logger.info("WSS server started successfully")
        logger.info("Waiting for ESP32 devices...")
        monitor_task = asyncio.create_task(monitor_devices(state))
        try:
            await asyncio.Future()
        finally:
            monitor_task.cancel()
            try:
                await monitor_task
            except asyncio.CancelledError:
                pass
            stop_http_server(state)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server stopped by user")
    except Exception as exc:
        logger.exception("Server terminated: %s", exc)
