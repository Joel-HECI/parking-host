import asyncio
import json
import logging
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path

import websockets
from websockets.server import ServerConnection


# ============================================================
# CONFIGURATION
# ============================================================

HOST = "0.0.0.0"
PORT = 8766

WEBSOCKET_PATH = "/parking"

BASE_DIR = Path(__file__).resolve().parent

CERT_DIR = BASE_DIR / "certs"
DEVICE_FILE = BASE_DIR / "devices.json"
DATA_DIR = BASE_DIR / "data"

SERVER_CERT = CERT_DIR / "server.crt"
SERVER_KEY = CERT_DIR / "server.key"

SENSOR_LOG = DATA_DIR / "sensors.jsonl"


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

logger = logging.getLogger("parking-host")


# ============================================================
# GLOBAL DEVICE STATE
# ============================================================

connected_devices = {}


# ============================================================
# UTILITIES
# ============================================================

def utc_timestamp():
    return datetime.now(timezone.utc).isoformat()


def load_devices():
    with open(DEVICE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def log_sensor_event(message):
    DATA_DIR.mkdir(exist_ok=True)

    with open(
        SENSOR_LOG,
        "a",
        encoding="utf-8"
    ) as f:

        f.write(
            json.dumps(message) + "\n"
        )


# ============================================================
# SEND JSON
# ============================================================

async def send_json(websocket, message):

    data = json.dumps(message)

    await websocket.send(data)

    logger.info(
        "TX %s",
        data
    )


# ============================================================
# AUTHENTICATION
# ============================================================

async def authenticate(websocket, message):

    device_id = message.get("device_id")
    token = message.get("token")

    if not device_id or not token:
        logger.warning(
            "Authentication missing device_id/token"
        )

        return False

    devices = load_devices()

    device = devices.get(device_id)

    if device is None:

        logger.warning(
            "Unknown device: %s",
            device_id
        )

        return False

    if not device.get("enabled", False):

        logger.warning(
            "Disabled device attempted connection: %s",
            device_id
        )

        return False

    if token != device.get("token"):

        logger.warning(
            "Invalid token for device: %s",
            device_id
        )

        return False

    connected_devices[device_id] = {
        "websocket": websocket,
        "name": device.get("name"),
        "spot": device.get("spot"),
        "connected_at": utc_timestamp(),
        "last_seen": utc_timestamp()
    }

    logger.info(
        "Device authenticated: %s (%s)",
        device_id,
        device.get("spot")
    )

    await send_json(
        websocket,
        {
            "type": "auth_ok",
            "device_id": device_id,
            "server_time": utc_timestamp()
        }
    )

    return True


# ============================================================
# PROCESS SENSOR
# ============================================================

async def process_sensor(
    websocket,
    device_id,
    message
):

    sensor = message.get("sensor", {})

    sensor_type = sensor.get("type")
    gpio = sensor.get("gpio")
    value = sensor.get("value")

    logger.info(
        "SENSOR | device=%s sensor=%s gpio=%s value=%s",
        device_id,
        sensor_type,
        gpio,
        value
    )

    event = {
        "type": "sensor",
        "device_id": device_id,
        "sensor": sensor,
        "timestamp": message.get(
            "timestamp",
            utc_timestamp()
        ),
        "server_timestamp": utc_timestamp()
    }

    log_sensor_event(event)


# ============================================================
# PROCESS STATUS
# ============================================================

async def process_status(
    websocket,
    device_id,
    message
):

    if device_id in connected_devices:

        connected_devices[
            device_id
        ]["last_seen"] = utc_timestamp()

    logger.info(
        "STATUS | device=%s wifi=%s",
        device_id,
        message.get("wifi")
    )


# ============================================================
# PROCESS PONG
# ============================================================

async def process_pong(
    websocket,
    device_id
):

    if device_id in connected_devices:

        connected_devices[
            device_id
        ]["last_seen"] = utc_timestamp()

    logger.debug(
        "PONG | device=%s",
        device_id
    )


# ============================================================
# HANDLE MESSAGE
# ============================================================

async def process_message(
    websocket,
    message,
    device_id
):

    message_type = message.get("type")

    if message_type == "sensor":

        await process_sensor(
            websocket,
            device_id,
            message
        )

    elif message_type == "status":

        await process_status(
            websocket,
            device_id,
            message
        )

    elif message_type == "pong":

        await process_pong(
            websocket,
            device_id
        )

    else:

        logger.warning(
            "Unknown message type from %s: %s",
            device_id,
            message_type
        )


# ============================================================
# CLIENT HANDLER
# ============================================================

async def handle_client(websocket):

    remote = websocket.remote_address

    logger.info(
        "Incoming WSS connection from %s",
        remote
    )

    device_id = None
    authenticated = False

    try:

        # ----------------------------------------------------
        # Require authentication within 10 seconds
        # ----------------------------------------------------

        try:

            raw_message = await asyncio.wait_for(
                websocket.recv(),
                timeout=10
            )

        except asyncio.TimeoutError:

            logger.warning(
                "Authentication timeout from %s",
                remote
            )

            await websocket.close(
                code=4001,
                reason="Authentication timeout"
            )

            return

        # ----------------------------------------------------
        # Parse authentication message
        # ----------------------------------------------------

        try:

            message = json.loads(
                raw_message
            )

        except json.JSONDecodeError:

            logger.warning(
                "Invalid JSON from %s",
                remote
            )

            await websocket.close(
                code=4002,
                reason="Invalid JSON"
            )

            return

        # ----------------------------------------------------
        # First message MUST be auth
        # ----------------------------------------------------

        if message.get("type") != "auth":

            logger.warning(
                "First message was not authentication"
            )

            await websocket.close(
                code=4003,
                reason="Authentication required"
            )

            return

        # ----------------------------------------------------
        # Authenticate
        # ----------------------------------------------------

        authenticated = await authenticate(
            websocket,
            message
        )

        if not authenticated:

            await send_json(
                websocket,
                {
                    "type": "auth_failed"
                }
            )

            await websocket.close(
                code=4004,
                reason="Authentication failed"
            )

            return

        device_id = message.get(
            "device_id"
        )

        # ----------------------------------------------------
        # Main message loop
        # ----------------------------------------------------

        async for raw_message in websocket:

            if isinstance(
                raw_message,
                bytes
            ):

                logger.info(
                    "Binary frame from %s: %d bytes",
                    device_id,
                    len(raw_message)
                )

                # Camera frames will be processed here later.

                continue

            try:

                message = json.loads(
                    raw_message
                )

            except json.JSONDecodeError:

                logger.warning(
                    "Invalid JSON from %s",
                    device_id
                )

                continue

            logger.info(
                "RX %s",
                raw_message
            )

            await process_message(
                websocket,
                message,
                device_id
            )

    except websockets.exceptions.ConnectionClosed as e:

        logger.info(
            "Connection closed: %s",
            e
        )

    except Exception:

        logger.exception(
            "Unexpected client error"
        )

    finally:

        if device_id:

            existing = connected_devices.get(
                device_id
            )

            if (
                existing
                and existing["websocket"] is websocket
            ):

                del connected_devices[
                    device_id
                ]

            logger.info(
                "Device disconnected: %s",
                device_id
            )


# ============================================================
# SERVER PING
# ============================================================

async def ping_devices():

    while True:

        await asyncio.sleep(15)

        devices = list(
            connected_devices.items()
        )

        for device_id, info in devices:

            websocket = info["websocket"]

            try:

                await send_json(
                    websocket,
                    {
                        "type": "ping",
                        "timestamp": utc_timestamp()
                    }
                )

            except Exception:

                logger.warning(
                    "Could not ping %s",
                    device_id
                )


# ============================================================
# DEVICE MONITOR
# ============================================================

async def monitor_devices():

    while True:

        await asyncio.sleep(5)

        now = time.time()

        for device_id, info in list(
            connected_devices.items()
        ):

            logger.debug(
                "ONLINE | %s | spot=%s",
                device_id,
                info["spot"]
            )


# ============================================================
# MAIN
# ============================================================

async def main():

    DATA_DIR.mkdir(
        exist_ok=True
    )

    # --------------------------------------------------------
    # TLS configuration
    # --------------------------------------------------------

    ssl_context = ssl.SSLContext(
        ssl.PROTOCOL_TLS_SERVER
    )

    ssl_context.minimum_version = (
        ssl.TLSVersion.TLSv1_2
    )

    ssl_context.load_cert_chain(
        certfile=SERVER_CERT,
        keyfile=SERVER_KEY
    )

    # --------------------------------------------------------
    # Start WSS server
    # --------------------------------------------------------

    logger.info(
        "Starting WSS server"
    )

    logger.info(
        "Listening on %s:%d",
        HOST,
        PORT
    )

    logger.info(
        "WebSocket path: %s",
        WEBSOCKET_PATH
    )

    async with websockets.serve(
        handle_client,
        HOST,
        PORT,
        ssl=ssl_context,
        ping_interval=20,
        ping_timeout=10,
        max_size=5 * 1024 * 1024
    ):

        logger.info(
            "WSS server ready"
        )

        await asyncio.gather(
            ping_devices(),
            monitor_devices()
        )


if __name__ == "__main__":

    try:

        asyncio.run(
            main()
        )

    except KeyboardInterrupt:

        logger.info(
            "Server stopped"
        )
