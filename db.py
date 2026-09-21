import hashlib
import json
import os
from datetime import datetime, timezone
from dotenv import load_dotenv
import psycopg
from psycopg.rows import dict_row


load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")


def get_connection():
    return psycopg.connect(
        DATABASE_URL,
        row_factory=dict_row
    )


def hash_token(token: str) -> str:
    return hashlib.sha256(
        token.encode("utf-8")
    ).hexdigest()


def get_device(device_id: str):
    with get_connection() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                SELECT
                    id,
                    device_id,
                    name,
                    spot,
                    token_hash,
                    enabled,
                    last_seen,
                    metadata
                FROM devices
                WHERE device_id = %s
                """,
                (device_id,)
            )

            return cur.fetchone()


def authenticate_device(
    device_id: str,
    token: str
):
    device = get_device(device_id)

    if device is None:
        return None

    if not device["enabled"]:
        return None

    supplied_hash = hash_token(token)

    if supplied_hash != device["token_hash"]:
        return None

    return device


def update_last_seen(device_id: str):

    with get_connection() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                UPDATE devices
                SET last_seen = NOW()
                WHERE device_id = %s
                """,
                (device_id,)
            )


def insert_event(
    device_id: str,
    event_type: str,
    payload: dict,
    event_time=None,
    metadata=None
):

    if metadata is None:
        metadata = {}

    with get_connection() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO events (
                    device_id,
                    event_type,
                    event_time,
                    payload,
                    metadata
                )
                SELECT
                    d.id,
                    %s,
                    %s,
                    %s::jsonb,
                    %s::jsonb
                FROM devices d
                WHERE d.device_id = %s
                RETURNING id
                """,
                (
                    event_type,
                    event_time,
                    json.dumps(payload),
                    json.dumps(metadata),
                    device_id
                )
            )

            result = cur.fetchone()

            if result is None:
                raise ValueError(
                    f"Unknown device: {device_id}"
                )

            return result["id"]


def register_device(
    device_id: str,
    token: str,
    name: str = None,
    spot: str = None,
    metadata=None
):

    if metadata is None:
        metadata = {}

    token_hash = hash_token(token)

    with get_connection() as conn:

        with conn.cursor() as cur:

            cur.execute(
                """
                INSERT INTO devices (
                    device_id,
                    name,
                    spot,
                    token_hash,
                    metadata
                )
                VALUES (
                    %s,
                    %s,
                    %s,
                    %s,
                    %s::jsonb
                )
                ON CONFLICT (device_id)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    spot = EXCLUDED.spot,
                    token_hash = EXCLUDED.token_hash,
                    metadata = EXCLUDED.metadata,
                    enabled = TRUE
                RETURNING id
                """,
                (
                    device_id,
                    name,
                    spot,
                    token_hash,
                    json.dumps(metadata)
                )
            )

            return cur.fetchone()["id"]
