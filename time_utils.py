from __future__ import annotations

from datetime import date, datetime, time, timezone


def format_utc_timestamp(value: datetime | date | time | None) -> str | None:
    if value is None:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.strftime("%Y-%m-%d %H:%M:%S UTC")

    if isinstance(value, date):
        return value.strftime("%Y-%m-%d UTC")

    if isinstance(value, time):
        return value.strftime("%H:%M:%S UTC")

    return str(value)


def utc_now_timestamp() -> str:
    return format_utc_timestamp(datetime.now(timezone.utc))
