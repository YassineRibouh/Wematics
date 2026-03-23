from __future__ import annotations

from datetime import UTC, datetime


def utc_now() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def utc_from_timestamp(value: float) -> datetime:
    return datetime.fromtimestamp(value, UTC).replace(tzinfo=None)
