from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

TIME_PATTERNS = [
    re.compile(
        r"(?P<date>\d{4}-\d{2}-\d{2})T(?P<h>\d{2})-(?P<m>\d{2})-(?P<s>\d{2})(?P<off>[+-]\d{2}-\d{2})"
    ),
    re.compile(r"(?P<dt>\d{4}-\d{2}-\d{2}_\d{2}_\d{2}_\d{2})"),
    re.compile(r"(?P<dt>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?)"),
]


def parse_filename_timestamp(filename: str) -> datetime | None:
    """
    Parse timestamps from known Wematics filename formats.
    Returns timezone-aware UTC datetimes when offset exists, otherwise naive datetime.
    """
    base = filename
    for suffix in [".webp", ".jpg", ".png", ".jpeg", ".csv", ".txt", ".json"]:
        if base.lower().endswith(suffix):
            base = base[: -len(suffix)]
            break

    match = TIME_PATTERNS[0].search(base)
    if match:
        date_part = match.group("date")
        hour = match.group("h")
        minute = match.group("m")
        second = match.group("s")
        offset_raw = match.group("off")
        sign = offset_raw[0]
        off_hours = offset_raw[1:3]
        off_minutes = offset_raw[4:6]
        offset = f"{sign}{off_hours}:{off_minutes}"
        token = f"{date_part}T{hour}:{minute}:{second}{offset}"
        try:
            return datetime.fromisoformat(token).astimezone(UTC)
        except ValueError:
            pass

    match = TIME_PATTERNS[1].search(base)
    if match:
        try:
            return datetime.strptime(match.group("dt"), "%Y-%m-%d_%H_%M_%S")
        except ValueError:
            pass

    match = TIME_PATTERNS[2].search(base)
    if match:
        token = match.group("dt")
        try:
            return datetime.fromisoformat(token)
        except ValueError:
            pass

    return None
def within_time_window(value: datetime | None, start_hhmmss: str | None, end_hhmmss: str | None) -> bool:
    if value is None:
        return True
    if start_hhmmss is None and end_hhmmss is None:
        return True
    t = value.timetz() if value.tzinfo else value.time()
    if start_hhmmss:
        start = datetime.strptime(start_hhmmss, "%H:%M:%S").time()
        if t < start:
            return False
    if end_hhmmss:
        end = datetime.strptime(end_hhmmss, "%H:%M:%S").time()
        if t > end:
            return False
    return True


def cadence_expected_count(start: datetime | None, end: datetime | None, cadence_seconds: int) -> int:
    if not start or not end or cadence_seconds <= 0:
        return 0
    span = (end - start).total_seconds()
    if span < 0:
        return 0
    return int(span // cadence_seconds) + 1
