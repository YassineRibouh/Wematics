from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from app.core.time import utc_from_timestamp, utc_now
from app.services.timestamps import parse_filename_timestamp, within_time_window


def resolve_dates_from_mode(mode: str, available_dates: list[str], params: dict) -> list[str]:
    ordered = sorted(set(available_dates))
    match mode:
        case "single_date":
            value = params.get("date")
            return [value] if value else []
        case "date_range":
            start = params.get("date_from")
            end = params.get("date_to")
            return [day for day in ordered if (start is None or day >= start) and (end is None or day <= end)]
        case "rolling_days":
            days = int(params.get("rolling_days") or 0)
            if days <= 0:
                return []
            start = (utc_now() - timedelta(days=days - 1)).strftime("%Y-%m-%d")
            return [day for day in ordered if day >= start]
        case "backfill_months":
            months = int(params.get("backfill_months") or 0)
            if months <= 0:
                return []
            start = (utc_now() - timedelta(days=months * 30)).strftime("%Y-%m-%d")
            return [day for day in ordered if day >= start]
        case "latest_only":
            return ordered[-1:] if ordered else []
        case _:
            return []


def apply_file_selection(files: list[str], file_selection: str, newest_n: int | None = None) -> list[str]:
    ordered = sorted(files)
    if not ordered:
        return ordered
    if file_selection == "newest_only":
        return [ordered[-1]]
    if file_selection == "newest_n":
        n = max(1, int(newest_n or 1))
        return ordered[-n:]
    return ordered


def filter_files_by_time_window(files: list[str], start_time: str | None, end_time: str | None) -> list[str]:
    if not start_time and not end_time:
        return files
    result = []
    for filename in files:
        timestamp = parse_filename_timestamp(filename)
        if within_time_window(timestamp, start_time, end_time):
            result.append(filename)
    return result


def should_refresh_csv(
    policy: str,
    local_path: Path,
    remote_filename: str,
    now: datetime | None = None,
    schedule_hours: int = 24,
) -> bool:
    if policy == "always_refresh":
        return True
    if policy == "never_refresh":
        return False
    now = now or utc_now()
    if policy == "scheduled_refresh":
        if not local_path.exists():
            return True
        age_hours = (now - utc_from_timestamp(local_path.stat().st_mtime)).total_seconds() / 3600
        return age_hours >= schedule_hours
    if policy == "remote_newer":
        if not local_path.exists():
            return True
        remote_ts = parse_filename_timestamp(remote_filename)
        if remote_ts is None:
            age_hours = (now - utc_from_timestamp(local_path.stat().st_mtime)).total_seconds() / 3600
            return age_hours >= 24
        local_mtime = utc_from_timestamp(local_path.stat().st_mtime)
        remote_naive = remote_ts.replace(tzinfo=None) if remote_ts.tzinfo else remote_ts
        return remote_naive > local_mtime
    return False
