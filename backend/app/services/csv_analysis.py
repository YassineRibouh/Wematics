from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime
from math import ceil
from pathlib import Path

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models import CsvAnalysisCache
from app.services.hash_utils import sha256_file


def _parse_number(value: str) -> float | None:
    token = value.strip()
    if not token:
        return None
    if "," in token and "." not in token:
        token = token.replace(",", ".")
    try:
        return float(token)
    except ValueError:
        return None


def _parse_time(value: str) -> datetime | None:
    token = value.strip()
    if not token:
        return None
    normalized = token.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d_%H:%M:%S",
        "%Y-%m-%d_%H_%M_%S",
        "%Y/%m/%d %H:%M:%S",
        "%d.%m.%Y %H:%M:%S",
    ):
        try:
            return datetime.strptime(token, fmt)
        except ValueError:
            continue
    return None


@dataclass(slots=True)
class _ColumnStats:
    non_empty: int = 0
    numeric_count: int = 0
    datetime_count: int = 0
    min_value: float | None = None
    max_value: float | None = None

    def push(self, value: str) -> None:
        token = value.strip()
        if not token:
            return
        self.non_empty += 1
        number = _parse_number(token)
        if number is not None:
            self.numeric_count += 1
            self.min_value = number if self.min_value is None else min(self.min_value, number)
            self.max_value = number if self.max_value is None else max(self.max_value, number)
        if _parse_time(token) is not None:
            self.datetime_count += 1


def _quality_hits(matches: int, non_empty: int) -> float:
    if non_empty <= 0:
        return 0.0
    return matches / non_empty


def analyze_csv_for_time_plot(
    path: Path,
    max_rows: int = 3000,
    max_points: int = 900,
    requested_time_column: str | None = None,
    requested_value_column: str | None = None,
) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {path}")
    if max_rows <= 0:
        raise ValueError("max_rows must be > 0")

    with path.open("r", encoding="utf-8", errors="ignore", newline="") as handle:
        reader = csv.DictReader(handle)
        headers = list(reader.fieldnames or [])
        if not headers:
            return {
                "headers": [],
                "rows_scanned": 0,
                "truncated": False,
                "issues": ["CSV header row is missing or unreadable."],
                "time_columns": [],
                "numeric_columns": [],
                "suggested_time_column": None,
                "suggested_value_column": None,
                "plot": None,
                "columns": [],
            }

        stats = {name: _ColumnStats() for name in headers}
        rows: list[dict[str, str]] = []
        truncated = False

        for idx, row in enumerate(reader):
            if idx >= max_rows:
                truncated = True
                break
            cleaned: dict[str, str] = {}
            for name in headers:
                value = (row.get(name) if row else "") or ""
                text = str(value)
                cleaned[name] = text
                stats[name].push(text)
            rows.append(cleaned)

    rows_scanned = len(rows)
    issues: list[str] = []
    if rows_scanned == 0:
        issues.append("CSV has no data rows.")

    columns = []
    for name in headers:
        item = stats[name]
        columns.append(
            {
                "name": name,
                "non_empty": item.non_empty,
                "numeric_count": item.numeric_count,
                "datetime_count": item.datetime_count,
                "numeric_min": item.min_value,
                "numeric_max": item.max_value,
            }
        )

    def _time_rank(name: str) -> tuple[int, float, int]:
        lowered = name.lower()
        semantic_bonus = 2 if ("time" in lowered or "date" in lowered or "stamp" in lowered) else 0
        stat = stats[name]
        return (
            semantic_bonus,
            _quality_hits(stat.datetime_count, stat.non_empty),
            stat.datetime_count,
        )

    def _numeric_rank(name: str) -> tuple[float, int]:
        stat = stats[name]
        return (_quality_hits(stat.numeric_count, stat.non_empty), stat.numeric_count)

    time_columns = [
        {
            "name": name,
            "coverage": round(_quality_hits(stats[name].datetime_count, stats[name].non_empty), 4),
            "matches": stats[name].datetime_count,
        }
        for name in sorted(
            [h for h in headers if stats[h].datetime_count > 0],
            key=_time_rank,
            reverse=True,
        )
    ]
    numeric_columns = [
        {
            "name": name,
            "coverage": round(_quality_hits(stats[name].numeric_count, stats[name].non_empty), 4),
            "matches": stats[name].numeric_count,
            "min": stats[name].min_value,
            "max": stats[name].max_value,
        }
        for name in sorted(
            [h for h in headers if stats[h].numeric_count > 0],
            key=_numeric_rank,
            reverse=True,
        )
    ]

    suggested_time_column = None
    if requested_time_column and any(item["name"] == requested_time_column for item in time_columns):
        suggested_time_column = requested_time_column
    elif time_columns:
        suggested_time_column = time_columns[0]["name"]

    suggested_value_column = None
    if requested_value_column and any(item["name"] == requested_value_column for item in numeric_columns):
        suggested_value_column = requested_value_column
    elif numeric_columns:
        preferred = [item["name"] for item in numeric_columns if item["name"] != suggested_time_column]
        suggested_value_column = preferred[0] if preferred else numeric_columns[0]["name"]

    plot = None
    if suggested_time_column and suggested_value_column:
        points = []
        skipped_rows = 0
        for row in rows:
            dt = _parse_time(row.get(suggested_time_column, ""))
            val = _parse_number(row.get(suggested_value_column, ""))
            if dt is None or val is None:
                skipped_rows += 1
                continue
            points.append({"time": dt.isoformat(), "value": val})

        points.sort(key=lambda item: item["time"])
        if len(points) > max_points:
            step = ceil(len(points) / max_points)
            points = points[::step]

        if points:
            plot = {
                "time_column": suggested_time_column,
                "value_column": suggested_value_column,
                "points": points,
                "skipped_rows": skipped_rows,
            }
        else:
            issues.append(
                f"No plottable values found for time column '{suggested_time_column}' and value column '{suggested_value_column}'."
            )
    else:
        if not time_columns:
            issues.append("No timestamp-like column detected in this CSV.")
        if not numeric_columns:
            issues.append("No numeric column detected in this CSV.")

    return {
        "headers": headers,
        "rows_scanned": rows_scanned,
        "truncated": truncated,
        "issues": issues,
        "time_columns": time_columns,
        "numeric_columns": numeric_columns,
        "suggested_time_column": suggested_time_column,
        "suggested_value_column": suggested_value_column,
        "plot": plot,
        "columns": columns,
    }


def analyze_csv_for_time_plot_cached(
    db: Session,
    path: Path,
    max_rows: int = 3000,
    max_points: int = 900,
    requested_time_column: str | None = None,
    requested_value_column: str | None = None,
) -> dict:
    file_hash = sha256_file(path)
    cache_key = (
        f"{file_hash}|rows:{max_rows}|points:{max_points}|"
        f"time:{requested_time_column or ''}|value:{requested_value_column or ''}"
    )
    cached = db.scalar(select(CsvAnalysisCache).where(CsvAnalysisCache.cache_key == cache_key))
    if cached is not None:
        return dict(cached.result_json or {})

    result = analyze_csv_for_time_plot(
        path=path,
        max_rows=max_rows,
        max_points=max_points,
        requested_time_column=requested_time_column,
        requested_value_column=requested_value_column,
    )
    row = CsvAnalysisCache(
        cache_key=cache_key,
        file_hash=file_hash,
        rows_limit=max_rows,
        time_column=requested_time_column,
        value_column=requested_value_column,
        result_json=json.loads(json.dumps(result, ensure_ascii=True)),
    )
    db.add(row)
    db.commit()
    return result
