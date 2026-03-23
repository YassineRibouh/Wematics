from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.models import FileRecord, FileSource, VariableGlossary
from app.services.timestamps import cadence_expected_count


@dataclass(slots=True)
class GapRow:
    date: str
    missing_start: str | None
    missing_end: str | None
    expected_count: int
    observed_count: int
    completeness_pct: float
    missing_count: int


def _normalize_second(value: datetime) -> datetime:
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.replace(microsecond=0)


def _group_by_date(records: list[FileRecord]) -> dict[str, list[FileRecord]]:
    grouped: dict[str, list[FileRecord]] = {}
    for row in records:
        grouped.setdefault(row.date, []).append(row)
    return grouped


class DiffService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def _cadence_for_variable(self, variable: str, fallback: int) -> int:
        glossary = self.db.scalar(select(VariableGlossary).where(VariableGlossary.variable == variable))
        if glossary and glossary.expected_cadence_seconds:
            return glossary.expected_cadence_seconds
        return fallback

    def compare(
        self,
        source_a: FileSource,
        source_b: FileSource,
        camera: str,
        variable: str,
        date_from: str | None = None,
        date_to: str | None = None,
        cadence_seconds: int = 15,
    ) -> dict:
        stmt = select(FileRecord).where(
            and_(
                FileRecord.camera == camera,
                FileRecord.variable == variable,
                FileRecord.source.in_([source_a, source_b]),
            )
        )
        if date_from:
            stmt = stmt.where(FileRecord.date >= date_from)
        if date_to:
            stmt = stmt.where(FileRecord.date <= date_to)
        records = self.db.scalars(stmt).all()
        grouped = {
            source_a: _group_by_date([r for r in records if r.source == source_a]),
            source_b: _group_by_date([r for r in records if r.source == source_b]),
        }

        a_dates = set(grouped[source_a].keys())
        b_dates = set(grouped[source_b].keys())
        missing_dates = sorted(a_dates - b_dates)

        cadence = self._cadence_for_variable(variable=variable, fallback=cadence_seconds)
        gaps: list[GapRow] = []
        for day in sorted(a_dates):
            base_rows = grouped[source_a].get(day, [])
            target_rows = grouped[source_b].get(day, [])

            base_times = [_normalize_second(r.parsed_timestamp) for r in base_rows if r.parsed_timestamp]
            target_set = {
                _normalize_second(r.parsed_timestamp)
                for r in target_rows
                if r.parsed_timestamp
            }

            expected_count = len(base_rows)
            if base_times and cadence > 0:
                expected_count = max(expected_count, cadence_expected_count(min(base_times), max(base_times), cadence))

            observed_count = len(target_rows)
            missing_points = sorted(t for t in base_times if t not in target_set)
            missing_count = len(missing_points)
            completeness = 100.0 if expected_count == 0 else (observed_count / expected_count) * 100.0

            if missing_points:
                start = missing_points[0]
                prev = missing_points[0]
                for item in missing_points[1:]:
                    if (item - prev) > timedelta(seconds=max(cadence, 1) * 1.5):
                        gaps.append(
                            GapRow(
                                date=day,
                                missing_start=start.isoformat(),
                                missing_end=prev.isoformat(),
                                expected_count=expected_count,
                                observed_count=observed_count,
                                completeness_pct=round(completeness, 2),
                                missing_count=missing_count,
                            )
                        )
                        start = item
                    prev = item
                gaps.append(
                    GapRow(
                        date=day,
                        missing_start=start.isoformat(),
                        missing_end=prev.isoformat(),
                        expected_count=expected_count,
                        observed_count=observed_count,
                        completeness_pct=round(completeness, 2),
                        missing_count=missing_count,
                    )
                )
            elif completeness < 100.0:
                gaps.append(
                    GapRow(
                        date=day,
                        missing_start=None,
                        missing_end=None,
                        expected_count=expected_count,
                        observed_count=observed_count,
                        completeness_pct=round(completeness, 2),
                        missing_count=max(expected_count - observed_count, 0),
                    )
                )

        latest_a = max((r.parsed_timestamp for r in records if r.source == source_a and r.parsed_timestamp), default=None)
        latest_b = max((r.parsed_timestamp for r in records if r.source == source_b and r.parsed_timestamp), default=None)
        return {
            "source_a": source_a.value,
            "source_b": source_b.value,
            "camera": camera,
            "variable": variable,
            "missing_dates": missing_dates,
            "gap_rows": [asdict(row) for row in gaps],
            "summary": {
                "dates_in_source_a": len(a_dates),
                "dates_in_source_b": len(b_dates),
                "missing_dates_count": len(missing_dates),
                "partial_days_count": len({row.date for row in gaps}),
                "latest_source_a": latest_a.isoformat() if latest_a else None,
                "latest_source_b": latest_b.isoformat() if latest_b else None,
            },
        }
