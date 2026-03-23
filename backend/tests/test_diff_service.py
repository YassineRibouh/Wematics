from __future__ import annotations

from datetime import datetime

from app.models import FileRecord, FileSource, VariableGlossary
from app.services.diff_service import DiffService


def test_diff_computation_identifies_partial_day(db_session):
    db_session.add(
        VariableGlossary(variable="RGB", description="rgb frames", expected_cadence_seconds=15, is_image_like=True)
    )
    db_session.add_all(
        [
            FileRecord(
                source=FileSource.remote,
                camera="ROS",
                variable="RGB",
                date="2026-02-10",
                filename="2026-02-10T10-00-00+01-00_rgb.webp",
                parsed_timestamp=datetime(2026, 2, 10, 9, 0, 0),
            ),
            FileRecord(
                source=FileSource.remote,
                camera="ROS",
                variable="RGB",
                date="2026-02-10",
                filename="2026-02-10T10-00-15+01-00_rgb.webp",
                parsed_timestamp=datetime(2026, 2, 10, 9, 0, 15),
            ),
            FileRecord(
                source=FileSource.remote,
                camera="ROS",
                variable="RGB",
                date="2026-02-10",
                filename="2026-02-10T10-00-30+01-00_rgb.webp",
                parsed_timestamp=datetime(2026, 2, 10, 9, 0, 30),
            ),
            FileRecord(
                source=FileSource.local,
                camera="ROS",
                variable="RGB",
                date="2026-02-10",
                filename="2026-02-10T10-00-00+01-00_rgb.webp",
                parsed_timestamp=datetime(2026, 2, 10, 9, 0, 0),
            ),
            FileRecord(
                source=FileSource.local,
                camera="ROS",
                variable="RGB",
                date="2026-02-10",
                filename="2026-02-10T10-00-30+01-00_rgb.webp",
                parsed_timestamp=datetime(2026, 2, 10, 9, 0, 30),
            ),
        ]
    )
    db_session.commit()

    result = DiffService(db_session).compare(
        source_a=FileSource.remote,
        source_b=FileSource.local,
        camera="ROS",
        variable="RGB",
        cadence_seconds=15,
    )

    assert result["summary"]["missing_dates_count"] == 0
    assert result["summary"]["partial_days_count"] == 1
    assert len(result["gap_rows"]) >= 1
    assert result["gap_rows"][0]["date"] == "2026-02-10"
    assert result["gap_rows"][0]["missing_count"] >= 1

