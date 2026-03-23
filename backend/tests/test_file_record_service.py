from __future__ import annotations

from sqlalchemy import and_, select

from app.models import FileRecord, FileSource
from app.services.file_record_service import get_or_create_file_record


def test_get_or_create_file_record_is_idempotent(db_session) -> None:
    kwargs = {
        "source": FileSource.local,
        "camera": "ROS",
        "variable": "RGB",
        "date": "2025-10-30",
        "filename": "2025-10-30T10-41-45+01-00_rgb.webp",
    }

    first, created_first = get_or_create_file_record(db_session, **kwargs)
    second, created_second = get_or_create_file_record(db_session, **kwargs)
    db_session.commit()

    rows = db_session.scalars(
        select(FileRecord).where(
            and_(
                FileRecord.source == FileSource.local,
                FileRecord.camera == "ROS",
                FileRecord.variable == "RGB",
                FileRecord.date == "2025-10-30",
                FileRecord.filename == "2025-10-30T10-41-45+01-00_rgb.webp",
            )
        )
    ).all()

    assert created_first is True
    assert created_second is False
    assert first.id == second.id
    assert len(rows) == 1
