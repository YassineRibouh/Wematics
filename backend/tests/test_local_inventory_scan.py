from __future__ import annotations

from sqlalchemy import and_, select

from app.core.config import get_settings
from app.models import FileRecord, FileSource
from app.services.local_inventory_service import LocalInventoryService


def test_scan_incremental_handles_pending_local_rows(db_session, tmp_path, monkeypatch) -> None:
    settings = get_settings()
    monkeypatch.setattr(settings, "archive_base_path", str(tmp_path))

    camera = "ROS"
    variable = "RGB"
    date = "2025-10-01"
    filename = "2025-10-01T15-49-15+02-00_rgb.webp"
    date_dir = tmp_path / camera / variable / date
    date_dir.mkdir(parents=True, exist_ok=True)
    local_path = date_dir / filename
    local_path.write_bytes(b"test-image")

    # Stage a local file row without flushing/committing first, matching worker behavior.
    db_session.add(
        FileRecord(
            source=FileSource.local,
            camera=camera,
            variable=variable,
            date=date,
            filename=filename,
            local_path=str(local_path),
            downloaded=True,
        )
    )

    summary = LocalInventoryService(db_session).scan_incremental(camera=camera, variable=variable, force=True)

    rows = db_session.scalars(
        select(FileRecord).where(
            and_(
                FileRecord.source == FileSource.local,
                FileRecord.camera == camera,
                FileRecord.variable == variable,
                FileRecord.date == date,
                FileRecord.filename == filename,
            )
        )
    ).all()

    assert summary.scanned_files == 1
    assert len(rows) == 1
    assert rows[0].local_path == str(local_path)
