from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from threading import Lock

from sqlalchemy import and_, delete, desc, func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.time import utc_from_timestamp, utc_now
from app.models import FileAuditEvent, FileRecord, FileSource, LocalDateInventory
from app.services.file_record_service import get_or_create_file_record
from app.services.paths import safe_join
from app.services.timestamps import parse_filename_timestamp

logger = logging.getLogger(__name__)
_LAST_SCAN_BY_SCOPE: dict[tuple[str, str], float] = {}
_LAST_SCAN_LOCK = Lock()


@dataclass(slots=True)
class ScanSummary:
    scanned_dates: int
    scanned_files: int
    skipped_dates: int


class LocalInventoryService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.settings = get_settings()
        self.base_path = self.settings.archive_base_dir

    def _iter_camera_variable_date_paths(
        self, camera: str | None = None, variable: str | None = None
    ) -> list[tuple[str, str, str, Path]]:
        paths: list[tuple[str, str, str, Path]] = []
        cameras = [camera] if camera else [p.name for p in self.base_path.iterdir() if p.is_dir()]
        for cam in cameras:
            cam_path = safe_join(self.base_path, cam)
            if not cam_path.exists():
                continue
            variables = [variable] if variable else [p.name for p in cam_path.iterdir() if p.is_dir()]
            for var in variables:
                var_path = safe_join(cam_path, var)
                if not var_path.exists():
                    continue
                for date_path in var_path.iterdir():
                    if date_path.is_dir():
                        paths.append((cam, var, date_path.name, date_path))
        return paths

    def scan_incremental(self, camera: str | None = None, variable: str | None = None, force: bool = False) -> ScanSummary:
        cache_seconds = max(0, int(self.settings.local_scan_cache_seconds))
        scope_key = (camera or "*", variable or "*")
        now_ts = time.time()
        if cache_seconds > 0 and not force:
            with _LAST_SCAN_LOCK:
                previous = _LAST_SCAN_BY_SCOPE.get(scope_key)
            if previous is not None and (now_ts - previous) < cache_seconds:
                return ScanSummary(scanned_dates=0, scanned_files=0, skipped_dates=0)

        self.db.flush()

        scanned_dates = 0
        scanned_files = 0
        skipped_dates = 0

        candidate_paths = self._iter_camera_variable_date_paths(camera=camera, variable=variable)
        inventory_map = {
            (row.camera, row.variable, row.date): row
            for row in self.db.scalars(select(LocalDateInventory)).all()
        }

        for cam, var, date, date_path in candidate_paths:
            last_modified = utc_from_timestamp(date_path.stat().st_mtime)
            key = (cam, var, date)
            existing = inventory_map.get(key)
            if existing and existing.last_modified and existing.last_modified >= last_modified:
                skipped_dates += 1
                continue

            scanned_dates += 1
            total_size = 0
            discovered: dict[str, tuple[int, datetime | None, str]] = {}
            for file_path in date_path.iterdir():
                if not file_path.is_file():
                    continue
                filename = file_path.name
                size = file_path.stat().st_size
                ts = parse_filename_timestamp(filename)
                total_size += size
                scanned_files += 1
                discovered[filename] = (size, ts, str(file_path))

            if existing is None:
                existing = LocalDateInventory(camera=cam, variable=var, date=date)
                self.db.add(existing)
            existing.file_count = len(discovered)
            existing.total_size = total_size
            existing.last_modified = last_modified
            existing.scanned_at = utc_now()

            file_records = {
                row.filename: row
                for row in self.db.scalars(
                    select(FileRecord).where(
                        and_(
                            FileRecord.source == FileSource.local,
                            FileRecord.camera == cam,
                            FileRecord.variable == var,
                            FileRecord.date == date,
                        )
                    )
                ).all()
            }
            for filename, (size, ts, path_value) in discovered.items():
                row = file_records.get(filename)
                created = False
                if row is None:
                    row, created = get_or_create_file_record(
                        self.db,
                        source=FileSource.local,
                        camera=cam,
                        variable=var,
                        date=date,
                        filename=filename,
                    )
                    file_records[filename] = row
                if created:
                    self.db.add(
                        FileAuditEvent(
                            camera=cam,
                            variable=var,
                            date=date,
                            filename=filename,
                            source=FileSource.local,
                            action="first_seen_local",
                        )
                    )
                row.file_size = size
                row.parsed_timestamp = ts
                row.local_path = path_value
                row.downloaded = True
                row.downloaded_at = row.downloaded_at or utc_now()
                row.seen_at = utc_now()

            stale_names = set(file_records.keys()) - set(discovered.keys())
            if stale_names:
                self.db.execute(
                    delete(FileRecord).where(
                        and_(
                            FileRecord.source == FileSource.local,
                            FileRecord.camera == cam,
                            FileRecord.variable == var,
                            FileRecord.date == date,
                            FileRecord.filename.in_(stale_names),
                        )
                    )
                )

        self.db.commit()
        if cache_seconds > 0:
            with _LAST_SCAN_LOCK:
                _LAST_SCAN_BY_SCOPE[scope_key] = now_ts
        return ScanSummary(scanned_dates=scanned_dates, scanned_files=scanned_files, skipped_dates=skipped_dates)

    def list_date_inventory(self, camera: str | None = None, variable: str | None = None) -> list[LocalDateInventory]:
        stmt = select(LocalDateInventory).order_by(desc(LocalDateInventory.date))
        if camera:
            stmt = stmt.where(LocalDateInventory.camera == camera)
        if variable:
            stmt = stmt.where(LocalDateInventory.variable == variable)
        return self.db.scalars(stmt).all()

    def list_files(
        self,
        camera: str,
        variable: str,
        date: str,
        page: int = 1,
        page_size: int = 200,
        search: str | None = None,
    ) -> tuple[list[FileRecord], int]:
        stmt = select(FileRecord).where(
            and_(
                FileRecord.source == FileSource.local,
                FileRecord.camera == camera,
                FileRecord.variable == variable,
                FileRecord.date == date,
            )
        )
        count_stmt = select(func.count(FileRecord.id)).where(
            and_(
                FileRecord.source == FileSource.local,
                FileRecord.camera == camera,
                FileRecord.variable == variable,
                FileRecord.date == date,
            )
        )
        if search:
            stmt = stmt.where(FileRecord.filename.ilike(f"%{search}%"))
            count_stmt = count_stmt.where(FileRecord.filename.ilike(f"%{search}%"))
        stmt = stmt.order_by(FileRecord.parsed_timestamp.asc().nullslast(), FileRecord.filename.asc())
        total = int(self.db.scalar(count_stmt) or 0)
        offset = max(0, page - 1) * page_size
        rows = self.db.scalars(stmt.offset(offset).limit(page_size)).all()
        return rows, total

    def summarize_storage(self, camera: str | None = None, variable: str | None = None) -> dict:
        stmt = select(
            func.count(LocalDateInventory.id),
            func.coalesce(func.sum(LocalDateInventory.file_count), 0),
            func.coalesce(func.sum(LocalDateInventory.total_size), 0),
            func.max(LocalDateInventory.scanned_at),
        )
        if camera:
            stmt = stmt.where(LocalDateInventory.camera == camera)
        if variable:
            stmt = stmt.where(LocalDateInventory.variable == variable)
        dates, files, bytes_total, scanned_at = self.db.execute(stmt).one()
        return {
            "dates": int(dates or 0),
            "files": int(files or 0),
            "bytes": int(bytes_total or 0),
            "last_scan_at": scanned_at.isoformat() if scanned_at else None,
        }
