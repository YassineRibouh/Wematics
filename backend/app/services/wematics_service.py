from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from threading import Lock

from sqlalchemy import and_, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.time import utc_now
from app.models import Camera, FileAuditEvent, FileRecord, FileSource, RemoteDateCache
from app.services.file_record_service import get_or_create_file_record
from app.services.timestamps import parse_filename_timestamp

logger = logging.getLogger(__name__)
_DATES_CACHE: dict[tuple[str, str, str], "_CacheEntry"] = {}
_DATES_CACHE_LOCK = Lock()


class WematicsUnavailableError(RuntimeError):
    pass


def _load_client_class():
    try:
        from wematics import Skycamera

        return Skycamera
    except Exception:
        pass
    try:
        from wematics import Pyranocam

        return Pyranocam
    except Exception as exc:
        raise WematicsUnavailableError(
            "Could not import Skycamera or Pyranocam from wematics package."
        ) from exc


@dataclass(slots=True)
class _CacheEntry:
    value: dict
    expires_at: float


class WematicsService:
    def __init__(self, db: Session, api_key: str | None = None) -> None:
        self.db = db
        self.settings = get_settings()
        self.api_key = api_key or self.settings.wematics_api_key
        self._client = None

    @property
    def client(self):
        if self._client is None:
            if not self.api_key:
                raise WematicsUnavailableError(
                    "Wematics API key is missing. Set WEMATICS_API_KEY in environment or .env and restart the backend. "
                    f"Checked env files: {', '.join(self.settings.env_files_checked)}"
                )
            client_cls = _load_client_class()
            self._client = client_cls(self.api_key)
        return self._client

    def _with_retries(self, fn, *args, **kwargs):
        attempts = self.settings.remote_retry_attempts
        base_delay = self.settings.remote_retry_base_delay_seconds
        last_error = None
        for idx in range(attempts):
            try:
                return fn(*args, **kwargs)
            except Exception as exc:
                last_error = exc
                if idx >= attempts - 1:
                    break
                delay = base_delay * (2**idx)
                logger.warning("Wematics call failed (%s), retrying in %.2fs", exc, delay)
                time.sleep(delay)
        raise RuntimeError(f"Wematics request failed after {attempts} attempts: {last_error}") from last_error

    def list_cameras(self) -> list[str]:
        payload = self._with_retries(self.client.list_cameras)
        cameras = payload.get("cameras", []) if isinstance(payload, dict) else []
        for camera in cameras:
            exists = self.db.scalar(select(Camera).where(Camera.name == camera))
            if not exists:
                self.db.add(Camera(name=camera))
        self.db.commit()
        return cameras

    def list_variables(self, camera: str) -> list[str]:
        payload = self._with_retries(self.client.list_variables, camera)
        return payload.get("variables", []) if isinstance(payload, dict) else []

    def list_dates(self, camera: str, variable: str, timezone: str = "local", force_refresh: bool = False) -> list[str]:
        key = (camera, variable, timezone)
        now = time.time()
        with _DATES_CACHE_LOCK:
            entry = _DATES_CACHE.get(key)
            if entry and entry.expires_at > now and not force_refresh:
                return entry.value.get("dates", [])

        payload = self._with_retries(self.client.list_dates, camera, variable)
        dates = payload.get("dates", []) if isinstance(payload, dict) else []
        with _DATES_CACHE_LOCK:
            _DATES_CACHE[key] = _CacheEntry(
                value={"dates": dates},
                expires_at=now + self.settings.remote_dates_ttl_seconds,
            )

        existing = {
            row.date: row
            for row in self.db.scalars(
                select(RemoteDateCache).where(
                    and_(
                        RemoteDateCache.camera == camera,
                        RemoteDateCache.variable == variable,
                        RemoteDateCache.timezone == timezone,
                    )
                )
            ).all()
        }
        for date in dates:
            row = existing.get(date)
            if row is None:
                row = RemoteDateCache(camera=camera, variable=variable, timezone=timezone, date=date)
                self.db.add(row)
            row.fetched_at = utc_now()
        self.db.commit()
        return dates

    def list_files(self, camera: str, variable: str, date: str, timezone: str = "local") -> list[str]:
        payload = self._with_retries(self.client.list_files, camera, variable, date, timezone)
        files = payload.get("files", []) if isinstance(payload, dict) else []

        existing = {
            row.filename: row
            for row in self.db.scalars(
                select(FileRecord).where(
                    and_(
                        FileRecord.source == FileSource.remote,
                        FileRecord.camera == camera,
                        FileRecord.variable == variable,
                        FileRecord.date == date,
                    )
                )
            ).all()
        }
        now = utc_now()
        for name in files:
            item = existing.get(name)
            created = False
            if item is None:
                item, created = get_or_create_file_record(
                    self.db,
                    source=FileSource.remote,
                    camera=camera,
                    variable=variable,
                    date=date,
                    filename=name,
                )
                existing[name] = item
            if created:
                self.db.add(
                    FileAuditEvent(
                        camera=camera,
                        variable=variable,
                        date=date,
                        filename=name,
                        action="first_seen_remote",
                        source=FileSource.remote,
                    )
                )
            item.parsed_timestamp = parse_filename_timestamp(name)
            item.seen_at = now
        self.db.commit()
        return files

    def download_file(self, camera: str, variable: str, filename: str, target_dir: str, timezone: str = "local") -> None:
        self._with_retries(self.client.download_file, camera, variable, filename, target_dir, timezone)
