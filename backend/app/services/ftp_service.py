from __future__ import annotations

import ftplib
import logging
import posixpath
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from sqlalchemy import and_, func, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.time import utc_now
from app.models import FTPDateInventory, FileAuditEvent, FileRecord, FileSource
from app.services.file_record_service import get_or_create_file_record
from app.services.hash_utils import sha256_bytes, sha256_file

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class UploadOutcome:
    action: str
    remote_path: str | None
    reason: str | None = None


class FTPClientAdapter:
    def __init__(self, ftp: ftplib.FTP) -> None:
        self.ftp = ftp

    def makedirs(self, remote_dir: str) -> None:
        if not remote_dir or remote_dir == "/":
            return
        parts = [part for part in remote_dir.split("/") if part]
        cursor = "/"
        for part in parts:
            cursor = posixpath.join(cursor, part)
            try:
                self.ftp.mkd(cursor)
            except ftplib.error_perm:
                pass

    def file_size(self, path: str) -> int | None:
        try:
            size = self.ftp.size(path)
            return int(size) if size is not None else None
        except Exception:
            return None

    def file_exists(self, path: str) -> bool:
        return self.file_size(path) is not None

    def store_binary(self, local_path: Path, remote_path: str) -> None:
        with local_path.open("rb") as handle:
            self.ftp.storbinary(f"STOR {remote_path}", handle)

    def rename(self, source: str, destination: str) -> None:
        self.ftp.rename(source, destination)

    def delete(self, path: str) -> None:
        self.ftp.delete(path)

    def list_names(self, path: str) -> list[str]:
        try:
            return self.ftp.nlst(path)
        except Exception:
            return []

    def read_binary(self, path: str) -> bytes:
        chunks: list[bytes] = []
        self.ftp.retrbinary(f"RETR {path}", chunks.append)
        return b"".join(chunks)

    def list_directory(self, path: str) -> list[dict]:
        try:
            items = []
            for name, facts in self.ftp.mlsd(path):
                if name in {".", ".."}:
                    continue
                item_type = "dir" if facts.get("type") == "dir" else "file"
                size = None
                if facts.get("size") is not None:
                    try:
                        size = int(facts["size"])
                    except (TypeError, ValueError):
                        size = None
                modified = None
                raw_modified = facts.get("modify")
                if raw_modified:
                    try:
                        modified = datetime.strptime(raw_modified, "%Y%m%d%H%M%S").isoformat()
                    except ValueError:
                        modified = None
                items.append(
                    {
                        "name": name,
                        "path": posixpath.join(path.rstrip("/") or "/", name),
                        "type": item_type,
                        "size": size,
                        "modified": modified,
                    }
                )
            return items
        except Exception:
            fallback = []
            for raw_name in self.list_names(path):
                full_path = raw_name if str(raw_name).startswith("/") else posixpath.join(path.rstrip("/") or "/", str(raw_name))
                name = posixpath.basename(full_path.rstrip("/")) or full_path
                size = self.file_size(full_path)
                fallback.append(
                    {
                        "name": name,
                        "path": full_path,
                        "type": "file" if size is not None else "dir",
                        "size": size,
                        "modified": None,
                    }
                )
            return fallback


class FTPService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.settings = get_settings()

    @staticmethod
    def _normalize_remote_path(path: str) -> str:
        value = (path or "/").strip().replace("\\", "/")
        if not value.startswith("/"):
            value = f"/{value}"
        parts: list[str] = []
        for part in value.split("/"):
            if not part or part == ".":
                continue
            if part == "..":
                if parts:
                    parts.pop()
                continue
            parts.append(part)
        return "/" + "/".join(parts) if parts else "/"

    @classmethod
    def _is_subpath(cls, path: str, base: str) -> bool:
        normalized_path = cls._normalize_remote_path(path)
        normalized_base = cls._normalize_remote_path(base)
        if normalized_base == "/":
            return True
        return normalized_path == normalized_base or normalized_path.startswith(f"{normalized_base}/")

    def _assert_writable(self, remote_path: str) -> None:
        normalized = self._normalize_remote_path(remote_path)
        for protected in self.settings.ftp_read_only_path_list:
            if self._is_subpath(normalized, protected):
                raise RuntimeError(
                    f"Write blocked for protected FTP path '{protected}'. Requested path: '{normalized}'."
                )

    @contextmanager
    def connect(self):
        if not self.settings.ftp_host:
            raise RuntimeError("FTP host is not configured.")
        ftp = ftplib.FTP()
        ftp.connect(self.settings.ftp_host, self.settings.ftp_port, timeout=self.settings.ftp_timeout_seconds)
        ftp.login(self.settings.ftp_user or "", self.settings.ftp_password or "")
        ftp.set_pasv(self.settings.ftp_passive_mode)
        try:
            yield FTPClientAdapter(ftp)
        finally:
            try:
                ftp.quit()
            except Exception:
                ftp.close()

    def _retry(self, fn):
        attempts = self.settings.ftp_max_retries
        base_delay = self.settings.ftp_retry_base_delay_seconds
        last_error = None
        for idx in range(attempts):
            try:
                return fn()
            except Exception as exc:
                last_error = exc
                if idx == attempts - 1:
                    break
                delay = base_delay * (2**idx)
                logger.warning("FTP operation failed (%s). Retry in %.2fs", exc, delay)
                time.sleep(delay)
        raise RuntimeError(f"FTP operation failed after {attempts} attempts: {last_error}") from last_error

    def _build_destination(
        self,
        run_id: str,
        camera: str,
        variable: str,
        date: str,
        filename: str,
        stable_mode: bool = False,
    ) -> tuple[str, str]:
        if stable_mode:
            base_dir = posixpath.join(self.settings.ftp_base_path, camera, variable, date)
        else:
            base_dir = posixpath.join(self.settings.ftp_base_path, run_id, camera, variable, date)
        return base_dir, posixpath.join(base_dir, filename)

    def _build_conflict_destination(self, run_id: str, camera: str, variable: str, date: str, filename: str) -> tuple[str, str]:
        conflict_dir = posixpath.join(self.settings.ftp_conflict_base_path, run_id, camera, variable, date)
        return conflict_dir, posixpath.join(conflict_dir, filename)

    def _record_uploaded_file(
        self,
        camera: str,
        variable: str,
        date: str,
        filename: str,
        remote_path: str,
        size: int,
        checksum: str | None = None,
    ) -> None:
        row, _ = get_or_create_file_record(
            self.db,
            source=FileSource.ftp,
            camera=camera,
            variable=variable,
            date=date,
            filename=filename,
        )
        row.file_size = size
        row.ftp_path = remote_path
        if checksum:
            row.checksum = checksum
        row.uploaded = True
        row.uploaded_at = utc_now()
        row.seen_at = utc_now()

        inv = self.db.scalar(
            select(FTPDateInventory).where(
                and_(
                    FTPDateInventory.camera == camera,
                    FTPDateInventory.variable == variable,
                    FTPDateInventory.date == date,
                )
            )
        )
        if inv is None:
            inv = FTPDateInventory(camera=camera, variable=variable, date=date)
            self.db.add(inv)
        inv.scanned_at = utc_now()
        inv_counts = self.db.execute(
            select(
                func.count(FileRecord.id),
                func.coalesce(func.sum(FileRecord.file_size), 0),
            ).where(
                and_(
                    FileRecord.source == FileSource.ftp,
                    FileRecord.camera == camera,
                    FileRecord.variable == variable,
                    FileRecord.date == date,
                )
            )
        ).one()
        inv.file_count = int(inv_counts[0] or 0)
        inv.total_size = int(inv_counts[1] or 0)

    def list_inventory(self, camera: str | None = None, variable: str | None = None) -> list[FTPDateInventory]:
        stmt = select(FTPDateInventory).order_by(FTPDateInventory.date.desc())
        if camera:
            stmt = stmt.where(FTPDateInventory.camera == camera)
        if variable:
            stmt = stmt.where(FTPDateInventory.variable == variable)
        return self.db.scalars(stmt).all()

    def upload_additive(
        self,
        client: FTPClientAdapter,
        run_id: str,
        camera: str,
        variable: str,
        date: str,
        filename: str,
        local_path: Path,
        dry_run: bool = False,
        stable_mode: bool = False,
        verify_checksum: bool = False,
        job_id: str | None = None,
    ) -> UploadOutcome:
        local_size = local_path.stat().st_size
        local_checksum = sha256_file(local_path) if verify_checksum else None
        base_dir, final_path = self._build_destination(
            run_id=run_id,
            camera=camera,
            variable=variable,
            date=date,
            filename=filename,
            stable_mode=stable_mode,
        )
        conflict_dir, conflict_path = self._build_conflict_destination(run_id, camera, variable, date, filename)
        self._assert_writable(base_dir)
        self._assert_writable(final_path)
        self._assert_writable(conflict_dir)
        self._assert_writable(conflict_path)

        existing_size = client.file_size(final_path)
        if existing_size is not None:
            if existing_size == local_size and not verify_checksum:
                self.db.add(
                    FileAuditEvent(
                        camera=camera,
                        variable=variable,
                        date=date,
                        filename=filename,
                        action="ftp_skip_exists",
                        reason="size_match",
                        source=FileSource.ftp,
                        job_id=job_id,
                        details_json={"remote_path": final_path, "size": local_size},
                    )
                )
                self.db.commit()
                return UploadOutcome(action="skip", remote_path=final_path, reason="already_uploaded")
            if existing_size == local_size and verify_checksum:
                remote_checksum = sha256_bytes(client.read_binary(final_path))
                if local_checksum == remote_checksum:
                    self.db.add(
                        FileAuditEvent(
                            camera=camera,
                            variable=variable,
                            date=date,
                            filename=filename,
                            action="ftp_skip_exists",
                            reason="checksum_match",
                            source=FileSource.ftp,
                            job_id=job_id,
                            details_json={
                                "remote_path": final_path,
                                "size": local_size,
                                "checksum": local_checksum,
                            },
                        )
                    )
                    self.db.commit()
                    return UploadOutcome(action="skip", remote_path=final_path, reason="already_uploaded")
            base_dir, final_path = conflict_dir, conflict_path

        if dry_run:
            return UploadOutcome(action="plan_upload", remote_path=final_path, reason="dry_run")

        client.makedirs(base_dir)
        temp_name = f".{filename}.part-{uuid.uuid4().hex[:8]}"
        temp_path = posixpath.join(base_dir, temp_name)
        self._assert_writable(temp_path)

        def _do_upload():
            client.store_binary(local_path, temp_path)
            if client.file_exists(final_path):
                existing = client.file_size(final_path)
                if existing == local_size:
                    if verify_checksum:
                        remote_checksum = sha256_bytes(client.read_binary(final_path))
                        if remote_checksum == local_checksum:
                            try:
                                client.delete(temp_path)
                            except Exception:
                                pass
                            return "skip_race"
                    else:
                        try:
                            client.delete(temp_path)
                        except Exception:
                            pass
                        return "skip_race"
                alt_dir, _ = self._build_conflict_destination(run_id, camera, variable, date, filename)
                alt_path = posixpath.join(alt_dir, f"{filename}.{uuid.uuid4().hex[:8]}")
                self._assert_writable(alt_dir)
                self._assert_writable(alt_path)
                client.makedirs(alt_dir)
                client.rename(temp_path, alt_path)
                return alt_path
            client.rename(temp_path, final_path)
            return final_path

        result_path = self._retry(_do_upload)
        if result_path == "skip_race":
            self.db.add(
                FileAuditEvent(
                    camera=camera,
                    variable=variable,
                    date=date,
                    filename=filename,
                    action="ftp_skip_exists",
                    reason="race_size_match",
                    source=FileSource.ftp,
                    job_id=job_id,
                    details_json={"remote_path": final_path, "size": local_size},
                )
            )
            self.db.commit()
            return UploadOutcome(action="skip", remote_path=final_path, reason="already_uploaded")

        remote_path = str(result_path)
        if verify_checksum:
            remote_checksum = sha256_bytes(client.read_binary(remote_path))
            if remote_checksum != local_checksum:
                raise RuntimeError(
                    f"Checksum mismatch after upload for {filename}: local={local_checksum}, remote={remote_checksum}"
                )
        self._record_uploaded_file(
            camera,
            variable,
            date,
            filename,
            remote_path,
            local_size,
            checksum=local_checksum,
        )
        self.db.add(
            FileAuditEvent(
                camera=camera,
                variable=variable,
                date=date,
                filename=filename,
                action="uploaded_ftp",
                source=FileSource.ftp,
                job_id=job_id,
                details_json={
                    "remote_path": remote_path,
                    "size": local_size,
                    "checksum": local_checksum,
                },
            )
        )
        self.db.commit()
        return UploadOutcome(action="uploaded", remote_path=remote_path)
