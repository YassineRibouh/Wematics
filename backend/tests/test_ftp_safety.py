from __future__ import annotations

from pathlib import Path

import pytest

from app.services.ftp_service import FTPService


class FakeFTPClient:
    def __init__(self):
        self.files: dict[str, bytes] = {}

    def makedirs(self, _path: str) -> None:
        return

    def file_size(self, path: str):
        blob = self.files.get(path)
        return None if blob is None else len(blob)

    def file_exists(self, path: str):
        return path in self.files

    def store_binary(self, local_path: Path, remote_path: str):
        self.files[remote_path] = local_path.read_bytes()

    def rename(self, source: str, destination: str):
        self.files[destination] = self.files.pop(source)

    def delete(self, path: str):
        if path in self.files:
            del self.files[path]

    def read_binary(self, path: str) -> bytes:
        return self.files.get(path, b"")


def test_ftp_skip_when_existing_size_matches(db_session, tmp_path):
    service = FTPService(db_session)
    fake = FakeFTPClient()

    local = tmp_path / "sample.webp"
    local.write_bytes(b"123456")
    final_path = f"{service.settings.ftp_base_path}/run1/ROS/RGB/2026-02-10/sample.webp"
    fake.files[final_path] = b"123456"

    outcome = service.upload_additive(
        client=fake,
        run_id="run1",
        camera="ROS",
        variable="RGB",
        date="2026-02-10",
        filename="sample.webp",
        local_path=local,
        dry_run=False,
        stable_mode=False,
        job_id=None,
    )

    assert outcome.action == "skip"
    assert outcome.remote_path == final_path


def test_ftp_conflict_path_when_existing_differs(db_session, tmp_path):
    service = FTPService(db_session)
    fake = FakeFTPClient()

    local = tmp_path / "sample.webp"
    local.write_bytes(b"local-content")
    final_path = f"{service.settings.ftp_base_path}/run2/ROS/RGB/2026-02-10/sample.webp"
    fake.files[final_path] = b"old-different-content"

    outcome = service.upload_additive(
        client=fake,
        run_id="run2",
        camera="ROS",
        variable="RGB",
        date="2026-02-10",
        filename="sample.webp",
        local_path=local,
        dry_run=False,
        stable_mode=False,
        job_id=None,
    )

    assert outcome.action == "uploaded"
    assert f"{service.settings.ftp_conflict_base_path}/run2/ROS/RGB/2026-02-10/" in (outcome.remote_path or "")
    assert fake.files[final_path] == b"old-different-content"


def test_ftp_write_blocked_for_protected_images_path(db_session, tmp_path):
    service = FTPService(db_session)
    fake = FakeFTPClient()
    original = (
        service.settings.ftp_base_path,
        service.settings.ftp_conflict_base_path,
        service.settings.ftp_read_only_paths,
    )
    try:
        service.settings.ftp_base_path = "/images"
        service.settings.ftp_conflict_base_path = "/images_conflicts"
        service.settings.ftp_read_only_paths = "/images"

        local = tmp_path / "sample.webp"
        local.write_bytes(b"local-content")

        with pytest.raises(RuntimeError, match="Write blocked"):
            service.upload_additive(
                client=fake,
                run_id="run3",
                camera="ROS",
                variable="RGB",
                date="2026-02-10",
                filename="sample.webp",
                local_path=local,
                dry_run=False,
                stable_mode=False,
                job_id=None,
            )
    finally:
        service.settings.ftp_base_path = original[0]
        service.settings.ftp_conflict_base_path = original[1]
        service.settings.ftp_read_only_paths = original[2]


def test_ftp_skip_when_checksum_matches(db_session, tmp_path):
    service = FTPService(db_session)
    fake = FakeFTPClient()

    local = tmp_path / "sample.webp"
    local.write_bytes(b"checksum-content")
    final_path = f"{service.settings.ftp_base_path}/run9/ROS/RGB/2026-02-10/sample.webp"
    fake.files[final_path] = b"checksum-content"

    outcome = service.upload_additive(
        client=fake,
        run_id="run9",
        camera="ROS",
        variable="RGB",
        date="2026-02-10",
        filename="sample.webp",
        local_path=local,
        dry_run=False,
        stable_mode=False,
        verify_checksum=True,
        job_id=None,
    )
    assert outcome.action == "skip"
    assert outcome.remote_path == final_path
