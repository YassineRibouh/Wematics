from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

from sqlalchemy import select

from app.models import FileRecord, FileSource, Job, JobKind
from app.services.ftp_service import FTPService
from app.services.wematics_service import WematicsService
from app.workers.engine import DownloadTask, JobEngine


class FakeFTPClient:
    def __init__(self) -> None:
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


def test_prepare_transfer_tasks_uses_temp_workspace(db_session, tmp_path, monkeypatch) -> None:
    engine = JobEngine()
    engine.settings.transfer_temp_base_path = str(tmp_path / "transfer-temp")
    job = Job(id="job-temp", kind=JobKind.transfer, params_json={})
    params = {
        "camera": "ROS",
        "variable": "RGB",
        "timezone": "local",
        "mode": "single_date",
        "date": "2026-02-10",
        "file_selection": "all",
    }

    monkeypatch.setattr(WematicsService, "list_dates", lambda self, camera, variable, timezone="local": ["2026-02-10"])
    monkeypatch.setattr(WematicsService, "list_files", lambda self, camera, variable, date, timezone="local": ["sample.webp"])

    tasks, requested_dates, missing_dates = engine._prepare_transfer_tasks(db_session, job, params)

    assert requested_dates == ["2026-02-10"]
    assert missing_dates == []
    assert len(tasks) == 1
    assert str(tasks[0].local_path).startswith(str((tmp_path / "transfer-temp").resolve()))
    assert "\\downloads\\" not in str(tasks[0].local_path)


def test_transfer_chunk_uploads_to_ftp_and_removes_temp_file(db_session, tmp_path, monkeypatch) -> None:
    engine = JobEngine()
    fake_client = FakeFTPClient()
    job_temp_root = tmp_path / "transfer-job"
    task = DownloadTask(
        camera="ROS",
        variable="RGB",
        date="2026-02-10",
        filename="sample.webp",
        timezone="local",
        target_dir=str(job_temp_root / "ROS" / "RGB" / "2026-02-10"),
        local_path=job_temp_root / "ROS" / "RGB" / "2026-02-10" / "sample.webp",
    )

    @contextmanager
    def fake_db_context():
        try:
            yield db_session
            db_session.commit()
        except Exception:
            db_session.rollback()
            raise

    @contextmanager
    def fake_connect(self):
        yield fake_client

    def fake_download(self, camera: str, variable: str, filename: str, target_dir: str, timezone: str = "local") -> None:
        path = Path(target_dir)
        path.mkdir(parents=True, exist_ok=True)
        (path / filename).write_bytes(b"test-image")

    monkeypatch.setattr("app.workers.engine.db_context", fake_db_context)
    monkeypatch.setattr(FTPService, "connect", fake_connect)
    monkeypatch.setattr(WematicsService, "download_file", fake_download)

    result = engine._transfer_chunk(
        [task],
        run_id="job-transfer",
        stable_mode=False,
        verify_checksum=False,
        dry_run=False,
        job_id="job-transfer",
        job_temp_root=job_temp_root,
    )

    uploaded_rows = db_session.scalars(select(FileRecord).where(FileRecord.source == FileSource.ftp)).all()
    local_rows = db_session.scalars(select(FileRecord).where(FileRecord.source == FileSource.local)).all()

    assert result == {"downloaded": 1, "uploaded": 1, "skipped": 0, "planned": 0, "errors": 0}
    assert not task.local_path.exists()
    assert uploaded_rows[0].ftp_path == "/wematics/job-transfer/ROS/RGB/2026-02-10/sample.webp"
    assert local_rows == []


def test_cleanup_transfer_temp_file_keeps_shared_directory(tmp_path) -> None:
    engine = JobEngine()
    shared_dir = tmp_path / "transfer-job" / "ROS" / "RGB" / "2026-02-10"
    shared_dir.mkdir(parents=True, exist_ok=True)
    file_path = shared_dir / "sample.webp"
    sibling = shared_dir / "sibling.webp"
    file_path.write_bytes(b"one")
    sibling.write_bytes(b"two")

    engine._cleanup_transfer_temp_file(file_path)

    assert not file_path.exists()
    assert sibling.exists()
    assert shared_dir.exists()
