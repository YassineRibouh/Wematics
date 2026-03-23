from __future__ import annotations

from contextlib import contextmanager

from app.models import FileRecord, FileSource, Job, JobKind, JobStatus
from app.workers.engine import JobEngine


def test_run_job_recovers_after_integrity_error(db_session, monkeypatch) -> None:
    job_row = Job(kind=JobKind.download, status=JobStatus.running, params_json={})
    db_session.add(job_row)
    db_session.flush()

    db_session.add(
        FileRecord(
            source=FileSource.local,
            camera="ROS",
            variable="RGB",
            date="2025-10-30",
            filename="2025-10-30T10-41-45+01-00_rgb.webp",
        )
    )
    db_session.commit()
    db_session.expunge(job_row)

    @contextmanager
    def fake_db_context():
        try:
            yield db_session
            db_session.commit()
        except Exception:
            db_session.rollback()
            raise

    monkeypatch.setattr("app.workers.engine.db_context", fake_db_context)

    def duplicate_insert(self, db, job, params):
        db.add(
            FileRecord(
                source=FileSource.local,
                camera="ROS",
                variable="RGB",
                date="2025-10-30",
                filename="2025-10-30T10-41-45+01-00_rgb.webp",
            )
        )
        db.flush()

    monkeypatch.setattr(JobEngine, "_execute_download", duplicate_insert)

    engine = JobEngine()
    engine._run_job(job_row)

    persisted = db_session.get(Job, job_row.id)
    assert persisted is not None
    assert persisted.retry_count == 1
    assert persisted.status == JobStatus.queued
