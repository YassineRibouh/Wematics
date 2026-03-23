from __future__ import annotations

from contextlib import contextmanager

from sqlalchemy import func, select

from app.models import Job, JobEvent, JobKind, JobStatus
from app.workers.engine import JobEngine, _job_windows_overlap


def _patch_db_context(monkeypatch, db_session) -> None:
    @contextmanager
    def fake_db_context():
        try:
            yield db_session
            db_session.commit()
        except Exception:
            db_session.rollback()
            raise

    monkeypatch.setattr("app.workers.engine.db_context", fake_db_context)


def test_job_windows_overlap_supports_wildcards_and_disjoint_ranges() -> None:
    inventory_params = {"camera": "ROS", "variable": None}
    download_params = {
        "camera": "ROS",
        "variable": "RGB",
        "mode": "single_date",
        "date": "2025-10-30",
    }
    disjoint_params = {
        "camera": "ROS",
        "variable": "RGB",
        "mode": "single_date",
        "date": "2025-11-01",
    }

    assert _job_windows_overlap(inventory_params, download_params) is True
    assert _job_windows_overlap(download_params, disjoint_params) is False


def test_enqueue_reuses_active_job_when_window_overlaps(db_session, monkeypatch) -> None:
    _patch_db_context(monkeypatch, db_session)
    existing = Job(
        kind=JobKind.download,
        status=JobStatus.queued,
        params_json={
            "camera": "ROS",
            "variable": "RGB",
            "mode": "date_range",
            "date_from": "2025-10-29",
            "date_to": "2025-10-31",
        },
    )
    db_session.add(existing)
    db_session.commit()

    engine = JobEngine()
    result = engine.enqueue(
        kind=JobKind.transfer,
        params={
            "camera": "ROS",
            "variable": "RGB",
            "mode": "single_date",
            "date": "2025-10-30",
        },
    )

    assert result.id == existing.id
    assert db_session.scalar(select(func.count(Job.id))) == 1
    dedupe_events = db_session.scalars(
        select(JobEvent).where(JobEvent.job_id == existing.id, JobEvent.message.like("Skipped overlapping enqueue request%"))
    ).all()
    assert len(dedupe_events) == 1


def test_enqueue_allows_non_overlapping_window(db_session, monkeypatch) -> None:
    _patch_db_context(monkeypatch, db_session)
    existing = Job(
        kind=JobKind.download,
        status=JobStatus.running,
        params_json={
            "camera": "ROS",
            "variable": "RGB",
            "mode": "single_date",
            "date": "2025-10-30",
        },
    )
    db_session.add(existing)
    db_session.commit()

    engine = JobEngine()
    result = engine.enqueue(
        kind=JobKind.download,
        params={
            "camera": "ROS",
            "variable": "RGB",
            "mode": "single_date",
            "date": "2025-11-02",
        },
    )

    assert result.id != existing.id
    assert db_session.scalar(select(func.count(Job.id))) == 2
