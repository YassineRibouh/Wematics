from __future__ import annotations

from app.models import Job, JobKind, JobStatus
from app.services.wematics_service import WematicsService
from app.workers.engine import JobEngine


def test_cancel_queued_job_marks_it_cancelled(db_session) -> None:
    engine = JobEngine()
    row = Job(kind=JobKind.transfer, status=JobStatus.queued, params_json={})
    db_session.add(row)
    db_session.commit()

    updated = engine.cancel_job(db_session, row.id)
    db_session.commit()

    assert updated.status == JobStatus.cancelled
    assert updated.progress_json["cancel_requested"] is True
    assert updated.progress_json["stage"] == "cancelled"


def test_cancel_running_job_sets_runtime_stop_flag(db_session, monkeypatch) -> None:
    engine = JobEngine()
    row = Job(kind=JobKind.transfer, status=JobStatus.running, params_json={})
    db_session.add(row)
    db_session.commit()

    monkeypatch.setattr(engine, "_flush_runtime_progress", lambda job_id: None)
    tracker = engine._create_progress_tracker(row.id, stage="transfer", queued_files=12)

    updated = engine.cancel_job(db_session, row.id)

    assert updated.status == JobStatus.running
    assert tracker.snapshot()["cancel_requested"] is True
    assert engine._get_cancel_event(row.id).is_set() is True
    engine._release_runtime(row.id)


def test_prepare_transfer_tasks_filters_resume_failures(db_session, monkeypatch, tmp_path) -> None:
    engine = JobEngine()
    engine.settings.transfer_temp_base_path = str(tmp_path / "transfer-temp")
    job = Job(id="resume-job", kind=JobKind.transfer, params_json={})
    params = {
        "camera": "ROS",
        "variable": "RGB",
        "timezone": "local",
        "mode": "single_date",
        "date": "2026-02-10",
        "file_selection": "all",
        "resume_failure_files": [{"date": "2026-02-10", "filename": "retry.webp"}],
    }

    monkeypatch.setattr(WematicsService, "list_dates", lambda self, camera, variable, timezone="local": ["2026-02-10"])
    monkeypatch.setattr(
        WematicsService,
        "list_files",
        lambda self, camera, variable, date, timezone="local": ["skip.webp", "retry.webp"],
    )

    tasks, _, _ = engine._prepare_transfer_tasks(db_session, job, params)

    assert [task.filename for task in tasks] == ["retry.webp"]
