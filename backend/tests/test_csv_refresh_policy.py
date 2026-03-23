from __future__ import annotations

from datetime import timedelta

from app.core.time import utc_now
from app.services.job_utils import should_refresh_csv


def test_csv_policy_always_refresh(tmp_path):
    path = tmp_path / "measurements.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    assert should_refresh_csv("always_refresh", path, "measurements.csv") is True


def test_csv_policy_never_refresh(tmp_path):
    path = tmp_path / "measurements.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    assert should_refresh_csv("never_refresh", path, "measurements.csv") is False


def test_csv_policy_scheduled_refresh(tmp_path):
    path = tmp_path / "measurements.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    old_now = utc_now() + timedelta(hours=25)
    assert should_refresh_csv("scheduled_refresh", path, "measurements.csv", now=old_now, schedule_hours=24) is True


def test_csv_policy_remote_newer_fallback_by_age(tmp_path):
    path = tmp_path / "measurements.csv"
    path.write_text("a,b\n1,2\n", encoding="utf-8")
    old_now = utc_now() + timedelta(hours=25)
    # Filename without timestamp uses fallback age policy.
    assert should_refresh_csv("remote_newer", path, "measurements.csv", now=old_now) is True
