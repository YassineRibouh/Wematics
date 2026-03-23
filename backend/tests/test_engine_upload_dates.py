from __future__ import annotations

from app.models import LocalDateInventory
from app.workers.engine import JobEngine


def test_resolve_upload_dates_uses_scalar_inventory_values(db_session) -> None:
    db_session.add_all(
        [
            LocalDateInventory(camera="ROS", variable="RGB", date="2026-02-10", file_count=3, total_size=123),
            LocalDateInventory(camera="ROS", variable="RGB", date="2026-02-12", file_count=2, total_size=456),
        ]
    )
    db_session.flush()

    engine = JobEngine.__new__(JobEngine)
    params = {"camera": "ROS", "variable": "RGB", "mode": "latest_only"}

    dates = engine._resolve_upload_dates(db_session, params)

    assert dates == ["2026-02-12"]
