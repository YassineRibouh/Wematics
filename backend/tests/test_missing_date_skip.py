from __future__ import annotations

from app.workers.engine import _requested_dates


def test_missing_date_skip_behavior_for_range():
    available = ["2026-02-10", "2026-02-12"]
    params = {"mode": "date_range", "date_from": "2026-02-10", "date_to": "2026-02-12"}

    requested = _requested_dates("date_range", params, available)
    missing = [d for d in requested if d not in set(available)]

    assert requested == ["2026-02-10", "2026-02-11", "2026-02-12"]
    assert missing == ["2026-02-11"]

