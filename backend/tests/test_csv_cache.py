from __future__ import annotations

from sqlalchemy import func, select

from app.models import CsvAnalysisCache
from app.services.csv_analysis import analyze_csv_for_time_plot_cached


def test_csv_analysis_cached_result_reused(db_session, tmp_path):
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text(
        "timestamp,ghi\n"
        "2026-02-14T10:00:00,120.5\n"
        "2026-02-14T10:15:00,130.1\n",
        encoding="utf-8",
    )

    first = analyze_csv_for_time_plot_cached(db_session, csv_file, max_rows=3000)
    second = analyze_csv_for_time_plot_cached(db_session, csv_file, max_rows=3000)

    count = int(db_session.scalar(select(func.count(CsvAnalysisCache.id))) or 0)
    assert count == 1
    assert first["rows_scanned"] == second["rows_scanned"]
    assert first["plot"]["points"] == second["plot"]["points"]
