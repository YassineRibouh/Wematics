from __future__ import annotations

from app.services.csv_analysis import analyze_csv_for_time_plot


def test_csv_analysis_detects_time_and_numeric_columns(tmp_path):
    csv_file = tmp_path / "sample.csv"
    csv_file.write_text(
        "timestamp,ghi,temp_c,status\n"
        "2026-02-14T10:00:00,120.5,3.2,ok\n"
        "2026-02-14T10:15:00,130.1,3.4,ok\n"
        "2026-02-14T10:30:00,140.9,3.5,ok\n",
        encoding="utf-8",
    )

    result = analyze_csv_for_time_plot(csv_file)

    assert result["rows_scanned"] == 3
    assert result["suggested_time_column"] == "timestamp"
    assert result["suggested_value_column"] in {"ghi", "temp_c"}
    assert any(item["name"] == "timestamp" for item in result["time_columns"])
    assert any(item["name"] == "ghi" for item in result["numeric_columns"])
    assert result["plot"] is not None
    assert len(result["plot"]["points"]) == 3


def test_csv_analysis_returns_issue_when_no_plot_columns(tmp_path):
    csv_file = tmp_path / "text_only.csv"
    csv_file.write_text(
        "a,b,c\n"
        "x,y,z\n"
        "m,n,o\n",
        encoding="utf-8",
    )

    result = analyze_csv_for_time_plot(csv_file)

    assert result["plot"] is None
    assert result["time_columns"] == []
    assert result["numeric_columns"] == []
    assert any("No timestamp-like column" in msg for msg in result["issues"])
