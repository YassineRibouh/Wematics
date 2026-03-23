from __future__ import annotations

from app.services.timestamps import parse_filename_timestamp


def test_parse_iso_offset_with_hyphen_time():
    value = parse_filename_timestamp("2026-02-10T07-25-15+01-00_rgb.webp")
    assert value is not None
    assert value.tzinfo is not None
    assert value.strftime("%Y-%m-%dT%H:%M:%S") == "2026-02-10T06:25:15"


def test_parse_underscore_datetime():
    value = parse_filename_timestamp("2026-02-10_07_25_15.csv")
    assert value is not None
    assert value.strftime("%Y-%m-%d %H:%M:%S") == "2026-02-10 07:25:15"


def test_parse_iso_with_colons():
    value = parse_filename_timestamp("2026-02-10T13:00:00_some.csv")
    assert value is not None
    assert value.strftime("%Y-%m-%d %H:%M:%S") == "2026-02-10 13:00:00"


def test_parse_iso_with_fractional_seconds():
    value = parse_filename_timestamp("2026-02-10T13:00:00.123_ghi.csv")
    assert value is not None
    assert value.strftime("%Y-%m-%d %H:%M:%S") == "2026-02-10 13:00:00"

