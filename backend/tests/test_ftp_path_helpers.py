from __future__ import annotations

from app.api.routes import _normalized_ftp_path, _parent_ftp_path


def test_normalized_ftp_path_handles_relative_and_parent_segments():
    assert _normalized_ftp_path("incoming/../other/source") == "/other/source"
    assert _normalized_ftp_path("/a/b/../../c") == "/c"
    assert _normalized_ftp_path("\\alpha\\beta") == "/alpha/beta"


def test_parent_ftp_path_stays_at_root():
    assert _parent_ftp_path("/") == "/"
    assert _parent_ftp_path("/incoming") == "/"
    assert _parent_ftp_path("/incoming/wematics") == "/incoming"
