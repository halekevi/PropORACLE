from ui_runner import app as app_mod


def test_filter_grade_report_dates_drops_missing_html(tmp_path, monkeypatch):
    templates = tmp_path / "templates"
    archive = templates / "archive"
    templates.mkdir()
    archive.mkdir()
    (templates / "ticket_eval_2026-09-15.html").write_text("ok", encoding="utf-8")
    monkeypatch.setattr(app_mod, "TEMPLATES_DIR", templates)
    monkeypatch.setattr(app_mod, "ARCHIVE_DIR", archive)
    out = app_mod._filter_grade_report_dates_to_html(
        ["2026-09-15", "2026-09-16"], "ticket_eval_"
    )
    assert out == ["2026-09-15"]


def test_filter_grade_report_dates_keeps_archive_html(tmp_path, monkeypatch):
    templates = tmp_path / "templates"
    archive = templates / "archive"
    templates.mkdir()
    archive.mkdir()
    (archive / "ticket_eval_2026-09-14.html").write_text("ok", encoding="utf-8")
    monkeypatch.setattr(app_mod, "TEMPLATES_DIR", templates)
    monkeypatch.setattr(app_mod, "ARCHIVE_DIR", archive)
    out = app_mod._filter_grade_report_dates_to_html(
        ["2026-09-14"], "ticket_eval_"
    )
    assert out == ["2026-09-14"]
