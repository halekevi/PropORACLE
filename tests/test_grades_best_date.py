"""Grades hub should land on a date that actually has ticket_eval HTML."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture
def grades_dirs(tmp_path, monkeypatch):
    from ui_runner import app as m

    templates = tmp_path / "templates"
    archive = templates / "archive"
    templates.mkdir()
    archive.mkdir()
    monkeypatch.setattr(m, "TEMPLATES_DIR", templates)
    monkeypatch.setattr(m, "ARCHIVE_DIR", archive)
    monkeypatch.setattr(m, "_GRADES_HTML_RAW_BASE", "")
    return templates, archive, m


def _cards_html() -> str:
    return '<article class="ticket-card">leg</article>' + ("y" * 6000)


def test_best_grades_date_skips_tiny_and_missing(grades_dirs):
    templates, _archive, m = grades_dirs
    (templates / "ticket_eval_2026-09-23.html").write_text("tiny", encoding="utf-8")
    (templates / "ticket_eval_2026-09-22.html").write_text("x" * 6000, encoding="utf-8")
    (templates / "ticket_eval_2026-09-17.html").write_text(_cards_html(), encoding="utf-8")
    assert m._best_grades_date() == "2026-09-17"


def test_best_grades_date_skips_empty_shell(grades_dirs):
    templates, _archive, m = grades_dirs
    (templates / "ticket_eval_2026-09-24.html").write_text("z" * 58000, encoding="utf-8")
    (templates / "ticket_eval_2026-09-17.html").write_text(_cards_html(), encoding="utf-8")
    assert m._best_grades_date() == "2026-09-17"


def test_best_grades_date_uses_archive(grades_dirs):
    templates, archive, m = grades_dirs
    (archive / "ticket_eval_2026-09-16.html").write_text(_cards_html(), encoding="utf-8")
    (templates / "ticket_eval_2026-09-23.html").write_bytes(b"nope")
    assert m._best_grades_date() == "2026-09-16"


def test_latest_with_tickets_api(grades_dirs):
    templates, _archive, m = grades_dirs
    (templates / "ticket_eval_2026-09-18.html").write_text(_cards_html(), encoding="utf-8")
    client = m.app.test_client()
    res = client.get("/api/grades/latest-with-tickets")
    assert res.status_code == 200
    body = res.get_json()
    assert body["ok"] is True
    assert body["date"] == "2026-09-18"
    rd = client.get("/api/grades/report_dates")
    assert rd.status_code == 200
    assert rd.get_json()["latest_with_tickets"] == "2026-09-18"
