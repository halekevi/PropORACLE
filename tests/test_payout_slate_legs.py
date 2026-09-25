"""Payout Load-from-slate options include EV and ticket groups."""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture
def slate_client(tmp_path, monkeypatch):
    from ui_runner import app as m

    templates = tmp_path / "templates"
    templates.mkdir()
    payload = {
        "date": "2026-09-24",
        "generated_at": "2026-09-24 12:00:00 UTC",
        "groups": [
            {
                "group_name": "NBA Power Play 2-Leg #1",
                "tickets": [
                    {
                        "ticket_no": 1,
                        "ev_power": 2.97,
                        "legs": [
                            {
                                "player": "Example Player",
                                "sport": "NBA",
                                "prop_type": "Points",
                                "pick_type": "Standard",
                                "direction": "OVER",
                                "line": 20.5,
                                "standard_line": 20.5,
                            }
                        ],
                    }
                ],
            }
        ],
    }
    (templates / "tickets_latest.json").write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(m, "TEMPLATES_DIR", templates)
    monkeypatch.setattr(m, "_template_json_available", lambda name: (templates / name).is_file())
    monkeypatch.setattr(m, "read_json_cached", lambda path: json.loads(Path(path).read_text(encoding="utf-8")))
    return m.app.test_client()


def test_slate_legs_includes_ev_and_group(slate_client):
    res = slate_client.get("/api/slate-legs")
    assert res.status_code == 200
    body = res.get_json()
    tickets = body.get("tickets") or []
    assert len(tickets) == 1
    t = tickets[0]
    assert t["group_name"] == "NBA Power Play 2-Leg #1"
    assert t["ticket_no"] == 1
    assert t["ev"] == pytest.approx(2.97)
    assert t["legs"][0]["player"] == "Example Player"


def test_payout_page_has_one_subnav_and_no_tab_bar():
    html = Path("ui_runner/templates/payout_calculator.html").read_text(encoding="utf-8")
    assert "{% include '_payout_subnav.html' %}" in html
    assert 'class="tab-bar"' not in html
    assert "Tyrese Haliburton" not in html
    init = html.split("/* ─── INIT")[-1]
    assert "addCalcLeg('standard')" not in init
    assert "addCalcLeg('goblin')" not in init
    assert "loadSlateTicketOptions()" in init.split("try{")[0]
    assert "if(a) a.textContent=n;" in html
    log = Path("ui_runner/templates/payout_log.html").read_text(encoding="utf-8")
    assert 'class="tab-bar"' not in log
    sub = Path("ui_runner/templates/_payout_subnav.html").read_text(encoding="utf-8")
    assert sub.count("<nav class=\"payout-subnav\"") == 1
    assert 'href="/payout/log"' in sub
    assert 'href="/payout?tab=cards"' in sub
