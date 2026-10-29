"""CBB quote probe: one mix per slate from 11/1, Demons never, skip-if-ok."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

import collect_payout_data as cpd  # noqa: E402
from utils.cbb_season import CBB_QUOTE_RESUME, cbb_quote_scrape_enabled  # noqa: E402

_PS1 = ROOT / "scripts" / "run_live_payout_capture.ps1"


def test_cbb_quote_resume_matches_opener():
    assert CBB_QUOTE_RESUME == "2026-11-01"
    assert cbb_quote_scrape_enabled("2026-10-31") is False
    assert cbb_quote_scrape_enabled("2026-11-01") is True
    assert cbb_quote_scrape_enabled("2026-11-02") is True


def test_plan_prefers_two_goblin_points_rebounds():
    cards = [
        {"player": "A", "prop_type": "Assists", "pick_type": "Goblin", "line": 1.5},
        {"player": "B", "prop_type": "Points", "pick_type": "Goblin", "line": 8.5},
        {"player": "C", "prop_type": "Rebounds", "pick_type": "Goblin", "line": 3.5},
        {"player": "D", "prop_type": "Points", "pick_type": "Standard", "line": 14.5},
        {"player": "E", "prop_type": "Points", "pick_type": "Demon", "line": 22.5},
    ]
    plan = cpd.plan_cbb_probe_mix(cards)
    assert plan is not None
    assert plan["type"] == "2G"
    assert plan["n_goblin"] == 2
    assert plan["n_standard"] == 0
    props = [item["card"]["prop_type"] for item in plan["cards"]]
    assert props == ["Points", "Rebounds"]
    players = {item["card"]["player"] for item in plan["cards"]}
    assert players == {"B", "C"}
    assert all(item["direction"] == "OVER" for item in plan["cards"])


def test_plan_drops_demons_and_falls_back_to_standard():
    cards = [
        {"player": "A", "prop_type": "Points", "pick_type": "Demon", "line": 22.5},
        {"player": "B", "prop_type": "Points", "pick_type": "Standard", "line": 12.5},
        {"player": "C", "prop_type": "Rebounds", "pick_type": "Standard", "line": 5.5},
    ]
    plan = cpd.plan_cbb_probe_mix(cards)
    assert plan is not None
    assert plan["type"] == "2S"
    assert plan["n_standard"] == 2
    players = {item["card"]["player"] for item in plan["cards"]}
    assert players == {"B", "C"}


def test_plan_none_when_board_too_thin():
    assert cpd.plan_cbb_probe_mix([]) is None
    assert (
        cpd.plan_cbb_probe_mix(
            [{"player": "A", "prop_type": "Points", "pick_type": "Goblin", "line": 8.5}]
        )
        is None
    )


def test_cbb_probe_already_ok_from_report(tmp_path, monkeypatch):
    report = tmp_path / "payout_cbb_probe_2026-11-01.json"
    report.write_text(
        json.dumps({"summary": {"n_ok": 1}, "slips": [{"status": "ok", "power_min_x": 2.2}]}),
        encoding="utf-8",
    )
    monkeypatch.setattr(cpd, "PAYOUT_LADDER_LIVE_CDP_PATH", tmp_path / "missing.json")
    assert cpd.cbb_probe_already_ok("2026-11-01", report) is True
    empty = tmp_path / "empty.json"
    empty.write_text(json.dumps({"summary": {"n_ok": 0}, "slips": []}), encoding="utf-8")
    assert cpd.cbb_probe_already_ok("2026-11-01", empty) is False


def test_daily_payout_script_calls_cbb_probe():
    text = _PS1.read_text(encoding="utf-8")
    assert "--cbb-probe" in text
    assert "Invoke-CbbProbeCapture" in text
    assert "2026-11-01" in text
