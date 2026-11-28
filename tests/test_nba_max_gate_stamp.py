import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ui_runner"))
import app as ui_app  # noqa: E402


def _row(**kw):
    base = {"sport": "NBA", "game_date": "2026-11-28", "player": "Franz Wagner", "prop": "Rebounds",
            "pick_type": "Goblin", "dir": "OVER", "line": 4.5}
    base.update(kw)
    return base


def test_stamps_only_matching_nba_rows_on_slate(monkeypatch):
    idx = {ui_app._max_gate_key("Franz Wagner", "Rebounds", "Goblin", "OVER", 4.5): {"hit": 78.9, "n": 337, "gate": "L10 8+"}}
    monkeypatch.setattr(ui_app, "_nba_max_gate_index", lambda: ("2026-11-28", idx))
    rows = [
        _row(),
        _row(line=5.5),
        _row(game_date="2026-11-27"),
        _row(sport="NBA1Q"),
        _row(pick_type="Demon"),
    ]
    ui_app._stamp_nba_max_gates(rows)
    assert rows[0]["max_gate"]["hit"] == 78.9
    assert all("max_gate" not in r for r in rows[1:])


def test_stamps_other_sports_by_sport_date_and_prop_label(monkeypatch):
    k = "NHL|2026-11-27|" + ui_app._max_gate_key("Connor Brown", ui_app._max_gate_prop_key("Shots On Goal"), "Goblin", "OVER", 0.5)
    monkeypatch.setattr(ui_app, "_nba_max_gate_index", lambda: ("", {}))
    monkeypatch.setattr(ui_app, "_sports_max_gate_index", lambda: {k: {"hit": 78.8, "n": 104, "gate": "L5 5/5"}})
    base = dict(sport="NHL", game_date="2026-11-27", player="Connor Brown", prop="shots_on_goal",
                pick_type="Goblin", dir="OVER", line=0.5)
    rows = [dict(base), dict(base, sport="NHL1P"), dict(base, game_date="2026-11-28"), dict(base, dir="UNDER")]
    ui_app._stamp_max_gates(rows)
    assert rows[0]["max_gate"]["hit"] == 78.8
    assert all("max_gate" not in r for r in rows[1:])


def test_prop_key_matches_board_and_slate_labels():
    assert ui_app._max_gate_prop_key("Pass+Rush Yds") == ui_app._max_gate_prop_key("pass_rush_yds")


def test_key_normalizes_case_and_line():
    assert ui_app._max_gate_key("Franz Wagner", "Rebounds", "Goblin", "OVER", 4.5) == \
        ui_app._max_gate_key("franz wagner", "rebounds", "goblin", "over", "4.50")
