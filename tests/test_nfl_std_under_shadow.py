"""NFL Standard UNDER shadow: pre-registered gate, two arms, grading, bar."""
from __future__ import annotations

import json
from pathlib import Path

from utils import nfl_std_under_shadow as S


def _row(player: str, prop: str, line: float, *, l10u=9, avg=None, pt="Standard", side="UNDER", team="DAL", opp="NYG"):
    return {
        "sport": "NFL",
        "league": "NFL",
        "player": player,
        "team": team,
        "opp_team": opp,
        "prop": prop,
        "line": line,
        "pick_type": pt,
        "side": side,
        "l10_under": l10u,
        "avg_l5": line - 2.0 if avg is None else avg,
        "start_time": "2026-10-25T13:00:00-04:00",
    }


def _step1(root: Path, date: str, mixed: bool = True) -> None:
    d = root / "outputs" / date / "nfl"
    d.mkdir(parents=True)
    rows = ["projection_id,pick_type,line", "1,Standard,6.5"]
    if mixed:
        rows.append("2,Goblin,0.5")
    (d / "step1_pp_props_today.csv").write_text("\n".join(rows) + "\n", encoding="utf-8")


def test_leg_gate_and_props():
    ok, why = S.leg_qualifies(_row("A", "tackles_ast", 6.5), face_verified=True)
    assert ok, why
    assert S.leg_qualifies(_row("A", "pass_completions", 20.5), face_verified=True) == (False, "prop_excluded")
    assert S.leg_qualifies(_row("A", "sacks", 0.5, avg=-0.6), face_verified=True) == (False, "prop_excluded")
    assert S.leg_qualifies(_row("A", "rushing_yards", 40.5), face_verified=True) == (False, "prop_not_listed")
    assert S.leg_qualifies(_row("A", "tackles_ast", 6.5, l10u=7), face_verified=True) == (False, "l10_under_lt_8")
    assert S.leg_qualifies(_row("A", "tackles_ast", 6.5, avg=6.0), face_verified=True) == (False, "cover_lt_1")
    assert S.leg_qualifies(_row("A", "tackles_ast", 6.5, pt="Goblin"), face_verified=True) == (False, "not_literal_standard")
    assert S.leg_qualifies(_row("A", "tackles_ast", 6.5, side="OVER"), face_verified=True) == (False, "not_under")
    assert S.leg_qualifies(_row("A", "tackles_ast", 6.5), face_verified=False)[1] == "face_unverified_step1_no_pick_type"


def test_rule4_arms_share_one_leg_log(tmp_path: Path):
    _step1(tmp_path, "2026-10-25")
    board = [
        _row("A", "tackles_ast", 4.5, avg=2.0),
        _row("B", "tackles_ast", 6.5, team="NYG", opp="DAL"),
        _row("C", "tackles_ast", 8.5, team="PHI", opp="WAS"),
        _row("D", "tackles_ast", 5.5, team="WAS", opp="PHI", avg=3.0),
    ]
    log = tmp_path / "track.jsonl"
    res = S.record_nfl_std_under_shadow(board, date="2026-10-25", root=tmp_path, path=log)
    assert res["face_verified_board"] is True
    assert res["n_legs"] == 4
    assert res["n_rule4_blocked"] == 1  # 4.5 line
    assert res["n_slips"]["rule4_waived"] == 2
    assert res["n_slips"]["rule4_applied"] == 1
    rows = [json.loads(l) for l in log.read_text().splitlines()]
    legs = [r for r in rows if r["kind"] == "leg"]
    assert all(r["placed"] is False and r["exclude_from_book"] for r in rows)
    assert all(r["rule4_role_source"] == "unavailable_line_only" for r in legs)
    # Re-run replaces this slate's rows instead of duplicating.
    S.record_nfl_std_under_shadow(board, date="2026-10-25", root=tmp_path, path=log)
    assert len([json.loads(l) for l in log.read_text().splitlines()]) == len(rows)


def test_default_standard_board_is_not_face_verified(tmp_path: Path):
    _step1(tmp_path, "2026-10-25", mixed=False)
    log = tmp_path / "track.jsonl"
    res = S.record_nfl_std_under_shadow([_row("A", "tackles_ast", 6.5)], date="2026-10-25", root=tmp_path, path=log)
    assert res["face_verified_board"] is False
    assert res["n_legs"] == 0
    assert res["skipped"] == {"face_unverified_step1_no_pick_type": 1}


def test_grade_confirms_face_on_board(tmp_path: Path):
    _step1(tmp_path, "2026-10-25")
    log = tmp_path / "track.jsonl"
    board = [
        _row("A", "tackles_ast", 5.5, avg=2.0),
        _row("B", "tackles_ast", 6.5, team="NYG", opp="DAL"),
        _row("C", "tackles_ast", 8.5, team="PHI", opp="WAS"),
    ]
    S.record_nfl_std_under_shadow(board, date="2026-10-25", root=tmp_path, path=log)
    tpl = tmp_path / "templates"
    tpl.mkdir()
    graded = {
        "props": [
            {"sport": "NFL", "player": "A", "prop": "tackles_ast", "direction": "UNDER", "line": 5.5, "pick_type": "Standard", "result": "HIT", "actual_value": 2},
            {"sport": "NFL", "player": "B", "prop": "tackles_ast", "direction": "UNDER", "line": 6.5, "pick_type": "Goblin", "result": "HIT", "actual_value": 3},
            # C never on the graded board at 8.5
        ]
    }
    (tpl / "graded_props_2026-10-25.json").write_text(json.dumps(graded), encoding="utf-8")
    counts = S.grade_pending(path=log, templates=tpl)
    assert counts["graded"] == 1 and counts["no_face_on_board"] == 2
    rows = [json.loads(l) for l in log.read_text().splitlines()]
    by = {r["player"]: r for r in rows if r["kind"] == "leg"}
    assert by["A"]["result"] == "HIT"
    assert by["B"]["result"] == "NOT_ON_BOARD"
    assert by["C"]["result"] == "NOT_ON_BOARD"
    summ = S.summarize(path=log, today="2026-10-26")
    arm = summ["arms"]["rule4_waived"]
    assert arm["decided"] == 1 and arm["hits"] == 1 and arm["not_on_board"] == 2
    assert arm["bar_met"] is False
    assert summ["bar"]["quote_basis"] == "flat_3x_unverified"
    assert summ["arms"]["rule4_applied"]["decided"] == 1
