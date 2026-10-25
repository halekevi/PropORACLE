"""NFL Demon volume observe: role × favorite gate, no backfill, no tickets."""
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from utils import nfl_demon_volume_shadow as S


def _row(
    player: str,
    prop: str,
    line: float,
    *,
    pt="Demon",
    side="OVER",
    team="NYG",
    opp="PHI",
    spread=-3.5,
    snap=70.0,
    tgt=None,
    rush_share=None,
    rec_share=None,
    avg_l5=14.0,
    injury="",
    start="2026-11-01T13:00:00-04:00",
):
    rec = {
        "sport": "NFL",
        "league": "NFL",
        "player": player,
        "team": team,
        "opp_team": opp,
        "prop": prop,
        "line": line,
        "pick_type": pt,
        "side": side,
        "spread": spread,
        "snap_pct_L3": snap,
        "target_share_L3": tgt,
        "rush_att_share": rush_share,
        "rec_tgt_share": rec_share,
        "avg_l5": avg_l5,
        "injury_status": injury,
        "start_time": start,
    }
    return rec


def _step1(root: Path, date: str, mixed: bool = True) -> None:
    d = root / "outputs" / date / "nfl"
    d.mkdir(parents=True)
    if mixed:
        rows = ["projection_id,pick_type,line", "1,Demon,9.5", "2,Goblin,19.5"]
    else:
        rows = ["projection_id,pick_type,line", "1,Standard,9.5"]
    (d / "step1_pp_props_today.csv").write_text("\n".join(rows) + "\n", encoding="utf-8")


def test_frozen_spec_matches_prereg():
    S.assert_frozen_spec()
    spec = S.frozen_spec()
    assert spec["amendments_closed"] is True
    assert spec["close_max_dog"] == 3.5
    assert spec["props_in"] == ["receptions", "rush_attempts"]


def test_volume_prop_aliases():
    assert S.volume_prop("Rush Attempts") == "rush_attempts"
    assert S.volume_prop("Rushing Attempts") == "rush_attempts"
    assert S.volume_prop("Receptions") == "receptions"
    assert S.volume_prop("recs") == "receptions"
    assert S.volume_prop("fg_made") == "fg_made"


def test_leg_gate_role_and_script():
    lead = _row("Skattebo", "Rush Attempts", 9.5, snap=72.0)
    backup = _row("Backup", "Rush Attempts", 4.5, snap=28.0)
    wr = _row("Nabers", "Receptions", 4.5, snap=80.0, tgt=28.0)
    wr2 = _row("Other", "Receptions", 2.5, snap=55.0, tgt=12.0)
    roles = S.team_role_index([lead, backup, wr, wr2])
    ok, why = S.leg_qualifies(lead, face_verified=True, roles=roles)
    assert ok, why
    assert why == "snap_l3"
    assert S.leg_qualifies(backup, face_verified=True, roles=roles) == (False, "not_lead_back")
    ok_wr, why_wr = S.leg_qualifies(wr, face_verified=True, roles=roles)
    assert ok_wr, why_wr
    assert S.leg_qualifies(wr2, face_verified=True, roles=roles) == (False, "not_target_leader")
    close = dict(lead, spread=3.5)
    ok_close, why_close = S.leg_qualifies(close, face_verified=True, roles=roles)
    assert ok_close, why_close
    pickem = dict(lead, spread=0.0)
    ok_pe, why_pe = S.leg_qualifies(pickem, face_verified=True, roles=roles)
    assert ok_pe, why_pe
    big_dog = dict(lead, spread=7.5)
    assert S.leg_qualifies(big_dog, face_verified=True, roles=roles) == (False, "not_favored_or_close")
    missing = dict(lead)
    missing.pop("spread")
    assert S.leg_qualifies(missing, face_verified=True, roles=roles) == (False, "spread_unknown")
    no_role = dict(lead, snap_pct_L3=None)
    assert S.leg_qualifies(no_role, face_verified=True, roles=roles) == (False, "role_unavailable")
    assert S.leg_qualifies(dict(lead, pick_type="Goblin"), face_verified=True, roles=roles) == (
        False,
        "not_literal_demon",
    )
    assert S.leg_qualifies(dict(lead, side="UNDER"), face_verified=True, roles=roles) == (False, "demon_under_invalid")
    assert S.leg_qualifies(_row("K", "fg_made", 1.5), face_verified=True, roles=roles) == (False, "prop_not_listed")
    assert S.leg_qualifies(lead, face_verified=False, roles=roles)[1] == "face_unverified_step1_no_pick_type"
    today = dict(lead, start_time="2026-10-25T13:00:00-04:00")
    assert S.leg_qualifies(today, face_verified=True, roles=roles) == (False, "game_before_prereg")


def test_weekly_recheck_committee_injury_and_line():
    lead = _row("Skattebo", "Rush Attempts", 9.5, snap=55.0)
    partner = _row("Tracy", "Rush Attempts", 6.5, snap=48.0)
    roles = S.team_role_index([lead, partner])
    assert S.leg_qualifies(lead, face_verified=True, roles=roles) == (False, "committee")

    solo = _row("Skattebo", "Rush Attempts", 9.5, snap=72.0)
    backup = _row("Tracy", "Rush Attempts", 4.5, snap=28.0)
    roles2 = S.team_role_index([solo, backup])
    hurt = dict(solo, injury_status="Questionable")
    assert S.leg_qualifies(hurt, face_verified=True, roles=roles2) == (False, "injury_taking_carries")
    raised = dict(solo, line=14.5, avg_l5=12.0)
    assert S.leg_qualifies(raised, face_verified=True, roles=roles2) == (False, "line_not_low_vs_recent")
    no_avg = dict(solo)
    no_avg.pop("avg_l5")
    assert S.leg_qualifies(no_avg, face_verified=True, roles=roles2) == (False, "line_vs_recent_unknown")
    ok, why = S.leg_qualifies(solo, face_verified=True, roles=roles2)
    assert ok, why


def test_record_excludes_skattebo_slate_and_is_not_placed(tmp_path: Path):
    _step1(tmp_path, "2026-11-01")
    lead = _row("Skattebo", "Rush Attempts", 9.5, snap=72.0)
    backup = _row("Tracy", "Rush Attempts", 4.5, snap=28.0)
    wr = _row("Nabers", "Receptions", 4.5, snap=80.0, tgt=28.0)
    already = _row(
        "Done",
        "Rush Attempts",
        8.5,
        team="DAL",
        opp="NYG",
        snap=65.0,
        start="2026-11-01T12:00:00-04:00",
    )
    motiver = _row("Skattebo", "Rush Attempts", 9.5, snap=72.0, start="2026-10-25T13:00:00-04:00")
    log = tmp_path / "track.jsonl"
    now = datetime.fromisoformat("2026-11-01T12:30:00-04:00")
    res = S.record_nfl_demon_volume_shadow(
        [lead, backup, wr, already, motiver],
        date="2026-11-01",
        root=tmp_path,
        path=log,
        now=now,
    )
    assert res["placed"] is False
    assert res["face_verified_board"] is True
    assert res["n_legs"] == 2
    assert res["n_rush_attempts"] == 1
    assert res["n_receptions"] == 1
    assert res["skipped"].get("already_started") == 1
    assert res["skipped"].get("game_before_prereg") == 1
    rows = [json.loads(l) for l in log.read_text().splitlines()]
    assert all(r["placed"] is False and r["exclude_from_book"] for r in rows)
    assert {r["player"] for r in rows} == {"Skattebo", "Nabers"}
    S.record_nfl_demon_volume_shadow(
        [lead, backup, wr], date="2026-11-01", root=tmp_path, path=log, now=now
    )
    assert len([json.loads(l) for l in log.read_text().splitlines()]) == 2


def test_default_standard_board_is_not_face_verified(tmp_path: Path):
    _step1(tmp_path, "2026-11-01", mixed=False)
    log = tmp_path / "track.jsonl"
    res = S.record_nfl_demon_volume_shadow(
        [_row("A", "Rush Attempts", 9.5)], date="2026-11-01", root=tmp_path, path=log
    )
    assert res["face_verified_board"] is False
    assert res["n_legs"] == 0
    assert res["skipped"] == {"face_unverified_step1_no_pick_type": 1}


def test_grade_requires_demon_face(tmp_path: Path):
    _step1(tmp_path, "2026-11-01")
    log = tmp_path / "track.jsonl"
    board = [
        _row("A", "Rush Attempts", 9.5, snap=70.0),
        _row("B", "Receptions", 4.5, team="PHI", opp="NYG", snap=80.0, tgt=30.0),
    ]
    S.record_nfl_demon_volume_shadow(board, date="2026-11-01", root=tmp_path, path=log)
    tpl = tmp_path / "templates"
    tpl.mkdir()
    graded = {
        "props": [
            {
                "sport": "NFL",
                "player": "A",
                "prop": "rush_attempts",
                "direction": "OVER",
                "line": 9.5,
                "pick_type": "Demon",
                "result": "HIT",
                "actual_value": 16,
            },
            {
                "sport": "NFL",
                "player": "B",
                "prop": "receptions",
                "direction": "OVER",
                "line": 4.5,
                "pick_type": "Goblin",
                "result": "HIT",
                "actual_value": 6,
            },
        ]
    }
    (tpl / "graded_props_2026-11-01.json").write_text(json.dumps(graded), encoding="utf-8")
    counts = S.grade_pending(path=log, templates=tpl)
    assert counts["graded"] == 1 and counts["no_face_on_board"] == 1
    rows = [json.loads(l) for l in log.read_text().splitlines()]
    by = {r["player"]: r for r in rows}
    assert by["A"]["result"] == "HIT" and by["A"]["actual"] == 16
    assert by["B"]["result"] == "NOT_ON_BOARD"
    summ = S.summarize(path=log, today="2026-11-02")
    assert summ["bar"]["quote_basis"] == "unverified_needs_demon_n_correct"
    arm = summ["arms"]["rush_attempts"]
    assert arm["decided"] == 1 and arm["hits"] == 1
    assert arm["bar_met"] is False
    assert summ["arms"]["receptions"]["not_on_board"] == 1
