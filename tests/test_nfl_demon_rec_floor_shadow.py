"""NFL Demon receptions L5-floor observe: top-2 + line under L5 min."""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from utils import nfl_demon_rec_floor_shadow as F
from utils import nfl_demon_volume_shadow as V


def _row(
    player: str,
    line: float,
    *,
    pt="Demon",
    side="OVER",
    prop="Receptions",
    team="LV",
    tgt=22.0,
    games=(6, 5, 8, 4, 7),
    start="2026-11-01T13:00:00-04:00",
):
    rec = {
        "sport": "NFL",
        "league": "NFL",
        "player": player,
        "team": team,
        "opp_team": "KC",
        "prop": prop,
        "line": line,
        "pick_type": pt,
        "side": side,
        "target_share_L3": tgt,
        "start_time": start,
    }
    for i, val in enumerate(games, start=1):
        rec[f"current_stat_g{i}"] = val
    return rec


def _step1(root: Path, date: str) -> None:
    d = root / "outputs" / date / "nfl"
    d.mkdir(parents=True)
    (d / "step1_pp_props_today.csv").write_text(
        "projection_id,pick_type,line\n1,Demon,3.5\n2,Goblin,1.5\n",
        encoding="utf-8",
    )


def test_frozen_volume_gate_untouched():
    V.assert_frozen_spec()
    spec = json_spec()
    assert spec["track"] == "nfl_demon_rec_floor_shadow"
    assert "nfl_demon_volume_shadow" != spec["track"]


def json_spec():
    F.assert_frozen_spec()
    return F.frozen_spec()


def test_new_track_spec_is_not_the_leader_gate():
    spec = json_spec()
    assert spec["rank_max"] == 2
    assert spec["min_tgt_share_pp"] == 15
    assert spec["l5_games_min"] == 5
    assert spec["props_in"] == ["receptions"]


def test_tucker_style_number_two_clears_floor():
    bowers = _row("Bowers", 5.5, tgt=28.0, games=(8, 7, 9, 6, 8))
    tucker = _row("Tucker", 3.5, tgt=22.0, games=(6, 5, 8, 4, 7))
    ranks = F.team_tgt_ranks([bowers, tucker])
    ok, why = F.leg_qualifies(tucker, face_verified=True, ranks=ranks)
    assert ok, why
    assert why == "tgt_rank_2"
    assert F.l5_floor(tucker) == 4


def test_target_leader_also_clears_when_floor_holds():
    bowers = _row("Bowers", 5.5, tgt=28.0, games=(8, 7, 9, 6, 8))
    tucker = _row("Tucker", 3.5, tgt=22.0, games=(6, 5, 8, 4, 7))
    ranks = F.team_tgt_ranks([bowers, tucker])
    ok, why = F.leg_qualifies(bowers, face_verified=True, ranks=ranks)
    assert ok, why
    assert why == "tgt_rank_1"


def test_third_option_and_thin_share_fail():
    board = [
        _row("Bowers", 5.5, tgt=28.0),
        _row("Tucker", 3.5, tgt=22.0),
        _row("Third", 1.5, tgt=12.0, games=(4, 3, 5, 2, 3)),
    ]
    ranks = F.team_tgt_ranks(board)
    ok, why = F.leg_qualifies(board[2], face_verified=True, ranks=ranks)
    assert not ok
    assert why in {"not_top2_target", "share_below_floor"}


def test_line_at_or_above_l5_min_fails():
    row = _row("Tucker", 4.0, tgt=22.0, games=(6, 5, 8, 4, 7))
    ranks = F.team_tgt_ranks([row])
    ok, why = F.leg_qualifies(row, face_verified=True, ranks=ranks)
    assert not ok
    assert why == "line_not_below_l5_floor"


def test_yards_and_rush_attempts_are_out():
    rec_yds = _row("Tucker", 39.5, prop="Receiving Yards", tgt=22.0)
    rush = _row("Skattebo", 9.5, prop="Rush Attempts", team="NYG", tgt=None)
    rush["rush_att_share"] = 0.55
    ranks = F.team_tgt_ranks([rec_yds, rush])
    assert F.leg_qualifies(rec_yds, face_verified=True, ranks=ranks)[1] == "prop_not_listed"
    assert F.leg_qualifies(rush, face_verified=True, ranks=ranks)[1] == "prop_not_listed"


def test_today_is_excluded_from_the_log(tmp_path: Path):
    _step1(tmp_path, "2026-10-25")
    row = _row("Tucker", 3.5, start="2026-10-25T16:25:00-04:00")
    res = F.record_nfl_demon_rec_floor_shadow(
        [row],
        date="2026-10-25",
        root=tmp_path,
        path=tmp_path / "log.jsonl",
        now=datetime.fromisoformat("2026-10-25T12:00:00-04:00"),
    )
    assert res["n_legs"] == 0
    assert res["skipped"].get("game_before_prereg", 0) >= 1


def test_forward_number_two_logs(tmp_path: Path):
    _step1(tmp_path, "2026-11-01")
    bowers = _row("Bowers", 5.5, tgt=28.0, games=(8, 7, 9, 6, 8))
    tucker = _row("Tucker", 3.5, tgt=22.0, games=(6, 5, 8, 4, 7))
    res = F.record_nfl_demon_rec_floor_shadow(
        [bowers, tucker],
        date="2026-11-01",
        root=tmp_path,
        path=tmp_path / "log.jsonl",
        now=datetime.fromisoformat("2026-11-01T10:00:00-04:00"),
    )
    assert res["n_legs"] == 2
    names = {l["player"] for l in res["legs"]}
    assert names == {"Bowers", "Tucker"}
