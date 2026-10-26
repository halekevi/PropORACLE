"""NBA/NHL volume-floor tracks: separate, frozen, NFL untouched."""
from __future__ import annotations

from pathlib import Path

from utils import nba_attempts_floor_shadow as ATT
from utils import nba_assists_floor_shadow as AST
from utils import nba_rebounds_floor_shadow as REB
from utils import nhl_sog_floor_shadow as SOG
from utils.volume_floor_observe import l5_current_season_n, l5_floor, role_tag
from utils.volume_floor_role_tag import nfl_role_tag


def _nba(player, prop, line, *, pt="Demon", games=(12, 11, 14, 10, 13), avg=12, team="BOS"):
    rec = {
        "sport": "NBA",
        "player": player,
        "team": team,
        "prop": prop,
        "line": line,
        "pick_type": pt,
        "side": "OVER",
        "avg_l5": avg,
        "start_time": "2026-11-01T19:30:00-04:00",
        "minutes_tier": "HIGH",
    }
    for i, val in enumerate(games, start=1):
        rec[f"stat_g{i}"] = val
    return rec


def test_each_track_has_its_own_name():
    names = {REB.TRACK, AST.TRACK, ATT.TRACK, SOG.TRACK}
    assert len(names) == 4
    assert "nfl_demon_volume_shadow" not in names
    assert "nfl_demon_rec_floor_shadow" not in names
    assert SOG.TRACK != "nhl_sog_shadow"


def test_preregs_locked():
    from utils.volume_floor_observe import assert_spec

    for spec in (REB.SPEC, AST.SPEC, ATT.SPEC, SOG.SPEC):
        assert_spec(spec)


def test_rebounds_clears_top2_below_floor(tmp_path: Path):
    a = _nba("A", "Rebounds", 7.5, avg=14)
    b = _nba("B", "Rebounds", 6.5, avg=11)
    c = _nba("C", "Rebounds", 4.5, avg=5)
    step = tmp_path / "outputs" / "2026-11-01" / "nba"
    step.mkdir(parents=True)
    (step / "step1_pp_props_today.csv").write_text(
        "projection_id,pick_type,line\n1,Demon,7.5\n2,Goblin,3.5\n",
        encoding="utf-8",
    )
    res = REB.record_nba_rebounds_floor_shadow(
        [a, b, c], date="2026-11-01", root=tmp_path, path=tmp_path / "log.jsonl"
    )
    assert res["n_legs"] == 2
    names = {l["player"] for l in res["legs"]}
    assert names == {"A", "B"}
    assert all(l["role_tag"] in {"lead", "secondary"} for l in res["legs"])


def test_attempts_drops_made_shots(tmp_path: Path):
    row = _nba("A", "FG Made", 4.5, avg=8)
    step = tmp_path / "outputs" / "2026-11-01" / "nba"
    step.mkdir(parents=True)
    (step / "step1_pp_props_today.csv").write_text(
        "projection_id,pick_type,line\n1,Demon,4.5\n2,Goblin,2.5\n",
        encoding="utf-8",
    )
    res = ATT.record_nba_attempts_floor_shadow(
        [row], date="2026-11-01", root=tmp_path, path=tmp_path / "log.jsonl"
    )
    assert res["n_legs"] == 0


def test_goblin_stays_out(tmp_path: Path):
    row = _nba("A", "Assists", 3.5, pt="Goblin", avg=8)
    step = tmp_path / "outputs" / "2026-11-01" / "nba"
    step.mkdir(parents=True)
    (step / "step1_pp_props_today.csv").write_text(
        "projection_id,pick_type,line\n1,Demon,7.5\n2,Goblin,3.5\n",
        encoding="utf-8",
    )
    res = AST.record_nba_assists_floor_shadow(
        [row], date="2026-11-01", root=tmp_path, path=tmp_path / "log.jsonl"
    )
    assert res["n_legs"] == 0


def test_l5_floor_is_min():
    row = _nba("A", "Rebounds", 7.5, games=(12, 9, 14, 11, 10))
    assert l5_floor(row) == 9


def test_l5_current_season_n_is_log_only(tmp_path: Path):
    a = _nba("A", "Rebounds", 7.5, avg=14)
    a["current_stat_g1"] = 12
    a["current_stat_g2"] = 11
    b = _nba("B", "Rebounds", 6.5, avg=11)
    b["window_mix"] = "RROOO"
    step = tmp_path / "outputs" / "2026-11-01" / "nba"
    step.mkdir(parents=True)
    (step / "step1_pp_props_today.csv").write_text(
        "projection_id,pick_type,line\n1,Demon,7.5\n2,Goblin,3.5\n",
        encoding="utf-8",
    )
    res = REB.record_nba_rebounds_floor_shadow(
        [a, b], date="2026-11-01", root=tmp_path, path=tmp_path / "log.jsonl"
    )
    assert res["n_legs"] == 2
    by = {l["player"]: l["l5_current_season_n"] for l in res["legs"]}
    assert by["A"] == 2
    assert by["B"] == 2
    assert l5_current_season_n({"current_stat_g1": 8}) == 1
    assert l5_current_season_n({"window_mix": "OOOOO"}) == 0


def test_backup_tag_does_not_block():
    assert role_tag(1, 50, backup_below=40) == "lead"
    assert role_tag(1, 20, backup_below=40) == "backup"
    assert role_tag(2, 55, backup_below=40) == "secondary"
    assert nfl_role_tag({"role_source": "tgt_rank_1", "snap_pct_L3": 22}) == "backup"
    assert nfl_role_tag({"role_source": "tgt_rank_1", "snap_pct_L3": 70}) == "lead"


def test_frozen_nfl_files_not_imported_for_eligibility():
    src = Path(__file__).resolve().parents[1] / "utils" / "volume_floor_observe.py"
    text = src.read_text(encoding="utf-8")
    assert "from utils.nfl_demon_volume_shadow import" not in text
    assert "from utils.nfl_demon_rec_floor_shadow import" not in text
