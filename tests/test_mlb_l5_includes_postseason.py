"""Playoff games count in the MLB L5 game log."""
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
MLB_SCRIPTS = REPO_ROOT / "Sports" / "MLB" / "scripts"


def _load():
    path = MLB_SCRIPTS / "step4_attach_player_stats_mlb.py"
    spec = importlib.util.spec_from_file_location("mlb_step4_l5_postseason", path)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def test_fetch_merges_regular_season_and_postseason(monkeypatch):
    step4 = _load()
    calls: list[str] = []

    def fake_get(url, retries=3):
        calls.append(url)
        if "gameType=P" in url:
            return {
                "stats": [
                    {
                        "splits": [
                            {"date": "2026-10-07", "game": {"gamePk": 2}, "stat": {"hits": 1}},
                            {"date": "2026-10-03", "game": {"gamePk": 3}, "stat": {"hits": 0}},
                        ]
                    }
                ]
            }
        return {
            "stats": [
                {
                    "splits": [
                        {"date": "2026-09-27", "game": {"gamePk": 1}, "stat": {"hits": 1}},
                        {"date": "2026-10-07", "game": {"gamePk": 2}, "stat": {"hits": 9}},
                    ]
                }
            ]
        }

    monkeypatch.setattr(step4, "_get", fake_get)
    splits = step4.fetch_game_log("668930", "hitting", "2026")
    assert any("gameType=R" in url for url in calls)
    assert any("gameType=P" in url for url in calls)
    assert [sp["game"]["gamePk"] for sp in splits] == [1, 3, 2]
    october = next(sp for sp in splits if sp["game"]["gamePk"] == 2)
    assert october["stat"]["hits"] == 1


def _hit_split(game_pk: int, date: str, hits: int) -> dict:
    return {
        "date": date,
        "game": {"gamePk": game_pk},
        "stat": {"hits": hits, "atBats": 4},
        "team": {"id": 158},
        "opponent": {"id": 119},
        "player": {"id": 668930},
    }


def test_update_cache_writes_every_game_after_last_stored(monkeypatch):
    step4 = _load()
    step4._CACHE_INDEX = None
    monkeypatch.setattr(step4, "open_game_pks", lambda: set())
    monkeypatch.setattr(
        step4,
        "fetch_live_feed_stats",
        lambda game_pk: {"first_inning": {}, "pitcher": {}, "hitter": {}},
    )
    splits = [_hit_split(1, "2026-09-27", 1)]
    splits += [_hit_split(100 + i, f"2026-10-{i + 1:02d}", i % 2) for i in range(12)]
    monkeypatch.setattr(step4, "fetch_game_log", lambda *args, **kwargs: splits)

    cache = step4.pd.DataFrame(
        [
            {
                "MLB_PLAYER_ID": "668930",
                "SEASON": "2026",
                "GAME_DATE": "2026-09-27",
                "GAME_ID": "1",
                "PLAYER_TYPE": "hitter",
                "PROP_NORM": "hits",
                "STAT_VALUE": "1",
                "TEAM_ID": "158",
                "OPP_TEAM_ID": "119",
            }
        ]
    )
    # Fill the stored game so the refresh does not treat September as a hole.
    props = [
        "hits", "total_bases", "home_runs", "rbi", "runs", "walks",
        "stolen_bases", "fantasy_score", "hits_runs_rbi", "singles", "doubles",
        "triples", "hitter_strikeouts", "plate_appearances", "pitches_seen",
    ]
    rows = []
    for prop in props:
        row = cache.iloc[0].to_dict()
        row["PROP_NORM"] = prop
        rows.append(row)
    cache = step4.pd.DataFrame(rows)

    updated, added = step4.update_cache(cache, "668930", "hitter", "2026", n_games=2)
    ids = set(updated["GAME_ID"].astype(str))
    assert added == 12
    assert ids == {str(1)} | {str(100 + i) for i in range(12)}


def test_update_cache_skips_in_progress_game(monkeypatch):
    step4 = _load()
    step4._CACHE_INDEX = None
    monkeypatch.setattr(step4, "open_game_pks", lambda: {"999"})
    monkeypatch.setattr(
        step4,
        "fetch_live_feed_stats",
        lambda game_pk: {"first_inning": {}, "pitcher": {}, "hitter": {}},
    )
    monkeypatch.setattr(
        step4,
        "fetch_game_log",
        lambda *args, **kwargs: [
            _hit_split(1, "2026-09-27", 1),
            _hit_split(999, "2026-10-12", 3),
            _hit_split(1000, "2026-10-11", 1),
        ],
    )
    cache = step4.pd.DataFrame(columns=step4.CACHE_COLS)
    updated, added = step4.update_cache(cache, "668930", "hitter", "2026", n_games=10)
    ids = set(updated["GAME_ID"].astype(str))
    assert "999" not in ids
    assert "1000" in ids
    assert added == 2
