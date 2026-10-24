"""MLB/WNBA postseason prices at the cell minus 3. Other postseason stays blocked."""
from __future__ import annotations

import json
from pathlib import Path

from utils.postseason_slate import (
    apply_postseason_price,
    counts_toward_gated_book,
    power_ev,
    season_blocks_ticket,
)


def test_preseason_blocks_and_mlb_wnba_postseason_is_cell_minus_3():
    pre = {"sport": "MLB", "season_type": "1", "team": "NYY", "opp_team": "BOS"}
    mlb_post = {"sport": "MLB", "season_type": "3", "team": "NYY", "opp_team": "BOS"}
    wnba_post = {"sport": "WNBA1H", "season_type": "postseason", "team": "NY", "opp_team": "LV"}
    nfl_post = {"sport": "NFL", "season_type": "3", "team": "KC", "opp_team": "BUF"}
    unknown = {"sport": "MLB", "team": "NYY"}
    assert season_blocks_ticket(pre)
    assert not season_blocks_ticket(mlb_post)
    assert not season_blocks_ticket(wnba_post)
    assert season_blocks_ticket(nfl_post)
    assert season_blocks_ticket(unknown)
    nbap = {"sport": "NBA", "league": "NBAP", "team": "BOS", "opp_team": "NYK"}
    assert season_blocks_ticket(nbap)
    nbap_id = {"sport": "NBA", "league_id": "237", "team": "BOS", "opp_team": "NYK"}
    assert season_blocks_ticket(nbap_id)
    assert apply_postseason_price(mlb_post, 0.803)
    assert abs(mlb_post["p"] - 0.773) < 1e-9
    assert mlb_post["postseason"] == 1
    assert mlb_post["p_source"] == "postseason_cell_minus_3"
    assert mlb_post["p"] != 0.763
    assert not apply_postseason_price(dict(mlb_post), None)
    # 3-leg at 2.2x on 77.3% is about +1.6% EV. Another 2 points is negative.
    assert power_ev([0.773, 0.773, 0.773], 2.2) > 0
    assert power_ev([0.753, 0.753, 0.753], 2.2) < 0


def test_prizepicks_wnba_abbr_joins_espn_postseason():
    import utils.postseason_slate as ps

    def fake_scoreboard(family, day):
        assert family == "WNBA"
        return [
            {
                "competitions": [
                    {
                        "competitors": [
                            {"team": {"abbreviation": "NY"}},
                            {"team": {"abbreviation": "ATL"}},
                        ]
                    }
                ],
                "season": {"type": 3, "slug": "post-season"},
            }
        ]

    orig = ps._scoreboard
    ps._scoreboard = fake_scoreboard
    try:
        row = {
            "sport": "WNBA",
            "team": "NYL",
            "opp_team": "ATL",
            "start_time": "2026-10-09T19:30:00-04:00",
        }
        assert ps.season_class(row) == "postseason"
        assert not ps.season_blocks_ticket(row)
    finally:
        ps._scoreboard = orig


def test_postseason_slip_is_out_of_the_71_book():
    book = Path(__file__).resolve().parents[1] / "data" / "reports" / "gated_goblin_slip_manifest_20261007.json"
    payload = json.loads(book.read_text(encoding="utf-8"))
    slips = payload.get("slips") or []
    assert slips
    assert all(counts_toward_gated_book(slip) for slip in slips)
    tagged = {
        "resolved": True,
        "postseason": 1,
        "ticket_track": "gated_goblin_postseason",
        "players": ["Example"],
    }
    assert not counts_toward_gated_book(tagged)
    kept = [slip for slip in slips if counts_toward_gated_book(slip)]
    assert len(kept) == len(slips)
