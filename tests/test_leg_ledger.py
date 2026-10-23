from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from proporacle.data.leg_ledger import (
    EDGE_RATIO_FLAG,
    LEG_ID_VERSION,
    collision_groups,
    fill_summary,
    ingest_graded_file,
    ingest_tickets_file,
    leg_id_v1,
    read_table_df,
    rows_from_graded_payload,
    ticket_join_summary,
)
from proporacle.data.table_io import parquet_available

pytestmark = pytest.mark.skipif(not parquet_available(), reason="pyarrow/fastparquet not installed")

_REPO = Path(__file__).resolve().parents[1]
_FULL_DAY = _REPO / "ui_runner" / "templates" / "graded_props_2026-10-20.json"


def _graded_payload() -> dict:
    return {
        "date": "2026-10-20",
        "count": 2,
        "props": [
            {
                "sport": "NBA",
                "player": "Joel Embiid",
                "team": "PHI",
                "opp_team": "NYK",
                "prop": "Steals",
                "line": "0.5",
                "direction": "UNDER",
                "pick_type": "Standard",
                "league": "",
                "pp_projection_id": "",
                "ml_prob": "0.667",
                "edge": "-0.3",
                "usage_pct": "",
                "l5_over": "1.0",
                "result": "MISS",
                "hit": 0,
                "actual_value": "2.0",
                "on_ticket": False,
            },
            {
                "sport": "NBA",
                "player": "Anfernee Simons",
                "prop": "Points",
                "line": "3.5",
                "direction": "OVER",
                "pick_type": "Goblin",
                "ml_prob": "0.5",
                "edge": "11.3",
                "result": "HIT",
                "hit": 1,
                "actual_value": "13.0",
            },
        ],
    }


def test_leg_id_v1_canonical_aliases():
    a = leg_id_v1(
        slate_date="2026-10-20",
        sport="NBA",
        player="Joel Embiid",
        prop="Steals",
        line=0.5,
        direction="UNDER",
        pick_type="Standard",
    )
    b = leg_id_v1(
        slate_date="2026-10-20",
        sport="nba",
        player=" joel  embiid ",
        prop="stl",
        line="0.50",
        direction="U",
        pick_type="standard",
    )
    c = leg_id_v1(
        slate_date="2026-10-20",
        sport="NBA",
        player="Joel Embiid",
        prop="Points",
        line=12.5,
        direction="OVER",
        pick_type="Goblin",
    )
    d = leg_id_v1(
        slate_date="2026-10-20",
        sport="NBA",
        player="Joel Embiid",
        prop="pts",
        line="12.50",
        direction="HIGHER",
        pick_type="goblin",
    )
    assert a.startswith("v1:")
    assert a == b
    assert c == d
    assert a != c
    assert LEG_ID_VERSION == "v1"


def test_projection_id_not_in_hash():
    a = leg_id_v1(
        slate_date="2026-10-20",
        sport="MLB",
        player="Teoscar Hernández",
        prop="Hitter Strikeouts",
        line=0.5,
        direction="OVER",
        pick_type="Goblin",
        pp_projection_id="",
    )
    b = leg_id_v1(
        slate_date="2026-10-20",
        sport="MLB",
        player="Teoscar Hernandez",
        prop="hitter ks",
        line=0.5,
        direction="OVER",
        pick_type="Goblin",
        pp_projection_id="99999",
    )
    assert a == b


def test_ingest_graded_and_tickets_join(tmp_path: Path):
    gpath = tmp_path / "graded_props_2026-10-20.json"
    gpath.write_text(json.dumps(_graded_payload()), encoding="utf-8")
    ledger = tmp_path / "ledger"
    first = ingest_graded_file(gpath, root=ledger)
    second = ingest_graded_file(gpath, root=ledger)
    assert first["skipped"] is False
    assert second["skipped"] is True
    assert len(list((ledger / "legs").rglob("*.parquet"))) == 1

    legs = read_table_df("legs", ledger)
    assert len(legs) == 2
    assert "player_fold" in legs.columns
    assert "prop_canon" in legs.columns
    assert "source_sha256" in legs.columns
    assert set(legs["result"]) == {"HIT", "MISS"}

    lid = leg_id_v1(
        slate_date="2026-10-20",
        sport="NBA",
        player="Joel Embiid",
        prop="Steals",
        line=0.5,
        direction="UNDER",
        pick_type="Standard",
    )
    tpath = tmp_path / "tickets.json"
    tpath.write_text(
        json.dumps(
            {
                "date": "2026-10-20",
                "groups": [
                    {
                        "group_name": "Goblin-70 Power 3",
                        "tickets": [
                            {
                                "ticket_id": "t1",
                                "ticket_no": 1,
                                "power_payout": 3.25,
                                "payout": {"power": 3.25, "payout_source": "n_correct_live"},
                                "legs": [
                                    {
                                        "sport": "NBA",
                                        "player": "Joel Embiid",
                                        "prop": "Steals",
                                        "line": 0.5,
                                        "direction": "UNDER",
                                        "pick_type": "Standard",
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    ingest_tickets_file(tpath, root=ledger)
    tlegs = read_table_df("ticket_legs", ledger)
    assert list(tlegs["leg_id"]) == [lid]
    joins = ticket_join_summary(ledger)
    assert float(joins["match_rate"].iloc[0]) == 1.0

    fill = fill_summary(ledger)
    nba = fill[fill["sport"] == "NBA"].iloc[0]
    assert nba["n"] == 2
    assert nba["proj_id_fill"] == 0.0
    simons_ratio = 11.3 / 3.5
    assert simons_ratio < EDGE_RATIO_FLAG
    assert nba["edge_ratio_gt5"] == 0.0


def test_blank_player_or_prop_dropped():
    payload = {
        "date": "2026-10-20",
        "props": [{"sport": "CFB", "player": "X", "prop": "", "line": 1.5, "result": "HIT"}],
    }
    legs, _feats = rows_from_graded_payload(payload)
    assert legs == []


@pytest.mark.skipif(not _FULL_DAY.is_file(), reason="graded_props_2026-10-20.json not in checkout")
def test_full_day_no_false_id_collapse():
    payload = json.loads(_FULL_DAY.read_text(encoding="utf-8"))
    bad = collision_groups(payload)
    assert bad == [], bad[:3]
    legs, _ = rows_from_graded_payload(payload)
    assert len(legs) == payload["count"] or len(legs) > 1000
    n_id = len({r["leg_id"] for r in legs})
    n_canon = len(
        {
            (
                r["slate_date_et"],
                r["sport"],
                r["player_fold"],
                r["prop_canon"],
                r["line_norm"],
                r["direction_norm"],
                r["pick_norm"],
            )
            for r in legs
        }
    )
    assert n_id == n_canon
