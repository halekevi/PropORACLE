from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from proporacle.data.leg_ledger import (
    EDGE_RATIO_FLAG,
    LEG_ID_VERSION,
    classify_unmatched_ticket_legs,
    collision_groups,
    fill_summary,
    ingest_graded_file,
    ingest_tickets_file,
    leg_id_v1,
    read_table_df,
    rows_from_graded_payload,
    ticket_join_summary,
    analysis_legs,
    current_legs,
    face_status,
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


def test_stale_nba1q_ticket_legs_excluded_from_join(tmp_path: Path):
    ledger = tmp_path / "ledger"
    tpath = tmp_path / "tickets.json"
    tpath.write_text(
        json.dumps(
            {
                "date": "2026-10-08",
                "groups": [
                    {
                        "group_name": "NBA1Q Power 3",
                        "tickets": [
                            {
                                "ticket_id": "phantom",
                                "legs": [
                                    {
                                        "sport": "NBA1Q",
                                        "player": "Jalen Williams",
                                        "team": "OKC",
                                        "opp": "SAS",
                                        "prop": "Points",
                                        "line": 5.5,
                                        "direction": "OVER",
                                        "pick_type": "Goblin",
                                        "game_date": "2026-05-30",
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
    assert len(tlegs) == 1
    assert str(tlegs.iloc[0]["invalid_reason"]) == "stale_board"
    joins = ticket_join_summary(ledger)
    assert joins.empty or int(joins["ticket_leg_rows"].sum()) == 0


def test_classify_unmatched_no_actual_vs_fingerprint(tmp_path: Path):
    ledger = tmp_path / "ledger"
    gpath = tmp_path / "graded_props_2026-10-05.json"
    gpath.write_text(json.dumps(_graded_payload() | {"date": "2026-10-05"}), encoding="utf-8")
    ingest_graded_file(gpath, root=ledger)
    tpath = tmp_path / "tickets.json"
    tpath.write_text(
        json.dumps(
            {
                "date": "2026-10-05",
                "groups": [
                    {
                        "tickets": [
                            {
                                "ticket_id": "t-fp",
                                "legs": [
                                    {
                                        "sport": "NBA",
                                        "player": "Joel Embiid",
                                        "prop": "Steals",
                                        "line": 0.5,
                                        "direction": "OVER",
                                        "pick_type": "Goblin",
                                    }
                                ],
                            },
                            {
                                "ticket_id": "t-miss",
                                "legs": [
                                    {
                                        "sport": "TENNIS",
                                        "player": "Schoolkate",
                                        "prop": "Total Games Won",
                                        "line": 12.5,
                                        "direction": "OVER",
                                        "pick_type": "standard",
                                    }
                                ],
                            },
                        ]
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    ingest_tickets_file(tpath, root=ledger)
    miss = classify_unmatched_ticket_legs(ledger, since="2026-10-01", until="2026-10-10")
    by_ticket = dict(zip(miss["ticket_id"], miss["unmatched_reason"]))
    assert by_ticket["t-fp"] == "fingerprint"
    assert by_ticket["t-miss"] == "no_actual"


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


def _default_label_payload(date: str, n: int = 120) -> dict:
    """A graded day whose step1 never stored pick_type: every row Standard, rungs stacked."""
    props = []
    for i in range(n):
        props.append(
            {
                "sport": "NFL",
                "player": f"Player {i // 3}",
                "prop": "Receiving Yards",
                "line": str(4.5 + 27.0 * (i % 3)),  # 4.5 / 31.5 / 58.5 ladder, all "Standard"
                "direction": "OVER",
                "pick_type": "Standard",
                "result": "HIT" if i % 2 else "MISS",
                "hit": i % 2,
                "actual_value": "40.0",
            }
        )
    return {"date": date, "count": n, "props": props}


def test_face_status_is_first_class_and_analysis_legs_filters(tmp_path: Path, capsys):
    ledger = tmp_path / "ledger"
    good = tmp_path / "graded_props_2026-10-20.json"
    good.write_text(json.dumps(_graded_payload()), encoding="utf-8")
    bad = tmp_path / "graded_props_2026-10-19.json"
    bad.write_text(json.dumps(_default_label_payload("2026-10-19")), encoding="utf-8")
    ingest_graded_file(good, root=ledger)
    ingest_graded_file(bad, root=ledger)

    legs = current_legs(ledger)
    assert {"face_status", "face_verified"} <= set(legs.columns)
    by_date = legs.groupby("slate_date")["face_verified"].first().to_dict()
    assert bool(by_date["2026-10-20"])
    assert not bool(by_date["2026-10-19"])
    assert legs.loc[legs["slate_date"] == "2026-10-19", "face_status"].iloc[0] == "default_labels"

    st = face_status(ledger)
    assert (ledger / "face_status" / "face_status.parquet").is_file()
    assert set(st["slate_date"]) == {"2026-10-19", "2026-10-20"}

    kept = analysis_legs(ledger)
    assert set(kept["slate_date"]) == {"2026-10-20"}
    assert "dropped" in capsys.readouterr().out
    everything = analysis_legs(ledger, include_unverified=True)
    assert set(everything["slate_date"]) == {"2026-10-19", "2026-10-20"}
