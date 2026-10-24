"""Unranked tennis names must not collapse onto Ann Li / other short last names."""

from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "Sports" / "Tennis" / "scripts"))

from tennis_shared import (  # noqa: E402
    resolve_athlete_id,
    search_espn_tennis_athlete,
)


_ANN_LI = [
    {
        "espn_athlete_id": "3380",
        "player": "Ann Li",
        "tour": "WTA",
        "rank": 40,
        "player_key": "ann li",
    },
    {
        "espn_athlete_id": "111",
        "player": "Rajeev Ram",
        "tour": "ATP",
        "rank": 80,
        "player_key": "rajeev ram",
    },
    {
        "espn_athlete_id": "13479",
        "player": "Mary Stoiana",
        "tour": "WTA",
        "rank": 122,
        "player_key": "mary stoiana",
    },
]


def test_full_names_with_li_do_not_resolve_to_ann_li():
    for name in (
        "Jordyn Hazelitt",
        "Elizabeth Mandlik",
        "Alice Tubello",
        "Gauthier Onclin",
        "Julia Avdeeva",
        "Liam Draxl",
    ):
        eid, _tour = resolve_athlete_id(name, _ANN_LI)
        assert eid != "3380", name
        assert eid == "", name


def test_abbrev_ram_r_still_matches():
    eid, tour = resolve_athlete_id("Ram R", _ANN_LI)
    assert eid == "111"
    assert tour == "ATP"


def test_rankings_id_kept_for_stoiana():
    eid, tour = resolve_athlete_id("Mary Stoiana", _ANN_LI)
    assert eid == "13479"
    assert tour == "WTA"


def test_search_rejects_samsonova_for_samson():
    """Laura Samson must not take Laura Samsonova's ESPN id from search hits."""
    items = [
        {
            "sport": "tennis",
            "displayName": "Laura Samsonova",
            "id": "13513",
            "league": "wta",
        },
        {
            "sport": "tennis",
            "displayName": "Laura Samson",
            "id": "15069",
            "league": "wta",
        },
    ]

    def _fake_fetch(_url: str, timeout: int = 25):
        return {"items": items}

    import tennis_shared as ts

    orig = ts.fetch_json
    ts.fetch_json = _fake_fetch
    try:
        hit = search_espn_tennis_athlete("Laura Samson")
    finally:
        ts.fetch_json = orig
    assert hit is not None
    assert hit["espn_athlete_id"] == "15069"
    assert hit["player_key"] == "laura samson"


def test_search_matches_family_given_flip():
    items = [
        {
            "sport": "tennis",
            "displayName": "Guo Hanyu",
            "id": "3149",
            "league": "wta",
        }
    ]

    def _fake_fetch(_url: str, timeout: int = 25):
        return {"items": items}

    import tennis_shared as ts

    orig = ts.fetch_json
    ts.fetch_json = _fake_fetch
    try:
        hit = search_espn_tennis_athlete("Hanyu Guo")
    finally:
        ts.fetch_json = orig
    assert hit is not None
    assert hit["espn_athlete_id"] == "3149"


def test_search_rejects_ann_li_for_hazelitt():
    items = [
        {
            "sport": "tennis",
            "displayName": "Ann Li",
            "id": "3380",
            "league": "wta",
        }
    ]

    def _fake_fetch(_url: str, timeout: int = 25):
        return {"items": items}

    import tennis_shared as ts

    orig = ts.fetch_json
    ts.fetch_json = _fake_fetch
    try:
        hit = search_espn_tennis_athlete("Jordyn Hazelitt")
    finally:
        ts.fetch_json = orig
    assert hit is None
