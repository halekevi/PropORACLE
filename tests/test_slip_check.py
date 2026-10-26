"""Pre-submit slip check: 3 max, distinct teams, cleaned/gift-scan, props, quote."""
from __future__ import annotations

import json
from pathlib import Path

from ml.slip_check import (
    PLACE_FAIL,
    SCREENSHOT_MSG,
    check_slip,
    format_result,
    is_clean_gift,
    main,
    parse_leg_arg,
    parse_legs,
    screenshot_message,
    unsupported_reason,
)


def _leg(
    player: str,
    prop: str = "Points",
    *,
    side: str = "OVER",
    line: float = 20.5,
    rung: str = "goblin",
    team: str = "BOS",
    sport: str = "NBA",
    **kw,
) -> dict:
    rec = {
        "player": player,
        "prop": prop,
        "side": side,
        "line": line,
        "rung": rung,
        "pick_type": rung,
        "team": team,
        "sport": sport,
    }
    rec.update(kw)
    return rec


def _listed(*legs: dict) -> list[dict]:
    return [dict(x) for x in legs]


def _gift(leg: dict, *, all_pass: bool = True, published_after_start: bool = False) -> dict:
    rec = dict(leg)
    rec["all_pass"] = all_pass
    rec["published_after_start"] = published_after_start
    return rec


A = _leg("Tatum", "Points", team="BOS", line=20.5, standard_line=24.5, goblin_delta=4.0)
B = _leg("Brunson", "Assists", team="NYK", line=6.5, standard_line=8.5, goblin_delta=2.0)
C = _leg("Edwards", "Rebounds", team="MIN", line=5.5, standard_line=7.5, goblin_delta=2.0)
LISTED = _listed(A, B, C)
QUOTE_OK = 1.65


def test_parse_pipe_and_free_line():
    row = parse_leg_arg("Cam Skattebo|Rush Attempts|OVER|9.5|Goblin|NYG")
    assert row["player"] == "Cam Skattebo"
    assert row["prop"] == "Rush Attempts"
    assert row["side"] == "OVER"
    assert row["line"] == 9.5
    assert row["rung"] == "goblin"
    assert row["team"] == "NYG"
    free = parse_legs("Cam Skattebo Rush Attempts Over 9.5 Goblin NYG")
    assert len(free) == 1
    assert free[0]["player"] == "Cam Skattebo"
    assert free[0]["line"] == 9.5
    assert free[0]["rung"] == "goblin"


def test_parse_block_paste():
    text = "\n".join(
        [
            "Jayson Tatum",
            "Points",
            "Over 20.5 Goblin",
            "BOS",
            "Josh Hart",
            "Rebounds",
            "Over 8.5 Standard",
            "NYK",
        ]
    )
    rows = parse_legs(text)
    assert len(rows) == 2
    assert rows[0]["player"] == "Jayson Tatum"
    assert rows[1]["player"] == "Josh Hart"
    assert rows[1]["rung"] == "standard"


def test_does_not_import_frozen_nfl_tracks():
    import inspect

    import ml.slip_check as sc

    src = inspect.getsource(sc)
    assert "nfl_std_under_shadow" not in src
    assert "nfl_demon_volume_shadow" not in src
    assert "nfl_demon_rec_floor_shadow" not in src


def test_screenshot_is_not_implemented():
    assert screenshot_message("slip.png") == SCREENSHOT_MSG
    assert screenshot_message("legs.txt") == ""


def test_six_leg_power_fails_max():
    extra = [
        _leg("A", team="BOS"),
        _leg("B", team="NYK"),
        _leg("C", team="MIN"),
        _leg("D", team="CHI"),
        _leg("E", team="MIA"),
        _leg("F", team="DAL"),
    ]
    rec = check_slip(extra, listed=extra, quote=QUOTE_OK, write=False)
    assert rec["verdict"] == "FAIL"
    assert rec["placed"] is False
    assert any("3 legs max" in x for x in rec["fails"])


def test_four_same_team_fails():
    tor = [
        _leg("A", team="TOR"),
        _leg("B", team="TOR"),
        _leg("C", team="TOR"),
        _leg("D", team="TOR"),
    ]
    rec = check_slip(tor, listed=tor, quote=QUOTE_OK, write=False)
    assert rec["verdict"] == "FAIL"
    assert any("three different teams" in x for x in rec["fails"])
    assert any("3 legs max" in x for x in rec["fails"])


def test_three_same_team_fails_even_at_three_legs():
    tor = [
        _leg("A", team="TOR"),
        _leg("B", team="TOR"),
        _leg("C", team="TOR"),
    ]
    rec = check_slip(tor, listed=tor, quote=QUOTE_OK, write=False)
    assert rec["verdict"] == "FAIL"
    assert any("three different teams" in x for x in rec["fails"])
    assert not any("3 legs max" in x for x in rec["fails"])


def test_leg_not_on_cleaned_or_gift_scan_fails():
    rec = check_slip(
        [A, B, _leg("Random", team="CHI")],
        listed=LISTED,
        gifts=[],
        quote=QUOTE_OK,
        write=False,
    )
    assert rec["verdict"] == "FAIL"
    assert any("cleaned / gift-scan" in x and "Random" in x for x in rec["fails"])


def test_flagged_and_after_start_gifts_are_not_submittable():
    flagged = _gift(A, all_pass=False, published_after_start=False)
    hindsight = _gift(B, all_pass=True, published_after_start=True)
    assert is_clean_gift(flagged) is False
    assert is_clean_gift(hindsight) is False
    rec = check_slip(
        [A, B, C],
        listed=[],
        gifts=[flagged, hindsight, _gift(C)],
        quote=QUOTE_OK,
        write=False,
    )
    assert rec["verdict"] == "FAIL"
    assert any("cleaned / gift-scan" in x for x in rec["fails"])


def test_clean_gift_counts_as_listed():
    rec = check_slip(
        [A, B, C],
        listed=[],
        gifts=[_gift(A), _gift(B), _gift(C)],
        quote=QUOTE_OK,
        write=False,
    )
    assert rec["verdict"] == "PASS"


def test_unsupported_prop_fails():
    ypc = _leg("Jones", "YPC", team="KC", sport="NFL", line=4.5, rung="standard")
    rec = check_slip(
        [A, B, ypc],
        listed=_listed(A, B, ypc),
        quote=QUOTE_OK,
        write=False,
    )
    assert rec["verdict"] == "FAIL"
    assert any("unsupported prop" in x and "YPC" in x for x in rec["fails"])
    assert "YPC" in unsupported_reason(ypc)
    fantasy = _leg("Hart", "Fantasy Score", team="NYK")
    assert "fantasy" in unsupported_reason(fantasy)
    demon_u = _leg("X", "Points", side="UNDER", rung="demon", team="BOS")
    assert "Demon UNDER" in unsupported_reason(demon_u)


def test_quote_below_floor_fails():
    rec = check_slip([A, B, C], listed=LISTED, quote=1.2, write=False)
    assert rec["verdict"] == "FAIL"
    assert any("1.2" in x and "1.4" in x for x in rec["fails"])


def test_missing_real_scrape_fails_closed():
    rec = check_slip([A, B, C], listed=LISTED, quote=None, write=False)
    assert rec["verdict"] == "FAIL"
    assert any("missing real scrape" in x for x in rec["fails"])
    fake = check_slip(
        [A, B, C],
        listed=LISTED,
        quote={"payout_x": 1.65, "payout_source": "mix_floor", "captured_at": "2026-10-25"},
        write=False,
    )
    assert fake["verdict"] == "FAIL"
    assert any("missing real scrape" in x for x in fake["fails"])


def test_three_leg_three_team_listed_quoted_passes():
    rec = check_slip([A, B, C], listed=LISTED, quote=QUOTE_OK, write=False)
    assert rec["verdict"] == "PASS"
    assert rec["fails"] == []
    assert rec["placed"] is False
    assert rec["quote"] == QUOTE_OK
    assert format_result(rec) == "PASS"


def test_log_observe_only(tmp_path: Path, monkeypatch):
    dest = tmp_path / "data" / "reports" / "gift_scan" / "slip_check.jsonl"
    rec = check_slip(
        [A, B, C],
        listed=LISTED,
        quote=QUOTE_OK,
        write=True,
        repo=tmp_path,
    )
    assert rec["placed"] is False
    assert dest.is_file()
    line = dest.read_text(encoding="utf-8").strip()
    assert '"placed": false' in line
    assert '"verdict": "PASS"' in line


def test_tonight_six_pick_fails_max_and_list():
    """Afternoon/tonight 6-pick shape: 6 legs, Demons, not on tonight's clean gifts."""
    slip = [
        _leg("Andre Szmyt", "fg_made", team="CLE", sport="NFL", line=1.5, rung="standard"),
        _leg("Eddy Pineiro", "kicking_points", team="SF", sport="NFL", line=7.5, rung="standard"),
        _leg("Cam Skattebo", "rush_attempts", team="NYG", sport="NFL", line=9.5, rung="demon"),
        _leg("Malik Washington", "receiving_yards", team="MIA", sport="NFL", line=39.5, rung="standard"),
        _leg("Tre Tucker", "receptions", team="LV", sport="NFL", line=3.5, rung="demon"),
        _leg("Josh Jacobs", "rushing_yards", team="GB", sport="NFL", line=68.5, rung="standard"),
    ]
    rec = check_slip(slip, listed=[], gifts=[], quote=None, write=False)
    assert rec["verdict"] == "FAIL"
    assert any("3 legs max" in x for x in rec["fails"])
    assert any("cleaned / gift-scan" in x for x in rec["fails"])
    assert any("missing real scrape" in x for x in rec["fails"])


REAL_QUOTE = {
    "payout_x": 1.65,
    "payout_source": "live_cdp",
    "captured_at": "2026-10-25T00:00:00+00:00",
}


def test_check_only_stays_placed_false():
    rec = check_slip([A, B, C], listed=LISTED, quote=QUOTE_OK, write=False)
    assert rec["placed"] is False
    rec2 = check_slip([A, B, C], listed=LISTED, quote=REAL_QUOTE, write=False)
    assert rec2["verdict"] == "PASS"
    assert rec2["placed"] is False


def test_place_without_real_quote_fails():
    missing = check_slip([A, B, C], listed=LISTED, quote=None, place=True, write=False)
    assert missing["placed"] is False
    assert missing["verdict"] == "FAIL"
    assert PLACE_FAIL in missing["fails"]

    bare = check_slip([A, B, C], listed=LISTED, quote=QUOTE_OK, place=True, write=False)
    assert bare["placed"] is False
    assert PLACE_FAIL in bare["fails"]

    floor = check_slip(
        [A, B, C],
        listed=LISTED,
        quote={"payout_x": 1.65, "payout_source": "mix_floor", "captured_at": "2026-10-25"},
        place=True,
        write=False,
    )
    assert floor["placed"] is False
    assert PLACE_FAIL in floor["fails"]

    first = check_slip(
        [A, B, C],
        listed=LISTED,
        quote={"payout_x": 25.0, "payout_source": "first_place", "captured_at": "2026-10-25"},
        place=True,
        write=False,
    )
    assert first["placed"] is False
    assert PLACE_FAIL in first["fails"]


def test_place_with_live_cdp_writes_placed_true(tmp_path: Path):
    dest = tmp_path / "data" / "reports" / "gift_scan" / "slip_check.jsonl"
    rec = check_slip(
        [A, B, C],
        listed=LISTED,
        quote=REAL_QUOTE,
        place=True,
        write=True,
        repo=tmp_path,
    )
    assert rec["placed"] is True
    assert rec["verdict"] == "PASS"
    assert rec["quote"] == 1.65
    assert rec["quote_meta"]["payout_source"] == "live_cdp"
    cap = str(rec["quote_meta"]["captured_at"] or "")
    assert cap and cap != REAL_QUOTE["captured_at"]
    assert "T" in cap
    assert dest.is_file()
    line = dest.read_text(encoding="utf-8").strip()
    assert '"placed": true' in line
    assert "live_cdp" in line
    assert "1.65" in line
    logged = json.loads(line)
    assert logged["placed"] is True
    assert logged["captured_at"] == cap


def test_cli_place_without_quote_fails(capsys):
    rc = main(
        [
            "--place",
            "--no-boards",
            "--no-log",
            "--json",
            "--legs",
            "Tatum|Points|OVER|20.5|Goblin|BOS",
            "--legs",
            "Brunson|Assists|OVER|6.5|Goblin|NYK",
            "--legs",
            "Edwards|Rebounds|OVER|5.5|Goblin|MIN",
        ]
    )
    assert rc == 2
    out = json.loads(capsys.readouterr().out)
    assert out["placed"] is False
    assert PLACE_FAIL in (out.get("fails") or [])


def test_cli_place_with_live_cdp(capsys):
    main(
        [
            "--place",
            "--quote",
            "1.65",
            "--source",
            "live_cdp",
            "--no-boards",
            "--no-log",
            "--json",
            "--legs",
            "Tatum|Points|OVER|20.5|Goblin|BOS",
            "--legs",
            "Brunson|Assists|OVER|6.5|Goblin|NYK",
            "--legs",
            "Edwards|Rebounds|OVER|5.5|Goblin|MIN",
        ]
    )
    out = json.loads(capsys.readouterr().out)
    assert out["placed"] is True
    assert out["quote"] == 1.65
    assert (out.get("quote_meta") or {}).get("payout_source") == "live_cdp"
    cap = str((out.get("quote_meta") or {}).get("captured_at") or "")
    assert cap and "T" in cap
