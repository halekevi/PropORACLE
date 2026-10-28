"""Demon value scanner: alert only, per-slip implied_p, Wilson bar."""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

from utils.demon_value import (
    CUSHION,
    FORMULA_DIFFERENT_GAME_BELOW,
    FORMULA_SAME_GAME_CONFOUNDED,
    PROOF_N,
    REED_AUSAR_QUOTE,
    SKIP_CUR_HR,
    SKIP_MINUTES_ROLE,
    SKIP_N_CUR,
    SKIP_TEAM_CHANGED,
    SKIP_WILSON_SCRAPE_FLOOR,
    STANDARD_P_REF,
    alert_bar,
    one_per_player,
    assert_prereg,
    evaluate_row,
    format_list_section,
    geometric_mean_p,
    grade_pending,
    implied_p_from_pair,
    load_reference,
    formula_ok_status,
    pair_is_settled,
    pair_is_unconnected,
    pick_standard_for_demon,
    pick_standards_for_demon,
    pick_two_demons_different_games,
    reference_p,
    refs_unconnected_from_demon,
    same_game,
    proof_report,
    quote_key,
    same_demon_two_standards,
    scan,
    scrape_candidates,
    scrape_prefilter,
    trust_mix_n_correct,
    trust_two_standard_n_correct,
    trusted_quote,
    two_demon_product_check,
    two_standard_sanity,
)


CUR = date(2025, 10, 22)
LAST = date(2024, 10, 22)
SLATE = "2026-10-27"


def _no_team(row, slate):
    return False, ""


def _games(n: int, hits: int, *, start: date = date(2025, 10, 25)) -> list[dict]:
    out = []
    h = 0
    for i in range(n):
        hit = h < hits
        if hit:
            h += 1
        out.append(
            {
                "game_date": (start + timedelta(days=i)).isoformat(),
                "actual": 2 if hit else 0,
                "minutes": 24,
                "window": "regular",
                "team": "BOS",
            }
        )
    return out


def _split_games(
    *,
    n_cur: int,
    hits_cur: int,
    n_last: int = 20,
    hits_last: int = 16,
    min_cur: float = 24.0,
    min_last: float = 24.0,
    team_cur: str = "BOS",
    team_last: str = "BOS",
) -> list[dict]:
    out = []
    h = 0
    for i in range(n_last):
        hit = h < hits_last
        if hit:
            h += 1
        out.append(
            {
                "game_date": (date(2024, 11, 1) + timedelta(days=i)).isoformat(),
                "actual": 2 if hit else 0,
                "minutes": min_last,
                "window": "regular",
                "team": team_last,
            }
        )
    h = 0
    for i in range(n_cur):
        hit = h < hits_cur
        if hit:
            h += 1
        out.append(
            {
                "game_date": (date(2025, 10, 25) + timedelta(days=i)).isoformat(),
                "actual": 2 if hit else 0,
                "minutes": min_cur,
                "window": "regular",
                "team": team_cur,
            }
        )
    return out


def _pass_games() -> list[dict]:
    """Clears STEP 0: n>=15, n_cur>=3, cur_hr>=0.60, Wilson low>=0.50, same team, minutes."""
    return _split_games(n_cur=20, hits_cur=16, n_last=20, hits_last=16)


def _demon(**kw):
    rec = {
        "sport": "NBA",
        "player": "Test Player",
        "team": "BOS",
        "prop": "3PTM",
        "line": 0.5,
        "pick_type": "Demon",
        "side": "OVER",
        "minutes_proj": 28,
        "game_date": SLATE,
        "matchup": "BOS vs BKN",
    }
    rec.update(kw)
    return rec


def _ref(**kw):
    rec = {
        "player": "Ref Leg",
        "prop": "3PTM",
        "line": 0.5,
        "pick_type": "demon",
        "team": "NYK",
        "implied_p": 0.40,
        "lineup_type": "power",
    }
    rec.update(kw)
    return rec


def _quote(power_x: float, **kw):
    rec = {
        "power_x": power_x,
        "to_win": 20.0 * power_x,
        "entry": 20.0,
        "lineup_type": "power",
        "reversion_disclaimer": True,
        "quote_trusted": True,
    }
    rec.update(kw)
    return rec


def test_one_per_player_keeps_highest_wilson():
    rows = [
        {"player": "Tyrese Maxey", "prop": "FG Attempted", "line": 17.5, "wilson_lo": 0.644},
        {"player": "Andrew Wiggins", "prop": "Offensive Rebounds", "line": 0.5, "wilson_lo": 0.636},
        {"player": "Tyrese Maxey", "prop": "Points", "line": 25.5, "wilson_lo": 0.566},
        {"player": "Zion Williamson", "prop": "Pts+Rebs", "line": 24.5, "wilson_lo": 0.553},
        {"player": "Zion Williamson", "prop": "Points", "line": 19.5, "wilson_lo": 0.538},
    ]
    got = one_per_player(rows)
    assert [(r["player"], r["prop"], r["line"]) for r in got] == [
        ("Tyrese Maxey", "FG Attempted", 17.5),
        ("Andrew Wiggins", "Offensive Rebounds", 0.5),
        ("Zion Williamson", "Pts+Rebs", 24.5),
    ]


def test_prereg_locked():
    spec = assert_prereg()
    assert spec["alert_only"] is True
    assert spec["never_ticket"] is True
    assert spec["placed_default"] is False
    assert spec["ticketed"] is False
    assert spec["never_submit"] is True
    assert abs(float(spec["p_ref"]) - STANDARD_P_REF) < 0.002


def test_standard_2leg_is_577():
    assert abs(STANDARD_P_REF - (3.0 ** -0.5)) < 1e-12
    rec = two_standard_sanity(3.0)
    assert rec["ok"] is True
    assert abs(rec["implied_p"] - rec["expected_p"]) < 1e-9
    assert round(rec["implied_p"] * 100, 1) == 57.7
    p = implied_p_from_pair(3.0, STANDARD_P_REF)
    assert p is not None
    assert abs(p - STANDARD_P_REF) < 1e-12
    ref = load_reference()
    assert abs(float(reference_p(ref)) - STANDARD_P_REF) < 1e-9
    # A 3.5x "Balance as To Win" sanity must fail.
    assert two_standard_sanity(3.5)["ok"] is False
    blank_to_win = trust_two_standard_n_correct(
        {"n_correct_raw": 3.0, "quote_trusted": False, "power_x": None}
    )
    assert blank_to_win is not None
    assert blank_to_win["power_x"] == 3.0
    assert two_standard_sanity(blank_to_win["power_x"])["ok"] is True
    mix = trust_mix_n_correct(
        {"n_correct_raw": 3.25, "first_place_x": 3.0, "quote_trusted": False, "power_x": None}
    )
    assert mix is not None and mix["power_x"] == 3.25
    p_mix = implied_p_from_pair(3.25, STANDARD_P_REF)
    assert p_mix is not None
    assert 0.52 < p_mix < 0.54
    # 1st-place 3x without a distinct N-correct is not a mix quote.
    assert trust_mix_n_correct({"n_correct_raw": 3.0, "first_place_x": 3.0}) is None


def test_standard_must_be_other_team():
    stds = [
        {
            "player": "Home Std",
            "team": "BOS",
            "sport": "NBA",
            "pick_type": "Standard",
            "side": "OVER",
            "prop": "Points",
            "line": 20.5,
        },
        {
            "player": "Away Std",
            "team": "NYK",
            "sport": "NBA",
            "pick_type": "Standard",
            "side": "OVER",
            "prop": "Points",
            "line": 18.5,
        },
    ]
    got = pick_standard_for_demon(_demon(team="BOS"), stds)
    assert got is not None
    assert got["team"] == "NYK"
    assert pick_standard_for_demon(_demon(team="BOS"), stds[:1]) is None


def test_implied_p_divides_out_reference():
    # Reed+Ausar 8.76x, equal-price bootstrap ~33.8%.
    gm = geometric_mean_p(8.76)
    assert gm is not None
    assert abs(gm - 8.76 ** -0.5) < 1e-9
    assert 0.337 < gm < 0.339
    p = implied_p_from_pair(8.76, 0.40)
    assert p is not None
    assert abs(p - 1 / (8.76 * 0.40)) < 1e-9
    # Standard reference: implied_p = 1 / (quote × 0.577).
    p_std = implied_p_from_pair(8.76, STANDARD_P_REF)
    assert p_std is not None
    assert abs(p_std - 1 / (8.76 * STANDARD_P_REF)) < 1e-9


def test_same_demon_two_standards_must_match():
    rec = same_demon_two_standards(3.25, 3.25)
    assert rec["ok"] is True
    assert rec["delta"] == 0.0
    assert same_demon_two_standards(3.25, 3.5)["ok"] is False
    assert same_demon_two_standards(3.25, None)["ok"] is False


def test_two_demon_product_reed_ausar():
    p = REED_AUSAR_QUOTE ** -0.5
    rec = two_demon_product_check(REED_AUSAR_QUOTE, p, p)
    assert rec["ok"] is True
    assert rec["below_product"] is False
    assert abs(rec["expected"] - REED_AUSAR_QUOTE) < 1e-9
    assert two_demon_product_check(12.0, p, p)["ok"] is False
    # Two 3.25x Demons vs Standard → implied 0.5329 each → ~3.52x together.
    p_std = implied_p_from_pair(3.25, STANDARD_P_REF)
    mix = two_demon_product_check(3.5, p_std, p_std)
    assert mix["ok"] is True
    low = two_demon_product_check(5.0, p_std, p_std)
    assert low["ok"] is False
    # 9x vs 10x expected is below the product, not rounding.
    reed = two_demon_product_check(9.0, 0.433013, 0.23094)
    assert reed["below_product"] is True
    assert formula_ok_status(different_game=None) == FORMULA_SAME_GAME_CONFOUNDED
    assert formula_ok_status(different_game=reed) == FORMULA_DIFFERENT_GAME_BELOW


def test_two_demons_must_be_different_games():
    reed = {
        "player": "Paul Reed",
        "team": "DET",
        "opp_team": "NYK",
        "matchup": "NYK vs DET",
        "pp_game_id": "190275",
        "pick_type": "demon",
        "prop": "3PTM",
        "line": 0.5,
    }
    ausar = {
        "player": "Ausar Thompson",
        "team": "DET",
        "opp_team": "NYK",
        "matchup": "NYK vs DET",
        "pp_game_id": "190275",
        "pick_type": "demon",
        "prop": "3PTM",
        "line": 0.5,
    }
    wemby = {
        "player": "Victor Wembanyama",
        "team": "SAS",
        "opp_team": "BOS",
        "matchup": "BOS vs SAS",
        "pp_game_id": "190280",
        "pick_type": "demon",
        "prop": "Turnovers",
        "line": 2.5,
    }
    assert same_game(reed, ausar) is True
    assert same_game(reed, wemby) is False
    assert pick_two_demons_different_games([reed, ausar]) is None
    pair = pick_two_demons_different_games([reed, ausar, wemby])
    assert pair is not None
    names = {pair[0]["player"], pair[1]["player"]}
    assert "Victor Wembanyama" in names
    assert names != {"Paul Reed", "Ausar Thompson"}


def test_standard_skips_same_game():
    towns = {
        "player": "Karl-Anthony Towns",
        "team": "NYK",
        "opp_team": "DET",
        "matchup": "NYK vs DET",
        "pp_game_id": "190275",
        "sport": "NBA",
        "pick_type": "Demon",
        "side": "OVER",
        "prop": "Turnovers",
        "line": 2.5,
    }
    robinson = {
        "player": "Duncan Robinson",
        "team": "DET",
        "opp_team": "NYK",
        "matchup": "NYK vs DET",
        "pp_game_id": "190275",
        "sport": "NBA",
        "pick_type": "Standard",
        "side": "OVER",
        "prop": "Points",
        "line": 12.5,
    }
    harper = {
        "player": "Dylan Harper",
        "team": "SAS",
        "opp_team": "BOS",
        "matchup": "BOS vs SAS",
        "pp_game_id": "190280",
        "sport": "NBA",
        "pick_type": "Standard",
        "side": "OVER",
        "prop": "Points",
        "line": 17.5,
    }
    assert same_game(towns, robinson) is True
    got = pick_standard_for_demon(towns, [robinson, harper])
    assert got is not None
    assert got["player"] == "Dylan Harper"
    assert pick_standard_for_demon(towns, [robinson]) is None


def _nba_leg(player, team, opp, gid, *, pick="standard", prop="Points", line=12.5):
    return {
        "player": player,
        "team": team,
        "opp": opp,
        "opp_team": opp,
        "matchup": f"{team} vs {opp}",
        "pp_game_id": gid,
        "sport": "NBA",
        "pick_type": pick,
        "side": "OVER",
        "prop": prop,
        "line": line,
    }


def test_unconnected_pair_needs_four_games():
    towns = _nba_leg("Karl-Anthony Towns", "NYK", "DET", "190275", pick="demon", prop="Turnovers", line=2.5)
    towns["minutes_proj"] = 32
    robinson = _nba_leg("Duncan Robinson", "DET", "NYK", "190275")
    brown = _nba_leg("Mikel Brown", "BKN", "BOS", "190273", pick="demon", prop="3PTM", line=1.5)
    harper = _nba_leg("Dylan Harper", "SAS", "SAC", "190274")
    morant = _nba_leg("Ja Morant", "POR", "LAL", "190512", prop="Assists", line=6.0)
    assert same_game(towns, robinson) is True
    assert pair_is_unconnected(towns, brown, robinson, harper) is False
    assert pair_is_unconnected(towns, brown, morant, harper) is True
    assert refs_unconnected_from_demon(towns, robinson, morant) is False
    assert refs_unconnected_from_demon(towns, morant, harper) is True
    connected = {
        "unconnected": False,
        "ok": True,
        "quote_ab": 3.5,
        "quote_a_vs_std": 3.25,
        "quote_b_vs_std": 3.25,
    }
    complete = {
        "unconnected": True,
        "ok": True,
        "quote_ab": 3.5,
        "quote_a_vs_std": 3.25,
        "quote_b_vs_std": 3.25,
    }
    assert pair_is_settled(connected) is False
    assert pair_is_settled(complete) is True
    # Towns + Robinson is same-game even though they are two teams.
    rec = evaluate_row(
        towns,
        date_str=SLATE,
        quote=_quote(
            3.25,
            ref_leg={
                "player": "Duncan Robinson",
                "team": "DET",
                "opp": "NYK",
                "pp_game_id": "190275",
                "pick_type": "standard",
            },
        ),
        reference=_ref(team="DET"),
        games=_games(40, 20),
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=_no_team,
    )
    assert rec["implied_p"] is None
    assert rec["qualified"] is False
    assert "ref_same_game" in rec["why"]
    assert "same_game_as_ref" in rec["flags"]


def test_two_standards_for_same_demon():
    stds = [
        {"player": "A", "team": "BOS", "sport": "NBA", "pick_type": "Standard", "side": "OVER", "prop": "Points", "line": 20.5},
        {"player": "B", "team": "NYK", "sport": "NBA", "pick_type": "Standard", "side": "OVER", "prop": "Points", "line": 18.5},
        {"player": "C", "team": "DET", "sport": "NBA", "pick_type": "Standard", "side": "OVER", "prop": "Points", "line": 12.5},
    ]
    got = pick_standards_for_demon(_demon(team="BOS"), stds, n=2)
    assert len(got) == 2
    assert {r["team"] for r in got} == {"NYK", "DET"}


def test_scraper_source_never_clicks_submit():
    import re

    src = Path("scripts/scan_demon_value.py").read_text(encoding="utf-8")
    assert "Never presses Submit" in src or "never presses Submit" in src
    assert "clear_slip" in src
    assert not re.search(r"click\([^\)]*Submit", src)
    assert "Submit Lineup" in src  # named only so we never click it
    # STEP 0 runs scrape_candidates before any CDP scrape_pair_quotes.
    main = src.split("def main")[-1]
    assert "scrape_candidates(" in main
    assert main.index("scrape_candidates(") < main.index("scrape_pair_quotes(")


def test_no_quote_fail_closed():
    rec = evaluate_row(
        _demon(),
        date_str=SLATE,
        quote=None,
        reference=_ref(),
        games=_games(40, 20),
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=_no_team,
    )
    assert rec["qualified"] is False
    assert rec["placed"] is False
    assert rec["ticketed"] is False
    assert "no_trusted_2leg_quote" in rec["why"]


def test_untrusted_quote_skipped():
    rec = evaluate_row(
        _demon(),
        date_str=SLATE,
        quote={"power_x": 3.0, "quote_trusted": False, "first_place_used": True},
        reference=_ref(),
        games=_games(40, 20),
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=_no_team,
    )
    assert rec["qualified"] is False
    assert trusted_quote({"power_x": 3.0, "quote_trusted": False}) is None


def test_tonight_0p5_threes_do_not_alert():
    """Reed 18.5% n=65 vs ~33.8% implied. Well under the 15% cushion."""
    p_ref = geometric_mean_p(8.76)
    rec = evaluate_row(
        _demon(player="Paul Reed", team="DET", minutes_proj=22),
        date_str=SLATE,
        quote=_quote(8.76),
        reference=_ref(implied_p=p_ref, team="NYK"),
        games=_games(65, 12),  # 18.5%
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=_no_team,
    )
    assert rec["n"] == 65
    assert rec["implied_p"] is not None
    assert rec["wilson_low"] < rec["implied_p"] * CUSHION
    assert rec["qualified"] is False
    assert "wilson_low_below_implied_cushion" in rec["why"]


def test_wilson_bar_alerts_when_clearly_above_price(tmp_path: Path):
    # STEP 0 passers (cur_hr 80%, Wilson lo ~0.65) vs implied 25% * 1.15 = 28.7%.
    row = _demon(player="Value Demon")
    quotes = {quote_key(row): _quote(10.0)}  # 1/(10*0.40) = 0.25
    res = scan(
        date_str=SLATE,
        board=[row],
        quotes=quotes,
        reference=_ref(implied_p=0.40),
        games_by_player={"valuedemon": _pass_games()},
        alert=False,
        write=True,
        team_changed_fn=_no_team,
        current_opener=CUR,
        last_opener=LAST,
        log_path=tmp_path / "demon_value_forward.jsonl",
        latest_path=tmp_path / "demon_value_latest.json",
    )
    assert res["ticketed"] is False
    assert res["n_alerts"] == 1
    alert = res["alerts"][0]
    assert alert["placed"] is False
    assert alert["qualified"] is True
    assert alert_bar(alert["wilson_low"], alert["implied_p"]) is True
    log = (tmp_path / "demon_value_forward.jsonl").read_text(encoding="utf-8")
    assert "Value Demon" in log
    assert '"placed": false' in log.lower() or '"placed": false' in log


def test_n_under_15_skips():
    rec = evaluate_row(
        _demon(),
        date_str=SLATE,
        quote=_quote(10.0),
        reference=_ref(),
        games=_games(14, 10),
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=_no_team,
    )
    assert rec["qualified"] is False
    assert "n<15" in rec["why"]


def test_injury_and_unconfirmed_minutes_skip():
    rec = evaluate_row(
        _demon(injury_status="Q", minutes_proj=28),
        date_str=SLATE,
        quote=_quote(10.0),
        reference=_ref(),
        games=_games(80, 40),
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=_no_team,
    )
    assert rec["qualified"] is False
    assert "injury_tag" in rec["why"]
    rec2 = evaluate_row(
        _demon(minutes_proj=8, minutes_tier="LOW"),
        date_str=SLATE,
        quote=_quote(10.0),
        reference=_ref(),
        games=_games(80, 40),
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=_no_team,
    )
    assert rec2["qualified"] is False
    assert "role_or_minutes_unconfirmed" in rec2["why"]


def test_same_team_flags_does_not_block():
    rec = evaluate_row(
        _demon(player="Value Demon", team="NYK"),
        date_str=SLATE,
        quote=_quote(10.0),
        reference=_ref(implied_p=0.40, team="NYK"),
        games=_games(80, 40),
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=_no_team,
    )
    assert rec["qualified"] is True
    assert "same_team_as_ref" in rec["flags"]
    assert "same_game_as_ref" not in rec["flags"]
    assert rec["implied_p"] is not None


def test_team_change_blocks():
    rec = evaluate_row(
        _demon(),
        date_str=SLATE,
        quote=_quote(10.0),
        reference=_ref(),
        games=_games(80, 40),
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=lambda row, slate: (True, "team_changed:DET->BOS"),
    )
    assert rec["qualified"] is False
    assert "team_changed_last_5" in rec["why"]


def test_demon_under_dropped():
    rec = evaluate_row(
        _demon(side="UNDER"),
        date_str=SLATE,
        quote=_quote(10.0),
        reference=_ref(),
        games=_games(80, 40),
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=_no_team,
    )
    assert rec["qualified"] is False
    assert "demon_under_invalid" in rec["why"]


def test_fifty_alerts_turn_off_when_they_miss_price():
    alerts = []
    for i in range(PROOF_N):
        alerts.append(
            {
                "qualified": True,
                "result": "hit" if i < 10 else "miss",  # 20%
                "implied_p": 0.30,
            }
        )
    proof = proof_report(alerts)
    assert proof["ready"] is True
    assert proof["beats_price"] is False
    assert proof["scanner_enabled"] is False


def test_fifty_alerts_stay_on_when_they_beat_price():
    alerts = []
    for i in range(PROOF_N):
        alerts.append(
            {
                "qualified": True,
                "result": "hit" if i < 30 else "miss",  # 60%
                "implied_p": 0.25,
            }
        )
    proof = proof_report(alerts)
    assert proof["ready"] is True
    assert proof["beats_price"] is True
    assert proof["scanner_enabled"] is True


def test_grade_pending_fills_hit_miss(tmp_path: Path):
    path = tmp_path / "fwd.jsonl"
    row = _demon(player="Value Demon")
    quotes = {quote_key(row): _quote(10.0)}
    scan(
        date_str=SLATE,
        board=[row],
        quotes=quotes,
        reference=_ref(implied_p=0.40),
        games_by_player={"valuedemon": _pass_games()},
        alert=False,
        write=True,
        team_changed_fn=_no_team,
        current_opener=CUR,
        last_opener=LAST,
        log_path=path,
        latest_path=tmp_path / "latest.json",
    )
    rec = grade_pending(
        log_path=path,
        actuals={quote_key(row): 2.0},
        as_of=date(2026, 10, 28),
    )
    assert rec["n_graded"] == 1
    logged = path.read_text(encoding="utf-8")
    assert '"result": "hit"' in logged
    assert '"placed": false' in logged


def test_quote_key_aliases_3ptm():
    a = quote_key({"sport": "NBA", "player": "Mikel Brown", "prop": "3-PT Made", "line": 1.5})
    b = quote_key({"sport": "nba", "player": "Mikel Brown", "prop_type": "3PTM", "line": 1.5})
    assert a == b
    assert a.endswith("|3ptm|1.5")


def test_opener_pair_keeps_last_season():
    from utils.demon_value import _opener_pair

    cur, last = _opener_pair(date(2026, 10, 27))
    assert last < cur
    assert cur.year == 2026
    assert last.year == 2025


def test_jr_suffix_matches_box_name():
    import pandas as pd

    from utils.demon_value import nba_games_for_player

    boxes = pd.DataFrame(
        {
            "_pn": ["mikel brown jr."],
            "game_date": ["2025-11-01"],
            "fg3m": [2],
            "minutes": [24],
            "window": ["regular"],
            "team": ["BKN"],
        }
    )
    games = nba_games_for_player(boxes, "Mikel Brown", prop="3PTM", as_of=date(2026, 10, 27))
    assert len(games) == 1
    assert games[0]["actual"] == 2


def test_recent_minutes_confirm_role_when_board_blank():
    rec = evaluate_row(
        _demon(minutes_proj=None, minutes=None, minutes_tier="1.0"),
        date_str=SLATE,
        quote=_quote(10.0),
        reference=_ref(implied_p=0.40, team="NYK"),
        games=_games(80, 40),
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=_no_team,
    )
    assert rec["role_confirmed"] is True
    assert rec["qualified"] is True


def test_list_section_empty_when_none_qualify():
    text = format_list_section(
        {"date": SLATE, "scanner_enabled": True, "alerts": [], "n_skipped": 4},
        date=SLATE,
    )
    assert "Demon value" in text
    assert "not a ticket" in text
    assert "none cleared" in text


def test_balance_quote_is_not_trusted():
    # 1st-place 3x / Balance $652.41 must not become power_x.
    assert trusted_quote({"power_x": None, "to_win": 652.41, "quote_trusted": False}) is None
    assert trusted_quote({"first_place_used": True, "power_x": 3.0}) is None


def _pre(row=None, games=None, **kw):
    return scrape_prefilter(
        row or _demon(),
        games=games if games is not None else _pass_games(),
        date_str=SLATE,
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=kw.get("team_changed_fn", _no_team),
    )


def test_scrape_prefilter_passes_stable_high_hr():
    pre = _pre()
    assert pre["ok"] is True
    assert pre["why"] == []
    assert pre["n_cur"] >= 3
    assert pre["cur_hr"] >= 0.60
    assert pre["wilson_low"] >= 0.50
    assert pre["team_changed"] is False


def test_scrape_prefilter_wilson_floor():
    # 12/20 = 60% current HR, Wilson low ~0.39 — cannot beat cheapest implied × 1.15.
    pre = _pre(games=_split_games(n_cur=20, hits_cur=12, n_last=0, hits_last=0))
    assert pre["ok"] is False
    assert SKIP_WILSON_SCRAPE_FLOOR in pre["why"]
    assert SKIP_N_CUR not in pre["why"]
    assert SKIP_CUR_HR not in pre["why"]


def test_scrape_prefilter_n_cur():
    pre = _pre(games=_split_games(n_cur=2, hits_cur=2, n_last=20, hits_last=20))
    assert pre["ok"] is False
    assert SKIP_N_CUR in pre["why"]


def test_scrape_prefilter_cur_hr():
    pre = _pre(games=_split_games(n_cur=10, hits_cur=5, n_last=20, hits_last=20))
    assert pre["ok"] is False
    assert SKIP_CUR_HR in pre["why"]
    assert SKIP_N_CUR not in pre["why"]


def test_scrape_prefilter_drops_team_changed_jaquez():
    """Permanent filter drops last-season team != board team (Jaquez MIA→MIL)."""
    row = _demon(player="Jaime Jaquez", team="MIL")
    games = _split_games(
        n_cur=20, hits_cur=16, n_last=20, hits_last=16, team_cur="MIL", team_last="MIA"
    )
    pre = _pre(row, games)
    assert pre["ok"] is False
    assert SKIP_TEAM_CHANGED in pre["why"]
    assert pre["team_changed"] is True


def test_scrape_prefilter_drops_team_changed_derozan():
    row = _demon(player="DeMar DeRozan", team="DEN")
    games = _split_games(
        n_cur=20, hits_cur=16, n_last=20, hits_last=16, team_cur="DEN", team_last="SAC"
    )
    pre = _pre(row, games)
    assert pre["ok"] is False
    assert SKIP_TEAM_CHANGED in pre["why"]


def test_scrape_prefilter_minutes_role():
    pre = _pre(
        games=_split_games(
            n_cur=20, hits_cur=16, n_last=20, hits_last=16, min_cur=20.0, min_last=32.0
        )
    )
    assert pre["ok"] is False
    assert SKIP_MINUTES_ROLE in pre["why"]


def test_scan_prefilter_skip_not_no_trusted_quote(tmp_path: Path):
    """Failing STEP 0 must not be lumped into no_trusted_2leg_quote."""
    low = _demon(player="Low Wilson")
    passer = _demon(player="Value Demon")
    res = scan(
        date_str=SLATE,
        board=[low, passer],
        quotes={},
        reference=_ref(implied_p=0.40),
        games_by_player={
            "lowwilson": _split_games(n_cur=20, hits_cur=12, n_last=0, hits_last=0),
            "valuedemon": _pass_games(),
        },
        alert=False,
        write=True,
        team_changed_fn=_no_team,
        current_opener=CUR,
        last_opener=LAST,
        log_path=tmp_path / "fwd.jsonl",
        latest_path=tmp_path / "latest.json",
    )
    assert res["n_scrape_candidates"] == 1
    assert res["skip_reasons"].get(SKIP_WILSON_SCRAPE_FLOOR, 0) == 1
    assert res["skip_reasons"].get("no_trusted_2leg_quote", 0) == 1
    by_player = {r["player"]: r for r in res["scanned"]}
    assert SKIP_WILSON_SCRAPE_FLOOR in by_player["Low Wilson"]["why"]
    assert "no_trusted_2leg_quote" not in by_player["Low Wilson"]["why"]
    assert "no_trusted_2leg_quote" in by_player["Value Demon"]["why"]


def test_scrape_candidates_drops_team_changed():
    jaquez = _demon(player="Jaime Jaquez", team="MIL")
    maxey = _demon(player="Tyrese Maxey", team="PHI")
    passers, skipped = scrape_candidates(
        [jaquez, maxey],
        date_str=SLATE,
        games_by_player={
            "jaimejaquez": _split_games(
                n_cur=20, hits_cur=16, n_last=20, hits_last=16, team_cur="MIL", team_last="MIA"
            ),
            "tyresemaxey": _split_games(
                n_cur=20, hits_cur=16, n_last=20, hits_last=16, team_cur="PHI", team_last="PHI"
            ),
        },
        current_opener=CUR,
        last_opener=LAST,
        team_changed_fn=_no_team,
    )
    assert [r["player"] for r in passers] == ["Tyrese Maxey"]
    assert any(SKIP_TEAM_CHANGED in str(r.get("why")) for r in skipped)
