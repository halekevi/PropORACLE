"""Tennis Abstract matchmx parse — aces / DF / break points, never missing→0."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "Sports" / "Tennis" / "scripts"))

from tennis_shared import (  # noqa: E402
    collect_history_values,
    match_is_qualifying,
    ta_player_side_from_score,
)
from tennis_ta_matchlog import (  # noqa: E402
    backfill_score_fields,
    needs_ta_matchlog,
    parse_js_quoted_array,
    parse_player_page,
    ta_index_key,
    ta_page_url,
    ta_player_fresh,
    ta_slug,
    ta_slug_candidates,
)


_FIXTURE = r"""
var matchhead = ["date","tourn","surf","level","wl","rank","seed","entry","round",
                 "score","max","opp","orank","oseed","oentry","ohand","obday",
                 "oht","ocountry","oactive","time","aces","dfs","pts","firsts","fwon",
                 "swon",'games',"saved","chances","oaces","odfs","opts","ofirsts",
                 "ofwon","oswon",'ogames',"osaved","ochances", "obackhand", "chartlink",
                 "pslink","whserver","matchid","wh","roundnum","matchnum"]
var matchmx = [["20260823", "ATP Winston-Salem", "Hard", "A", "L", "64", "", "", "R32", "W/O", "3", "Fabian Marozsan", "63", "", "", "R", "19990101", "180", "HUN", "1", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "2", "2"],
        ["20260823", "ATP Winston-Salem", "Hard", "A", "W", "64", "", "", "R64", "6-3 7-6(4)", "3", "Sho Shimabukuro", "135", "", "", "R", "19970101", "183", "JPN", "1", "1:32", "3", "6", "80", "50", "30", "10", "10", "0", "2", "4", "2", "70", "40", "20", "8", "9", "3", "7", "", "", "", "", "", "", "1", "1"],
        ["20260813", "Cincinnati Masters", "Hard", "M", "L", "64", "", "", "R64", "4-6 6-3 3-6", "3", "Flavio Cobolli", "22", "", "", "R", "20020501", "183", "ITA", "1", "2:01", "3", "0", "90", "55", "33", "12", "14", "7", "12", "8", "1", "88", "50", "30", "11", "13", "4", "8", "", "", "", "", "", "", "2", "2"],
        ["20260801", "Qual Event", "Hard", "A", "W", "64", "", "Q", "Q1", "6-1 6-2", "3", "Qual Opp", "300", "", "Q", "R", "19900101", "180", "USA", "1", "0:55", "9", "1", "40", "25", "20", "8", "8", "0", "0", "2", "0", "30", "15", "8", "6", "8", "1", "5", "", "", "", "", "", "", "0", "1"]]
"""


def test_needs_ta_matchlog_norms():
    assert needs_ta_matchlog("aces")
    assert needs_ta_matchlog("double_faults")
    assert needs_ta_matchlog("break_points_won")
    assert needs_ta_matchlog("", "Double Faults")
    assert needs_ta_matchlog("", "Break Points Won")
    assert not needs_ta_matchlog("games_won")
    assert not needs_ta_matchlog("match_total_games")


def test_ta_slug_strips_punctuation():
    assert ta_slug("Miomir Kecmanovic") == "MiomirKecmanovic"
    assert ta_slug("Christopher O'Connell") == "ChristopherOConnell"
    assert ta_slug("Iga Świątek") == "IgaSwiatek"
    assert ta_slug_candidates("Hanyu Guo") == ["HanyuGuo", "GuoHanyu"]
    assert ta_slug_candidates("Yeon Woo Ku") == ["YeonWooKu", "KuYeonWoo"]


def test_parse_mixed_quote_matchhead():
    head = parse_js_quoted_array(_FIXTURE, "matchhead")
    assert head[0] == "date"
    assert "aces" in head
    assert "dfs" in head
    assert "osaved" in head
    assert "ochances" in head
    assert "games" in head
    assert "ogames" in head


def test_walkover_and_missing_stats_are_none_not_zero():
    rows = parse_player_page(_FIXTURE)
    wo = next(r for r in rows if r["score"] == "W/O")
    assert wo["aces"] is None
    assert wo["double_faults"] is None
    assert wo["break_points_won"] is None


def test_zero_double_faults_kept():
    rows = parse_player_page(_FIXTURE)
    cin = next(r for r in rows if r["tourn"] == "Cincinnati Masters")
    assert cin["aces"] == 3.0
    assert cin["double_faults"] == 0.0
    assert cin["break_points_won"] == 4.0


def test_break_points_won_is_opponent_faced_minus_saved():
    rows = parse_player_page(_FIXTURE)
    r64 = next(r for r in rows if r["round"] == "R64" and r["tourn"] == "ATP Winston-Salem")
    assert r64["aces"] == 3.0
    assert r64["double_faults"] == 6.0
    assert r64["break_points_won"] == 4.0


def test_score_parses_total_games_and_games_won():
    rows = parse_player_page(_FIXTURE)
    r64 = next(r for r in rows if r["round"] == "R64" and r["tourn"] == "ATP Winston-Salem")
    assert r64["wl"] == "W"
    assert r64["match_total_games"] == 22.0
    assert r64["games_won"] == 13.0
    assert r64["sets_won"] == 2.0
    cin = next(r for r in rows if r["tourn"] == "Cincinnati Masters")
    assert cin["wl"] == "L"
    assert cin["match_total_games"] == 28.0
    # Player-first loss: first numbers are this player (13), not the winner (15).
    assert cin["games_won"] == 13.0
    assert cin["sets_won"] == 1.0
    q1 = next(r for r in rows if r["round"] == "Q1")
    games = collect_history_values(rows, "match_total_games", last_n=5)
    assert q1["match_total_games"] == 15.0
    assert games == [22.0, 28.0]
    rows = parse_player_page(_FIXTURE)
    q1 = next(r for r in rows if r["round"] == "Q1")
    assert q1["is_qualifying"] is True
    assert match_is_qualifying(q1)
    aces = collect_history_values(rows, "aces", last_n=5)
    assert aces == [3.0, 3.0]


def test_winner_first_loss_uses_loser_games_not_winner_games():
    # Semenistaja vs Ristic: TA stored L + winner-first "6-2 6-3" (she won 5, not 12).
    lost_straight = ta_player_side_from_score("6-2 6-3", "L")
    assert lost_straight is not None
    assert lost_straight["match_total_games"] == 17.0
    assert lost_straight["games_won"] == 5.0
    assert lost_straight["sets_won"] == 0.0
    won_straight = ta_player_side_from_score("6-2 6-3", "W")
    assert won_straight is not None
    assert won_straight["games_won"] == 12.0
    assert won_straight["sets_won"] == 2.0
    # Hamburg: L + "3-6 6-3 6-2" is winner-first (winner 15, loser 11).
    hamburg = ta_player_side_from_score("3-6 6-3 6-2", "L")
    assert hamburg is not None
    assert hamburg["games_won"] == 11.0
    assert hamburg["sets_won"] == 1.0
    # Cincinnati fixture stays player-first: L + "4-6 6-3 3-6" → 13.
    cin = ta_player_side_from_score("4-6 6-3 3-6", "L")
    assert cin is not None
    assert cin["games_won"] == 13.0
    assert cin["sets_won"] == 1.0
    win_tb = ta_player_side_from_score("6-3 7-6(4)", "W")
    assert win_tb is not None
    assert win_tb["games_won"] == 13.0
    ret = ta_player_side_from_score("5-1 RET", "L")
    assert ret is not None
    assert ret["games_won"] is None
    assert ret["sets_won"] is None


def test_backfill_rewrites_winner_first_losses():
    rows = backfill_score_fields(
        [{"score": "6-2 6-3", "wl": "L", "games_won": 12.0, "sets_won": 2.0}]
    )
    assert rows[0]["games_won"] == 5.0
    assert rows[0]["sets_won"] == 0.0
    assert rows[0]["match_total_games"] == 17.0


def test_newest_first_orders_same_day_by_round():
    rows = parse_player_page(_FIXTURE)
    dates = [(r["date"], r["round"]) for r in rows]
    assert dates[0][0] == "20260823"
    assert dates[0][1] == "R32"
    assert dates[1][1] == "R64"


_JSMATCHES = r"""
var fullname = 'Iga Swiatek';
var matchmx = [["20260813", "Cincinnati", "Hard", "PM", "W", "5", "7", "", "R64", "6-3 6-0", "3", "Emiliana Arango", "80", "", "", "R", "19900101", "170", "COL", "1", "1:00", "2", "1", "50", "30", "20", "8", "10", "0", "1", "1", "0", "40", "20", "10", "6", "8", "2", "5", "", "", "", "", "", "", "1", "1"]];
"""


def test_jsmatches_without_matchhead_uses_default_columns():
    rows = parse_player_page(_JSMATCHES)
    assert len(rows) == 1
    assert rows[0]["aces"] == 2.0
    assert rows[0]["double_faults"] == 1.0
    assert rows[0]["break_points_won"] == 3.0


def test_wta_page_url_is_not_atp_player_classic():
    url = ta_page_url("IgaSwiatek", "WTA")
    assert "/wplayer-classic.cgi" in url
    assert "/cgi-bin/player-classic.cgi" not in url
    assert ta_page_url("MiomirKecmanovic", "ATP").endswith(
        "player-classic.cgi?p=MiomirKecmanovic"
    )


def test_index_key_includes_tour():
    assert ta_index_key("Iga Swiatek", "WTA") != ta_index_key("Iga Swiatek", "ATP")
    assert ta_index_key("Iga Swiatek", "WTA").startswith("WTA:")


def test_ta_player_fresh_rejects_stale_log():
    idx = {
        ta_index_key("Miomir Kecmanovic", "ATP"): [
            {"date": "20181029", "aces": 5.0},
        ]
    }
    assert ta_player_fresh(idx, "Miomir Kecmanovic", date(2026, 8, 25), tour="ATP") is False
    idx[ta_index_key("Miomir Kecmanovic", "ATP")] = [{"date": "20260823", "aces": 3.0}]
    assert ta_player_fresh(idx, "Miomir Kecmanovic", date(2026, 8, 25), tour="ATP") is True
    assert ta_player_fresh(idx, "Miomir Kecmanovic", date(2026, 8, 25), tour="WTA") is False
    idx[ta_index_key("Colton Smith", "ATP")] = [{"date": "20260810"}]
    assert ta_player_fresh(idx, "Colton Smith", date(2026, 8, 25), tour="ATP") is True
