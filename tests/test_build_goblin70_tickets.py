"""Goblin-70 ticket pool and /tickets publish payload."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from build_goblin70_tickets import (  # noqa: E402
    pack_yolo,
    playable_tickets,
    to_web_payload,
)
from utils.ticket_70_pool import (  # noqa: E402
    goblin_70_eligible,
    goblin_sort_key,
    live_board_fill_ok,
    nflp_std_over_eligible,
    nflp_ticket_eligible,
    standard_ticket_eligible,
    ticket_excluded_from_winrate,
    ticket_gate_passes,
)


def _gob(**kwargs) -> dict:
    row = {
        "sport": "WNBA",
        "player": "Ezi Magbegor",
        "prop": "Rebounds",
        "side": "OVER",
        "line": 5.5,
        "pick_type": "Goblin",
        "l5_over": 5,
        "l10_over": 8,
        "cover": 2.4,
        "def": "Weak",
        "prop_tier": "A",
    }
    row.update(kwargs)
    return row


def test_cover_floor_blocks_wnba_under_2():
    assert not goblin_70_eligible(_gob(cover=1.1))
    # Cover floor still applies. D is now required except tennis/golf.
    assert goblin_70_eligible(_gob(prop="Points", cover=4.3, l5_over=5, l10_over=8))
    # Per-prop units: 3s +0.8 is a real cover; PRA +2.5 is not.
    assert goblin_70_eligible(_gob(prop="3-PT Made", cover=0.8, l5_over=5, l10_over=8))
    assert goblin_70_eligible(_gob(prop="FG Attempted", cover=1.2, l5_over=5, l10_over=8))
    assert not goblin_70_eligible(_gob(prop="Pts+Rebs+Asts", cover=2.5, l5_over=5, l10_over=8))
    # PRA cover floor is 3.7; Off AND prop_tier S/A are required (_gob stamps A).
    assert not goblin_70_eligible(
        _gob(prop="Pts+Rebs+Asts", cover=4.0, l5_over=5, l10_over=8)
    )
    assert goblin_70_eligible(
        _gob(
            prop="Pts+Rebs+Asts",
            cover=4.0,
            l5_over=5,
            l10_over=8,
            usage_tier="high",
        )
    )
    assert goblin_70_eligible(
        _gob(
            prop="Pts+Rebs+Asts",
            cover=4.0,
            l5_over=5,
            l10_over=8,
            minutes_tier="HIGH",
        )
    )
    # Catalog PRA is B — Off alone is not enough when tier is stamped B.
    assert not goblin_70_eligible(
        _gob(
            prop="Pts+Rebs+Asts",
            cover=4.0,
            l5_over=5,
            l10_over=8,
            usage_tier="high",
            prop_tier="B",
        )
    )
    assert not goblin_70_eligible(
        _gob(
            prop="Pts+Rebs+Asts",
            cover=4.0,
            l5_over=5,
            l10_over=8,
            usage_tier="medium",
            minutes_tier="MEDIUM",
        )
    )
    assert not goblin_70_eligible(_gob(cover=4.3, l5_over=5, l10_over=8, **{"def": "Avg"}))
    # Hits/TB: BA>=.275 + L5=5 + leaky opp pitch (no L10, no cover).
    assert goblin_70_eligible(
        _gob(
            sport="MLB",
            prop="Hits",
            cover=0.4,
            l5_over=5,
            batting_avg=0.280,
            **{"def": "Weak"},
        )
    )
    assert goblin_70_eligible(
        _gob(
            sport="MLB",
            prop="Total Bases",
            cover=0.2,
            l5_over=5,
            batting_avg=0.300,
            **{"def": "Weak"},
        )
    )
    assert not goblin_70_eligible(
        _gob(
            sport="MLB",
            prop="Hits",
            cover=1.5,
            l5_over=4,
            batting_avg=0.280,
            **{"def": "Weak"},
        )
    )
    assert not goblin_70_eligible(
        _gob(
            sport="MLB",
            prop="Total Bases",
            cover=1.5,
            l5_over=5,
            batting_avg=0.250,
            **{"def": "Weak"},
        )
    )
    hitter_k = _gob(
        sport="MLB",
        prop="Hitter Strikeouts",
        cover=1.2,
        l5_over=2,
        l10_over=8,
        k_rate=0.30,
        checks={"D": True},
    )
    hitter_k["def"] = "Elite"
    # Keep-gate may pass; Goblin-70 tickets hard-fade hitter_ks.
    assert not goblin_70_eligible(hitter_k)
    hrrbi_ok = _gob(
        sport="MLB",
        prop="Hits+Runs+RBIs",
        cover=1.2,
        l5_over=5,
        batting_avg=0.290,
        checks={"D": True},
    )
    hrrbi_ok["def"] = "Weak"
    assert goblin_70_eligible(hrrbi_ok)
    hrrbi_no_d = _gob(
        sport="MLB",
        prop="Hits+Runs+RBIs",
        cover=1.2,
        l5_over=5,
        batting_avg=0.290,
        checks={"D": False},
    )
    hrrbi_no_d["def"] = "Avg"
    assert not goblin_70_eligible(hrrbi_no_d)


def test_nflp_ticket_gate():
    kicker = {
        "sport": "NFL",
        "player": "Andres Borregales",
        "prop": "FG Made",
        "side": "OVER",
        "pick_type": "Goblin",
        "league": "NFLP",
        "starter_policy": "plays",
        "l5_over": 5,
        "checks": {"D": False},
    }
    assert nflp_ticket_eligible(kicker)
    sit = dict(kicker, prop="Rush Yards", starter_policy="sit")
    assert not nflp_ticket_eligible(sit)
    backup = dict(
        kicker,
        player="Shane Buechele",
        prop="Pass Yards",
        starter_policy="backup",
        l5_over=1,
        checks={"D": True},
    )
    assert nflp_ticket_eligible(backup)
    assert not nflp_ticket_eligible(dict(backup, checks={"D": False}))
    std_backup = dict(
        backup,
        pick_type="Standard",
        checks={"D": True},
    )
    assert nflp_std_over_eligible(std_backup)
    assert not nflp_std_over_eligible(dict(std_backup, checks={"D": False}))


def test_live_board_fill_rejects_list_gate_and_prop_swap():
    swiatek = _gob(
        sport="TENNIS",
        player="Iga Swiatek",
        prop="Total Games",
        line=14.5,
        cover=5.7,
        l5_over=5,
        l10_over=8,
        **{"def": "Weak"},
    )
    keys = _gob(
        sport="TENNIS",
        player="Madison Keys",
        prop="Total Games",
        line=18.5,
        cover=3.0,
        l5_over=3,
        l10_over=8,
        **{"def": "Below Avg"},
    )
    zheng_tg = _gob(
        sport="TENNIS",
        player="Qinwen Zheng",
        prop="Total Games",
        line=16.5,
        cover=8.1,
        l5_over=5,
        l10_over=4,
        **{"def": "Below Avg"},
    )
    zheng_won = _gob(
        sport="TENNIS",
        player="Qinwen Zheng",
        prop="Total Games Won",
        line=11.5,
        standard_line=12.5,  # only 1 under Standard — not keep
        cover=2.0,
        l5_over=3,
        l10_over=4,
        **{"def": "Below Avg"},
    )
    pool = [swiatek, keys, zheng_tg, zheng_won]
    ok, why = live_board_fill_ok(pool, player="Iga Swiatek", prop="Total Games", line=14.5)
    assert ok and why == "ok"
    ok, why = live_board_fill_ok(pool, player="Madison Keys", prop="Total Games", line=18.5)
    assert not ok and why == "not_goblin70"
    ok, why = live_board_fill_ok(
        pool, player="Qinwen Zheng", prop="Total Games Won", line=11.5
    )
    assert not ok and why == "not_goblin70"
    ok, why = live_board_fill_ok(
        pool, player="Qinwen Zheng", prop="Total Games", line=21.0
    )
    assert not ok and why == "line_mismatch"
    assert not goblin_70_eligible(keys)
    assert not goblin_70_eligible(zheng_won)


def test_tennis_keep_gates_skip_d():
    tennis = _gob(
        sport="TENNIS",
        prop="Total Games Won",
        line=7.5,
        standard_line=12.5,
        cover=1.5,
        l5_over=3,
        l10_over=3,
        **{"def": "Elite"},
    )
    assert ticket_gate_passes(tennis)
    assert goblin_70_eligible(tennis)
    assert not ticket_gate_passes(dict(tennis, standard_line=9.5))
    tg = _gob(
        sport="TENNIS",
        prop="Total Games",
        cover=5.0,
        l5_over=4,
        l10_over=8,
        **{"def": "Elite"},
    )
    assert goblin_70_eligible(tg)
    assert not goblin_70_eligible(dict(tg, l5_over=3))
    golf = _gob(
        sport="GOLF",
        prop="Strokes",
        cover=2.0,
        l5_over=5,
        l10_over=8,
        **{"def": "N/A"},
    )
    assert ticket_gate_passes(golf)
    assert goblin_70_eligible(golf)
    assert not ticket_gate_passes(dict(golf, l10_over=7))
    cfb = _gob(
        sport="CFB",
        prop="Pass Yards",
        cover=15.0,
        l5_over=5,
        l10_over=8,
        **{"def": "Weak"},
    )
    assert goblin_70_eligible(cfb)
    assert not goblin_70_eligible({**cfb, "def": "Avg"})
    nfl = _gob(
        sport="NFL",
        prop="Receiving Yards",
        cover=12.0,
        l5_over=5,
        l10_over=8,
        **{"def": "Below Avg"},
        league="NFL",
    )
    # NFL Goblin OVER held until Week 2+ tagged ledger unlocks it.
    assert not goblin_70_eligible(nfl)
    cbb = _gob(
        sport="CBB",
        prop="Points",
        cover=4.0,
        l5_over=5,
        l10_over=8,
        **{"def": "Weak"},
    )
    assert goblin_70_eligible(cbb)


def test_standard_over_under_use_same_gate():
    std_u = {
        "sport": "WNBA",
        "player": "Jackie Young",
        "prop": "Steals",
        "side": "UNDER",
        "line": 1.5,
        "pick_type": "Standard",
        "l5_under": 5,
        "l10_under": 8,
        "cover": -2.0,
        "def": "Elite",
    }
    assert standard_ticket_eligible(std_u)
    assert not standard_ticket_eligible({**std_u, "def": "Weak"})
    assert not standard_ticket_eligible(dict(std_u, l10_under=7))
    std_o = _gob(
        pick_type="Standard",
        prop="Rebounds",
        cover=2.4,
        l5_over=5,
        l10_over=8,
        **{"def": "Weak"},
    )
    assert standard_ticket_eligible(std_o)
    tennis_std = _gob(
        sport="TENNIS",
        pick_type="Standard",
        prop="Total Games",
        cover=5.0,
        l5_over=5,
        l10_over=8,
        **{"def": "Avg"},
    )
    assert not standard_ticket_eligible(tennis_std)
    tennis_aces_u = _gob(
        sport="TENNIS",
        pick_type="Standard",
        side="UNDER",
        prop="Aces",
        cover=0.0,
        l5_under=1,
        l10_under=2,
        **{"def": "Avg"},
    )
    tennis_df_u = dict(tennis_aces_u, prop="Double Faults")
    # Serve UNDER keep removed after missing→0 inflation was backfilled away.
    assert not standard_ticket_eligible(tennis_aces_u)
    assert not standard_ticket_eligible(tennis_df_u)
    assert not goblin_70_eligible(
        _gob(sport="TENNIS", prop="Aces", cover=3.0, l5_over=5)
    )
    assert not goblin_70_eligible(
        _gob(sport="SOCCER", prop="Shots On Target", cover=1.5, l5_over=5)
    )
    assert not goblin_70_eligible(
        _gob(sport="MLB", prop="Singles", cover=1.2, l5_over=5)
    )
    assert not goblin_70_eligible(_gob(pick_type="Demon", cover=4.0))
    assert not goblin_70_eligible(_gob(pick_type="Standard", cover=4.0))


def test_l5_and_mlb_floor():
    assert not goblin_70_eligible(_gob(l5_over=3, cover=4.0))
    assert not goblin_70_eligible(_gob(l5_over=4, l10_over=8, cover=4.0))
    assert not goblin_70_eligible(_gob(l5_over=5, l10_over=7, cover=4.0))
    assert goblin_70_eligible(
        _gob(
            sport="MLB",
            player="MacKenzie Gore",
            prop="Pitcher Strikeouts",
            cover=1.0,
            l5_over=5,
            l10_over=8,
            own_def_tier="Elite",
        )
    )
    assert not goblin_70_eligible(
        _gob(
            sport="MLB",
            player="MacKenzie Gore",
            prop="Pitcher Strikeouts",
            cover=1.0,
            l5_over=5,
            l10_over=8,
            own_def_tier="Weak",
        )
    )


def test_web_payload_keeps_standard_gate_and_uses_n_correct():
    payload = {
        "date": "2026-08-26",
        "payout_note": "N-correct / To Win only.",
        "pool": {"goblin_70": 1},
        "tickets": [
            {
                "id": "P3-1",
                "family": "goblin",
                "product": "Power",
                "n_legs": 3,
                "mean_leg_p": 0.763,
                "sweep_pct": 44.4,
                "cash_pct": 44.4,
                "ev_n_correct": 0.887,
                "n_correct": {3: 2.0},
                "payout_note": "0S+3G Power 3-correct 2x (live slip; N-correct / To Win)",
                "legs": [
                    {
                        "sport": "MLB",
                        "player": "MacKenzie Gore",
                        "prop": "Pitcher Strikeouts",
                        "side": "OVER",
                        "line": 3.5,
                        "pick_type": "Goblin",
                        "l5": 5,
                        "cover": 2.9,
                        "d": "Below Avg",
                        "tier": "S",
                        "matchup": "TEX vs CWS",
                        "team": "TEX",
                        "p": 0.763,
                    },
                    {
                        "sport": "TENNIS",
                        "player": "Toby Samuel",
                        "prop": "Total Games",
                        "side": "OVER",
                        "line": 17.5,
                        "pick_type": "Goblin",
                        "l5": 5,
                        "cover": 7.1,
                        "tier": "A",
                        "matchup": "TOBY SAMUEL vs BILLY HARRIS (ATP / HARD)",
                        "p": 0.763,
                    },
                    {
                        "sport": "WNBA",
                        "player": "Jade Melbourne",
                        "prop": "Points",
                        "side": "OVER",
                        "line": 7.5,
                        "pick_type": "Goblin",
                        "l5": 5,
                        "cover": 6.5,
                        "tier": "A",
                        "matchup": "SEA vs TOR",
                        "team": "SEA",
                        "p": 0.763,
                    },
                ],
            },
            {
                "id": "SF3-1",
                "family": "standard",
                "product": "Flex",
                "n_legs": 3,
                "mean_leg_p": 0.65,
                "sweep_pct": 28.0,
                "cash_pct": 72.0,
                "ev_n_correct": 1.18,
                "n_correct": {3: 2.25, 2: 1.25},
                "legs": [
                    {
                        "sport": "MLB",
                        "player": "Brady House",
                        "prop": "Hits+Runs+RBIs",
                        "side": "UNDER",
                        "line": 1.5,
                        "pick_type": "Standard",
                        "p": 0.66,
                    }
                ],
            },
        ],
    }
    assert len(playable_tickets(payload)) == 2
    web = to_web_payload(payload)
    assert web["allow_standard"] is True
    assert web["ticket_track"] == "goblin70"
    slips = [t for g in web["groups"] for t in g["tickets"]]
    assert len(slips) == 2
    slip = next(
        t for t in slips if all(leg.get("pick_type") == "Goblin" for leg in t["legs"])
    )
    legs = slip["legs"]
    assert all(leg["pick_type"] == "Goblin" for leg in legs)
    assert all(leg["direction"] == "OVER" for leg in legs)
    pay = slip["payout"]
    assert pay["min_guarantee"] == 2.0
    assert pay["sweep_payout_x"] == 2.0
    assert pay["audit_all_hit_x"] == 2.0
    assert pay["n_correct"][3] == 2.0
    assert "1st" not in str(pay.get("payout_note") or "").lower()


def test_named_wnba_and_nfl_groups_are_playable():
    payload = {
        "date": "2026-08-27",
        "pool": {"goblin_70": 2, "nflp_goblin": 2},
        "tickets": [
            {
                "id": "WNB3-1",
                "family": "goblin",
                "product": "Power",
                "n_legs": 2,
                "web_group": "WNBA Goblin-70 Power 2",
                "mean_leg_p": 0.763,
                "sweep_pct": 58.2,
                "cash_pct": 58.2,
                "ev_n_correct": 1.28,
                "n_correct": {2: 2.2},
                "payout_note": "0S+2G Power median 2.2x",
                "legs": [
                    {
                        "sport": "WNBA",
                        "player": "Shakira Austin",
                        "prop": "Points",
                        "side": "OVER",
                        "line": 11.5,
                        "pick_type": "Goblin",
                        "p": 0.763,
                    },
                    {
                        "sport": "WNBA",
                        "player": "Kiki Iriafen",
                        "prop": "Pts+Rebs+Asts",
                        "side": "OVER",
                        "line": 19.5,
                        "pick_type": "Goblin",
                        "p": 0.763,
                    },
                ],
            },
            {
                "id": "NFL2-1",
                "family": "nflp",
                "product": "Power",
                "n_legs": 2,
                "web_group": "NFL Power 2",
                "mean_leg_p": 0.70,
                "sweep_pct": 49.0,
                "cash_pct": 49.0,
                "ev_n_correct": 1.08,
                "n_correct": {2: 2.2},
                "payout_note": "0S+2G Power median 2.2x",
                "legs": [
                    {
                        "sport": "NFL",
                        "player": "Andres Borregales",
                        "prop": "Kicking Points",
                        "side": "OVER",
                        "line": 6.5,
                        "pick_type": "Goblin",
                        "p": 0.70,
                    },
                    {
                        "sport": "NFL",
                        "player": "Shane Buechele",
                        "prop": "Pass Yards",
                        "side": "OVER",
                        "line": 149.5,
                        "pick_type": "Goblin",
                        "p": 0.62,
                    },
                ],
            },
        ],
    }
    assert len(playable_tickets(payload)) == 2
    web = to_web_payload(payload)
    names = {g["group_name"] for g in web["groups"]}
    assert "WNBA Goblin-70 Power 2" in names
    assert "NFL Power 2" in names
    sports = {
        leg["sport"]
        for g in web["groups"]
        for t in g["tickets"]
        for leg in t["legs"]
    }
    assert "WNBA" in sports
    assert "NFL" in sports


def test_goblin_power3_n_correct_is_live_2x():
    from build_goblin70_tickets import PAY, ticket_math

    assert PAY[("goblin", 3, "Power")]["n_correct"][3] == 2.0
    math = ticket_math(
        [{"p": 0.763}, {"p": 0.763}, {"p": 0.763}],
        "Power",
        "goblin",
    )
    assert math["n_correct"][3] == 2.0
    assert abs(math["ev_n_correct"] - (0.763 ** 3) * 2.0) < 0.001


def test_goblin70_ignores_ml_prob():
    """ml_prob cannot add or drop a Goblin-70 leg."""
    ok = _gob()
    assert goblin_70_eligible(ok)
    assert goblin_70_eligible(dict(ok, ml_prob=0.05))
    assert goblin_70_eligible(dict(ok, ml_prob=0.99))
    dead = dict(ok, l5_over=4, ml_prob=0.99)
    assert not goblin_70_eligible(dead)
    no_d = dict(ok, ml_prob=0.99)
    no_d["def"] = "Avg"
    assert not goblin_70_eligible(no_d)


def test_goblin70_est_win_prob_uses_gate_not_ml():
    from build_goblin70_tickets import _ticket_to_web, ticket_math

    legs = [
        {
            "player": "A",
            "sport": "WNBA",
            "prop": "Points",
            "side": "OVER",
            "line": 10.5,
            "p": 0.745,
            "l5": 5,
            "ml_prob": 0.99,
            "hit_rate": 1.0,
        },
        {
            "player": "B",
            "sport": "MLB",
            "prop": "Hits",
            "side": "OVER",
            "line": 0.5,
            "p": 0.745,
            "l5": 5,
            "ml_prob": 0.99,
            "hit_rate": 1.0,
        },
        {
            "player": "C",
            "sport": "Tennis",
            "prop": "Games Won",
            "side": "OVER",
            "line": 8.5,
            "p": 0.745,
            "l5": 5,
            "ml_prob": 0.99,
            "hit_rate": 1.0,
        },
    ]
    math = ticket_math(legs, "Power", "goblin")
    ticket = {
        **math,
        "n_legs": 3,
        "product": "Power",
        "legs": legs,
    }
    web = _ticket_to_web(ticket, date="2026-09-02", ticket_no=1, group_name="X")
    assert web["est_win_prob"] == round(math["sweep_pct"] / 100.0, 4)
    assert web["est_win_prob"] < 0.99 ** 3
    assert all(leg["ml_prob"] == 0.99 for leg in web["legs"])


def test_goblin70_web_leg_splits_hr_and_ml():
    from build_goblin70_tickets import _leg_to_web

    web = _leg_to_web(
        {
            "player": "Gerrit Cole",
            "sport": "MLB",
            "prop": "Pitcher Strikeouts",
            "side": "OVER",
            "line": 3.5,
            "p": 0.763,
            "l5": 5,
            "cover": 3.6,
            "ml_prob": 0.92,
            "hit_rate": 1.0,
            "standard_line": 5.5,
        },
        ticket_id="t1",
        date="2026-08-27",
    )
    assert web["hit_rate"] == 1.0
    assert web["ml_prob"] == 0.92
    assert web["hit_rate"] != web["ml_prob"]
    assert web["best_cross_book"] == "PP"
    assert web["best_cross_line"] == 3.5
    assert web["cross_edge_vs_pp"] == 0.0
    assert web["standard_line"] == 5.5


def test_merge_keeps_goblin70_and_graded_main():
    from build_goblin70_tickets import merge_web_payload, is_g70_group

    g70 = {
        "date": "2026-08-27",
        "ticket_track": "goblin70",
        "mode": "goblin70",
        "groups": [
            {
                "group_name": "X-Sport Goblin-70 Power 3",
                "tickets": [{"ticket_track": "goblin70", "ticket_id": "g70-1"}],
            }
        ],
    }
    main = {
        "date": "2026-08-27",
        "ticket_track": "graded_main",
        "filters": {"min_hit_rate": 0.72},
        "groups": [
            {
                "group_name": "X-Sport Goblin-70 Power 3",
                "tickets": [{"ticket_track": "goblin70"}],
            },
            {
                "group_name": "STRONG 3-Leg",
                "tickets": [{"ticket_track": "graded_main", "ticket_id": "main-1"}],
            },
            {
                "group_name": "MLB 2-Leg Goblin OVER",
                "tickets": [{"ticket_track": "graded_main", "ticket_id": "main-2"}],
            },
        ],
    }
    mixer = [g for g in main["groups"] if not is_g70_group(g)]
    merged = merge_web_payload(g70, main, mixer)
    names = [g["group_name"] for g in merged["groups"]]
    assert names[0] == "X-Sport Goblin-70 Power 3"
    assert "STRONG 3-Leg" in names
    assert "MLB 2-Leg Goblin OVER" in names
    assert names.count("X-Sport Goblin-70 Power 3") == 1
    assert merged["mode"] == "goblin70+graded_main"
    assert merged["tracks"] == ["goblin70", "graded_main"]


def test_union_mixer_keeps_live_core_and_pool_tennis():
    from build_goblin70_tickets import union_mixer_groups

    live = [
        {"group_name": "STRONG 3-Leg", "tickets": [{"ticket_id": "s1"}]},
        {"group_name": "MLB Core Power 2 #3", "tickets": [{"ticket_id": "m1"}]},
    ]
    pool = [
        {"group_name": "TENNIS Core Power 2 #1", "tickets": [{"ticket_id": "t1"}]},
        {"group_name": "STRONG 3-Leg", "tickets": [{"ticket_id": "stale"}]},
    ]
    names = [g["group_name"] for g in union_mixer_groups(live, pool)]
    assert names == [
        "STRONG 3-Leg",
        "MLB Core Power 2 #3",
        "TENNIS Core Power 2 #1",
    ]
    by_name = {g["group_name"]: g for g in union_mixer_groups(live, pool)}
    assert by_name["STRONG 3-Leg"]["tickets"][0]["ticket_id"] == "s1"


def test_patch_mixer_updates_line_and_keeps_unmatched():
    from build_goblin70_tickets import patch_mixer_groups

    groups = [
        {
            "group_name": "STRONG 3-Leg",
            "tickets": [
                {
                    "ticket_id": "keep-line-move",
                    "legs": [
                        {
                            "sport": "WNBA",
                            "player": "Kiki Iriafen",
                            "prop_type": "Pts+Rebs",
                            "pick_type": "Goblin",
                            "direction": "OVER",
                            "line": 19.5,
                        },
                        {
                            "sport": "MLB",
                            "player": "Gerrit Cole",
                            "prop_type": "Pitcher Strikeouts",
                            "pick_type": "Goblin",
                            "direction": "OVER",
                            "line": 3.5,
                        },
                    ],
                },
                {
                    "ticket_id": "keep-missing-prop",
                    "legs": [
                        {
                            "sport": "WNBA",
                            "player": "Gone Player",
                            "prop_type": "Points",
                            "pick_type": "Goblin",
                            "direction": "OVER",
                            "line": 10.5,
                        }
                    ],
                },
            ],
        }
    ]
    board = [
        {
            "sport": "WNBA",
            "player": "Kiki Iriafen",
            "prop": "Pts+Rebs",
            "pick_type": "Goblin",
            "side": "OVER",
            "line": 18.5,
            "cover": 7.8,
            "l5_over": 5,
        },
        {
            "sport": "MLB",
            "player": "Gerrit Cole",
            "prop": "Pitcher Strikeouts",
            "pick_type": "Goblin",
            "side": "OVER",
            "line": 3.5,
            "cover": 3.6,
            "l5_over": 5,
        },
    ]
    out, stats = patch_mixer_groups(groups, board)
    assert stats["updated"] == 1
    assert stats["dropped"] == 0
    ids = [t["ticket_id"] for t in out[0]["tickets"]]
    assert ids == ["keep-line-move", "keep-missing-prop"]
    legs = out[0]["tickets"][0]["legs"]
    assert legs[0]["line"] == 18.5
    assert legs[1]["line"] == 3.5


def test_patch_mixer_keeps_leg_when_sport_not_fetched():
    from build_goblin70_tickets import patch_mixer_groups

    groups = [
        {
            "group_name": "TENNIS 2-Leg Goblin OVER",
            "tickets": [
                {
                    "ticket_id": "tennis-1",
                    "legs": [
                        {
                            "sport": "TENNIS",
                            "player": "Qinwen Zheng",
                            "prop_type": "Total Games",
                            "pick_type": "Goblin",
                            "direction": "OVER",
                            "line": 18.5,
                        }
                    ],
                }
            ],
        }
    ]
    board = [
        {
            "sport": "MLB",
            "player": "Gerrit Cole",
            "prop": "Pitcher Strikeouts",
            "pick_type": "Goblin",
            "side": "OVER",
            "line": 3.5,
        }
    ]
    out, stats = patch_mixer_groups(groups, board)
    assert stats["dropped"] == 0
    assert stats["unchanged"] == 1
    assert out[0]["tickets"][0]["legs"][0]["line"] == 18.5


def _yolo_seed(i: int, sport: str, player: str, prop: str = "Points") -> dict:
    return {
        "sport": sport,
        "player": player,
        "prop": prop,
        "side": "OVER",
        "line": 1.5 + i,
        "pick_type": "Goblin",
        "p": 0.745,
        "prop_tier": "S" if i < 4 else "A",
        "cover": 8.0 - i * 0.2,
        "l5_over": 5,
        "l10_over": 8,
        "def": "Weak",
        "matchup": f"HOME vs AWAY {i}",
        "team": "HOME",
    }


def test_pack_yolo_power_6_5_4_unique_players():
    pool = []
    sports = ["MLB", "TENNIS", "WNBA", "CFB", "NBA", "NHL"]
    for i in range(16):
        pool.append(
            _yolo_seed(
                i,
                sports[i % len(sports)],
                f"Player {i}",
                "Pitcher Strikeouts" if sports[i % len(sports)] == "MLB" else "Points",
            )
        )
    combos = pack_yolo(pool)
    assert [len(c) for c in combos] == [6, 5, 4]
    names = [x["player"] for c in combos for x in c]
    assert len(names) == len(set(names))
    first_players = {x["player"] for x in combos[0]}
    assert "Player 0" in first_players


def test_pack_yolo_mixes_one_standard():
    gob = [
        _yolo_seed(
            i,
            ["MLB", "TENNIS", "CFB", "NBA", "NHL", "WNBA"][i % 6],
            f"Gob {i}",
            "Pitcher Strikeouts" if i % 6 == 0 else "Points",
        )
        for i in range(12)
    ]
    std = [
        {
            "sport": "Tennis",
            "player": "Serve Ace",
            "prop": "Aces",
            "side": "UNDER",
            "line": 3.5,
            "pick_type": "Standard",
            "p": 0.90,
            "prop_tier": "A",
            "badge": "Platinum",
            "cover": 2.0,
            "l5_under": 5,
            "l10_under": 8,
            "def": "Elite",
            "matchup": "ACE vs DF",
            "team": "ACE",
        }
    ]
    combos = pack_yolo(gob, std)
    assert combos
    assert sum(1 for x in combos[0] if x.get("pick_type") == "Standard") == 1
    assert combos[0][0]["player"] == "Serve Ace"


def test_yolo_web_payload_is_power_excluded_from_winrate():
    payload = {
        "date": "2026-09-07",
        "pool": {"goblin_70": 6},
        "tickets": [
            {
                "id": "Y6-1",
                "family": "mix",
                "product": "Power",
                "n_legs": 6,
                "exclude_from_winrate": True,
                "web_group": "YOLO Goblin-70 Power 6",
                "mean_leg_p": 0.745,
                "sweep_pct": 17.0,
                "cash_pct": 17.0,
                "ev_n_correct": 1.4,
                "n_correct": {6: 8.75},
                "payout_note": "1S+5G Power fallback 8.75x",
                "legs": [
                    {
                        "sport": "Tennis",
                        "player": "Serve Ace",
                        "prop": "Aces",
                        "side": "UNDER",
                        "line": 3.5,
                        "pick_type": "Standard",
                        "p": 0.90,
                    }
                ]
                + [
                    {
                        "sport": "MLB",
                        "player": f"Yolo {i}",
                        "prop": "Pitcher Strikeouts",
                        "side": "OVER",
                        "line": 2.5,
                        "pick_type": "Goblin",
                        "p": 0.745,
                    }
                    for i in range(5)
                ],
            }
        ],
    }
    web = to_web_payload(payload)
    assert web["groups"][0]["group_name"] == "YOLO Goblin-70 Power 6"
    slip = web["groups"][0]["tickets"][0]
    assert slip["core_recipe"] == "goblin70_yolo"
    assert slip["n_legs"] == 6
    assert slip["play"] == "Power"
    assert slip["ticket_track"] == "goblin70_yolo"
    assert slip["exclude_from_winrate"] is True
    assert slip["payout"]["n_correct"][6] == 8.75
    assert "1st" not in str(slip["payout"].get("payout_note") or "").lower()
    assert ticket_excluded_from_winrate(slip, "YOLO Goblin-70 Power 6")


def test_goblin_sort_key_platinum_before_s_bronze():
    plat = _gob(prop="Points", badge="Platinum", prop_tier="A", player="Plat")
    bronze = _gob(prop="Points", badge="Bronze", prop_tier="S", player="Bron")
    assert goblin_sort_key(plat) < goblin_sort_key(bronze)


def test_ticket_excluded_from_winrate_helper():
    assert ticket_excluded_from_winrate({"exclude_from_winrate": True})
    assert ticket_excluded_from_winrate({}, "YOLO Goblin-70 Power 6")
    assert ticket_excluded_from_winrate(
        {"id": "Y6-1", "product": "Power", "n_legs": 6}
    )
    assert not ticket_excluded_from_winrate(
        {"id": "P3-1", "product": "Power", "n_legs": 3}
    )
