"""Gap orders Overs inside the gate. It does not change a price or a floor."""
from __future__ import annotations

from utils.gap_ranker import (
    apply_stop,
    cell_order_key,
    gap_order_key,
    log_slate,
    score_sport,
    top_k,
)
from utils.ticket_ev_build import build_ev_slips


def _over(player: str, *, gap: float, hit: int, n: int, p: float = 0.8, **kw) -> dict:
    row = {
        "sport": "WNBA",
        "player": player,
        "prop": "Points",
        "side": "OVER",
        "pick_type": "Standard",
        "p": p,
        "gap_z": gap,
        "cell_hit": hit,
        "cell_n": n,
        "matchup": kw.pop("matchup", f"Game {player}"),
    }
    row.update(kw)
    return row


def test_gap_orders_overs_that_clear_the_wilson_floor():
    wide = _over("Wide", gap=1.4, hit=360, n=400)
    thin = _over("Thin", gap=0.2, hit=360, n=400)
    assert gap_order_key(wide) < gap_order_key(thin)


def test_a_cell_under_the_admission_bar_cannot_jump_the_line():
    short_n = _over("Short", gap=4.0, hit=29, n=29, pick_type="Goblin")
    weak = _over("Weak", gap=4.0, hit=30, n=80, pick_type="Goblin")
    solid = _over("Solid", gap=0.2, hit=300, n=400, pick_type="Goblin")
    assert gap_order_key(solid) < gap_order_key(short_n)
    assert gap_order_key(solid) < gap_order_key(weak)
    assert cell_order_key(solid) < cell_order_key(weak)


def test_a_goblin_cell_above_0_65_sorts_by_gap():
    wide = _over("Wide", gap=2.0, hit=250, n=350, pick_type="Goblin")
    thin = _over("Thin", gap=0.2, hit=300, n=350, pick_type="Goblin")
    assert gap_order_key(wide) < gap_order_key(thin)
    assert cell_order_key(thin) < cell_order_key(wide)


def test_standard_under_is_not_ranked_by_gap():
    under = _over("Under", gap=5.0, hit=360, n=400, pick_type="Standard", side="UNDER")
    over = _over("Over", gap=0.1, hit=360, n=400)
    assert gap_order_key(over) < gap_order_key(under)


def test_top_k_keeps_one_leg_per_player():
    rows = [
        _over("A", gap=2.0, hit=360, n=400, prop="Points"),
        _over("A", gap=1.5, hit=360, n=400, prop="Rebounds"),
        _over("B", gap=0.4, hit=360, n=400),
    ]
    picked = top_k(rows, gap_order_key, k=6)
    assert [row["player"] for row in picked] == ["A", "B"]
    assert picked[0]["prop"] == "Points"


def test_ev_within_two_points_breaks_on_gap():
    pool = [
        _over("A", gap=0.1, hit=360, n=400, p=0.80),
        _over("B", gap=0.1, hit=360, n=400, p=0.80),
        _over("C", gap=0.1, hit=360, n=400, p=0.80),
        _over("D", gap=3.0, hit=360, n=400, p=0.795),
    ]
    slips = build_ev_slips(pool, family="standard", taken=set(), fold=lambda name: str(name))
    assert [leg["player"] for leg in slips[0]["legs"]] == ["A", "B", "D"]


def test_a_wider_ev_gap_still_wins():
    pool = [
        _over("A", gap=0.1, hit=360, n=400, p=0.80),
        _over("B", gap=0.1, hit=360, n=400, p=0.80),
        _over("C", gap=0.1, hit=360, n=400, p=0.80),
        _over("D", gap=5.0, hit=360, n=400, p=0.70),
    ]
    slips = build_ev_slips(pool, family="standard", taken=set(), fold=lambda name: str(name))
    assert [leg["player"] for leg in slips[0]["legs"]] == ["A", "B", "C"]


def test_log_keeps_both_orders_and_a_rewrite_keeps_hits(tmp_path):
    path = tmp_path / "gap_ranker_log.jsonl"
    rows = [_over(f"P{i}", gap=float(i), hit=360, n=400, matchup=f"G{i % 4}") for i in range(12)]
    logged = log_slate(rows, date="2026-09-22", path=path, state_path=tmp_path / "missing.json")
    assert logged[0]["qualifying"] is True
    assert logged[0]["order_used"] == "gap"
    assert logged[0]["orders_differ"] is True
    assert logged[0]["legs_reordered"]
    assert logged[0]["ev_gap"] == 0.0
    logged[0]["legs"][0]["hit"] = 1
    path.write_text(
        __import__("json").dumps(logged[0]) + "\n",
        encoding="utf-8",
    )
    again = log_slate(rows, date="2026-09-22", path=path, state_path=tmp_path / "missing.json")
    kept = [leg for leg in again[0]["legs"] if leg["hit"] == 1]
    assert len(kept) == 1


def _slate(day: str, *, gap_hits: int, all_hit: bool = False) -> dict:
    legs = []
    for i in range(6):
        legs.append(
            {
                "id": f"a{i}|points",
                "player": f"a{i}",
                "prop": "points",
                "side": "OVER",
                "pick_type": "Standard",
                "gap_z": 2.0,
                "wilson_low": 0.90,
                "clears_floor": True,
                "game": f"g{i}",
                "hit": 1 if all_hit or i < gap_hits else 0,
            }
        )
    for i in range(6):
        legs.append(
            {
                "id": f"b{i}|points",
                "player": f"b{i}",
                "prop": "points",
                "side": "OVER",
                "pick_type": "Standard",
                "gap_z": 0.1,
                "wilson_low": 0.97,
                "clears_floor": True,
                "game": f"h{i}",
                "hit": 1 if all_hit or gap_hits != 6 else 0,
            }
        )
    return {
        "date": day,
        "sport": "MLB",
        "qualifying": True,
        "order_used": "gap",
        "pool_n": 12,
        "games": 12,
        "legs": legs,
    }


def test_gap_stays_on_when_it_beats_cell_rate_after_20_days():
    records = [_slate(f"2026-09-{i:02d}", gap_hits=6) for i in range(1, 21)]
    state = apply_stop(records, {"sports": {}})
    assert state["sports"]["MLB"]["enabled"] is True
    assert state["sports"]["MLB"]["decision"] == "gap stays on"
    assert state["sports"]["MLB"]["graded_days"] == 20


def test_gap_turns_off_when_it_does_not_beat_cell_rate():
    records = [_slate(f"2026-09-{i:02d}", gap_hits=0) for i in range(1, 21)]
    # gap_hits 0 makes the high-gap legs miss and the high-wilson legs hit.
    scored = score_sport(records, "MLB")
    assert scored["gap"] == 0
    assert scored["cell"] == 1
    state = apply_stop(records, {"sports": {}})
    assert state["sports"]["MLB"]["enabled"] is False
    assert state["sports"]["MLB"]["decision"] == "gap off"


def test_nineteen_days_stay_a_trial():
    records = [_slate(f"2026-08-{i:02d}", gap_hits=0) for i in range(1, 20)]
    state = apply_stop(records, {"sports": {}})
    assert state["sports"]["MLB"]["enabled"] is True
    assert state["sports"]["MLB"]["decision"] == "trial"


def _same_order(day: str) -> dict:
    legs = []
    for i in range(12):
        legs.append(
            {
                "id": f"p{i}|points",
                "player": f"p{i}",
                "prop": "points",
                "side": "OVER",
                "pick_type": "Goblin",
                "gap_z": 1.0,
                "wilson_low": 0.70,
                "clears_floor": True,
                "game": f"g{i}",
                "hit": 0,
                "p": 0.75,
            }
        )
    return {
        "date": day,
        "sport": "MLB",
        "qualifying": True,
        "pool_n": 12,
        "games": 12,
        "legs": legs,
    }


def test_identical_orders_do_not_count_toward_the_stop():
    records = [_same_order(f"2026-07-{i:02d}") for i in range(1, 26)]
    scored = score_sport(records, "MLB")
    assert scored["qualifying"] == 25
    assert scored["differing"] == 0
    assert scored["differing_graded"] == 0
    state = apply_stop(records, {"sports": {}})
    assert state["sports"]["MLB"]["enabled"] is True
    assert state["sports"]["MLB"]["decision"] == "trial"


def test_a_tie_on_differing_slates_is_not_a_loss():
    records = [_slate(f"2026-06-{i:02d}", gap_hits=6, all_hit=True) for i in range(1, 21)]
    scored = score_sport(records, "MLB")
    assert scored["differing_graded"] == 20
    assert scored["gap"] == scored["cell"] == 1
    assert scored["loses_to_cell"] is False
    state = apply_stop(records, {"sports": {}})
    assert state["sports"]["MLB"]["enabled"] is True
    assert state["sports"]["MLB"]["decision"] == "trial"


def _previous_month(today) -> str:
    if today.month == 1:
        return f"{today.year - 1}-12"
    return f"{today.year}-{today.month - 1:02d}"


def test_an_off_sport_stays_off_until_next_month_then_can_return():
    from datetime import date

    today = date.today()
    winning = [_slate(f"2026-05-{i:02d}", gap_hits=6) for i in range(1, 21)]
    held = apply_stop(
        winning,
        {
            "sports": {
                "MLB": {
                    "enabled": False,
                    "decision": "gap off",
                    "reviewed_month": today.strftime("%Y-%m"),
                }
            }
        },
    )
    assert held["sports"]["MLB"]["enabled"] is False
    assert held["sports"]["MLB"]["decision"] == "gap off"

    tied = [_slate(f"2026-04-{i:02d}", gap_hits=6, all_hit=True) for i in range(1, 21)]
    returned = apply_stop(
        tied,
        {
            "sports": {
                "MLB": {
                    "enabled": False,
                    "decision": "gap off",
                    "reviewed_month": _previous_month(today),
                }
            }
        },
    )
    assert returned["sports"]["MLB"]["enabled"] is True
    assert returned["sports"]["MLB"]["decision"] == "trial"
