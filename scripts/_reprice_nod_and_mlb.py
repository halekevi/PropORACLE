"""Reprice Oct boards from no-D WNBA cells and MLB as-of keep cells.

The price file's single cell is what build() reads via lookup_category_hr.
For these props that cell is not the keep gate.
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import build_goblin70_tickets as g70  # noqa: E402
from utils.category_hit_rate import load_category_hr_index, lookup_category_hr  # noqa: E402
from utils.n_correct_payout import _percentile, capture_composition_samples  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import (  # noqa: E402
    P_TENNIS_MATCH_TOTAL_GOBLIN,
    goblin_70_eligible,
    goblin_ticket_p,
)
from utils.ticket_ev_build import attach_leg_p, build_ev_slips  # noqa: E402
from utils.ticket_keep_registry import rule_for  # noqa: E402


def _p(*a):
    print(*a, flush=True)


def _band(hits: int, n: int) -> str:
    p = hits / n
    se = (p * (1.0 - p) / n) ** 0.5
    return f"{hits}/{n} ({p:.1%}, band {p - 1.96 * se:.1%}–{p + 1.96 * se:.1%})"


# Price-file cell the builder reads, then the cell to use instead.
# hits/n are the integer cells behind the published percents.
BUILDER_CELL = {
    ("MLB", "pitcher_ks"): ("L5=5+D", 46, 64),
    ("MLB", "hits_allowed"): ("L5=5+D", 32, 57),
    ("MLB", "pitching_outs"): ("listed L5>=4", 58, 96),
    ("WNBA", "threes"): ("L5=5+L10>=8+D", 64, 79),
    ("WNBA", "pts+ast"): ("L5=5+L10>=8+D", 63, 75),
}
REPRICE = {
    ("MLB", "pitcher_ks"): ("L10>=8 parent", 743, 975),
    ("MLB", "hits_allowed"): ("L10>=8 parent", 633, 823),
    ("MLB", "pitching_outs"): ("L10>=8 parent", 399, 498),
    ("WNBA", "threes"): ("L5=5+L10>=8, no D", 147, 191),
    ("WNBA", "pts+ast"): ("L5=5+L10>=8, no D", 171, 229),
}


def _quotes() -> dict[str, tuple[float, float, int]]:
    out = {}
    for label, n, ns, ng in (
        ("2-leg all Goblin", 2, 0, 2),
        ("3-leg all Goblin", 3, 0, 3),
    ):
        samples = capture_composition_samples(ns, ng, n)
        if not samples:
            out[label] = (0.0, 0.0, 0)
            continue
        out[label] = (_percentile(samples, 25), _percentile(samples, 50), len(samples))
    return out


def _legs(day: str) -> list[dict]:
    board = g70.load_today_board(day, skip_golf=True)
    index = load_category_hr_index()
    gob = []
    for r in board:
        if not goblin_70_eligible(r):
            continue
        if g70.fold_name(r.get("player")) in g70.SKIP_PLAYERS:
            continue
        rec = dict(r)
        rule = rule_for(rec.get("sport"))
        recipe = f"{rec.get('sport')}:{(rule[0] if rule else 'off')}"
        attach_leg_p(rec, recipe=recipe, index=index)
        sport = str(rec.get("sport") or "")
        prop = canon_prop(sport, str(rec.get("prop") or ""))
        if rec.get("p_source") == "gate_cell":
            child = lookup_category_hr(
                index,
                sport=sport,
                prop=rec.get("prop"),
                pick_type=rec.get("pick_type"),
                direction=rec.get("side") or rec.get("direction"),
            )
            if child and child.get("hr") is not None:
                rec["p"] = float(child["hr"])
                rec["p_n"] = float(child.get("n") or 0)
                rec["p_hit"] = float(child.get("hit") or 0)
                rec["p_source"] = "gate_cell"
        if rec.get("p") is None or rec.get("p_source") != "gate_cell":
            rec["p"] = goblin_ticket_p(rec)
            rec["p_source"] = "l5eq5_floor"
        rec["prop_canon"] = prop
        built = BUILDER_CELL.get((sport.upper() if sport.upper() != "SOCCER" else sport, prop))
        # sport keys in BUILDER are MLB/WNBA as stored
        built = BUILDER_CELL.get((str(rec.get("sport") or "").strip(), prop))
        if built:
            rec["builder_window"] = built[0]
        else:
            rec["builder_window"] = rec.get("p_source")
        if str(rec.get("sport") or "").upper() == "TENNIS" and prop == "match_total_games":
            rec["p"] = P_TENNIS_MATCH_TOTAL_GOBLIN
            rec["p_n"] = 208
            rec["p_source"] = "tennis_tg_corrected"
        gob.append(rec)
    return gob


def _apply_reprice(legs: list[dict]) -> None:
    for rec in legs:
        key = (str(rec.get("sport") or "").strip(), rec.get("prop_canon"))
        cell = REPRICE.get(key)
        if not cell:
            continue
        label, hits, n = cell
        rec["p"] = hits / n
        rec["p_n"] = n
        rec["p_source"] = label


def _run(legs: list[dict], tag: str, q2: float, q3: float) -> None:
    import utils.ticket_ev_build as te

    real = te.power_multiplier

    def fake(n_legs: int, *, family: str, n_goblin: int | None = None):
        if family == "goblin" and n_legs == 2:
            return q2, "n_correct_p25" if tag == "p25" else "n_correct_median"
        if family == "goblin" and n_legs == 3:
            return q3, "n_correct_p25" if tag == "p25" else "n_correct_median"
        return real(n_legs, family=family, n_goblin=n_goblin)

    te.power_multiplier = fake
    try:
        slips = build_ev_slips(legs, taken=set(), fold=g70.fold_name)
    finally:
        te.power_multiplier = real
    _p(f"  {tag}: {len(slips)} slips")
    for s in slips[:12]:
        names = ", ".join(
            f"{leg.get('player')} {leg.get('prop')} "
            f"p={float(leg.get('p') or 0):.3f} [{leg.get('p_source')}]"
            for leg in s["legs"]
        )
        _p(
            f"    {s['product']} {len(s['legs'])} ev={s['ev']:+.3f} "
            f"x={s['all_hit_x']} {names}"
        )


def price_day(day: str, quotes: dict) -> None:
    _p(f"\n=== {day} builder cell, then reprice ===")
    legs = _legs(day)
    _p(f"eligible {len(legs)}")
    focus = {"pitcher_ks", "hits_allowed", "pitching_outs", "threes", "pts+ast", "earned_runs"}
    for rec in legs:
        prop = rec.get("prop_canon")
        if prop not in focus and str(rec.get("sport") or "").upper() != "MLB":
            continue
        if str(rec.get("sport") or "").upper() == "MLB" and prop not in focus:
            continue
        built = BUILDER_CELL.get((str(rec.get("sport") or "").strip(), prop))
        built_s = _band(built[1], built[2]) + f" {built[0]}" if built else rec.get("p_source")
        _p(
            f"  {rec.get('player')} {rec.get('sport')} {rec.get('prop')}: "
            f"builder p={float(rec.get('p') or 0):.3f} n={rec.get('p_n')} "
            f"window={built_s}"
        )
    _apply_reprice(legs)
    _p("repriced legs:")
    for rec in legs:
        key = (str(rec.get("sport") or "").strip(), rec.get("prop_canon"))
        if key not in REPRICE:
            continue
        label, hits, n = REPRICE[key]
        _p(f"  {rec.get('player')} {rec.get('prop')}: {_band(hits, n)} {label}")
    q2 = quotes["2-leg all Goblin"]
    q3 = quotes["3-leg all Goblin"]
    _p(f"quotes 2-leg p25 {q2[0]:.3f}x median {q2[1]:.3f}x n={q2[2]}")
    _p(f"quotes 3-leg p25 {q3[0]:.3f}x median {q3[1]:.3f}x n={q3[2]}")
    _run(legs, "p25", q2[0], q3[0])
    _run(legs, "median", q2[1], q3[1])
    sources = Counter(
        f"{r.get('sport')}|{r.get('prop_canon')}|{float(r.get('p') or 0):.3f}"
        for r in legs
    )
    _p("pool after reprice:")
    for k, n in sources.most_common(30):
        _p(f"  {n}  {k}")


def main() -> None:
    _p("cells the builder reads vs the cells used below")
    for key, (window, h, n) in BUILDER_CELL.items():
        label, rh, rn = REPRICE[key]
        _p(f"  {key[0]} {key[1]}: builder {_band(h, n)} ({window})")
        _p(f"    reprice {_band(rh, rn)} ({label})")
    quotes = _quotes()
    price_day("2026-10-08", quotes)
    price_day("2026-10-07", quotes)


if __name__ == "__main__":
    main()
