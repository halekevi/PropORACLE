"""Refit Games Won on board-opponent attaches; price today's board from cells."""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from datetime import date, timedelta
from itertools import combinations
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import build_goblin70_tickets as g70  # noqa: E402
from utils.category_hit_rate import load_category_hr_index, lookup_category_hr  # noqa: E402
from utils.n_correct_payout import (  # noqa: E402
    _percentile,
    capture_composition_samples,
)
from utils.prop_norm import canon_prop  # noqa: E402
from utils.tennis_keep_gates import (  # noqa: E402
    GAMES_WON_STD_DISCOUNT_MIN,
    tennis_std_discount,
)
from utils.ticket_70_pool import goblin_70_eligible, goblin_ticket_p  # noqa: E402
from utils.ticket_ev_build import build_ev_slips, power_multiplier  # noqa: E402
from utils.ticket_keep_registry import rule_for  # noqa: E402
from _wrong_match_and_cap_effect import (  # noqa: E402
    _board_matches,
    _load_board_opp_map,
    _norm,
    _opp_match,
)


def _p(*a):
    print(*a, flush=True)


def _rate(hits: int, n: int) -> str:
    if not n:
        return "n=0"
    p = hits / n
    se = (p * (1 - p) / n) ** 0.5
    return f"{hits}/{n} ({p:.1%}, band {p-1.96*se:.1%}–{p+1.96*se:.1%})"


def _std_maps() -> dict[str, dict[tuple[str, float], float]]:
    """day -> (player_norm, goblin line) -> standard_line from step8."""
    out: dict[str, dict[tuple[str, float], float]] = {}
    root = MAIN / "outputs"
    if not root.is_dir():
        return out
    for day_dir in root.iterdir():
        if not day_dir.is_dir() or len(day_dir.name) != 10:
            continue
        files = list(day_dir.glob("**/step8*tennis*clean*.parquet")) + list(
            day_dir.glob("**/step8*tennis*clean*.xlsx")
        )
        if not files:
            continue
        path = files[0]
        try:
            df = pd.read_parquet(path) if path.suffix == ".parquet" else pd.read_excel(path)
        except Exception:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        pc = cols.get("player") or cols.get("player_name")
        lc = cols.get("line")
        sc = cols.get("standard_line") or cols.get("standard line")
        pr = cols.get("prop_type") or cols.get("prop")
        if not (pc and lc and sc):
            continue
        bucket: dict[tuple[str, float], float] = {}
        for _, r in df.iterrows():
            if pr and canon_prop("TENNIS", str(r.get(pr) or "")) not in {"", "games_won"}:
                if canon_prop("TENNIS", str(r.get(pr) or "")) != "games_won":
                    continue
            try:
                line = float(r.get(lc))
                std = float(r.get(sc))
            except (TypeError, ValueError):
                continue
            bucket[(_norm(r.get(pc)), round(line, 3))] = std
        if bucket:
            out[day_dir.name] = bucket
    return out


def refit_cells() -> None:
    _p("=== CELL REFIT (board-opponent attach only) ===")
    # Reuse actuals player-days
    pairs: dict[tuple[str, str], float] = {}
    seen = set()
    for root in (MAIN / "outputs", ROOT / "outputs"):
        if not root.is_dir():
            continue
        for path in sorted(root.glob("**/actuals_tennis_*.csv")):
            if path.name in seen:
                continue
            seen.add(path.name)
            day = path.stem.replace("actuals_tennis_", "")[:10]
            try:
                df = pd.read_csv(path, dtype=str)
            except Exception:
                continue
            for _, r in df.iterrows():
                if canon_prop("TENNIS", str(r.get("prop_type") or "")) != "games_won":
                    continue
                try:
                    actual = float(r.get("actual"))
                except (TypeError, ValueError):
                    continue
                pairs[(_norm(r.get("player")), day)] = actual
    days = sorted({d for _, d in pairs})
    pull = set()
    for d in days:
        dd = date.fromisoformat(d)
        for off in (-1, 0, 1):
            pull.add((dd + timedelta(days=off)).isoformat())
    boards = {}
    maps = {}
    for i, d in enumerate(sorted(pull)):
        boards[d] = _board_matches(d)
        maps[d] = _load_board_opp_map(d)
        if (i + 1) % 25 == 0 or i + 1 == len(pull):
            _p(f"  loaded {i+1}/{len(pull)}")

    correct: dict[tuple[str, str], float] = {}
    for (player, day), stored in pairs.items():
        board = ""
        for d in (
            day,
            (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
            (date.fromisoformat(day) + timedelta(days=1)).isoformat(),
        ):
            found = (maps.get(d) or {}).get(player, "")
            if found and "UNKNOWN" not in found.upper():
                board = found
                break
        if not board:
            continue
        cands = []
        for d in (
            day,
            (date.fromisoformat(day) + timedelta(days=1)).isoformat(),
            (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
        ):
            cands.extend(boards.get(d, {}).get(player) or [])
        matched = [c for c in cands if _opp_match(board, c["opp"]) and not c["retired"]]
        if not matched:
            matched = [c for c in cands if _opp_match(board, c["opp"])]
        if not matched:
            continue
        matched.sort(key=lambda c: (c["retired"], 0 if c["ymd"] == day else 1))
        correct[(player, day)] = float(matched[0]["games"])
    _p(f"player-days with a board-opponent match: {len(correct)}")

    std_by_day = _std_maps()
    templates = MAIN / "ui_runner" / "templates"
    if not templates.is_dir():
        templates = ROOT / "ui_runner" / "templates"
    ungated = Counter()
    gated = Counter()
    no_std = 0
    used = 0
    for path in sorted(templates.glob("graded_props_2026-*.json")):
        day = path.stem.replace("graded_props_", "")[:10]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if not str(row.get("sport") or "").upper().startswith("TENNIS"):
                continue
            if "goblin" not in str(row.get("pick_type") or "").lower():
                continue
            if str(row.get("direction") or "").upper() != "OVER":
                continue
            if canon_prop("TENNIS", str(row.get("prop") or "")) != "games_won":
                continue
            key = (_norm(row.get("player")), day)
            if key not in correct:
                continue
            try:
                line = float(row.get("line"))
            except (TypeError, ValueError):
                continue
            gw = correct[key]
            hit = gw > line
            ungated["HIT" if hit else "MISS"] += 1
            used += 1
            std = row.get("standard_line")
            try:
                std_f = float(std) if std is not None else None
            except (TypeError, ValueError):
                std_f = None
            if std_f is None:
                std_f = (std_by_day.get(day) or {}).get((_norm(row.get("player")), round(line, 3)))
            if std_f is None:
                no_std += 1
                continue
            disc = tennis_std_discount(
                {
                    "line": line,
                    "standard_line": std_f,
                    "side": "OVER",
                    "pick_type": "Goblin",
                    "prop": "games_won",
                    "sport": "TENNIS",
                }
            )
            if disc is not None and disc >= GAMES_WON_STD_DISCOUNT_MIN:
                gated["HIT" if hit else "MISS"] += 1
    uh, un = ungated["HIT"], ungated["HIT"] + ungated["MISS"]
    gh, gn = gated["HIT"], gated["HIT"] + gated["MISS"]
    _p(f"verified ungated Goblin OVER Games Won: {_rate(uh, un)}")
    _p(f"verified keep (Standard−Goblin>=4): {_rate(gh, gn)}")
    _p(f"catalog comparison point: 76.3% ungated (1322/1733)")
    _p(f"rows used {used}; keep rows missing standard_line {no_std}")
    if gn and un:
        _p(f"keep minus ungated on this set: {gh/gn - uh/un:+.1%}")


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
        out[label] = (
            _percentile(samples, 25),
            _percentile(samples, 50),
            len(samples),
        )
    return out


def price_today(day: str = "2026-10-07") -> None:
    _p(f"\n=== CELL PRICING {day} ===")
    quotes = _quotes()
    for label, (p25, med, n) in quotes.items():
        _p(f"  {label}: p25 {p25:.3f}x  median {med:.3f}x  n={n}")
    board = g70.load_today_board(day, skip_golf=True)
    index = load_category_hr_index()
    gob = []
    sources = Counter()
    for r in board:
        if not goblin_70_eligible(r):
            continue
        if g70.fold_name(r.get("player")) in g70.SKIP_PLAYERS:
            continue
        rec = dict(r)
        rule = rule_for(rec.get("sport"))
        recipe = f"{rec.get('sport')}:{(rule[0] if rule else 'off')}"
        from utils.ticket_ev_build import attach_leg_p

        attach_leg_p(rec, recipe=recipe, index=index)
        if rec.get("p_source") == "gate_cell":
            child = lookup_category_hr(
                index,
                sport=rec.get("sport"),
                prop=rec.get("prop"),
                pick_type=rec.get("pick_type"),
                direction=rec.get("side") or rec.get("direction"),
            )
            if child and child.get("hr") is not None:
                rec["p"] = float(child["hr"])
                rec["p_n"] = float(child.get("n") or 0)
                rec["p_source"] = "gate_cell"
        if rec.get("p") is None or rec.get("p_source") != "gate_cell":
            rec["p"] = goblin_ticket_p(rec)
            rec["p_source"] = "l5eq5_floor"
        sources[f"{rec.get('sport')}|{canon_prop(str(rec.get('sport') or ''), str(rec.get('prop') or ''))}|{rec['p_source']}|{rec['p']:.3f}"] += 1
        gob.append(rec)
    _p(f"eligible legs {len(gob)}")
    for k, n in sources.most_common(20):
        _p(f"  {n}  {k}")

    def _run(tag: str, q2: float, q3: float) -> None:
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
            slips = build_ev_slips(gob, taken=set(), fold=g70.fold_name)
        finally:
            te.power_multiplier = real
        _p(f"  {tag}: {len(slips)} slips")
        for s in slips[:8]:
            names = ", ".join(
                f"{leg.get('player')} {leg.get('prop')} p={float(leg.get('p') or 0):.2f}"
                for leg in s["legs"]
            )
            _p(
                f"    {s['product']} {len(s['legs'])} ev={s['ev']:+.3f} "
                f"x={s['all_hit_x']} {names}"
            )

    q2 = quotes["2-leg all Goblin"]
    q3 = quotes["3-leg all Goblin"]
    _run("p25", q2[0], q3[0])
    _run("median", q2[1], q3[1])


def main() -> None:
    if "--price-only" not in sys.argv:
        refit_cells()
    price_today("2026-10-07")


if __name__ == "__main__":
    main()
