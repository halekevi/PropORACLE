#!/usr/bin/env python3
"""Standard UNDER candidate checks before anything is believed.

The all-sports gate search found the only non-artifact edge on Standard
UNDER (2-pick Power 3x, breakeven 57.7%/leg): NHL `L10>=8` 70.9% n=1266 and
NFL generic 84.6% n=136. Four checks each, nothing wired:

  1. prop x line breakdown — is the rate a 0.5-line counting artifact?
  2. quote reality — PrizePicks Standard Power is flat (2-leg 3x / 3-leg 6x);
     count real scraped all-Standard quotes and flag legs whose stored line
     differs from the board Standard line (adjusted face mislabelled Standard).
  3. walk-forward by date — chronological OOS and a rolling by-month table;
     Wilson lower bound must clear 0.577 on dates the gate never saw.
  4. correlation — empirical same-date pair hit rate for same-game pairs vs
     different-game pairs vs p**2 inside the slice.

  py -3.14 -m ml.standard_under_checks            # NHL + NFL
  py -3.14 -m ml.standard_under_checks --sport NHL --gate "L10>=8"
"""
from __future__ import annotations

import argparse
import itertools
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from discover_gates_sport import (  # noqa: E402
    build_atoms,
    iter_sport_rows,
    oos_split_dates,
    passes,
    rate,
    wilson_low,
)
from ml.quotes import STANDARD_QUOTES, breakeven  # noqa: E402

OUT_DIR = _REPO / "data" / "reports" / "ml_discover_gates"
BE2 = round(breakeven(2, 3.0), 4)  # 0.5774
BE3 = round(breakeven(3, 6.0), 4)  # 0.5503

DEFAULT_GATES = {
    "NHL": ("L10>=8",),
    "NFL": ("L5=5 + L10>=8 + D_ok", "L10>=8 + cover>=1", "L10>=8"),
}

GATE_ATOMS = {
    "L10>=8": ("L10>=8",),
    "L5=5 + L10>=8": ("L5=5", "L10>=8"),
    "L5=5 + L10>=8 + D_ok": ("L5=5", "L10>=8", "D_ok"),
    "L10>=8 + cover>=1": ("L10>=8", "cover>=1"),
    "L10>=8 + D_ok": ("L10>=8", "D_ok"),
}


def _cell(pool: list[dict]) -> dict:
    n = len(pool)
    h = sum(r["hit"] for r in pool)
    lo = wilson_low(h, n)
    return {"n": n, "hits": h, "hr": rate(h, n), "wilson_low": lo, "clears_3x_wilson": bool(lo is not None and lo >= BE2)}


def _game_key(r: dict) -> str:
    team = str(r.get("team") or "").strip().upper()
    opp = str(r.get("opp_team") or "").strip().upper()
    if team and opp:
        return "|".join(sorted((team, opp)))
    return ""


def check_prop_line(pool: list[dict]) -> dict:
    by_prop: dict[str, list[dict]] = defaultdict(list)
    by_prop_line: dict[tuple[str, str], list[dict]] = defaultdict(list)
    half = []
    other = []
    for r in pool:
        by_prop[r["prop"]].append(r)
        line = r.get("line")
        lk = "0.5" if line is not None and abs(line - 0.5) < 1e-9 else ("1.5" if line is not None and abs(line - 1.5) < 1e-9 else "2.5+")
        by_prop_line[(r["prop"], lk)].append(r)
        (half if lk == "0.5" else other).append(r)
    props = []
    for prop, grp in sorted(by_prop.items(), key=lambda kv: -len(kv[1])):
        c = _cell(grp)
        c["prop"] = prop
        c["lines"] = {
            lk: _cell(g2) for (p2, lk), g2 in sorted(by_prop_line.items()) if p2 == prop and len(g2) >= 10
        }
        props.append(c)
    return {
        "line_0.5": _cell(half),
        "line_not_0.5": _cell(other),
        "share_0.5": round(len(half) / max(len(pool), 1), 3),
        "by_prop": props,
    }


def check_quote(pool: list[dict]) -> dict:
    """Flat Standard quotes; flag mislabelled adjusted faces."""
    scrapes = []
    legs_path = _REPO / "data" / "reports" / "payout_scrape_legs.jsonl"
    if legs_path.is_file():
        for ln in legs_path.read_text(encoding="utf-8").splitlines():
            if not ln.strip():
                continue
            try:
                rec = json.loads(ln)
            except Exception:
                continue
            if int(rec.get("n_g") or 0) == 0 and int(rec.get("n_d") or 0) == 0:
                scrapes.append(
                    {
                        "date": rec.get("date"),
                        "n_legs": rec.get("n_legs"),
                        "power_x": rec.get("power_min_x"),
                        "captured_at": rec.get("captured_at") or "",
                        "sports": sorted({str(l.get("sport")) for l in rec.get("legs") or []}),
                    }
                )
    # Mislabelled adjusted face: Standard leg whose line != board standard_line
    mismatch = []
    have_std = 0
    for r in pool:
        sl = r.get("standard_line")
        if sl is None or r.get("line") is None:
            continue
        have_std += 1
        if abs(float(sl) - float(r["line"])) > 1e-9:
            mismatch.append(r)
    # book_label maps blank / dash pick_type to Standard. Only a literal
    # "Standard" label is a verified flat-quote face.
    labelled = [r for r in pool if str(r.get("pick_type_raw") or "").strip().lower() == "standard"]
    unlabelled = [r for r in pool if r not in labelled]
    return {
        "flat_quote": {"power2_x": 3.0, "power3_x": 6.0, "breakeven_2": BE2, "breakeven_3": BE3},
        "scraped_all_standard": scrapes,
        "scraped_all_standard_with_capture": sum(1 for s in scrapes if s["captured_at"]),
        "legs_with_board_standard_line": have_std,
        "legs_line_ne_standard_line": len(mismatch),
        "mismatch_hr": _cell(mismatch) if mismatch else None,
        "labelled_standard": _cell(labelled),
        "unlabelled_pick_type": _cell(unlabelled) if unlabelled else None,
        "note": (
            "PrizePicks all-Standard Power pays a flat 3x (2-pick) / 6x (3-pick) by product rule; "
            "the only thing that can be wrong is the face label. A Standard leg whose line is "
            "not the board Standard line is an adjusted face recorded as Standard."
        ),
    }


def check_walk_forward(pool: list[dict], all_dates: list[str]) -> dict:
    train, test = oos_split_dates(all_dates)
    tr = [r for r in pool if r["date"] in train]
    te = [r for r in pool if r["date"] in test]
    by_month: dict[str, list[dict]] = defaultdict(list)
    for r in pool:
        by_month[r["date"][:7]].append(r)
    months = []
    for m, grp in sorted(by_month.items()):
        c = _cell(grp)
        c["month"] = m
        months.append(c)
    # Expanding walk-forward: for each month, gate judged only on prior months' rate
    expanding = []
    seen: list[dict] = []
    for m, grp in sorted(by_month.items()):
        prior = _cell(seen) if seen else None
        cur = _cell(grp)
        expanding.append(
            {
                "month": m,
                "prior_hr": prior["hr"] if prior else None,
                "prior_n": prior["n"] if prior else 0,
                "month_hr": cur["hr"],
                "month_n": cur["n"],
                "prior_said_play": bool(prior and prior["wilson_low"] is not None and prior["wilson_low"] >= BE2),
            }
        )
        seen.extend(grp)
    played = [e for e in expanding if e["prior_said_play"]]
    played_rows = [r for r in pool if r["date"][:7] in {e["month"] for e in played}]
    # Season type by calendar (NHL / NBA playoffs mid-Apr → Jun; NFL/CFB have none here).
    rs = [r for r in pool if not _is_playoff_month(r["date"])]
    po = [r for r in pool if _is_playoff_month(r["date"])]
    return {
        "train": _cell(tr),
        "oos": _cell(te),
        "oos_dates": sorted(test)[:3] + (["..."] if len(test) > 3 else []),
        "by_month": months,
        "expanding": expanding,
        "forward_only_when_prior_cleared": _cell(played_rows) if played_rows else None,
        "regular_season": _cell(rs),
        "playoff_window": _cell(po) if po else None,
    }


def _is_playoff_month(date: str) -> bool:
    """Apr 15 → Jun 30 (NHL / NBA postseason calendar)."""
    md = date[5:10]
    return "04-15" <= md <= "06-30"


def check_correlation(pool: list[dict]) -> dict:
    by_date: dict[str, list[dict]] = defaultdict(list)
    for r in pool:
        by_date[r["date"]].append(r)
    same_game = []  # joint hit of pairs
    diff_game = []
    same_team = []
    same_game_exp: list[float] = []
    diff_game_exp: list[float] = []
    same_team_exp: list[float] = []
    for d, legs in by_date.items():
        if len(legs) < 2 or len(legs) > 80:
            continue
        p_d = sum(r["hit"] for r in legs) / len(legs)
        for a, b in itertools.combinations(legs, 2):
            if a.get("player_norm") == b.get("player_norm"):
                continue
            joint = a["hit"] * b["hit"]
            ga, gb = _game_key(a), _game_key(b)
            if ga and gb and ga == gb:
                same_game.append(joint)
                same_game_exp.append(p_d * p_d)
                if str(a.get("team") or "").upper() == str(b.get("team") or "").upper():
                    same_team.append(joint)
                    same_team_exp.append(p_d * p_d)
            else:
                diff_game.append(joint)
                diff_game_exp.append(p_d * p_d)
    p = rate(sum(r["hit"] for r in pool), len(pool)) or 0.0

    def _pair(vals: list[int], expected: list[float]) -> dict:
        n = len(vals)
        h = sum(vals)
        exp = (sum(expected) / n) if n else None
        return {
            "pairs": n,
            "joint_hr": rate(h, n),
            "independent_p2_pooled": round(p * p, 4),
            "independent_date_matched": round(exp, 4) if exp is not None else None,
            "lift_vs_date_matched": round((h / n) / exp, 3) if n and exp else None,
            "ev_3x": round(3.0 * (h / n) - 1.0, 4) if n else None,
        }

    return {
        "leg_p": p,
        "same_game_pairs": _pair(same_game, same_game_exp),
        "same_team_pairs": _pair(same_team, same_team_exp),
        "different_game_pairs": _pair(diff_game, diff_game_exp),
        "note": (
            "Pairs are all same-date combinations inside the slice (not optimised slips). "
            "Date-matched baseline = that date's p_d^2, so a hot month with many legs does not "
            "masquerade as correlation."
        ),
    }


def run(sport: str, gate: str) -> dict:
    rows, meta = iter_sport_rows(sport)
    under = [r for r in rows if r["book"] == "Standard UNDER"]
    all_dates = sorted({r["date"] for r in under})
    atoms = GATE_ATOMS[gate]
    pool = [r for r in under if passes(build_atoms(r), atoms)]
    out = {
        "sport": sport,
        "gate": gate,
        "ungated": _cell(under),
        "slice": _cell(pool),
        "dates": [all_dates[0], all_dates[-1]] if all_dates else [],
        "prop_line": check_prop_line(pool),
        "quote": check_quote(pool),
        "walk_forward": check_walk_forward(pool, all_dates),
        "correlation": check_correlation(pool),
    }
    pl = out["prop_line"]
    wf = out["walk_forward"]
    corr = out["correlation"]
    verdict = []
    if pl["share_0.5"] >= 0.5:
        verdict.append(f"{pl['share_0.5']:.0%} of the slice is 0.5-line")
    if pl["line_not_0.5"]["n"] >= 30 and pl["line_not_0.5"]["clears_3x_wilson"]:
        verdict.append("non-0.5 lines still clear 3x on Wilson")
    elif pl["line_not_0.5"]["n"] >= 30:
        verdict.append("non-0.5 lines do NOT clear 3x on Wilson")
    if wf["oos"]["n"] >= 30:
        verdict.append(f"OOS {'clears' if wf['oos']['clears_3x_wilson'] else 'does not clear'} 3x Wilson (n={wf['oos']['n']})")
    else:
        verdict.append(f"OOS too thin (n={wf['oos']['n']})")
    fw = wf.get("forward_only_when_prior_cleared")
    if fw:
        verdict.append(f"forward-only months {100*fw['hr']:.1f}% n={fw['n']} {'clears' if fw['clears_3x_wilson'] else 'misses'} 3x Wilson")
    po = wf.get("playoff_window")
    if po and po["n"] >= 30:
        rs = wf["regular_season"]
        verdict.append(
            f"regular season {100*(rs['hr'] or 0):.1f}% n={rs['n']} vs playoff window {100*(po['hr'] or 0):.1f}% n={po['n']}"
        )
    sg = corr["same_game_pairs"]
    if sg["pairs"] >= 30 and sg["lift_vs_date_matched"] is not None:
        verdict.append(
            f"same-game pair joint {100*sg['joint_hr']:.1f}% vs date-matched p²={100*sg['independent_date_matched']:.1f}% (lift {sg['lift_vs_date_matched']})"
        )
    q = out["quote"]
    if q["legs_line_ne_standard_line"]:
        verdict.append(f"{q['legs_line_ne_standard_line']} legs have line != board Standard line")
    if q.get("unlabelled_pick_type"):
        u = q["unlabelled_pick_type"]
        verdict.append(f"{u['n']} legs have blank/dash pick_type (HR {100*(u['hr'] or 0):.1f}%) — not verified Standard")
    out["verdict"] = verdict
    return out


def _md(res: dict) -> list[str]:
    pl = res["prop_line"]
    wf = res["walk_forward"]
    corr = res["correlation"]
    q = res["quote"]
    s = res["slice"]
    L = [
        f"## {res['sport']} Standard UNDER — `{res['gate']}`",
        "",
        f"Window {res['dates'][0]} → {res['dates'][1]}. Ungated {100*res['ungated']['hr']:.1f}% n={res['ungated']['n']}. "
        f"Slice **{100*s['hr']:.1f}% n={s['n']}** Wilson lo {s['wilson_low']} (3x breakeven {BE2}).",
        "",
        "### 1. Prop × line",
        "",
        f"0.5-line share **{pl['share_0.5']:.0%}** · 0.5-line {100*(pl['line_0.5']['hr'] or 0):.1f}% n={pl['line_0.5']['n']} · "
        f"non-0.5 **{100*(pl['line_not_0.5']['hr'] or 0):.1f}% n={pl['line_not_0.5']['n']}** Wilson {pl['line_not_0.5']['wilson_low']} "
        f"{'clears' if pl['line_not_0.5']['clears_3x_wilson'] else 'misses'} 3x",
        "",
        "| prop | HR | n | Wilson lo | lines (HR n) |",
        "|---|---|---|---|---|",
    ]
    for p in pl["by_prop"]:
        if p["n"] < 15:
            continue
        lines = ", ".join(f"{lk}: {100*(c['hr'] or 0):.0f}% n={c['n']}" for lk, c in p["lines"].items())
        L.append(f"| {p['prop']} | {100*(p['hr'] or 0):.1f}% | {p['n']} | {p['wilson_low']} | {lines} |")
    L += [
        "",
        "### 2. Quote",
        "",
        f"All-Standard Power is flat 3x / 6x. Scraped all-Standard slips on file: {len(q['scraped_all_standard'])} "
        f"({q['scraped_all_standard_with_capture']} with captured_at). "
        f"Legs with a board Standard line: {q['legs_with_board_standard_line']}; line ≠ Standard line: **{q['legs_line_ne_standard_line']}**"
        + (f" (HR {100*q['mismatch_hr']['hr']:.1f}%)" if q.get("mismatch_hr") else ""),
        "",
        f"Literal `Standard` label: {100*(q['labelled_standard']['hr'] or 0):.1f}% n={q['labelled_standard']['n']} "
        f"Wilson {q['labelled_standard']['wilson_low']}"
        + (
            f" · blank/dash label: {100*(q['unlabelled_pick_type']['hr'] or 0):.1f}% n={q['unlabelled_pick_type']['n']} (unverified face)"
            if q.get("unlabelled_pick_type")
            else " · no blank/dash labels"
        ),
        "",
        "### 3. Walk-forward",
        "",
        f"Train {100*(wf['train']['hr'] or 0):.1f}% n={wf['train']['n']} · OOS **{100*(wf['oos']['hr'] or 0):.1f}% n={wf['oos']['n']}** "
        f"Wilson {wf['oos']['wilson_low']} {'clears' if wf['oos']['clears_3x_wilson'] else 'misses'} 3x",
        "",
        "| month | n | HR | Wilson lo | prior HR (n) | prior said play |",
        "|---|---|---|---|---|---|",
    ]
    mo = {m["month"]: m for m in wf["by_month"]}
    for e in wf["expanding"]:
        m = mo[e["month"]]
        L.append(
            f"| {e['month']} | {m['n']} | {100*(m['hr'] or 0):.1f}% | {m['wilson_low']} | "
            f"{(100*e['prior_hr']):.1f}% ({e['prior_n']}) | {'yes' if e['prior_said_play'] else 'no'} |"
            if e["prior_hr"] is not None
            else f"| {e['month']} | {m['n']} | {100*(m['hr'] or 0):.1f}% | {m['wilson_low']} | — | no |"
        )
    fw = wf.get("forward_only_when_prior_cleared")
    if fw:
        L += ["", f"Forward-only (months where the prior record already cleared 3x): **{100*fw['hr']:.1f}% n={fw['n']}** Wilson {fw['wilson_low']}"]
    if wf.get("playoff_window"):
        rs, po = wf["regular_season"], wf["playoff_window"]
        L += [
            "",
            f"Regular-season dates **{100*(rs['hr'] or 0):.1f}% n={rs['n']}** Wilson {rs['wilson_low']} · "
            f"Apr 15–Jun 30 window {100*(po['hr'] or 0):.1f}% n={po['n']} Wilson {po['wilson_low']} "
            "(postseason legs are already blocked from tickets)",
        ]
    L += [
        "",
        "### 4. Correlation (same-date pairs inside the slice)",
        "",
        "| pair type | pairs | joint HR | date-matched p² | lift | EV @3x |",
        "|---|---|---|---|---|---|",
    ]
    for k in ("same_game_pairs", "same_team_pairs", "different_game_pairs"):
        c = corr[k]
        L.append(
            f"| {k.replace('_', ' ')} | {c['pairs']} | {100*(c['joint_hr'] or 0):.1f}% | "
            f"{100*(c['independent_date_matched'] or 0):.1f}% | {c['lift_vs_date_matched']} | {c['ev_3x']} |"
        )
    L += ["", "**Verdict:** " + "; ".join(res["verdict"]), ""]
    return L


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sport", default="")
    ap.add_argument("--gate", default="")
    args = ap.parse_args(argv)
    plan: list[tuple[str, str]] = []
    if args.sport:
        gates = (args.gate,) if args.gate else DEFAULT_GATES.get(args.sport, ("L10>=8",))
        plan = [(args.sport, g) for g in gates]
    else:
        for sp, gates in DEFAULT_GATES.items():
            plan += [(sp, g) for g in gates]
    results = []
    cache: dict[str, None] = {}
    for sp, g in plan:
        print(f"[std-under] {sp} {g}")
        res = run(sp, g)
        results.append(res)
        print("  ", "; ".join(res["verdict"]))
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "standard_under_checks.json").write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    md = [
        "# Standard UNDER candidates — four checks before belief",
        "",
        f"2-pick Power 3x breakeven {BE2}; 3-pick 6x {BE3}. Nothing here is wired. "
        "Promotion bar is the 10-11 forward-shadow rule.",
        "",
    ]
    for res in results:
        md += _md(res)
    (OUT_DIR / "standard_under_checks.md").write_text("\n".join(md), encoding="utf-8")
    print(f"wrote {OUT_DIR / 'standard_under_checks.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
