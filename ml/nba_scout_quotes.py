#!/usr/bin/env python3
"""NBA Goblin OVER: generic gate vs proj_edge scout, priced at scraped quotes.

score_nba_stat_projections --backtest prices both books at the old 2.5x
Goblin breakeven (0.737). This reruns the same walk-forward scoring and
reprices GATE (L5=5 + L10>=8 + D) against daily top-N proj_edge at the live
quotes in ml.quotes (Power-2 1.40x, Power-3 1.65x / 2.00x / 2.50x).

The earlier n=0 scout came from calling attach_nba_projections without the
walk-forward cell priors: with no proj_hold_cell column every leg is held and
_daily_topn has no candidates. This passes lookup= and index= like the
backtest does.

  py -3.14 -m ml.nba_scout_quotes [--from 2026-02-19] [--to 2026-10-22]
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from ml.quotes import breakeven, ev_at, quotes_for_book  # noqa: E402
from score_nba_stat_projections import (  # noqa: E402
    _attach_gate,
    _daily_topn,
    _hitmiss_binary,
    _leg_key,
    _load_graded,
    _maybe_fit_k,
    _pt_label,
    attach_nba_projections,
    wilson_interval,
)
from utils.nba_player_projection import DEFAULT_DB, NbaBoxIndex  # noqa: E402
from utils.expanding_marginals import walk_forward_lookups_by_date  # noqa: E402

OUT_DIR = _REPO / "data" / "reports" / "ml_discover_gates"

# Set by main() from --book; sleeves and by-prop tables price against it.
BOOK = "Goblin OVER"
QUOTES = quotes_for_book(BOOK)


def _book_pick() -> str:
    return "Goblin" if BOOK.lower().startswith("goblin") else "Standard"


def _sleeve(frame: pd.DataFrame, label: str, goblin_only: bool = True) -> dict:
    if frame is None or frame.empty:
        return {"label": label, "n": 0}
    tmp = frame.copy()
    if goblin_only and "pick_type" in tmp.columns:
        tmp = tmp.loc[tmp["pick_type"].map(_pt_label).eq(_book_pick())]
    y = _hitmiss_binary(tmp)
    ok = y.isin([0.0, 1.0])
    y = y.loc[ok]
    n = int(len(y))
    h = int(y.sum())
    p, lo, hi = wilson_interval(float(h), float(n))
    rec = {
        "label": label,
        "n": n,
        "hits": h,
        "hr": round(p, 4) if n else None,
        "wilson_low": round(lo, 4) if n else None,
        "wilson_high": round(hi, 4) if n else None,
        "quotes": {},
    }
    for name, legs, px in QUOTES:
        be = round(breakeven(legs, px), 4)
        rec["quotes"][name] = {
            "breakeven": be,
            "ev": ev_at(p, legs, px) if n else None,
            "point_clears": bool(n and p >= be),
            "wilson_clears": bool(n and lo >= be),
        }
    return rec


def _by_prop(frame: pd.DataFrame, min_n: int = 20) -> list[dict]:
    if frame is None or frame.empty:
        return []
    tmp = frame.copy()
    if "pick_type" in tmp.columns:
        tmp = tmp.loc[tmp["pick_type"].map(_pt_label).eq(_book_pick())]
    tmp["_y"] = _hitmiss_binary(tmp)
    tmp = tmp.loc[tmp["_y"].isin([0.0, 1.0])]
    col = "prop" if "prop" in tmp.columns else ("stat_type" if "stat_type" in tmp.columns else None)
    if col is None:
        return []
    q_primary, q_obs = QUOTES[0], QUOTES[1]
    rows = []
    for prop, g in tmp.groupby(tmp[col].astype(str).str.strip(), sort=False):
        n = len(g)
        if n < min_n:
            continue
        h = int(g["_y"].sum())
        p, lo, _ = wilson_interval(float(h), float(n))
        rows.append(
            {
                "prop": prop,
                "n": n,
                "hr": round(p, 4),
                "wilson_low": round(lo, 4),
                "clears_primary": bool(lo >= breakeven(q_primary[1], q_primary[2])),
                "clears_observe": bool(lo >= breakeven(q_obs[1], q_obs[2])),
            }
        )
    rows.sort(key=lambda r: (-r["wilson_low"], -r["n"]))
    return rows


def _row(rec: dict) -> str:
    cols = len(QUOTES)
    if not rec or not rec.get("n"):
        return f"| {rec.get('label', '?')} | — | 0 | — |" + " — |" * cols + " no |"
    q = rec["quotes"]
    evs = " | ".join(str(q[name]["ev"]) for name, _, _ in QUOTES)
    primary = QUOTES[1][0] if len(QUOTES) > 1 else QUOTES[0][0]
    return (
        f"| {rec['label']} | {100 * rec['hr']:.1f}% | {rec['n']} | {rec['wilson_low']} | "
        f"{evs} | {'yes' if q[primary]['wilson_clears'] else 'no'} |"
    )


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", dest="date_from", default="")
    ap.add_argument("--to", dest="date_to", default="")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--templates", default=str(_REPO / "ui_runner" / "templates"))
    ap.add_argument("--top-k", type=int, default=3, help="Also score the daily top-K proj_edge legs")
    ap.add_argument(
        "--book",
        default="Goblin OVER",
        choices=["Goblin OVER", "Standard OVER", "Standard UNDER"],
        help="Book to score; Standard books price at 3x / 6x",
    )
    args = ap.parse_args(argv)
    global BOOK, QUOTES
    BOOK = args.book
    QUOTES = quotes_for_book(BOOK)
    book_pick = _book_pick()
    book_dir = "OVER" if BOOK.endswith("OVER") else "UNDER"

    graded = _load_graded(Path(args.templates))
    sport = graded.get("sport", pd.Series("", index=graded.index)).astype(str).str.strip().str.upper()
    nba = graded.loc[sport.eq("NBA")].copy()
    fd = nba["file_date"].astype(str).str[:10]
    if args.date_from:
        nba = nba.loc[fd >= args.date_from[:10]]
        fd = nba["file_date"].astype(str).str[:10]
    if args.date_to:
        nba = nba.loc[fd <= args.date_to[:10]]
    if nba.empty:
        print("[scout] no NBA graded rows")
        return 1

    index = NbaBoxIndex(args.db)
    if index.empty():
        print("[scout] empty box DB — everything holds; aborting")
        return 1
    gd = index.df["game_date"].astype(str).str[:10]
    print(f"[scout] box window {gd.min()} -> {gd.max()} n={len(index.df)}")
    _maybe_fit_k(index, "2026-01-01")

    print("[scout] walk-forward cell priors...")
    lookups = walk_forward_lookups_by_date(graded.loc[sport.eq("NBA")])
    dates = sorted({str(x)[:10] for x in nba["file_date"].astype(str)})
    parts = []
    for i, d in enumerate(dates):
        day = nba.loc[nba["file_date"].astype(str).str[:10].eq(d)].copy()
        scored = attach_nba_projections(
            day, as_of=d, db_path=Path(args.db), index=index, lookup=lookups.get(d, {})
        )
        scored["_gate"] = _attach_gate(scored, index, d)
        parts.append(scored)
        if (i + 1) % 10 == 0 or i == len(dates) - 1:
            print(f"[scout] scored {i + 1}/{len(dates)} ({d})")
    out = pd.concat(parts, ignore_index=True)
    pt = out["pick_type"].map(_pt_label) if "pick_type" in out.columns else pd.Series("Standard", index=out.index)
    direction = out["direction"].astype(str).str.upper() if "direction" in out.columns else pd.Series("", index=out.index)
    book_mask = pt.eq(book_pick) & direction.eq(book_dir)
    out = out.loc[book_mask].copy()
    hold = pd.to_numeric(out["proj_hold"], errors="coerce").fillna(1.0)
    y = _hitmiss_binary(out)
    print(
        f"[scout] {BOOK} rows={len(out)} hit/miss={int(y.isin([0, 1]).sum())} "
        f"held={hold.eq(1).mean():.1%} proj_edge_filled={pd.to_numeric(out['proj_edge'], errors='coerce').notna().sum()}"
    )

    gate_df, top_df = _daily_topn(out, "proj_edge")
    _, ev_df = _daily_topn(out, "proj_ev_edge")

    # Fixed daily top-K regardless of gate volume (what a 3-leg slip would take)
    topk_parts = []
    for _, day in out.groupby(out["file_date"].astype(str).str[:10], sort=True):
        yy = _hitmiss_binary(day)
        cand = day.loc[
            pd.to_numeric(day["proj_hold"], errors="coerce").fillna(1).eq(0)
            & yy.isin([0.0, 1.0])
            & pd.to_numeric(day["proj_edge"], errors="coerce").notna()
        ]
        if not cand.empty:
            topk_parts.append(cand.nlargest(args.top_k, "proj_edge", keep="first"))
    topk_df = pd.concat(topk_parts, ignore_index=True) if topk_parts else pd.DataFrame()

    both_df = only_df = pd.DataFrame()
    if not gate_df.empty and not top_df.empty:
        gset = {_leg_key(r) for _, r in gate_df.iterrows()}
        tkeys = top_df.apply(_leg_key, axis=1)
        both_df = top_df.loc[tkeys.isin(gset)]
        only_df = top_df.loc[~tkeys.isin(gset)]

    # Gate pool split by proj_edge median (does the scout stack the gate?)
    g2 = gate_df.copy()
    hi_df = lo_df = pd.DataFrame()
    if not g2.empty:
        g2["_edge"] = pd.to_numeric(g2["proj_edge"], errors="coerce")
        med = float(g2["_edge"].median()) if g2["_edge"].notna().any() else 0.0
        hi_df = g2.loc[g2["_edge"] >= med]
        lo_df = g2.loc[g2["_edge"] < med]

    sleeves = [
        _sleeve(out, f"ungated {BOOK}"),
        _sleeve(gate_df, "GATE L5=5 + L10>=8 + D"),
        _sleeve(top_df, "scout top-N proj_edge (matched gate volume)"),
        _sleeve(ev_df, "scout top-N proj_ev_edge (matched)"),
        _sleeve(topk_df, f"scout top-{args.top_k}/day proj_edge"),
        _sleeve(both_df, "gate AND scout"),
        _sleeve(only_df, "scout minus gate"),
        _sleeve(hi_df, "GATE high proj_edge half"),
        _sleeve(lo_df, "GATE low proj_edge half"),
    ]
    by_prop_gate = _by_prop(gate_df)
    by_prop_scout = _by_prop(top_df)

    payload = {
        "window": [dates[0], dates[-1]],
        "n_dates": len(dates),
        "sleeves": sleeves,
        "gate_by_prop": by_prop_gate,
        "scout_by_prop": by_prop_scout,
        "book": BOOK,
        "breakevens": {name: round(breakeven(l, x), 4) for name, l, x in QUOTES},
    }
    slug = BOOK.lower().replace(" ", "_")
    stem = "nba_scout_quotes" if BOOK == "Goblin OVER" else f"nba_scout_quotes_{slug}"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / f"{stem}.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")

    be_txt = " · ".join(f"{name} {100*breakeven(l, x):.1f}%" for name, l, x in QUOTES)
    ev_hdr = " | ".join(f"EV@{x}x" for _, _, x in QUOTES)
    primary_name = QUOTES[1][0] if len(QUOTES) > 1 else QUOTES[0][0]
    prop_hdr = f"| prop | HR | n | Wilson lo | clears {QUOTES[0][0]} | clears {QUOTES[1][0] if len(QUOTES) > 1 else QUOTES[0][0]} |"
    lines = [
        f"# NBA {BOOK} — generic gate vs proj_edge scout at scraped quotes",
        "",
        f"Window {dates[0]} → {dates[-1]} ({len(dates)} slates). Walk-forward box + cell priors; "
        "hold = thin player or thin cell. Scout candidate pool = unheld legs only.",
        "",
        f"Breakevens: {be_txt}.",
        "",
        f"| sleeve | HR | n | Wilson lo | {ev_hdr} | Wilson clears {primary_name} |",
        "|---|---|---|---|" + "---|" * len(QUOTES) + "---|",
        *(_row(s) for s in sleeves),
        "",
        "## GATE by prop (n>=20)",
        "",
        prop_hdr,
        "|---|---|---|---|---|---|",
        *(
            f"| {r['prop']} | {100*r['hr']:.1f}% | {r['n']} | {r['wilson_low']} | "
            f"{'yes' if r['clears_primary'] else 'no'} | {'yes' if r['clears_observe'] else 'no'} |"
            for r in by_prop_gate
        ),
        "",
        "## Scout top-N by prop (n>=20)",
        "",
        prop_hdr,
        "|---|---|---|---|---|---|",
        *(
            f"| {r['prop']} | {100*r['hr']:.1f}% | {r['n']} | {r['wilson_low']} | "
            f"{'yes' if r['clears_primary'] else 'no'} | {'yes' if r['clears_observe'] else 'no'} |"
            for r in by_prop_scout
        ),
        "",
        f"NBA {BOOK} stays shadow until a sleeve's Wilson lower bound clears the scraped quote.",
        "",
    ]
    (OUT_DIR / f"{stem}.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines[6:6 + 2 + len(sleeves)]))
    print(f"wrote {OUT_DIR / f'{stem}.md'}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
