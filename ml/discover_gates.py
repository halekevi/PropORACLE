#!/usr/bin/env python3
"""Discover readable gates across every sport, scored at scraped quotes.

  py -3.14 -m ml.discover_gates
  py -3.14 -m ml.discover_gates --sport NBA --book "Goblin OVER"

Does not wire keep_gates. NBA G70 stays shadow until a slice clears Wilson
at a scraped quote on dates the search has not seen.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from discover_gates_sport import (  # noqa: E402
    BOOKS,
    SPORT_CFG,
    build_atoms,
    gate_catalog,
    iter_sport_rows,
    oos_split_dates,
    passes,
    rate,
    wilson_low,
)
from ml.quotes import GOBLIN_QUOTES, STANDARD_QUOTES, breakeven, score_quotes  # noqa: E402
from ml.tools import binning as tool_binning  # noqa: E402
from ml.tools import rules as tool_rules  # noqa: E402
from ml.tools import subgroups as tool_subgroups  # noqa: E402
from ml.tools import trees as tool_trees  # noqa: E402
from utils.discover_gates_split_balance import split_balance_for_cell  # noqa: E402

OUT_DIR = _REPO / "data" / "reports" / "ml_discover_gates"
GENERIC = "L5=5 + L10>=8 + D_ok"
GENERIC_ATOMS = ("L5=5", "L10>=8", "D_ok")
GOLFLIKE = "L5=5 + L10>=8"
GOLFLIKE_ATOMS = ("L5=5", "L10>=8")
FEAT_ATOMS = (
    "L5=5",
    "L10>=8",
    "D_ok",
    "cover>=1",
    "cover>=2",
    "minutes_high",
    "role_starter",
    "line_0.5",
    "season_HR>70%",
)


def _tool_versions() -> dict[str, str]:
    return {
        "pysubgroup": tool_subgroups.available(),
        "imodels": tool_rules.available(),
        "optbinning": tool_binning.available(),
        "sklearn": tool_trees.available(),
        "shap": _ver("shap"),
        "lightgbm": _ver("lightgbm"),
    }


def _ver(name: str) -> str:
    try:
        mod = __import__(name)
        return getattr(mod, "__version__", "present")
    except Exception:
        return "MISSING"


def _binom_p(hits: int, n: int, p0: float) -> float | None:
    if n <= 0:
        return None
    try:
        from scipy.stats import binomtest

        return float(binomtest(hits, n, p=p0, alternative="greater").pvalue)
    except Exception:
        return None


def _bh_keep(pvals: list[float | None], alpha: float = 0.10) -> set[int]:
    indexed = [(i, p) for i, p in enumerate(pvals) if p is not None]
    m = len(indexed)
    if m == 0:
        return set()
    indexed.sort(key=lambda t: t[1])
    cutoff = 0
    for rank, (_i, p) in enumerate(indexed, 1):
        if p <= (rank / m) * alpha:
            cutoff = rank
    return {indexed[j][0] for j in range(cutoff)}


def _score_pool(pool: list[dict], train: set[str], test: set[str], book: str) -> dict:
    h = sum(int(r["hit"]) for r in pool)
    n = len(pool)
    oh = sum(int(r["hit"]) for r in pool if r["date"] in test)
    on = sum(1 for r in pool if r["date"] in test)
    th = sum(int(r["hit"]) for r in pool if r["date"] in train)
    tn = sum(1 for r in pool if r["date"] in train)
    hr = rate(h, n)
    ohr = rate(oh, on)
    wlo = wilson_low(h, n)
    rec = {
        "hr": hr,
        "n": n,
        "hits": h,
        "wilson_low": wlo,
        "train_hr": rate(th, tn),
        "train_n": tn,
        "oos_hr": ohr,
        "oos_n": on,
        "quotes": score_quotes(hr, n, oos_hr=ohr, oos_n=on, wilson_low=wlo, book=book),
    }
    bal = split_balance_for_cell(pool, train, test)
    rec["split_flag"] = bool(bal.get("flagged") or (tn < 15 and on >= 25))
    rec["train_share"] = bal.get("train_share")
    return rec


def _feat_frame(rows: list[dict]) -> pd.DataFrame:
    recs = []
    for r in rows:
        a = build_atoms(r)
        recs.append(
            {
                "hit": int(r["hit"]),
                "date": r["date"],
                "prop": str(r.get("prop") or ""),
                "line": float(r["line"] or 0),
                **{k: int(bool(a.get(k))) for k in FEAT_ATOMS},
            }
        )
    return pd.DataFrame(recs)


def _optional_tools(rows: list[dict], train: set[str], enabled: bool, use_pysubgroup: bool = False) -> dict:
    if not enabled or not rows:
        return {"skipped": True}
    fdf = _feat_frame(rows)
    tr = fdf["date"].isin(train)
    train_df = fdf.loc[tr]
    if len(train_df) > 8000:
        train_df = train_df.sample(8000, random_state=0)
    names = [c for c in FEAT_ATOMS if c in train_df.columns]
    X = train_df[names].to_numpy(dtype=float) if len(train_df) else None
    y = train_df["hit"].to_numpy(dtype=int) if len(train_df) else None
    sg_cols = [c for c in names if c != "line"]
    sg_df = train_df[["hit", *sg_cols]].copy() if len(train_df) else None
    return {
        "pysubgroup": (
            tool_subgroups.discover(sg_df, ignore=(), depth=2, k=8, min_n=40)
            if use_pysubgroup
            else {"status": "skipped", "rules": []}
        ),
        "figs": tool_rules.fit_figs(X, y, max_rules=6, feature_names=names),
        "optbinning_line": tool_binning.bin_feature(
            train_df["line"].to_numpy() if len(train_df) else None,
            y,
            name="line",
            min_n=40,
        ),
        "sklearn_tree": tool_trees.fit_depth3(X, y, feature_names=names, min_leaf=25),
    }


def _prop_breakdown(pool: list[dict], train: set[str], test: set[str], book: str) -> list[dict]:
    by: dict[str, list[dict]] = defaultdict(list)
    for r in pool:
        by[str(r.get("prop") or "?")].append(r)
    out = []
    for prop, grp in sorted(by.items(), key=lambda kv: -len(kv[1])):
        rec = _score_pool(grp, train, test, book)
        rec["prop"] = prop
        out.append(rec)
    return out


def run_book(sport: str, book: str, rows: list[dict], *, min_n: int, optional: bool, use_pysubgroup: bool = False) -> dict:
    dates = sorted({r["date"] for r in rows})
    train, test = oos_split_dates(dates)
    catalog = []
    p0 = breakeven(3, 1.65) if book.startswith("Goblin") else breakeven(2, 3.00)
    pvals: list[float | None] = []
    unique_gates = gate_catalog()
    for name, atoms in unique_gates:
        pool = [r for r in rows if passes(build_atoms(r), atoms)]
        if len(pool) < min_n:
            continue
        rec = _score_pool(pool, train, test, book)
        rec["gate"] = name
        rec["p_vs_quote"] = _binom_p(rec["hits"], rec["n"], p0)
        catalog.append(rec)
        pvals.append(rec["p_vs_quote"])
    keep = _bh_keep(pvals, alpha=0.10)
    for i, rec in enumerate(catalog):
        rec["bh_keep"] = i in keep
    catalog.sort(
        key=lambda g: (
            -int(bool(g.get("bh_keep"))),
            -(g.get("wilson_low") or 0),
            -(g.get("hr") or 0),
            -g.get("n", 0),
        )
    )

    def named(atoms, label):
        pool = [r for r in rows if passes(build_atoms(r), atoms)]
        rec = _score_pool(pool, train, test, book)
        rec["gate"] = label
        return rec

    ungated = named((), "all")
    generic = named(GENERIC_ATOMS, GENERIC)
    golflike = named(GOLFLIKE_ATOMS, GOLFLIKE)
    qkey_hi = "power3_1.65x" if book.startswith("Goblin") else "power2_3.00x"
    qkey_obs = "power3_2.00x" if book.startswith("Goblin") else "power3_6.00x"

    def clears(key: str) -> list[dict]:
        return [
            g
            for g in catalog
            if (g.get("quotes") or {}).get(key, {}).get("wilson_clears")
        ]

    line05 = None
    if sport == "NBA" and book == "Goblin OVER":
        pool = [r for r in rows if passes(build_atoms(r), ("L5=5", "L10>=8", "line_0.5"))]
        rec = _score_pool(pool, train, test, book)
        rec["gate"] = "L5=5 + L10>=8 + line_0.5"
        rec["by_prop"] = _prop_breakdown(pool, train, test, book)
        line05 = rec

    return {
        "sport": sport,
        "book": book,
        "n": len(rows),
        "dates": dates,
        "train_dates_n": len(train),
        "oos_dates_n": len(test),
        "oos_dates": sorted(test)[:8] + (["…"] if len(test) > 8 else []),
        "ungated": ungated,
        "generic_stack": generic,
        "l5_l10": golflike,
        "n_catalog": len(catalog),
        "clears_wilson_primary": [g["gate"] for g in clears(qkey_hi)[:12]],
        "clears_wilson_observe": [g["gate"] for g in clears(qkey_obs)[:12]],
        "bh_keep_gates": [g["gate"] for g in catalog if g.get("bh_keep")][:12],
        "top": catalog[:12],
        "line_0_5": line05,
        "optional": _optional_tools(rows, train, optional, use_pysubgroup=use_pysubgroup),
        "note": (
            "Wilson must clear the quote. A point-estimate lift is not enough. "
            "split_flag / train_n<15 means do not wire."
        ),
    }


def _md_row(label: str, rec: dict | None, q_primary: str, q_obs: str) -> str:
    if not rec:
        return f"| {label} | — | — | — | — | — | — |"
    qp = (rec.get("quotes") or {}).get(q_primary) or {}
    qo = (rec.get("quotes") or {}).get(q_obs) or {}
    split = "flag" if rec.get("split_flag") else ""
    return (
        f"| {label} | {100*(rec.get('hr') or 0):.1f}% | {rec.get('n')} | "
        f"{rec.get('wilson_low')} | {qp.get('ev')} | {qo.get('ev')} | {split} |"
    )


def write_outputs(results: list[dict], tools: dict[str, str]) -> Path:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"tools": tools, "books": results}
    (OUT_DIR / "all.json").write_text(json.dumps(payload, indent=2), encoding="utf-8")
    for rec in results:
        slug = f"{rec['sport']}_{rec['book'].replace(' ', '_').lower()}"
        (OUT_DIR / f"{slug}.json").write_text(json.dumps(rec, indent=2), encoding="utf-8")

    lines = [
        "# Gate discovery — all sports vs scraped quotes",
        "",
        "Search target is EV at the real quote, not hit rate. "
        "Nothing here is wired into keep_gates. NBA G70 stays shadow.",
        "",
        "Face filter (2026-10-24): rows on sport-dates whose graded faces were default labels "
        "(zero Goblin/Demon, e.g. NFL 09-09..10-19) or Standard ladders are dropped before scoring. "
        "Earlier NFL figures in this folder (Goblin OVER n=576, Standard UNDER 84.6% n=136) were "
        "computed on those labels and are superseded by the per-book `face_filter` block.",
        "",
        "## Tools",
        "",
        *(f"- `{k}`: {v}" for k, v in tools.items()),
        "",
        "Goblin breakeven: Power-2 1.40x = 84.5%. Power-3 1.65x = 84.6%. "
        "Power-3 2.00x = 79.4%. Power-3 2.50x = 73.7%.",
        "Standard breakeven: Power-2 3x = 57.7%. Power-3 6x = 55.0%.",
        "",
    ]
    by_sport: dict[str, list[dict]] = defaultdict(list)
    for rec in results:
        by_sport[rec["sport"]].append(rec)
    for sport in sorted(by_sport):
        lines += [f"## {sport}", ""]
        for rec in by_sport[sport]:
            gob = rec["book"].startswith("Goblin")
            qp = "power3_1.65x" if gob else "power2_3.00x"
            qo = "power3_2.00x" if gob else "power3_6.00x"
            lines += [
                f"### {rec['book']}  n={rec['n']}  dates={rec['dates'][0] if rec['dates'] else '?'} → {rec['dates'][-1] if rec['dates'] else '?'}",
                "",
                f"| sleeve | HR | n | Wilson lo | EV @ {'1.65x' if gob else '3x'} | EV @ {'2.00x' if gob else '6x'} | split |",
                "|---|---|---|---|---|---|---|",
                _md_row("ungated", rec.get("ungated"), qp, qo),
                _md_row("L5=5 + L10>=8 + D", rec.get("generic_stack"), qp, qo),
                _md_row("L5=5 + L10>=8", rec.get("l5_l10"), qp, qo),
                "",
                f"Wilson-clears primary: **{len(rec.get('clears_wilson_primary') or [])}** "
                f"`{(rec.get('clears_wilson_primary') or ['—'])[:4]}`",
                f"Wilson-clears observe: **{len(rec.get('clears_wilson_observe') or [])}** "
                f"`{(rec.get('clears_wilson_observe') or ['—'])[:4]}`",
                f"BH-keep vs quote: `{(rec.get('bh_keep_gates') or ['—'])[:6]}`",
                "",
            ]
            if rec.get("top"):
                lines += [
                    "| HR | n | Wilson lo | train_n | oos_n | flag | gate |",
                    "|---|---|---|---|---|---|---|",
                ]
                for g in rec["top"][:8]:
                    lines.append(
                        f"| {100*(g.get('hr') or 0):.1f}% | {g.get('n')} | {g.get('wilson_low')} | "
                        f"{g.get('train_n')} | {g.get('oos_n')} | "
                        f"{'flag' if g.get('split_flag') else ''} | `{g.get('gate')}` |"
                    )
                lines.append("")
            opt = rec.get("optional") or {}
            if opt.get("sklearn_tree", {}).get("rules"):
                lines.append("sklearn depth-3 (train dates):")
                for rule in (opt["sklearn_tree"].get("rules") or [])[:4]:
                    lines.append(f"- `{rule.get('rule')}`  HR {100*(rule.get('hr') or 0):.1f}% n={rule.get('n')}")
                lines.append("")
            figs = opt.get("figs") or {}
            if figs.get("status") == "ok" and figs.get("rules"):
                lines.append("FIGS:")
                for rule in figs["rules"][:6]:
                    lines.append(f"- `{rule}`")
                lines.append("")
            sg = opt.get("pysubgroup") or {}
            if sg.get("status") == "ok" and sg.get("rules"):
                lines.append("pysubgroup:")
                for rule in sg["rules"][:5]:
                    lines.append(
                        f"- `{rule.get('rule')}`  HR {100*(rule.get('hr') or 0):.1f}% n={rule.get('n')}"
                    )
                lines.append("")
            if rec.get("line_0_5"):
                lines += ["NBA `L5=5 + L10>=8 + line_0.5` by prop (not a general G70 sleeve):", ""]
                lines += ["| prop | HR | n | Wilson lo | train_n |", "|---|---|---|---|---|"]
                for p in rec["line_0_5"].get("by_prop") or []:
                    lines.append(
                        f"| {p.get('prop')} | {100*(p.get('hr') or 0):.1f}% | {p.get('n')} | "
                        f"{p.get('wilson_low')} | {p.get('train_n')} |"
                    )
                lines.append("")
    lines += [
        "## Decision",
        "",
        "NBA Goblin-70 stays shadow. Do not wire a catalog slice that only clears "
        "because it is a 0.5-line counting prop or because train_n is near zero.",
        "",
        "Scout (proj_edge) vs generic gate at scraped quotes: `py -3.14 -m ml.nba_scout_quotes` "
        "→ `nba_scout_quotes.md` in this folder.",
        "",
    ]
    summary = OUT_DIR / "summary.md"
    summary.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return summary


_FACE_STATUS_CACHE: dict[str, set[tuple[str, str]]] = {}


def unverified_face_dates() -> set[tuple[str, str]]:
    """(SPORT, date) pairs whose graded faces are default labels or Standard ladders.

    Built from the leg ledger once per run (utils.face_ladder). NFL 09-09..10-19,
    the Feb-Jun NHL / Soccer / Tennis blank-label spans, CBB late Feb-Mar and the
    NBA playoff ladders all land here. Rows on those dates cannot tell a Goblin
    rung from a Standard face, so no gate may be scored on them.
    """
    if "all" in _FACE_STATUS_CACHE:
        return _FACE_STATUS_CACHE["all"]
    try:
        from proporacle.data.leg_ledger import face_status

        st = face_status()  # stored per-sport-date table; computed once if missing
        bad = {
            (str(r.sport).upper(), str(r.slate_date)[:10])
            for r in st.itertuples(index=False)
            if not bool(r.face_verified)
        }
    except Exception as exc:  # ledger missing on this tree
        print(f"  face filter unavailable ({exc}); scoring on labels as-is", flush=True)
        bad = set()
    _FACE_STATUS_CACHE["all"] = bad
    return bad


def drop_invalid_stat_attach(sport: str, rows: list[dict]) -> tuple[list[dict], dict]:
    """Drop YPC / first-N / longest reception / completion-stat rows (2026-10-25)."""
    try:
        from utils.stat_attach_invalid import tag_rows
    except Exception as exc:  # pragma: no cover
        print(f"  stat-attach filter unavailable ({exc})", flush=True)
        return rows, {"dropped": 0, "by_reason": {}}
    return tag_rows(rows, sport_key=sport)


def drop_unverified_face_dates(sport: str, rows: list[dict]) -> tuple[list[dict], dict]:
    bad = unverified_face_dates()
    key_sport = {"SOCCER": "SOCCER", "TENNIS": "TENNIS"}.get(sport.upper(), sport.upper())
    kept, dropped, dates = [], 0, set()
    for r in rows:
        ds = str(r.get("date") or "")[:10]
        sp = str(r.get("sport") or key_sport).upper()
        if (sp, ds) in bad or (key_sport, ds) in bad:
            dropped += 1
            dates.add(ds)
            continue
        kept.append(r)
    return kept, {"dropped": dropped, "dates": sorted(dates)}


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sport", default="ALL", help="Sport key or ALL")
    ap.add_argument("--book", default="ALL", help='Goblin OVER / Standard OVER / Standard UNDER / ALL')
    ap.add_argument("--min-n", type=int, default=20)
    ap.add_argument("--no-optional", action="store_true", help="Skip pysubgroup/FIGS/optbinning/tree")
    ap.add_argument(
        "--pysubgroup",
        action="store_true",
        help="Run pysubgroup (slow; skipped by default after empty-slice hangs)",
    )
    args = ap.parse_args(argv)
    sports = sorted(SPORT_CFG) if args.sport.upper() == "ALL" else [args.sport]
    books = list(BOOKS) if args.book.upper() == "ALL" else [args.book]
    tools = _tool_versions()
    print("tools:", tools, flush=True)
    results: list[dict] = []
    for sport in sports:
        if sport not in SPORT_CFG:
            print(f"skip unknown sport {sport}")
            continue
        print(f"loading {sport}...", flush=True)
        rows, meta = iter_sport_rows(sport)
        print(f"  unique={len(rows)} raw={meta.get('unique_n')} dates={len({r['date'] for r in rows})}", flush=True)
        rows, face_meta = drop_unverified_face_dates(sport, rows)
        if face_meta["dropped"]:
            print(
                f"  face filter: dropped {face_meta['dropped']} rows on {len(face_meta['dates'])} "
                f"unverified-face dates (pick_type defaulted / Standard ladders); kept {len(rows)}",
                flush=True,
            )
        rows, attach_meta = drop_invalid_stat_attach(sport, rows)
        if attach_meta["dropped"]:
            print(
                f"  stat-attach filter: dropped {attach_meta['dropped']} sibling-mapped / "
                f"zero-filled props (YPC, first-N, longest rec, completion); kept {len(rows)}",
                flush=True,
            )
        for book in books:
            pool = [r for r in rows if r.get("book") == book]
            if len(pool) < args.min_n:
                print(f"  {book}: n={len(pool)} skip", flush=True)
                continue
            rec = run_book(
                sport,
                book,
                pool,
                min_n=args.min_n,
                optional=not args.no_optional,
                use_pysubgroup=args.pysubgroup,
            )
            rec["face_filter"] = {
                "rows_dropped": face_meta["dropped"],
                "unverified_dates": face_meta["dates"],
                "note": "rows on default-label / Standard-ladder dates excluded (utils.face_ladder)",
            }
            rec["stat_attach_filter"] = {
                "rows_dropped": attach_meta["dropped"],
                "by_reason": attach_meta.get("by_reason") or {},
                "note": "YPC / first-N / longest reception / completion stats tagged invalid 2026-10-25",
            }
            results.append(rec)
            print(
                f"  {book}: n={rec['n']} generic_hr={rec['generic_stack'].get('hr')} "
                f"clears_1.65/3x={len(rec['clears_wilson_primary'])}",
                flush=True,
            )
    path = write_outputs(results, tools)
    print(f"wrote {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
