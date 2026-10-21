#!/usr/bin/env python3
"""Inventory display_prop vs raw graded prop labels + zero-actual UNDER flags.

Same graded_props discovery as all_ranked_prop_hit_rates (main_cp first).
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(r"H:/PropORACLE")
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

import prop_hit_tiers as T  # noqa: E402
from utils.prop_norm import display_prop, fold  # noqa: E402

MAIN = REPO.parent / "PropORACLE_main_cp"
GRADED_DIRS = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    REPO / "ui_runner" / "templates",
    REPO / "mobile" / "www",
]


def graded_paths() -> list[Path]:
    by_date: dict[str, Path] = {}
    for d in GRADED_DIRS:
        if not d.is_dir():
            continue
        for p in sorted(d.glob("graded_props_20*.json")):
            if ".bak_" in p.name:
                continue
            date = p.stem.replace("graded_props_", "")[:10]
            if date not in by_date:
                by_date[date] = p
    return [by_date[k] for k in sorted(by_date)]


def actual_num(row: dict) -> float | None:
    v = row.get("actual_value")
    if v is None or v == "":
        v = row.get("actual")
    if v is None or v == "":
        return None
    s = str(v).strip()
    if s.lower() in ("nan", "none", "null", "—", "–", "-", "void"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def main() -> None:
    # (sport, canon) -> Counter(raw_label)
    raw_by: dict[tuple[str, str], Counter] = defaultdict(Counter)
    # (sport, book, canon) -> {n, hits, zero, known}
    cells: dict[tuple[str, str, str], dict] = defaultdict(
        lambda: {"n": 0, "hits": 0, "zero": 0, "known": 0}
    )
    dates: list[str] = []

    for path in graded_paths():
        date = path.stem.replace("graded_props_", "")[:10]
        dates.append(date)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for r in raw.get("props") or []:
            if not isinstance(r, dict):
                continue
            res = str(r.get("result") or "").strip().upper()
            if res not in ("HIT", "MISS"):
                continue
            sport = T.norm_sport(str(r.get("sport") or ""))
            if not sport or sport == "UNKNOWN":
                continue
            side = str(r.get("direction") or r.get("over_under") or "").strip().upper()
            if side in ("O", "OVER"):
                side = "OVER"
            elif side in ("U", "UNDER"):
                side = "UNDER"
            else:
                continue
            book = T.book_label(str(r.get("pick_type") or ""), side)
            if book not in ("Goblin OVER", "Standard OVER", "Standard UNDER"):
                continue
            prop_raw = str(r.get("prop") or r.get("prop_type") or "")
            canon = T.canon_prop(sport, prop_raw)
            if not canon:
                continue
            raw_by[(sport, canon)][prop_raw] += 1
            c = cells[(sport, book, canon)]
            c["n"] += 1
            c["hits"] += 1 if res == "HIT" else 0
            a = actual_num(r)
            if a is not None:
                c["known"] += 1
                if a == 0.0:
                    c["zero"] += 1

    splits = []
    for (sport, canon), ctr in sorted(raw_by.items()):
        disp = display_prop(canon)
        fold_disp = fold(disp)
        raws = ctr.most_common()
        # Meaningful split: display fold != every raw fold, OR display text not in raw labels
        raw_folds = {fold(lab) for lab, _ in raws}
        display_in_raw = any(lab == disp for lab, _ in raws)
        fold_match = fold_disp in raw_folds
        # Also flag when multiple distinct raw families collapse to one canon
        multi_family = len(raw_folds) >= 2
        if (not display_in_raw) or (not fold_match) or multi_family:
            # attach Std UNDER zero share when present
            under = cells.get((sport, "Standard UNDER", canon), {})
            over = cells.get((sport, "Standard OVER", canon), {})
            gob = cells.get((sport, "Goblin OVER", canon), {})
            u_known = under.get("known") or 0
            u_zero = under.get("zero") or 0
            splits.append(
                {
                    "sport": sport,
                    "canon": canon,
                    "display": disp,
                    "display_in_raw_labels": display_in_raw,
                    "display_fold_matches_any_raw": fold_match,
                    "multi_raw_family": multi_family,
                    "raw_labels": raws[:8],
                    "n_raw_variants": len(raws),
                    "std_under": {
                        "n": under.get("n", 0),
                        "hr": (under["hits"] / under["n"]) if under.get("n") else None,
                        "zero_share": (u_zero / u_known) if u_known else None,
                        "zero": u_zero,
                        "known": u_known,
                    },
                    "std_over_n": over.get("n", 0),
                    "goblin_over_n": gob.get("n", 0),
                }
            )

    # High zero-share UNDER among ALL live books (not only splits)
    under_flags = []
    for (sport, book, canon), c in cells.items():
        if book != "Standard UNDER":
            continue
        if c["n"] < 40 or c["known"] < 40:
            continue
        zs = c["zero"] / c["known"] if c["known"] else 0
        if zs < 0.40:
            continue
        under_flags.append(
            {
                "sport": sport,
                "canon": canon,
                "display": display_prop(canon),
                "n": c["n"],
                "hr": c["hits"] / c["n"],
                "zero_share": zs,
                "zero": c["zero"],
                "known": c["known"],
                "is_display_split": any(
                    s["sport"] == sport and s["canon"] == canon for s in splits
                ),
            }
        )
    under_flags.sort(key=lambda r: (-r["zero_share"], -r["n"]))

    # Pure display≠raw (ignore case/underscore-only variants)
    hard_splits = [
        s
        for s in splits
        if (not s["display_in_raw_labels"]) and (not s["display_fold_matches_any_raw"])
    ]
    soft_splits = [
        s
        for s in splits
        if s not in hard_splits
        and (not s["display_in_raw_labels"] or s["multi_raw_family"])
    ]

    out = {
        "window": {
            "from": dates[0] if dates else None,
            "to": dates[-1] if dates else None,
            "n_files": len(dates),
        },
        "n_sport_canon_pairs": len(raw_by),
        "n_hard_display_splits": len(hard_splits),
        "n_soft_splits": len(soft_splits),
        "hard_display_splits": hard_splits,
        "soft_splits_sample": soft_splits[:40],
        "std_under_high_zero_share": under_flags,
        "note": (
            "hard_display_splits = display_prop(canon) fold does not match any raw "
            "graded label fold (e.g. NHL sog → 'Shots On Target' vs raw 'Shots On Goal'). "
            "soft = display text absent but fold matches, or multiple raw families."
        ),
    }
    path = REPO / "data" / "reports" / "audit_prop_display_raw_splits.json"
    path.write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")

    print(f"window {out['window']} pairs={out['n_sport_canon_pairs']}")
    print(f"HARD display splits ({len(hard_splits)}):")
    for s in hard_splits:
        u = s["std_under"]
        zs = None if u["zero_share"] is None else round(100 * u["zero_share"], 1)
        hr = None if u["hr"] is None else round(100 * u["hr"], 1)
        print(
            f"  {s['sport']:8} {s['canon']:20} display={s['display']!r:28} "
            f"raw={s['raw_labels'][:3]} UNDER n={u['n']} hr={hr}% zero={zs}%"
        )
    print(f"\nStd UNDER zero_share>=40% n>=40 ({len(under_flags)}):")
    for r in under_flags[:25]:
        print(
            f"  {r['sport']:8} {r['canon']:20} {r['display']!r:28} "
            f"n={r['n']} hr={round(100*r['hr'],1)}% zero={round(100*r['zero_share'],1)}% "
            f"split={r['is_display_split']}"
        )
    print("Wrote", path)


if __name__ == "__main__":
    main()
