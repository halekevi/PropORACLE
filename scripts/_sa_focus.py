"""S/A-only listed rates, then which gates lift the rest into that band."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

OPEN = Path(r"H:\PropORACLE")
sys.path.insert(0, str(OPEN / "scripts"))
from _hot_minus_shadow import shadow_reason  # noqa: E402
from _prop_category_gates import BOOKS, GOBLIN_FLOOR, build_merged, overall  # noqa: E402

S_KEYS = {
    ("MLB", "Goblin OVER", "walks allowed"),
    ("MLB", "Goblin OVER", "hits allowed"),
    ("MLB", "Goblin OVER", "pitcher_ks"),
    ("MLB", "Goblin OVER", "earned runs allowed"),
    ("MLB", "Goblin OVER", "pitches thrown"),
}
A_KEYS = {
    ("MLB", "Goblin OVER", "pitching outs"),
    ("WNBA", "Goblin OVER", "pts+reb"),
    ("WNBA", "Goblin OVER", "points"),
    ("WNBA", "Goblin OVER", "assists"),
    ("WNBA", "Goblin OVER", "rebounds"),
    ("WNBA", "Goblin OVER", "threes"),
    ("WNBA", "Goblin OVER", "pra"),
    ("WNBA", "Goblin OVER", "pts+ast"),
    ("Tennis", "Goblin OVER", "total games"),
    ("Tennis", "Goblin OVER", "games_won"),
}

WEAK = {"weak", "easy", "easiest", "below avg", "below average"}
ELITE = {"elite", "hard", "hardest", "tough", "above avg", "above average", "solid"}


def fmt(r):
    if not r or not r.get("n"):
        return "—"
    return f"{r['rate']}%  {r['hits']}/{r['n']}"


def key_tuple(row):
    return (row["sport"], row["book"], row["prop"])


def d_pass(row) -> bool:
    tier = str(row.get("def_tier") or "").strip().lower()
    if not tier or tier in {"nan", "none", ""}:
        return False
    over = row["direction"] == "OVER"
    hitter_k = row["sport"] == "MLB" and row["prop"] == "hitter_ks"
    if hitter_k:
        return (tier in ELITE) if over else (tier in WEAK)
    return (tier in WEAK) if over else (tier in ELITE)


def main():
    merged = build_merged()
    listed = merged[merged["list_l5"] & merged["book"].isin(BOOKS)].copy()
    keys = list(zip(listed["sport"], listed["book"], listed["prop"]))
    listed["sa"] = ["S" if k in S_KEYS else "A" if k in A_KEYS else "rest" for k in keys]
    listed["shadow"] = [shadow_reason(s, b, p) is not None for s, b, p in zip(listed["sport"], listed["book"], listed["prop"])]
    listed["d_ok"] = listed.apply(d_pass, axis=1)
    listed["l5_5"] = listed["l5_hits"] >= 5
    listed["z"] = listed["min_gap"] / pd.to_numeric(listed["dist_std"], errors="coerce")
    listed["z"] = listed["z"].replace([np.inf, -np.inf], np.nan)

    s = listed[listed["sa"] == "S"]
    a = listed[listed["sa"] == "A"]
    sa = listed[listed["sa"].isin(["S", "A"])]
    rest = listed[(listed["sa"] == "rest") & ~listed["shadow"]]
    shadow = listed[listed["shadow"]]

    gates = {
        "as_listed": rest,
        "cover_floor": rest[rest["cover_ok"]],
        "L5=5": rest[rest["l5_5"]],
        "L5=5 + cover": rest[rest["l5_5"] & rest["cover_ok"]],
        "D pass": rest[rest["d_ok"]],
        "cover + D": rest[rest["cover_ok"] & rest["d_ok"]],
        "L5=5 + D": rest[rest["l5_5"] & rest["d_ok"]],
        "z>=1": rest[rest["z"] >= 1],
        "z>=1.5": rest[rest["z"] >= 1.5],
        "cover + z>=1": rest[rest["cover_ok"] & (rest["z"] >= 1)],
    }

    promote = []
    for (sport, book, prop), g in rest.groupby(["sport", "book", "prop"]):
        if len(g) < 20:
            continue
        base = overall(g)
        best = {"gate": "as_listed", **base}
        for name, fn in (
            ("cover_floor", g["cover_ok"]),
            ("L5=5", g["l5_5"]),
            ("L5=5 + cover", g["l5_5"] & g["cover_ok"]),
            ("D pass", g["d_ok"]),
            ("cover + D", g["cover_ok"] & g["d_ok"]),
            ("z>=1", g["z"] >= 1),
            ("z>=1.5", g["z"] >= 1.5),
        ):
            sub = g[fn]
            rec = overall(sub)
            if rec["n"] < 15:
                continue
            if rec["rate"] is not None and rec["rate"] >= 70 and rec["n"] >= 20:
                promote.append(
                    {
                        "sport": sport,
                        "book": book,
                        "prop": prop,
                        "base": base,
                        "gate": name,
                        "promoted": rec,
                        "lift_pp": round(rec["rate"] - (base["rate"] or 0), 1),
                    }
                )
            if rec["rate"] is not None and (best["rate"] is None or rec["rate"] > best["rate"]):
                best = {"gate": name, **rec}
        # keep one summary even if nothing hits 70
        if not any(p["sport"] == sport and p["book"] == book and p["prop"] == prop for p in promote):
            promote.append(
                {
                    "sport": sport,
                    "book": book,
                    "prop": prop,
                    "base": base,
                    "gate": best["gate"],
                    "promoted": {k: best[k] for k in ("hits", "n", "rate")},
                    "lift_pp": round((best["rate"] or 0) - (base["rate"] or 0), 1),
                    "reached_70": False,
                }
            )

    reached = [p for p in promote if p.get("reached_70") is not False and p["promoted"]["rate"] and p["promoted"]["rate"] >= 70]
    # de-dupe: keep gates that reach 70; for others keep best only
    hit70 = [p for p in promote if p["promoted"].get("rate") and p["promoted"]["rate"] >= 70 and p.get("reached_70") is not False]
    missed = []
    seen = set()
    for p in promote:
        k = (p["sport"], p["book"], p["prop"])
        if k in {(x["sport"], x["book"], x["prop"]) for x in hit70}:
            continue
        if k in seen:
            continue
        if p.get("reached_70") is False:
            missed.append(p)
            seen.add(k)

    report = {
        "listed_all": overall(listed),
        "S": overall(s),
        "A": overall(a),
        "S_plus_A": overall(sa),
        "rest_no_shadow": overall(rest),
        "shadow": overall(shadow),
        "rest_gates": {name: overall(df) for name, df in gates.items()},
        "promoted_to_70": hit70,
        "best_but_short": missed,
    }
    dest = OPEN / "data" / "reports" / "sa_focus.json"
    dest.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("FOCUS")
    print("  all listed ", fmt(report["listed_all"]))
    print("  S         ", fmt(report["S"]))
    print("  A         ", fmt(report["A"]))
    print("  S+A       ", fmt(report["S_plus_A"]))
    print("  rest      ", fmt(report["rest_no_shadow"]))
    print("  shadow    ", fmt(report["shadow"]))
    print()
    print("REST GATES (try to lift into 70%+)")
    for name, rec in report["rest_gates"].items():
        print(f"  {name:18} {fmt(rec)}")
    print()
    print("PROPS THAT REACH 70% WITH A GATE (n>=20 after gate)")
    hit70.sort(key=lambda p: (-p["promoted"]["rate"], -p["promoted"]["n"]))
    for p in hit70:
        print(
            f"  {p['sport']:7} {p['book']:16} {p['prop']:18} "
            f"{fmt(p['base']):18} + {p['gate']:16} -> {fmt(p['promoted'])}  ({p['lift_pp']:+.1f}pp)"
        )
    print()
    print("BEST GATE BUT STILL <70% (n>=20 listed)")
    missed.sort(key=lambda p: (-(p["promoted"]["rate"] or 0), -p["base"]["n"]))
    for p in missed:
        print(
            f"  {p['sport']:7} {p['book']:16} {p['prop']:18} "
            f"{fmt(p['base']):18} best {p['gate']:16} {fmt(p['promoted'])}"
        )
    print("wrote", dest)


if __name__ == "__main__":
    main()
