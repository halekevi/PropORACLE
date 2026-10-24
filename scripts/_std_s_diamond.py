"""Standard props in S/A tiers and Diamond/Platinum badges — today + history."""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(r"H:\PropORACLE\scripts")))
import rank_best_props_today as R  # noqa: E402
from _avg_gap_by_badge import score_badge  # noqa: E402
from _prop_category_gates import BOOKS, build_merged, overall  # noqa: E402
from prop_hit_tiers import assign_tier  # noqa: E402

DATE = "2026-08-26"
ROOT = Path(r"H:\PropORACLE_main_cp")
PROMOS = ("Diamond", "Platinum", "Gold", "Silver", "Bronze", "none")
TIERS = ("S", "A", "B", "C", "D", "W")


def fmt(r):
    if not r or not r.get("n"):
        return "—"
    return f"{r['rate']}%  {r['hits']}/{r['n']}"


def d_ok_row(rec) -> bool:
    return bool((rec.get("checks") or {}).get("D") is True)


def dump_today():
    all_rows = []
    for sport, folder, fname in R.SPORTS:
        df = R.load_sport(ROOT, DATE, sport, folder, fname)
        if df.empty:
            continue
        all_rows.extend(R.recs(df))
    listed = []
    for sport, folder, fname in R.SPORTS:
        so, su, gob = R.bucket(all_rows, sport)
        for cat, lst in (("Standard OVER", so), ("Standard UNDER", su), ("Goblin OVER", gob)):
            for r in lst:
                listed.append({**r, "cat": cat})
    std = [r for r in listed if str(r.get("pick_type")) == "Standard"]
    gob = [r for r in listed if str(r.get("pick_type")) == "Goblin"]
    print("\n===== TODAY", DATE, "listed =====")
    print("Standard", len(std), "Goblin", len(gob))
    print("Standard by tier", Counter(r.get("prop_tier") or "—" for r in std))
    print("Standard by promo", Counter(r.get("promo") or r.get("badge") or "—" for r in std))
    print("Goblin by tier", Counter(r.get("prop_tier") or "—" for r in gob))
    hot = [
        r
        for r in std
        if (r.get("prop_tier") in ("S", "A"))
        or (r.get("promo") in ("Diamond", "Platinum"))
    ]
    print(f"Standard S/A or Diamond/Plat n={len(hot)}")
    hot.sort(
        key=lambda r: (
            {"S": 0, "A": 1, "B": 2, "C": 3, "D": 4, "W": 5}.get(r.get("prop_tier") or "", 9),
            {"Diamond": 0, "Platinum": 1}.get(r.get("promo") or "", 9),
            -(r.get("cover") or 0),
        )
    )
    for r in hot[:80]:
        print(
            f"  {r.get('prop_tier')}/{r.get('promo') or r.get('badge')}  "
            f"{r.get('sport'):7} {r.get('side'):5} {r.get('player'):22} {r.get('prop'):18} "
            f"{(r.get('side') or 'O')[0]}{r.get('line')}  cov {r.get('cover')}  "
            f"{r.get('def')}  {r.get('matchup')}"
        )
    return std, listed


def dump_hist():
    merged = build_merged()
    listed = merged[merged["list_l5"] & merged["book"].isin(BOOKS)].copy()
    print("scoring", len(listed), flush=True)
    recs = listed.to_dict("records")
    listed["promo"] = [score_badge(r) for r in recs]
    info = []
    for rec in recs:
        tier = str(rec.get("def_tier") or "").strip().lower()
        over = rec.get("direction") == "OVER"
        if rec.get("sport") == "MLB" and rec.get("prop") == "hitter_ks":
            ok = ("elite" in tier or "above" in tier) if over else (
                "weak" in tier or "below" in tier or "easy" in tier
            )
        elif over:
            ok = "weak" in tier or "below" in tier or "easy" in tier
        else:
            ok = "elite" in tier or "above" in tier or "hard" in tier
        info.append(
            assign_tier(
                sport=rec["sport"],
                pick_type=rec["pick"],
                side=rec["direction"],
                prop=rec.get("prop_n") or rec.get("prop"),
                cover=rec.get("min_gap"),
                d_ok=ok,
            )
        )
    listed["prop_tier"] = [x["prop_tier"] for x in info]
    std = listed[listed["pick"] == "Standard"]
    gob = listed[listed["pick"] == "Goblin"]
    print("\n===== HISTORY listed L5>=4 =====")
    print("Standard", fmt(overall(std)), "Goblin", fmt(overall(gob)))
    print("\nStandard by tier")
    for t in TIERS:
        sub = std[std["prop_tier"] == t]
        print(f"  {t} {fmt(overall(sub))}")
    print("\nStandard by promo")
    for p in PROMOS:
        sub = std[std["promo"] == p]
        print(f"  {p:10} {fmt(overall(sub))}")
    print("\nStandard tier x Diamond/Plat/Gold")
    for t in TIERS:
        for p in ("Diamond", "Platinum", "Gold"):
            sub = std[(std["prop_tier"] == t) & (std["promo"] == p)]
            if len(sub):
                print(f"  {t}/{p:9} {fmt(overall(sub))}")
    print("\nGoblin S+A Diamond/Plat for comparison")
    sa = gob[gob["prop_tier"].isin(["S", "A"])]
    for p in ("Diamond", "Platinum", "Gold"):
        print(f"  Goblin S+A {p:9} {fmt(overall(sa[sa['promo']==p]))}")
    print(f"  Goblin S+A all          {fmt(overall(sa))}")
    dest = Path(r"H:\PropORACLE\data\reports\standard_s_diamond.json")
    rows = []
    for t in TIERS:
        for p in PROMOS:
            sub = std[(std["prop_tier"] == t) & (std["promo"] == p)]
            if len(sub):
                rows.append({"tier": t, "promo": p, **overall(sub)})
    dest.write_text(
        json.dumps(
            {
                "standard_all": overall(std),
                "goblin_all": overall(gob),
                "standard_cross": rows,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print("wrote", dest)


if __name__ == "__main__":
    dump_today()
    dump_hist()
