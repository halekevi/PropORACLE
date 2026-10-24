"""Hit rates for prop tier (S–D) × badge (Diamond→Bronze)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

OPEN = Path(r"H:\PropORACLE")
sys.path.insert(0, str(OPEN / "scripts"))
from _avg_gap_by_badge import score_badge  # noqa: E402
from _prop_category_gates import BOOKS, build_merged, overall  # noqa: E402
from prop_hit_tiers import assign_tier  # noqa: E402

PROMOS = ("Diamond", "Platinum", "Gold", "Silver", "Bronze", "none")
TIERS = ("S", "A", "B", "C", "D", "W")


def fmt(r):
    if not r or not r.get("n"):
        return "—"
    return f"{r['rate']}%  {r['hits']}/{r['n']}"


def main():
    merged = build_merged()
    listed = merged[merged["list_l5"] & merged["book"].isin(BOOKS)].copy()
    print("scoring badges", len(listed), flush=True)
    listed["promo"] = [score_badge(r) for r in listed.to_dict("records")]
    listed["d_ok"] = listed["promo"].ne("none")  # placeholder, overwritten
    info = []
    for rec in listed.to_dict("records"):
        d_ok = False
        tier = str(rec.get("def_tier") or "").strip().lower()
        over = rec.get("direction") == "OVER"
        if rec.get("sport") == "MLB" and rec.get("prop") == "hitter_ks":
            d_ok = ("elite" in tier or "above" in tier) if over else ("weak" in tier or "below" in tier or "easy" in tier)
        elif over:
            d_ok = "weak" in tier or "below" in tier or "easy" in tier
        else:
            d_ok = "elite" in tier or "above" in tier or "hard" in tier
        cover = rec.get("g_l5") if rec.get("book", "").startswith("Goblin") else rec.get("min_gap")
        info.append(
            assign_tier(
                sport=rec["sport"],
                pick_type=rec["pick"],
                side=rec["direction"],
                prop=rec.get("prop_n") or rec.get("prop"),
                cover=cover,
                d_ok=d_ok,
            )
        )
    listed["prop_tier"] = [x["prop_tier"] for x in info]
    listed["prop_tier_base"] = [x["prop_tier_base"] for x in info]
    listed["prop_shadow"] = [x["prop_shadow"] for x in info]
    listed["prop_promoted"] = [x["prop_promoted"] for x in info]

    cross = {}
    for t in TIERS:
        cross[t] = {"all": overall(listed[listed["prop_tier"] == t])}
        for b in PROMOS:
            sub = listed[(listed["prop_tier"] == t) & (listed["promo"] == b)]
            if len(sub):
                cross[t][b] = overall(sub)

    by_promo = {b: overall(listed[listed["promo"] == b]) for b in PROMOS}
    by_tier = {t: overall(listed[listed["prop_tier"] == t]) for t in TIERS}
    sa = listed[listed["prop_tier"].isin(["S", "A"])]
    sa_gold = sa[sa["promo"].isin(["Diamond", "Platinum", "Gold"])]
    sa_any = sa
    promoted = listed[listed["prop_promoted"]]

    report = {
        "listed": overall(listed),
        "by_tier": by_tier,
        "by_promo": by_promo,
        "cross": cross,
        "S_plus_A": overall(sa_any),
        "S_plus_A_Gold_up": overall(sa_gold),
        "promoted": overall(promoted),
        "promoted_by_reason": {},
    }
    dest = OPEN / "data" / "reports" / "tier_x_badge.json"
    dest.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("LISTED", fmt(report["listed"]))
    print("S+A (effective, incl promotions)", fmt(report["S_plus_A"]))
    print("S+A Diamond/Plat/Gold", fmt(report["S_plus_A_Gold_up"]))
    print("promoted into S/A", fmt(report["promoted"]))
    print()
    print("TIER")
    for t in TIERS:
        print(f"  {t}  {fmt(by_tier[t])}")
    print()
    print("BADGE (all listed)")
    for b in PROMOS:
        print(f"  {b:10} {fmt(by_promo[b])}")
    print()
    print("TIER x BADGE")
    hdr = f"{'':3} " + " ".join(f"{b:14}" for b in ("all",) + PROMOS[:5])
    print(hdr)
    for t in TIERS:
        bits = [f"{t:2}"]
        for b in ("all",) + PROMOS[:5]:
            bits.append(f"{fmt(cross[t].get(b)):14}")
        print("  " + " ".join(bits))
    print("wrote", dest)


if __name__ == "__main__":
    main()
