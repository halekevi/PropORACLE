"""Print today's S+A (and promoted) plays from the live step8 board."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.stdout.reconfigure(encoding="utf-8")
sys.path.insert(0, str(Path(r"H:\PropORACLE\scripts")))
import rank_best_props_today as R  # noqa: E402

DATE = "2026-08-26"
ROOT = Path(r"H:\PropORACLE_main_cp")


def skip_er(r) -> bool:
    return "earned run" in str(r.get("prop") or "").lower() and float(r.get("line") or 99) <= 0.5


def row(r, cat):
    return {
        "sport": r.get("sport"),
        "tier": r.get("prop_tier"),
        "promo": r.get("promo") or r.get("badge") or "",
        "promoted": bool(r.get("prop_promoted")),
        "reason": r.get("prop_promote_reason") or "",
        "shadow": bool(r.get("prop_shadow")),
        "book": cat,
        "player": r.get("player"),
        "prop": r.get("prop"),
        "side": r.get("side"),
        "line": r.get("line"),
        "l5_over": r.get("l5_over"),
        "l5_under": r.get("l5_under"),
        "avg": r.get("season_avg"),
        "cover": r.get("cover"),
        "d": r.get("def"),
        "d_rank": r.get("def_rank"),
        "miss": r.get("miss_s") or "",
        "matchup": r.get("matchup") or "",
    }


def main():
    all_rows = []
    for sport, folder, fname in R.SPORTS:
        df = R.load_sport(ROOT, DATE, sport, folder, fname)
        if df.empty:
            print(f"{sport}: no step8")
            continue
        all_rows.extend(R.recs(df))
    hot = []
    rest_gold = []
    for sport, folder, fname in R.SPORTS:
        so, su, gob = R.bucket(all_rows, sport)
        for cat, lst, side in (
            ("Standard OVER", so, "OVER"),
            ("Standard UNDER", su, "UNDER"),
            ("Goblin OVER", gob, "OVER"),
        ):
            for r in lst:
                if cat == "Goblin OVER" and skip_er(r):
                    continue
                rec = row(r, cat)
                if rec["tier"] in ("S", "A"):
                    hot.append(rec)
                elif rec["promo"] in ("Diamond", "Platinum", "Gold") and rec["tier"] in ("B", "C"):
                    rest_gold.append(rec)
    print(f"S+A n={len(hot)}  B/C Gold+ n={len(rest_gold)}")
    by = {}
    for rec in hot:
        by.setdefault(rec["sport"], []).append(rec)
    for sport in ("WNBA", "MLB", "SOCCER", "TENNIS"):
        rows = by.get(sport) or []
        print(f"\n===== {sport} S+A ({len(rows)}) =====")
        if not rows:
            print("  (none)")
            continue
        for rec in rows:
            star = "*" if rec["promoted"] else ""
            miss = f"  miss {rec['miss']}" if rec["miss"] else ""
            print(
                f"  {rec['tier']}/{rec['promo']}{star:1}  {rec['player']:24} {rec['prop']:18} "
                f"{rec['side'][0]}{rec['line']}  L5 {rec['l5_over']}/{rec['l5_under']}  "
                f"cov {rec['cover']:+.2f}  {rec['d'] or 'no-D'}{miss}  {rec['matchup']}"
            )
    dest = Path(r"H:\PropORACLE\data\reports\best_props_sa_2026-08-26.json")
    dest.write_text(json.dumps({"hot": hot, "bc_gold": rest_gold}, indent=2, ensure_ascii=False), encoding="utf-8")
    print("\nwrote", dest)


if __name__ == "__main__":
    main()
