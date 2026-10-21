"""CFB live ticket stack on unique player-games. No L10 in the baseline.

Observe-only. Does not change keep-gates.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import _tmp_cfb_l5_recut as R  # noqa: E402
from prop_hit_tiers import cover_clears_floor, cover_need  # noqa: E402

OUT = ROOT / "data" / "reports" / "cfb_live_stack.json"

# Chip, book, whether the live card requires directional D.
CHIPS = [
    ("rush_yds", "Goblin OVER", True),
    ("pass_yds", "Goblin OVER", False),
    ("rush_yds", "Standard UNDER", True),
    ("rec_yds", "Goblin OVER", True),
]


def load_signed_edge(path: Path) -> dict:
    import pandas as pd

    parquet = path.with_suffix(".parquet")
    if parquet.exists():
        df = pd.read_parquet(parquet)
    else:
        df = pd.read_excel(path, engine="openpyxl")
    cm = {str(c).strip().lower(): c for c in df.columns}

    def col(*names):
        for n in names:
            if n.lower() in cm:
                return cm[n.lower()]
        return None

    import prop_hit_tiers as T

    c_player = col("player")
    c_prop = col("prop", "prop_type", "prop_norm")
    c_pick = col("pick type", "pick_type")
    c_line = col("line")
    c_dir = col("direction")
    c_edge = col("edge")
    out = {}
    for row in df.to_dict("records"):
        prop = T.canon_prop("CFB", str(row.get(c_prop) or ""))
        if not prop:
            continue
        side = R.side_of(row.get(c_dir) if c_dir else None)
        if side is None:
            continue
        pick = R.pick_of(row.get(c_pick) if c_pick else "")
        line = R.num(row.get(c_line) if c_line else None)
        key = (R.norm_player(row.get(c_player) if c_player else None), prop, pick, line, side)
        edge = R.num(row.get(c_edge) if c_edge else None)
        if edge is None or key in out:
            continue
        out[key] = edge
    return out


def collect() -> list[dict]:
    rows = []
    for date in R.TAPE:
        gpath = R.graded_for(date)
        spath = R.find_step8(date)
        if not gpath or not spath:
            continue
        idx = R.load_step8(spath)
        edges = load_signed_edge(spath)
        raw = json.loads(gpath.read_text(encoding="utf-8"))
        for r in raw.get("props") or []:
            if not isinstance(r, dict) or not R.is_cfb(r):
                continue
            res = str(r.get("result") or "").upper()
            if res not in {"HIT", "MISS"}:
                continue
            import prop_hit_tiers as T

            prop = T.canon_prop("CFB", str(r.get("prop") or r.get("prop_type") or ""))
            if not prop:
                continue
            side = R.side_of(r.get("direction") or r.get("over_under") or r.get("side"))
            if side is None:
                continue
            pick = R.pick_of(r.get("pick_type"))
            book = R.book_of(pick, side)
            if book not in R.BOOKS:
                continue
            line = R.num(r.get("line"))
            key = (R.norm_player(r.get("player")), prop, pick, line, side)
            s8 = idx.get(key) or {}
            rec = {
                "date": date,
                "player": r.get("player"),
                "player_norm": key[0],
                "prop": prop,
                "book": book,
                "pick": pick,
                "side": side,
                "line": line,
                "hit": 1 if res == "HIT" else 0,
                "l5_over": R.first_num(r.get("l5_over"), r.get("last5_over"), s8.get("l5_over")),
                "l5_under": R.first_num(r.get("l5_under"), r.get("last5_under"), s8.get("l5_under")),
                "l10_over": R.first_num(
                    r.get("l10_over"), r.get("line_hits_over_10"), s8.get("l10_over")
                ),
                "l10_under": R.first_num(
                    r.get("l10_under"), r.get("line_hits_under_10"), s8.get("l10_under")
                ),
                "def_tier": r.get("def_tier") or r.get("def") or s8.get("def_tier") or "",
                "cover": edges.get(key),
            }
            rec["l5"] = R.l5_dir(rec, side)
            rec["l10"] = R.l10_dir(rec, side)
            rec["d"] = R.d_ok(side, rec["def_tier"])
            rec["cover_ok"] = cover_clears_floor("CFB", rec["cover"], side, prop)
            rows.append(rec)
    unique = R.unique_dedupe(rows)
    # 8/28 and 8/29 stamped the same game twice (Hall). Collapse only when the
    # later row is the next day and the line is within 1. A later Saturday at
    # the same number is a different game.
    from collections import defaultdict

    groups = defaultdict(list)
    for r in unique:
        groups[(r["player_norm"], r["prop"], r["book"], r["side"])].append(r)
    out = []
    for g in groups.values():
        g = sorted(g, key=lambda x: x["date"])
        kept = []
        for r in g:
            # Same game stamped on adjacent days (Hall 8/28 and 8/29). A later
            # Saturday at the same line is a different game and stays.
            gap = None
            if kept:
                from datetime import date as _date

                gap = (_date.fromisoformat(r["date"]) - _date.fromisoformat(kept[-1]["date"])).days
            if kept and gap is not None and gap <= 1 and abs((r.get("line") or 0) - (kept[-1].get("line") or 0)) <= 1.0:
                kept[-1] = r
            else:
                kept.append(r)
        out.extend(kept)
    return out


def pack(pool) -> dict:
    n = len(pool)
    h = sum(r["hit"] for r in pool)
    return {"h": h, "n": n, "hr": round(100 * h / n, 1) if n else None}


def fmt(c: dict) -> str:
    if not c["n"]:
        return "empty"
    return f"{c['hr']:.1f}% {c['h']}/{c['n']}"


def main() -> None:
    unique = collect()
    report = {"unique": len(unique), "chips": []}
    print(f"unique player-games {len(unique)}")
    print("Live card = L5=5 + chip D/cover. L10 is the lift column, not the gate.")
    for prop, book, need_d in CHIPS:
        pool = [r for r in unique if r["prop"] == prop and r["book"] == book]
        l5 = [r for r in pool if r["l5"] == 5]
        live = [
            r
            for r in l5
            if r["cover_ok"] and (r["d"] if need_d else True)
        ]
        live_l10 = [r for r in live if r["l10"] is not None and r["l10"] >= 8]
        live_no_l10 = [r for r in live if not (r["l10"] is not None and r["l10"] >= 8)]
        need = cover_need("CFB", prop)
        label = R.LABEL.get(prop, prop)
        d_txt = "D+cover" if need_d else "cover, no D"
        base = pack(live)
        plus = pack(live_l10)
        lift = None
        if base["hr"] is not None and plus["n"]:
            lift = round(plus["hr"] - base["hr"], 1)
        row = {
            "label": label,
            "book": book,
            "rule": f"L5=5 + {d_txt} (cover floor {need})",
            "ungated": pack(pool),
            "l5eq5": pack(l5),
            "live": base,
            "live_plus_l10": plus,
            "live_fail_l10": pack(live_no_l10),
            "lift_pp": lift,
            "cover_filled_on_l5": sum(1 for r in l5 if r["cover"] is not None),
        }
        report["chips"].append(row)
        print(f"\n==== {label} {book} ====")
        print(f"  rule                         {row['rule']}")
        print(f"  ungated unique               {fmt(row['ungated'])}")
        print(f"  L5=5                         {fmt(row['l5eq5'])}")
        print(f"  LIVE (no L10)                {fmt(row['live'])}")
        print(f"  LIVE + L10>=8                {fmt(row['live_plus_l10'])}  lift {lift}")
        print(f"  LIVE but L10<8 (now on card) {fmt(row['live_fail_l10'])}")
        print(f"  cover filled on L5=5         {row['cover_filled_on_l5']}/{len(l5)}")
        if book != "Goblin OVER" or prop not in {"rush_yds", "rec_yds"}:
            both_drop = [r for r in l5 if not (r["cover_ok"] and (r["d"] if need_d else True))]
            print(f"  rows the live rule removed   {fmt(pack(both_drop))}")
            row["live_removed"] = pack(both_drop)
        if prop in {"rush_yds", "rec_yds"} and book in {"Goblin OVER", "Standard UNDER"}:
            d_only = [r for r in l5 if r["d"]]
            c_only = [r for r in l5 if r["cover_ok"]]
            d_drop = [r for r in l5 if not r["d"]]
            c_drop = [r for r in l5 if not r["cover_ok"]]
            both_drop = [r for r in l5 if not (r["cover_ok"] and r["d"])]
            print(f"  L5=5 + D only                {fmt(pack(d_only))}")
            print(f"  L5=5, D removed              {fmt(pack(d_drop))}")
            print(f"  L5=5 + cover only            {fmt(pack(c_only))}")
            print(f"  L5=5, cover removed          {fmt(pack(c_drop))}")
            print(f"  L5=5, D+cover removed        {fmt(pack(both_drop))}")
            row["l5_d"] = pack(d_only)
            row["l5_d_removed"] = pack(d_drop)
            row["l5_cover"] = pack(c_only)
            row["l5_cover_removed"] = pack(c_drop)
            row["live_removed"] = pack(both_drop)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
