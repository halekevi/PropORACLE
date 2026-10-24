"""Joint gate combos for Goblin OVER / Standard OVER / Standard UNDER.

Uses the same step8×graded join as backtest_three_book_gates.py.

  py -3.14 scripts/prop_gate_combo_search.py
"""
from __future__ import annotations

import json
import math
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

OPEN = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path.insert(0, str(OPEN / "scripts"))
import _avg_gap_by_badge as B  # noqa: E402
import prop_hit_tiers as T  # noqa: E402

GRADED = MAIN / "ui_runner" / "templates"
OUT = OPEN / "data" / "reports" / "prop_gate_combo_search.json"
CACHE = OPEN / "data" / "reports" / "_prop_gate_combo_merged.parquet"
GOBLIN_FLOOR = T.GOBLIN_FLOOR
SA = T.S_KEYS | T.A_KEYS


def rate(h, n):
    return None if n <= 0 else round(100.0 * h / n, 1)


def cell(mask, hit):
    n = int(mask.sum())
    h = int(hit[mask].sum()) if n else 0
    return {"hits": h, "n": n, "rate": rate(h, n)}


def d_aligned(sport, prop, direction, def_b) -> bool:
    d = str(def_b or "")
    over = str(direction).upper() == "OVER"
    hitter_k = sport == "MLB" and prop == "hitter_ks"
    if hitter_k:
        want = ("Elite", "Above Avg") if over else ("Weak", "Below Avg")
    elif over:
        want = ("Weak", "Below Avg")
    else:
        want = ("Elite", "Above Avg")
    return d in want


def sport_cover_ok(sport, gap) -> bool:
    need = GOBLIN_FLOOR.get(sport, 1.0)
    return gap is not None and gap == gap and gap >= need


def main() -> None:
    dates = sorted(
        p.name[len("graded_props_") : -5] for p in GRADED.glob("graded_props_20*.json")
    )
    if CACHE.is_file():
        print(f"cache {CACHE}", flush=True)
        merged = pd.read_parquet(CACHE)
        hit = merged["hit"].astype(int)
        l5 = pd.to_numeric(merged["l5_hits"], errors="coerce")
        l10 = pd.to_numeric(merged["l10_hits"], errors="coerce")
        szn = pd.to_numeric(merged["szn_hr"], errors="coerce")
        g5 = pd.to_numeric(merged["g_l5"], errors="coerce")
        _combo_report(merged, hit, l5, l10, szn, g5)
        return
    frames = []
    for date in dates:
        for sport in B.SPORTS:
            p = B.find_step8(date, sport)
            if not p:
                continue
            print(f"load {date} {sport}", flush=True)
            frames.append(B.load_step8(p, sport, date))
    step8 = pd.concat(frames, ignore_index=True)

    rows = []
    for date in dates:
        raw = json.loads((GRADED / f"graded_props_{date}.json").read_text(encoding="utf-8"))
        for g in raw.get("props", []):
            sport = str(g.get("sport") or "")
            if sport not in B.SPORTS:
                continue
            result = str(g.get("result") or "").upper()
            if result not in ("HIT", "MISS"):
                continue
            pk = str(g.get("pick_type") or "").strip().lower()
            if "goblin" in pk:
                pick = "Goblin"
            elif "dem" in pk:
                continue
            else:
                pick = "Standard"
            d = str(g.get("direction") or g.get("over_under") or "").strip().upper()
            if d in ("O", "OVER"):
                d = "OVER"
            elif d in ("U", "UNDER"):
                d = "UNDER"
            else:
                continue
            try:
                line_v = float(g.get("line"))
            except (TypeError, ValueError):
                line_v = math.nan
            rows.append(
                {
                    "date": date,
                    "sport": sport,
                    "player_n": re.sub(
                        r"\s+",
                        " ",
                        re.sub(r"[^a-z0-9 ]+", " ", str(g.get("player") or "").strip().lower()),
                    ).strip(),
                    "prop_n": re.sub(r"\s+", " ", str(g.get("prop") or "").strip().lower()),
                    "line": line_v,
                    "direction": d,
                    "pick": pick,
                    "hit": 1 if result == "HIT" else 0,
                }
            )
    graded = pd.DataFrame(rows)
    graded["line_r"] = graded["line"].round(2)
    keys = ["date", "sport", "player_n", "prop_n", "line_r", "direction"]
    merged = graded.merge(step8, on=keys, how="inner")
    leftover = graded.merge(merged[keys].drop_duplicates().assign(_ok=1), on=keys, how="left")
    leftover = leftover[leftover["_ok"].isna()].drop(columns="_ok")
    loose = leftover.merge(
        step8.drop(columns=["line_r"]).drop_duplicates(
            ["date", "sport", "player_n", "prop_n", "direction"], keep="last"
        ),
        on=["date", "sport", "player_n", "prop_n", "direction"],
        how="inner",
    )
    if len(loose):
        merged = pd.concat([merged, loose], ignore_index=True)

    line = pd.to_numeric(merged["line"], errors="coerce")
    over = merged["direction"].eq("OVER")
    for src, dest in (("l5", "g_l5"), ("l10", "g_l10"), ("szn", "g_szn")):
        avg = pd.to_numeric(merged[src], errors="coerce")
        gap = pd.Series(np.nan, index=merged.index)
        gap = gap.mask(over, avg - line)
        gap = gap.mask(~over, line - avg)
        merged[dest] = gap
    merged["min_gap"] = merged[["g_l5", "g_l10", "g_szn"]].min(axis=1, skipna=True)
    merged["book"] = merged["pick"] + " " + merged["direction"]
    l5_col = np.where(over, merged["l5_over"], merged["l5_under"])
    l10_col = np.where(over, merged.get("l10_over", np.nan), merged.get("l10_under", np.nan))
    merged["l5_hits"] = pd.to_numeric(pd.Series(l5_col, index=merged.index), errors="coerce")
    merged["l10_hits"] = pd.to_numeric(pd.Series(l10_col, index=merged.index), errors="coerce")
    hr = pd.to_numeric(merged.get("hit_rate"), errors="coerce")
    merged["szn_hr"] = np.where(hr > 1.0, hr / 100.0, hr)
    merged["prop"] = [T.canon_prop(s, p) for s, p in zip(merged["sport"], merged["prop_n"])]
    merged["shadow"] = [
        T.is_shadow(s, b, p) for s, b, p in zip(merged["sport"], merged["book"], merged["prop"])
    ]
    merged["sa"] = [
        (s, b, p) in SA for s, b, p in zip(merged["sport"], merged["book"], merged["prop"])
    ]
    def_b = merged["def_tier"].astype(str).str.strip()
    low = def_b.str.lower()
    merged["def_b"] = ""
    merged.loc[low.str.contains("elite", na=False), "def_b"] = "Elite"
    merged.loc[low.str.contains("above", na=False), "def_b"] = "Above Avg"
    merged.loc[low.str.contains("below", na=False), "def_b"] = "Below Avg"
    merged.loc[low.str.contains("weak", na=False), "def_b"] = "Weak"
    merged.loc[low.isin(["avg", "average", "medium"]), "def_b"] = "Avg"
    merged["d_ok"] = [
        d_aligned(s, p, d, db)
        for s, p, d, db in zip(merged["sport"], merged["prop"], merged["direction"], merged["def_b"])
    ]
    merged["cover_ok"] = [
        sport_cover_ok(s, g) for s, g in zip(merged["sport"], merged["min_gap"])
    ]
    mt = merged["min_tier"].astype(str).str.upper()
    merged["high_min"] = mt.str.contains("HIGH", na=False)
    merged = merged[merged["book"].isin(("Goblin OVER", "Standard OVER", "Standard UNDER"))].copy()
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    try:
        merged.to_parquet(CACHE, index=False)
        print("wrote", CACHE, flush=True)
    except Exception as exc:
        print("parquet skip", exc, flush=True)
    hit = merged["hit"].astype(int)
    l5 = merged["l5_hits"]
    l10 = merged["l10_hits"]
    szn = merged["szn_hr"]
    g5 = merged["g_l5"]
    _combo_report(merged, hit, l5, l10, szn, g5)


def _combo_report(merged, hit, l5, l10, szn, g5) -> None:
    combos = {
        "all": pd.Series(True, index=merged.index),
        "L5>=4": l5 >= 4,
        "L5=5": l5 == 5,
        "L10>=8": l10 >= 8,
        "szn>70": szn > 0.70,
        "gap>=1": g5 >= 1,
        "gap>=2": g5 >= 2,
        "sport cover floor": merged["cover_ok"],
        "D aligned": merged["d_ok"],
        "D not Avg": merged["def_b"].ne("Avg") & merged["def_b"].ne(""),
        "not shadow": ~merged["shadow"],
        "S/A market": merged["sa"],
        "L5>=4 + cover": (l5 >= 4) & merged["cover_ok"],
        "L5>=4 + gap>=1": (l5 >= 4) & (g5 >= 1),
        "L5>=4 + gap>=2": (l5 >= 4) & (g5 >= 2),
        "L5>=4 + L10>=8": (l5 >= 4) & (l10 >= 8),
        "L5>=4 + szn>70": (l5 >= 4) & (szn > 0.70),
        "L5>=4 + D aligned": (l5 >= 4) & merged["d_ok"],
        "L5>=4 + D not Avg": (l5 >= 4) & merged["def_b"].ne("Avg") & merged["def_b"].ne(""),
        "L5>=4 + not shadow": (l5 >= 4) & ~merged["shadow"],
        "L5>=4 + S/A market": (l5 >= 4) & merged["sa"],
        "L5>=4 + cover + not shadow": (l5 >= 4) & merged["cover_ok"] & ~merged["shadow"],
        "L5>=4 + cover + L10>=8": (l5 >= 4) & merged["cover_ok"] & (l10 >= 8),
        "L5>=4 + cover + szn>70": (l5 >= 4) & merged["cover_ok"] & (szn > 0.70),
        "L5>=4 + cover + L10>=8 + szn>70": (l5 >= 4) & merged["cover_ok"] & (l10 >= 8) & (szn > 0.70),
        "L5>=4 + cover + D aligned": (l5 >= 4) & merged["cover_ok"] & merged["d_ok"],
        "L5>=4 + cover + D not Avg": (l5 >= 4)
        & merged["cover_ok"]
        & merged["def_b"].ne("Avg")
        & merged["def_b"].ne(""),
        "L5=5 + cover": (l5 == 5) & merged["cover_ok"],
        "L5=5 + cover + not shadow": (l5 == 5) & merged["cover_ok"] & ~merged["shadow"],
        "L5=5 + gap>=1": (l5 == 5) & (g5 >= 1),
        "L5=5 + gap>=2": (l5 == 5) & (g5 >= 2),
        "L5=5 + L10>=8": (l5 == 5) & (l10 >= 8),
        "L5=5 + szn>70": (l5 == 5) & (szn > 0.70),
        "L5=5 + D aligned": (l5 == 5) & merged["d_ok"],
        "L5=5 + D not Avg": (l5 == 5) & merged["def_b"].ne("Avg") & merged["def_b"].ne(""),
        "L5=5 + not shadow": (l5 == 5) & ~merged["shadow"],
        "L5=5 + S/A market": (l5 == 5) & merged["sa"],
        "L5=5 + HIGH min": (l5 == 5) & merged["high_min"],
        "L5=5 + Diamond (L10+szn)": (l5 == 5) & (l10 >= 8) & (szn > 0.70),
        "L5=5 + cover + Diamond": (l5 == 5) & merged["cover_ok"] & (l10 >= 8) & (szn > 0.70),
        "L5=5 + D aligned + gap>=1": (l5 == 5) & merged["d_ok"] & (g5 >= 1),
        "L5=5 + cover + D aligned": (l5 == 5) & merged["cover_ok"] & merged["d_ok"],
        "L5>=4 + cover + S/A": (l5 >= 4) & merged["cover_ok"] & merged["sa"],
        "Platinum-like (L5>=4 + one of L10/szn)": (l5 >= 4)
        & ((l10 >= 8) | (szn > 0.70))
        & ~((l10 >= 8) & (szn > 0.70)),
        "Diamond-like (L5>=4 + L10 + szn)": (l5 >= 4) & (l10 >= 8) & (szn > 0.70),
        "L5>=4 + cover + Platinum-like": (l5 >= 4)
        & merged["cover_ok"]
        & ((l10 >= 8) | (szn > 0.70)),
        "L5>=4 + HIGH min": (l5 >= 4) & merged["high_min"],
        "L5>=4 + D aligned + gap>=1": (l5 >= 4) & merged["d_ok"] & (g5 >= 1),
    }

    payload = {
        "from": str(merged["date"].min()),
        "to": str(merged["date"].max()),
        "n_joined": int(len(merged)),
        "by_book": {},
        "standard_by_sport": {},
    }
    print(f"joined {len(merged)} {payload['from']} -> {payload['to']}", flush=True)

    for book in ("Goblin OVER", "Standard OVER", "Standard UNDER"):
        bm = merged["book"].eq(book)
        recs = {}
        print(f"\n===== {book} =====")
        for name, mask in combos.items():
            c = cell(bm & mask, hit)
            recs[name] = c
            if c["n"] >= 10:
                print(f"  {name:48} {c['rate']:5.1f}%  {c['hits']}/{c['n']}")
        payload["by_book"][book] = recs

    for book in ("Standard OVER", "Standard UNDER"):
        payload["standard_by_sport"][book] = {}
        print(f"\n===== {book} by sport (key combos) =====")
        for sport in ("WNBA", "MLB", "Soccer", "Tennis"):
            sm = merged["book"].eq(book) & merged["sport"].eq(sport)
            recs = {}
            for name in (
                "all",
                "L5>=4",
                "L5=5",
                "Diamond-like (L5>=4 + L10 + szn)",
                "L5=5 + Diamond (L10+szn)",
                "L5=5 + L10>=8",
                "L5=5 + szn>70",
                "L5=5 + gap>=1",
                "L5=5 + D aligned",
                "L5=5 + not shadow",
                "L5=5 + cover",
                "L5=5 + HIGH min",
                "L5>=4 + D aligned",
                "L5>=4 + not shadow",
                "L10>=8",
            ):
                recs[name] = cell(sm & combos[name], hit)
            payload["standard_by_sport"][book][sport] = recs
            a = recs["all"]
            b4 = recs["L5>=4"]
            b5 = recs["L5=5"]
            dia = recs["Diamond-like (L5>=4 + L10 + szn)"]
            print(
                f"  {sport:8} all {a['rate']}% {a['hits']}/{a['n']}   "
                f"L5>=4 {b4['rate']}% {b4['hits']}/{b4['n']}   "
                f"L5=5 {b5['rate']}% {b5['hits']}/{b5['n']}   "
                f"Diamond {dia['rate']}% {dia['hits']}/{dia['n']}"
            )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\nwrote", OUT)


if __name__ == "__main__":
    main()
