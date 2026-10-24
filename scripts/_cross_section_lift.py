"""Cross-section lift: L5/L10/season, D, minutes, role, deltas vs book baseline.

Reads data/retrain_dataset.csv (step8 × graded HIT/MISS). Writes
data/reports/cross_section_lift.json.

  py -3.14 scripts/_cross_section_lift.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from prop_hit_tiers import cover_need  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402

INP = ROOT / "data" / "retrain_dataset.csv"
OUT = ROOT / "data" / "reports" / "cross_section_lift.json"

COLS = [
    "sport",
    "prop",
    "line",
    "direction",
    "pick_type",
    "pick_type_group",
    "result_binary",
    "file_date",
    "l5_over",
    "l5_under",
    "hit_rate_l5",
    "hit_rate_l10",
    "def_tier",
    "def_tier_norm",
    "stat_def_tier",
    "minutes_tier",
    "min_per_game",
    "min_last5_avg",
    "min_season_avg",
    "usage_pct",
    "usage_tier",
    "star_tier",
    "role_tier",
    "team_top3_rank",
    "top3_weak_overperformer",
    "top3_elite_fader",
    "stat_last5_avg",
    "stat_last10_avg",
    "stat_season_avg",
    "consistency_grade",
]

N_SINGLE = 80
N_STACK = 60
N_SPORT = 40
N_PROP = 40


def _pct(h: int, n: int) -> float | None:
    if n <= 0:
        return None
    return round(100.0 * h / n, 1)


def _norm_rate(s: pd.Series) -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    if x.notna().any() and float(x.dropna().median()) > 1.5:
        x = x / 100.0
    return x


def _def_band(s: object) -> str:
    t = str(s or "").strip().replace("_", " ").title()
    t = t.replace("Avg", "Avg")
    if t in {"Weak", "Below Avg", "Below Average"}:
        return "Weak|Below"
    if t in {"Elite", "Above Avg", "Above Average"}:
        return "Elite|Above"
    if t in {"Avg", "Average"}:
        return "Avg"
    if t in {"", "Nan", "None", "Unknown", "Na"}:
        return "missing"
    return t or "missing"


def _d_aligned(sport: str, prop: str, direction: str, band: str) -> str:
    if band in {"missing", "Avg"}:
        return band
    over = str(direction).upper() == "OVER"
    hitter_k = sport == "MLB" and prop == "hitter_ks"
    if hitter_k:
        want_over = band == "Elite|Above"
    else:
        want_over = band == "Weak|Below"
    if over == want_over and band in {"Weak|Below", "Elite|Above"}:
        return "aligned"
    if band in {"Weak|Below", "Elite|Above"}:
        return "misaligned"
    return band


def _book(pick_type: str, direction: str) -> str | None:
    pt = str(pick_type or "").strip().lower()
    d = str(direction or "").strip().upper()
    if "demon" in pt:
        return None
    if "goblin" in pt:
        return "Goblin OVER" if d == "OVER" else None
    if "standard" in pt or pt in {"", "nan"}:
        if d == "OVER":
            return "Standard OVER"
        if d == "UNDER":
            return "Standard UNDER"
    return None


def _l10_hits(rate: pd.Series) -> pd.Series:
    r = _norm_rate(rate)
    return (r * 10.0).round()


def cell(mask: pd.Series, hit: pd.Series) -> dict:
    n = int(mask.sum())
    h = int(hit[mask].sum()) if n else 0
    return {"hits": h, "n": n, "hr": _pct(h, n)}


def lift_row(name: str, mask: pd.Series, hit: pd.Series, base_hr: float, min_n: int) -> dict | None:
    c = cell(mask, hit)
    if c["n"] < min_n or c["hr"] is None:
        return None
    c["gate"] = name
    c["lift"] = round(c["hr"] - base_hr, 1)
    return c


def main() -> None:
    print(f"load {INP}", flush=True)
    df = pd.read_csv(INP, usecols=lambda c: c in COLS, low_memory=False)
    df["result_binary"] = pd.to_numeric(df["result_binary"], errors="coerce")
    df = df[df["result_binary"].isin((0, 1))].copy()
    df["sport"] = df["sport"].astype(str).str.strip()
    df["direction"] = df["direction"].astype(str).str.strip().str.upper()
    df["canon"] = [canon_prop(s, p) for s, p in zip(df["sport"], df["prop"])]
    df["book"] = [_book(pt, d) for pt, d in zip(df["pick_type"], df["direction"])]
    df = df[df["book"].notna()].copy()

    over = df["direction"].eq("OVER")
    l5o = pd.to_numeric(df["l5_over"], errors="coerce")
    l5u = pd.to_numeric(df["l5_under"], errors="coerce")
    df["dir_l5"] = np.where(over, l5o, l5u)
    df["dir_l10"] = _l10_hits(df["hit_rate_l10"])

    line = pd.to_numeric(df["line"], errors="coerce")
    a5 = pd.to_numeric(df["stat_last5_avg"], errors="coerce")
    a10 = pd.to_numeric(df["stat_last10_avg"], errors="coerce")
    asz = pd.to_numeric(df["stat_season_avg"], errors="coerce")
    df["d_l5"] = np.where(over, a5 - line, line - a5)
    df["d_l10"] = np.where(over, a10 - line, line - a10)
    df["d_szn"] = np.where(over, asz - line, line - asz)
    df["hot_l5"] = np.where(over, a5 - asz, asz - a5)
    df["hot_l10"] = np.where(over, a10 - asz, asz - a10)

    def_src = df["stat_def_tier"].where(df["stat_def_tier"].notna() & (df["stat_def_tier"].astype(str).str.strip() != ""), df["def_tier_norm"])
    def_src = def_src.where(def_src.notna() & (def_src.astype(str).str.strip() != ""), df["def_tier"])
    df["def_band"] = def_src.map(_def_band)
    df["d_status"] = [
        _d_aligned(s, p, d, b)
        for s, p, d, b in zip(df["sport"], df["canon"], df["direction"], df["def_band"])
    ]

    top = pd.to_numeric(df["team_top3_rank"], errors="coerce")
    df["role"] = np.where(top.le(3) & top.notna(), "top3", np.where(top.notna(), "not_top3", "missing"))

    mt = df["minutes_tier"].astype(str).str.strip().str.upper()
    df["min_band"] = np.where(
        mt.isin({"HIGH", "HI", "STARTER"}),
        "HIGH",
        np.where(mt.isin({"MED", "MEDIUM", "MID"}), "MED", np.where(mt.isin({"LOW", "BENCH"}), "LOW", "missing")),
    )

    ut = df["usage_tier"].astype(str).str.strip().str.upper()
    df["usage_band"] = np.where(
        ut.isin({"HIGH", "STAR", "ELITE"}),
        "HIGH",
        np.where(ut.isin({"", "NAN", "NONE", "UNKNOWN"}), "missing", "other"),
    )

    need = [cover_need(s, p) for s, p in zip(df["sport"], df["canon"])]
    df["cover_need"] = need
    df["cover_ok"] = df["d_l5"] >= df["cover_need"]

    hit = df["result_binary"].astype(int)
    fill = {
        "n_rows": int(len(df)),
        "date_min": str(df["file_date"].min()),
        "date_max": str(df["file_date"].max()),
        "dir_l5": round(100.0 * df["dir_l5"].notna().mean(), 1),
        "dir_l10": round(100.0 * df["dir_l10"].notna().mean(), 1),
        "stat_last5_avg": round(100.0 * a5.notna().mean(), 1),
        "stat_last10_avg": round(100.0 * a10.notna().mean(), 1),
        "stat_season_avg": round(100.0 * asz.notna().mean(), 1),
        "def_known": round(100.0 * df["def_band"].ne("missing").mean(), 1),
        "minutes_tier": round(100.0 * df["min_band"].ne("missing").mean(), 1),
        "team_top3_rank": round(100.0 * top.notna().mean(), 1),
        "team_bottom3_rank": 0.0,
        "usage_tier": round(100.0 * df["usage_band"].ne("missing").mean(), 1),
        "note": "team_bottom3_rank is not on retrain_dataset.csv; role uses team_top3_rank only.",
    }

    books = ["Goblin OVER", "Standard OVER", "Standard UNDER"]
    sports = sorted(df["sport"].unique())

    single: list[dict] = []
    stacks: list[dict] = []
    sport_stacks: list[dict] = []
    prop_cells: list[dict] = []
    baselines: list[dict] = []

    def add_gates(sub: pd.DataFrame, book: str, sport: str | None, min_n: int, dest: list[dict]) -> float:
        h = sub["result_binary"].astype(int)
        n = len(sub)
        base = 100.0 * float(h.mean()) if n else 0.0
        baselines.append({"book": book, "sport": sport or "ALL", "n": n, "hr": round(base, 1)})

        l5 = pd.to_numeric(sub["dir_l5"], errors="coerce")
        l10 = pd.to_numeric(sub["dir_l10"], errors="coerce")
        gates = {
            "L5 0-3": l5.le(3) & l5.notna(),
            "L5=4": l5.eq(4),
            "L5>=4": l5.ge(4),
            "L5=5": l5.eq(5),
            "L10<=6": l10.le(6) & l10.notna(),
            "L10>=8": l10.ge(8),
            "L10>=9": l10.ge(9),
            "D aligned": sub["d_status"].eq("aligned"),
            "D misaligned": sub["d_status"].eq("misaligned"),
            "D Avg": sub["d_status"].eq("Avg"),
            "D missing": sub["d_status"].eq("missing"),
            "minutes HIGH": sub["min_band"].eq("HIGH"),
            "minutes MED": sub["min_band"].eq("MED"),
            "minutes LOW": sub["min_band"].eq("LOW"),
            "minutes missing": sub["min_band"].eq("missing"),
            "top3 at stat": sub["role"].eq("top3"),
            "not top3": sub["role"].eq("not_top3"),
            "role missing": sub["role"].eq("missing"),
            "cover vs L5": sub["cover_ok"].eq(True) & sub["d_l5"].notna(),
            "no cover vs L5": sub["cover_ok"].eq(False) & sub["d_l5"].notna(),
            "L5 avg missing": sub["d_l5"].isna(),
            "season covers line": sub["d_szn"].gt(0),
            "season under line": sub["d_szn"].le(0) & sub["d_szn"].notna(),
            "L10 covers line": sub["d_l10"].gt(0),
            "hot L5 vs season": sub["hot_l5"].gt(0),
            "cold L5 vs season": sub["hot_l5"].le(0) & sub["hot_l5"].notna(),
            "usage HIGH": sub["usage_band"].eq("HIGH"),
            "top3 Weak overperformer": pd.to_numeric(sub["top3_weak_overperformer"], errors="coerce").eq(1),
            "top3 Elite fader": pd.to_numeric(sub["top3_elite_fader"], errors="coerce").eq(1),
        }
        joints = {
            "L5>=4 + D aligned": (l5.ge(4)) & sub["d_status"].eq("aligned"),
            "L5>=4 + cover": (l5.ge(4)) & sub["cover_ok"].eq(True),
            "L5>=4 + minutes HIGH": (l5.ge(4)) & sub["min_band"].eq("HIGH"),
            "L5>=4 + top3": (l5.ge(4)) & sub["role"].eq("top3"),
            "L5>=4 + season covers": (l5.ge(4)) & sub["d_szn"].gt(0),
            "L5>=4 + hot L5": (l5.ge(4)) & sub["hot_l5"].gt(0),
            "L5>=4 + L10>=8": (l5.ge(4)) & l10.ge(8),
            "L5=5 + cover": (l5.eq(5)) & sub["cover_ok"].eq(True),
            "L5=5 + D aligned": (l5.eq(5)) & sub["d_status"].eq("aligned"),
            "D aligned + cover": sub["d_status"].eq("aligned") & sub["cover_ok"].eq(True),
            "L5>=4 + D + cover": (l5.ge(4)) & sub["d_status"].eq("aligned") & sub["cover_ok"].eq(True),
            "L5=5 + D + cover": (l5.eq(5)) & sub["d_status"].eq("aligned") & sub["cover_ok"].eq(True),
            "L5=5 + D + minutes HIGH": (l5.eq(5)) & sub["d_status"].eq("aligned") & sub["min_band"].eq("HIGH"),
            "L5>=4 + D + minutes HIGH": (l5.ge(4)) & sub["d_status"].eq("aligned") & sub["min_band"].eq("HIGH"),
            "L5>=4 + cover + top3": (l5.ge(4)) & sub["cover_ok"].eq(True) & sub["role"].eq("top3"),
            "L5>=4 + cover + minutes HIGH": (l5.ge(4)) & sub["cover_ok"].eq(True) & sub["min_band"].eq("HIGH"),
            "listed L5>=4 + D + cover + minutes HIGH": (l5.ge(4))
            & sub["d_status"].eq("aligned")
            & sub["cover_ok"].eq(True)
            & sub["min_band"].eq("HIGH"),
        }
        for name, mask in {**gates, **joints}.items():
            row = lift_row(name, mask.fillna(False), h, base, min_n)
            if row:
                row["book"] = book
                row["sport"] = sport or "ALL"
                dest.append(row)
        return base

    for book in books:
        sub = df[df["book"].eq(book)]
        add_gates(sub, book, None, N_SINGLE if book != "Goblin OVER" else N_STACK, single)
        for sport in sports:
            ss = sub[sub["sport"].eq(sport)]
            if len(ss) < N_SPORT:
                continue
            add_gates(ss, book, sport, N_SPORT, sport_stacks)

    # Joint stack table (all-sport) kept separately for ranking
    for book in books:
        sub = df[df["book"].eq(book)]
        h = sub["result_binary"].astype(int)
        base = 100.0 * float(h.mean())
        l5 = pd.to_numeric(sub["dir_l5"], errors="coerce")
        l10 = pd.to_numeric(sub["dir_l10"], errors="coerce")
        joint_names = [
            "L5>=4 + D aligned",
            "L5>=4 + cover",
            "L5>=4 + minutes HIGH",
            "L5>=4 + top3",
            "L5>=4 + season covers",
            "L5>=4 + hot L5",
            "L5>=4 + L10>=8",
            "L5=5 + cover",
            "L5=5 + D aligned",
            "D aligned + cover",
            "L5>=4 + D + cover",
            "L5=5 + D + cover",
            "L5=5 + D + minutes HIGH",
            "L5>=4 + D + minutes HIGH",
            "L5>=4 + cover + top3",
            "L5>=4 + cover + minutes HIGH",
            "listed L5>=4 + D + cover + minutes HIGH",
        ]
        for r in single:
            if r["book"] == book and r["sport"] == "ALL" and r["gate"] in joint_names:
                stacks.append(r)

    # Prop-level: listed L5>=4 vs extra gates, n>=40
    for book in books:
        for sport in sports:
            for prop in sorted(df.loc[df["sport"].eq(sport), "canon"].dropna().unique()):
                ss = df[df["book"].eq(book) & df["sport"].eq(sport) & df["canon"].eq(prop)]
                if len(ss) < N_PROP:
                    continue
                h = ss["result_binary"].astype(int)
                base = 100.0 * float(h.mean())
                l5 = pd.to_numeric(ss["dir_l5"], errors="coerce")
                listed = l5.ge(4)
                extra = {
                    "open": pd.Series(True, index=ss.index),
                    "L5>=4": listed,
                    "L5>=4 + D": listed & ss["d_status"].eq("aligned"),
                    "L5>=4 + cover": listed & ss["cover_ok"].eq(True),
                    "L5>=4 + D + cover": listed & ss["d_status"].eq("aligned") & ss["cover_ok"].eq(True),
                    "L5>=4 + minutes HIGH": listed & ss["min_band"].eq("HIGH"),
                    "L5>=4 + top3": listed & ss["role"].eq("top3"),
                    "L5=5": l5.eq(5),
                    "L5=5 + D": l5.eq(5) & ss["d_status"].eq("aligned"),
                    "L5=5 + cover": l5.eq(5) & ss["cover_ok"].eq(True),
                    "L5=5 + D + cover": l5.eq(5) & ss["d_status"].eq("aligned") & ss["cover_ok"].eq(True),
                }
                row = {"book": book, "sport": sport, "prop": prop, "open_n": len(ss), "open_hr": round(base, 1)}
                keep = False
                for name, mask in extra.items():
                    c = cell(mask.fillna(False), h)
                    floor = 15 if "L5=5" in name else 25
                    if c["n"] < floor and name != "open":
                        row[name] = None
                        continue
                    row[name] = c
                    if name != "open" and c["hr"] is not None and c["n"] >= N_PROP:
                        keep = True
                if keep:
                    prop_cells.append(row)

    def rank_key(r: dict) -> tuple:
        return (-(r.get("lift") or -999), -(r.get("hr") or 0), -r["n"])

    # Incremental lift vs L5>=4 (same book, all sports)
    incremental: list[dict] = []
    for book in books:
        listed = next((r for r in single if r["book"] == book and r["sport"] == "ALL" and r["gate"] == "L5>=4"), None)
        if not listed:
            continue
        for r in single:
            if r["book"] != book or r["sport"] != "ALL":
                continue
            if not r["gate"].startswith("L5"):
                continue
            incremental.append(
                {
                    **r,
                    "vs_listed": round(r["hr"] - listed["hr"], 1),
                    "listed_hr": listed["hr"],
                    "listed_n": listed["n"],
                }
            )

    d_cover_ladder: list[dict] = []
    for book in books:
        for sport in [None, *sports]:
            sub = df[df["book"].eq(book)] if sport is None else df[df["book"].eq(book) & df["sport"].eq(sport)]
            if len(sub) < 20:
                continue
            h = sub["result_binary"].astype(int)
            base = 100.0 * float(h.mean()) if len(sub) else 0.0
            l5 = pd.to_numeric(sub["dir_l5"], errors="coerce")
            specs = {
                "L5>=4": l5.ge(4),
                "L5=5": l5.eq(5),
                "L5>=4 + D aligned": l5.ge(4) & sub["d_status"].eq("aligned"),
                "L5=5 + D aligned": l5.eq(5) & sub["d_status"].eq("aligned"),
                "L5>=4 + cover": l5.ge(4) & sub["cover_ok"].eq(True),
                "L5=5 + cover": l5.eq(5) & sub["cover_ok"].eq(True),
                "L5>=4 + D + cover": l5.ge(4) & sub["d_status"].eq("aligned") & sub["cover_ok"].eq(True),
                "L5=5 + D + cover": l5.eq(5) & sub["d_status"].eq("aligned") & sub["cover_ok"].eq(True),
            }
            rec = {"book": book, "sport": sport or "ALL", "open_n": len(sub), "open_hr": round(base, 1)}
            for name, mask in specs.items():
                row = lift_row(name, mask.fillna(False), h, base, 20)
                rec[name] = row
            d_cover_ladder.append(rec)

    KEY_GATES = {
        "L5>=4",
        "L5=5",
        "D aligned",
        "cover vs L5",
        "minutes HIGH",
        "top3 at stat",
        "season covers line",
        "hot L5 vs season",
        "L10>=8",
        "L5>=4 + D aligned",
        "L5>=4 + cover",
        "L5>=4 + minutes HIGH",
        "L5>=4 + top3",
        "L5>=4 + season covers",
        "L5>=4 + hot L5",
        "L5>=4 + L10>=8",
        "L5=5 + cover",
        "L5=5 + D aligned",
        "L5=5 + D + cover",
        "D aligned + cover",
        "L5>=4 + D + cover",
        "L5>=4 + D + minutes HIGH",
        "L5>=4 + cover + top3",
        "L5>=4 + cover + minutes HIGH",
        "listed L5>=4 + D + cover + minutes HIGH",
    }
    payload = {
        "source": str(INP).replace("\\", "/"),
        "fill": fill,
        "baselines": [b for b in baselines if b["sport"] == "ALL" or b["n"] >= N_SPORT],
        "single_all": sorted(
            [r for r in single if r["sport"] == "ALL"],
            key=rank_key,
        ),
        "stacks_all": sorted(
            [r for r in stacks if r["sport"] == "ALL"],
            key=rank_key,
        ),
        "sport_stacks": sorted(
            [r for r in sport_stacks if r["gate"] in KEY_GATES],
            key=rank_key,
        ),
        "d_cover_ladder": d_cover_ladder,
        "incremental_vs_listed": sorted(incremental, key=lambda r: (r["book"], -abs(r.get("vs_listed") or 0))),
        "prop_cells": prop_cells,
        "rules": {
            "books": "Goblin OVER only; Standard OVER; Standard UNDER. Demons dropped.",
            "d": "Overs vs Weak|Below; Unders vs Elite|Above; Avg never aligns. MLB hitter_ks inverted.",
            "cover": "Play-side last5 avg minus line, per-prop unit from prop_hit_tiers.cover_need.",
            "n_floor": {"single_all": N_SINGLE, "stack": N_STACK, "sport": N_SPORT, "prop": N_PROP},
            "missing": "Bottom-3 on team at the stat is not in the retrain join.",
        },
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"wrote {OUT} rows={fill['n_rows']} {fill['date_min']}..{fill['date_max']}", flush=True)

    # Console digest
    print("\n=== FILL ===", flush=True)
    for k, v in fill.items():
        print(f"  {k}: {v}", flush=True)
    print("\n=== BASELINES ===", flush=True)
    for b in baselines:
        if b["sport"] == "ALL":
            print(f"  {b['book']}: {b['hr']}% n={b['n']}", flush=True)
    print("\n=== D+COVER LADDER ===", flush=True)
    keep_sp = {"ALL", "WNBA", "MLB", "Soccer", "Tennis", "NBA"}
    for rec in d_cover_ladder:
        if rec["sport"] not in keep_sp:
            continue
        a = rec.get("L5>=4 + D + cover")
        b = rec.get("L5=5 + D + cover")
        if not a and not b:
            continue
        a_s = f"{a['hr']}% n={a['n']}" if a else "--"
        b_s = f"{b['hr']}% n={b['n']}" if b else "--"
        print(f"  {rec['sport']:7} {rec['book']:16}  L5>=4+D+cov {a_s:18}  L5=5+D+cov {b_s}", flush=True)


if __name__ == "__main__":
    main()
