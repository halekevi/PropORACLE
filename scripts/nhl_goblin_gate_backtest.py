#!/usr/bin/env python3
"""NHL keep-gate backtest (read-only).

Reads graded props and the dated NHL pipeline outputs under ``--root`` (the live checkout) and
writes ``data/reports/nhl_goblin_gate_backtest.json`` under ``--out-root`` (default: this repo).
Nothing in the live checkout is modified.

Bar (set before looking at results): n >= 100 per cell and hit rate >= 0.745 (the Goblin-70
book). Anything below stays off tickets. ``shadow_watch`` lists thinner cells for shadow
grading only; they are directional, not a gate.

Sections
  1. graded_props_*.json alone: hit rate by (prop, direction, pick_type) and by tier, plus gates
     that use only fields already on the graded row (l5_over / l10_over / def_tier).
  2. Join to dated step8 (player + prop + line + pick_type + direction + date): def_tier,
     goalie save %, PP1 flag, TOI bucket, scoring/pp tier, back-to-back.
  3. Stage survival: step1 -> step8 rows by prop per date, with the fetched_at of each stage.
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import re
import unicodedata
from pathlib import Path

import pandas as pd

BAR_N = 100
BAR_HR = 0.745
SHADOW_N = 40
SHADOW_HR = 0.68


def wilson(h: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (0.0, 0.0)
    p = h / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (round(c - m, 3), round(c + m, 3))


def norm_name(s: object) -> str:
    s = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z]", "", s.lower())


def norm_prop(s: object) -> str:
    s = str(s or "").strip().lower().replace(" ", "_")
    return {"saves": "goalie_saves"}.get(s, s)


def cell(g: pd.DataFrame) -> dict:
    n = int(len(g))
    h = int(g["hit"].sum())
    lo, hi = wilson(h, n)
    return {
        "n": n,
        "hits": h,
        "hr": round(h / n, 3) if n else None,
        "wilson_lo": lo,
        "wilson_hi": hi,
        "player_days": int(g[["_date", "_pn"]].drop_duplicates().shape[0]),
        "dates": int(g["_date"].nunique()),
    }


def flag(c: dict) -> str:
    if c["n"] >= BAR_N and c["hr"] is not None and c["hr"] >= BAR_HR:
        return "PASS"
    if c["n"] >= SHADOW_N and c["hr"] is not None and c["hr"] >= SHADOW_HR:
        return "SHADOW_WATCH"
    return ""


def table(df: pd.DataFrame, keys: list[str], min_n: int = 1) -> list[dict]:
    out = []
    for k, g in df.groupby(keys, dropna=False):
        k = k if isinstance(k, tuple) else (k,)
        c = cell(g)
        if c["n"] < min_n:
            continue
        row = dict(zip(keys, [None if (isinstance(x, float) and math.isnan(x)) else x for x in k]))
        row.update(c)
        row["flag"] = flag(c)
        out.append(row)
    return sorted(out, key=lambda r: (-(r["hr"] or 0), -r["n"]))


def load_graded(root: Path) -> pd.DataFrame:
    rows = []
    for f in sorted(glob.glob(str(root / "ui_runner" / "templates" / "graded_props_*.json"))):
        d = json.load(open(f, encoding="utf-8"))
        date = d.get("date") or Path(f).stem[-10:]
        for r in d.get("props", []):
            if str(r.get("sport", "")).upper() == "NHL":
                r = dict(r)
                r["_date"] = str(date)
                rows.append(r)
    df = pd.DataFrame(rows)
    key = ["_date", "player", "prop", "line", "direction", "pick_type"]
    n_all = len(df)
    df = df.drop_duplicates(key)
    df.attrs["n_raw"] = n_all
    df.attrs["n_dedup_removed"] = n_all - len(df)
    return df


def prep(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["prop"] = df["prop"].map(norm_prop)
    df["pick_type"] = df["pick_type"].astype(str).str.title()
    df["direction"] = df["direction"].astype(str).str.upper()
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    for c in ("l5_over", "l5_under", "l10_over", "l10_under", "l10_games_played", "hit_rate"):
        if c in df:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    df["_pn"] = df["player"].map(norm_name)
    return df


def directional_l5(df: pd.DataFrame) -> pd.Series:
    return df["l5_over"].where(df["direction"] == "OVER", df["l5_under"])


def directional_l10(df: pd.DataFrame) -> pd.Series:
    return df["l10_over"].where(df["direction"] == "OVER", df["l10_under"])


def gate_masks(g: pd.DataFrame) -> dict[str, pd.Series]:
    l5, l10 = directional_l5(g), directional_l10(g)
    dt = g["def_tier"].astype(str)
    weak = dt.isin(["Weak", "Below Avg"])
    return {
        "ungated": pd.Series(True, index=g.index),
        "L5=5": l5 == 5,
        "L5>=4": l5 >= 4,
        "L10>=8": l10 >= 8,
        "L5=5 & L10>=8": (l5 == 5) & (l10 >= 8),
        "D (Weak|Below Avg)": weak,
        "L5=5 & D": (l5 == 5) & weak,
        "L5=5 & L10>=8 & D": (l5 == 5) & (l10 >= 8) & weak,
    }


def gate_table(df: pd.DataFrame, by_prop: bool = True) -> list[dict]:
    out = []
    groups = df.groupby("prop") if by_prop else [("ALL", df)]
    for prop, g in groups:
        for name, m in gate_masks(g).items():
            sub = g[m.fillna(False)]
            if sub.empty:
                continue
            c = cell(sub)
            out.append({"prop": prop, "gate": name, **c, "flag": flag(c)})
    return sorted(out, key=lambda r: (-(r["hr"] or 0), -r["n"]))


def load_step8(root: Path) -> tuple[pd.DataFrame, list[str]]:
    frames, dates = [], []
    for p in sorted(glob.glob(str(root / "outputs" / "20*" / "nhl" / "step8_nhl_direction_clean.xlsx"))):
        date = Path(p).parents[1].name
        try:
            d = pd.read_excel(p)
        except Exception:
            continue
        d["_date"] = date
        frames.append(d)
        dates.append(date)
    if not frames:
        return pd.DataFrame(), []
    s8 = pd.concat(frames, ignore_index=True)
    s8["prop"] = s8["prop_type"].map(norm_prop)
    s8["pick_type"] = s8["pick_type"].astype(str).str.title()
    s8["direction"] = s8["direction"].astype(str).str.upper()
    s8["line"] = pd.to_numeric(s8["line"], errors="coerce")
    s8["_pn"] = s8["player"].map(norm_name)
    return s8, dates


def bucket(s: pd.Series, bins: list[float], labels: list[str]) -> pd.Series:
    return pd.cut(pd.to_numeric(s, errors="coerce"), bins=bins, labels=labels).astype(object).fillna("unknown")


def stage_survival(root: Path) -> list[dict]:
    out = []
    for d in sorted(glob.glob(str(root / "outputs" / "20*" / "nhl"))):
        date = Path(d).parent.name
        rec: dict = {"date": date}
        try:
            s1 = pd.read_csv(Path(d) / "step1_nhl_props.csv", low_memory=False)
            rec["step1_rows"] = int(len(s1))
            rec["step1_fetched_at_max"] = str(s1["fetched_at"].max()) if "fetched_at" in s1 else None
            rec["step1_by_prop"] = s1["stat_type"].map(norm_prop).value_counts().to_dict()
        except Exception as e:  # noqa: BLE001
            rec["step1_error"] = str(e)[:80]
        try:
            s2 = pd.read_csv(Path(d) / "step2_nhl_picktypes.csv", low_memory=False)
            rec["step2_rows"] = int(len(s2))
            rec["step2_fetched_at_max"] = str(s2["fetched_at"].max()) if "fetched_at" in s2 else None
        except Exception:
            pass
        p8 = Path(d) / "step8_nhl_direction_clean.xlsx"
        if p8.exists():
            s8 = pd.read_excel(p8)
            rec["step8_rows"] = int(len(s8))
            rec["step8_by_prop"] = s8["prop_type"].map(norm_prop).value_counts().to_dict()
            rec["step8_by_pick_type"] = s8["pick_type"].astype(str).str.lower().value_counts().to_dict()
        s1p, s8p = rec.get("step1_by_prop", {}), rec.get("step8_by_prop", {})
        rec["props_in_step1_not_step8"] = {k: int(v) for k, v in s1p.items() if k not in s8p}
        rec["step1_is_newer_than_step2"] = bool(
            rec.get("step1_fetched_at_max") and rec.get("step2_fetched_at_max")
            and rec["step1_fetched_at_max"] > rec["step2_fetched_at_max"]
        )
        out.append(rec)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=r"H:\PropORACLE_main_cp", help="checkout holding graded props + outputs (read-only)")
    ap.add_argument("--out-root", default=str(Path(__file__).resolve().parents[1]))
    args = ap.parse_args()
    root, out_root = Path(args.root), Path(args.out_root)

    raw = load_graded(root)
    n_raw, n_dedup = raw.attrs["n_raw"], raw.attrs["n_dedup_removed"]
    df = prep(raw)
    graded = df[df["result"].isin(["HIT", "MISS"])].copy()
    graded["hit"] = (graded["result"] == "HIT").astype(int)
    report: dict = {
        "bar": {"n_min": BAR_N, "hr_min": BAR_HR, "shadow_n_min": SHADOW_N, "shadow_hr_min": SHADOW_HR},
        "scope": {
            "graded_props_files": len(glob.glob(str(root / "ui_runner" / "templates" / "graded_props_*.json"))),
            "nhl_rows_raw": n_raw,
            "duplicates_removed": n_dedup,
            "graded_hit_miss": int(len(graded)),
            "pushes_excluded": int((df["result"] == "PUSH").sum()),
            "date_min": graded["_date"].min(),
            "date_max": graded["_date"].max(),
            "dates": int(graded["_date"].nunique()),
            "note": "Cells share players across line ladders and days; use player_days, not n, as the independent sample.",
        },
    }

    # ---- 1. graded alone -------------------------------------------------
    report["overall"] = cell(graded)
    report["by_pick_type"] = table(graded, ["pick_type"])
    report["by_pick_type_direction"] = table(graded, ["pick_type", "direction"])
    report["by_prop_direction_pick_type"] = table(graded, ["prop", "direction", "pick_type"], min_n=20)
    report["by_pick_type_tier"] = table(graded, ["pick_type", "tier"])
    report["by_date_pick_type"] = table(graded, ["_date", "pick_type"])

    gob = graded[(graded["pick_type"] == "Goblin") & (graded["direction"] == "OVER")]
    report["goblin_over"] = {
        "overall": cell(gob),
        "cells_n_ge_100_hr_ge_745": [r for r in table(gob, ["prop"]) if r["flag"] == "PASS"],
        "by_prop": table(gob, ["prop"]),
        "by_prop_line": table(gob, ["prop", "line"], min_n=30),
        "by_tier": table(gob, ["tier"]),
        "l5_l10_coverage": {
            "rows": int(len(gob)),
            "l5_present": int(directional_l5(gob).notna().sum()),
            "l10_present": int(directional_l10(gob).notna().sum()),
            "def_tier_present": int((gob["def_tier"].astype(str).str.len() > 0).sum()),
        },
        "gates_by_prop": gate_table(gob),
        "gates_all_props": gate_table(gob, by_prop=False),
    }
    std = graded[graded["pick_type"] == "Standard"]
    report["standard"] = {
        "by_prop_direction": table(std, ["prop", "direction"], min_n=30),
        "gates_by_prop_direction": [
            {"direction": d, **r} for d, sub in std.groupby("direction") for r in gate_table(sub)
        ],
        "note": "Standard has no NHL cell in the book; shown for comparison only.",
    }
    dem = graded[graded["pick_type"] == "Demon"]
    report["demon_share"] = {
        "rows": int(len(dem)), "share_of_graded": round(len(dem) / len(graded), 3), "hr": cell(dem)["hr"],
        "note": "Demons are never picked; they dominate the pooled hit rate.",
    }

    # ---- 2. join to step8 -------------------------------------------------
    s8, s8_dates = load_step8(root)
    j: dict = {"step8_dates_on_disk": s8_dates}
    if not s8.empty:
        keep = ["_date", "_pn", "prop", "line", "pick_type", "direction", "def_tier", "def_rank", "goalie_sv_pct",
                "on_pp1_line", "toi_avg_L10", "scoring_tier", "pp_tier", "back_to_back", "rest_days", "is_home",
                "opp_gaa", "opp_shots_allowed_avg", "implied_team_total", "game_total", "hr_L5", "hr_L10",
                "pp_pts_per_game", "player_avg_shots_L10", "over_L5_raw", "over_L10_raw", "sample_L5", "sample_L10"]
        s8k = s8[[c for c in keep if c in s8.columns]].rename(columns={"def_tier": "def_tier_s8"})
        s8k = s8k.drop_duplicates(["_date", "_pn", "prop", "line", "pick_type", "direction"])
        gj = graded[graded["_date"].isin(s8_dates)].merge(
            s8k, on=["_date", "_pn", "prop", "line", "pick_type", "direction"], how="left", indicator=True)
        gj["joined"] = gj["_merge"] == "both"
        j["join"] = {
            "graded_rows_on_step8_dates": int(len(gj)),
            "joined": int(gj["joined"].sum()),
            "join_rate": round(float(gj["joined"].mean()), 3),
            "goblin_over_join_rate": round(float(gj[(gj.pick_type == "Goblin") & (gj.direction == "OVER")]["joined"].mean()), 3),
            "caveat": "step8 is the board at the pipeline run time; lines can move before grading, and "
                      "pp_projection_id is only exported going forward, so old dates join on player+prop+line.",
        }
        gg = gj[gj["joined"] & (gj.pick_type == "Goblin") & (gj.direction == "OVER")].copy()
        gg["goalie_sv_bucket"] = bucket(gg.get("goalie_sv_pct"), [0, .89, .905, .915, 1.1], ["<.890", ".890-.905", ".905-.915", ">=.915"])
        gg["toi_bucket"] = bucket(gg.get("toi_avg_L10"), [0, 14, 17, 20, 99], ["<14", "14-17", "17-20", "20+"])
        gg["pp1"] = gg.get("on_pp1_line").astype(str).str.lower().map(
            lambda v: "PP1" if v in ("1", "1.0", "true", "yes") else "not PP1")
        gg["def_tier_s8"] = gg["def_tier_s8"].fillna("unknown")
        gg["b2b"] = gg.get("back_to_back").astype(str).str.lower().map(
            lambda v: "B2B" if v in ("1", "1.0", "true", "yes") else "rested")
        j["goblin_over_joined"] = {"overall": cell(gg)}
        # Graded rows carry no NHL l5/l10, so the L5/L10 gates come from step8 (over_L5_raw /
        # over_L10_raw are the over counts at this line, valid only on a full 5 / 10 game window).
        gg["_l5"] = pd.to_numeric(gg.get("over_L5_raw"), errors="coerce").where(
            pd.to_numeric(gg.get("sample_L5"), errors="coerce") == 5)
        gg["_l10"] = pd.to_numeric(gg.get("over_L10_raw"), errors="coerce").where(
            pd.to_numeric(gg.get("sample_L10"), errors="coerce") == 10)
        gg["l5_over"], gg["l10_over"], gg["l5_under"], gg["l10_under"] = gg["_l5"], gg["_l10"], None, None
        gg["def_tier"] = gg["def_tier_s8"]
        j["goblin_over_joined"]["coverage"] = {
            "rows": int(len(gg)),
            "full_L5_window": int(gg["_l5"].notna().sum()),
            "full_L10_window": int(gg["_l10"].notna().sum()),
            "goalie_sv_pct_present": int(pd.to_numeric(gg.get("goalie_sv_pct"), errors="coerce").notna().sum()),
            "toi_avg_L10_present": int(pd.to_numeric(gg.get("toi_avg_L10"), errors="coerce").notna().sum()),
            "on_pp1_line_true": int((gg["pp1"] == "PP1").sum()),
        }
        j["goblin_over_joined"]["l5_l10_gates_by_prop"] = gate_table(gg)
        j["goblin_over_joined"]["l5_l10_gates_all_props"] = gate_table(gg, by_prop=False)
        # Hollow NHL rows (shots 0, no TOI) fed step4/5 until 2026-10-21, so split before/after the fix.
        j["goblin_over_joined"]["l5_l10_gates_by_period"] = {
            "through_2026-10-20 (hollow-row era)": gate_table(gg[gg["_date"] <= "2026-10-20"], by_prop=False),
            "from_2026-10-21 (fixed)": gate_table(gg[gg["_date"] >= "2026-10-21"], by_prop=False),
        }
        for col in ("def_tier_s8", "goalie_sv_bucket", "pp1", "toi_bucket", "scoring_tier", "pp_tier", "b2b", "is_home"):
            if col in gg:
                j["goblin_over_joined"][f"by_{col}"] = table(gg, [col])
                j["goblin_over_joined"][f"by_prop_x_{col}"] = table(gg, ["prop", col], min_n=30)
    report["step8_join"] = j

    # ---- 3. filters between step1 and step8 -------------------------------
    report["stage_survival"] = stage_survival(root)

    # ---- verdict ----------------------------------------------------------
    passing = [r for r in report["goblin_over"]["gates_by_prop"] + report["goblin_over"]["gates_all_props"] if r["flag"] == "PASS"]
    watch = [r for r in report["goblin_over"]["gates_by_prop"] if r["flag"] == "SHADOW_WATCH"]
    jj = report["step8_join"].get("goblin_over_joined", {})
    watch += [{"source": "l5_l10_gates_by_prop", **r} for r in jj.get("l5_l10_gates_by_prop", []) if r["flag"] == "SHADOW_WATCH"]
    passing += [{"source": "l5_l10_gates_by_prop", **r} for r in jj.get("l5_l10_gates_by_prop", []) if r["flag"] == "PASS"]
    passing += [{"source": "l5_l10_gates_all_props", **r} for r in jj.get("l5_l10_gates_all_props", []) if r["flag"] == "PASS"]
    for key in ("by_prop_x_def_tier_s8", "by_prop_x_goalie_sv_bucket", "by_prop_x_pp1", "by_prop_x_toi_bucket"):
        watch += [{"source": key, **r} for r in jj.get(key, []) if r["flag"] == "SHADOW_WATCH"]
        passing += [{"source": key, **r} for r in jj.get(key, []) if r["flag"] == "PASS"]
    report["verdict"] = {
        "goblin_over_cells_clearing_bar": passing,
        "shadow_watch": sorted(watch, key=lambda r: (-r["hr"], -r["n"]))[:25],
        "decision": "NO_NHL_GATE" if not passing else "REVIEW_CELLS",
    }

    out = out_root / "data" / "reports" / "nhl_goblin_gate_backtest.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"wrote {out} ({out.stat().st_size // 1024} KB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
