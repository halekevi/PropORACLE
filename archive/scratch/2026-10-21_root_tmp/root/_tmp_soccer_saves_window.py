"""Soccer keep-prop listed HR 2026-09-01..20, Saves vs Shots/SOT, Sunday split."""
from __future__ import annotations

import json
import sys
import unicodedata
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from utils.prop_norm import canon_prop  # noqa: E402
from utils.soccer_keep_gates import soccer_keep_eligible  # noqa: E402

OUT = ROOT / "_tmp_soccer_saves_window.json"
COLS = [
    "player",
    "prop_type",
    "pick_type",
    "line",
    "start_time",
    "game_date",
    "league",
    "last5_over",
    "last5_under",
    "l5_over",
    "l5_under",
    "l10_over",
    "l10_under",
    "line_hits_over_10",
    "line_hits_under_10",
    "starter_tier",
    "shot_volume",
    "team_top3_rank",
    "OWN_DEF_TIER",
    "own_def_tier",
    "DEF_TIER",
    "def_tier",
    "OPP_OFF_TIER",
    "opp_off_tier",
    "model_dir",
    "final_bet_direction",
    "bet_direction",
]


def pname(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.casefold().replace(".", " ").replace("'", "").replace("’", "")
    return " ".join(t.split())


def pflt(v):
    try:
        if v in (None, "", "nan"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def on_date(start_time, game_date, day: str) -> bool:
    if start_time not in (None, "", "nan"):
        ts = pd.to_datetime(start_time, errors="coerce", utc=True)
        if pd.notna(ts):
            return ts.tz_convert("America/New_York").strftime("%Y-%m-%d") == day
    gd = str(game_date or "")[:10]
    return gd == day


def grade(actual, line, side):
    if actual is None or line is None:
        return None
    if abs(actual - line) < 1e-9:
        return "PUSH"
    hit = actual > line if side == "OVER" else actual < line
    return "HIT" if hit else "MISS"


def load_actuals(day: str) -> dict:
    p = MAIN / "outputs" / day / f"actuals_soccer_{day}.csv"
    if not p.is_file():
        return {}
    df = pd.read_csv(p, usecols=lambda c: c in {"player", "prop_type", "actual"})
    out = {}
    for _, r in df.iterrows():
        act = pflt(r.get("actual"))
        if act is None:
            continue
        out[(pname(r.get("player")), canon_prop("Soccer", str(r.get("prop_type") or "")))] = act
    return out


def step8_path(day: str) -> Path | None:
    for p in (
        MAIN / "outputs" / day / "soccer" / "step8_soccer_direction.csv",
        MAIN / "outputs" / day / "soccer" / "step8_soccer_direction_clean.xlsx",
    ):
        if p.is_file():
            return p
    return None


def load_step8(day: str) -> pd.DataFrame:
    p = step8_path(day)
    if p is None:
        return pd.DataFrame()
    if p.suffix.lower() == ".csv":
        hdr = pd.read_csv(p, nrows=0)
        use = [c for c in COLS if c in hdr.columns]
        return pd.read_csv(p, usecols=use, low_memory=False)
    return pd.read_excel(p)


def tally(rows: list[dict]) -> dict:
    hit = sum(1 for x in rows if x["grade"] == "HIT")
    miss = sum(1 for x in rows if x["grade"] == "MISS")
    decided = hit + miss
    return {
        "n": len(rows),
        "hit": hit,
        "miss": miss,
        "open": sum(1 for x in rows if x["grade"] == "OPEN"),
        "decided": decided,
        "rate": round(100.0 * hit / decided, 1) if decided else None,
        "rate_s": f"{100.0 * hit / decided:.1f}% ({hit}/{decided})" if decided else "—",
    }


def main() -> None:
    start = date(2026, 9, 1)
    end = date(2026, 9, 20)
    by_day = []
    buckets = defaultdict(list)
    all_listed = []
    d = start
    while d <= end:
        day = d.isoformat()
        df = load_step8(day)
        acts = load_actuals(day)
        listed = []
        if df is not None and not df.empty:
            seen = set()
            for _, r in df.iterrows():
                rec = {k: r.get(k) for k in df.columns}
                rec["sport"] = "SOCCER"
                rec["prop"] = rec.get("prop_type")
                rec["side"] = str(
                    rec.get("final_bet_direction")
                    or rec.get("bet_direction")
                    or rec.get("model_dir")
                    or ""
                ).upper()
                if not soccer_keep_eligible(rec):
                    continue
                if not on_date(rec.get("start_time"), rec.get("game_date"), day):
                    continue
                prop = canon_prop("Soccer", str(rec.get("prop") or ""))
                line = pflt(rec.get("line"))
                pt = str(rec.get("pick_type") or "").title()
                side = rec["side"]
                player = str(rec.get("player") or "")
                k = (pname(player), prop, line, pt, side)
                if k in seen:
                    continue
                seen.add(k)
                g = grade(acts.get((pname(player), prop)), line, side) or "OPEN"
                row = {
                    "date": day,
                    "dow": d.strftime("%a"),
                    "player": player,
                    "prop": prop,
                    "side": side,
                    "pick_type": pt,
                    "line": line,
                    "grade": g,
                    "sunday": d.weekday() == 6,
                }
                listed.append(row)
                all_listed.append(row)
        by_day.append({"date": day, "dow": d.strftime("%a"), "sunday": d.weekday() == 6, **tally(listed)})
        for row in listed:
            buckets[(row["prop"], row["side"], "Sun" if row["sunday"] else "other")].append(row)
            buckets[(row["prop"], row["side"], "all")].append(row)
            buckets[(row["prop"], "ALL", "Sun" if row["sunday"] else "other")].append(row)
            buckets[(row["prop"], "ALL", "all")].append(row)
        d += timedelta(days=1)

    cells = {
        f"{prop}|{side}|{win}": tally(rows)
        for (prop, side, win), rows in sorted(buckets.items())
    }
    sundays = [x for x in by_day if x["sunday"]]
    payload = {
        "by_day": by_day,
        "sundays": sundays,
        "cells": cells,
        "catalog_note": "Goblin OVER Saves L5>=4 catalog 80.8% n=26 (keep-prop-gates-lift)",
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("SUNDAYS")
    for x in sundays:
        print(day_line(x) if False else x)
    print("CELLS")
    for k, v in cells.items():
        if v["decided"]:
            print(k, v["rate_s"])
    print("wrote", OUT)


if __name__ == "__main__":
    main()
