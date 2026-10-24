"""Hit rates by sport × book × prop category. Find prop-specific gates/fades."""
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

GRADED = MAIN / "ui_runner" / "templates"
GOBLIN_FLOOR = {"WNBA": 2.0, "MLB": 1.0, "Tennis": 2.0, "Soccer": 1.0}
BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER")
MIN_N = 20


def rate(h, n):
    return None if n <= 0 else round(100.0 * h / n, 1)


def overall(df):
    n = int(len(df))
    h = int(df["hit"].sum()) if n else 0
    return {"hits": h, "n": n, "rate": rate(h, n)}


def canon_prop(sport: str, prop: str) -> str:
    p = re.sub(r"\s+", " ", str(prop or "").strip().lower())
    p = p.replace("points + rebounds + assists", "pra")
    p = p.replace("pts+rebs+asts", "pra").replace("pts + rebs + asts", "pra")
    p = p.replace("points + rebounds", "pts+reb").replace("pts+rebs", "pts+reb")
    p = p.replace("points + assists", "pts+ast").replace("pts+asts", "pts+ast")
    p = p.replace("rebounds + assists", "reb+ast").replace("rebs+asts", "reb+ast")
    p = p.replace("blocked shots", "blocks")
    aliases = {
        "pts": "points",
        "reb": "rebounds",
        "ast": "assists",
        "fg made": "fgm",
        "3-pt made": "threes",
        "3pt made": "threes",
        "three pointers made": "threes",
        "shots on goal": "sog",
        "shot on target": "sog",
        "goalie saves": "saves",
        "goalkeeper saves": "saves",
        "pitcher strikeouts": "pitcher_ks",
        "hitter strikeouts": "hitter_ks",
        "hitter strikeout": "hitter_ks",
        "strikeouts": "strikeouts",
        "total bases": "total_bases",
        "hits+runs+rbi": "h+r+rbi",
        "hits + runs + rbis": "h+r+rbi",
        "runs + rbis": "r+rbi",
        "total games won": "games_won",
        "games won": "games_won",
        "aces": "aces",
        "double faults": "double_faults",
        "1st set games won": "set1_games",
        "fantasy score": "fantasy",
    }
    return aliases.get(p, p)


def mlb_ks_family(prop: str) -> str:
    p = prop.lower()
    if "hitter" in p and "strike" in p:
        return "hitter_ks"
    if "pitcher" in p and "strike" in p:
        return "pitcher_ks"
    if p in ("strikeouts", "ks", "k's"):
        return "strikeouts"  # board-dependent
    return prop


def build_merged() -> pd.DataFrame:
    dates = sorted(p.name[len("graded_props_") : -5] for p in GRADED.glob("graded_props_20*.json"))
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
            elif "demon" in pk:
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
    under = merged["direction"].eq("UNDER")
    for src, dest in (("l5", "g_l5"), ("szn", "g_szn"), ("l10", "g_l10")):
        avg = pd.to_numeric(merged[src], errors="coerce")
        gap = pd.Series(np.nan, index=merged.index)
        gap = gap.mask(over, avg - line)
        gap = gap.mask(under, line - avg)
        merged[dest] = gap
    merged["min_gap"] = merged[["g_l5", "g_l10", "g_szn"]].min(axis=1, skipna=True)
    merged["book"] = merged["pick"] + " " + merged["direction"]
    l5_dir = np.where(over, merged["l5_over"], merged["l5_under"])
    merged["l5_hits"] = pd.to_numeric(pd.Series(l5_dir, index=merged.index), errors="coerce")
    merged["list_l5"] = merged["l5_hits"] >= 4
    merged["prop"] = [canon_prop(s, p) for s, p in zip(merged["sport"], merged["prop_n"])]
    floor = merged["sport"].map(GOBLIN_FLOOR)
    merged["cover_ok"] = merged["min_gap"] >= floor
    return merged


def main():
    merged = build_merged()

    recs = []
    for (sport, book, prop), g in merged.groupby(["sport", "book", "prop"]):
        if book not in BOOKS:
            continue
        listed = g[g["list_l5"]]
        need = GOBLIN_FLOOR.get(sport, 1.0)
        floored = listed[pd.to_numeric(listed["min_gap"], errors="coerce") >= need] if book == "Goblin OVER" else listed
        against = listed[pd.to_numeric(listed["min_gap"], errors="coerce") < 0]
        big = listed[pd.to_numeric(listed["min_gap"], errors="coerce") >= need]
        recs.append(
            {
                "sport": sport,
                "book": book,
                "prop": prop,
                "all": overall(g),
                "listed": overall(listed),
                "cover_floor": overall(floored) if book == "Goblin OVER" else None,
                "listed_against": overall(against),
                "listed_cover_ok": overall(big),
                "mean_line": None
                if pd.to_numeric(g["line"], errors="coerce").notna().sum() == 0
                else round(float(pd.to_numeric(g["line"], errors="coerce").mean()), 2),
                "mean_l5_gap": None
                if pd.to_numeric(g["g_l5"], errors="coerce").notna().sum() == 0
                else round(float(pd.to_numeric(g["g_l5"], errors="coerce").mean()), 2),
            }
        )
    recs.sort(key=lambda r: (r["sport"], r["book"], -(r["listed"]["n"] or 0)))

    findings = []
    for r in recs:
        n = r["listed"]["n"]
        if n < MIN_N:
            continue
        hr = r["listed"]["rate"]
        all_hr = r["all"]["rate"]
        sport, book, prop = r["sport"], r["book"], r["prop"]
        note = None
        if book == "Goblin OVER" and r["cover_floor"] and r["cover_floor"]["n"] >= 15:
            cf = r["cover_floor"]["rate"]
            if cf is not None and hr is not None and cf - hr >= 3:
                note = f"cover floor lifts {hr} -> {cf} (n={r['cover_floor']['n']})"
            if cf is not None and hr is not None and hr - cf >= 3 and r["cover_floor"]["n"] >= 20:
                note = f"cover floor HURTS {hr} -> {cf}"
        if hr is not None and hr <= 45 and n >= 30:
            findings.append({"kind": "fade_prop", "sport": sport, "book": book, "prop": prop, "listed": r["listed"], "note": "listed HR <=45%"})
        if hr is not None and hr >= 70 and n >= 25:
            findings.append({"kind": "keep_hot", "sport": sport, "book": book, "prop": prop, "listed": r["listed"], "note": note})
        if all_hr is not None and hr is not None and (hr - all_hr) < 1 and n >= 40:
            findings.append({"kind": "l5_no_lift", "sport": sport, "book": book, "prop": prop, "all": r["all"], "listed": r["listed"]})
        if book == "Goblin OVER" and r["cover_floor"] and r["cover_floor"]["n"] >= 20:
            cf = r["cover_floor"]["rate"]
            if cf is not None and hr is not None and cf - hr >= 3:
                findings.append({"kind": "cover_lifts", "sport": sport, "book": book, "prop": prop, "listed": r["listed"], "cover_floor": r["cover_floor"]})
        if book == "Standard UNDER" and r["listed_against"]["n"] >= 15:
            findings.append({"kind": "under_against", "sport": sport, "book": book, "prop": prop, "against": r["listed_against"], "listed": r["listed"]})

    # compact tables per sport/book with n>=MIN_N listed or n>=40 all
    by_sport = {}
    for sport in B.SPORTS:
        by_sport[sport] = {}
        for book in BOOKS:
            rows_b = [
                {
                    "prop": r["prop"],
                    "all": r["all"],
                    "listed": r["listed"],
                    "cover_floor": r["cover_floor"],
                    "listed_against": r["listed_against"],
                    "mean_line": r["mean_line"],
                    "mean_l5_gap": r["mean_l5_gap"],
                }
                for r in recs
                if r["sport"] == sport and r["book"] == book and (r["listed"]["n"] >= MIN_N or r["all"]["n"] >= 40)
            ]
            by_sport[sport][book] = rows_b

    out = {
        "window": {"first": str(merged["date"].min()), "last": str(merged["date"].max()), "n": int(len(merged))},
        "min_n_listed": MIN_N,
        "by_sport": by_sport,
        "findings": findings,
    }
    dest = OPEN / "data" / "reports" / "prop_category_gates.json"
    dest.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps({"findings": findings, "by_sport": by_sport}, indent=2)[:18000])
    print("wrote", dest)


if __name__ == "__main__":
    main()
