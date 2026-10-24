"""L10 >=8 vs >=9 (and =8 / =9 / =10) on the step8 × graded join.

Uses real directional L10 Over/Under vs today's line — not retrain hit_rate_l10
(that column is a 0/1 flag).

  py -3.14 scripts/_l10_nine_vs_eight.py
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
sys.path.insert(0, str(OPEN))
import _avg_gap_by_badge as B  # noqa: E402
import prop_hit_tiers as T  # noqa: E402

GRADED = MAIN / "ui_runner" / "templates"
CACHE = OPEN / "data" / "reports" / "_l10_gate_merged.parquet"
OUT = OPEN / "data" / "reports" / "l10_nine_vs_eight.json"
BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER")
SPORTS = B.SPORTS


def rate(h, n):
    return None if n <= 0 else round(100.0 * h / n, 1)


def cell(mask, hit):
    n = int(mask.sum())
    h = int(hit[mask].sum()) if n else 0
    return {"hits": h, "n": n, "hr": rate(h, n)}


def find_step8(date: str, sport: str) -> Path | None:
    p = B.find_step8(date, sport)
    if p:
        return p
    folder = B.SPORT_FOLDER[sport]
    slug = folder
    for root in (B.MAIN, B.OPEN):
        d = root / "outputs" / date
        for cand in (
            d / folder / f"step8_{slug}_direction.csv",
            d / f"step8_{slug}_direction.csv",
            d / folder / f"step8_{slug}_direction_clean.csv",
        ):
            if cand.exists():
                return cand
    return None


def load_step8(path: Path, sport: str, date: str) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, low_memory=False, encoding="utf-8-sig")
    else:
        df = pd.read_excel(path)
    c_player = B._col(df, "Player", "player", "player_name")
    c_prop = B._col(df, "Prop", "prop", "prop_type")
    c_line = B._col(df, "Line", "line", "line_score")
    c_dir = B._col(df, "Direction", "direction", "bet_direction")
    if not all([c_player, c_prop, c_line, c_dir]):
        return pd.DataFrame()
    line = pd.to_numeric(df[c_line], errors="coerce")
    direction = B._norm_dir_s(df[c_dir])
    gcols = []
    for i in range(1, 11):
        c = B._col(df, f"G{i}", f"g{i}", f"stat_g{i}", f"stat_G{i}")
        if c:
            gcols.append(c)
    l10_g = pd.Series(np.nan, index=df.index)
    if gcols:
        gmat = df[gcols].apply(pd.to_numeric, errors="coerce")
        over = direction.eq("OVER")
        ho = gmat.gt(line, axis=0).sum(axis=1)
        hu = gmat.lt(line, axis=0).sum(axis=1)
        nn = gmat.notna().sum(axis=1)
        l10_g = pd.Series(np.where(over, ho, hu), index=df.index, dtype=float)
        l10_g = l10_g.where(nn >= 8, np.nan)
    l10_over = B._series(df, B._col(df, "L10 Over", "l10_over", "last10_over"))
    l10_under = B._series(df, B._col(df, "L10 Under", "l10_under", "last10_under"))
    l5_over = B._series(df, B._col(df, "L5 Over", "l5_over", "last5_over"))
    l5_under = B._series(df, B._col(df, "L5 Under", "l5_under", "last5_under"))
    return pd.DataFrame(
        {
            "date": date,
            "sport": sport,
            "player_n": B._norm_name_s(df[c_player]),
            "prop_n": B._norm_prop_s(df[c_prop]),
            "prop_raw": df[c_prop].astype(str),
            "line_r": line.round(2),
            "direction": direction,
            "l5_over": l5_over,
            "l5_under": l5_under,
            "l10_over": l10_over,
            "l10_under": l10_under,
            "l10_hits_g": l10_g,
        }
    ).drop_duplicates(["player_n", "prop_n", "line_r", "direction"], keep="last")


def build_merged() -> pd.DataFrame:
    dates = sorted(p.name[len("graded_props_") : -5] for p in GRADED.glob("graded_props_20*.json"))
    frames = []
    for date in dates:
        for sport in SPORTS:
            p = find_step8(date, sport)
            if not p:
                continue
            print(f"load {date} {sport} {p.name}", flush=True)
            frames.append(load_step8(p, sport, date))
    if not frames:
        raise SystemExit("no step8 boards")
    step8 = pd.concat(frames, ignore_index=True)
    rows = []
    for date in dates:
        gp = GRADED / f"graded_props_{date}.json"
        if not gp.is_file():
            continue
        raw = json.loads(gp.read_text(encoding="utf-8"))
        for g in raw.get("props", []) or []:
            sport = str(g.get("sport") or "")
            if sport not in SPORTS:
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
    over = merged["direction"].eq("OVER")
    l5_col = np.where(over, merged["l5_over"], merged["l5_under"])
    l10_col = np.where(over, merged["l10_over"], merged["l10_under"])
    merged["l5_hits"] = pd.to_numeric(pd.Series(l5_col, index=merged.index), errors="coerce")
    merged["l10_hits"] = pd.to_numeric(pd.Series(l10_col, index=merged.index), errors="coerce")
    merged["l10_hits"] = merged["l10_hits"].fillna(pd.to_numeric(merged["l10_hits_g"], errors="coerce"))
    merged["book"] = merged["pick"] + " " + merged["direction"]
    merged = merged[merged["book"].isin(BOOKS)].copy()
    merged["prop"] = [T.canon_prop(s, p) for s, p in zip(merged["sport"], merged["prop_n"])]
    return merged


def gates(sub: pd.DataFrame) -> dict:
    hit = sub["hit"].astype(int)
    l5 = pd.to_numeric(sub["l5_hits"], errors="coerce")
    l10 = pd.to_numeric(sub["l10_hits"], errors="coerce")
    listed = l5.ge(4)
    specs = {
        "open": pd.Series(True, index=sub.index),
        "L5>=4": listed,
        "L10=8": l10.eq(8),
        "L10>=8": l10.ge(8),
        "L10>=9": l10.ge(9),
        "L10=9": l10.eq(9),
        "L10=10": l10.eq(10),
        "L5>=4 + L10>=8": listed & l10.ge(8),
        "L5>=4 + L10>=9": listed & l10.ge(9),
        "L5>=4 + L10=8": listed & l10.eq(8),
        "L5=5 + L10>=9": l5.eq(5) & l10.ge(9),
    }
    out = {}
    for name, mask in specs.items():
        out[name] = cell(mask.fillna(False), hit)
    known = int(l10.notna().sum())
    out["l10_fill_n"] = known
    out["l10_fill_pct"] = round(100.0 * known / max(len(sub), 1), 1)
    return out


def main() -> None:
    if CACHE.is_file():
        print(f"cache {CACHE}", flush=True)
        merged = pd.read_parquet(CACHE)
    else:
        merged = build_merged()
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(CACHE, index=False)
        print(f"wrote cache {CACHE} n={len(merged)}", flush=True)

    payload = {
        "n": int(len(merged)),
        "from": str(merged["date"].min()),
        "to": str(merged["date"].max()),
        "note": (
            "Directional L10 hits vs today's line (step8 L10 Over/Under, "
            "filled from G1–G10 when the count is missing). ≥8 is the Diamond L10 gate; "
            "this file adds ≥9 / =8 / =9 / =10."
        ),
        "by_book": {},
        "by_sport": [],
        "by_prop": [],
    }
    print(f"joined {payload['n']} {payload['from']} -> {payload['to']}", flush=True)

    dist_rows = []
    for book in BOOKS:
        sub = merged[merged["book"] == book]
        g = gates(sub)
        payload["by_book"][book] = g
        print(f"\n{book} open {g['open']['hr']}% n={g['open']['n']}  L10 fill {g['l10_fill_pct']}%")
        for k in ("L10=8", "L10>=8", "L10>=9", "L10=9", "L10=10", "L5>=4", "L5>=4 + L10>=8", "L5>=4 + L10>=9"):
            c = g[k]
            print(f"  {k:22} {c['hr']}% n={c['n']}")
        l10 = pd.to_numeric(sub["l10_hits"], errors="coerce")
        hit = sub["hit"].astype(int)
        for k in range(0, 11):
            dist_rows.append({"book": book, "sport": "ALL", "l10": k, **cell(l10.eq(k), hit)})

    for book in BOOKS:
        for sport in SPORTS:
            sub = merged[(merged["book"] == book) & (merged["sport"] == sport)]
            if len(sub) < 40:
                continue
            rec = {"book": book, "sport": sport, **gates(sub)}
            payload["by_sport"].append(rec)
            g8 = rec["L10>=8"]
            g9 = rec["L10>=9"]
            print(
                f"{sport:7} {book:16}  >=8 {g8['hr']}% n={g8['n']}   >=9 {g9['hr']}% n={g9['n']}"
            )

    for book in BOOKS:
        for sport in SPORTS:
            props = merged.loc[(merged["book"] == book) & (merged["sport"] == sport), "prop"].dropna().unique()
            for prop in sorted(props):
                sub = merged[
                    (merged["book"] == book) & (merged["sport"] == sport) & (merged["prop"] == prop)
                ]
                if len(sub) < 40:
                    continue
                g = gates(sub)
                if (g["L10>=8"]["n"] or 0) < 25 and (g["L10>=9"]["n"] or 0) < 25:
                    continue
                payload["by_prop"].append({"book": book, "sport": sport, "prop": prop, **g})

    payload["l10_dist"] = dist_rows
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"\nwrote {OUT}", flush=True)


if __name__ == "__main__":
    main()
