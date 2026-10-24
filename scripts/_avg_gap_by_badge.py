"""Hit rates by sport × badge × avg-vs-line gap.

Joins graded HIT/MISS to dated step8 boards, scores Diamond→Bronze with
rank_best_props_today, and buckets cover with sport-specific gap sizes
(MLB/Soccer lines are small; WNBA/Tennis can run larger).
"""
from __future__ import annotations

import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

OPEN = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
if str(OPEN / "scripts") not in sys.path:
    sys.path.insert(0, str(OPEN / "scripts"))
if str(OPEN) not in sys.path:
    sys.path.insert(0, str(OPEN))
import rank_best_props_today as R  # noqa: E402

GRADED = MAIN / "ui_runner" / "templates"
SPORTS = ("WNBA", "MLB", "Soccer", "Tennis")
SPORT_FOLDER = {"WNBA": "wnba", "MLB": "mlb", "Soccer": "soccer", "Tennis": "tennis"}
SPORT_U = {"WNBA": "WNBA", "MLB": "MLB", "Soccer": "SOCCER", "Tennis": "TENNIS"}
BADGES = ("Diamond", "Platinum", "Gold", "Silver", "Bronze")

WNBA_GAPS = [
    ("against", lambda g: g < -1e-9),
    ("0–1", lambda g: (g >= 0) & (g < 1)),
    ("1–2", lambda g: (g >= 1) & (g < 2)),
    ("2–3", lambda g: (g >= 2) & (g < 3)),
    ("3–5", lambda g: (g >= 3) & (g < 5)),
    ("5+", lambda g: g >= 5),
]
SMALL_GAPS = [
    ("against", lambda g: g < -1e-9),
    ("0–0.5", lambda g: (g >= 0) & (g < 0.5)),
    ("0.5–1", lambda g: (g >= 0.5) & (g < 1)),
    ("1–1.5", lambda g: (g >= 1) & (g < 1.5)),
    ("1.5–2", lambda g: (g >= 1.5) & (g < 2)),
    ("2+", lambda g: g >= 2),
]
TENNIS_GAPS = WNBA_GAPS
SPORT_GAPS = {
    "WNBA": WNBA_GAPS,
    "MLB": SMALL_GAPS,
    "Soccer": SMALL_GAPS,
    "Tennis": TENNIS_GAPS,
}


def _col(df, *names):
    lower = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def _norm_name_s(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip().str.lower()
    x = x.str.replace(r"[^a-z0-9 ]+", " ", regex=True)
    return x.str.replace(r"\s+", " ", regex=True).str.strip()


def _norm_prop_s(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip().str.lower().str.replace(r"\s+", " ", regex=True)


def _norm_dir_s(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip().str.upper()
    return x.replace({"O": "OVER", "U": "UNDER"})


def _same(a: pd.Series, b: pd.Series) -> pd.Series:
    return a.notna() & b.notna() & ((a - b).abs() < 1e-6)


def find_step8(date: str, sport: str) -> Path | None:
    folder = SPORT_FOLDER[sport]
    slug = folder
    for root in (MAIN, OPEN):
        d = root / "outputs" / date
        for p in (
            d / folder / f"step8_{slug}_direction_clean.xlsx",
            d / folder / f"step8_{slug}_direction_clean_{date}.xlsx",
            d / f"step8_{slug}_direction_clean_{date}.xlsx",
            d / f"step8_{slug}_direction_clean.xlsx",
        ):
            if p.exists():
                return p
    return None


def _series(df, col, numeric=True):
    if col is None:
        return pd.Series(np.nan if numeric else "", index=df.index)
    if numeric:
        return pd.to_numeric(df[col], errors="coerce")
    return df[col]


def load_step8(path: Path, sport: str, date: str) -> pd.DataFrame:
    df = pd.read_excel(path)
    c_player = _col(df, "Player", "player", "player_name")
    c_prop = _col(df, "Prop", "prop", "prop_type")
    c_line = _col(df, "Line", "line", "line_score")
    c_dir = _col(df, "Direction", "direction", "bet_direction")
    c_l5 = _col(df, "Last 5 Avg", "last_5_avg", "stat_last5_avg", "l5_avg")
    c_l10 = _col(df, "Last 10 Avg", "last_10_avg", "stat_last10_avg", "l10_avg")
    c_szn = _col(df, "Season Avg", "season_avg", "stat_season_avg")
    if not all([c_player, c_prop, c_line, c_dir]):
        return pd.DataFrame()

    gcols = []
    for i in range(1, 11):
        c = _col(df, f"G{i}", f"g{i}", f"stat_g{i}", f"stat_G{i}")
        if c:
            gcols.append(c)
    gmat = df[gcols].apply(pd.to_numeric, errors="coerce") if gcols else pd.DataFrame(index=df.index)
    g_l5 = gmat.iloc[:, :5].mean(axis=1) if gmat.shape[1] else pd.Series(np.nan, index=df.index)
    g_l10 = gmat.mean(axis=1) if gmat.shape[1] else pd.Series(np.nan, index=df.index)
    l5 = _series(df, c_l5).fillna(g_l5)
    l10 = _series(df, c_l10).fillna(g_l10)
    szn = _series(df, c_szn)
    l10 = pd.Series(np.where(_same(l10, l5) & g_l10.notna() & ~_same(g_l10, l5), g_l10, l10), index=df.index)
    l10 = pd.Series(np.where(_same(l10, szn) & g_l10.notna() & ~_same(g_l10, szn), g_l10, l10), index=df.index)
    l5 = pd.Series(np.where(_same(l5, szn) & g_l5.notna() & ~_same(g_l5, szn), g_l5, l5), index=df.index)

    line = pd.to_numeric(df[c_line], errors="coerce")
    def_rank = _series(df, _col(df, "Def Rank", "def_rank", "stat_def_rank", "opponent_rank"))
    n_teams = None
    if sport != "Tennis" and def_rank.notna().any():
        mx = float(def_rank.max())
        if mx >= 5:
            n_teams = int(mx)

    out = pd.DataFrame(
        {
            "date": date,
            "sport": sport,
            "player_n": _norm_name_s(df[c_player]),
            "prop_n": _norm_prop_s(df[c_prop]),
            "prop_raw": df[c_prop].astype(str),
            "line_r": line.round(2),
            "direction": _norm_dir_s(df[c_dir]),
            "l5": l5,
            "l10": l10,
            "szn": szn,
            "l5_over": _series(df, _col(df, "L5 Over", "l5_over", "last5_over")),
            "l5_under": _series(df, _col(df, "L5 Under", "l5_under", "last5_under")),
            "l10_over": _series(df, _col(df, "L10 Over", "l10_over")),
            "l10_under": _series(df, _col(df, "L10 Under", "l10_under")),
            "hit_rate": _series(df, _col(df, "Hit Rate", "hit_rate")),
            "strat_n": _series(df, _col(df, "Strat N", "strat_n")),
            "def_tier": _series(df, _col(df, "Def Tier", "def_tier", "DEF_TIER"), numeric=False).astype(str),
            "def_rank": def_rank,
            "opponent_rank": _series(df, _col(df, "opponent_rank", "Def Rank", "def_rank")),
            "n_teams": n_teams if n_teams is not None else np.nan,
            "min_tier": _series(df, _col(df, "Min Tier", "minutes_tier", "min_tier"), numeric=False).astype(str),
            "avg_min": _series(df, _col(df, "Avg Min", "avg_minutes", "avg_min")),
            "starter_tier": _series(df, _col(df, "Starter Tier", "starter_tier"), numeric=False).astype(str),
            "pos": _series(df, _col(df, "Pos", "pos", "position"), numeric=False).astype(str),
            "bat_order": _series(df, _col(df, "Bat Order", "batting_order_tier", "batting_order_pos"), numeric=False).astype(str),
            "lineup_confirmed": _series(df, _col(df, "Lineup Confirmed", "lineup_confirmed"), numeric=False).astype(str),
            "dist_std": _series(df, _col(df, "distribution_std", "Distribution Std")),
            "l10_rebuilt": g_l10,
        }
    )
    return out.drop_duplicates(["player_n", "prop_n", "line_r", "direction"], keep="last")


def rate(hits, n):
    if n <= 0:
        return None
    return round(100.0 * hits / n, 1)


def mean_r(s):
    s = pd.to_numeric(s, errors="coerce")
    if s.notna().sum() == 0:
        return None
    return round(float(s.mean()), 2)


def overall(df):
    n = int(len(df))
    h = int(df["hit"].sum()) if n else 0
    return {
        "hits": h,
        "n": n,
        "rate": rate(h, n),
        "mean_l5_gap": mean_r(df["g_l5"]) if n else None,
        "mean_l10_gap": mean_r(df["g_l10"]) if n else None,
        "mean_szn_gap": mean_r(df["g_szn"]) if n else None,
        "mean_min_gap": mean_r(df["min_gap"]) if n else None,
        "median_l5_gap": None
        if not n
        else round(float(pd.to_numeric(df["g_l5"], errors="coerce").median()), 2)
        if pd.to_numeric(df["g_l5"], errors="coerce").notna().any()
        else None,
    }


def tabulate(df, field, specs):
    s = pd.to_numeric(df[field], errors="coerce")
    out = {}
    for name, fn in specs:
        mask = s.notna() & fn(s)
        n = int(mask.sum())
        if n == 0:
            continue
        h = int(df.loc[mask, "hit"].sum())
        sub = df.loc[mask]
        out[name] = {
            "hits": h,
            "n": n,
            "rate": rate(h, n),
            "mean_l5_gap": mean_r(sub["g_l5"]),
            "mean_szn_gap": mean_r(sub["g_szn"]),
        }
    return out


def score_badge(row) -> str:
    sport_u = SPORT_U[row["sport"]]
    line = row["line"]
    if line is not None and (isinstance(line, float) and math.isnan(line)):
        line = None
    avg = row["szn"] if pd.notna(row["szn"]) else (row["l5"] if pd.notna(row["l5"]) else None)
    cover = None if avg is None or line is None else float(avg) - float(line)
    rec = {
        "sport": sport_u,
        "prop": str(row.get("prop_raw") or row.get("prop_n") or ""),
        "line": line,
        "pick_type": row["pick"],
        "side": row["direction"],
        "model_dir": row["direction"],
        "l5_over": None if pd.isna(row["l5_over"]) else int(row["l5_over"]),
        "l5_under": None if pd.isna(row["l5_under"]) else int(row["l5_under"]),
        "l10_over": None if pd.isna(row["l10_over"]) else int(row["l10_over"]),
        "l10_under": None if pd.isna(row["l10_under"]) else int(row["l10_under"]),
        "cover": None if cover is None else round(cover, 2),
        "def": R._def(
            {
                "sport": sport_u,
                "Def Tier": row.get("def_tier"),
                "def_tier": row.get("def_tier"),
                "opponent_rank": row.get("opponent_rank"),
                "Def Rank": row.get("def_rank"),
            }
        ),
        "def_rank": None if pd.isna(row["def_rank"]) else int(row["def_rank"]),
        "opponent_rank": None if pd.isna(row["opponent_rank"]) else int(row["opponent_rank"]),
        "hit_rate": None if pd.isna(row["hit_rate"]) else float(row["hit_rate"]),
        "strat_n": None if pd.isna(row["strat_n"]) else int(row["strat_n"]),
        "season_avg": None if avg is None else float(avg),
        "Season Avg": None if avg is None else float(avg),
        "L5 Over": None if pd.isna(row["l5_over"]) else int(row["l5_over"]),
        "L5 Under": None if pd.isna(row["l5_under"]) else int(row["l5_under"]),
        "L10 Over": None if pd.isna(row["l10_over"]) else int(row["l10_over"]),
        "L10 Under": None if pd.isna(row["l10_under"]) else int(row["l10_under"]),
        "Hit Rate": None if pd.isna(row["hit_rate"]) else float(row["hit_rate"]),
        "Strat N": None if pd.isna(row["strat_n"]) else int(row["strat_n"]),
        "Def Rank": None if pd.isna(row["def_rank"]) else int(row["def_rank"]),
        "Direction": row["direction"],
    }
    if sport_u == "TENNIS":
        rec["def"] = R._atp_tier_from_rank(rec["opponent_rank"] or rec["def_rank"])
        rec["def_rank"] = rec["opponent_rank"] or rec["def_rank"]
    n_teams = row.get("n_teams")
    n_teams = int(n_teams) if pd.notna(n_teams) else None
    rec.update(R._badge(rec, n_teams))
    rec["l10"] = rec["l10_over"] if rec["side"] == "OVER" else rec["l10_under"]
    rec["season_hr"] = R._season_hr(rec)
    rec["season_n"] = R._season_n(rec) or 0
    promo = R._promo(rec)
    if promo in BADGES:
        return promo
    if rec.get("badge") in BADGES:
        return rec["badge"]
    return "none"


def main():
    dates = sorted(p.name[len("graded_props_") : -5] for p in GRADED.glob("graded_props_20*.json"))
    step_frames = []
    n_files = 0
    for date in dates:
        for sport in SPORTS:
            p = find_step8(date, sport)
            if not p:
                continue
            print(f"load {date} {sport}", flush=True)
            step_frames.append(load_step8(p, sport, date))
            n_files += 1
    step8 = pd.concat(step_frames, ignore_index=True) if step_frames else pd.DataFrame()
    print("step8", len(step8), "files", n_files, flush=True)

    graded_rows = []
    for date in dates:
        raw = json.loads((GRADED / f"graded_props_{date}.json").read_text(encoding="utf-8"))
        for g in raw.get("props", []):
            sport = str(g.get("sport") or "")
            if sport not in SPORTS:
                continue
            result = str(g.get("result") or "").upper()
            if result not in ("HIT", "MISS"):
                continue
            pk = str(g.get("pick_type") or "").strip().lower()
            if "goblin" in pk:
                pick = "Goblin"
            elif "demon" in pk:
                pick = "Demon"
            else:
                pick = "Standard"
            d = str(g.get("direction") or g.get("over_under") or "").strip().upper()
            if d in ("O", "OVER"):
                d = "OVER"
            elif d in ("U", "UNDER"):
                d = "UNDER"
            try:
                line_v = float(g.get("line"))
            except (TypeError, ValueError):
                line_v = math.nan
            graded_rows.append(
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
    graded = pd.DataFrame(graded_rows)
    graded["line_r"] = graded["line"].round(2)
    keys = ["date", "sport", "player_n", "prop_n", "line_r", "direction"]
    merged = graded.merge(step8, on=keys, how="inner")
    leftover = graded.merge(merged[keys].drop_duplicates().assign(_hit=1), on=keys, how="left")
    leftover = leftover[leftover["_hit"].isna()].drop(columns="_hit")
    loose = leftover.merge(
        step8.drop(columns=["line_r"]).drop_duplicates(
            ["date", "sport", "player_n", "prop_n", "direction"], keep="last"
        ),
        on=["date", "sport", "player_n", "prop_n", "direction"],
        how="inner",
    )
    if len(loose):
        merged = pd.concat([merged, loose], ignore_index=True)
    print("merged", len(merged), flush=True)

    line = pd.to_numeric(merged["line"], errors="coerce")
    over = merged["direction"].eq("OVER")
    under = merged["direction"].eq("UNDER")
    for src, dest in (("l5", "g_l5"), ("l10", "g_l10"), ("szn", "g_szn")):
        avg = pd.to_numeric(merged[src], errors="coerce")
        gap = pd.Series(np.nan, index=merged.index)
        gap = gap.mask(over, avg - line)
        gap = gap.mask(under, line - avg)
        merged[dest] = gap
    merged["min_gap"] = merged[["g_l5", "g_l10", "g_szn"]].min(axis=1, skipna=True)

    sg = merged[merged["pick"].isin(["Standard", "Goblin"])].copy()
    print("scoring badges", len(sg), flush=True)
    sg["badge"] = [score_badge(r) for r in sg.to_dict("records")]
    print(sg["badge"].value_counts().to_string(), flush=True)

    def pack_sport(df, sport):
        specs = SPORT_GAPS[sport]
        by_badge = {}
        for b in BADGES + ("none",):
            sub = df[df["badge"].eq(b)]
            by_badge[b] = {
                **overall(sub),
                "min_gap": tabulate(sub, "min_gap", specs),
                "l5_gap": tabulate(sub, "g_l5", specs),
            }
        return {
            "overall": overall(df),
            "by_badge": by_badge,
            "gap_scale": [name for name, _ in specs],
            "all_min_gap": tabulate(df, "min_gap", specs),
            "all_l5_gap": tabulate(df, "g_l5", specs),
        }

    by_sport = {sp: pack_sport(sg[sg["sport"].eq(sp)], sp) for sp in SPORTS}
    combined_badge = {b: overall(sg[sg["badge"].eq(b)]) for b in BADGES + ("none",)}

    out = {
        "window": {
            "first": str(sg["date"].min()) if len(sg) else None,
            "last": str(sg["date"].max()) if len(sg) else None,
            "n_dates": int(sg["date"].nunique()) if len(sg) else 0,
            "n_board_files": n_files,
        },
        "universe": "Std+Goblin decided HIT/MISS joined to step8. Badges = live Diamond/Platinum/Gold/Silver/Bronze recipe.",
        "gap_def": "OVER: avg-line; UNDER: line-avg. Positive = on the pick side. MLB/Soccer use 0.5-pt buckets; WNBA/Tennis use 1-pt buckets.",
        "combined_badge": combined_badge,
        "by_sport": by_sport,
        "badge_counts": {k: int(v) for k, v in sg["badge"].value_counts().items()},
    }
    dest = OPEN / "data" / "reports" / "avg_gap_by_sport_badge.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(out, indent=2), encoding="utf-8")
    print(json.dumps({"window": out["window"], "combined_badge": combined_badge, "by_sport": by_sport, "badge_counts": out["badge_counts"]}, indent=2))
    print("wrote", dest)


if __name__ == "__main__":
    main()
