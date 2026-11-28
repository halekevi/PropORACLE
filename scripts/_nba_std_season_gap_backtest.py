"""NBA Standard props: season-average gap to line vs Over/Under hit rate.

Season average is computed from the box DB (current regular season, games
strictly before the slate date) — the board's stat_season_avg is the L10 mean.
Actual is that player's box line on the slate date. No box row or 0 minutes
-> skipped (not graded). Combos are skipped.

  py -3.14 scripts/_nba_std_season_gap_backtest.py [--start 2026-10-21] [--end 2026-11-25]
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SEASON_START = "2026-10-20"
PROP_COL = {
    "pts": "pts", "reb": "reb", "ast": "ast", "pra": "pra", "pr": "pr", "pa": "pa", "ra": "ra",
    "fga": "fga", "fgm": "fgm", "3ptattempted": "fg3a", "3ptmade": "fg3m",
    "freethrowsattempted": "fta", "freethrowsmade": "ftm",
    "twopointersattempted": "fg2a", "twopointersmade": "fg2m",
    "stl": "stl", "blk": "blk", "stocks": "bs", "tov": "tov", "personalfouls": "pf",
    "offensiverebounds": "oreb", "defensiverebounds": "dreb", "fantasy": "fantasy_score", "dunks": "dunks",
}
PROP_GROUP = {
    "pts": "Points", "reb": "Rebounds", "ast": "Assists",
    "pra": "PRA / combos", "pr": "PRA / combos", "pa": "PRA / combos", "ra": "PRA / combos",
    "fga": "Shot volume", "fgm": "Shot volume", "3ptattempted": "Shot volume", "twopointersattempted": "Shot volume",
    "twopointersmade": "Shot volume", "freethrowsattempted": "Free throws", "freethrowsmade": "Free throws",
    "3ptmade": "3PM", "stl": "Stl/Blk/Stocks", "blk": "Stl/Blk/Stocks", "stocks": "Stl/Blk/Stocks",
}
REL_BINS = [-10, -0.30, -0.20, -0.10, -0.05, 0.0, 0.05, 0.10, 0.20, 0.30, 10]
REL_LABELS = ["<= -30%", "-30..-20%", "-20..-10%", "-10..-5%", "-5..0%", "0..+5%", "+5..+10%", "+10..+20%", "+20..+30%", ">= +30%"]


def load_box() -> pd.DataFrame:
    con = sqlite3.connect(ROOT / "data" / "cache" / "proporacle_ref.db")
    cols = sorted(set(PROP_COL.values()))
    box = pd.read_sql(
        f"SELECT game_date, espn_athlete_id, minutes, {', '.join(cols)} FROM nba "
        "WHERE season_window IS NULL",
        con,
    )
    box["espn_athlete_id"] = box["espn_athlete_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    return box.drop_duplicates(["espn_athlete_id", "game_date"])


def load_boards(start: str, end: str, pick_type: str = "Standard") -> pd.DataFrame:
    frames = []
    for d in sorted((ROOT / "outputs").glob("2026-*")):
        if not (start <= d.name <= end):
            continue
        f = d / "nba" / "step4_with_stats.csv"
        if not f.exists():
            continue
        b = pd.read_csv(f, low_memory=False, usecols=lambda c: c in {
            "player", "prop_norm", "line", "pick_type", "ESPN_ATHLETE_ID", "game_date", "is_combo_player",
            "DEF_TIER", "OVERALL_DEF_RANK", "team", "opp_team", "standard_line"})
        for c in ("DEF_TIER", "OVERALL_DEF_RANK", "standard_line"):
            if c not in b.columns:
                b[c] = None
        b = b[(b["pick_type"].astype(str) == pick_type) & b["prop_norm"].isin(PROP_COL)]
        if "is_combo_player" in b:
            b = b[pd.to_numeric(b["is_combo_player"], errors="coerce").fillna(0).astype(int) == 0]
        b["slate"] = b["game_date"].astype(str).str[:10]
        b = b[b["slate"] == d.name]
        frames.append(b)
    out = pd.concat(frames, ignore_index=True)
    out["espn"] = out["ESPN_ATHLETE_ID"].astype(str).str.replace(r"\.0$", "", regex=True)
    out["line"] = pd.to_numeric(out["line"], errors="coerce")
    return out.dropna(subset=["line"]).drop_duplicates(["slate", "espn", "prop_norm", "line"])


def build(start: str, end: str, min_games: int, pick_type: str = "Standard") -> pd.DataFrame:
    box = load_box()
    boards = load_boards(start, end, pick_type)
    by_player = {k: g.sort_values("game_date") for k, g in box.groupby("espn_athlete_id")}
    rows = []
    for r in boards.itertuples(index=False):
        g = by_player.get(r.espn)
        if g is None:
            continue
        col = PROP_COL[r.prop_norm]
        today = g[g["game_date"] == r.slate]
        if today.empty or not (pd.to_numeric(today["minutes"], errors="coerce").fillna(0) > 0).any():
            continue
        played = g[(g["game_date"] < r.slate) & (pd.to_numeric(g["minutes"], errors="coerce").fillna(0) > 0)]
        prior_vals = pd.to_numeric(played.loc[played["game_date"] >= SEASON_START, col], errors="coerce").dropna()
        if len(prior_vals) < min_games:
            continue
        # Card window: newest RS + playoff games across seasons (preseason excluded at load).
        recent = pd.to_numeric(played[col], errors="coerce").dropna().iloc[::-1]
        l5, l10 = recent.iloc[:5], recent.iloc[:10]
        mins = pd.to_numeric(played["minutes"], errors="coerce").dropna().iloc[::-1]
        min_season = pd.to_numeric(played.loc[played["game_date"] >= SEASON_START, "minutes"], errors="coerce").mean()
        actual = float(pd.to_numeric(today[col], errors="coerce").iloc[0])
        season = float(prior_vals.mean())
        rows.append({
            "slate": r.slate, "player": r.player, "prop": r.prop_norm, "group": PROP_GROUP.get(r.prop_norm, "Other"),
            "line": r.line, "season_avg": round(season, 2), "games": len(prior_vals), "actual": actual,
            "gap": season - r.line, "rel_gap": (season - r.line) / r.line if r.line else 0.0,
            "over_hit": int(actual > r.line), "under_hit": int(actual < r.line), "push": int(actual == r.line),
            "l5_n": len(l5), "l5_over": int((l5 > r.line).sum()), "l5_under": int((l5 < r.line).sum()),
            "l10_n": len(l10), "l10_over": int((l10 > r.line).sum()), "l10_under": int((l10 < r.line).sum()),
            "l5_avg": round(float(l5.mean()), 2), "l10_avg": round(float(l10.mean()), 2),
            "min_l5": round(float(mins.iloc[:5].mean()), 1), "min_season": round(float(min_season), 1),
            "def_tier": r.DEF_TIER, "def_rank": r.OVERALL_DEF_RANK,
            "standard_line": pd.to_numeric(r.standard_line, errors="coerce"),
        })
    return pd.DataFrame(rows)


def table(df: pd.DataFrame, by: str) -> pd.DataFrame:
    dd = df[df["push"] == 0]
    t = dd.groupby(by, observed=True).agg(n=("over_hit", "size"), over_hit=("over_hit", "mean"), under_hit=("under_hit", "mean"))
    t["over_hit"] = (t["over_hit"] * 100).round(1)
    t["under_hit"] = (t["under_hit"] * 100).round(1)
    return t


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-10-21")
    ap.add_argument("--end", default="2026-11-25")
    ap.add_argument("--min-games", type=int, default=3)
    ap.add_argument("--pick-type", default="Standard", choices=["Standard", "Goblin"])
    args = ap.parse_args()
    df = build(args.start, args.end, args.min_games, args.pick_type)
    df["rel_bin"] = pd.cut(df["rel_gap"], REL_BINS, labels=REL_LABELS)
    print(f"{args.pick_type} legs graded: {len(df)}  slates: {df['slate'].nunique()}  ({df['slate'].min()} .. {df['slate'].max()})")
    print(f"Baseline: Over {df['over_hit'].mean():.1%}  Under {df['under_hit'].mean():.1%}  push {df['push'].mean():.1%}\n")
    print("Season avg vs line (relative gap) — all Standard props")
    print(table(df, "rel_bin").to_string(), "\n")
    for grp in ["Points", "Rebounds", "Assists", "PRA / combos", "3PM", "Shot volume", "Free throws", "Stl/Blk/Stocks"]:
        sub = df[df["group"] == grp]
        if len(sub) < 40:
            continue
        print(f"{grp} (n={len(sub)})")
        print(table(sub, "rel_bin").to_string(), "\n")
    stem = "nba_std_season_gap_backtest" if args.pick_type == "Standard" else "nba_goblin_season_gap_backtest"
    out = ROOT / "data" / "reports" / f"{stem}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps({
        "range": [args.start, args.end], "n": len(df),
        "all": table(df, "rel_bin").reset_index().astype({"rel_bin": str}).to_dict("records"),
    }, indent=2), encoding="utf-8")
    df.to_csv(out.with_suffix(".csv"), index=False)
    print(f"-> {out}")


if __name__ == "__main__":
    main()
