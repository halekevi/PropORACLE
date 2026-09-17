#!/usr/bin/env python3
"""Build NFL OL / DL / secondary / box unit ranks from free nflverse feeds.

Sources (no auth):
  - pfr_advstats week pass/rush/def (pressure, YBC, coverage)
  - stats_team_reg (sack rates, EPA proxies)
  - ftn_charting (box size, blitz rate)
  - nextgen_stats rolling receiving (separation → secondary proxy)

Writes::

  Sports/NFL/data/nfl_ol_ranks.csv
  Sports/NFL/data/nfl_dl_ranks.csv
  Sports/NFL/data/nfl_secondary_ranks.csv
  Sports/NFL/data/nfl_box_ranks.csv

Refresh::

  py -3.14 Sports/NFL/scripts/build_nfl_unit_line_ranks.py
  py -3.14 Sports/NFL/scripts/build_nfl_unit_line_ranks.py --season 2026
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_SCRIPT = Path(__file__).resolve().parent
_NFL = _SCRIPT.parent
_REPO = _SCRIPT.parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.nfl_espn_context import canon_nfl_abbr  # noqa: E402

UA = {"User-Agent": "PropOracle/1.0 (+https://github.com/halekevi/PropORACLE)"}
NFLVERSE = "https://github.com/nflverse/nflverse-data/releases/download"

OUT_OL = _NFL / "data" / "nfl_ol_ranks.csv"
OUT_DL = _NFL / "data" / "nfl_dl_ranks.csv"
OUT_SEC = _NFL / "data" / "nfl_secondary_ranks.csv"
OUT_BOX = _NFL / "data" / "nfl_box_ranks.csv"
OUT_META = _NFL / "data" / "nfl_unit_line_ranks_meta.json"

# Canonical 32 (PropORACLE abbreviations).
TEAMS_32 = (
    "ARI", "ATL", "BAL", "BUF", "CAR", "CHI", "CIN", "CLE",
    "DAL", "DEN", "DET", "GB", "HOU", "IND", "JAX", "KC",
    "LAC", "LAR", "LV", "MIA", "MIN", "NE", "NO", "NYG",
    "NYJ", "PHI", "PIT", "SEA", "SF", "TB", "TEN", "WSH",
)


def _get_bytes(url: str, timeout: float = 90.0) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def _read_csv(url: str) -> pd.DataFrame:
    raw = _get_bytes(url)
    if url.endswith(".gz"):
        raw = gzip.decompress(raw)
    return pd.read_csv(io.BytesIO(raw))


def _canon_series(s: pd.Series) -> pd.Series:
    return s.map(lambda x: canon_nfl_abbr(x) or "")


def _rank_asc(series: pd.Series) -> pd.Series:
    """1 = best (lowest rate/yards allowed)."""
    return series.rank(method="min", ascending=True).astype("Int64")


def _rank_desc(series: pd.Series) -> pd.Series:
    """1 = best (highest pressure / stop rate)."""
    return series.rank(method="min", ascending=False).astype("Int64")


def _tier_from_rank(rank: object, n: int = 32) -> str:
    try:
        r = int(rank)
    except (TypeError, ValueError):
        return "Avg"
    if r <= max(1, n // 5):
        return "Elite"
    if r <= max(2, (2 * n) // 5):
        return "Above Avg"
    if r <= max(3, (3 * n) // 5):
        return "Avg"
    if r <= max(4, (4 * n) // 5):
        return "Below Avg"
    return "Weak"


def infer_season(today: datetime | None = None) -> int:
    d = today or datetime.now(timezone.utc)
    return d.year if d.month >= 9 else d.year - 1


def _load_pfr_week(kind: str, season: int) -> pd.DataFrame:
    url = f"{NFLVERSE}/pfr_advstats/advstats_week_{kind}_{season}.csv.gz"
    df = _read_csv(url)
    if "game_type" in df.columns:
        df = df[df["game_type"].astype(str).str.upper().eq("REG")].copy()
    df["team"] = _canon_series(df["team"])
    if "opponent" in df.columns:
        df["opponent"] = _canon_series(df["opponent"])
    return df[df["team"].isin(TEAMS_32)].copy()


def _load_team_reg(season: int) -> pd.DataFrame:
    url = f"{NFLVERSE}/stats_team/stats_team_reg_{season}.csv.gz"
    df = _read_csv(url)
    if "season_type" in df.columns:
        df = df[df["season_type"].astype(str).str.upper().isin(("REG", "REGULAR"))].copy()
    df["team"] = _canon_series(df["team"])
    return df[df["team"].isin(TEAMS_32)].copy()


def _load_ftn(season: int) -> pd.DataFrame:
    # Prefer csv; fall back to parquet-less path
    for name in (f"ftn_charting_{season}.csv", f"ftn_charting_{season}.csv.gz"):
        url = f"{NFLVERSE}/ftn_charting/{name}"
        try:
            return _read_csv(url)
        except Exception:
            continue
    return pd.DataFrame()


def _load_ngs_receiving(season: int) -> pd.DataFrame:
    """Season rollup rows are week==0 in the rolling ngs_receiving file."""
    url = f"{NFLVERSE}/nextgen_stats/ngs_receiving.csv.gz"
    df = _read_csv(url)
    df = df[(df["season"] == season) & (df["season_type"].astype(str).str.upper() == "REG")].copy()
    # Prefer season aggregate (week 0); else all weeks.
    if (df["week"] == 0).any():
        df = df[df["week"] == 0].copy()
    df["team"] = _canon_series(df["team_abbr"])
    return df[df["team"].isin(TEAMS_32)].copy()


def build_ol(pfr_pass: pd.DataFrame, pfr_rush: pd.DataFrame, team: pd.DataFrame) -> pd.DataFrame:
    """Own OL: pass-block from pressure/sack allowed; run-block from YBC."""
    rows: dict[str, dict[str, Any]] = {t: {"team": t} for t in TEAMS_32}

    if not pfr_pass.empty:
        g = (
            pfr_pass.groupby("team", as_index=False)
            .agg(
                times_pressured=("times_pressured", "sum"),
                times_sacked=("times_sacked", "sum"),
                times_blitzed=("times_blitzed", "sum"),
                drop_attempts=("times_pressured", "count"),
            )
        )
        # Approximate dropbacks = pressured / pressured_pct mean when available
        if "times_pressured_pct" in pfr_pass.columns:
            pct = (
                pfr_pass.groupby("team")["times_pressured_pct"]
                .mean()
                .rename("pressure_pct")
            )
            g = g.merge(pct, left_on="team", right_index=True, how="left")
        else:
            g["pressure_pct"] = np.nan
        for _, r in g.iterrows():
            t = r["team"]
            rows[t]["pressure_rate_allowed"] = (
                float(r["pressure_pct"]) if pd.notna(r.get("pressure_pct")) else np.nan
            )
            rows[t]["sacks_allowed"] = float(r["times_sacked"]) if pd.notna(r["times_sacked"]) else np.nan

    if not team.empty:
        t2 = team.copy()
        t2["sack_rate_allowed"] = np.where(
            t2["attempts"].fillna(0) > 0,
            t2["sacks_suffered"] / (t2["attempts"] + t2["sacks_suffered"]),
            np.nan,
        )
        for _, r in t2.iterrows():
            t = r["team"]
            rows[t]["sack_rate_allowed"] = float(r["sack_rate_allowed"]) if pd.notna(r["sack_rate_allowed"]) else np.nan
            rows[t]["games"] = int(r["games"]) if pd.notna(r.get("games")) else np.nan

    if not pfr_rush.empty:
        g = (
            pfr_rush.groupby("team", as_index=False)
            .agg(
                carries=("carries", "sum"),
                ybc=("rushing_yards_before_contact", "sum"),
            )
        )
        g["yards_before_contact"] = np.where(
            g["carries"] > 0, g["ybc"] / g["carries"], np.nan
        )
        for _, r in g.iterrows():
            rows[r["team"]]["yards_before_contact"] = float(r["yards_before_contact"])

    df = pd.DataFrame([rows[t] for t in TEAMS_32])
    # Stuff rate proxy: inverse of YBC (low YBC = stuffed) — store as rank helper only
    df["pass_block_rank"] = _rank_asc(
        df["pressure_rate_allowed"].fillna(df["sack_rate_allowed"])
    )
    df["run_block_rank"] = _rank_desc(df["yards_before_contact"])
    # Combined OL rank: average of pass+run ranks
    df["ol_rank"] = (
        (df["pass_block_rank"].astype(float) + df["run_block_rank"].astype(float)) / 2.0
    ).rank(method="min").astype("Int64")
    df["tier"] = df["ol_rank"].map(_tier_from_rank)
    df["season"] = int(team["season"].iloc[0]) if not team.empty and "season" in team.columns else np.nan
    cols = [
        "team",
        "season",
        "games",
        "ol_rank",
        "pass_block_rank",
        "run_block_rank",
        "sack_rate_allowed",
        "pressure_rate_allowed",
        "yards_before_contact",
        "sacks_allowed",
        "tier",
    ]
    return df[[c for c in cols if c in df.columns]]


def build_dl(pfr_def: pd.DataFrame, pfr_rush: pd.DataFrame, team: pd.DataFrame) -> pd.DataFrame:
    """Opp-facing DL: pass rush from pressures/sacks; run stop from opp YBC allowed."""
    rows: dict[str, dict[str, Any]] = {t: {"team": t} for t in TEAMS_32}

    if not pfr_def.empty:
        g = pfr_def.groupby("team", as_index=False).agg(
            pressures=("def_pressures", "sum"),
            sacks=("def_sacks", "sum"),
            hurries=("def_times_hurried", "sum"),
            blitzes=("def_times_blitzed", "sum"),
        )
        for _, r in g.iterrows():
            t = r["team"]
            rows[t]["pressures"] = float(r["pressures"]) if pd.notna(r["pressures"]) else np.nan
            rows[t]["sacks"] = float(r["sacks"]) if pd.notna(r["sacks"]) else np.nan
            rows[t]["hurries"] = float(r["hurries"]) if pd.notna(r["hurries"]) else np.nan

    if not team.empty:
        for _, r in team.iterrows():
            t = r["team"]
            gms = float(r["games"]) if pd.notna(r.get("games")) and r["games"] else np.nan
            rows[t]["games"] = gms
            if pd.notna(gms) and gms > 0:
                rows[t]["sacks_pg"] = float(r.get("def_sacks") or 0) / gms
                rows[t]["qb_hits_pg"] = float(r.get("def_qb_hits") or 0) / gms
                rows[t]["tfl_pg"] = float(r.get("def_tackles_for_loss") or 0) / gms

    # Run stop: yards before contact allowed = opponent rushers' YBC against this defense
    if not pfr_rush.empty and "opponent" in pfr_rush.columns:
        g = (
            pfr_rush.groupby("opponent", as_index=False)
            .agg(
                carries=("carries", "sum"),
                ybc=("rushing_yards_before_contact", "sum"),
                yac=("rushing_yards_after_contact", "sum"),
            )
            .rename(columns={"opponent": "team"})
        )
        g["yards_before_contact_allowed"] = np.where(
            g["carries"] > 0, g["ybc"] / g["carries"], np.nan
        )
        g["yards_after_contact_allowed"] = np.where(
            g["carries"] > 0, g["yac"] / g["carries"], np.nan
        )
        for _, r in g.iterrows():
            if r["team"] not in rows:
                continue
            rows[r["team"]]["yards_before_contact_allowed"] = float(
                r["yards_before_contact_allowed"]
            )
            rows[r["team"]]["yards_after_contact_allowed"] = float(
                r["yards_after_contact_allowed"]
            )

    df = pd.DataFrame([rows[t] for t in TEAMS_32])
    df["pass_rush_rank"] = _rank_desc(
        df["sacks_pg"].fillna(df.get("pressures", pd.Series(dtype=float)))
    )
    df["run_stop_rank"] = _rank_asc(df["yards_before_contact_allowed"])
    df["dl_rank"] = (
        (df["pass_rush_rank"].astype(float) + df["run_stop_rank"].astype(float)) / 2.0
    ).rank(method="min").astype("Int64")
    df["tier"] = df["dl_rank"].map(_tier_from_rank)
    df["season"] = int(team["season"].iloc[0]) if not team.empty and "season" in team.columns else np.nan
    cols = [
        "team",
        "season",
        "games",
        "dl_rank",
        "pass_rush_rank",
        "run_stop_rank",
        "sacks_pg",
        "qb_hits_pg",
        "tfl_pg",
        "pressures",
        "sacks",
        "yards_before_contact_allowed",
        "yards_after_contact_allowed",
        "tier",
    ]
    return df[[c for c in cols if c in df.columns]]


def build_secondary(pfr_def: pd.DataFrame, ngs_rec: pd.DataFrame) -> pd.DataFrame:
    """Coverage unit: PFR def targets allowed + NGS separation allowed (via opp WR)."""
    rows: dict[str, dict[str, Any]] = {t: {"team": t} for t in TEAMS_32}

    if not pfr_def.empty:
        g = pfr_def.groupby("team", as_index=False).agg(
            targets=("def_targets", "sum"),
            completions=("def_completions_allowed", "sum"),
            yards=("def_yards_allowed", "sum"),
            adot=("def_adot", "mean"),
            air_yards=("def_air_yards_completed", "sum"),
            yac=("def_yards_after_catch", "sum"),
            ints=("def_ints", "sum"),
            rating=("def_passer_rating_allowed", "mean"),
        )
        g["completion_pct_allowed"] = np.where(
            g["targets"] > 0, g["completions"] / g["targets"], np.nan
        )
        g["yards_per_target"] = np.where(
            g["targets"] > 0, g["yards"] / g["targets"], np.nan
        )
        for _, r in g.iterrows():
            t = r["team"]
            rows[t].update(
                {
                    "targets": float(r["targets"]),
                    "completion_pct_allowed": float(r["completion_pct_allowed"])
                    if pd.notna(r["completion_pct_allowed"])
                    else np.nan,
                    "yards_per_target": float(r["yards_per_target"])
                    if pd.notna(r["yards_per_target"])
                    else np.nan,
                    "air_yards_allowed": float(r["air_yards"]) if pd.notna(r["air_yards"]) else np.nan,
                    "adot_allowed": float(r["adot"]) if pd.notna(r["adot"]) else np.nan,
                    "yac_allowed": float(r["yac"]) if pd.notna(r["yac"]) else np.nan,
                    "passer_rating_allowed": float(r["rating"]) if pd.notna(r["rating"]) else np.nan,
                    "ints": float(r["ints"]) if pd.notna(r["ints"]) else np.nan,
                }
            )

    # Separation allowed ≈ mean WR separation against that defense is hard without
    # play-level join; use own-team NGS separation as scheme-aggression proxy only
    # for documentation (not primary rank). Slot/outside left blank until charting.
    if not ngs_rec.empty:
        g = ngs_rec.groupby("team", as_index=False).agg(
            avg_separation=("avg_separation", "mean"),
            avg_cushion=("avg_cushion", "mean"),
        )
        for _, r in g.iterrows():
            if r["team"] in rows:
                rows[r["team"]]["own_wr_avg_separation"] = float(r["avg_separation"]) if pd.notna(r["avg_separation"]) else np.nan
                rows[r["team"]]["own_wr_avg_cushion"] = float(r["avg_cushion"]) if pd.notna(r["avg_cushion"]) else np.nan

    df = pd.DataFrame([rows[t] for t in TEAMS_32])
    df["coverage_rank"] = _rank_asc(df["yards_per_target"])
    # Slot/outside placeholders (same as coverage until alignment splits exist)
    df["slot_rank"] = df["coverage_rank"]
    df["outside_rank"] = df["coverage_rank"]
    df["secondary_rank"] = df["coverage_rank"]
    df["tier"] = df["secondary_rank"].map(_tier_from_rank)
    df["coverage_scheme"] = ""  # Zone/Man/Mixed — fill from FTN/NGS later
    cols = [
        "team",
        "secondary_rank",
        "coverage_rank",
        "slot_rank",
        "outside_rank",
        "yards_per_target",
        "completion_pct_allowed",
        "adot_allowed",
        "air_yards_allowed",
        "yac_allowed",
        "passer_rating_allowed",
        "ints",
        "targets",
        "own_wr_avg_separation",
        "own_wr_avg_cushion",
        "coverage_scheme",
        "tier",
    ]
    return df[[c for c in cols if c in df.columns]]


def build_box(ftn: pd.DataFrame, pfr_rush: pd.DataFrame, pfr_def: pd.DataFrame) -> pd.DataFrame:
    """Front-seven / box: FTN box size + blitz; run-stop from YAC/YBC allowed."""
    rows: dict[str, dict[str, Any]] = {t: {"team": t} for t in TEAMS_32}

    if not ftn.empty:
        # Map game → defense team via nflverse game id (AWAY_HOME) is awkward;
        # FTN rows are play-level without defense team. Infer from nflverse_game_id
        # + possession would need pbp. Use blitz/box as league-wide until joined.
        # Instead: join via pfr_def blitz counts as primary; FTN for season means
        # only if we can parse game id teams.
        ftn = ftn.copy()
        if "nflverse_game_id" in ftn.columns:
            # game_id like 2025_01_DAL_PHI → away=DAL home=PHI; defense flips by play
            # Without possession we approximate home/away split of blitz by averaging
            # both teams in the game (weak). Better: skip team assign from FTN alone.
            pass

    if not pfr_def.empty:
        g = pfr_def.groupby("team", as_index=False).agg(
            blitzes=("def_times_blitzed", "sum"),
            pressures=("def_pressures", "sum"),
            targets=("def_targets", "sum"),
        )
        # blitz rate proxy = blitzes / (targets + pressures) rough snap proxy
        g["blitz_rate"] = np.where(
            (g["targets"] + g["pressures"]) > 0,
            g["blitzes"] / (g["targets"] + g["pressures"]),
            np.nan,
        )
        for _, r in g.iterrows():
            rows[r["team"]]["blitz_rate"] = float(r["blitz_rate"]) if pd.notna(r["blitz_rate"]) else np.nan
            rows[r["team"]]["blitzes"] = float(r["blitzes"]) if pd.notna(r["blitzes"]) else np.nan

    if not pfr_rush.empty and "opponent" in pfr_rush.columns:
        g = (
            pfr_rush.groupby("opponent", as_index=False)
            .agg(
                carries=("carries", "sum"),
                yac=("rushing_yards_after_contact", "sum"),
                ybc=("rushing_yards_before_contact", "sum"),
            )
            .rename(columns={"opponent": "team"})
        )
        g["yac_allowed"] = np.where(g["carries"] > 0, g["yac"] / g["carries"], np.nan)
        g["ybc_allowed"] = np.where(g["carries"] > 0, g["ybc"] / g["carries"], np.nan)
        # stuff proxy: share of carries with ybc<=0 not available; use low ybc as stop
        for _, r in g.iterrows():
            if r["team"] not in rows:
                continue
            rows[r["team"]]["yards_after_contact_allowed"] = float(r["yac_allowed"])
            rows[r["team"]]["yards_before_contact_allowed"] = float(r["ybc_allowed"])

    df = pd.DataFrame([rows[t] for t in TEAMS_32])
    df["run_stop_rank"] = _rank_asc(df["yards_before_contact_allowed"])
    df["box_rank"] = df["run_stop_rank"]
    df["tier"] = df["box_rank"].map(_tier_from_rank)
    cols = [
        "team",
        "box_rank",
        "run_stop_rank",
        "blitz_rate",
        "blitzes",
        "yards_before_contact_allowed",
        "yards_after_contact_allowed",
        "tier",
    ]
    return df[[c for c in cols if c in df.columns]]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--season", type=int, default=0, help="NFL season year (default: current)")
    args = ap.parse_args()
    season = int(args.season) or infer_season()

    print(f"[unit-line] season={season}")
    sources: dict[str, str] = {}
    try:
        pfr_pass = _load_pfr_week("pass", season)
        sources["pfr_pass"] = f"advstats_week_pass_{season}"
        print(f"  pfr pass rows={len(pfr_pass)}")
    except Exception as exc:
        print(f"  pfr pass FAIL: {exc}")
        pfr_pass = pd.DataFrame()

    try:
        pfr_rush = _load_pfr_week("rush", season)
        sources["pfr_rush"] = f"advstats_week_rush_{season}"
        print(f"  pfr rush rows={len(pfr_rush)}")
    except Exception as exc:
        print(f"  pfr rush FAIL: {exc}")
        pfr_rush = pd.DataFrame()

    try:
        pfr_def = _load_pfr_week("def", season)
        sources["pfr_def"] = f"advstats_week_def_{season}"
        print(f"  pfr def rows={len(pfr_def)}")
    except Exception as exc:
        print(f"  pfr def FAIL: {exc}")
        pfr_def = pd.DataFrame()

    try:
        team = _load_team_reg(season)
        sources["stats_team"] = f"stats_team_reg_{season}"
        print(f"  team rows={len(team)}")
    except Exception as exc:
        print(f"  team FAIL: {exc}")
        team = pd.DataFrame()

    try:
        ngs = _load_ngs_receiving(season)
        sources["ngs_receiving"] = "ngs_receiving.csv.gz"
        print(f"  ngs rec rows={len(ngs)}")
    except Exception as exc:
        print(f"  ngs FAIL: {exc}")
        ngs = pd.DataFrame()

    try:
        ftn = _load_ftn(season)
        sources["ftn"] = f"ftn_charting_{season}"
        print(f"  ftn rows={len(ftn)}")
    except Exception as exc:
        print(f"  ftn FAIL: {exc}")
        ftn = pd.DataFrame()

    ol = build_ol(pfr_pass, pfr_rush, team)
    dl = build_dl(pfr_def, pfr_rush, team)
    sec = build_secondary(pfr_def, ngs)
    box = build_box(ftn, pfr_rush, pfr_def)

    for frame in (ol, dl, sec, box):
        if "season" not in frame.columns or frame["season"].isna().all():
            frame["season"] = season

    OUT_OL.parent.mkdir(parents=True, exist_ok=True)
    ol.to_csv(OUT_OL, index=False)
    dl.to_csv(OUT_DL, index=False)
    sec.to_csv(OUT_SEC, index=False)
    box.to_csv(OUT_BOX, index=False)

    meta = {
        "season": season,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "sources": sources,
        "paths": {
            "ol": str(OUT_OL.relative_to(_REPO)),
            "dl": str(OUT_DL.relative_to(_REPO)),
            "secondary": str(OUT_SEC.relative_to(_REPO)),
            "box": str(OUT_BOX.relative_to(_REPO)),
        },
        "notes": [
            "Ranks: 1=best. OL pass-block uses pressure/sack allowed (low=good).",
            "DL pass-rush uses sacks/game (high=good); run-stop uses opp YBC allowed (low=good).",
            "Secondary uses yards/target allowed (low=good). Slot/outside mirror coverage until alignment splits.",
            "coverage_scheme left blank for manual/NGS fill (Zone/Man/Mixed).",
            "FTN box size not team-joined yet without pbp possession; blitz from PFR def.",
        ],
    }
    OUT_META.write_text(json.dumps(meta, indent=2), encoding="utf-8")

    print(f"  wrote {OUT_OL}")
    print(f"  wrote {OUT_DL}")
    print(f"  wrote {OUT_SEC}")
    print(f"  wrote {OUT_BOX}")
    print(f"  wrote {OUT_META}")
    print("\nOL top-5 pass-block:")
    print(ol.nsmallest(5, "pass_block_rank")[["team", "pass_block_rank", "pressure_rate_allowed", "sack_rate_allowed", "tier"]].to_string(index=False))
    print("\nDL top-5 pass-rush:")
    print(dl.nsmallest(5, "pass_rush_rank")[["team", "pass_rush_rank", "sacks_pg", "tier"]].to_string(index=False))
    print("\nSecondary top-5 coverage:")
    print(sec.nsmallest(5, "coverage_rank")[["team", "coverage_rank", "yards_per_target", "completion_pct_allowed", "tier"]].to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
