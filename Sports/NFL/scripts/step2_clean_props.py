#!/usr/bin/env python3
"""
NFL step2 — normalize PrizePicks prop types and infer position group.

Reads step1 CSV, writes NFL/data/outputs/step2_clean_props.csv

Run from repo NFL folder (or anywhere with paths adjusted):
  set NFL_PIPELINE_ACTIVE=1
  py -3.14 scripts/step2_clean_props.py
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_NFL_ROOT = _SCRIPT_DIR.parent
_REPO_ROOT = _SCRIPT_DIR.resolve().parents[2]
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from _nfl_pipeline_active import require_nfl_pipeline_active_or_exit
from utils.nfl_prop_defense import fill_opp_team_from_game
from utils.nfl_espn_context import espn_nfl_opp_map_for_dates, iso_to_et_date
from utils.nfl_unsupported_props import mark_football_unsupported_props

PASSING_PROPS = frozenset(
    {
        "passing_yards",
        "passing_tds",
        "pass_attempts",
        "completions",
        "interceptions",
    }
)
RUSHING_PROPS = frozenset({"rushing_yards", "rushing_attempts", "rushing_tds", "carries"})
RECEIVING_PROPS = frozenset({"receiving_yards", "receptions", "targets", "receiving_tds"})
SCORING_PROPS = frozenset({"anytime_td", "first_touchdown", "first_td"})
DEFENSE_PROPS = frozenset({"sacks", "tackles", "solo_tackles", "assisted_tackles", "defensive_interceptions"})


def _norm_prop(raw: str) -> str:
    s = str(raw or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s


def _pos_bucket(pos_raw: str) -> str:
    p = str(pos_raw or "").strip().upper()
    if not p:
        return ""
    if p in ("QB",):
        return "QB"
    if p in ("RB", "FB"):
        return "RB"
    if p in ("TE",):
        return "TE"
    if p in ("WR",):
        return "WR"
    if p in ("DST", "DEF", "D", "D/ST"):
        return "DEF"
    if "LB" in p or "DE" in p or "DT" in p or "CB" in p or "S" in p or "DB" in p:
        return "DEF"
    return ""


def _position_group_from_row(norm_prop: str, pos_raw: str) -> str:
    bucket = _pos_bucket(pos_raw)
    if norm_prop == "interceptions" and bucket == "DEF":
        return "DEF"
    if norm_prop in PASSING_PROPS:
        return bucket if bucket == "QB" else ("QB" if not bucket else bucket)
    if norm_prop in RUSHING_PROPS:
        if bucket == "QB":
            return "QB"
        return "RB" if not bucket else bucket
    if norm_prop in RECEIVING_PROPS:
        if bucket in ("TE", "RB", "QB"):
            return bucket
        return "WR" if not bucket else bucket
    if norm_prop in SCORING_PROPS:
        return bucket or "UNK"
    if norm_prop in DEFENSE_PROPS:
        return "DEF"
    return bucket or "UNK"


def main() -> None:
    require_nfl_pipeline_active_or_exit()

    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="data/outputs/step1_pp_props_today.csv")
    ap.add_argument("--output", default="data/outputs/step2_clean_props.csv")
    args = ap.parse_args()

    in_path = Path(args.input)
    if not in_path.is_file():
        print(f"[NFL step2] Missing input: {in_path}")
        sys.exit(1)

    df = pd.read_csv(in_path, encoding="utf-8-sig")
    if df.empty:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"[NFL step2] Wrote empty {out}")
        return

    # NFLP (44) stays; NFLSZN season-long is not a game board.
    lid = df["league_id"].astype(str) if "league_id" in df.columns else pd.Series("", index=df.index)
    lg = df["league"].astype(str).str.upper() if "league" in df.columns else pd.Series("", index=df.index)
    szn = lid.eq("163") | lg.eq("NFLSZN")
    n_szn = int(szn.sum()) if len(df) else 0
    if n_szn:
        df = df.loc[~szn].reset_index(drop=True)
        print(f"[NFL step2] Dropped {n_szn} NFLSZN rows (NFL + NFLP game boards only)")

    if df.empty:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"[NFL step2] Wrote empty {out} after dropping NFLSZN")
        return

    prop_col = "prop_type" if "prop_type" in df.columns else ""
    if not prop_col:
        print("[NFL step2] prop_type column missing")
        sys.exit(1)

    df = df.copy()
    df["prop_type_normalized"] = df[prop_col].map(_norm_prop)
    if "pick_type" not in df.columns:
        print(
            "[NFL step2] FATAL: pick_type missing from step1 CSV — "
            "refusing to default everything to Standard (Week 1 hole)."
        )
        sys.exit(1)
    else:
        blank = df["pick_type"].isna() | df["pick_type"].astype(str).str.strip().isin(
            ["", "nan", "None", "NaN"]
        )
        if len(df) and int(blank.sum()) == len(df):
            print("[NFL step2] FATAL: pick_type is all empty — refusing to continue.")
            sys.exit(1)
        df["pick_type"] = (
            df["pick_type"]
            .astype(str)
            .str.strip()
            .replace({"": "Standard", "nan": "Standard", "None": "Standard"})
        )
        low = df["pick_type"].str.lower()
        df.loc[low.str.contains("gob", na=False), "pick_type"] = "Goblin"
        df.loc[low.str.contains("dem", na=False), "pick_type"] = "Demon"
        df.loc[~low.str.contains("gob|dem", na=False, regex=True), "pick_type"] = "Standard"
        vc = df["pick_type"].value_counts().to_dict()
        print(f"[NFL step2] pick_type: {vc}")
        if len(df) >= 100 and vc.get("Goblin", 0) == 0 and vc.get("Demon", 0) == 0:
            print(
                "[NFL step2] FATAL: board n>=100 but pick_type is 100% Standard — "
                "refusing to continue (Week 1 hole)."
            )
            sys.exit(1)
    pos_col = "pos" if "pos" in df.columns else None
    pos_series = df[pos_col] if pos_col else pd.Series([""] * len(df))
    df["position_group"] = [
        _position_group_from_row(p, str(pos_series.iloc[i]))
        for i, p in enumerate(df["prop_type_normalized"])
    ]
    # Period / first-N / half-quarter boards: gradeable after the game, but
    # boxscore L5 fuzzy-matches onto full-game yards/attempts. Mark here
    # (Tennis 1st-set pattern) so step5 skips fabricated hit rates.
    df = mark_football_unsupported_props(df, prop_col="prop_type_normalized")
    n_unsup = int(pd.to_numeric(df["unsupported_prop"], errors="coerce").fillna(0).sum())
    if n_unsup:
        print(f"[NFL step2] unsupported_prop={n_unsup} (period/first-N; no box L5)")
    df = fill_opp_team_from_game(df)
    espn_map: dict[str, str] = {}
    if "opp_team" in df.columns:
        blank = df["opp_team"].astype(str).str.strip().isin(["", "nan", "None", "NaN"])
        if blank.any():
            dates: list[str] = []
            if "start_time" in df.columns:
                dates = sorted({iso_to_et_date(x) for x in df["start_time"].tolist() if iso_to_et_date(x)})
            if "fetch_date" in df.columns:
                dates.extend(str(x)[:10] for x in df["fetch_date"].tolist() if str(x).strip())
            dates = [d for d in dict.fromkeys(dates) if d]
            if dates:
                try:
                    espn_map = espn_nfl_opp_map_for_dates(dates)
                except Exception as exc:
                    print(f"[NFL step2] ESPN opp map skipped: {exc}")
            if espn_map:
                df = fill_opp_team_from_game(df, espn_opp_map=espn_map)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"[NFL step2] Wrote {out_path} rows={len(df)}")


if __name__ == "__main__":
    main()
