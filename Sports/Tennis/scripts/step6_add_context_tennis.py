#!/usr/bin/env python3
"""Tennis step6 — surface, round, ranking_diff, is_grand_slam, roles for step7/8."""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
# Sports/Tennis/scripts → PropORACLE
_REPO_ROOT = _SCRIPT_DIR.parents[2]
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))
from tennis_shared import (
    build_sackmann_player_index,
    ensure_opponent_atp_wta_rank,
    ensure_sackmann_matches,
    hydrate_rankings_from_slate,
    load_match_games_cache,
    load_or_refresh_rankings,
    norm_key,
    is_doubles_pair,
    split_pair,
    resolve_opp_rank_pair,
)
from utils.tennis_load_tier import assign_tennis_load_tier


ROUND_RE = re.compile(r"\b(R128|R64|R32|R16|QF|SF|F)\b", re.I)
GRAND_SLAMS = (
    "australian open",
    "roland garros",
    "french open",
    "wimbledon",
    "us open",
)


def infer_surface(team_or_tourn: str) -> str:
    t = str(team_or_tourn or "").lower()
    if any(x in t for x in ("roland", "monte carlo", "barcelona", "madrid", "rome", "clay")):
        return "clay"
    if any(x in t for x in ("wimbledon", "queen", "halle", "grass")):
        return "grass"
    return "hard"


def infer_round(row: pd.Series) -> str:
    blob = f"{row.get('prop_type','')} {row.get('team','')} {row.get('tournament','')}"
    m = ROUND_RE.search(blob)
    return m.group(1).upper() if m else ""


def main() -> None:
    print("[Tennis step6] Starting...")
    root = _SCRIPT_DIR.parent
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="outputs/step5_tennis_hit_rates.csv")
    ap.add_argument("--output", default="outputs/step6_tennis_role_context.csv")
    ap.add_argument("--rankings-cache", default="cache/tennis_rankings.json")
    ap.add_argument("--match-cache", default="cache/tennis_match_games.json")
    args = ap.parse_args()

    inp = Path(args.input)
    if not inp.is_absolute():
        inp = root / inp
    out = Path(args.output)
    if not out.is_absolute():
        out = root / out

    df = pd.read_csv(inp, dtype=str, encoding="utf-8-sig").fillna("")
    if df.empty:
        print("ERROR [Tennis step6] empty input")
        sys.exit(1)

    team_src = df["team"].astype(str) if "team" in df.columns else pd.Series([""] * len(df))
    tourn = df["tournament"].astype(str) if "tournament" in df.columns else pd.Series([""] * len(df))
    surf_src = (tourn.where(tourn.str.len() > 2, team_src)).fillna("")
    df["surface"] = [infer_surface(surf_src.iat[i]) for i in range(len(df))]
    df["round"] = [infer_round(df.iloc[i]) for i in range(len(df))]

    blob_gs = (surf_src + " " + team_src).str.lower()
    df["is_grand_slam"] = blob_gs.apply(lambda s: int(any(g in s for g in GRAND_SLAMS)))

    rpath = Path(args.rankings_cache)
    if not rpath.is_absolute():
        rpath = root / rpath
    rankings = load_or_refresh_rankings(rpath)
    rankings = hydrate_rankings_from_slate(df, rankings, cache_path=rpath.parent / "tennis_opp_rank_cache.json")
    by_id = {str(r["espn_athlete_id"]): float(r.get("rank") or 999) for r in rankings}
    by_pk = {str(r.get("player_key") or ""): float(r.get("rank") or 999) for r in rankings if r.get("player_key")}

    pranks = []
    for _, r in df.iterrows():
        aid = str(r.get("espn_athlete_id", "")).strip()
        if aid and aid in by_id:
            pranks.append(by_id[aid])
        elif is_doubles_pair(str(r.get("player", ""))):
            best = 999.0
            for part in split_pair(str(r.get("player", ""))):
                pk = norm_key(part)
                best = min(best, by_pk.get(pk, 999.0))
            pranks.append(best)
        else:
            pk = norm_key(str(r.get("player", "")))
            pranks.append(by_pk.get(pk, 999.0))
    df["player_atp_rank"] = pranks

    opp_col = "opp_team" if "opp_team" in df.columns else "opp"
    oranks = []
    for i in range(len(df)):
        v = resolve_opp_rank_pair(str(df.iloc[i].get(opp_col, "")), rankings)
        oranks.append(np.nan if v is None else v)
    df["opponent_rank"] = oranks
    # opp_team is often blank until game pairing; backfill then map ATP/WTA rank.
    df = ensure_opponent_atp_wta_rank(df)
    n_filled = int(pd.to_numeric(df["opponent_rank"], errors="coerce").notna().sum())
    print(f"[Tennis step6] opponent_rank (ATP/WTA) filled {n_filled}/{len(df)}")
    df["ranking_diff"] = pd.to_numeric(df["player_atp_rank"], errors="coerce") - pd.to_numeric(
        df["opponent_rank"], errors="coerce"
    )

    if "best_surface" not in df.columns or df["best_surface"].astype(str).str.len().max() < 2:
        df["best_surface"] = ""

    df["position_group"] = df["tour"].astype(str).str.upper()
    df["shot_role"] = np.where(df["prop_norm"].astype(str).str.contains("ace"), "SERVE_HEAVY", "NEUTRAL")
    df["usage_role"] = np.where(pd.to_numeric(df["player_atp_rank"], errors="coerce") <= 32, "ELITE", "FIELD")

    # Match-load / fatigue soft: Sackmann minutes (+ games/sets/rest). Missing → UNKNOWN.
    by_pk: dict = {}
    by_espn: dict = {}
    hand_by: dict[str, str] = {}
    try:
        sack = ensure_sackmann_matches()
        if sack is not None and not sack.empty:
            by_pk = build_sackmann_player_index(sack)
            for _, rd in sack.iterrows():
                for name_col, hand_col in (
                    ("winner_name", "winner_hand"),
                    ("loser_name", "loser_hand"),
                ):
                    nm = norm_key(str(rd.get(name_col) or ""))
                    hd = str(rd.get(hand_col) or "").strip().upper()
                    if nm and hd in {"L", "R", "U"}:
                        hand_by[nm] = hd
            print(f"[Tennis step6] Sackmann load index players={len(by_pk)} hands={len(hand_by)}")
    except Exception as exc:
        print(f"[Tennis step6] WARN Sackmann load index skipped: {exc}")

    mpath = Path(args.match_cache)
    if not mpath.is_absolute():
        mpath = root / mpath
    try:
        espn_cache = load_match_games_cache(mpath)
        # ESPN rows lack minutes; still useful for games/sets/date when Sackmann miss.
        by_espn = {str(k): list(v) for k, v in (espn_cache or {}).items() if v}
        print(f"[Tennis step6] ESPN match cache athletes={len(by_espn)}")
    except Exception as exc:
        print(f"[Tennis step6] WARN ESPN match cache skipped: {exc}")

    df = assign_tennis_load_tier(
        df,
        history_by_player=by_pk,
        history_by_espn_id=by_espn,
        player_col="player" if "player" in df.columns else "Player",
        espn_col="espn_athlete_id",
        player_key_fn=norm_key,
    )
    mt_counts = df["minutes_tier"].astype(str).value_counts().to_dict()
    print(f"[Tennis step6] minutes_tier (load): {mt_counts}")

    # Keep-gate factors: today's opp hand + L5 2nd-serve points won %.
    opp_name_col = "opp_team" if "opp_team" in df.columns else ("opp" if "opp" in df.columns else None)
    opp_hands: list[str] = []
    opp_lefty: list[str] = []
    sw_l5: list[object] = []
    for i in range(len(df)):
        row = df.iloc[i]
        opp_raw = str(row.get(opp_name_col, "") if opp_name_col else "")
        oh = ""
        if opp_raw and not is_doubles_pair(opp_raw):
            oh = hand_by.get(norm_key(opp_raw), "")
        elif opp_raw and is_doubles_pair(opp_raw):
            hands = [hand_by.get(norm_key(p), "") for p in split_pair(opp_raw)]
            if "L" in hands:
                oh = "L"
            elif any(h in {"R", "U"} for h in hands):
                oh = next(h for h in hands if h in {"R", "U"})
        if not oh:
            hist = by_pk.get(norm_key(str(row.get("player", "")))) or []
            if hist:
                oh = str(hist[0].get("opp_hand") or "").strip().upper()
                if oh not in {"L", "R", "U"}:
                    oh = ""
        opp_hands.append(oh)
        opp_lefty.append("Y" if oh == "L" else ("N" if oh in {"R", "U"} else ""))

        vals: list[float] = []
        for rec in (by_pk.get(norm_key(str(row.get("player", "")))) or [])[:5]:
            try:
                v = float(rec.get("second_won_pct"))
            except (TypeError, ValueError):
                continue
            if v == v:
                vals.append(v)
        sw_l5.append(round(sum(vals) / len(vals), 2) if vals else "")

    df["opponent_hand"] = opp_hands
    df["opp_lefty"] = opp_lefty
    df["l5_second_won_pct"] = sw_l5
    n_lefty = int((df["opp_lefty"] == "Y").sum())
    n_sw = int(pd.to_numeric(df["l5_second_won_pct"], errors="coerce").notna().sum())
    print(f"[Tennis step6] opp_lefty Y={n_lefty}/{len(df)}  l5_second_won_pct filled={n_sw}/{len(df)}")

    o5 = pd.to_numeric(df.get("line_hits_over_5", np.nan), errors="coerce")
    u5 = pd.to_numeric(df.get("line_hits_under_5", np.nan), errors="coerce")
    df["last5_over"] = o5
    df["last5_under"] = u5

    df["game_script_mult"] = 1.0
    df["game_script_note"] = ""

    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"OK [Tennis step6] -> {out}  rows={len(df)}")


if __name__ == "__main__":
    main()
