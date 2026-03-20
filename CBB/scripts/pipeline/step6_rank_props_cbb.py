#!/usr/bin/env python3
"""
cbb_step6_rank_props.py  (v3 — under-direction fixes)
-------------------------------------------------------
Ranks CBB props using the same signal set as NBA step7:
  - Weighted projection blend: last5 (50%) + last10 (30%) + season (20%)
  - Direction-aware avg_vs_line signal
  - Blended hit rate (last5 50% + last10 50%)
  - Defense-adjusted edge  (divisor fixed for full D1 ranking scale)
  - Prop-type weight  (same table as NBA)
  - Bayesian prop hit-rate prior  (same table as NBA)
  - Reliability multiplier  (consistent with NBA)

Input : step5b_with_stats_cbb.csv  (or any step5b_cbb.csv)
Output: step6_ranked_props_cbb.xlsx + optional CSV
"""

from __future__ import annotations

import argparse
import math
from typing import Optional
from datetime import datetime

import numpy as np
import pandas as pd

# ── Head-to-Head (H2H) utility ────────────────────────────────────────────────
def _attach_h2h(df: "pd.DataFrame", cache_path: str, sport: str,
                player_col: str, opp_col: str, prop_col: str, line_col: str) -> "pd.DataFrame":
    """
    Attach H2H stats per row using the boxscore cache (step5b format).
    Cache columns: player_norm, opp_team_abbr, game_date, PTS, REB, AST, STL, BLK, TO, 3PM, MIN

    Adds columns:
      h2h_games      – number of H2H games found vs this opponent
      h2h_avg        – player's average stat value vs this opponent
      h2h_over_rate  – fraction of those games over the current line
      h2h_last       – most recent game value vs this opponent
    """
    import os

    df["h2h_games"]     = 0
    df["h2h_avg"]       = np.nan
    df["h2h_over_rate"] = np.nan
    df["h2h_last"]      = np.nan

    if not cache_path or not os.path.exists(cache_path):
        return df

    try:
        cache = pd.read_csv(cache_path, low_memory=False)
    except Exception:
        return df

    cache.columns = [c.lower().strip() for c in cache.columns]

    # Need player, opponent, and stat columns
    p_col = next((c for c in ["player_norm","player_name","player","name"] if c in cache.columns), None)
    o_col = next((c for c in ["opp_team_abbr","opp_team","opp","opponent"] if c in cache.columns), None)

    if not p_col or not o_col:
        print(f"  [H2H] Cache missing player ({p_col}) or opp ({o_col}) column — skipping")
        return df

    # Stat column map matching prop_value() logic
    stat_cols = {c: c for c in ["pts","reb","ast","stl","blk","to","3pm"] if c in cache.columns}
    if not stat_cols:
        print(f"  [H2H] Cache has no stat columns — skipping")
        return df

    def _norm(x):
        return str(x).strip().lower() if x and str(x).strip() else ""

    def _cache_prop_value(row, prop_norm: str):
        """Compute the stat value for a prop type from a cache row."""
        p = str(prop_norm).lower().strip()
        pts = float(row.get("pts") or 0)
        reb = float(row.get("reb") or 0)
        ast = float(row.get("ast") or 0)
        stl = float(row.get("stl") or 0)
        blk = float(row.get("blk") or 0)
        tov = float(row.get("to") or 0)
        tpm = row.get("3pm")
        tpm = float(tpm) if tpm not in (None, "", "nan") else None

        m = {"pts": pts, "reb": reb, "ast": ast, "stl": stl, "blk": blk,
             "tov": tov, "to": tov, "3pm": tpm, "fg3m": tpm,
             "stocks": stl + blk,
             "pra": pts + reb + ast, "pr": pts + reb,
             "pa": pts + ast, "ra": reb + ast,
             "fantasy": pts + 1.2*reb + 1.5*ast + 3*stl + 3*blk - tov}
        return m.get(p)

    # Build lookup: (player_norm, opp_norm) -> list of cache rows
    lookup: dict = {}
    for _, row in cache.iterrows():
        pk = (_norm(row.get(p_col, "")), _norm(row.get(o_col, "")))
        if pk[0] and pk[1]:
            lookup.setdefault(pk, []).append(row)

    matched = 0
    for idx, r in df.iterrows():
        player   = _norm(r.get(player_col, ""))
        opp      = _norm(r.get(opp_col, ""))
        prop     = str(r.get(prop_col, "")).lower().strip()
        line_val = r.get(line_col, None)
        try:
            line_f = float(line_val)
        except (TypeError, ValueError):
            line_f = None

        entries = lookup.get((player, opp), [])
        if not entries:
            continue

        # Sort by date desc, take up to 10
        try:
            entries_sorted = sorted(entries, key=lambda x: str(x.get("game_date", "")), reverse=True)[:10]
        except Exception:
            entries_sorted = entries[:10]

        vals = [v for e in entries_sorted
                if (v := _cache_prop_value(e, prop)) is not None
                and float(e.get("min") or e.get("MIN") or 0) > 0]

        if not vals:
            continue

        matched += 1
        avg  = round(float(np.mean(vals)), 2)
        last = round(float(vals[0]), 2)
        over_rate = (round(sum(1 for v in vals if line_f is not None and v > line_f) / len(vals), 3)
                     if line_f is not None else np.nan)

        df.at[idx, "h2h_games"]     = len(vals)
        df.at[idx, "h2h_avg"]       = avg
        df.at[idx, "h2h_over_rate"] = over_rate
        df.at[idx, "h2h_last"]      = last

    print(f"  H2H: {matched}/{len(df)} rows matched")
    return df
# ─────────────────────────────────────────────────────────────────────────────



def _to_num(s):
    return pd.to_numeric(s, errors="coerce")


def _norm_pick_type(x: str) -> str:
    t = str(x or "").strip().lower()
    if "gob" in t: return "Goblin"
    if "dem" in t: return "Demon"
    return "Standard"


def _forced_over(pick_type: str) -> int:
    return 1 if _norm_pick_type(pick_type) in ("Goblin", "Demon") else 0


# ── Prop weights (same as NBA step7) ─────────────────────────────────────────
_PROP_WEIGHTS = {
    "pts":   1.03, "reb":   1.06, "ast":   1.05,
    "stl":   1.08, "blk":   1.02, "stocks": 1.04,
    "fg3m":  1.03, "fg3a":  0.88, "fg2m":  1.01,
    "fg2a":  0.92, "fgm":   0.99, "fga":   0.99,
    "ftm":   1.01, "fta":   0.98, "tov":   0.94,
    "pf":    0.85, "pr":    1.01, "pa":    1.01,
    "pra":   0.99, "ra":    1.02, "fantasy": 1.00,
}

def _prop_weight(prop_norm: str) -> float:
    return float(_PROP_WEIGHTS.get(str(prop_norm).lower().strip(), 0.93))


# ── Bayesian prior hit rates (same as NBA step7) ──────────────────────────────
_PROP_HIT_RATE_PRIOR = {
    "stl": 0.697, "fantasy": 0.674,
    "fg3m": 0.623, "reb": 0.617, "ast": 0.593,
    "ftm": 0.583, "pr": 0.568,  "pts": 0.566,
    "stocks": 0.547, "blk": 0.545, "pra": 0.545,
    "fga": 0.558, "pa": 0.557,  "fgm": 0.519,
    "fg2m": 0.528, "fg2a": 0.463, "tov": 0.484,
    "fg3a": 0.444, "pf": 0.424,  "fta": 0.545,
}

def _prop_hr_prior(prop_norm: str, direction: str) -> float:
    key = str(prop_norm).lower().strip()
    base = _PROP_HIT_RATE_PRIOR.get(key, 0.545)
    if direction == "UNDER":
        if key == "fantasy":     return 0.371
        if key in ("fga","fg2a"): return 0.645
        if key == "reb":          return 0.591
        if key in ("pts","pr","pra"): return 0.540
        return float(1.0 - base)
    return float(base)


def _reliability_mult(pick_type: str) -> float:
    """Consistent with NBA step7: Goblin lines are easier so slight boost,
    Demon lines are harder so penalty."""
    return {"Standard": 1.00, "Goblin": 1.06, "Demon": 0.75}.get(
        _norm_pick_type(pick_type), 0.97
    )



def _edge_transform(edge: float, cap=3.0, power=0.85) -> float:
    if np.isnan(edge): return np.nan
    s = 1.0 if edge >= 0 else -1.0
    return s * (min(abs(edge), cap) ** power)


def _tier(score: float, eligible_scores=None) -> str:
    """Assign tier based on rank_score.
    Thresholds calibrated to actual CBB score distribution:
      scores range ~-1.2 to +1.6, median ~-0.42
      A = top ~5%  (score >= 0.96)
      B = top ~10% (score >= 0.68)
      C = top ~25% (score >= 0.13)
      D = everything else
    """
    if np.isnan(score): return "D"
    if score >= 0.96:  return "A"
    if score >= 0.68:  return "B"
    if score >= 0.13:  return "C"
    return "D"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",      required=True)
    ap.add_argument("--output",     default="step6_ranked_props_cbb.xlsx")
    ap.add_argument("--output_csv", default="")
    ap.add_argument("--cache", default="cbb_boxscore_cache.csv", help="Path to CBB boxscore cache CSV")
    ap.add_argument("--date", default="", help="Filter to YYYY-MM-DD using start_time")
    args = ap.parse_args()

    df = pd.read_csv(args.input, dtype=str).fillna("")
    print(f"→ Loaded: {args.input} | rows={len(df)}")

    if "start_time" in df.columns:
        target_date = (args.date or datetime.now().strftime("%Y-%m-%d")).strip()
        start_dt = pd.to_datetime(df["start_time"], errors="coerce")
        keep_mask = start_dt.dt.strftime("%Y-%m-%d").eq(target_date)
        kept = int(keep_mask.sum())
        total = len(df)
        df = df.loc[keep_mask].copy()
        print(f"[DateFilter] Kept {kept}/{total} rows for {target_date} (dropped {total - kept} rows)")
        if df.empty:
            print("⚠️ Date filter returned no rows; writing empty outputs.")

    # Only rank OK rows
    ok = df["stat_status"].astype(str).str.upper().eq("OK") if "stat_status" in df.columns else \
         df.get("status3", pd.Series([""] * len(df))).astype(str).str.upper().eq("OK")

    out = df.copy()

    line_num = _to_num(out["line"])

    # ── Projection: weighted blend last5/last10/season ──────────────────────
    l5  = _to_num(out.get("stat_last5_avg",  pd.Series([""] * len(out))))
    l10 = _to_num(out.get("stat_last10_avg", pd.Series([""] * len(out))))
    ssn = _to_num(out.get("stat_season_avg", pd.Series([""] * len(out))))

    def blend_proj(row_idx):
        weights = [(l5.iloc[row_idx], 0.50), (l10.iloc[row_idx], 0.30), (ssn.iloc[row_idx], 0.20)]
        tv = tw = 0.0
        for v, w in weights:
            if not np.isnan(v): tv += v * w; tw += w
        return tv / tw if tw >= 0.1 else np.nan

    proj = pd.Series([blend_proj(i) for i in range(len(out))], index=out.index)
    out["projection"] = proj

    # ── Edge ────────────────────────────────────────────────────────────────
    out["edge"]     = proj - line_num
    out["abs_edge"] = out["edge"].abs()

    # ── Direction / eligibility ──────────────────────────────────────────────
    pick_type = out.get("pick_type", pd.Series(["Standard"] * len(out))).astype(str)
    forced    = pick_type.apply(_forced_over).astype(int)
    out["forced_over_only"] = forced

    bet_dir = np.where(forced.eq(1), "OVER",
              np.where(out["edge"] >= 0, "OVER", "UNDER"))
    out["bet_direction"] = bet_dir

    eligible   = pd.Series(True,  index=out.index)
    void_reason= pd.Series("",    index=out.index)

    miss = line_num.isna() | proj.isna()
    eligible.loc[miss]   = False
    void_reason.loc[miss] = "NO_PROJECTION_OR_LINE"

    # Drop Demon entirely + neg-edge Goblin to audit sheet (not eligible)
    is_demon     = pick_type.apply(lambda x: _norm_pick_type(x) == "Demon")
    goblin_neg   = pick_type.apply(lambda x: _norm_pick_type(x) == "Goblin") & (out.get("edge", pd.Series(0.0, index=out.index)).pipe(lambda s: pd.to_numeric(s, errors="coerce")).fillna(0) < 0)
    drop_mask    = is_demon | goblin_neg
    eligible.loc[drop_mask]    = False
    void_reason.loc[is_demon]  = "DROPPED_DEMON_AUDIT"
    void_reason.loc[goblin_neg & ~is_demon] = "DROPPED_NEG_EDGE_GOBDEM"

    # also mark non-OK rows ineligible
    eligible.loc[~ok] = False
    void_reason.loc[~ok & void_reason.eq("")] = "STAT_NOT_OK"

    out["eligible"]    = eligible.astype(int)
    out["void_reason"] = void_reason

    elig_mask = eligible

    # ── Defense adjustment (CBB: D1 has ~362 teams, NOT 30) ─────────────────
    # Try multiple possible column names for defense rank
    def_rank_col = next((c for c in ["OVERALL_DEF_RANK","OPP_OVERALL_DEF_RANK","opp_def_rank"] if c in out.columns), "")
    if def_rank_col:
        def_rank_num = _to_num(out[def_rank_col])
        # Auto-detect scale: if max rank > 40, assume full D1 (~362 teams)
        max_rank = def_rank_num.max()
        n_teams  = 362.0 if max_rank > 40 else 30.0
        mid_rank = (n_teams + 1.0) / 2.0
        # Derive def_tier from rank if not already set
        def _rank_to_tier(r):
            try:
                r = float(r)
                if r <= 72:    return "Elite"
                elif r <= 144: return "Above Avg"
                elif r <= 252: return "Avg"
                else:          return "Weak"
            except (TypeError, ValueError):
                return ""
        _dt = out["def_tier"] if "def_tier" in out.columns else pd.Series([], dtype=str)
        _dt_empty = _dt.isna() | (_dt.astype(str).str.strip() == "")
        if "def_tier" not in out.columns or _dt_empty.all():
            out["def_tier"]     = def_rank_num.apply(_rank_to_tier)
            out["opp_def_tier"] = out["def_tier"]
    else:
        def_rank_num = pd.Series([np.nan] * len(out), index=out.index)
        n_teams  = 362.0
        mid_rank = 181.5

    def _def_adj(row_idx):
        rank = def_rank_num.iloc[row_idx]
        if np.isnan(rank): return 0.0
        # Scale: best defense (rank=1) gives -6% boost to opposing scorer,
        # worst defense (rank=n_teams) gives +6% boost
        return float((rank - mid_rank) / mid_rank * 0.06)

    def_adj = pd.Series([_def_adj(i) for i in range(len(out))], index=out.index)
    out["def_adj"] = def_adj

    proj_adj = proj * (1 + def_adj)
    out["projection_adj"] = proj_adj
    out["edge_adj"]       = proj_adj - line_num
    # For UNDERs, a negative edge_adj is actually favourable — flip sign so the
    # score contribution is positive when projection < line (correct UNDER direction).
    def _edge_adj_dr_directional(row_idx):
        x = out["edge_adj"].iloc[row_idx]
        if isinstance(x, float) and np.isnan(x):
            return np.nan
        direction = str(out["bet_direction"].iloc[row_idx]).upper()
        signed = -float(x) if direction == "UNDER" else float(x)
        return _edge_transform(signed)

    out["edge_adj_dr"] = pd.Series(
        [_edge_adj_dr_directional(i) for i in range(len(out))], index=out.index
    )

    def _def_signal(row_idx):
        rank = def_rank_num.iloc[row_idx]
        direction = str(out["bet_direction"].iloc[row_idx]).upper()
        if np.isnan(rank): return 0.0
        # Normalize to [-1, +1] using full D1 scale
        signal = (rank - 1.0) / (n_teams - 1.0) * 2.0 - 1.0
        return float(signal if direction == "OVER" else -signal)

    def_signal = pd.Series([_def_signal(i) for i in range(len(out))], index=out.index)
    out["def_rank_signal"] = def_signal

    # ── Hit rate: blend last5 + last10 (direction-aware) ────────────────────
    # Pre-load both OVER and UNDER columns so we can pick the right one per row
    hr_over5   = _to_num(out.get("line_hit_rate_over_ou_5",  pd.Series([np.nan]*len(out))))
    hr_over10  = _to_num(out.get("line_hit_rate_over_ou_10", pd.Series([np.nan]*len(out))))
    hr_under5  = _to_num(out.get("line_hit_rate_under_ou_5",  pd.Series([np.nan]*len(out))))
    hr_under10 = _to_num(out.get("line_hit_rate_under_ou_10", pd.Series([np.nan]*len(out))))

    def blend_hr(row_idx):
        direction = str(out["bet_direction"].iloc[row_idx]).upper()
        if direction == "UNDER":
            h5  = hr_under5.iloc[row_idx]
            h10 = hr_under10.iloc[row_idx]
            # Fallback: derive UNDER rate as 1 - OVER rate if UNDER columns missing
            if np.isnan(h5) and not np.isnan(hr_over5.iloc[row_idx]):
                h5  = 1.0 - hr_over5.iloc[row_idx]
            if np.isnan(h10) and not np.isnan(hr_over10.iloc[row_idx]):
                h10 = 1.0 - hr_over10.iloc[row_idx]
        else:
            h5  = hr_over5.iloc[row_idx]
            h10 = hr_over10.iloc[row_idx]
        if not np.isnan(h5) and not np.isnan(h10): return h5 * 0.50 + h10 * 0.50
        if not np.isnan(h5):  return h5
        if not np.isnan(h10): return h10
        return np.nan

    line_hit_rate = pd.Series([blend_hr(i) for i in range(len(out))], index=out.index)
    out["line_hit_rate"] = line_hit_rate

    # ── Avg vs line (direction-aware) ────────────────────────────────────────
    for col in ("stat_last5_avg","stat_last10_avg","stat_season_avg"):
        out[f"_{col}_n"] = _to_num(out.get(col, pd.Series([""] * len(out))))

    line_filled = line_num.fillna(0)

    def _avg_vs_line(row_idx):
        ln = line_filled.iloc[row_idx]
        if ln == 0 or np.isnan(ln): return 0.0
        direction = str(out["bet_direction"].iloc[row_idx]).upper()
        score = tw = 0.0
        for col, w in [("_stat_last5_avg_n",0.50),("_stat_last10_avg_n",0.30),("_stat_season_avg_n",0.20)]:
            v = out[col].iloc[row_idx]
            if not np.isnan(v):
                raw = np.clip((v - ln) / ln, -1.0, 1.0)
                score += (-raw if direction == "UNDER" else raw) * w
                tw += w
        return float(score / tw) if tw > 0.1 else 0.0

    avg_vs_line = pd.Series([_avg_vs_line(i) for i in range(len(out))], index=out.index)
    out["avg_vs_line"] = avg_vs_line

    # ── Composite score (mirrors NBA step7) ─────────────────────────────────
    prop_norm_col = out.get("prop_norm", out.get("prop_type", pd.Series([""] * len(out)))).astype(str)
    prop_w   = prop_norm_col.apply(_prop_weight)
    rel_mult = pick_type.apply(_reliability_mult)

    hr_signal = (line_hit_rate - 0.5) * 2.0   # centre on 0, range ~[-1, +1]

    def _prior_signal(row_idx):
        pn  = str(prop_norm_col.iloc[row_idx])
        bd  = str(out["bet_direction"].iloc[row_idx]).upper()
        hr  = line_hit_rate.iloc[row_idx]
        pri = _prop_hr_prior(pn, bd)
        if np.isnan(hr): return float((pri - 0.5) * 2.0)
        return float(((hr + pri) / 2.0 - 0.5) * 2.0)

    prior_signal = pd.Series([_prior_signal(i) for i in range(len(out))], index=out.index)

    # Weighted composite
    raw_score = (
        out["edge_adj_dr"].fillna(0)  * 0.35
        + avg_vs_line                  * 0.20
        + def_signal                   * 0.15
        + hr_signal.fillna(0)          * 0.15
        + prior_signal                 * 0.15
    ) * prop_w * rel_mult

    # Zero out ineligible rows
    score = raw_score.where(elig_mask, other=np.nan)

    out["rank_score"] = score
    out["tier"]       = out["rank_score"].apply(
        lambda x: _tier(x) if not (isinstance(x, float) and np.isnan(x)) else "D")

    # ── Final bet direction (step8-style logic inline) ────────────────────────
    final_dir = np.where(forced.eq(1), "OVER",
                np.where(out["edge"] >= 0, "OVER", "UNDER"))
    out["final_bet_direction"] = final_dir

    # ── Clean up temp columns ─────────────────────────────────────────────────
    drop_cols = [c for c in out.columns if c.startswith("_stat_")]
    # Remove always-blank ESPN ID columns (populated by step5 which is not part of CBB pipeline)
    drop_cols += [c for c in ("team_id", "espn_athlete_id", "attach_status") if c in out.columns]
    out.drop(columns=drop_cols, inplace=True)

    # ── Sort ──────────────────────────────────────────────────────────────────
    drop_mask_final = out["void_reason"].isin(["DROPPED_DEMON_AUDIT", "DROPPED_NEG_EDGE_GOBDEM"])
    dropped_df  = out[drop_mask_final].copy()
    out_active  = out[~drop_mask_final].copy()
    out_sorted  = out_active.sort_values("rank_score", ascending=False, na_position="last")
    elig_sorted = elig_mask.reindex(out_sorted.index).fillna(False)

    # ── Head-to-Head stats ───────────────────────────────────────────────────
    player_col = next((c for c in ["player_norm","player","pp_player","player_name"] if c in out.columns), "")
    opp_col    = next((c for c in ["pp_opp_team","opp_team_abbr","opp_team","opp"] if c in out.columns), "")
    prop_col   = next((c for c in ["prop_norm","prop_type"] if c in out.columns), "prop_norm")
    if player_col and opp_col:
        out = _attach_h2h(out, args.cache, "cbb", player_col, opp_col, prop_col, "line")
        print(f"  H2H: {(out['h2h_games'] > 0).sum()}/{len(out)} rows matched")

    # ── Write Excel ───────────────────────────────────────────────────────────
    with pd.ExcelWriter(args.output, engine="openpyxl") as xw:
        out_sorted.to_excel(xw, index=False, sheet_name="ALL")
        out_sorted[elig_sorted].to_excel(xw, index=False, sheet_name="ELIGIBLE")
        for t in ["A","B","C","D"]:
            sub = out_sorted[out_sorted["tier"] == t]
            if len(sub): sub.to_excel(xw, index=False, sheet_name=f"TIER_{t}")
        if not dropped_df.empty:
            dropped_df.to_excel(xw, index=False, sheet_name="DROPPED")

    print(f"✅ Saved → {args.output}")
    print(f"ALL rows (active) : {len(out_sorted)}")
    print(f"DROPPED rows      : {len(dropped_df)}  (Demon + neg-edge Goblin, audit only)")
    print("Tier breakdown:")
    print(out_sorted["tier"].value_counts().to_string())
    print("\nVoid reasons (active):")
    vr = out_sorted.loc[~elig_sorted, "void_reason"].value_counts()
    print(vr.to_string() if len(vr) else "(none)")

    if args.output_csv:
        out_sorted.to_csv(args.output_csv, index=False)
        print(f"✅ Saved CSV → {args.output_csv}")


if __name__ == "__main__":
    main()
