#!/usr/bin/env python3
"""Build Sports/NFL/data/nfl_leg_training_rows.csv from graded W1..latest.

One row per unique player-game x prop x side x pick_type.
Skip VOID / NO_ACTUAL / DNP / Demon / PUSH. Dedupe ladder rungs.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(_REPO)]

from utils.defense_tiers import def_tier_from_overall_rank  # noqa: E402
from utils.nfl_prop_defense import (  # noqa: E402
    RECEIVING_PROPS,
    prop_def_axis,
)

OUT_CSV = _REPO / "Sports" / "NFL" / "data" / "nfl_leg_training_rows.csv"
SNAP_PATH = _REPO / "Sports" / "NFL" / "data" / "nfl_snap_pct_cache.json"
WEEK1_THU = date(2026, 9, 3)  # 2026 RS week-1 Thursday


def _norm_prop(raw: object) -> str:
    s = str(raw or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_")


def _fold_player(raw: object) -> str:
    return re.sub(r"\s+", " ", str(raw or "").lower().replace(".", "").strip())


def nfl_week(d: date) -> int:
    if d < WEEK1_THU:
        return 0
    return (d - WEEK1_THU).days // 7 + 1


def prop_family(prop: object) -> str:
    p = _norm_prop(prop)
    if p in RECEIVING_PROPS or "receiv" in p or p in {"rec", "receptions", "targets"} or "rec_yd" in p:
        return "receiving"
    axis = prop_def_axis(prop)
    if axis == "rush":
        return "rush"
    if axis == "kick":
        return "kicking"
    if axis == "opp_rush_off":
        return "tackles"
    return "pass"


def _graded_roots() -> list[Path]:
    # Prefer main_cp (full nightly grades) over the thinner Cursor checkout.
    roots = []
    for r in (Path(r"H:/PropORACLE_main_cp"), _REPO.parent / "PropORACLE_main_cp", _REPO):
        if r.exists() and (r / "outputs").exists():
            roots.append(r)
    out: list[Path] = []
    seen: set[str] = set()
    for r in roots:
        key = str(r.resolve()).lower()
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def _load_graded(max_week: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    seen_dates: set[str] = set()
    for root in _graded_roots():
        for p in sorted((root / "outputs").glob("*/graded_nfl_*.xlsx")):
            d = p.parent.name
            try:
                dd = date.fromisoformat(d)
            except ValueError:
                continue
            wk = nfl_week(dd)
            if wk < 1 or wk > max_week:
                continue
            if d in seen_dates:
                continue
            try:
                df = pd.read_excel(p, sheet_name="Box Raw")
            except Exception as e:
                print(f"skip {p}: {e}")
                continue
            if df.empty:
                continue
            df = df.copy()
            df["slate_date"] = d
            df["week"] = wk
            df["source_root"] = str(root)
            frames.append(df)
            seen_dates.add(d)
            print(f"graded {d} W{wk} rows={len(df)} from {root.name}")
    if not frames:
        raise SystemExit("No graded NFL rows found for W1..max_week")
    return pd.concat(frames, ignore_index=True)


def _load_step8_index() -> dict[str, pd.DataFrame]:
    """slate_date -> step8 frame (best available)."""
    by: dict[str, pd.DataFrame] = {}
    paths: list[Path] = []
    for root in _graded_roots():
        paths.extend(root.rglob("step8_nfl_direction_clean*.parquet"))
        paths.extend(root.rglob("step8_nfl_direction_clean*.xlsx"))
    # prefer parquet, then dated files
    for p in sorted(paths, key=lambda x: (0 if x.suffix == ".parquet" else 1, str(x))):
        # date from name or parent
        m = re.search(r"(20\d{2}-\d{2}-\d{2})", p.name) or re.search(
            r"(20\d{2}-\d{2}-\d{2})", str(p.parent)
        )
        if not m:
            continue
        d = m.group(1)
        if d in by:
            continue
        try:
            if p.suffix == ".parquet":
                df = pd.read_parquet(p)
            else:
                df = pd.read_excel(p)
        except Exception as e:
            print(f"step8 skip {p}: {e}")
            continue
        by[d] = df
        print(f"step8 {d} rows={len(df)} ({p.name})")
    return by


def _load_snap() -> dict[str, float]:
    if not SNAP_PATH.exists():
        return {}
    obj = json.loads(SNAP_PATH.read_text(encoding="utf-8"))
    players = obj.get("players") or {}
    out: dict[str, float] = {}
    for k, v in players.items():
        if not isinstance(v, dict):
            continue
        name = _fold_player(v.get("player") or k)
        try:
            snap = float(v.get("snap_pct_L3"))
        except (TypeError, ValueError):
            continue
        if name:
            out[name] = snap
    return out


def _load_pace_by_team_date() -> dict[tuple[str, str], str]:
    """(team, slate_date) -> pace_tier from game_context caches."""
    out: dict[tuple[str, str], str] = {}
    for p in sorted((_REPO / "Sports" / "NFL" / "data").glob("game_context_cache_*.csv")):
        m = re.search(r"(20\d{2}-\d{2}-\d{2})", p.name)
        if not m:
            continue
        d = m.group(1)
        try:
            df = pd.read_csv(p)
        except Exception:
            continue
        for _, r in df.iterrows():
            team = str(r.get("team") or "").strip().upper()
            tier = str(r.get("pace_tier") or "").strip()
            if team and tier:
                out[(team, d)] = tier
    return out


def _col(df: pd.DataFrame, *names: str) -> str | None:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for n in names:
        if n.lower() in lower:
            return lower[n.lower()]
    return None


def _tier_from_rank(rank: object) -> str:
    return def_tier_from_overall_rank(rank, 32, na_label="")


def _dir_hits(row: pd.Series, side: str, which: str) -> float:
    side_u = side.upper()
    if which == "l5":
        over_k, under_k = "l5_over", "l5_under"
    else:
        over_k, under_k = "l10_over", "l10_under"
    key = over_k if side_u == "OVER" else under_k
    # also Title Case from step8 merge
    for k in (key, key.replace("_", " "), "L5 Over" if which == "l5" and side_u == "OVER" else None):
        if k and k in row.index and pd.notna(row.get(k)):
            try:
                return float(row.get(k))
            except (TypeError, ValueError):
                pass
    # graded aliases
    for k in row.index:
        kl = str(k).lower().replace(" ", "_")
        if which == "l5" and side_u == "OVER" and kl in {"l5_over", "l5over"}:
            try:
                return float(row.get(k))
            except (TypeError, ValueError):
                return float("nan")
        if which == "l5" and side_u == "UNDER" and kl in {"l5_under", "l5under"}:
            try:
                return float(row.get(k))
            except (TypeError, ValueError):
                return float("nan")
        if which == "l10" and side_u == "OVER" and kl in {"l10_over", "l10over"}:
            try:
                return float(row.get(k))
            except (TypeError, ValueError):
                return float("nan")
        if which == "l10" and side_u == "UNDER" and kl in {"l10_under", "l10under"}:
            try:
                return float(row.get(k))
            except (TypeError, ValueError):
                return float("nan")
    return float("nan")


def _stat_window(row: pd.Series, n: int = 5) -> list[float]:
    vals: list[float] = []
    for i in range(1, n + 1):
        for key in (f"Stat G{i}", f"stat_g{i}", f"StatG{i}"):
            if key in row.index and pd.notna(row.get(key)):
                try:
                    vals.append(float(row.get(key)))
                except (TypeError, ValueError):
                    pass
                break
    return vals


def _cover_min_from_window(vals: list[float], line: float, side: str) -> tuple[float, float]:
    if not vals or not math.isfinite(line) or abs(line) < 1e-9:
        return float("nan"), float("nan")
    side_u = side.upper()
    mean_v = float(np.mean(vals))
    if side_u == "OVER":
        cover = mean_v - line
        margins = [v - line for v in vals]
    else:
        cover = line - mean_v
        margins = [line - v for v in vals]
    return cover / line, (min(margins) / line if margins else float("nan"))


def _pick_d_tier(row: pd.Series, family: str) -> str:
    # Prefer explicit Def Tier when axis matches; else rank→tier on correct axis.
    def_tier = str(row.get("def_tier") or row.get("Def Tier") or "").strip()
    axis = str(row.get("Def Axis") or row.get("def_axis") or "").strip().lower()
    if family == "rush":
        rank = row.get("Opp Rush Def Rank", row.get("opp_rush_def_rank"))
        return _tier_from_rank(rank) or def_tier
    if family == "tackles":
        rank = row.get("Opp Rush Off Rank", row.get("opp_rush_off_rank"))
        return _tier_from_rank(rank) or def_tier
    if family == "kicking":
        rank = row.get("Opp FG Def Rank", row.get("opp_fg_def_rank"))
        return _tier_from_rank(rank) or def_tier
    # pass / receiving
    rank = row.get("Opp Pass Def Rank", row.get("opp_pass_def_rank"))
    t = _tier_from_rank(rank)
    return t or def_tier


def _not_elite_off(row: pd.Series, family: str) -> float:
    if family == "rush":
        rank = row.get("Team Rush Off Rank", row.get("team_rush_off_rank"))
    else:
        rank = row.get("Team Pass Off Rank", row.get("team_pass_off_rank"))
    tier = _tier_from_rank(rank)
    if not tier:
        ident = str(row.get("Off Identity") or row.get("off_identity") or "").lower()
        if "elite" in ident:
            return 0.0
        if ident:
            return 1.0
        return float("nan")
    return 0.0 if tier == "Elite" else 1.0


def _merge_step8(graded: pd.DataFrame, step8_by_date: dict[str, pd.DataFrame]) -> pd.DataFrame:
    pieces: list[pd.DataFrame] = []
    for d, g in graded.groupby("slate_date", sort=False):
        s8 = step8_by_date.get(str(d))
        g = g.copy()
        g["_player_key"] = g["player"].map(_fold_player)
        g["_prop_key"] = g["prop_type_norm"].map(_norm_prop) if "prop_type_norm" in g.columns else ""
        if s8 is None or s8.empty:
            pieces.append(g)
            continue
        s = s8.copy()
        pc = _col(s, "Player", "player")
        pr = _col(s, "Prop", "prop", "prop_type")
        ln = _col(s, "Line", "line")
        dr = _col(s, "Direction", "direction", "bet_direction", "side")
        pt = _col(s, "Pick Type", "pick_type")
        if not pc or not pr:
            pieces.append(g)
            continue
        s["_player_key"] = s[pc].map(_fold_player)
        s["_prop_key"] = s[pr].map(_norm_prop)
        if ln:
            s["_line"] = pd.to_numeric(s[ln], errors="coerce")
        if dr:
            s["_side"] = s[dr].astype(str).str.upper().str.strip()
        if pt:
            s["_pick"] = s[pt].astype(str).str.strip()
        # collapse duplicate step8 keys: keep first
        s = s.drop_duplicates(subset=["_player_key", "_prop_key"], keep="first")
        keep_cols = [c for c in s.columns if c not in g.columns or c.startswith("_")]
        merged = g.merge(
            s[["_player_key", "_prop_key"] + [c for c in keep_cols if c not in {"_player_key", "_prop_key"}]],
            on=["_player_key", "_prop_key"],
            how="left",
            suffixes=("", "_s8"),
        )
        pieces.append(merged)
    return pd.concat(pieces, ignore_index=True)


def _gate70_flag(row: dict[str, Any]) -> int:
    """Research / ticket-stack proxy: directional L5=5 and L10>=8."""
    try:
        l5 = float(row.get("l5_hits"))
        l10 = float(row.get("l10_hits"))
    except (TypeError, ValueError):
        return 0
    if not (math.isfinite(l5) and math.isfinite(l10)):
        return 0
    return int(l5 >= 5 - 1e-9 and l10 >= 8 - 1e-9)


def build(max_week: int = 8) -> pd.DataFrame:
    raw = _load_graded(max_week)
    # normalize core fields
    if "prop_type_norm" not in raw.columns:
        raw["prop_type_norm"] = raw.get("prop_type", raw.get("Prop", ""))
    if "bet_direction" not in raw.columns:
        raw["bet_direction"] = raw.get("direction", raw.get("side", ""))
    if "pick_type" not in raw.columns:
        raw["pick_type"] = raw.get("Pick Type", "Standard")

    raw["player"] = raw["player"].astype(str)
    # drop combos
    raw = raw[~raw["player"].str.contains(r"\+", regex=True, na=False)].copy()
    raw["prop_norm"] = raw["prop_type_norm"].map(_norm_prop)
    raw["side"] = raw["bet_direction"].astype(str).str.upper().str.strip()
    raw["pick_type"] = raw["pick_type"].astype(str).str.strip()
    raw["line"] = pd.to_numeric(raw["line"], errors="coerce")
    raw["result_u"] = raw["result"].astype(str).str.upper().str.strip()
    raw["void_u"] = raw.get("void_reason_grade", pd.Series([""] * len(raw))).astype(str).str.upper()

    # filters
    raw = raw[raw["side"].isin(["OVER", "UNDER"])]
    raw = raw[~raw["pick_type"].str.contains("Demon", case=False, na=False)]
    raw = raw[raw["result_u"].isin(["HIT", "MISS"])]
    raw = raw[~raw["void_u"].str.contains("DNP|NO_ACTUAL|NOT_ON_GAME|VOID", regex=True, na=False)]

    step8 = _load_step8_index()
    merged = _merge_step8(raw, step8)
    snaps = _load_snap()
    pace_map = _load_pace_by_team_date()

    rows: list[dict[str, Any]] = []
    for _, r in merged.iterrows():
        side = str(r.get("side") or "").upper()
        line = r.get("line")
        try:
            line_f = float(line)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(line_f):
            continue
        family = prop_family(r.get("prop_norm") or r.get("prop_type_norm"))
        l5 = _dir_hits(r, side, "l5")
        l10 = _dir_hits(r, side, "l10")
        vals = _stat_window(r, 5)
        cover_pct, min_m_pct = _cover_min_from_window(vals, line_f, side)
        # if window missing but projection exists, approximate cover
        if not math.isfinite(cover_pct) and abs(line_f) > 1e-9:
            proj = r.get("projection", r.get("Projection"))
            try:
                proj_f = float(proj)
                if side == "OVER":
                    cover_pct = (proj_f - line_f) / line_f
                else:
                    cover_pct = (line_f - proj_f) / line_f
            except (TypeError, ValueError):
                pass

        player = str(r.get("player") or "")
        team = str(r.get("team") or r.get("Team") or "").strip().upper()
        snap = snaps.get(_fold_player(player))
        if family == "kicking":
            snap = None
        pace = pace_map.get((team, str(r.get("slate_date"))))
        if not pace:
            # derive from Game Total if present
            gt = r.get("Game Total", r.get("game_total"))
            try:
                gt_f = float(gt)
                pace = "HIGH" if gt_f >= 48 else ("LOW" if gt_f <= 40 else "NORMAL")
            except (TypeError, ValueError):
                pace = ""

        d_tier = _pick_d_tier(r, family)
        not_elite = _not_elite_off(r, family)
        hit = 1 if str(r.get("result_u")) == "HIT" else 0
        row = {
            "slate_date": r.get("slate_date"),
            "week": int(r.get("week") or 0),
            "player": player,
            "team": team,
            "opp_team": r.get("opp_team") or r.get("Opp"),
            "prop": r.get("prop_type_norm") or r.get("prop_norm"),
            "prop_norm": r.get("prop_norm"),
            "prop_family": family,
            "side": side,
            "pick_type": r.get("pick_type"),
            "line": line_f,
            "hit": hit,
            "result": r.get("result_u"),
            "actual": r.get("actual"),
            "l5_hits": l5,
            "l10_hits": l10,
            "min_m_pct": min_m_pct,
            "cover_pct": cover_pct,
            "snap_l3": snap if snap is not None else float("nan"),
            "opp_d_tier": d_tier,
            "opp_d_axis": prop_def_axis(r.get("prop_norm")),
            "not_elite_off": not_elite,
            "pace_tier": pace,
        }
        row["gate70"] = _gate70_flag(row)
        rows.append(row)

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("No training rows after filters")

    # Dedupe ladder: player x slate x prop x side x pick_type
    df["_pkey"] = (
        df["player"].map(_fold_player)
        + "|"
        + df["slate_date"].astype(str)
        + "|"
        + df["prop_norm"].astype(str)
        + "|"
        + df["side"]
        + "|"
        + df["pick_type"].astype(str).str.lower()
    )

    def _dedupe_group(g: pd.DataFrame) -> pd.DataFrame:
        if len(g) == 1:
            return g
        g5 = g[g["l5_hits"] >= 5 - 1e-9]
        if len(g5):
            # deepest L5=5 rung: OVER max line, UNDER min line
            if str(g5.iloc[0]["side"]).upper() == "OVER":
                return g5.loc[[g5["line"].idxmax()]]
            return g5.loc[[g5["line"].idxmin()]]
        # closest to median line of the group
        med = float(g["line"].median())
        idx = (g["line"] - med).abs().idxmin()
        return g.loc[[idx]]

    deduped = df.groupby("_pkey", group_keys=False).apply(_dedupe_group, include_groups=False)
    deduped = deduped.reset_index(drop=True)
    # restore prop_norm if apply dropped helper oddly
    if "prop_norm" not in deduped.columns and "prop" in deduped.columns:
        deduped["prop_norm"] = deduped["prop"].map(_norm_prop)

    print(
        f"raw_decided={len(df)} deduped={len(deduped)} "
        f"weeks={sorted(deduped['week'].unique().tolist())} "
        f"gate70={int(deduped['gate70'].sum())}"
    )
    return deduped


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--max-week", type=int, default=8)
    ap.add_argument("--out", type=Path, default=OUT_CSV)
    args = ap.parse_args()
    df = build(max_week=int(args.max_week))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"WROTE {args.out} rows={len(df)}")
    # tiny summary
    print(df.groupby("week")["hit"].agg(["count", "mean"]).to_string())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
