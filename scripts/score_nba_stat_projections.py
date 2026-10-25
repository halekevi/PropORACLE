#!/usr/bin/env python3
"""Score NBA slates with walk-forward base rates + shrunk player P(over line).

Does not overwrite ``projection`` / ``ml_prob``. Adds:

  proj_mu, proj_mu_raw, proj_n, proj_family, proj_p_over, proj_p_under,
  proj_p_push, proj_p_play, base_rate, base_rate_n, proj_edge, proj_ev_edge,
  proj_hold, proj_minutes, proj_k

Usage:
  py -3.14 scripts/score_nba_stat_projections.py --input outputs/DATE/nba/step7_ranked_props.xlsx --date DATE
  py -3.14 scripts/score_nba_stat_projections.py --backtest --from 2026-01-01 --to 2026-04-13
"""
from __future__ import annotations

import argparse
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

_ROOT = Path(__file__).resolve().parent.parent
_SCRIPTS = _ROOT / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from utils.defense_tiers import d_aligned  # noqa: E402
from utils.expanding_marginals import (  # noqa: E402
    attach_hierarchical_base_rate,
    compute_expanding_marginals_lookup,
    walk_forward_lookups_by_date,
)
from utils.nba_player_projection import (  # noqa: E402
    DEFAULT_DB,
    DEFAULT_SHRINK_K,
    MIN_GAMES_NO_HOLD,
    NbaBoxIndex,
    _stat_col,
    directional_l5_l10,
    pick_type_breakeven,
    project_from_index,
)
from utils.nba_proj_k import DEFAULT_K_PATH, fit_k_from_boxes, load_k_table, save_k_table  # noqa: E402
from utils.proj_isotonic import calibrate_walk_forward  # noqa: E402
from utils.stat_distributions import line_probs  # noqa: E402

PROJ_COLS = (
    "proj_mu",
    "proj_mu_raw",
    "proj_n",
    "proj_family",
    "proj_p_over",
    "proj_p_under",
    "proj_p_push",
    "proj_p_play",
    "proj_p_cal",
    "base_rate",
    "base_rate_n",
    "base_rate_n_coarse",
    "base_rate_level",
    "proj_edge",
    "proj_ev_edge",
    "proj_ev_edge_cal",
    "proj_hold",
    "proj_hold_legacy",
    "proj_minutes",
    "proj_k",
)


def _load_table(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in (".xlsx", ".xlsm"):
        try:
            from proporacle.data.table_io import read_table

            return read_table(path)
        except Exception:
            return pd.read_excel(path, engine="openpyxl")
    return pd.read_csv(path, encoding="utf-8-sig", low_memory=False)


def _write_table(path: Path, df: pd.DataFrame) -> None:
    if path.suffix.lower() in (".xlsx", ".xlsm"):
        try:
            from proporacle.data.table_io import write_excel_sheets, write_parquet_sidecar

            write_excel_sheets(path, {"ALL": df})
            write_parquet_sidecar(df, path)
            return
        except Exception:
            df.to_excel(path, index=False)
            return
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _load_graded(templates: Path) -> pd.DataFrame:
    from build_retrain_dataset import load_all_graded_props

    return load_all_graded_props(templates)


def _direction_series(df: pd.DataFrame) -> pd.Series:
    if "direction" in df.columns:
        d = df["direction"]
    elif "bet_direction" in df.columns:
        d = df["bet_direction"]
    else:
        d = pd.Series("OVER", index=df.index)
    return d.astype(str).str.strip().str.upper()


def _pick_type_series(df: pd.DataFrame) -> pd.Series:
    if "pick_type" not in df.columns:
        return pd.Series("Standard", index=df.index)
    return df["pick_type"].astype(str).str.strip()


def _hitmiss_binary(df: pd.DataFrame) -> pd.Series:
    """HIT=1, MISS=0. Exact actual==line is a PrizePicks push, not a miss."""
    y = pd.Series(np.nan, index=df.index, dtype=float)
    if "result" in df.columns:
        ru = df["result"].astype(str).str.strip().str.upper()
        y.loc[ru.isin(("HIT", "WIN", "WON"))] = 1.0
        y.loc[ru.isin(("MISS", "LOSS", "LOSE"))] = 0.0
        y.loc[ru.eq("PUSH")] = np.nan
    elif "result_binary" in df.columns:
        rb = pd.to_numeric(df["result_binary"], errors="coerce")
        y = rb.where(rb.isin([0.0, 1.0]))
    if "actual_value" in df.columns and "line" in df.columns:
        actual = pd.to_numeric(df["actual_value"], errors="coerce")
        line = pd.to_numeric(df["line"], errors="coerce")
        push = actual.notna() & line.notna() & ((actual - line).abs() < 1e-9)
        y.loc[push] = np.nan
    return y


def _ml_prob_series(df: pd.DataFrame) -> pd.Series:
    p = pd.to_numeric(df["ml_prob"], errors="coerce") if "ml_prob" in df.columns else pd.Series(np.nan, index=df.index)
    p = p.where(p <= 1.0, p / 100.0)
    return p.where((p >= 0.0) & (p <= 1.0))


def wilson_interval(k: float, n: float, z: float = 1.96) -> tuple[float, float, float]:
    if n <= 0:
        return float("nan"), float("nan"), float("nan")
    p = k / n
    z2 = z * z
    den = 1.0 + z2 / n
    center = (p + z2 / (2.0 * n)) / den
    half = z * math.sqrt((p * (1.0 - p) + z2 / (4.0 * n)) / n) / den
    return p, max(0.0, center - half), min(1.0, center + half)


def _fmt_wilson(k: float, n: float) -> str:
    p, lo, hi = wilson_interval(k, n)
    if not math.isfinite(p):
        return "n=0"
    return f"{p:.3f}  [{lo:.3f}, {hi:.3f}]  n={int(n)}"


def attach_nba_projections(
    df: pd.DataFrame,
    *,
    as_of: str,
    db_path: Path = DEFAULT_DB,
    graded: pd.DataFrame | None = None,
    sport: str = "NBA",
    shrink_k: float | None = None,
    index: NbaBoxIndex | None = None,
    lookup: dict | None = None,
) -> pd.DataFrame:
    out = df.copy()
    if "sport" not in out.columns:
        out["sport"] = sport
    if lookup is None:
        lookup = {}
        if graded is not None and not graded.empty:
            lookup = compute_expanding_marginals_lookup(graded, as_of=as_of)
    out = attach_hierarchical_base_rate(out, lookup)

    for c in PROJ_COLS:
        if c not in out.columns:
            out[c] = np.nan if c not in ("proj_family",) else ""

    box = index
    if box is None and Path(db_path).is_file():
        box = NbaBoxIndex(db_path)
    if box is None or box.empty():
        print(f"[nba-proj] WARN no boxscore DB at {db_path} - base rates only")
        dirs = _direction_series(out)
        out["proj_p_play"] = np.where(dirs.eq("UNDER"), 1.0 - out["base_rate"], out["base_rate"])
        out["proj_edge"] = 0.0
        out["proj_ev_edge"] = np.nan
        out["proj_hold"] = 1.0
        return out

    dirs = _direction_series(out)
    pts = _pick_type_series(out)
    dist_cache: dict[tuple[str, str], object] = {}
    for idx, row in out.iterrows():
        try:
            line_f = float(row.get("line")) if pd.notna(row.get("line")) else float("nan")
        except (TypeError, ValueError):
            continue
        espn_id = str(row.get("ESPN_ATHLETE_ID") or row.get("espn_athlete_id") or row.get("espn_id") or "")
        player = str(row.get("player_name") or row.get("player") or row.get("pp_player") or "")
        prop = row.get("prop_type") or row.get("prop") or row.get("stat_type") or row.get("stat_norm")
        col = _stat_col(prop) or ""
        ckey = (espn_id.strip() or player.strip().lower(), col)
        rec = dist_cache.get(ckey)
        if rec is None and ckey not in dist_cache:
            rec = project_from_index(
                box,
                espn_id=espn_id,
                player=player,
                prop=prop,
                line=line_f,
                before_date=as_of,
                position_hint=str(row.get("position") or row.get("pos") or ""),
                base_rate_n=999.0,
                shrink_k=shrink_k,
            )
            dist_cache[ckey] = rec
        if rec is None:
            continue
        p_over, p_under, p_push, family = line_probs(
            rec.mu_shrunk,
            line_f,
            var=rec.var,
            n=rec.n,
            family=rec.family,
            stat_col=col,
        )
        out.at[idx, "proj_mu"] = rec.mu_shrunk
        out.at[idx, "proj_mu_raw"] = rec.mu_raw
        out.at[idx, "proj_n"] = rec.n
        out.at[idx, "proj_family"] = family
        out.at[idx, "proj_p_over"] = p_over
        out.at[idx, "proj_p_under"] = p_under
        out.at[idx, "proj_p_push"] = p_push
        out.at[idx, "proj_minutes"] = rec.proj_minutes
        out.at[idx, "proj_k"] = rec.k_mean
        br_n_player_hold = rec.n < MIN_GAMES_NO_HOLD
        hold_cell = True
        if "proj_hold_cell" in out.columns:
            v = out.at[idx, "proj_hold_cell"]
            if pd.notna(v):
                hold_cell = float(v) >= 1.0
        hold_legacy_cell = True
        if "proj_hold_legacy_cell" in out.columns:
            v = out.at[idx, "proj_hold_legacy_cell"]
            if pd.notna(v):
                hold_legacy_cell = float(v) >= 1.0
        play = p_under if str(dirs.loc[idx]).upper() == "UNDER" else p_over
        out.at[idx, "proj_p_play"] = play
        br = float(out.at[idx, "base_rate"] or 0.5)
        out.at[idx, "proj_edge"] = play - br if math.isfinite(play) else np.nan
        be = pick_type_breakeven(pts.loc[idx])
        out.at[idx, "proj_ev_edge"] = play - be if math.isfinite(play) else np.nan
        out.at[idx, "proj_hold"] = 1.0 if (br_n_player_hold or hold_cell) else 0.0
        out.at[idx, "proj_hold_legacy"] = 1.0 if (br_n_player_hold or hold_legacy_cell) else 0.0
    return out


def score_and_freeze_live_board(
    df: pd.DataFrame,
    slate: object,
    *,
    pre_tip: bool,
    db_path: Path | None = None,
    templates: Path | None = None,
) -> tuple[pd.DataFrame, int]:
    """Score a live NBA board and merge proj_edge for games that have not tipped."""
    from utils.nba_proj_edge_snapshot import row_still_pre_tip, write_pre_tip_frozen_day
    from utils.nba_slate_minutes import apply_pre_tip_slate_minutes, load_pre_tip_lookup

    as_of = str(slate)[:10]
    if len(as_of) != 10:
        return df, 0
    out = df.copy()
    if "file_date" not in out.columns:
        out["file_date"] = as_of
    still_open = any(row_still_pre_tip(row, fallback=pre_tip) for _, row in out.iterrows())
    if not still_open:
        return out, 0
    graded = pd.DataFrame()
    tdir = Path(templates) if templates else _ROOT / "ui_runner" / "templates"
    if tdir.is_dir():
        try:
            graded = _load_graded(tdir)
        except Exception:
            graded = pd.DataFrame()
    lookup: dict = {}
    if not graded.empty and "sport" in graded.columns:
        sport = graded["sport"].astype(str).str.strip().str.upper()
        lookup = walk_forward_lookups_by_date(graded.loc[sport.eq("NBA")]).get(as_of, {})
    index = NbaBoxIndex(db_path or DEFAULT_DB)
    scored = attach_nba_projections(
        out, as_of=as_of, db_path=Path(db_path or DEFAULT_DB), index=index, lookup=lookup
    )
    scored["_gate"] = _attach_gate(scored, index, as_of)
    scored = apply_pre_tip_slate_minutes(scored, load_pre_tip_lookup())
    n = write_pre_tip_frozen_day(scored, as_of, pre_tip=pre_tip)
    return scored, n


def _print_slate_summary(df: pd.DataFrame) -> None:
    scored = df["proj_p_play"].notna() if "proj_p_play" in df.columns else pd.Series(False, index=df.index)
    n = int(scored.sum())
    print(f"[nba-proj] scored {n}/{len(df)} rows with proj_p_play")
    if n == 0:
        return
    sub = df.loc[scored]
    fat = sub.loc[pd.to_numeric(sub["proj_edge"], errors="coerce").fillna(0) >= 0.08]
    print(
        f"[nba-proj] mean p_play={pd.to_numeric(sub['proj_p_play'], errors='coerce').mean():.3f}  "
        f"mean edge={pd.to_numeric(sub['proj_edge'], errors='coerce').mean():.3f}  "
        f"mean ev_edge={pd.to_numeric(sub['proj_ev_edge'], errors='coerce').mean():.3f}  "
        f"fat(edge>=0.08)={len(fat)}  held={int(pd.to_numeric(sub['proj_hold'], errors='coerce').fillna(0).eq(1).sum())}"
    )


def _decile_table(p: pd.Series, y: pd.Series, label: str) -> pd.DataFrame:
    work = pd.DataFrame({"_p": p, "_y": y}).dropna()
    work = work.loc[work["_y"].isin([0.0, 1.0])]
    if len(work) < 30:
        print(f"[nba-proj] {label}: not enough rows for deciles ({len(work)})")
        return pd.DataFrame()
    try:
        work["decile"] = pd.qcut(work["_p"], 10, labels=False, duplicates="drop") + 1
    except ValueError:
        print(f"[nba-proj] {label}: qcut failed")
        return pd.DataFrame()
    tab = (
        work.groupby("decile")
        .agg(n=("_y", "count"), pred=("_p", "mean"), realized=("_y", "mean"))
        .reset_index()
    )
    print(f"\n=== {label} calibration by decile ===")
    print(tab.to_string(index=False, float_format=lambda v: f"{v:.3f}"))
    return tab


def _attach_gate(day: pd.DataFrame, index: NbaBoxIndex, as_of: str) -> pd.Series:
    flags = []
    dirs = _direction_series(day)
    games_cache: dict[str, pd.DataFrame] = {}
    for idx, row in day.iterrows():
        col = _stat_col(row.get("prop_type") or row.get("prop") or row.get("stat_type"))
        if not col:
            flags.append(False)
            continue
        try:
            line = float(row.get("line"))
        except (TypeError, ValueError):
            flags.append(False)
            continue
        espn_id = str(row.get("espn_athlete_id") or row.get("espn_id") or "")
        player = str(row.get("player") or row.get("player_name") or "")
        gkey = espn_id.strip() or player.strip().lower()
        if gkey not in games_cache:
            games_cache[gkey] = index.games_before(espn_id=espn_id, player=player, before_date=as_of)
        games = games_cache[gkey]
        l5, l10 = directional_l5_l10(games, col, line, str(dirs.loc[idx]))
        if l5 != 5 or l10 is None or l10 < 8:
            flags.append(False)
            continue
        flags.append(d_aligned("NBA", str(dirs.loc[idx]), row.get("def_tier"), str(row.get("prop") or "")))
    return pd.Series(flags, index=day.index)


def _print_split(sub: pd.DataFrame, ycol: str, title: str) -> None:
    print(f"\n=== {title} ===")
    if sub.empty:
        print("  (empty)")
        return
    pt = _pick_type_series(sub).str.lower()
    pt = np.where(pt.str.contains("goblin"), "Goblin", np.where(pt.str.contains("demon"), "Demon", "Standard"))
    d = _direction_series(sub)
    tmp = sub.copy()
    tmp["_pt"] = pt
    tmp["_dir"] = d
    tmp["_y"] = pd.to_numeric(tmp[ycol], errors="coerce")
    for (p, di), g in tmp.groupby(["_pt", "_dir"], dropna=False):
        yy = g["_y"].dropna()
        yy = yy.loc[yy.isin([0.0, 1.0])]
        print(f"  {p:10s} {di:5s}  {_fmt_wilson(float(yy.sum()), float(len(yy)))}")
    yy = tmp["_y"].dropna()
    yy = yy.loc[yy.isin([0.0, 1.0])]
    print(f"  {'ALL':10s} {'':5s}  {_fmt_wilson(float(yy.sum()), float(len(yy)))}")


def _maybe_fit_k(index: NbaBoxIndex, fit_until: str, *, refit: bool = False) -> dict:
    n_pre = 0
    if not index.empty() and "game_date" in index.df.columns:
        n_pre = int((index.df["game_date"].astype(str).str[:10] < str(fit_until)[:10]).sum())
    print(f"[nba-proj] boxes={len(index.df)}  pre-{fit_until}={n_pre}")
    if not refit and DEFAULT_K_PATH.is_file():
        table = load_k_table()
        index.k_table = table
        print(f"[nba-proj] k from {DEFAULT_K_PATH.name}: pts={table.get('pts')} stl={table.get('stl')}")
        return table
    if n_pre < 2000:
        table = load_k_table()
        print(f"[nba-proj] k frozen defaults (not enough pre-cutoff boxes to fit)")
        return table
    payload = fit_k_from_boxes(index.df, fit_until=fit_until)
    save_k_table(payload)
    index.k_table = {**payload.get("k_mean", {}), **{f"{k}__var": v for k, v in (payload.get("k_var") or {}).items()}}
    print(f"[nba-proj] k fit source={payload.get('source')}  k_mean={payload.get('k_mean')}")
    return index.k_table


def _leg_key(row: pd.Series) -> str:
    player = str(row.get("player") or row.get("player_name") or "").strip().lower()
    prop = str(row.get("prop") or row.get("prop_type") or "").strip().lower()
    line = row.get("line")
    try:
        line_s = f"{float(line):g}"
    except (TypeError, ValueError):
        line_s = str(line or "")
    pt = str(row.get("pick_type") or "").strip().lower()
    d = str(row.get("direction") or "").strip().upper()
    date = str(row.get("file_date") or "")[:10]
    return f"{date}|{player}|{prop}|{line_s}|{pt}|{d}"


def _pt_label(s: object) -> str:
    p = str(s or "").strip().lower()
    if "goblin" in p:
        return "Goblin"
    if "demon" in p:
        return "Demon"
    return "Standard"


def _print_ev_book(sub: pd.DataFrame, title: str) -> None:
    print(f"\n=== {title} ===")
    if sub.empty:
        print("  (empty)")
        return
    tmp = sub.copy()
    tmp["_y"] = _hitmiss_binary(tmp)
    tmp["_pt"] = tmp["pick_type"].map(_pt_label) if "pick_type" in tmp.columns else "Standard"
    tmp["_be"] = tmp["pick_type"].map(pick_type_breakeven) if "pick_type" in tmp.columns else pick_type_breakeven("Standard")
    tmp["_ev"] = tmp["_y"] - tmp["_be"]
    ok = tmp["_y"].isin([0.0, 1.0])
    tmp = tmp.loc[ok]
    for pt, g in tmp.groupby("_pt", dropna=False):
        ev = pd.to_numeric(g["_ev"], errors="coerce")
        yy = g["_y"]
        print(
            f"  {str(pt):10s}  hit={_fmt_wilson(float(yy.sum()), float(len(yy)))}  "
            f"EV/leg={float(ev.mean()):+.4f}  n={len(g)}  mix={len(g)/max(len(tmp),1):.1%}"
        )
    ev = pd.to_numeric(tmp["_ev"], errors="coerce")
    yy = tmp["_y"]
    print(
        f"  {'ALL':10s}  hit={_fmt_wilson(float(yy.sum()), float(len(yy)))}  "
        f"EV/leg={float(ev.mean()):+.4f}  n={len(tmp)}"
    )


def _daily_topn(out: pd.DataFrame, rank_col: str, cand_extra: pd.Series | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
    top_parts = []
    gate_parts = []
    for d, day in out.groupby(out["file_date"].astype(str).str[:10], sort=True):
        y = _hitmiss_binary(day)
        gate = day.loc[day["_gate"].astype(bool) & y.isin([0.0, 1.0])]
        n_gate = len(gate)
        hold = pd.to_numeric(day["proj_hold"], errors="coerce").fillna(1.0).eq(0)
        cand = day.loc[hold & y.isin([0.0, 1.0]) & pd.to_numeric(day[rank_col], errors="coerce").notna()]
        if cand_extra is not None:
            cand = cand.loc[cand_extra.reindex(cand.index).fillna(False).astype(bool)]
        if n_gate <= 0:
            continue
        gate_parts.append(gate)
        if cand.empty:
            continue
        top_parts.append(cand.nlargest(n_gate, rank_col, keep="first"))
    gate_df = pd.concat(gate_parts, ignore_index=True) if gate_parts else pd.DataFrame()
    top_df = pd.concat(top_parts, ignore_index=True) if top_parts else pd.DataFrame()
    return gate_df, top_df


def _backtest(args: argparse.Namespace) -> int:
    templates = Path(args.templates).expanduser().resolve()
    graded = _load_graded(templates)
    if graded.empty:
        print("[nba-proj] no graded_props JSON found")
        return 1
    sport = graded.get("sport", pd.Series("", index=graded.index)).astype(str).str.strip().str.upper()
    nba = graded.loc[sport.eq("NBA")].copy()
    d0 = str(args.date_from or "")[:10]
    d1 = str(args.date_to or "")[:10]
    fd = nba.get("file_date", pd.Series("", index=nba.index)).astype(str).str[:10]
    if d0:
        nba = nba.loc[fd >= d0]
        fd = nba.get("file_date", pd.Series("", index=nba.index)).astype(str).str[:10]
    if d1:
        nba = nba.loc[fd <= d1]
    if nba.empty:
        print("[nba-proj] no NBA graded rows in window")
        return 1

    index = NbaBoxIndex(args.db)
    if not index.empty() and "game_date" in index.df.columns:
        gd = index.df["game_date"].astype(str).str[:10]
        print(f"[nba-proj] box window {gd.min()} -> {gd.max()}  n={len(index.df)}")
    else:
        print("[nba-proj] empty box DB - almost everything will hold")
    _maybe_fit_k(index, str(args.fit_k_until or "2026-01-01"), refit=bool(args.refit_k))

    print("[nba-proj] building walk-forward cell priors...")
    daily_lookups = walk_forward_lookups_by_date(graded.loc[sport.eq("NBA")])
    dates = sorted({str(x)[:10] for x in nba["file_date"].astype(str) if str(x)[:10]})
    parts = []
    for i, d in enumerate(dates):
        day = nba.loc[nba["file_date"].astype(str).str[:10].eq(d)].copy()
        scored = attach_nba_projections(
            day,
            as_of=d,
            db_path=Path(args.db),
            index=index,
            lookup=daily_lookups.get(d, {}),
            shrink_k=float(args.shrink_k) if args.shrink_k else None,
        )
        scored["_gate"] = _attach_gate(scored, index, d)
        parts.append(scored)
        if (i + 1) % 5 == 0 or i == 0 or i == len(dates) - 1:
            print(f"[nba-proj] scored {i + 1}/{len(dates)} dates ({d}, {len(day)} rows)")
    out = pd.concat(parts, ignore_index=True)
    print("[nba-proj] shadow ranking: NBA Goblin OVER by proj_edge (pickle/gate unchanged)")
    y = _hitmiss_binary(out)
    out["_y"] = y
    p = pd.to_numeric(out["proj_p_play"], errors="coerce")
    hold = pd.to_numeric(out["proj_hold"], errors="coerce").fillna(1.0)
    hold_legacy = pd.to_numeric(out["proj_hold_legacy"], errors="coerce").fillna(1.0)
    hitmiss = y.isin([0.0, 1.0])
    unheld = hold.eq(0.0)
    usable = hitmiss & p.notna() & unheld

    n_all = len(out)
    n_hm = int(hitmiss.sum())
    print("\n=== hold ===")
    print(
        f"  rows={n_all}  hit/miss={n_hm}  "
        f"hold_new={int((hitmiss & hold.eq(1)).sum())/max(n_hm,1):.1%}  "
        f"hold_legacy_fine_cell={int((hitmiss & hold_legacy.eq(1)).sum())/max(n_hm,1):.1%}"
    )
    print(f"  usable unheld hit/miss={int(usable.sum())}")
    formerly = hitmiss & hold_legacy.eq(1.0) & hold.eq(0.0)
    print(f"  formerly-held (fine thin, coarse ok)={int(formerly.sum())}")

    if "actual_value" in out.columns and "line" in out.columns:
        line = pd.to_numeric(out["line"], errors="coerce")
        actual = pd.to_numeric(out["actual_value"], errors="coerce")
        integ = (line - np.round(line)).abs() < 1e-9
        push_act = integ & actual.notna() & ((actual - line).abs() < 1e-9)
        ru = out["result"].astype(str).str.strip().str.upper() if "result" in out.columns else pd.Series("", index=out.index)
        print("\n=== integer-line pushes (PrizePicks: actual==line) ===")
        print(
            f"  integer lines with actual={int((integ & actual.notna()).sum())}  "
            f"actual==line={int(push_act.sum())}  stored result=PUSH={int(ru.eq('PUSH').sum())}  "
            f"(excluded from HIT/MISS for this backtest)"
        )

    print("[nba-proj] walk-forward isotonic on proj_p_play...")
    out["proj_p_cal"] = calibrate_walk_forward(out, p_col="proj_p_play", y_col="_y")
    be = out["pick_type"].map(pick_type_breakeven) if "pick_type" in out.columns else pick_type_breakeven("Standard")
    pcal = pd.to_numeric(out["proj_p_cal"], errors="coerce")
    out["proj_ev_edge_cal"] = pcal - be

    _decile_table(p.loc[usable], y.loc[usable], "proj_p_play raw (unheld HIT/MISS)")
    cal_ok = usable & pcal.notna()
    _decile_table(pcal.loc[cal_ok], y.loc[cal_ok], "proj_p_cal isotonic (same unheld rows)")
    mlp = _ml_prob_series(out)
    ml_ok = usable & mlp.notna()
    _decile_table(mlp.loc[ml_ok], y.loc[ml_ok], "pickle ml_prob (same unheld rows with ml_prob)")

    gate_df, top_df = _daily_topn(out, "proj_edge")
    _, ev_raw_df = _daily_topn(out, "proj_ev_edge")
    _, ev_cal_df = _daily_topn(out, "proj_ev_edge_cal")
    print("\n=== top-N vs L5=5 + L10>=8 + D (matched daily volume) ===")
    print(f"  dates={out['file_date'].astype(str).str[:10].nunique()}  gate_rows={len(gate_df)}  topN_rows={len(top_df)}")
    _print_ev_book(gate_df, "GATE")
    _print_ev_book(top_df, "TOP-N proj_edge (scout)")
    _print_ev_book(ev_raw_df, "TOP-N proj_ev_edge raw P-BE")
    _print_ev_book(ev_cal_df, "TOP-N proj_ev_edge_cal (isotonic P-BE)")

    if not gate_df.empty and not top_df.empty:
        gset = {_leg_key(r) for _, r in gate_df.iterrows()}
        tset = {_leg_key(r) for _, r in top_df.iterrows()}
        both = gset & tset
        union = gset | tset
        print("\n=== gate vs top-N overlap ===")
        print(
            f"  |gate|={len(gset)}  |topN|={len(tset)}  intersection={len(both)}  "
            f"jaccard={len(both)/max(len(union),1):.3f}  "
            f"topN_only={len(tset-gset)}  gate_only={len(gset-tset)}"
        )
        both_df = top_df.loc[top_df.apply(_leg_key, axis=1).isin(both)]
        only_df = top_df.loc[top_df.apply(_leg_key, axis=1).isin(tset - gset)]
        gate_only_df = gate_df.loc[gate_df.apply(_leg_key, axis=1).isin(gset - tset)]
        _print_ev_book(both_df, "intersection (gate AND top-N)")
        _print_ev_book(only_df, "top-N minus gate (fat lines the gate misses)")
        _print_ev_book(gate_only_df, "gate minus top-N")

        # Stack: within the gate pool, split by proj_edge median
        g2 = gate_df.copy()
        g2["_edge"] = pd.to_numeric(g2["proj_edge"], errors="coerce")
        med = float(g2["_edge"].median()) if g2["_edge"].notna().any() else 0.0
        _print_ev_book(g2.loc[g2["_edge"] >= med], "GATE high proj_edge (stack)")
        _print_ev_book(g2.loc[g2["_edge"] < med], "GATE low proj_edge")

    # Formerly-held volume: same daily N among rows the old fine-cell hold would have sat
    fh_parts = []
    for d, day in out.groupby(out["file_date"].astype(str).str[:10], sort=True):
        yb = _hitmiss_binary(day)
        n_gate = int((day["_gate"].astype(bool) & yb.isin([0.0, 1.0])).sum())
        cand = day.loc[
            pd.to_numeric(day["proj_hold_legacy"], errors="coerce").fillna(1).eq(1)
            & pd.to_numeric(day["proj_hold"], errors="coerce").fillna(1).eq(0)
            & yb.isin([0.0, 1.0])
            & pd.to_numeric(day["proj_edge"], errors="coerce").notna()
        ]
        if n_gate <= 0 or cand.empty:
            continue
        fh_parts.append(cand.nlargest(n_gate, "proj_edge", keep="first"))
    if fh_parts:
        fh_df = pd.concat(fh_parts, ignore_index=True)
        _print_ev_book(fh_df, "TOP-N on formerly-held rows (fine cell thin, coarse ok)")
    else:
        print("\n=== TOP-N on formerly-held rows ===\n  (empty)")

    if args.output:
        Path(args.output).parent.mkdir(parents=True, exist_ok=True)
        keep = out.loc[usable].copy() if int(usable.sum()) else out
        keep.to_csv(args.output, index=False, encoding="utf-8-sig")
        print(f"[nba-proj] wrote {args.output}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--input", default="", help="NBA step7 xlsx/csv to score")
    ap.add_argument("--output", default="", help="Write path (default: overwrite --input)")
    ap.add_argument("--date", default="", help="Slate date YYYY-MM-DD (as-of cutoff)")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--templates", default=str(_ROOT / "ui_runner" / "templates"))
    ap.add_argument("--shrink-k", type=float, default=0.0, help="Override per-stat k (0 = use table)")
    ap.add_argument("--backtest", action="store_true")
    ap.add_argument("--from", dest="date_from", default="")
    ap.add_argument("--to", dest="date_to", default="")
    ap.add_argument("--fit-k-until", default="2026-01-01")
    ap.add_argument("--refit-k", action="store_true")
    args = ap.parse_args()

    if args.backtest:
        return _backtest(args)

    if not args.input:
        ap.error("--input is required unless --backtest")
    src = Path(args.input).expanduser().resolve()
    if not src.is_file():
        print(f"[nba-proj] missing {src}")
        return 1
    as_of = str(args.date or "")[:10]
    if len(as_of) != 10:
        as_of = src.parent.name if len(src.parent.name) == 10 else ""
    if len(as_of) != 10:
        print("[nba-proj] pass --date YYYY-MM-DD")
        return 1
    df = _load_table(src)
    graded = pd.DataFrame()
    templates = Path(args.templates)
    if templates.is_dir():
        try:
            graded = _load_graded(templates)
        except Exception as exc:
            print(f"[nba-proj] WARN graded_props load failed: {exc}")
    out = attach_nba_projections(
        df,
        as_of=as_of,
        db_path=Path(args.db),
        graded=graded if not graded.empty else None,
        shrink_k=float(args.shrink_k) if args.shrink_k else None,
    )
    dest = Path(args.output).expanduser().resolve() if args.output else src
    _write_table(dest, out)
    _print_slate_summary(out)
    print(f"[nba-proj] wrote {dest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
