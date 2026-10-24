#!/usr/bin/env python3
"""
CFB step8 — display workbook + hit-tracking columns for combined slate / grader.

Reads step6_ranked_cfb.xlsx (ALL), writes step8_cfb_direction_clean.xlsx.

Run:
  py -3.14 Sports/CFB/scripts/pipeline/step8_add_direction_context_cfb.py \\
      --input outputs/step6_ranked_cfb.xlsx --date 2025-11-15
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[4]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
from utils.hit_tracking_columns import attach_hit_tracking_columns  # noqa: E402
from utils.cfb_implied_totals import (  # noqa: E402
    attach_implied_totals,
    log_observe_overlay,
    write_observe_report,
)
from utils.cfb_line_secondary import attach_line_secondary  # noqa: E402
from proporacle.data.table_io import copy_parquet_sidecar, write_parquet_sidecar, read_table, table_exists, write_excel_sheets


def _col(df: pd.DataFrame, *names: str) -> pd.Series:
    for n in names:
        if n in df.columns:
            return df[n]
    return pd.Series([""] * len(df), index=df.index)


_SLATE_TO_RANK_ABBR = {"NCST": "NCSU"}


def _fill_team_attempt_rates(clean: pd.DataFrame, cfb_root: Path) -> pd.DataFrame:
    """Fill team pass/rush att/g and rush rate from unit rankings."""
    from utils.cfb_clock_script import rush_rate as _rr

    rank_path = cfb_root / "data" / "reference" / "cfb_team_unit_rankings.csv"
    if not rank_path.is_file() or "Team" not in clean.columns:
        return clean
    pass_ok = pd.to_numeric(clean.get("Team Pass Att Pg"), errors="coerce").notna().any()
    rush_ok = pd.to_numeric(clean.get("Team Rush Att Pg"), errors="coerce").notna().any()
    if not (pass_ok and rush_ok):
        try:
            rk = pd.read_csv(rank_path, dtype=str)
        except Exception:
            return clean
        if "team_abbr" not in rk.columns:
            return clean
        rk["abbr"] = rk["team_abbr"].astype(str).str.strip().str.upper()
        lookup = {str(r["abbr"]): r for _, r in rk.iterrows()}
        pass_pg, rush_pg = [], []
        for abbr in clean["Team"].astype(str).str.strip().str.upper():
            key = _SLATE_TO_RANK_ABBR.get(abbr, abbr)
            row = lookup.get(key)
            if row is None:
                pass_pg.append("")
                rush_pg.append("")
                continue
            pass_pg.append(row.get("off_pass_att_pg", ""))
            rush_pg.append(row.get("off_rush_att_pg", ""))
        if not pass_ok:
            clean["Team Pass Att Pg"] = pd.to_numeric(pass_pg, errors="coerce")
        if not rush_ok:
            clean["Team Rush Att Pg"] = pd.to_numeric(rush_pg, errors="coerce")
    if "Team Rush Rate" not in clean.columns or pd.to_numeric(
        clean.get("Team Rush Rate"), errors="coerce"
    ).isna().all():
        clean["Team Rush Rate"] = [
            _rr(r, p)
            for r, p in zip(
                pd.to_numeric(clean.get("Team Rush Att Pg"), errors="coerce").tolist(),
                pd.to_numeric(clean.get("Team Pass Att Pg"), errors="coerce").tolist(),
            )
        ]
    return clean


def _is_cfb1h(*parts: str) -> bool:
    blob = " ".join(str(p or "") for p in parts).lower().replace("\\", "/")
    return "cfb1h" in blob or "cfb_1h" in blob


def _copy_dated(out_xlsx: Path, slate_date: str, *, cfb1h: bool = False) -> None:
    d = (slate_date or "").strip()
    if not d or not out_xlsx.is_file():
        return
    folder = "cfb1h" if cfb1h else "cfb"
    stem = "step8_cfb1h_direction_clean" if cfb1h else "step8_cfb_direction_clean"
    dated = _REPO / "outputs" / d / folder / f"{stem}_{d}.xlsx"
    try:
        dated.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(out_xlsx, dated)
        copy_parquet_sidecar(out_xlsx, dated)
        print(f"[CFB step8] Dated copy -> {dated}")
    except Exception as exc:
        print(f"[CFB step8] WARN dated copy: {exc}")


def main() -> None:
    root = Path(__file__).resolve().parents[2]
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="outputs/step6_ranked_cfb.xlsx")
    ap.add_argument("--sheet", default="ALL")
    ap.add_argument("--output", default="outputs/step8_cfb_direction_clean.xlsx")
    ap.add_argument("--date", default="", help="Slate YYYY-MM-DD")
    args = ap.parse_args()

    inp = Path(args.input)
    if not inp.is_absolute():
        inp = root / inp
    if not table_exists(inp):
        raise SystemExit(f"Missing input: {inp}")

    df = read_table(inp, sheet=args.sheet, sheet_order=(args.sheet, "ALL"))
    if df.empty:
        out = Path(args.output)
        if not out.is_absolute():
            out = root / out
        out.parent.mkdir(parents=True, exist_ok=True)
        write_excel_sheets(out, {"ALL": pd.DataFrame()})
        write_parquet_sidecar(pd.DataFrame(), out)
        print(f"[CFB step8] Wrote empty {out}")
        return

    work = df.copy()
    if "player" not in work.columns:
        for c in ("player_name", "pp_player", "player_norm"):
            if c in work.columns:
                work["player"] = work[c]
                break
    if "prop_type" not in work.columns:
        work["prop_type"] = _col(work, "prop_norm", "stat_type")
    if "bet_direction" not in work.columns:
        work["bet_direction"] = _col(work, "final_bet_direction", "recommended_side", "direction")

    sport_code = "CFB1H" if _is_cfb1h(args.input, args.output) else "CFB"
    work = attach_hit_tracking_columns(work, sport_code)
    work["sport"] = sport_code

    tier = _col(work, "tier").astype(str).str.upper().str.strip()
    rs = pd.to_numeric(_col(work, "final_score", "rank_score"), errors="coerce")
    line = pd.to_numeric(_col(work, "line", "line_score"), errors="coerce")
    proj = pd.to_numeric(_col(work, "projection"), errors="coerce")
    edge = pd.to_numeric(_col(work, "edge"), errors="coerce")
    direction = _col(work, "final_bet_direction", "bet_direction").astype(str).str.upper().str.strip()

    clean = pd.DataFrame(
        {
            "Tier": tier,
            "Rank Score": rs.round(2),
            "Player": _col(work, "player"),
            "Pos": _col(work, "pos", "position"),
            "Team": _col(work, "team", "team_abbr"),
            "Opp": _col(work, "opp_team", "opp_team_abbr", "opp"),
            "Game Time": _col(work, "start_time", "game_time"),
            "Game Id": _col(work, "pp_game_id", "game_id"),
            "Prop": _col(work, "prop_type", "prop_norm"),
            "Pick Type": _col(work, "pick_type").fillna("Standard"),
            "Line": line,
            "Direction": direction,
            "Edge": edge.round(2),
            "Projection": proj.round(2),
            "Hit Rate": pd.to_numeric(work.get("hit_rate"), errors="coerce"),
            "Hit Rate L5": pd.to_numeric(work.get("hit_rate_l5"), errors="coerce"),
            "Hit Rate L10": pd.to_numeric(work.get("hit_rate_l10"), errors="coerce"),
            "L5 Over": pd.to_numeric(work.get("l5_over"), errors="coerce"),
            "L5 Under": pd.to_numeric(work.get("l5_under"), errors="coerce"),
            "L10 Over": pd.to_numeric(work.get("l10_over"), errors="coerce"),
            "L10 Under": pd.to_numeric(work.get("l10_under"), errors="coerce"),
            "Strat Hit Rate": pd.to_numeric(work.get("strat_hit_rate"), errors="coerce"),
            "Strat N": pd.to_numeric(work.get("strat_n"), errors="coerce"),
            "Sport Maturity": work.get("sport_signal_maturity", ""),
            "Confidence Tier": work.get("confidence_tier", ""),
            "Confidence Score": pd.to_numeric(work.get("confidence_score"), errors="coerce"),
            "Confidence Note": work.get("confidence_note", ""),
            "Def Tier": _col(work, "def_tier", "opp_def_tier"),
            "Def Rank": pd.to_numeric(
                _col(work, "opp_def_rank", "OVERALL_DEF_RANK", "OPP_OVERALL_DEF_RANK", "def_rank"),
                errors="coerce",
            ),
            "Season Avg": pd.to_numeric(
                _col(work, "stat_season_avg", "season_avg"), errors="coerce"
            ),
            "Last 5 Avg": pd.to_numeric(
                _col(work, "stat_last5_avg", "last5_avg"), errors="coerce"
            ),
            "Team Pass Att Pg": pd.to_numeric(
                _col(work, "team_pass_off_att_pg"), errors="coerce"
            ),
            "Team Rush Att Pg": pd.to_numeric(
                _col(work, "team_rush_off_att_pg"), errors="coerce"
            ),
            "Team Rush Rate": pd.to_numeric(_col(work, "team_rush_rate"), errors="coerce"),
            "Opp Rush Rate": pd.to_numeric(_col(work, "opp_rush_rate"), errors="coerce"),
            "Team Plays Pg": pd.to_numeric(_col(work, "team_plays_off_pg"), errors="coerce"),
            "Opp Plays Pg": pd.to_numeric(_col(work, "opp_plays_off_pg"), errors="coerce"),
            "Team Clock": _col(work, "team_clock_tier"),
            "Opp Clock": _col(work, "opp_clock_tier"),
            "Clock Script": _col(work, "clock_script_note", "matchup_clock_note"),
            "Clock Mult": pd.to_numeric(_col(work, "clock_script_mult"), errors="coerce"),
            "Season L5 Flag": _col(work, "season_l5_flag"),
            "Games Used": pd.to_numeric(_col(work, "games_used"), errors="coerce"),
            "L5 Sample N": pd.to_numeric(_col(work, "l5_sample_n"), errors="coerce"),
            "L10 Current N": pd.to_numeric(_col(work, "l10_current_n"), errors="coerce"),
            "Team TOP Min": pd.to_numeric(_col(work, "team_top_min_pg"), errors="coerce"),
            "Opp TOP Min": pd.to_numeric(_col(work, "opp_top_min_pg"), errors="coerce"),
            "Pass Att Pg": pd.to_numeric(_col(work, "pass_att_pg"), errors="coerce"),
            "Rush Att Pg": pd.to_numeric(_col(work, "rush_att_pg"), errors="coerce"),
            "Rush Share": (
                pd.to_numeric(_col(work, "rush_att_share"), errors="coerce") * 100
            ).round(1),
            "Pass Share": (
                pd.to_numeric(_col(work, "pass_att_share"), errors="coerce") * 100
            ).round(1),
            "Rec Pg": pd.to_numeric(_col(work, "rec_pg"), errors="coerce"),
            "Rec Tgt Pg": pd.to_numeric(_col(work, "rec_tgt_pg"), errors="coerce"),
            "Tgt Share": (
                pd.to_numeric(_col(work, "rec_tgt_share"), errors="coerce") * 100
            ).round(1),
        }
    )
    for i in range(1, 11):
        clean[f"Stat G{i}"] = pd.to_numeric(_col(work, f"stat_g{i}"), errors="coerce")
        clean[f"Current G{i}"] = pd.to_numeric(_col(work, f"current_stat_g{i}"), errors="coerce")

    pt_low = clean["Pick Type"].astype(str).str.strip().str.lower()
    forced = pt_low.isin(("goblin", "demon"))
    ln = pd.to_numeric(clean["Line"], errors="coerce")
    pj = pd.to_numeric(clean["Projection"], errors="coerce")
    has_pl = ln.notna() & pj.notna()
    signed_gap = pj - ln
    clean["Edge"] = signed_gap.where(has_pl, pd.to_numeric(clean["Edge"], errors="coerce")).round(2)
    clean["Abs Edge"] = pd.to_numeric(clean["Edge"], errors="coerce").abs().round(2)
    e_arr = pd.to_numeric(clean["Edge"], errors="coerce").to_numpy(dtype=float, na_value=np.nan)
    from_side = np.where(np.isfinite(e_arr) & (e_arr >= 0), "OVER", "UNDER")
    d_prev = clean["Direction"].astype(str).str.upper().str.strip().replace("", "OVER")
    clean["Direction"] = np.where(
        forced.to_numpy(),
        "OVER",
        np.where(has_pl.to_numpy(), from_side, d_prev.to_numpy()),
    )
    clean = _fill_team_attempt_rates(clean, root)
    slate_date = str(args.date or "").strip()
    clean, _imp_meta = attach_implied_totals(clean, slate_date=slate_date)
    log_observe_overlay(_imp_meta, clean)
    clean, _line_meta = attach_line_secondary(clean)
    print(
        f"[CFB step8] line/secondary observe-only: "
        f"{_line_meta.get('rows_matched', 0)}/{_line_meta.get('rows_total', 0)} rows, "
        f"table_teams={_line_meta.get('table_teams', 0)} weighting={_line_meta.get('weighting')}"
    )
    _rep_day = slate_date or str(_imp_meta.get("slate_date") or "").strip()
    if _rep_day:
        _rep = _REPO / "data" / "reports" / f"cfb_script_d_veto_{_rep_day}.json"
        try:
            write_observe_report(_rep, _imp_meta, clean)
            print(f"[CFB step8] Observe report -> {_rep}")
        except Exception as exc:
            print(f"[CFB step8] WARN observe report: {exc}")

    out_path = Path(args.output)
    if not out_path.is_absolute():
        out_path = root / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)
    sheets = {"ALL": clean}
    for t in ("A", "B", "C", "D"):
        sub = clean[clean["Tier"] == t]
        if len(sub):
            sheets[f"Tier {t}"] = sub
    write_excel_sheets(out_path, sheets)
    print(f"[CFB step8] Wrote {out_path} rows={len(clean)}")
    write_parquet_sidecar(clean, out_path)
    _copy_dated(out_path, str(args.date or "").strip(), cfb1h=(sport_code == "CFB1H"))


if __name__ == "__main__":
    main()
