#!/usr/bin/env python3
"""
Attach national pass/rush/TD/kick/INT/sack unit ranks and prop-aware
OVERALL_DEF_RANK / def_tier to the CFB slate.

Reads: data/reference/cfb_team_unit_rankings.csv (from build_cfb_unit_rankings.py)
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[4]
_CFB_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

REFRESH_SCRIPT = _REPO / "scripts" / "refresh_rankings.py"

from utils.cfb_opp_fill import fill_cfb_opp_from_espn
from utils.cfb_playoff_metadata import CFB_TEAM_ALIASES, cfb_abbr_lookup_keys, norm_cfb_team_abbr
from utils.cfb_prop_defense import AXIS_RANK_KEYS, prop_def_axis, prop_def_keys
from utils.defense_tiers import def_tier_from_overall_rank, normalize_def_tier_label

# PrizePicks slate abbr → rankings CSV abbr (also covered by CFB_TEAM_ALIASES)
_SLATE_TO_RANK_ABBR = {
    "NCST": "NCSU",
    "SCAR": "SC",
    "CCAR": "CCU",
    "TXAM": "TA&M",
    "TAMU": "TA&M",
    "OKLA": "OU",
    "IND": "IU",
}

NAT_RANK_FIELDS = (
    ("pass_off_rank_nat", "pass_off_tier_nat"),
    ("rush_off_rank_nat", "rush_off_tier_nat"),
    ("pass_def_rank_nat", "pass_def_tier_nat"),
    ("rush_def_rank_nat", "rush_def_tier_nat"),
    ("overall_off_rank_nat", "overall_off_tier_nat"),
    ("overall_def_rank_nat", "overall_def_tier_nat"),
    ("pass_td_off_rank_nat", "pass_td_off_tier_nat"),
    ("rush_td_off_rank_nat", "rush_td_off_tier_nat"),
    ("rec_td_off_rank_nat", "rec_td_off_tier_nat"),
    ("td_off_rank_nat", "td_off_tier_nat"),
    ("pass_td_def_rank_nat", "pass_td_def_tier_nat"),
    ("rush_td_def_rank_nat", "rush_td_def_tier_nat"),
    ("rec_td_def_rank_nat", "rec_td_def_tier_nat"),
    ("td_def_rank_nat", "td_def_tier_nat"),
    ("fg_off_rank_nat", "fg_off_tier_nat"),
    ("pat_off_rank_nat", "pat_off_tier_nat"),
    ("kick_pts_off_rank_nat", "kick_pts_off_tier_nat"),
    ("fg_def_rank_nat", "fg_def_tier_nat"),
    ("pat_def_rank_nat", "pat_def_tier_nat"),
    ("kick_pts_def_rank_nat", "kick_pts_def_tier_nat"),
    ("int_off_rank_nat", "int_off_tier_nat"),
    ("int_def_rank_nat", "int_def_tier_nat"),
    ("sacks_taken_rank_nat", "sacks_taken_tier_nat"),
    ("sacks_def_rank_nat", "sacks_def_tier_nat"),
    ("tackles_def_rank_nat", "tackles_def_tier_nat"),
    ("tfl_def_rank_nat", "tfl_def_tier_nat"),
    ("plays_off_rank_nat", "plays_off_tier_nat"),
    ("pass_off_att_rank_nat", "pass_off_att_tier_nat"),
    ("rush_off_att_rank_nat", "rush_off_att_tier_nat"),
)

# Conference rank 1 = best in that league. Tiers use conference_size, not FBS 136.
CONF_RANK_FIELDS = tuple(
    (name.replace("_nat", "_conf"), tier.replace("_nat", "_conf"))
    for name, tier in NAT_RANK_FIELDS
) + (
    ("pass_off_rank", "pass_off_tier"),
    ("rush_off_rank", "rush_off_tier"),
    ("pass_def_rank", "pass_def_tier"),
    ("rush_def_rank", "rush_def_tier"),
)

META_FIELDS = ("conference_name", "conference_id", "conference_size")

AVG_FIELDS = (
    "pass_off_yds_pg",
    "rush_off_yds_pg",
    "pass_def_yds_pg",
    "rush_def_yds_pg",
    "total_off_yds_pg",
    "total_def_yds_pg",
    "pass_td_def_pg",
    "rush_td_def_pg",
    "rec_td_def_pg",
    "td_def_pg",
    "fg_def_pg",
    "pat_def_pg",
    "kick_pts_def_pg",
    "int_def_pg",
    "sacks_taken_pg",
    "plays_off_pg",
    "pass_off_att_pg",
    "rush_off_att_pg",
    "rush_rate",
    "top_sec_pg",
    "top_min_pg",
    "clock_tier",
    "rush_rate_rank_nat",
    "top_rank_nat",
    "sacks_def_pg",
    "tackles_def_pg",
    "tfl_def_pg",
)


def _norm_key(x: object) -> str:
    return norm_cfb_team_abbr(x)


def _normalize_rankings_csv(df: pd.DataFrame) -> pd.DataFrame:
    """
    Map build_cfb_unit_rankings.py schema (off_*/def_*) to attach keys.
    ``*_nat`` = FBS-wide. ``*_conf`` / unsuffixed pass/rush = conference.
    """
    if "off_pass_rank" not in df.columns:
        return df

    from utils.cfb_clock_script import enrich_rankings_clock

    out = enrich_rankings_clock(df.copy())
    n = len(out)
    if "conference_size" in out.columns:
        n_conf = pd.to_numeric(out["conference_size"], errors="coerce")
    else:
        n_conf = pd.Series([pd.NA] * n)
    rank_map = (
        ("off_pass_rank", "pass_off_rank_nat", "pass_off_tier_nat", "pass_off_rank", "pass_off_tier"),
        ("off_rush_rank", "rush_off_rank_nat", "rush_off_tier_nat", "rush_off_rank", "rush_off_tier"),
        ("def_pass_rank", "pass_def_rank_nat", "pass_def_tier_nat", "pass_def_rank", "pass_def_tier"),
        ("def_rush_rank", "rush_def_rank_nat", "rush_def_tier_nat", "rush_def_rank", "rush_def_tier"),
        ("off_points_rank", "overall_off_rank_nat", "overall_off_tier_nat", "overall_off_rank", "overall_off_tier"),
        ("def_points_rank", "overall_def_rank_nat", "overall_def_tier_nat", "overall_def_rank", "overall_def_tier"),
        ("off_pass_td_rank", "pass_td_off_rank_nat", "pass_td_off_tier_nat", None, None),
        ("off_rush_td_rank", "rush_td_off_rank_nat", "rush_td_off_tier_nat", None, None),
        ("off_rec_td_rank", "rec_td_off_rank_nat", "rec_td_off_tier_nat", None, None),
        ("off_td_rank", "td_off_rank_nat", "td_off_tier_nat", None, None),
        ("def_pass_td_rank", "pass_td_def_rank_nat", "pass_td_def_tier_nat", None, None),
        ("def_rush_td_rank", "rush_td_def_rank_nat", "rush_td_def_tier_nat", None, None),
        ("def_rec_td_rank", "rec_td_def_rank_nat", "rec_td_def_tier_nat", None, None),
        ("def_td_rank", "td_def_rank_nat", "td_def_tier_nat", None, None),
        ("off_fg_rank", "fg_off_rank_nat", "fg_off_tier_nat", None, None),
        ("off_pat_rank", "pat_off_rank_nat", "pat_off_tier_nat", None, None),
        ("off_kick_pts_rank", "kick_pts_off_rank_nat", "kick_pts_off_tier_nat", None, None),
        ("def_fg_rank", "fg_def_rank_nat", "fg_def_tier_nat", None, None),
        ("def_pat_rank", "pat_def_rank_nat", "pat_def_tier_nat", None, None),
        ("def_kick_pts_rank", "kick_pts_def_rank_nat", "kick_pts_def_tier_nat", None, None),
        ("off_int_rank", "int_off_rank_nat", "int_off_tier_nat", None, None),
        ("def_int_rank", "int_def_rank_nat", "int_def_tier_nat", None, None),
        ("off_sacks_taken_rank", "sacks_taken_rank_nat", "sacks_taken_tier_nat", None, None),
        ("def_sacks_rank", "sacks_def_rank_nat", "sacks_def_tier_nat", None, None),
        ("def_tackles_rank", "tackles_def_rank_nat", "tackles_def_tier_nat", None, None),
        ("def_tfl_rank", "tfl_def_rank_nat", "tfl_def_tier_nat", None, None),
        ("off_plays_rank", "plays_off_rank_nat", "plays_off_tier_nat", None, None),
        ("off_pass_att_rank", "pass_off_att_rank_nat", "pass_off_att_tier_nat", None, None),
        ("off_rush_att_rank", "rush_off_att_rank_nat", "rush_off_att_tier_nat", None, None),
        ("off_rush_rate_rank", "rush_rate_rank_nat", "rush_rate_tier_nat", None, None),
        ("off_top_rank", "top_rank_nat", "top_tier_nat", None, None),
    )
    ypg_pairs = (
        ("off_pass_ypg", "pass_off_yds_pg"),
        ("off_rush_ypg", "rush_off_yds_pg"),
        ("def_pass_ypg_allowed", "pass_def_yds_pg"),
        ("def_rush_ypg_allowed", "rush_def_yds_pg"),
        ("off_points_pg", "total_off_yds_pg"),
        ("def_points_allowed_pg", "total_def_yds_pg"),
        ("def_pass_td_pg_allowed", "pass_td_def_pg"),
        ("def_rush_td_pg_allowed", "rush_td_def_pg"),
        ("def_rec_td_pg_allowed", "rec_td_def_pg"),
        ("def_td_pg_allowed", "td_def_pg"),
        ("def_fg_pg_allowed", "fg_def_pg"),
        ("def_pat_pg_allowed", "pat_def_pg"),
        ("def_kick_pts_pg_allowed", "kick_pts_def_pg"),
        ("def_int_forced_pg", "int_def_pg"),
        ("off_sacks_taken_pg", "sacks_taken_pg"),
        ("off_plays_pg", "plays_off_pg"),
        ("off_pass_att_pg", "pass_off_att_pg"),
        ("off_rush_att_pg", "rush_off_att_pg"),
        ("off_rush_rate", "rush_rate"),
        ("off_top_sec_pg", "top_sec_pg"),
        ("off_top_min_pg", "top_min_pg"),
        ("off_clock_tier", "clock_tier"),
        ("def_sacks_pg", "sacks_def_pg"),
        ("def_tackles_pg", "tackles_def_pg"),
        ("def_tfl_pg", "tfl_def_pg"),
    )

    def _tier_from_rank(r: object) -> str:
        if str(r).strip() in ("", "nan"):
            return ""
        return def_tier_from_overall_rank(int(float(r)), n)

    def _tier_conf(rank: object, size: object) -> str:
        if str(rank).strip() in ("", "nan"):
            return ""
        try:
            n_c = int(float(size)) if str(size).strip() not in ("", "nan") else 0
        except (TypeError, ValueError):
            n_c = 0
        if n_c <= 0:
            return ""
        return def_tier_from_overall_rank(int(float(rank)), n_c)

    for src, nat_r, nat_t, conf_legacy_r, conf_legacy_t in rank_map:
        if src not in out.columns:
            continue
        out[nat_r] = out[src]
        out[nat_t] = out[src].map(_tier_from_rank)
        conf_src = f"{src}_conf"
        conf_r = nat_r.replace("_nat", "_conf")
        conf_t = nat_t.replace("_nat", "_conf")
        if conf_src in out.columns:
            out[conf_r] = out[conf_src]
            out[conf_t] = [
                _tier_conf(rk, sz) for rk, sz in zip(out[conf_src].tolist(), n_conf.tolist())
            ]
            if conf_legacy_r and conf_legacy_t:
                out[conf_legacy_r] = out[conf_src]
                out[conf_legacy_t] = out[conf_t]
        elif conf_legacy_r and conf_legacy_t:
            out[conf_legacy_r] = out[src]
            out[conf_legacy_t] = out[nat_t]

    for src, dst in ypg_pairs:
        if src in out.columns:
            out[dst] = out[src]

    return out


def _maybe_refresh_cfb_rankings(rank_path: Path, season: int = 0) -> None:
    """Refresh reference rankings via scripts/refresh_rankings.py when stale (>7 days)."""
    if REFRESH_SCRIPT.is_file():
        cmd = [sys.executable, str(REFRESH_SCRIPT), "--sport", "cfb"]
        if season:
            cmd.extend(["--season", str(int(season))])
        proc = subprocess.run(cmd, cwd=str(_REPO), capture_output=True, text=True)
        if proc.stdout:
            print(proc.stdout.rstrip())
        if proc.returncode != 0 and proc.stderr:
            print(proc.stderr.rstrip(), file=sys.stderr)
        return

    build_script = _CFB_ROOT / "scripts" / "build_cfb_unit_rankings.py"
    if not rank_path.is_file() and build_script.is_file():
        print("[CFB step3] Rankings missing — running build_cfb_unit_rankings.py")
        cmd = [sys.executable, str(build_script)]
        if season:
            cmd.extend(["--season", str(int(season))])
        cmd.extend(["--out", str(rank_path)])
        proc = subprocess.run(cmd, cwd=str(_REPO), capture_output=True, text=True)
        if proc.stdout:
            print(proc.stdout.rstrip())
        if proc.returncode != 0 and proc.stderr:
            print(proc.stderr.rstrip(), file=sys.stderr)


def _load_lookup(path: Path) -> tuple[dict[str, dict], int]:
    raw = pd.read_csv(path, dtype=str).fillna("")
    n_teams = int(raw["team_id"].nunique()) if "team_id" in raw.columns else len(raw)
    df = _normalize_rankings_csv(raw)
    by_abbr: dict[str, dict] = {}
    for _, r in df.iterrows():
        raw = str(r.get("team_abbr", "") or "").strip().upper()
        if not raw:
            continue
        payload = {c: r[c] for c in df.columns if c != "team_abbr"}
        for key in cfb_abbr_lookup_keys(raw) | {raw, _norm_key(raw)}:
            by_abbr[key] = payload
        for alias, canon in CFB_TEAM_ALIASES.items():
            if _norm_key(canon) in {_norm_key(raw), raw} or _norm_key(alias) == _norm_key(raw):
                by_abbr[_norm_key(alias)] = payload
    for slate, rank_abbr in _SLATE_TO_RANK_ABBR.items():
        hit = by_abbr.get(rank_abbr) or by_abbr.get(_norm_key(rank_abbr))
        if hit is not None:
            by_abbr[slate] = hit
            by_abbr[_norm_key(slate)] = hit
    return by_abbr, n_teams


def _attach_side(
    df: pd.DataFrame,
    abbr_col: str,
    prefix: str,
    lookup: dict[str, dict],
    field_groups: tuple[tuple[str, str], ...],
    avg_fields: tuple[str, ...] = (),
) -> None:
    for rank_col, tier_col in field_groups:
        out_rank = f"{prefix}_{rank_col}"
        out_tier = f"{prefix}_{tier_col}"
        ranks, tiers = [], []
        for _, row in df.iterrows():
            abbr = _norm_key(row.get(abbr_col, ""))
            payload = lookup.get(abbr)
            if not payload:
                ranks.append("")
                tiers.append("")
                continue
            ranks.append(payload.get(rank_col, ""))
            t = payload.get(tier_col, "")
            tiers.append(normalize_def_tier_label(t) or str(t).strip())
        df[out_rank] = ranks
        df[out_tier] = tiers

    for avg_col in avg_fields:
        out_avg = f"{prefix}_{avg_col}"
        avgs = []
        for _, row in df.iterrows():
            abbr = _norm_key(row.get(abbr_col, ""))
            payload = lookup.get(abbr) or {}
            avgs.append(payload.get(avg_col, ""))
        df[out_avg] = avgs


def _prop_def_keys(prop: str) -> tuple[str, str, str, str]:
    """Return (opp_def_rank_nat, opp_def_tier_nat, opp_def_avg_col, matchup_tier_col)."""
    rk, tr, avg_col = prop_def_keys(prop)
    return rk, tr, avg_col, f"opp_{tr}"


def _attach_prop_def_context(
    df: pd.DataFrame,
    opp_col: str,
    prop_col: str,
    lookup: dict[str, dict],
    n_teams: int,
) -> None:
    """Map opponent national def rank/tier + allowed rate onto each prop row."""
    ranks, tiers, avgs, n_out, axes = [], [], [], [], []
    for _, row in df.iterrows():
        prop = str(row.get(prop_col, "") or row.get("prop_type", "") or row.get("stat_type", ""))
        payload = lookup.get(_norm_key(row.get(opp_col, "")))
        if not payload:
            ranks.append("")
            tiers.append("")
            avgs.append("")
            n_out.append("")
            axes.append("")
            continue
        rk, tr, avg_col, _ = _prop_def_keys(prop)
        rank_val = payload.get(rk, "")
        if str(rank_val).strip() in ("", "nan") and rk != "overall_def_rank_nat":
            rk, tr, avg_col = AXIS_RANK_KEYS["points"]
            rank_val = payload.get(rk, "")
        ranks.append(rank_val)
        t = payload.get(tr, "")
        tiers.append(normalize_def_tier_label(t) or str(t).strip())
        avgs.append(payload.get(avg_col, ""))
        n_out.append(str(n_teams))
        axes.append(prop_def_axis(prop))

    df["OVERALL_DEF_RANK"] = ranks
    df["OPP_OVERALL_DEF_RANK"] = ranks
    df["def_tier"] = tiers
    df["opp_def_tier"] = tiers
    df["opp_def_yds_pg_avg"] = avgs
    df["fbs_team_count"] = n_out
    df["cfb_def_axis"] = axes


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument(
        "--rankings",
        default="data/reference/cfb_team_unit_rankings.csv",
        help="Unit rankings CSV from build_cfb_unit_rankings.py",
    )
    ap.add_argument("--output", required=True)
    ap.add_argument("--season", type=int, default=0, help="CFB season for rankings refresh (0 = auto).")
    ap.add_argument(
        "--skip-refresh",
        action="store_true",
        help="Do not auto-refresh stale cfb_team_unit_rankings.csv.",
    )
    args = ap.parse_args()

    cfb_root = Path(__file__).resolve().parents[2]
    rank_path = Path(args.rankings)
    if not rank_path.is_absolute():
        rank_path = cfb_root / rank_path
    if not args.skip_refresh:
        _maybe_refresh_cfb_rankings(rank_path, season=int(args.season))
    if not rank_path.exists():
        raise SystemExit(
            f"Missing rankings file: {rank_path}\n"
            "Run: py -3.14 scripts/build_cfb_unit_rankings.py --season <year>"
        )

    df = pd.read_csv(args.input, dtype=str).fillna("")
    df = fill_cfb_opp_from_espn(df)
    if "opp_team_abbr" in df.columns and "pp_opp_team" in df.columns:
        blank = df["opp_team_abbr"].astype(str).str.strip().isin(["", "nan", "None"])
        df.loc[blank, "opp_team_abbr"] = df.loc[blank, "pp_opp_team"]
    lookup, n_teams = _load_lookup(rank_path)
    print(f"→ Rankings: {rank_path} | teams={n_teams} (lookup keys={len(lookup)})")

    team_col = next((c for c in ("team_abbr", "pp_team", "team") if c in df.columns), None)
    opp_col = next((c for c in ("opp_team_abbr", "pp_opp_team", "opp") if c in df.columns), None)
    prop_col = next((c for c in ("prop_norm", "prop_type") if c in df.columns), "prop_type")
    if not team_col:
        raise SystemExit(f"No team column in slate. Columns: {list(df.columns)}")

    all_rank_fields = CONF_RANK_FIELDS + NAT_RANK_FIELDS
    _attach_side(df, team_col, "team", lookup, all_rank_fields, AVG_FIELDS + META_FIELDS)
    if opp_col:
        _attach_side(df, opp_col, "opp", lookup, all_rank_fields, AVG_FIELDS + META_FIELDS)
        if "team_pass_off_rank_nat" in df.columns and "opp_pass_def_rank_nat" in df.columns:
            df["matchup_pass_off_vs_def_rank"] = df["team_pass_off_rank_nat"]
            df["matchup_pass_off_vs_def_tier"] = df["opp_pass_def_tier_nat"]
            df["matchup_rush_off_vs_def_rank"] = df["team_rush_off_rank_nat"]
            df["matchup_rush_off_vs_def_tier"] = df["opp_rush_def_tier_nat"]
            df["matchup_pass_off_vs_def_tier_nat"] = df["opp_pass_def_tier_nat"]
            df["matchup_rush_off_vs_def_tier_nat"] = df["opp_rush_def_tier_nat"]
        if "team_pass_off_rank_conf" in df.columns:
            df["matchup_pass_off_vs_def_rank_conf"] = df["team_pass_off_rank_conf"]
            df["matchup_pass_off_vs_def_tier_conf"] = df["opp_pass_def_tier_conf"]
            df["matchup_rush_off_vs_def_rank_conf"] = df["team_rush_off_rank_conf"]
            df["matchup_rush_off_vs_def_tier_conf"] = df["opp_rush_def_tier_conf"]
        if "team_clock_tier" in df.columns and "opp_clock_tier" in df.columns:
            own = df["team_clock_tier"].astype(str).replace({"nan": "", "None": ""})
            opp = df["opp_clock_tier"].astype(str).replace({"nan": "", "None": ""})
            df["matchup_clock_note"] = [
                " vs ".join(p for p in (a, b) if str(p).strip())
                for a, b in zip(own.tolist(), opp.tolist())
            ]
        _attach_prop_def_context(df, opp_col, prop_col, lookup, n_teams)

    misses = 0
    if team_col:
        for _, row in df.iterrows():
            if _norm_key(row.get(team_col, "")) not in lookup:
                misses += 1
    filled_def = int((df.get("def_tier", pd.Series(dtype=str)).astype(str).str.strip() != "").sum())
    print(f"→ Rows={len(df)} | team rank misses={misses} | def_tier filled={filled_def}")

    df.to_csv(args.output, index=False)
    print(f"✅ Saved → {args.output}")


if __name__ == "__main__":
    main()
