#!/usr/bin/env python3
"""Rank L5 = 5 Goblin / Standard Over / Standard Under by remaining gates.

Hard cut is directional L5 == 5 (no Demons) for WNBA/MLB/Soccer/Tennis.
NFL/NFLP is included as a fifth section: kickers still use L5 >= 4;
sit/cameo skill overs are dropped; backup skill overs need D (L5 not required).

Remaining gates, in order:
  L10, season avg vs line, cover distance, D, possessions/pace,
  offense efficiency on the prop, opponent def efficiency on the prop.

  py -3.14 scripts/rank_l5eq5_secondary.py --date 2026-08-27
  py -3.14 scripts/rank_l5eq5_secondary.py --date 2026-08-27 --step8-root H:\\PropORACLE_main_cp
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import rank_best_props_today as R  # noqa: E402
from prop_hit_tiers import TIER_RANK, PROMO_RANK, sort_key_tier_then_badge  # noqa: E402
from utils.defense_tiers import normalize_def_tier_label  # noqa: E402
from utils.nflp_playing_time import is_nflp, nflp_list_eligible  # noqa: E402
from utils.nfl_prop_defense import prop_def_axis  # noqa: E402

ET = ZoneInfo("America/New_York")
EXTRA = (
    "team_pace",
    "opp_pace",
    "pace_delta",
    "OPP_POSS",
    "DEF_EFF",
    "DEF_EFF_RANK",
    "stat_def_tier",
    "stat_def_rank",
    "stat_def_category",
    "hhs_ts_pct",
    "hhs_efg_pct",
    "usage_pct",
    "player_xg_per90",
    "player_shots_per90",
    "player_xag_per90",
    "GOALS_DEF_RANK",
    "GOALS_DEF_TIER",
    "SHOTS_DEF_RANK",
    "SHOTS_DEF_TIER",
    "opponent_rank",
    "first_serve_won_pct",
    "win_rate_on_surface",
    "aces_per_match",
    "avg_minutes",
    "avg_passes",
    "line_hit_rate_over_10",
    "line_hit_rate_under_10",
    "line_games_played_10",
    "Snap L3",
    "snap_pct_L3",
    "Starter Policy",
    "Expected Snaps",
    "Opp Pass Def Rank",
    "Opp Rush Def Rank",
    "Opp FG Def Rank",
    "pass_def_rank",
    "rush_def_rank",
    "fg_def_rank",
)


def _num(v) -> float | None:
    return R._flt(v)


def _l5_eq_5(r: dict) -> bool:
    side = str(r.get("side") or "")
    if side == "OVER":
        v = r.get("l5_over")
    elif side == "UNDER":
        v = r.get("l5_under")
    else:
        return False
    try:
        return int(v) == 5
    except (TypeError, ValueError):
        return False


def row_on_list(r: dict) -> bool:
    """L5=5 for in-season sports. NFLP uses playing-time + D, not 2025 L5."""
    if r.get("pick_type") == "Demon":
        return False
    sport = str(r.get("sport") or "").upper()
    if sport == "NFL" and is_nflp(r.get("league")):
        return nflp_list_eligible(
            policy=str(r.get("starter_policy") or "backup"),
            side=str(r.get("side") or ""),
            pick_type=str(r.get("pick_type") or "Standard"),
            d_ok=_d_ok(r),
            l5_over=r.get("l5_over"),
            l5_under=r.get("l5_under"),
        )
    return _l5_eq_5(r)


def _aligned_cover(r: dict) -> float | None:
    cover = r.get("cover")
    if not isinstance(cover, (int, float)):
        return None
    return float(cover) if r.get("side") == "OVER" else -float(cover)


def _season_hr_vs_line(r: dict, raw: dict) -> float | None:
    hr = r.get("season_hr")
    if isinstance(hr, (int, float)):
        return float(hr)
    side = r.get("side")
    col = "line_hit_rate_over_10" if side == "OVER" else "line_hit_rate_under_10"
    v = _num(raw.get(col))
    if v is None:
        return None
    return v / 100.0 if v > 1.5 else v


def _d_ok(r: dict) -> bool:
    return bool((r.get("checks") or {}).get("D") is True)


def _tier_ok(sport: str, tier: str, prop: str, *, over: bool) -> bool:
    canon = normalize_def_tier_label(tier) or str(tier or "").strip()
    if not canon:
        return False
    if over:
        return R._over_d_ok(sport, canon, prop)
    return R._under_d_ok(sport, canon, prop)


def _l10_n(r: dict, raw: dict) -> int | None:
    side = r.get("side")
    v = r.get("l10")
    if v is None:
        if side == "OVER":
            v = raw.get("line_hits_over_10") or raw.get("l10_over") or raw.get("L10 Over")
        else:
            v = raw.get("line_hits_under_10") or raw.get("l10_under") or raw.get("L10 Under")
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return None


def _poss_signal(r: dict, raw: dict) -> tuple[bool | None, str]:
    """Overs want more possessions/pace; unders want the inverse."""
    sport = str(r.get("sport") or "").upper()
    over = r.get("side") == "OVER"
    pace_d = _num(raw.get("pace_delta"))
    team_p = _num(raw.get("team_pace"))
    opp_p = _num(raw.get("opp_pace"))
    opp_poss = _num(raw.get("OPP_POSS"))
    bits = []
    if team_p is not None:
        bits.append(f"tm {team_p:.1f}")
    if opp_p is not None:
        bits.append(f"opp {opp_p:.1f}")
    if opp_poss is not None:
        bits.append(f"poss {opp_poss:.1f}")
    label = " ".join(bits)
    if sport == "TENNIS":
        return None, ""
    if sport == "NFL":
        pol = str(r.get("starter_policy") or raw.get("Starter Policy") or "").lower()
        snaps = str(r.get("expected_snaps") or raw.get("Expected Snaps") or "")
        snap = _num(raw.get("Snap L3") or raw.get("snap_pct_L3"))
        bits = [pol, snaps]
        if snap is not None:
            bits.append(f"snap {snap:.0f}")
        label = " ".join(x for x in bits if x)
        if pol in ("sit", "cameo"):
            return (not over), label
        if pol in ("backup", "plays"):
            return True if over else None, label
        return None, label
    if pace_d is not None:
        return (pace_d >= 0) if over else (pace_d <= 0), label or f"Δ{pace_d:+.1f}"
    if team_p is not None and opp_p is not None:
        return (opp_p >= team_p) if over else (opp_p <= team_p), label
    if opp_poss is not None:
        return None, label
    if opp_p is not None:
        return None, label
    return None, ""


def _off_signal(r: dict, raw: dict) -> tuple[bool | None, str]:
    """Player production efficiency on this prop family."""
    sport = str(r.get("sport") or "").upper()
    prop = str(r.get("prop") or "").lower()
    over = r.get("side") == "OVER"
    ts = _num(raw.get("hhs_ts_pct"))
    efg = _num(raw.get("hhs_efg_pct"))
    usg = _num(raw.get("usage_pct"))
    xg = _num(raw.get("player_xg_per90"))
    shots = _num(raw.get("player_shots_per90"))
    xag = _num(raw.get("player_xag_per90"))
    fs = _num(raw.get("first_serve_won_pct"))
    wr = _num(raw.get("win_rate_on_surface"))
    aces = _num(raw.get("aces_per_match"))
    passes = _num(raw.get("avg_passes"))
    if sport == "WNBA":
        parts = []
        if ts is not None:
            parts.append(f"ts {ts:.3f}")
        if efg is not None:
            parts.append(f"efg {efg:.3f}")
        if usg is not None:
            parts.append(f"usg {usg:.3f}")
        label = " ".join(parts)
        if ts is not None:
            return (ts >= 0.50) if over else (ts < 0.50), label
        if efg is not None:
            return (efg >= 0.48) if over else (efg < 0.48), label
        if usg is not None:
            return (usg >= 0.18) if over else (usg < 0.18), label
        return None, label
    if sport == "SOCCER":
        parts = []
        if xg is not None:
            parts.append(f"xG {xg:.2f}")
        if shots is not None:
            parts.append(f"sh {shots:.2f}")
        if xag is not None:
            parts.append(f"xAG {xag:.2f}")
        if passes is not None and "pass" in prop:
            parts.append(f"pass {passes:.1f}")
        label = " ".join(parts)
        if "save" in prop:
            return None, label
        if "pass" in prop and passes is not None:
            return (passes >= 30) if over else (passes < 30), label
        if xg is not None:
            return (xg >= 0.25) if over else (xg < 0.25), label
        if shots is not None:
            return (shots >= 1.5) if over else (shots < 1.5), label
        return None, label
    if sport == "TENNIS":
        parts = []
        if fs is not None:
            parts.append(f"1st {fs:.3f}")
        if wr is not None:
            parts.append(f"surf {wr:.3f}")
        if aces is not None and "ace" in prop:
            parts.append(f"aces {aces:.1f}")
        label = " ".join(parts)
        if "ace" in prop and aces is not None:
            return (aces >= 5) if over else (aces < 5), label
        if fs is not None:
            return (fs >= 0.65) if over else (fs < 0.65), label
        if wr is not None:
            return (wr >= 0.50) if over else (wr < 0.50), label
        return None, label
    return None, ""


def _opp_def_on_prop(r: dict, raw: dict) -> tuple[bool | None, str]:
    """Opponent's efficiency vs this stat, not just overall D."""
    sport = str(r.get("sport") or "").upper()
    prop = str(r.get("prop") or "").lower()
    over = r.get("side") == "OVER"
    stat_tier = R._clean(raw.get("stat_def_tier"))
    overall = r.get("def") or ""
    stat_rank = _num(raw.get("stat_def_rank"))
    def_eff = _num(raw.get("DEF_EFF"))
    def_eff_rk = _num(raw.get("DEF_EFF_RANK"))
    goals_t = R._clean(raw.get("GOALS_DEF_TIER"))
    shots_t = R._clean(raw.get("SHOTS_DEF_TIER"))
    opp_rk = _num(raw.get("opponent_rank"))
    if sport == "NFL":
        axis = prop_def_axis(r.get("prop") or raw.get("Prop") or prop)
        if axis == "rush":
            rk = _num(raw.get("Opp Rush Def Rank") or raw.get("opp_rush_def_rank"))
        elif axis == "kick":
            rk = _num(raw.get("Opp FG Def Rank") or raw.get("opp_fg_def_rank"))
        else:
            rk = _num(raw.get("Opp Pass Def Rank") or raw.get("opp_pass_def_rank"))
        label = f"{axis}#{int(rk)}" if rk is not None else (overall or "no-D")
        if rk is None:
            if overall:
                return _tier_ok(sport, overall, prop, over=over), label
            return False, "no-D"
        return (rk >= 17) if over else (rk <= 13), label
    if sport == "SOCCER":
        if "save" in prop:
            tier = overall
        elif "shot" in prop:
            tier = shots_t or overall
        else:
            tier = goals_t or overall
        label = tier or ""
        if shots_t or goals_t:
            label = f"{tier} shot#{raw.get('SHOTS_DEF_RANK') or ''} g#{raw.get('GOALS_DEF_RANK') or ''}".strip()
        if not tier:
            return False, "no-D"
        return _tier_ok(sport, tier, prop, over=over), label
    if sport == "TENNIS":
        label = f"opp#{int(opp_rk)}" if opp_rk is not None else (overall or "no-D")
        if opp_rk is None and not overall:
            return False, "no-D"
        if opp_rk is not None:
            return (opp_rk > R._ATP_AVG_MAX) if over else (opp_rk <= R._ATP_ABOVE_AVG_MAX), label
        return _tier_ok(sport, overall, prop, over=over), label
    # WNBA / MLB / default: prop-specific tier, else DEF_EFF rank, else overall D.
    bits = []
    if stat_tier:
        bits.append(stat_tier)
    if stat_rank is not None:
        bits.append(f"#{int(stat_rank)}")
    if def_eff is not None:
        bits.append(f"eff {def_eff:.1f}")
    label = " ".join(bits) or (overall or "no-D")
    if stat_tier:
        return _tier_ok(sport, stat_tier, prop, over=over), label
    if def_eff_rk is not None:
        # Rank 1 = stingiest. Overs want worse (higher number).
        return (def_eff_rk >= 7) if over else (def_eff_rk <= 5), label
    if overall:
        return _tier_ok(sport, overall, prop, over=over), label
    return False, "no-D"


def score_remaining(r: dict, raw: dict) -> dict:
    over = r.get("side") == "OVER"
    l10_n = _l10_n(r, raw)
    cover_a = _aligned_cover(r)
    line = r.get("line")
    try:
        line_f = float(line) if line is not None else None
    except (TypeError, ValueError):
        line_f = None
    season_hr = _season_hr_vs_line(r, raw)
    dist_need = R._delta_need(line_f) if line_f is not None else R.DELTA_FLOOR
    poss_ok, poss_s = _poss_signal(r, raw)
    off_ok, off_s = _off_signal(r, raw)
    odef_ok, odef_s = _opp_def_on_prop(r, raw)
    gates = {
        "L10": None if l10_n is None else l10_n >= 8,
        "Season": None if cover_a is None else cover_a > 0,
        "Distance": None if cover_a is None else cover_a >= dist_need,
        "D": _d_ok(r),
        "Poss": poss_ok,
        "Off": off_ok,
        "OppDef": odef_ok,
    }
    applicable = {k: v for k, v in gates.items() if v is not None}
    passed = [k for k, v in applicable.items() if v]
    missed = [k for k, v in applicable.items() if not v]
    return {
        "l10": l10_n,
        "season_hr": None if season_hr is None else round(season_hr, 3),
        "cover_aligned": None if cover_a is None else round(cover_a, 2),
        "poss": poss_s,
        "off": off_s,
        "opp_def_prop": odef_s,
        "gates": gates,
        "n_pass": len(passed),
        "n_app": len(applicable),
        "passed": passed,
        "missed": missed,
    }


def _row_key(r: pd.Series) -> tuple:
    return (
        str(r.get("player") or r.get("Player") or "").strip().casefold(),
        str(r.get("prop_type") or r.get("prop") or r.get("Prop") or "").strip().casefold(),
        str(r.get("pick_type") or r.get("Pick Type") or "").strip().casefold(),
        round(float(R._flt(r.get("line")) or 0), 3),
    )


def attach_raw(rows: list[dict], df: pd.DataFrame) -> None:
    lookup: dict[tuple, dict] = {}
    for _, raw in df.iterrows():
        lookup[_row_key(raw)] = raw.to_dict()
    for rec in rows:
        key = (
            str(rec.get("player") or "").strip().casefold(),
            str(rec.get("prop") or "").strip().casefold(),
            str(rec.get("pick_type") or "").strip().casefold(),
            round(float(rec.get("line") or 0), 3),
        )
        raw = lookup.get(key) or {}
        rec["raw"] = raw
        rec.update(score_remaining(rec, raw))


def l5eq5_buckets(rows: list[dict], sport: str) -> tuple[list[dict], list[dict], list[dict]]:
    std_o, std_u, gob = [], [], []
    for r in rows:
        if r.get("sport") != sport:
            continue
        if r.get("pick_type") == "Demon":
            continue
        if not row_on_list(r):
            continue
        if r.get("pick_type") == "Standard" and r.get("side") == "OVER":
            std_o.append(r)
        elif r.get("pick_type") == "Standard" and r.get("side") == "UNDER":
            std_u.append(r)
        elif r.get("pick_type") == "Goblin" and r.get("side") == "OVER":
            gob.append(r)

    def rank_key(r: dict, over: bool):
        return (
            -int(r.get("n_pass") or 0),
            TIER_RANK.get(str(r.get("prop_tier") or ""), 9),
            -int(r.get("l10") or 0),
            -(r.get("cover_aligned") if isinstance(r.get("cover_aligned"), (int, float)) else -99),
            PROMO_RANK.get(str(r.get("promo") or r.get("badge") or ""), 9),
            sort_key_tier_then_badge(r, over=over)[-1],
        )

    def dedup(lst, over: bool):
        seen: set[tuple] = set()
        out = []
        for r in sorted(lst, key=lambda x: rank_key(x, over)):
            k = (r.get("player"), r.get("prop"), r.get("line"), r.get("side"))
            if k in seen:
                continue
            seen.add(k)
            out.append(r)
        return out

    return dedup(std_o, True), dedup(std_u, False), dedup(gob, True)


def _fmt(r: dict) -> str:
    side = r.get("side") or "OVER"
    prefix = "O" if side == "OVER" else "U"
    badge = r.get("promo") or r.get("badge") or "—"
    tier = r.get("prop_tier") or "—"
    tag = f"W/{badge}" if r.get("prop_shadow") else f"{tier}/{badge}"
    l10 = r.get("l10")
    l10_s = f"{l10}/10" if l10 is not None else "—/10"
    avg = r.get("avg_l5") if r.get("avg_l5") is not None else r.get("season_avg")
    l10a = r.get("avg_l10")
    szn = r.get("avg_season") if r.get("avg_season") is not None else r.get("season_avg")
    avg_s = f"{avg:5.2f}" if isinstance(avg, (int, float)) else "  n/a"
    l10a_s = f"{l10a:5.2f}" if isinstance(l10a, (int, float)) else "  n/a"
    szn_s = f"{szn:5.2f}" if isinstance(szn, (int, float)) else "  n/a"
    cov = r.get("dist_l5") if r.get("dist_l5") is not None else r.get("cover")
    cov_s = f"{cov:+5.2f}" if isinstance(cov, (int, float)) else "  n/a"
    d = r.get("def") or "no-D"
    rk = r.get("def_rank")
    d_s = f"{d}#{rk}" if rk else d
    n_pass = r.get("n_pass")
    n_app = r.get("n_app")
    miss = ",".join(r.get("missed") or [])
    miss_s = f"  miss {miss}" if miss else ""
    extra = []
    if r.get("poss"):
        extra.append(r["poss"])
    if r.get("off"):
        extra.append(r["off"])
    if r.get("opp_def_prop"):
        extra.append(f"propD {r['opp_def_prop']}")
    extra_s = f"  {' | '.join(extra)}" if extra else ""
    pol = str(r.get("starter_policy") or "").strip()
    snaps = str(r.get("expected_snaps") or "").strip()
    pol_s = f"  {pol} {snaps}".rstrip() if pol and pol not in ("normal", "") else ""
    return (
        f"  {tag:12} {n_pass}/{n_app}  {r.get('player') or '':24} {r.get('prop') or '':18} "
        f"{prefix}{r.get('line')}  L10 {l10_s:5}  L5avg {avg_s}  L10avg {l10a_s}  szn {szn_s}  cov {cov_s}  {d_s:12}"
        f"{miss_s}{extra_s}{pol_s}  {r.get('matchup') or ''}"
    )


def print_sport(sport: str, std_o, std_u, gob) -> None:
    listed = std_o + std_u + gob
    title = "NFL/NFLP" if sport == "NFL" else sport
    extra = " L5=5" if sport != "NFL" else ""
    print(f"\n===== {title}{extra} =====  n={len(listed)}")
    if sport == "NFL":
        print(
            "  NFLP: 2025 L5/snap is starter volume, not tonight. "
            "Sit/cameo skill overs dropped. Backup skill overs need D (L5 not required). "
            "Kickers still use L5 >= 4."
        )
    print(f"Standard OVER  (n={len(std_o)})")
    if not std_o:
        print("  (none)")
    for r in std_o:
        print(_fmt(r))
    print(f"Standard UNDER (n={len(std_u)})")
    if not std_u:
        print("  (none)")
    for r in std_u:
        print(_fmt(r))
    print(f"Goblin OVER    (n={len(gob)})")
    if not gob:
        print("  (none)")
    for r in gob:
        print(_fmt(r))


def _xlsx_row(r: dict, category: str) -> dict:
    return {
        "Sport": r.get("sport"),
        "Category": category,
        "Gates_pass": r.get("n_pass"),
        "Gates_n": r.get("n_app"),
        "Missed": ",".join(r.get("missed") or []),
        "Prop_tier": r.get("prop_tier"),
        "Badge": r.get("promo") or r.get("badge"),
        "Shadow": bool(r.get("prop_shadow")),
        "Player": r.get("player"),
        "Prop": r.get("prop"),
        "Side": r.get("side"),
        "Line": r.get("line"),
        "L5": 5,
        "L10": r.get("l10"),
        "L5_avg": r.get("avg_l5"),
        "L10_avg": r.get("avg_l10"),
        "Season_avg": r.get("avg_season") if r.get("avg_season") is not None else r.get("season_avg"),
        "Dist_L5": r.get("dist_l5"),
        "Dist_L10": r.get("dist_l10"),
        "Dist_Season": r.get("dist_season"),
        "Season_HR": r.get("season_hr"),
        "Cover": r.get("cover"),
        "Cover_aligned": r.get("cover_aligned"),
        "D": r.get("def"),
        "D_rank": r.get("def_rank"),
        "Poss": r.get("poss"),
        "Off_eff": r.get("off"),
        "Opp_def_prop": r.get("opp_def_prop"),
        "Matchup": r.get("matchup"),
        "League": r.get("league"),
        "Starter_policy": r.get("starter_policy"),
        "Expected_snaps": r.get("expected_snaps"),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.now(ET).strftime("%Y-%m-%d"))
    ap.add_argument("--step8-root", default="")
    args = ap.parse_args()
    date = args.date
    candidates = [_REPO, _REPO.parent / "PropORACLE_main_cp"]
    if args.step8_root:
        candidates.insert(0, Path(args.step8_root))
    root = R._choose_step8_root(candidates, date)
    if root is None:
        print("no step8 root for", date)
        return 1
    print(f"date {date}  root {root}")
    print(
        "WNBA/MLB/Soccer/Tennis: L5=5 only. NFLP: playing-time gate "
        "(kickers L5>=4, backup overs need D, sit/cameo skill overs dropped). "
        "Ranked by remaining gates: L10, season vs line, cover distance, D, "
        "possessions, offense-on-prop, opponent-def-on-prop."
    )
    by_sport: dict[str, list[dict]] = {}
    buckets: dict[str, tuple] = {}
    for sport, folder, fname in R.SPORTS:
        df = R.load_sport(root, date, sport, folder, fname)
        rows = R.recs(df) if not df.empty else []
        if rows:
            attach_raw(rows, df)
        std_o, std_u, gob = l5eq5_buckets(rows, sport)
        buckets[sport] = (std_o, std_u, gob)
        by_sport[sport] = (
            [_xlsx_row(r, "Standard OVER") for r in std_o]
            + [_xlsx_row(r, "Standard UNDER") for r in std_u]
            + [_xlsx_row(r, "Goblin OVER") for r in gob]
        )
        print_sport(sport, std_o, std_u, gob)

    df_nfl = R.load_nfl(root, date)
    if df_nfl.empty:
        print("\n===== NFL/NFLP =====\n  (no step8 file)")
        buckets["NFL"] = ([], [], [])
        by_sport["NFL"] = []
    else:
        rows = R.recs(df_nfl)
        attach_raw(rows, df_nfl)
        std_o, std_u, gob = l5eq5_buckets(rows, "NFL")
        buckets["NFL"] = (std_o, std_u, gob)
        by_sport["NFL"] = (
            [_xlsx_row(r, "Standard OVER") for r in std_o]
            + [_xlsx_row(r, "Standard UNDER") for r in std_u]
            + [_xlsx_row(r, "Goblin OVER") for r in gob]
        )
        print_sport("NFL", std_o, std_u, gob)

    out_dir = _REPO / "outputs" / date
    out_dir.mkdir(parents=True, exist_ok=True)
    xlsx = out_dir / f"l5eq5_ranked_{date}.xlsx"
    frames = []
    for sport, rows in by_sport.items():
        frames.extend(rows)
        if not rows:
            frames.append({"Sport": sport, "Category": "none", "Player": "(no L5=5)"})
    pd.DataFrame(frames).to_excel(xlsx, index=False)
    payload = {
        "date": date,
        "step8_root": str(root),
        "gate": "L5==5",
        "sports": {
            sport: {
                "standard_over": len(buckets[sport][0]),
                "standard_under": len(buckets[sport][1]),
                "goblin_over": len(buckets[sport][2]),
            }
            for sport in buckets
        },
    }
    report = _REPO / "data" / "reports" / f"l5eq5_ranked_{date}.json"
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("\nwrote", xlsx)
    print("wrote", report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
