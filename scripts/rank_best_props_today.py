#!/usr/bin/env python3
"""Rank Standard Over/Under and Goblin Over plays for the active slate.

Always prints four sports: WNBA, MLB, Soccer, Tennis. Thin pools are listed
as empty, never omitted. NFL + NFLP share one step8 workbook and print as
an extra NFL section when that file exists. CFB, CFB1H, and Golf print as extra
sections when their step8 workbooks exist. CFB1H is a separate board (first-half
PBP L5) and is never merged into full-game CFB.

Season cover (badge) = season/L10 mean of that exact prop minus the posted
line. List ranking uses last-5 mean first, then L10, then season as
tiebreakers. Overs COVER when season avg > line; Unders COVER when
season avg < line.

List gate (hard): directional L5 >= 4, except WNBA/WNBA1H/WNBA1Q which
  require L5 = 5 for Standard OVER, Standard UNDER, and Goblin OVER.
  D is NOT a hard filter — a D miss only costs a badge (typically Silver
  if D is the only miss).
  NFLP (preseason) exception: 2025 L5 is not a lock. Sit/cameo skill overs
  are dropped; backup overs need D; kickers still use L5 >= 4.
  MLB Goblin OVER uses keep props 1-10. Tennis Goblin OVER uses keep gates
  (Games Won Standard−Goblin >=4; Total Games L5>=4 and L10>=8). Tennis
  Standard UNDER Aces/DF ungated; other tennis Standard off.

Badge = how many of six checks miss (N/A skipped, not a miss):
  L5 (>=4 on the play side; WNBA family >=5), Cover (avg on the right side of the line),
  Delta (play-side last-5 vs line >= per-prop typical gap, else max(0.5, 15% of line)),
  D (all sports O=Weak|Below Avg U=Elite|Above Avg; MLB hitter_strikeouts
  inverted O=Elite|Above Avg U=Weak|Below Avg; Avg never passes; CFB/CFB1H
  Pass Yards Goblin OVER skips D — same cfb_pass_yards_goblin_skip_d as
  tickets, N/A not a miss), Rank (O worse than median D, U top 40%;
  hitter Ks invert; tennis ATP #).
  Gold = 0 misses, Silver = 1, Bronze = 2.
  Diamond = Gold + directional L10 >= 8 AND season HR > 70% (min 10 games vs today's line).
  Platinum = Gold + exactly one of those two extra gates.
  CFB PRIOR_SEASON_FILL / PRIOR_SEASON_L5 never promote to Diamond/Platinum.

  Prop tier (S–D) is the market: S = MLB pitcher Goblins (Ks/walks/pitches/
  hits allowed), A = WNBA/tennis Goblin scoring (incl. 3PA) + pitching outs
  + soccer saves. Goblin FGA is B.
  Sort S→D then Diamond→Bronze. Console prints every listed row (L5≥4,
  WNBA family L5=5); leftover counts are listed by prop if a bucket is capped.
  Promotions can raise B/C into A/S (H+R+RBI + D, reb+ast cover, TB cover+D,
  hitter K cover). Shadow cells tag as W and stay on the list.

  py -3.14 scripts/rank_best_props_today.py --date 2026-08-18
  py -3.14 scripts/rank_best_props_today.py --date 2026-08-18 --step8-root H:\\...\\PropORACLE_main_cp
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
_SCRIPTS = Path(__file__).resolve().parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
from proporacle.data.table_io import read_table, table_exists, write_excel_sheets
from utils.slate_context_fill import is_hitter_strikeout_prop
from utils.defense_tiers import normalize_def_tier_label  # noqa: E402
from utils.ticket_tier_defense_gates import tennis_tight_match_note  # noqa: E402
from utils.nflp_playing_time import (  # noqa: E402
    expected_snaps_bucket,
    is_nflp,
    nflp_list_eligible,
    policy_from_row,
)
_TENNIS_SCRIPTS = _REPO / "Sports" / "Tennis" / "scripts"
if str(_TENNIS_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_TENNIS_SCRIPTS))
from tennis_shared import tennis_bo5_graded_on_bo3_tape  # noqa: E402
from prop_hit_tiers import (  # noqa: E402
    PROP_COVER_UNIT,
    assign_tier,
    canon_prop,
    norm_sport,
    sort_key_tier_then_badge,
)
from utils.ticket_70_pool import skip_earned_runs  # noqa: E402
from utils.mlb_keep_gates import mlb_goblin_keep_eligible  # noqa: E402
from utils.mlb_prop_matchup import format_mlb_matchup, mlb_matchup_parts  # noqa: E402
from utils.soccer_keep_gates import soccer_list_eligible  # noqa: E402
from utils.fantasy_prop_filter import is_fantasy_prop_label  # noqa: E402
from utils.cfb_keep_gates import (  # noqa: E402
    cfb_l8_bonus,
    cfb_list_eligible,
    cfb_pass_yards_goblin_skip_d,
)
from utils.tennis_keep_gates import tennis_list_eligible  # noqa: E402
from utils.l5_push_aware import is_clean_l5_five  # noqa: E402
from utils.l5_recency_policy import WNBA_L5_MIN, is_wnba_family  # noqa: E402
from utils.pp_board_prop_map import is_ou_ranker_prop  # noqa: E402
from utils.ticket_70_pool import skip_combo_player  # noqa: E402

WEAK = {"weak", "easy", "easiest"}
ELITE = {"elite", "hard", "hardest", "tough"}
WEAK_ALIGN = WEAK | {"below avg", "below average"}
ELITE_ALIGN = ELITE | {"above avg", "above average", "solid"}
# Fantasy stays on the full listing with ticketable=false (not a silent drop).
SKIP_PROPS: set[str] = set()
# Tennis ATP/WTA: lower # = stronger opponent (inverse of team D rank).
_ATP_ELITE_MAX = 10
_ATP_ABOVE_AVG_MAX = 25
_ATP_AVG_MAX = 50
_ATP_BELOW_AVG_MAX = 100
_UNKNOWN_OPP = {"unknown_opp", "unk", "unknown", ""}
DELTA_FLOOR = 0.50
DELTA_PCT = 0.15
BADGE_ORDER = {"Gold": 0, "Silver": 1, "Bronze": 2}
CFB_N_TEAMS = 122
BLOWOUT_SIT_TEAMS = {"USC", "FSU"}
SPORTS = (
    ("WNBA", "wnba", "step8_wnba_direction.csv"),
    ("MLB", "mlb", "step8_mlb_direction.csv"),
    ("SOCCER", "soccer", "step8_soccer_direction.csv"),
    ("TENNIS", "tennis", "step8_tennis_direction.csv"),
)
_STEP1_FILES = (
    ("wnba", "step1_wnba_props.csv"),
    ("mlb", "step1_mlb_props.csv"),
    ("soccer", "step1_soccer_props.csv"),
    ("tennis", "step1_tennis_props.csv"),
    ("nfl", "step1_pp_props_today.csv"),
    ("cfb", "step1_cfb.csv"),
    ("golf", "step1_golf_props.csv"),
    ("nba", "step1_pp_props_today.csv"),
)


def _parse_fetch_ts(v) -> datetime | None:
    s = str(v or "").strip()
    if len(s) < 10:
        return None
    try:
        return datetime.fromisoformat(s[:19].replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        pass
    try:
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except Exception:
        return None


def _root_board_freshness(root: Path, date: str) -> tuple[int, datetime]:
    """How many step1 boards were fetched on `date`, plus newest fetch timestamp."""
    same_day = 0
    latest = datetime.min
    for folder, fname in _STEP1_FILES:
        p = root / "outputs" / date / folder / fname
        if not p.is_file():
            continue
        ts = None
        try:
            df = pd.read_csv(p, nrows=5)
            for col in ("fetched_at", "line_asof"):
                if col in df.columns and len(df):
                    ts = _parse_fetch_ts(df[col].iloc[0])
                    if ts:
                        break
        except Exception:
            ts = None
        if ts is None:
            ts = datetime.fromtimestamp(p.stat().st_mtime)
        if ts.strftime("%Y-%m-%d") == date:
            same_day += 1
        naive = ts.replace(tzinfo=None)
        if naive > latest:
            latest = naive
    return same_day, latest


def _choose_step8_root(candidates: list[Path], date: str) -> Path | None:
    """Prefer a worktree whose step1 was fetched on the slate date (not yesterday's board)."""
    scored: list[tuple[int, datetime, Path]] = []
    for c in candidates:
        same_day, latest = _root_board_freshness(c, date)
        has8 = any(
            table_exists(c / "outputs" / date / folder / fname)
            for folder, fname in (
                ("wnba", "step8_wnba_direction.csv"),
                ("mlb", "step8_mlb_direction.csv"),
                ("soccer", "step8_soccer_direction.csv"),
                ("tennis", "step8_tennis_direction.csv"),
                ("nfl", "step8_nfl_direction_clean.xlsx"),
                ("cfb", "step8_cfb_direction_clean.xlsx"),
                ("golf", "step8_golf_direction_clean.xlsx"),
            )
        )
        if not has8:
            if same_day > 0:
                print(
                    f"  note: {c} has same-day step1 ({same_day}) but no step8 yet "
                    f"(newest={latest:%Y-%m-%d %H:%M}) — wait for 8AM pipeline to finish"
                )
            continue
        scored.append((same_day, latest, c))
    if not scored:
        return None
    scored.sort(key=lambda x: (x[0], x[1]), reverse=True)
    best = scored[0]
    for same_day, latest, c in scored[1:]:
        if c != best[2] and same_day < best[0]:
            print(
                f"  skip stale step8 root {c} "
                f"(same-day step1={same_day}, newest={latest:%Y-%m-%d %H:%M}) "
                f"-> using {best[2]} (same-day step1={best[0]}, newest={best[1]:%Y-%m-%d %H:%M})"
            )
    return best[2]


def _pick(v) -> str:
    s = str(v or "").strip().lower()
    if "dem" in s:
        return "Demon"
    if "gob" in s:
        return "Goblin"
    if "std" in s or s == "standard":
        return "Standard"
    return str(v or "").strip() or "Unknown"


def _dir(r) -> str:
    for c in ("final_bet_direction", "bet_direction", "model_dir", "Direction", "direction"):
        s = str(r.get(c) or "").strip().upper()
        if s in ("OVER", "UNDER"):
            return s
    return ""


def _model_dir(r) -> str:
    for c in ("model_dir", "Direction", "final_bet_direction"):
        s = str(r.get(c) or "").strip().upper()
        if s in ("OVER", "UNDER"):
            return s
    return ""


def _atp_tier_from_rank(rank) -> str:
    """Map individual opponent ATP/WTA rank to the same five D labels as team sports."""
    v = _num(rank)
    if v is None or v <= 0:
        return ""
    if v <= _ATP_ELITE_MAX:
        return "Elite"
    if v <= _ATP_ABOVE_AVG_MAX:
        return "Above Avg"
    if v <= _ATP_AVG_MAX:
        return "Avg"
    if v <= _ATP_BELOW_AVG_MAX:
        return "Below Avg"
    return "Weak"


def _opp_name(r) -> str:
    return _clean(r.get("opp_team") or r.get("opp") or r.get("Opp") or "").lower()


def _def_rank(r):
    sport = str(r.get("sport") or "").strip().upper()
    if sport == "TENNIS":
        if _opp_name(r) in _UNKNOWN_OPP:
            return None
        v = _num(r.get("opponent_rank")) or _num(r.get("opponent_def_rank"))
        return v if v is not None and v > 0 else None
    for c in (
        "OVERALL_DEF_RANK",
        "stat_def_rank",
        "def_rank",
        "opponent_def_rank",
        "opp_def_rank",
        "Def Rank",
    ):
        v = _num(r.get(c))
        if v is not None and v > 0:
            return v
    return None


def _n_teams(df: pd.DataFrame):
    if "sport" in df.columns and len(df):
        if str(df["sport"].iloc[0] or "").strip().upper() in ("CFB", "CFB1H"):
            return CFB_N_TEAMS
    for c in ("OVERALL_DEF_RANK", "stat_def_rank", "def_rank", "Def Rank"):
        if c not in df.columns:
            continue
        m = pd.to_numeric(df[c], errors="coerce").max()
        if pd.notna(m) and float(m) >= 5:
            return int(m)
    return None


def _delta_need(line: float, sport: str = "", prop: str = "") -> float:
    """Fair cover size: per-prop typical |last5-line|, else max(0.5, 15% of line)."""
    if sport:
        sport_n = norm_sport(sport)
        prop_n = canon_prop(sport_n, prop)
        if (sport_n, prop_n) in PROP_COVER_UNIT:
            return max(DELTA_FLOOR, float(PROP_COVER_UNIT[(sport_n, prop_n)]))
    return max(DELTA_FLOOR, abs(line) * DELTA_PCT)


def _clean(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    s = str(v).strip()
    return "" if s.lower() in ("", "nan", "none") else s


def _def(r) -> str:
    sport = str(r.get("sport") or "").strip().upper()
    if sport == "TENNIS":
        return _atp_tier_from_rank(_def_rank(r))
    raw = (
        _clean(r.get("stat_def_tier"))
        or _clean(r.get("DEF_TIER"))
        or _clean(r.get("def_tier"))
        or _clean(r.get("opp_def_tier"))
        or _clean(r.get("Def Tier"))
    )
    if not raw or raw.lower() in {"n/a", "na", "none"}:
        return ""
    # Canonical Elite→Weak (also maps legacy HARD/EASY* for back-compat).
    return normalize_def_tier_label(raw) or ""

def _over_d_ok(sport: str, tier: str, prop: str = "") -> bool:
    # Default all pipeline sports: Weak | Below Avg. Avg never passes.
    # Hitter Ks invert (wide): Elite | Above Avg. Pitcher Ks keep production.
    if sport == "MLB" and is_hitter_strikeout_prop(prop):
        return tier in ("Elite", "Above Avg")
    return tier in ("Weak", "Below Avg")


def _under_d_ok(sport: str, tier: str, prop: str = "") -> bool:
    # Default all pipeline sports: Elite | Above Avg. Avg never passes.
    # Hitter Ks invert (wide): Weak | Below Avg. Pitcher Ks keep production.
    if sport == "MLB" and is_hitter_strikeout_prop(prop):
        return tier in ("Weak", "Below Avg")
    return tier in ("Elite", "Above Avg")


def _num(v):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if str(v).strip() in ("", "nan", "None"):
            return None
        return int(float(v))
    except Exception:
        return None


def _flt(v):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        if str(v).strip() in ("", "nan", "None"):
            return None
        return float(v)
    except Exception:
        return None


def _first_num(r, *keys) -> float | None:
    for k in keys:
        v = _flt(r.get(k))
        if v is not None:
            return v
    return None


def _mean_stat_g(r, n: int) -> float | None:
    vals = []
    for i in range(1, n + 1):
        v = _flt(r.get(f"stat_g{i}"))
        if v is not None:
            vals.append(v)
    if not vals:
        return None
    return sum(vals) / len(vals)


def _avg_windows(r) -> tuple[float | None, float | None, float | None]:
    """Last-5, last-10, and season means for the posted prop.

    PrizePicks last-5 is the list sample. Season/L10 on MLB pitching is often
    pulled down by short outings (Lowder L5 94 vs L10/season 70).
    """
    l5 = _first_num(r, "stat_last5_avg", "Last 5 Avg", "last5_avg")
    if l5 is None:
        l5 = _mean_stat_g(r, 5)
    l10 = _first_num(r, "stat_last10_avg", "Last 10 Avg", "last10_avg")
    if l10 is None:
        l10 = _mean_stat_g(r, 10)
    seas = _first_num(r, "stat_season_avg", "season_avg", "Season Avg", "Projection")
    if seas is None:
        seas = l10 if l10 is not None else l5
    return l5, l10, seas


def _prop_avg(r) -> float | None:
    """Season / L10 mean (badge Cover). Prefer this over last-5 so Gold stays strict."""
    _l5, l10, seas = _avg_windows(r)
    if seas is not None:
        return seas
    if l10 is not None:
        return l10
    return _l5


def _dist(avg, line) -> float | None:
    if avg is None or line is None:
        return None
    return round(float(avg) - float(line), 2)


def l5_window_sort_key(r: dict) -> tuple:
    """Biggest |L5 avg − line| first; L10 then season break ties."""

    def absd(*vals) -> float:
        for v in vals:
            if isinstance(v, (int, float)):
                return abs(float(v))
        return -1.0

    return (
        -absd(r.get("dist_l5"), r.get("Dist_L5")),
        -absd(r.get("dist_l10"), r.get("Dist_L10")),
        -absd(r.get("dist_season"), r.get("Dist_Season")),
        str(r.get("sport") or r.get("Sport") or ""),
        str(r.get("player") or r.get("Player") or ""),
    )


def _first_count(*vals):
    """First numeric hit count, including 0 (do not treat 0/5 as missing)."""
    for v in vals:
        n = _num(v)
        if n is not None:
            return n
    return None


def _l5(r, over: bool):
    if over:
        return _first_count(r.get("l5_over"), r.get("last5_over"), r.get("L5 Over"))
    return _first_count(r.get("l5_under"), r.get("last5_under"), r.get("L5 Under"))


def _l5_push(r) -> int | None:
    return _first_count(r.get("l5_push"), r.get("last5_push"), r.get("L5 Push"), r.get("line_hits_push_5"))


def _l5_decided(r) -> int | None:
    return _first_count(
        r.get("l5_decided"),
        r.get("last5_decided"),
        r.get("L5 Decided"),
        r.get("line_hits_decided_5"),
    )


def _directional_l5_ok(r: dict, need: int, *, over: bool) -> bool:
    """List / reply L5 gate. need=5 requires five decided hits (no L5 pushes)."""
    hits = r.get("l5_over") if over else r.get("l5_under")
    if hits is None:
        return False
    if int(need) >= 5:
        return is_clean_l5_five(hits, r.get("l5_decided"), r.get("l5_push"))
    return float(hits) >= float(need)


def _l8(r, over: bool):
    if over:
        return _first_count(
            r.get("l8_over"),
            r.get("last8_over"),
            r.get("line_hits_over_8"),
        )
    return _first_count(
        r.get("l8_under"),
        r.get("last8_under"),
        r.get("line_hits_under_8"),
    )


def _l10(r, over: bool):
    if over:
        return _first_count(
            r.get("l10_over"),
            r.get("L10 Over"),
            r.get("last10_over"),
            r.get("line_hits_over_10"),
        )
    return _first_count(
        r.get("l10_under"),
        r.get("L10 Under"),
        r.get("last10_under"),
        r.get("line_hits_under_10"),
    )


def _season_hr(r) -> float | None:
    v = _flt(r.get("hit_rate")) or _flt(r.get("Hit Rate")) or _flt(r.get("season_hr"))
    if v is None:
        return None
    if v > 1.5:
        v = v / 100.0
    return v


def _season_n(r) -> int | None:
    n = (
        _num(r.get("strat_n"))
        or _num(r.get("Strat N"))
        or _num(r.get("season_n"))
        or _num(r.get("season_games"))
    )
    if n:
        return n
    sport = str(r.get("sport") or "").strip().upper()
    if sport in {"CFB", "CFB1H"}:
        gu = _num(r.get("games_used") or r.get("Games Used"))
        if gu:
            return gu
        # Do not treat a prior-season-filled L10 as n=10 (Diamond/Platinum cheat).
        if _cfb_prior_fill(r):
            return _num(r.get("l5_sample_n") or r.get("L5 Sample N")) or 0
    # CFB Week 1: Strat N is blank; L10 over+under is the 2025 sample vs today's line.
    l10o = _num(r.get("l10_over")) or _num(r.get("L10 Over"))
    l10u = _num(r.get("l10_under")) or _num(r.get("L10 Under"))
    if l10o is not None and l10u is not None:
        return int(l10o + l10u)
    return None


_CFB_PRIOR_FILL_FLAGS = frozenset({"PRIOR_SEASON_FILL", "PRIOR_SEASON_L5"})


def _cfb_prior_fill(r: dict) -> bool:
    """True when CFB L5/L10 mixed last year (not a clean current-season sample)."""
    sport = str(r.get("sport") or "").strip().upper()
    if sport not in {"CFB", "CFB1H"}:
        return False
    flag = str(
        r.get("season_l5_flag") or r.get("Season L5 Flag") or ""
    ).strip().upper()
    return flag in _CFB_PRIOR_FILL_FLAGS


def _promo(r: dict) -> str:
    gold = r.get("badge") == "Gold"
    l10 = r.get("l10")
    l10_ok = l10 is not None and l10 >= 8
    szn_ok = (
        r.get("season_hr") is not None
        and (r.get("season_n") or 0) >= 10
        and r["season_hr"] > 0.70
    )
    # Filled CFB L5 is listable; it is not a genuine 5-game 2026 Diamond/Platinum.
    if gold and _cfb_prior_fill(r):
        return "Gold"
    if gold and l10_ok and szn_ok:
        return "Diamond"
    if gold and (l10_ok != szn_ok):
        return "Platinum"
    return r.get("badge") or ""


def _promo_sort_key(r: dict) -> int:
    promo = r.get("promo") or r.get("Promo") or ""
    l10 = r.get("l10") or r.get("L10_over") or r.get("L10_under") or 0
    if promo == "Diamond":
        return 0
    if promo == "Platinum" and l10 >= 8:
        return 1
    if (r.get("badge") or r.get("Badge")) == "Gold":
        return 2
    if promo == "Platinum":
        return 3
    if (r.get("badge") or r.get("Badge")) == "Silver":
        return 4
    return 5


def _d_aligns(tier: str, over: bool) -> bool:
    low = (tier or "").strip().lower()
    if not low:
        return False
    if over:
        return low in WEAK_ALIGN or "below" in low or "easy" in low
    return low in ELITE_ALIGN or "above" in low or "hard" in low or "elite" in low


def _badge(rec: dict, n_teams: int | None) -> dict:
    """Six checks; Gold = 0 misses, Silver = 1, Bronze = 2. N/A checks are skipped."""
    side = rec.get("side") or ""
    over = side == "OVER"
    l5 = rec["l5_over"] if over else rec["l5_under"]
    cover = rec.get("cover")
    line = rec.get("line") if isinstance(rec.get("line"), (int, float)) else _flt(rec.get("line"))
    tier = rec.get("def") or ""
    rank = rec.get("def_rank")
    model = rec.get("model_dir") or ""
    sport = rec.get("sport") or ""

    checks: dict[str, bool | None] = {}
    l5_need = WNBA_L5_MIN if is_wnba_family(sport) else 4
    if l5 is None:
        checks["L5"] = None
    else:
        checks["L5"] = _directional_l5_ok(rec, l5_need, over=over)
    if cover is None:
        checks["Cover"] = None
    elif over:
        checks["Cover"] = cover > 0
    elif side == "UNDER":
        checks["Cover"] = cover < 0
    else:
        checks["Cover"] = None

    if cover is None or line is None:
        checks["Delta"] = None
    else:
        need = _delta_need(float(line), sport, str(rec.get("prop") or ""))
        gap = rec.get("dist_l5")
        if gap is None:
            gap = cover
        checks["Delta"] = gap >= need if over else gap <= -need

    if not model:
        checks["Dir"] = None
    elif rec.get("pick_type") == "Goblin":
        checks["Dir"] = model == "OVER"
    else:
        checks["Dir"] = model == side

    prop = str(rec.get("prop") or "")
    hitter_ks = sport == "MLB" and is_hitter_strikeout_prop(prop)
    # Same exemption as Goblin-70 tickets — one answer for "does D apply".
    skip_d = cfb_pass_yards_goblin_skip_d(rec)
    skip_matchup = not tier and rank is None
    if skip_matchup:
        # Unknown opponent / missing D: do not pass D — count as a miss when
        # we expected a matchup (blank tier with no rank). Pass Yards Goblin
        # OVER is the exception: D is N/A (ticket layer already skips it).
        checks["D"] = None if skip_d else False
        checks["Rank"] = None
    else:
        if skip_d:
            checks["D"] = None
        elif not tier:
            checks["D"] = False
        elif over:
            checks["D"] = _over_d_ok(sport, tier, prop)
        elif side == "UNDER":
            checks["D"] = _under_d_ok(sport, tier, prop)
        else:
            checks["D"] = False
        if rank is None:
            checks["Rank"] = None
        elif sport == "TENNIS":
            # Lower ATP # = stronger opponent (Elite). Overs want Weak/Below Avg (rank > 50).
            checks["Rank"] = rank > _ATP_AVG_MAX if over else rank <= _ATP_ABOVE_AVG_MAX
        elif not n_teams:
            checks["Rank"] = None
        elif hitter_ks:
            # Invert vs production: OVER favors stingy/Elite (low rank); UNDER favors Weak.
            checks["Rank"] = (
                rank <= int(math.floor(0.4 * n_teams))
                if over
                else rank >= int(math.ceil(0.5 * n_teams))
            )
        elif over:
            checks["Rank"] = rank >= int(math.ceil(0.5 * n_teams))
        else:
            checks["Rank"] = rank <= int(math.floor(0.4 * n_teams))

    applicable = {k: v for k, v in checks.items() if v is not None}
    misses = [k for k, v in applicable.items() if v is False]
    if len(applicable) < 4:
        badge = ""
    elif not misses:
        badge = "Gold"
    elif len(misses) == 1:
        badge = "Silver"
    elif len(misses) == 2:
        badge = "Bronze"
    else:
        badge = ""
    return {
        "checks": checks,
        "misses": misses,
        "n_app": len(applicable),
        "badge": badge,
        "miss_s": ", ".join(misses) if misses else "",
    }


def fill_tennis_opp_rank_from_slate(df: pd.DataFrame) -> pd.DataFrame:
    """Fill opponent_rank from hydrated ESPN rankings + slate cross-lookup."""
    tennis_scripts = _REPO / "Sports" / "Tennis" / "scripts"
    if str(tennis_scripts) not in sys.path:
        sys.path.insert(0, str(tennis_scripts))
    from tennis_shared import (
        fill_opponent_rank_from_slate_players,
        hydrate_rankings_from_slate,
        load_or_refresh_rankings,
        resolve_opp_rank_pair,
    )

    cache_dir = _REPO / "Sports" / "Tennis" / "cache"
    rankings = load_or_refresh_rankings(cache_dir / "tennis_rankings.json")
    rankings = hydrate_rankings_from_slate(
        df, rankings, cache_path=cache_dir / "tennis_opp_rank_cache.json"
    )
    opp_col = "opp_team" if "opp_team" in df.columns else "opp"
    filled = []
    for i in range(len(df)):
        v = resolve_opp_rank_pair(str(df.iloc[i].get(opp_col, "")), rankings)
        filled.append(v)
    out = df.copy()
    out["opponent_rank"] = filled
    return fill_opponent_rank_from_slate_players(out)


def filter_step8_to_slate_date(df: pd.DataFrame, date: str, sport: str = "") -> pd.DataFrame:
    """Keep rows whose game starts on the list window in Eastern.

    Day-ahead boards often store the fetch day in ``game_date`` while
    ``start_time`` is the actual tip. Prefer start_time when it parses.

    Tennis keeps today + 2 ET days so Saturday ranking still includes
    Monday slam first-round matches (US Open Total Games, etc.).
    MLB keeps today + 1 (postseason / day-ahead boards often tip tomorrow).
    """
    if df is None or getattr(df, "empty", True):
        return df
    keep = _slate_keep_dates(date, sport)
    if "start_time" in df.columns:
        st = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
        if st.notna().any():
            et = st.dt.tz_convert("America/New_York").dt.strftime("%Y-%m-%d")
            return df.loc[et.isin(keep)].copy()
    for col in ("game_date", "Game Date"):
        if col not in df.columns:
            continue
        gd = df[col].astype(str).str[:10]
        blank = gd.eq("") | gd.eq("nan") | gd.eq("None")
        # Period / NBA leftovers often have a blank Game Date with a May
        # matchup still in Team/Opp. Blank is not "today".
        sport_u = str(sport or "").upper()
        if sport_u in {"NBA", "NBA1Q", "NBA1H", "CBB", "WCBB", "WNBA", "WNBA1H", "WNBA1Q"}:
            return df.loc[gd.isin(keep)].copy()
        return df.loc[gd.isin(keep) | blank].copy()
    return df


def _slate_keep_dates(date: str, sport: str = "") -> set[str]:
    day = str(date or "")[:10]
    keep = {day} if day else set()
    sport_u = str(sport or "").upper()
    try:
        d0 = datetime.strptime(day, "%Y-%m-%d").date()
    except ValueError:
        return keep
    if sport_u == "TENNIS":
        return {(d0 + timedelta(days=i)).isoformat() for i in range(3)}
    if sport_u == "MLB":
        keep.add((d0 + timedelta(days=1)).isoformat())
    return keep


def step8_empty_reason(
    root: Path, date: str, sport: str, folder: str, fname: str
) -> str:
    """Explain an empty sport section — missing pipeline vs truly empty slate."""
    step8_path = root / "outputs" / date / folder / fname
    step1_name = {
        "wnba": "step1_wnba_props.csv",
        "mlb": "step1_mlb_props.csv",
        "soccer": "step1_soccer_props.csv",
        "tennis": "step1_tennis_props.csv",
        "nfl": "step1_pp_props_today.csv",
        "cfb": "step1_cfb.csv",
        "cfb1h": "step1_cfb1h.csv",
        "golf": "step1_golf_props.csv",
    }.get(folder)
    if table_exists(step8_path):
        return f"  (step8 present but 0 rows in list window for {date})"
    # MLB often lands as root xlsx without the folder CSV the SPORTS tuple names.
    if folder == "mlb":
        mlb_xlsx = root / "outputs" / date / f"step8_mlb_direction_clean_{date}.xlsx"
        if table_exists(mlb_xlsx):
            return f"  (step8 xlsx present but 0 rows in list window for {date})"
    if step1_name:
        step1_path = root / "outputs" / date / folder / step1_name
        if step1_path.is_file():
            n = _step1_row_count(step1_path)
            if n > 0:
                return (
                    f"  WARN: {sport} step8 missing — board exists ({n} rows) "
                    "but L5 not attached"
                )
            if n == 0:
                return f"  (step1 empty — no_slate for {date})"
    return "  (no step8 file)"


def _step1_row_count(path: Path) -> int:
    try:
        with path.open("r", encoding="utf-8", errors="replace") as fh:
            # header + data rows
            n = sum(1 for _ in fh) - 1
        return max(0, n)
    except OSError:
        return 0


def load_sport(root: Path, date: str, sport: str, folder: str, fname: str) -> pd.DataFrame:
    if sport == "MLB":
        return load_mlb(root, date)
    path = root / "outputs" / date / folder / fname
    if not table_exists(path):
        return pd.DataFrame()
    df = read_table(path)
    df["sport"] = sport
    if sport == "TENNIS":
        df = fill_tennis_opp_rank_from_slate(df)
    return filter_step8_to_slate_date(df, date, sport)


def load_mlb(root: Path, date: str) -> pd.DataFrame:
    """MLB step8: prefer dated folder CSV, then clean xlsx (root or Sports)."""
    candidates = [
        root / "outputs" / date / "mlb" / "step8_mlb_direction.csv",
        root / "outputs" / date / "mlb" / "step8_mlb_direction_clean.xlsx",
        root / "outputs" / date / "mlb" / f"step8_mlb_direction_clean_{date}.xlsx",
        root / "outputs" / date / f"step8_mlb_direction_clean_{date}.xlsx",
        root / "Sports" / "MLB" / "outputs" / "step8_mlb_direction_clean.xlsx",
        root / "Sports" / "MLB" / "step8_mlb_direction_clean.xlsx",
    ]
    path = next((p for p in candidates if table_exists(p)), None)
    if path is None:
        return pd.DataFrame()
    df = read_table(path, sheet_order=("ALL",))
    df["sport"] = "MLB"
    return filter_step8_to_slate_date(df, date, "MLB")


def _attach_nfl_box_logs(df: pd.DataFrame, root: Path) -> pd.DataFrame:
    """Copy stat_g1..10 from the NFL box-score file when step8 left them off.

    The clean workbook on disk is a display sheet. The five game values
    live on step5. Without them the L5=5 screen cannot see an NFL leg.
    """
    if df.empty:
        return df
    named = []
    for i in range(1, 6):
        for col in (f"stat_g{i}", f"G{i}", f"Stat G{i}", f"current_stat_g{i}", f"Current G{i}"):
            if col in df.columns:
                named.append(col)
    if named and df[named].notna().any().any():
        return df
    path = root / "Sports" / "NFL" / "data" / "outputs" / "step5_nfl_with_stats.csv"
    if not path.is_file():
        return df
    try:
        logs = pd.read_csv(path, low_memory=False)
    except OSError:
        return df
    stat_cols = [f"stat_g{i}" for i in range(1, 11) if f"stat_g{i}" in logs.columns]
    cur_cols = [f"current_stat_g{i}" for i in range(1, 11) if f"current_stat_g{i}" in logs.columns]
    if not stat_cols:
        return df

    def _key(frame: pd.DataFrame, player_cols: tuple[str, ...], prop_cols: tuple[str, ...]) -> pd.Series:
        player = pd.Series("", index=frame.index, dtype=object)
        for col in player_cols:
            if col in frame.columns:
                player = frame[col].astype(str)
                break
        prop = pd.Series("", index=frame.index, dtype=object)
        for col in prop_cols:
            if col in frame.columns:
                prop = frame[col]
                break
        line = pd.to_numeric(frame["line"] if "line" in frame.columns else frame.get("Line"), errors="coerce")
        folded = [
            canon_prop("NFL", raw)
            for raw in prop.tolist()
        ]
        names = player.astype(str).str.strip().str.lower()
        lines = line.map(lambda value: "" if pd.isna(value) else f"{float(value):.2f}")
        return names + "|" + pd.Series(folded, index=frame.index) + "|" + lines

    keep = ["_box_key", *stat_cols, *cur_cols]
    logs = logs.copy()
    logs["_box_key"] = _key(logs, ("player", "player_name"), ("prop_type", "prop"))
    logs = logs.dropna(subset=[stat_cols[0]])
    logs = logs.drop_duplicates("_box_key", keep="first")
    board = df.copy()
    board["_box_key"] = _key(board, ("Player", "player"), ("Prop", "prop", "prop_type"))
    merged = board.merge(logs[keep], on="_box_key", how="left", suffixes=("", "_box"))
    for col in (*stat_cols, *cur_cols):
        boxed = f"{col}_box"
        if boxed not in merged.columns:
            continue
        if col in merged.columns:
            merged[col] = merged[col].where(merged[col].notna(), merged[boxed])
        else:
            merged[col] = merged[boxed]
        merged = merged.drop(columns=[boxed])
    return merged.drop(columns=["_box_key"])


def load_nfl(root: Path, date: str) -> pd.DataFrame:
    """NFL + NFLP share one step8 workbook (preseason is the same D table)."""
    if _skip_inactive_slate(root, date, "nfl"):
        return pd.DataFrame()
    candidates = [
        root / "outputs" / date / "nfl" / "step8_nfl_direction_clean.xlsx",
        root / "outputs" / date / "nfl" / f"step8_nfl_direction_clean_{date}.xlsx",
        root / "Sports" / "NFL" / "outputs" / "step8_nfl_direction_clean.xlsx",
        root / "outputs" / date / f"step8_nfl_direction_clean_{date}.xlsx",
    ]
    path = next((p for p in candidates if table_exists(p)), None)
    if path is None:
        return pd.DataFrame()
    df = read_table(path, sheet_order=("ALL",))
    df["sport"] = "NFL"
    return _attach_nfl_box_logs(df, root)


def load_nhl(root: Path, date: str) -> pd.DataFrame:
    """NHL step8. G1..G10 are the five-game log the L5 screen reads."""
    candidates = [
        root / "outputs" / date / "nhl" / "step8_nhl_direction_clean.csv",
        root / "outputs" / date / "nhl" / "step8_nhl_direction_clean.xlsx",
        root / "outputs" / date / f"step8_nhl_direction_clean_{date}.xlsx",
    ]
    path = next((p for p in candidates if table_exists(p)), None)
    if path is None:
        return pd.DataFrame()
    df = read_table(path)
    df["sport"] = "NHL"
    return filter_step8_to_slate_date(df, date, "NHL")


def _prior_slate_date(date: str) -> str:
    try:
        return (datetime.strptime(str(date)[:10], "%Y-%m-%d").date() - timedelta(days=1)).isoformat()
    except ValueError:
        return ""


def load_cfb(root: Path, date: str) -> pd.DataFrame:
    """CFB Week 1+ step8 workbook (122-team FBS D table).

    Thursday tips often live on the Wednesday dated board when today's
    ``outputs/<date>/cfb`` folder was marked no_slate — also try date-1.
    """
    prior = _prior_slate_date(date)
    candidates = [
        root / "outputs" / date / "cfb" / "step8_cfb_direction_clean.xlsx",
        root / "outputs" / date / "cfb" / f"step8_cfb_direction_clean_{date}.xlsx",
        root / "outputs" / prior / "cfb" / "step8_cfb_direction_clean.xlsx" if prior else None,
        root / "outputs" / prior / "cfb" / f"step8_cfb_direction_clean_{prior}.xlsx" if prior else None,
        root / "Sports" / "CFB" / "outputs" / "step8_cfb_direction_clean.xlsx",
    ]
    path = next((p for p in candidates if p is not None and table_exists(p)), None)
    if path is None:
        return pd.DataFrame()
    df = read_table(path, sheet_order=("ALL",))
    df["sport"] = "CFB"
    return filter_step8_to_slate_date(df, date, "CFB")


def load_cfb1h(root: Path, date: str) -> pd.DataFrame:
    """CFB first-half board — separate from full-game CFB; L5 is Q1+Q2 PBP."""
    candidates = [
        root / "outputs" / date / "cfb1h" / "step8_cfb1h_direction_clean.xlsx",
        root / "outputs" / date / "cfb1h" / f"step8_cfb1h_direction_clean_{date}.xlsx",
        root / "Sports" / "CFB" / "outputs" / "step8_cfb1h_direction_clean.xlsx",
    ]
    path = next((p for p in candidates if table_exists(p)), None)
    if path is None:
        return pd.DataFrame()
    df = read_table(path, sheet_order=("ALL",))
    df["sport"] = "CFB1H"
    return df


def load_golf(root: Path, date: str) -> pd.DataFrame:
    """PGA tournament-week step8 workbook."""
    candidates = [
        root / "outputs" / date / "golf" / "step8_golf_direction_clean.xlsx",
        root / "outputs" / date / "golf" / f"step8_golf_direction_clean_{date}.xlsx",
        root / "Sports" / "Golf" / "outputs" / "step8_golf_direction_clean.xlsx",
    ]
    path = next((p for p in candidates if table_exists(p)), None)
    if path is None:
        return pd.DataFrame()
    df = read_table(path, sheet_order=("Golf", "ALL"))
    df["sport"] = "GOLF"
    return df


CBB_SEASON_END_2026 = "2026-04-07"
CBB_SEASON_RESUME = "2026-11-01"


def _cbb_season_active(date: str) -> bool:
    d = str(date or "")[:10]
    return bool(d) and (d < CBB_SEASON_END_2026 or d >= CBB_SEASON_RESUME)


def _load_cbb_family(root: Path, date: str, sport: str, folder: str) -> pd.DataFrame:
    """CBB/WCBB step8 when present (combined still uses step6 until step8 is wired)."""
    key = folder.lower()
    dated = [
        root / "outputs" / date / folder / f"step8_{key}_direction_clean.xlsx",
        root / "outputs" / date / folder / f"step8_{key}_direction_clean_{date}.xlsx",
    ]
    leftover = root / "Sports" / "CBB" / "outputs" / f"step8_{key}_direction_clean.xlsx"
    keep = _slate_keep_dates(date, sport)
    path = next((p for p in dated if table_exists(p)), None)
    if path is None and table_exists(leftover):
        trial = read_table(leftover, sheet_order=("ALL",))
        dates = _nba_game_dates(trial) if trial is not None else set()
        if trial is None or trial.empty or not dates or not (dates & keep):
            print(
                f"  [WARN] {sport} skip stale leftover {leftover.name} "
                f"game_dates={sorted(dates)[:3] if dates else 'none'} (want {sorted(keep)})"
            )
            return pd.DataFrame()
        path = leftover
    if path is None:
        return pd.DataFrame()
    df = read_table(path, sheet_order=("ALL",))
    df["sport"] = sport
    df = filter_step8_to_slate_date(df, date, sport)
    return df


def load_cbb(root: Path, date: str) -> pd.DataFrame:
    return _load_cbb_family(root, date, "CBB", "cbb")


def load_wcbb(root: Path, date: str) -> pd.DataFrame:
    return _load_cbb_family(root, date, "WCBB", "wcbb")


def _pipeline_sport_status(root: Path, date: str, sport_key: str) -> str:
    path = root / "outputs" / date / "pipeline_slate_status.json"
    if not path.is_file():
        return ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return ""
    sports = payload.get("sports") if isinstance(payload, dict) else None
    if not isinstance(sports, dict):
        return ""
    return str(sports.get(str(sport_key or "").strip().lower()) or "").strip().lower()


def _skip_inactive_slate(root: Path, date: str, sport_key: str) -> bool:
    """Honor pipeline_slate_status: off_season / no_slate boards are not today."""
    return _pipeline_sport_status(root, date, sport_key) in {"off_season", "no_slate"}


def _load_folder_step8(
    root: Path,
    date: str,
    sport: str,
    folder: str,
    sports_rel: tuple[str, ...],
) -> pd.DataFrame:
    """Period step8: dated outputs/<date>/<folder>/ first.

    Sports/<rel>/ static copies are leftovers (May NBA1Q Finals, May WNBA).
    Use one only when its Game Date intersects the slate window.
    """
    if _skip_inactive_slate(root, date, folder):
        return pd.DataFrame()
    stem = f"step8_{folder}_direction_clean"
    dated = [
        root / "outputs" / date / folder / f"{stem}.xlsx",
        root / "outputs" / date / folder / f"{stem}_{date}.xlsx",
        root / "outputs" / date / f"{stem}_{date}.xlsx",
    ]
    leftover = root.joinpath(*sports_rel) / f"{stem}.xlsx"
    keep = _slate_keep_dates(date, sport)
    path = next((p for p in dated if table_exists(p)), None)
    if path is None and table_exists(leftover):
        trial = read_table(leftover, sheet_order=("ALL",))
        dates = _nba_game_dates(trial) if trial is not None else set()
        if trial is None or trial.empty or not dates or not (dates & keep):
            print(
                f"  [WARN] {sport} skip stale leftover {leftover.name} "
                f"game_dates={sorted(dates)[:3] if dates else 'none'} (want {sorted(keep)})"
            )
            return pd.DataFrame()
        path = leftover
    if path is None:
        return pd.DataFrame()
    df = read_table(path, sheet_order=("ALL",))
    df["sport"] = sport
    df = filter_step8_to_slate_date(df, date, sport)
    return df


def _nba_game_dates(df: pd.DataFrame) -> set[str]:
    for col in ("game_date", "Game Date"):
        if col in df.columns:
            return {str(v)[:10] for v in df[col].dropna().tolist() if str(v)[:10] not in ("", "nan")}
    return set()


def load_nba(root: Path, date: str) -> pd.DataFrame:
    """Load NBA step8; reject stale Sports/May boards whose Game Date ≠ slate."""
    if _skip_inactive_slate(root, date, "nba"):
        return pd.DataFrame()
    keep = _slate_keep_dates(date, "NBA")
    candidates = [
        root / "outputs" / date / "nba" / "step8_all_direction_clean.xlsx",
        root / "outputs" / date / "nba" / "step8_nba_direction_clean.xlsx",
        root / "outputs" / date / "nba" / f"step8_nba_direction_clean_{date}.xlsx",
        root / "outputs" / date / "nba" / "step8_all_direction.csv",
        root / "outputs" / date / f"step8_nba_direction_clean_{date}.xlsx",
        root / "outputs" / date / "step8_nba_direction_clean.xlsx",
        root / "Sports" / "NBA" / "data" / "outputs" / "step8_all_direction_clean.xlsx",
        root / "Sports" / "NBA" / "step8_all_direction_clean.xlsx",
        root / "Sports" / "NBA" / "step8_nba_direction_clean.xlsx",
    ]
    df = pd.DataFrame()
    chosen: Path | None = None
    for path in candidates:
        if not table_exists(path):
            continue
        trial = read_table(path, sheet_order=("ALL",))
        if trial is None or trial.empty:
            continue
        dates = _nba_game_dates(trial)
        if dates and not (dates & keep):
            print(
                f"  [WARN] NBA skip stale board {path.name} "
                f"game_dates={sorted(dates)[:3]} (want {sorted(keep)})"
            )
            continue
        df = trial
        chosen = path
        break
    if df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["sport"] = "NBA"
    df = filter_step8_to_slate_date(df, date, "NBA")
    if df.empty:
        print(
            f"  [WARN] NBA board {chosen.name if chosen else '?'} had no rows "
            f"in slate window {sorted(keep)}"
        )
        return df
    try:
        from utils.roster_change import attach_roster_change_columns

        df = attach_roster_change_columns(df, sport="NBA", slate_date=date)
    except Exception as exc:
        print(f"  [WARN] roster_change attach skipped: {exc}")
    return df


def load_nba1q(root: Path, date: str) -> pd.DataFrame:
    return _load_folder_step8(root, date, "NBA1Q", "nba1q", ("Sports", "NBA"))


def load_nba1h(root: Path, date: str) -> pd.DataFrame:
    return _load_folder_step8(root, date, "NBA1H", "nba1h", ("Sports", "NBA"))


def load_wnba1q(root: Path, date: str) -> pd.DataFrame:
    return _load_folder_step8(root, date, "WNBA1Q", "wnba1q", ("Sports", "WNBA"))


def load_wnba1h(root: Path, date: str) -> pd.DataFrame:
    return _load_folder_step8(root, date, "WNBA1H", "wnba1h", ("Sports", "WNBA"))


def recs(df: pd.DataFrame) -> list[dict]:
    out = []
    n_teams = _n_teams(df)
    for _, r in df.iterrows():
        if str(r.get("sport") or "").upper() == "TENNIS" and tennis_bo5_graded_on_bo3_tape(r):
            continue
        prop = str(r.get("prop_type") or r.get("prop") or r.get("Prop") or "").strip()
        if prop.lower() in SKIP_PROPS:
            continue
        player = str(r.get("player") or r.get("Player") or "").strip()
        team = str(r.get("team") or r.get("Team") or "").strip()
        opp = str(
            r.get("opp_team") or r.get("Opp") or r.get("opponent") or r.get("opp") or ""
        ).strip()
        line = _flt(r.get("line"))
        if line is None:
            line = _flt(r.get("Line"))
        avg_l5, avg_l10, avg_seas = _avg_windows(r)
        avg = _prop_avg(r)
        cover = None if avg is None or line is None else avg - line
        side = _dir(r)
        clears = False
        if cover is not None:
            if side == "OVER":
                clears = cover > 0
            elif side == "UNDER":
                clears = cover < 0
        league = _clean(r.get("league") or r.get("League"))
        matchup = f"{team} vs {opp}".strip(" vs")
        if league:
            matchup = f"{matchup} ({league})" if matchup else league
        # Fantasy and other non-ticket markets stay listed with ticketable=false.
        raw_ticketable = r.get("ticketable")
        if raw_ticketable is None or str(raw_ticketable).strip() == "":
            ticketable = not is_fantasy_prop_label(prop)
        else:
            ticketable = str(raw_ticketable).strip().lower() not in {
                "0",
                "false",
                "no",
                "n",
            }
            if is_fantasy_prop_label(prop):
                ticketable = False
        rec = {
            "sport": str(r.get("sport") or ""),
            "ticketable": ticketable,
            "season_l5_flag": _clean(
                r.get("season_l5_flag") or r.get("Season L5 Flag")
            ),
            "games_used": _num(r.get("games_used") or r.get("Games Used")),
            "l5_sample_n": _num(r.get("l5_sample_n") or r.get("L5 Sample N")),
            "l5_window_n": _num(r.get("l5_window_n") or r.get("L5 Window N")),
            "l5_source_counts": _clean(
                r.get("l5_source_counts") or r.get("L5 Source Counts")
            ),
            "player": player,
            "stat_status": _clean(r.get("stat_status") or r.get("Stat Status")),
            "game_date": str(r.get("game_date") or r.get("Game Date") or "")[:10],
            "prop": prop,
            "line": r.get("line") if line is None else line,
            "pick_type": _pick(r.get("pick_type") or r.get("Pick Type")),
            "side": side,
            "model_dir": _model_dir(r),
            "l5_over": _l5(r, True),
            "l5_under": _l5(r, False),
            "l5_push": _l5_push(r),
            "l5_decided": _l5_decided(r),
            "thin_decided": (
                1
                if (
                    _l5_decided(r) is not None
                    and _l5_decided(r) < 3
                )
                else (0 if _l5_decided(r) is not None else None)
            ),
            "l10_over": _l10(r, True),
            "l10_under": _l10(r, False),
            "l8_over": _l8(r, True),
            "l8_under": _l8(r, False),
            "l8_window_n": _num(r.get("l8_window_n")),
            "avg_l5": None if avg_l5 is None else round(avg_l5, 2),
            "avg_l10": None if avg_l10 is None else round(avg_l10, 2),
            "avg_season": None if avg_seas is None else round(avg_seas, 2),
            "season_avg": None if avg is None else round(avg, 2),
            "dist_l5": _dist(avg_l5, line),
            "dist_l10": _dist(avg_l10, line),
            "dist_season": _dist(avg_seas, line),
            "cover": None if cover is None else round(cover, 2),
            "clears_line": clears,
            "def": _def(r),
            "def_rank": _def_rank(r),
            "matchup": matchup,
            "matchup_note": tennis_tight_match_note(
                prop, _def_rank(r), direction=side, opp_name=opp
            )
            or "",
            "league": league,
            "team": team,
            "opp_team": opp,
            "def_axis": _clean(r.get("def_axis")),
            "own_def_tier": _clean(r.get("own_def_tier") or r.get("team_def_tier")),
            "own_off_hits_tier": _clean(
                r.get("own_off_hits_tier") or r.get("team_off_hits_tier")
            ),
            "opp_pitching_tier": _clean(
                r.get("opp_pitching_tier") or r.get("opp_pitch_tier")
            ),
            "opp_off_hits_tier": _clean(
                r.get("opp_off_hits_tier") or r.get("OFF_HITS_TIER")
            ),
            "shot_volume": _clean(
                r.get("shot_volume")
                or r.get("Shot Volume")
                or r.get("shot_role")
                or r.get("Shot Role")
            ),
            "starter_tier": _clean(r.get("starter_tier") or r.get("Starter Tier")),
            "pass_role": _clean(r.get("pass_role") or r.get("Pass Role")),
            "usage_tier": _clean(
                r.get("usage_tier") or r.get("Usage Tier") or r.get("star_tier")
            ),
            "minutes_tier": _clean(
                r.get("minutes_tier") or r.get("min_tier") or r.get("Min Tier")
            ),
            "opp_lefty": _clean(r.get("opp_lefty") or r.get("Opp Lefty")),
            "opponent_hand": _clean(
                r.get("opponent_hand") or r.get("Opp Hand") or r.get("opp_hand")
            ),
            "l5_second_won_pct": _flt(
                r.get("l5_second_won_pct")
                or r.get("L5 2nd Won %")
                or r.get("second_won_l5")
            ),
            "player_atp_rank": _flt(
                r.get("player_atp_rank") or r.get("Player Rank") or r.get("player_rank")
            ),
            "opponent_rank": _flt(
                r.get("opponent_rank")
                or r.get("Opponent Rank")
                or r.get("opp_rank")
                or r.get("opponent_def_rank")
            ),
            "ranking_diff": _flt(r.get("ranking_diff") or r.get("Ranking Diff")),
            "is_franchise_star": r.get("is_franchise_star"),
            "team_top3_rank": r.get("team_top3_rank")
            if r.get("team_top3_rank") is not None
            else r.get("Team Top3 Rank"),
            "own_def_tier": _clean(
                r.get("own_def_tier")
                or r.get("OWN_DEF_TIER")
                or r.get("Own Def Tier")
            ),
            "opp_off_tier": _clean(
                r.get("opp_off_tier")
                or r.get("OPP_OFF_TIER")
                or r.get("Opp Off Tier")
                or r.get("OFF_TIER")
            ),
            "roster_changed": int(r.get("roster_changed") or 0) == 1
            or str(r.get("roster_changed") or "").strip().lower() in {"1", "true", "yes"},
            "roster_note": _clean(r.get("roster_note")),
            "new_teammates": _clean(r.get("new_teammates")),
            "roster_change_reason": _clean(r.get("roster_change_reason")),
        }
        rec["starter_policy"] = policy_from_row({**dict(r), **rec})
        rec["expected_snaps"] = expected_snaps_bucket(rec["starter_policy"])
        rec.update(_badge(rec, n_teams))
        if str(rec.get("sport") or "").upper() == "MLB":
            parts = mlb_matchup_parts(rec)
            rec["batting_strength"] = parts.get("batting_strength") or ""
            rec["opp_pitching"] = parts.get("opp_pitching") or ""
            rec["mlb_matchup"] = parts.get("text") or ""
        rec["l10"] = rec["l10_over"] if rec["side"] == "OVER" else rec["l10_under"]
        rec["season_hr"] = _season_hr(r)
        rec["season_n"] = _season_n(r) or 0
        rec["ml_prob"] = _flt(r.get("ml_prob") or r.get("ML Prob") or r.get("MLProb"))
        if rec["ml_prob"] is not None and rec["ml_prob"] > 1.5:
            rec["ml_prob"] = rec["ml_prob"] / 100.0
        sample_hr = _flt(r.get("last5_hit_rate") or r.get("hit_rate_L5"))
        if sample_hr is None:
            if side == "OVER":
                sample_hr = _flt(r.get("line_hit_rate_over_ou_5") or r.get("line_hit_rate_over_5"))
            else:
                sample_hr = _flt(r.get("line_hit_rate_under_ou_5") or r.get("line_hit_rate_under_5"))
        if sample_hr is None:
            l5n = rec["l5_over"] if side == "OVER" else rec["l5_under"]
            decided = rec.get("l5_decided")
            if isinstance(l5n, (int, float)) and isinstance(decided, (int, float)) and decided > 0:
                sample_hr = float(l5n) / float(decided)
            elif isinstance(l5n, (int, float)):
                sample_hr = float(l5n) / 5.0
        if sample_hr is not None and sample_hr > 1.5:
            sample_hr = sample_hr / 100.0
        rec["hit_rate"] = sample_hr if sample_hr is not None else rec["season_hr"]
        rec["standard_line"] = _flt(r.get("standard_line") or r.get("Standard Line"))
        rec["gap"] = _flt(r.get("gap") or r.get("Gap"))
        rec["line_underdog"] = _flt(r.get("line_underdog"))
        rec["line_draftkings"] = _flt(r.get("line_draftkings"))
        rec["line_vegas"] = _flt(r.get("line_vegas"))
        rec["best_cross_book"] = str(r.get("best_cross_book") or "").strip()
        rec["best_cross_line"] = _flt(r.get("best_cross_line"))
        rec["cross_edge_vs_pp"] = _flt(r.get("cross_edge_vs_pp"))
        # PrizePicks tip from step1/step8 — archive legs need this for start-date grading.
        st_raw = (
            r.get("start_time")
            or r.get("game_time")
            or r.get("Start Time")
            or r.get("Game Time")
            or ""
        )
        rec["start_time"] = str(st_raw).strip() if st_raw is not None else ""
        if rec["start_time"].lower() in {"", "nan", "none", "nat"}:
            rec["start_time"] = ""
        pp_gid = r.get("pp_game_id") or r.get("game_id") or ""
        rec["pp_game_id"] = str(pp_gid).strip() if pp_gid is not None else ""
        if rec["pp_game_id"].lower() in {"", "nan", "none"}:
            rec["pp_game_id"] = ""
        for i in range(1, 11):
            raw = r.get(f"stat_g{i}")
            if raw is None or (isinstance(raw, float) and raw != raw):
                raw = r.get(f"G{i}")
            if raw is None or raw == "" or (isinstance(raw, float) and raw != raw):
                raw = r.get(f"Stat G{i}")
            if raw is None or raw == "" or (isinstance(raw, float) and raw != raw):
                pass
            else:
                try:
                    rec[f"stat_g{i}"] = float(raw)
                except (TypeError, ValueError):
                    pass
            cur = r.get(f"current_stat_g{i}")
            if cur is None or cur == "" or (isinstance(cur, float) and cur != cur):
                cur = r.get(f"Current G{i}")
            if cur is None or cur == "" or (isinstance(cur, float) and cur != cur):
                continue
            try:
                rec[f"current_stat_g{i}"] = float(cur)
            except (TypeError, ValueError):
                continue
        src = (
            r.get("standard_line_source")
            or r.get("Standard Line Source")
            or r.get("std_line_source")
            or ""
        )
        rec["standard_line_source"] = str(src).strip() if src is not None else ""
        if rec["standard_line_source"].lower() in {"", "nan", "none"}:
            rec["standard_line_source"] = ""
        rec["promo"] = _promo(rec)
        rec.update(
            assign_tier(
                sport=rec.get("sport") or "",
                pick_type=rec.get("pick_type") or "",
                side=rec.get("side") or "",
                prop=prop,
                cover=rec.get("cover"),
                d_ok=bool((rec.get("checks") or {}).get("D") is True),
            )
        )
        if (
            rec.get("sport") == "CFB"
            and rec.get("side") == "OVER"
            and str(team).upper() in BLOWOUT_SIT_TEAMS
            and "kick" not in prop.lower()
            and "fg" not in prop.lower()
            and "pat" not in prop.lower()
        ):
            extra = "2H sit risk"
            rec["matchup_note"] = (
                f"{rec['matchup_note']} {extra}".strip() if rec.get("matchup_note") else extra
            )
        out.append(rec)
    return out


recs = recs  # diamond_2x_tickets import alias


def _clears_list_gate(r: dict) -> bool:
    """L5 >= 4 for in-season sports; WNBA family requires L5 = 5.

    NFLP uses playing-time + D instead of 2025 L5.
    MLB Goblin OVER uses keep props 1-10. MLB Standard stays off the list.
    Tennis Goblin OVER uses keep gates (Games Won Std-Goblin >=4 or
    L10>=8+(lefty|2nd-won>=45.7); Total Games L5>=4+L10>=8 or L10>=8+lefty).
    Tennis Standard UNDER Aces/DF pass ungated; Standard OVER Games Won uses
    L10>=8+(gap|2nd-won). Other tennis Standard stays off. Soccer uses keep
    gates (Shots L5=5+L10; SOT L5>=4+Off; Saves L5>=4+own D). Other soccer props
    stay off. CFB drops Goblin OVER rush_att / kick_pts; other CFB rows stay
    on L5>=4 (D badge-only).
    """
    pt = r.get("pick_type")
    side = r.get("side") or ""
    if pt == "Demon":
        return False
    sport_u = str(r.get("sport") or "").strip().upper()
    # Yes/no specials + multi-player combos stay off the O/U 5/5 list.
    if skip_combo_player(r):
        return False
    if not is_ou_ranker_prop(sport_u, r.get("prop") or r.get("prop_type") or ""):
        return False
    # Fantasy remains on the full xlsx listing (ticketable=false) but is not a
    # Standard/Goblin pool leg for tickets / best-props short stack.
    if is_fantasy_prop_label(r.get("prop") or r.get("prop_type") or ""):
        return False
    if r.get("ticketable") is False:
        return False
    if sport_u == "MLB":
        if pt == "Goblin" and side == "OVER":
            return mlb_goblin_keep_eligible(r)
        return False
    if sport_u == "TENNIS":
        return tennis_list_eligible(r)
    if sport_u in {"SOCCER", "SOC"}:
        return soccer_list_eligible(r)
    if sport_u in {"CFB", "CFB1H"} and not cfb_list_eligible(r):
        return False
    if str(r.get("sport") or "").upper() == "NFL" and is_nflp(r.get("league")):
        d_ok = bool((r.get("checks") or {}).get("D") is True)
        return nflp_list_eligible(
            policy=str(r.get("starter_policy") or ""),
            side=side,
            pick_type=pt or "Standard",
            d_ok=d_ok,
            l5_over=r.get("l5_over"),
            l5_under=r.get("l5_under"),
        )
    need = WNBA_L5_MIN if is_wnba_family(sport_u) else 4
    if pt == "Standard" and side == "OVER":
        return _directional_l5_ok(r, need, over=True)
    if pt == "Standard" and side == "UNDER":
        return _directional_l5_ok(r, need, over=False)
    if pt == "Goblin" and side == "OVER":
        return _directional_l5_ok(r, need, over=True)
    return False


def bucket(rows: list[dict], sport: str) -> tuple[list[dict], list[dict], list[dict]]:
    """Hard list gate = directional L5 >= 4 (WNBA family L5=5; NFLP: playing-time + D). D miss is badge-only except NFLP skill overs."""
    std_o, std_u, gob = [], [], []
    for r in rows:
        if r["sport"] != sport:
            continue
        if not _clears_list_gate(r):
            continue
        if r["pick_type"] == "Standard" and r["side"] == "OVER":
            std_o.append(r)
        elif r["pick_type"] == "Standard" and r["side"] == "UNDER":
            std_u.append(r)
        elif r["pick_type"] == "Goblin" and r["side"] == "OVER":
            gob.append(r)

    def dedup(lst, over: bool):
        seen = set()
        out = []
        lst = sorted(lst, key=lambda x: sort_key_tier_then_badge(x, over=over))
        for r in lst:
            k = (r["player"], r["prop"], r["line"])
            if k in seen:
                continue
            seen.add(k)
            out.append(r)
        return out

    return dedup(std_o, True), dedup(std_u, False), dedup(gob, True)


def _fmt(r: dict, side: str) -> str:
    line = r.get("line")
    prefix = "O" if side == "OVER" else "U"
    push = r.get("l5_push")
    if isinstance(push, (int, float)) and float(push) > 0:
        l5 = f"{r.get('l5_over')}/{r.get('l5_under')} P{int(push)}"
    else:
        l5 = f"{r.get('l5_over')}/{r.get('l5_under')}"
    d = r.get("def") or "no-D"
    rk = r.get("def_rank")
    d_s = f"{d}#{rk}" if rk else d
    if str(r.get("sport") or "").upper() == "MLB":
        labeled = r.get("mlb_matchup") or format_mlb_matchup(r)
        if labeled:
            d_s = labeled
    avg = r.get("season_avg")
    cover = r.get("cover")
    l5a = r.get("avg_l5")
    l10a = r.get("avg_l10")
    l5a_s = f"{l5a:5.2f}" if isinstance(l5a, (int, float)) else "  n/a"
    l10a_s = f"{l10a:5.2f}" if isinstance(l10a, (int, float)) else "  n/a"
    avg_s = f"{avg:5.2f}" if isinstance(avg, (int, float)) else "  n/a"
    if isinstance(cover, (int, float)):
        cov_s = f"{cover:+5.2f}"
    else:
        cov_s = "  n/a"
    badge = (r.get("promo") or r.get("badge") or "—")
    tier = r.get("prop_tier") or "—"
    if r.get("prop_shadow"):
        tag = f"W/{badge}"
    elif r.get("prop_promoted"):
        tag = f"{tier}/{badge}*"
    else:
        tag = f"{tier}/{badge}"
    miss = r.get("miss_s") or ""
    miss_s = f"  miss {miss}" if miss else ""
    note = str(r.get("matchup_note") or "").strip()
    note_s = f"  TIGHT {note}" if note else ""
    pol = str(r.get("starter_policy") or "").strip()
    snaps = str(r.get("expected_snaps") or "").strip()
    pol_s = ""
    if pol and pol not in ("normal", ""):
        pol_s = f"  {pol} {snaps}".rstrip()
    l8_s = "  L8bonus" if cfb_l8_bonus(r) else ""
    roster_s = ""
    if r.get("roster_changed"):
        rnote = str(r.get("roster_note") or "").strip()
        mates = str(r.get("new_teammates") or "").strip()
        if rnote:
            roster_s = f"  roster_changed({rnote})"
        else:
            roster_s = "  roster_changed"
        if mates and mates not in roster_s:
            roster_s += f"  new:{mates}"
    return (
        f"  {tag:12} {r['player']:24} {r['prop']:24} {prefix}{line}  "
        f"L5 {l5}  L5avg {l5a_s}  L10avg {l10a_s}  szn {avg_s}  cov {cov_s}  "
        f"{d_s:12}{miss_s}{note_s}{pol_s}{l8_s}{roster_s}  {r.get('matchup') or ''}"
    )


def _row_for_xlsx(r: dict, category: str) -> dict:
    side = r.get("side") or "OVER"
    prefix = "O" if side == "OVER" else "U"
    line = r.get("line")
    ticketable = r.get("ticketable")
    if ticketable is None:
        ticketable = not is_fantasy_prop_label(r.get("prop") or r.get("prop_type") or "")
    else:
        ticketable = bool(ticketable) and not is_fantasy_prop_label(
            r.get("prop") or r.get("prop_type") or ""
        )
    return {
        "Sport": r.get("sport") or "",
        "Prop_tier": r.get("prop_tier") or "",
        "Prop_tier_base": r.get("prop_tier_base") or "",
        "Shadow": bool(r.get("prop_shadow")),
        "Promoted": bool(r.get("prop_promoted")),
        "Promote_reason": r.get("prop_promote_reason") or "",
        "Badge": r.get("badge") or "",
        "Promo": r.get("promo") or r.get("badge") or "",
        "L10": r.get("l10"),
        "Season_HR": None if r.get("season_hr") is None else round(100 * r["season_hr"], 1),
        "Season_N": r.get("season_n") or 0,
        "Category": category,
        "Pick": r.get("pick_type") or "",
        "Player": r.get("player") or "",
        "Prop": r.get("prop") or "",
        "Side": side,
        "Line": f"{prefix}{line}",
        "Line_num": line,
        "L5_over": r.get("l5_over"),
        "L5_under": r.get("l5_under"),
        "L5_push": r.get("l5_push"),
        "L5_decided": r.get("l5_decided"),
        "Thin_decided": r.get("thin_decided"),
        "L5_avg": r.get("avg_l5"),
        "L10_avg": r.get("avg_l10"),
        "Season_avg": r.get("avg_season"),
        "Dist_L5": r.get("dist_l5"),
        "Dist_L10": r.get("dist_l10"),
        "Dist_Season": r.get("dist_season"),
        "Avg": r.get("season_avg"),
        "Cover": r.get("cover"),
        "D": r.get("def") or "",
        "D_rank": r.get("def_rank"),
        "Batting_strength": r.get("batting_strength") or "",
        "Opp_pitching": r.get("opp_pitching") or "",
        "Matchup_D": r.get("mlb_matchup") or "",
        "Misses": r.get("miss_s") or "",
        "Matchup": r.get("matchup") or "",
        "Note": r.get("matchup_note") or "",
        "League": r.get("league") or "",
        "Starter_policy": r.get("starter_policy") or "",
        "Expected_snaps": r.get("expected_snaps") or "",
        "Ticketable": bool(ticketable),
        "Roster_changed": bool(r.get("roster_changed")),
        "Roster_note": r.get("roster_note") or "",
        "New_teammates": r.get("new_teammates") or "",
    }


def sport_rows_for_xlsx(sport: str, std_o, std_u, gob, all_recs=None) -> list[dict]:
    rows: list[dict] = []
    for r in std_o:
        if skip_earned_runs(r):
            continue
        rows.append(_row_for_xlsx(r, "Standard OVER"))
    for r in std_u:
        if skip_earned_runs(r):
            continue
        rows.append(_row_for_xlsx(r, "Standard UNDER"))
    for r in gob:
        if skip_earned_runs(r):
            continue
        rows.append(_row_for_xlsx(r, "Goblin OVER"))
    # Fantasy stays on the full listing with Ticketable=false (not a silent drop).
    sport_u = str(sport or "").strip().upper()
    seen = {(r.get("Player"), r.get("Prop"), r.get("Line")) for r in rows}
    for r in all_recs or []:
        if str(r.get("sport") or "").strip().upper() != sport_u:
            continue
        if not is_fantasy_prop_label(r.get("prop") or r.get("prop_type") or ""):
            continue
        x = _row_for_xlsx(r, "Fantasy (ticketable=false)")
        x["Ticketable"] = False
        key = (x.get("Player"), x.get("Prop"), x.get("Line"))
        if key in seen:
            continue
        seen.add(key)
        rows.append(x)
    return rows


def write_best_props_xlsx(path: Path, by_sport: dict[str, list[dict]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    play = []
    all_rows = []
    for sport, rows in by_sport.items():
        all_rows.extend(rows)
        play.extend(
            [
                r
                for r in rows
                if r.get("Promo") in ("Diamond", "Platinum")
                or r.get("Badge") in ("Gold", "Silver")
            ]
        )
    from prop_hit_tiers import TIER_RANK, PROMO_RANK

    def sort_key(r):
        return (
            TIER_RANK.get(r.get("Prop_tier") or "", 9),
            PROMO_RANK.get(r.get("Promo") or r.get("Badge") or "", 9),
            str(r.get("Sport") or ""),
            str(r.get("Category") or ""),
            -float(r.get("Cover") or 0),
            str(r.get("Player") or ""),
        )

    play_df = pd.DataFrame(sorted(play, key=sort_key))
    all_df = pd.DataFrame(sorted(all_rows, key=sort_key))
    sa_rows = [
        r
        for r in all_rows
        if (r.get("Prop_tier") or "") in ("S", "A")
    ]
    sa_df = pd.DataFrame(sorted(sa_rows, key=sort_key))
    sheets = {
        "S+A hot": sa_df if not sa_df.empty else pd.DataFrame({"note": ["none"]}),
        "Play list (Gold+Silver)": play_df if not play_df.empty else pd.DataFrame({"note": ["none"]}),
        "All L5 4+": all_df if not all_df.empty else pd.DataFrame({"note": ["none"]}),
    }
    for sport, rows in by_sport.items():
        sdf = pd.DataFrame(sorted(rows, key=sort_key))
        if sdf.empty:
            sdf = pd.DataFrame({"note": [f"{sport}: none that clear L5 4+"]})
        sheets[sport[:31]] = sdf
    write_excel_sheets(path, sheets)


def _print_capped(rows: list[dict], side: str, limit: int | None) -> None:
    """Print list-gate rows. If capped, say which props were cut (FGA/FT used to vanish here)."""
    shown = rows if limit is None else rows[:limit]
    for r in shown:
        print(_fmt(r, side))
    if limit is None or len(rows) <= limit:
        return
    rest = rows[limit:]
    counts = Counter(str(r.get("prop") or "") for r in rest)
    bits = ", ".join(f"{p} x{n}" for p, n in counts.most_common(12))
    print(f"  … {len(rest)} more L5≥4 not shown ({bits})")


def print_sport(sport: str, std_o, std_u, gob, n_o=None, n_u=None, n_g=None) -> None:
    listed = std_o + std_u + gob
    n_dia = sum(1 for r in listed if r.get("promo") == "Diamond")
    n_plat = sum(1 for r in listed if r.get("promo") == "Platinum")
    n_gold = sum(1 for r in listed if r.get("badge") == "Gold")
    n_sil = sum(1 for r in listed if r.get("badge") == "Silver")
    n_brz = sum(1 for r in listed if r.get("badge") == "Bronze")
    n_tier = {t: sum(1 for r in listed if r.get("prop_tier") == t) for t in "SABCDW"}
    print(
        f"\n===== {sport} =====  "
        f"S {n_tier['S']}  A {n_tier['A']}  B {n_tier['B']}  "
        f"C {n_tier['C']}  D {n_tier['D']}  W {n_tier['W']}  |  "
        f"Diamond {n_dia}  Platinum {n_plat}  "
        f"Gold {n_gold}  Silver {n_sil}  Bronze {n_brz}"
    )
    empty = (
        "  (none that clear L5 4+ / NFLP playing-time gate)"
        if sport == "NFL"
        else "  (none that clear L5 4+)"
    )
    print(f"Standard OVER  (n={len(std_o)})")
    if not std_o:
        print(empty)
    else:
        _print_capped(std_o, "OVER", n_o)
    print(f"Standard UNDER (n={len(std_u)})")
    if not std_u:
        print(empty)
    else:
        _print_capped(std_u, "UNDER", n_u)
    vis = [r for r in gob if not skip_earned_runs(r)]
    print(f"Goblin OVER    (n={len(vis)})")
    if not vis:
        print(empty)
        return

    hot = [r for r in vis if r.get("prop_tier") in ("S", "A")]
    other = [r for r in vis if r.get("prop_tier") not in ("S", "A")]
    _print_capped(hot + other, "OVER", None if n_g is None else (len(hot) + n_g))


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="Slate date YYYY-MM-DD")
    ap.add_argument(
        "--step8-root",
        default="",
        help="Repo with outputs/<date>/*/step8 (default: this repo, then PropORACLE_main_cp)",
    )
    ap.add_argument(
        "--xlsx",
        default="",
        help="Write Gold/Silver/Bronze lists to this .xlsx (default: outputs/<date>/best_props_<date>.xlsx)",
    )
    args = ap.parse_args()
    date = str(args.date).strip()[:10]
    if args.step8_root:
        root = Path(args.step8_root)
        if not root.is_dir():
            print(f"--step8-root not a directory: {root}")
            return 1
    else:
        candidates = [_REPO]
        main_cp = _REPO.parent / "PropORACLE_main_cp"
        if main_cp.is_dir():
            candidates.append(main_cp)
        root = _choose_step8_root(candidates, date)
    if root is None:
        print("No step8 CSVs found for", date)
        return 1
    same_day, latest = _root_board_freshness(root, date)
    print(
        f"Best props {date}  step8={root}  "
        f"same-day step1={same_day}/{len(_STEP1_FILES)}  newest_fetch={latest:%Y-%m-%d %H:%M}"
    )
    if same_day == 0:
        print(
            "  WARN: no step1 board was fetched on this slate date in the chosen root "
            "(likely a day-prior board). Re-run after 8AM refresh on PropORACLE_main_cp."
        )
    all_rows: list[dict] = []
    by_sport: dict[str, list[dict]] = {}
    for sport, folder, fname in SPORTS:
        df = load_sport(root, date, sport, folder, fname)
        if df.empty:
            print(f"\n===== {sport} =====")
            print(step8_empty_reason(root, date, sport, folder, fname))
            by_sport[sport] = []
            continue
        all_rows.extend(recs(df))
        so, su, gob = bucket(all_rows, sport)
        print_sport(sport, so, su, gob)
        by_sport[sport] = sport_rows_for_xlsx(sport, so, su, gob, all_rows)
    df_nfl = load_nfl(root, date)
    if df_nfl.empty:
        print(
            "\n===== NFL =====\n"
            + step8_empty_reason(
                root, date, "NFL", "nfl", "step8_nfl_direction_clean.xlsx"
            )
        )
        by_sport["NFL"] = []
    else:
        all_rows.extend(recs(df_nfl))
        so, su, gob = bucket(all_rows, "NFL")
        print_sport("NFL", so, su, gob)
        by_sport["NFL"] = sport_rows_for_xlsx("NFL", so, su, gob, all_rows)
    df_nhl = load_nhl(root, date)
    if df_nhl.empty:
        print(
            "\n===== NHL =====\n"
            + step8_empty_reason(
                root, date, "NHL", "nhl", "step8_nhl_direction_clean.csv"
            )
        )
        by_sport["NHL"] = []
    else:
        # Shots-log vs card mismatch is a known caution — still print the list.
        print(
            "  [WARN] NHL: re-check SOG/Blocks lines vs PrizePicks card "
            "(shots log historically drifted); L5 alignment not auto-verified."
        )
        all_rows.extend(recs(df_nhl))
        so, su, gob = bucket(all_rows, "NHL")
        print_sport("NHL", so, su, gob)
        by_sport["NHL"] = sport_rows_for_xlsx("NHL", so, su, gob, all_rows)
    df_cfb = load_cfb(root, date)
    if df_cfb.empty:
        print(
            "\n===== CFB =====\n"
            + step8_empty_reason(
                root, date, "CFB", "cfb", "step8_cfb_direction_clean.xlsx"
            )
        )
        by_sport["CFB"] = []
    else:
        all_rows.extend(recs(df_cfb))
        so, su, gob = bucket(all_rows, "CFB")
        print_sport("CFB", so, su, gob, n_o=20, n_u=20, n_g=20)
        by_sport["CFB"] = sport_rows_for_xlsx("CFB", so, su, gob, all_rows)
    df_cfb1h = load_cfb1h(root, date)
    if df_cfb1h.empty:
        print(
            "\n===== CFB1H =====\n"
            + step8_empty_reason(
                root, date, "CFB1H", "cfb1h", "step8_cfb1h_direction_clean.xlsx"
            )
        )
        by_sport["CFB1H"] = []
    else:
        all_rows.extend(recs(df_cfb1h))
        so, su, gob = bucket(all_rows, "CFB1H")
        print_sport("CFB1H", so, su, gob, n_o=20, n_u=20, n_g=20)
        by_sport["CFB1H"] = sport_rows_for_xlsx("CFB1H", so, su, gob, all_rows)
    df_golf = load_golf(root, date)
    if df_golf.empty:
        print(
            "\n===== GOLF =====\n"
            + step8_empty_reason(
                root, date, "GOLF", "golf", "step8_golf_direction_clean.xlsx"
            )
        )
        by_sport["GOLF"] = []
    else:
        all_rows.extend(recs(df_golf))
        so, su, gob = bucket(all_rows, "GOLF")
        print_sport("GOLF", so, su, gob)
        by_sport["GOLF"] = sport_rows_for_xlsx("GOLF", so, su, gob, all_rows)
    for sport, loader in (
        ("CBB", load_cbb),
        ("WCBB", load_wcbb),
        ("NBA", load_nba),
        ("NBA1Q", load_nba1q),
        ("NBA1H", load_nba1h),
        ("WNBA1Q", load_wnba1q),
        ("WNBA1H", load_wnba1h),
    ):
        df_x = loader(root, date)
        if df_x.empty:
            if sport in {"CBB", "WCBB"} and _cbb_season_active(date):
                print(f"\n===== {sport} =====\n  (no step8 file)")
                by_sport[sport] = []
            continue
        all_rows.extend(recs(df_x))
        so, su, gob = bucket(all_rows, sport)
        print_sport(sport, so, su, gob)
        by_sport[sport] = sport_rows_for_xlsx(sport, so, su, gob, all_rows)
    xlsx_path = Path(str(args.xlsx).strip()) if str(args.xlsx or "").strip() else (
        root / "outputs" / date / f"best_props_{date}.xlsx"
    )
    if not xlsx_path.is_absolute():
        xlsx_path = (root / xlsx_path).resolve()
    write_best_props_xlsx(xlsx_path, by_sport)
    print(f"\nExcel -> {xlsx_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
