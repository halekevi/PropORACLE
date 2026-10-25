"""Attach / list / shadow guard: no L5 or L10 unless the stat attach is OK.

Rules:
  - ``stat_status`` not OK (unsupported, no history, too few games, mapping
    failure, implausible scale) → no L5/L10 counts, cannot qualify.
  - All-zero history on a volume stat for a starter → mapping failure.
  - L5 average implausible vs the posted line (YPC vs rush yards, longest
    reception as catch-count, Comp% as completion counts) → drop.
  - L5 hit counts with no games / no average behind them → drop.
  - Missing stats stay missing. Never zero-fill.
  - DNP-zero games are voided only with positive evidence
    (``utils.dnp_zero_void``).
"""
from __future__ import annotations

import math
from typing import Any, Iterable

from utils.dnp_zero_void import is_dnp_zero_actual
from utils.nfl_unsupported_props import is_football_period_split_prop, norm_football_prop

OK_PREFIXES = ("PRIOR_", "THIN_")
BLOCKING_STATUSES = frozenset(
    {
        "UNSUPPORTED_PROP",
        "UNSUPPORTED_2H",
        "NO_ID",
        "NO_DATA",
        "NO_BOX_HISTORY",
        "INSUFFICIENT_GAMES",
        "MAPPING_FAILURE",
        "IMPLAUSIBLE_L5",
        "STALE_HISTORY",
        "NO_STAT_KEY",
        "NO_ESPN_PLAYER",
        "NO_CACHE_DATA",
        "PENDING",
        "NO_MATCH",
    }
)

VOLUME_PROPS = frozenset(
    {
        "passing_yards",
        "rushing_yards",
        "receiving_yards",
        "receptions",
        "pass_completions",
        "completions",
        "pass_attempts",
        "rush_attempts",
        "kicking_points",
        "tackles",
        "tackles_ast",
        "tackles_assists",
        "sacks",
        "sacks_taken",
        "completion_percentage",
        "longest_reception",
        "longest_completion",
        "longest_rush",
        "shots",
        "shots_on_target",
        "sog",
        "shots_on_goal",
        "saves",
        "goalie_saves",
        "fantasy_score",
        "fantasy",
    }
)

L5_CLEAR_COLS = (
    "stat_last5_avg",
    "stat_last10_avg",
    "stat_season_avg",
    "last5_over",
    "last5_under",
    "last5_push",
    "last5_hit_rate",
    "line_hits_over_5",
    "line_hits_under_5",
    "line_hits_push_5",
    "line_hits_over_10",
    "line_hits_under_10",
    "line_hits_push_10",
    "line_hit_rate_over_5",
    "line_hit_rate_under_5",
    "line_hit_rate_over_ou_5",
    "line_hit_rate_under_ou_5",
    "line_hit_rate_over_ou_10",
    "line_hit_rate_under_ou_10",
    "l5_over",
    "l5_under",
    "l10_over",
    "l10_under",
    "avg_l5",
    "avg_l10",
)


def _num(raw: object) -> float | None:
    if raw is None:
        return None
    if isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float)):
        if isinstance(raw, float) and math.isnan(raw):
            return None
        return float(raw)
    s = str(raw).strip()
    if not s or s.lower() in {"nan", "none", "null", "", "—", "-", "n/a"}:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _get(row: Any, *keys: str) -> object:
    for k in keys:
        if isinstance(row, dict):
            if k in row:
                return row.get(k)
        elif hasattr(row, "get"):
            v = row.get(k)
            if v is not None and str(v).strip() not in {"", "nan", "None"}:
                return v
    return None


def _prop(row: Any) -> str:
    return norm_football_prop(
        _get(row, "prop_type_normalized", "prop_norm", "prop_type", "prop", "Prop")
    )


def attach_status_blocks_l5(status: object) -> bool:
    """True when this status must carry no L5/L10 and cannot list/gate/shadow."""
    s = str(status or "").strip().upper()
    if not s or s in {"NAN", "NONE"}:
        return False
    if s == "OK" or s.startswith(OK_PREFIXES):
        return False
    if s in BLOCKING_STATUSES:
        return True
    return True


def is_volume_stat(prop: object) -> bool:
    p = norm_football_prop(prop)
    if p in VOLUME_PROPS:
        return True
    if p.endswith("_yards") or p.endswith("_attempts"):
        return True
    if "completion" in p or "reception" in p or "tackle" in p:
        return True
    return False


def is_all_zero_history(vals: Iterable[object] | None) -> bool:
    known: list[float] = []
    for v in vals or []:
        n = _num(v)
        if n is None:
            continue
        known.append(n)
    return bool(known) and all(x == 0.0 for x in known)


def mapping_failure_all_zero(
    vals: Iterable[object] | None,
    prop: object,
    *,
    role: object = "",
) -> bool:
    """Starter + volume stat + every game 0 → mapping failure, not a real L5."""
    if not is_volume_stat(prop):
        return False
    if not is_all_zero_history(vals):
        return False
    role_s = str(role or "").strip().upper()
    # No role → still flag volume all-zero (sacks / Comp% / longest as 0.00).
    if role_s in {"BENCH", "DNP", "INACTIVE", "RESERVE"}:
        return False
    return True


def implausible_l5_vs_line(prop: object, l5_avg: object, line: object) -> str:
    """Empty string when the scale looks honest; else a short reason."""
    p = norm_football_prop(prop)
    avg = _num(l5_avg)
    ln = _num(line)
    if avg is None or ln is None:
        return ""
    if is_football_period_split_prop(p):
        return "unsupported_period_prop"
    if "per_carry" in p or p in {"ypc", "yards_per_carry"}:
        if avg > 15.0:
            return "implausible_ypc_scale"
    if "completion" in p and ("percent" in p or "pct" in p):
        if avg < 15.0 or avg > 100.0:
            return "implausible_comp_pct_scale"
    if "longest" in p and ("rec" in p or "reception" in p or "completion" in p or "rush" in p):
        if ln >= 10.0 and 0.0 <= avg <= 8.0:
            return "implausible_longest_scale"
    return ""


def l5_hits_without_games(row: Any) -> bool:
    """True when a printed L5 hit count has no games / average behind it."""
    over = _num(_get(row, "l5_over", "last5_over", "line_hits_over_5", "L5 Over"))
    under = _num(_get(row, "l5_under", "last5_under", "line_hits_under_5", "L5 Under"))
    hits = 0.0
    if over is not None:
        hits = max(hits, over)
    if under is not None:
        hits = max(hits, under)
    if hits <= 0:
        return False
    avg = _num(
        _get(
            row,
            "stat_last5_avg",
            "avg_l5",
            "avg_L5",
            "l5_avg",
            "L5 Avg",
            "last5_avg",
            "avg",
            "stat_season_avg",
            "avg_season",
            "season_avg",
        )
    )
    games = _num(_get(row, "games_used", "l5_sample_n", "Games Used", "L5 Sample N"))
    if avg is not None:
        return False
    if games is not None and games > 0:
        return False
    gcount = 0
    for i in range(1, 6):
        if _num(_get(row, f"stat_g{i}", f"G{i}")) is not None:
            gcount += 1
    if gcount > 0:
        return False
    return True


def void_dnp_zeros(vals: list, games: list | None = None) -> list:
    """Drop zeros that have positive DNP evidence. Unknown PT stays."""
    out = []
    for i, v in enumerate(vals or []):
        n = _num(v)
        if n is None:
            continue
        row: dict[str, Any] = {}
        if games and i < len(games) and isinstance(games[i], dict):
            row = games[i]
        if is_dnp_zero_actual(n, row):
            continue
        out.append(float(n) if n == n else n)
    return out


def decide_attach(
    vals: list | None,
    prop: object,
    line: object,
    *,
    role: object = "",
    min_games: int = 2,
    games: list | None = None,
) -> tuple[list | None, str]:
    """Return (kept_vals, stat_status). None vals → do not write L5/L10."""
    if is_football_period_split_prop(prop):
        return None, "UNSUPPORTED_PROP"
    cleaned = void_dnp_zeros(list(vals or []), games)
    if not cleaned:
        return None, "NO_DATA"
    if mapping_failure_all_zero(cleaned, prop, role=role):
        return None, "MAPPING_FAILURE"
    if len(cleaned) < min_games:
        return None, "INSUFFICIENT_GAMES"
    avg5 = sum(cleaned[:5]) / max(1, len(cleaned[:5]))
    why = implausible_l5_vs_line(prop, avg5, line)
    if why:
        return None, "IMPLAUSIBLE_L5"
    return cleaned, "OK"


def strip_l5_l10(row: Any) -> None:
    """Blank L5/L10 count and average fields on a mutable row."""
    for c in L5_CLEAR_COLS:
        if isinstance(row, dict):
            if c in row:
                row[c] = ""
        elif hasattr(row, "__setitem__"):
            try:
                row[c] = ""
            except Exception:
                pass


def row_qualifies_for_list(row: Any) -> tuple[bool, str]:
    """List / gate / shadow entry. Fail-closed on bad attach."""
    status = _get(row, "stat_status", "Stat Status")
    if attach_status_blocks_l5(status):
        return False, "stat_status_not_ok"
    prop = _prop(row)
    if is_football_period_split_prop(prop):
        return False, "unsupported_period_prop"
    if l5_hits_without_games(row):
        return False, "l5_hits_no_games"
    line = _num(_get(row, "line", "Line", "line_score"))
    avg = _num(
        _get(row, "stat_last5_avg", "avg_l5", "avg_L5", "l5_avg", "L5 Avg", "stat_season_avg", "avg_season")
    )
    why = implausible_l5_vs_line(prop, avg, line)
    if why:
        return False, why
    gvals = []
    for i in range(1, 6):
        gvals.append(_get(row, f"stat_g{i}"))
    if mapping_failure_all_zero(gvals, prop, role=_get(row, "position_group", "pos", "role")):
        return False, "all_zero_mapping_failure"
    if avg == 0.0 and mapping_failure_all_zero([0.0, 0.0, 0.0, 0.0, 0.0], prop):
        return False, "all_zero_mapping_failure"
    return True, ""


def history_from_row(row: Any) -> list[float]:
    vals = []
    for i in range(1, 11):
        n = _num(_get(row, f"stat_g{i}"))
        if n is not None:
            vals.append(n)
    return vals
