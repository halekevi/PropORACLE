"""Power-6 Standard buckets — log-only until hit-rate voids are trusted.

Do **not** drop long_parlay tickets from these buckets. The post-void table
looked too good (80%+ Standards); until DNP voids use positive evidence only
and leakage / true-Standard checks pass, `annotate_long_parlay_power6_buckets`
records what the filter *would* do without removing slips.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[1]
_RATES = _REPO / "data" / "reports" / "standard_l5eq5_hit_rates.json"

# Break-even for independent 6-leg Power at 37.5x.
BE_6PICK = 0.547

# Empty while the table is held — do not fall back to the inflated set.
FALLBACK_POWER6_BUCKETS: frozenset[tuple[str, str]] = frozenset()

# Full-game Standards that must not enter Power-6 even if a cell clears.
POWER6_BLOCKED_SPORTS: frozenset[str] = frozenset({"NBA", "WNBA", "WNBA1H", "WNBA1Q"})

# Max legs for blocked sports on Standard slips (2–3 picks).
STD_SHORT_MAX_LEGS = 3

# Tennis UNDER stays unproven at small n regardless of rate.
UNPROVEN_BUCKETS: frozenset[tuple[str, str]] = frozenset({("TENNIS", "UNDER")})


def _norm_sport(s: object) -> str:
    u = str(s or "").strip().upper()
    if u in {"SOC", "FOOTBALL"}:
        return "SOCCER"
    return u


def _norm_dir(s: object) -> str:
    d = str(s or "").strip().upper()
    if d in {"O", "OVER"}:
        return "OVER"
    if d in {"U", "UNDER"}:
        return "UNDER"
    return d


def load_recommended_power6_buckets(
    path: Path | None = None,
) -> frozenset[tuple[str, str]]:
    """Read recommended buckets from the latest hit-rate report (informational)."""
    p = path or _RATES
    if not p.is_file():
        return FALLBACK_POWER6_BUCKETS
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return FALLBACK_POWER6_BUCKETS
    sources = raw.get("sources") or []
    primary = sources[0] if sources else raw
    # Prefer an explicit trusted list; otherwise nothing (log-only hold).
    if primary.get("power6_trusted") is not True:
        return FALLBACK_POWER6_BUCKETS
    rows = primary.get("recommended_6pick_buckets") or primary.get(
        "buckets_ci_low_above_be"
    )
    if not rows:
        return FALLBACK_POWER6_BUCKETS
    out: set[tuple[str, str]] = set()
    for r in rows:
        if not isinstance(r, dict):
            continue
        sp = _norm_sport(r.get("sport"))
        direction = _norm_dir(r.get("direction"))
        if sp in POWER6_BLOCKED_SPORTS:
            continue
        if (sp, direction) in UNPROVEN_BUCKETS:
            continue
        if direction in {"OVER", "UNDER"}:
            out.add((sp, direction))
    return frozenset(out)


def power6_bucket_ok(
    sport: object,
    direction: object,
    *,
    buckets: frozenset[tuple[str, str]] | None = None,
) -> bool:
    """True when this Standard leg may enter a Power-6 / long_parlay slip."""
    sp = _norm_sport(sport)
    if sp in POWER6_BLOCKED_SPORTS:
        return False
    if (_norm_sport(sp), _norm_dir(direction)) in UNPROVEN_BUCKETS:
        return False
    allowed = buckets if buckets is not None else load_recommended_power6_buckets()
    if not allowed:
        return False
    return (_norm_sport(sp), _norm_dir(direction)) in allowed


def team_changed_blocks_power_l5(row: dict[str, Any]) -> bool:
    """NBA team_changed with <5 games on new club — out of L5=5 power plays."""
    if str(row.get("team_changed") or "").strip() in {"1", "true", "True"}:
        try:
            n = int(row.get("games_with_new_team") or 0)
        except (TypeError, ValueError):
            n = 0
        try:
            need = int(row.get("min_new_team_games") or 5)
        except (TypeError, ValueError):
            need = 5
        return n < need
    if row.get("power_l5_blocked"):
        return True
    return False


def annotate_long_parlay_power6_buckets(
    payload: dict[str, Any],
    *,
    buckets: frozenset[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Log-only: record which Standard long slips fail the bucket gate.

    Does **not** drop tickets. Wiring an inflated table into the builder is how
    a measurement bug becomes lost money.
    """
    if not isinstance(payload, dict):
        return payload
    allowed = buckets if buckets is not None else load_recommended_power6_buckets()
    out = dict(payload)
    would_drop = 0
    would_keep = 0
    sample_dropped: list[dict[str, Any]] = []
    for g in out.get("groups") or []:
        if not isinstance(g, dict):
            continue
        for t in g.get("tickets") or []:
            if not isinstance(t, dict):
                continue
            legs = t.get("legs") or t.get("picks") or []
            if not isinstance(legs, list) or not legs:
                continue
            std_legs = [
                leg
                for leg in legs
                if isinstance(leg, dict)
                and "standard"
                in str(leg.get("pick_type") or leg.get("Pick Type") or "").lower()
            ]
            if not std_legs:
                continue
            ok = True
            fail_reasons: list[str] = []
            for leg in std_legs:
                if team_changed_blocks_power_l5(leg):
                    ok = False
                    fail_reasons.append("team_changed")
                    break
                sport = leg.get("sport") or leg.get("Sport") or g.get("sport")
                direction = (
                    leg.get("direction")
                    or leg.get("side")
                    or leg.get("bet_direction")
                    or leg.get("Direction")
                )
                if not power6_bucket_ok(sport, direction, buckets=allowed):
                    ok = False
                    fail_reasons.append(
                        f"bucket:{_norm_sport(sport)}|{_norm_dir(direction)}"
                    )
                    break
            if ok:
                would_keep += 1
            else:
                would_drop += 1
                if len(sample_dropped) < 8:
                    sample_dropped.append(
                        {
                            "group": g.get("group_name") or g.get("sport"),
                            "reasons": fail_reasons,
                            "n_std_legs": len(std_legs),
                        }
                    )
    out["power6_bucket_filter"] = {
        "mode": "log_only",
        "trusted": bool(allowed),
        "allowed": sorted(f"{s}|{d}" for s, d in allowed),
        "tickets_would_drop": would_drop,
        "tickets_would_keep": would_keep,
        "tickets_dropped": 0,
        "sample_would_drop": sample_dropped,
        "note": (
            "Buckets are log-only until DNP voids use positive evidence only "
            "and leakage / true-Standard checks clear. Tennis UNDER unproven."
        ),
    }
    return out


def filter_long_parlay_groups_to_power6_buckets(
    payload: dict[str, Any],
    *,
    buckets: frozenset[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    """Deprecated alias — log-only; does not drop tickets."""
    return annotate_long_parlay_power6_buckets(payload, buckets=buckets)
