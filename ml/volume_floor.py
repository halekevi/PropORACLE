"""Locked helpers for the 2026-10-25 volume-floor backtests.

Definitions live in ``ml/prereg/volume_floor_backtest_2026-10-25.json``.
This module does not import or edit the three frozen forward tracks.
"""
from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Iterable

_REPO = Path(__file__).resolve().parents[1]
_PREREG_JSON = _REPO / "ml" / "prereg" / "volume_floor_backtest_2026-10-25.json"

VOLUME_CANONS: dict[str, frozenset[str]] = {
    "NBA": frozenset({"fga", "fg2a", "threes_att", "fta"}),
    "WNBA": frozenset({"fga", "fg2a", "threes_att", "fta"}),
    "NBA1H": frozenset({"fga", "fg2a", "threes_att", "fta"}),
    "WNBA1H": frozenset({"fga", "fg2a", "threes_att", "fta"}),
    "CBB": frozenset({"fga", "fg2a", "threes_att", "fta"}),
    "MLB": frozenset({"pitches_thrown"}),
    "NFL": frozenset(
        {"receptions", "rec", "rush_att", "rush_attempts", "rushattempts", "rec_tgt"}
    ),
    "CFB": frozenset({"rec", "rush_att", "rec_tgt"}),
    "Soccer": frozenset({"shots"}),
}
BOOKS_A = ("Goblin OVER", "Standard OVER", "Demon OVER")
GATE_ATOMS = ("L5=5", "L10>=8", "D_ok")
RANK_MAX = 2
MIN_TGT_SHARE_PP = 15.0
L5_GAMES_MIN = 5
MIN_N = 50
BH_ALPHA = 0.10
BREAK_EVEN = {
    "Goblin OVER": 0.846,
    "Standard OVER": 0.577,
    "Demon OVER": None,
}
FROZEN_TRACKS = (
    "nfl_std_under_shadow",
    "nfl_demon_volume_shadow",
    "nfl_demon_rec_floor_shadow",
)
PROD = frozenset({"Weak", "Below Avg"})


def load_prereg(*, path: Path | None = None) -> dict[str, Any]:
    dest = path or _PREREG_JSON
    return json.loads(dest.read_text(encoding="utf-8"))


def assert_prereg(*, path: Path | None = None) -> dict[str, Any]:
    spec = load_prereg(path=path)
    assert spec["track"] == "volume_floor_backtest"
    assert spec["prereg_date"] == "2026-10-25"
    assert spec["honest_ev"] is False
    assert spec["frozen_forward_tracks_do_not_edit"] == list(FROZEN_TRACKS)
    assert spec["A"]["min_n"] == MIN_N
    assert spec["A"]["bh_alpha"] == BH_ALPHA
    assert spec["role"]["rank_max"] == RANK_MAX
    assert float(spec["role"]["receptions_min_share_pp"]) == MIN_TGT_SHARE_PP
    return spec


def is_volume_prop(sport: str, prop: str) -> bool:
    allowed = VOLUME_CANONS.get(str(sport or "").strip())
    if not allowed:
        return False
    key = str(prop or "").strip().lower()
    return key in allowed


def _num(raw: object) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    return v


def directional_l5(row: dict[str, Any]) -> float | None:
    return _num(row.get("l5_over"))


def directional_l10(row: dict[str, Any]) -> float | None:
    return _num(row.get("l10_over"))


def d_ok_over(row: dict[str, Any]) -> bool:
    raw = str(row.get("def_tier") or "").strip()
    return raw in PROD


def atoms(row: dict[str, Any]) -> dict[str, bool]:
    l5 = directional_l5(row)
    l10 = directional_l10(row)
    return {
        "L5=5": l5 is not None and l5 == 5,
        "L10>=8": l10 is not None and l10 >= 8,
        "D_ok": d_ok_over(row),
    }


def passes_gate(row: dict[str, Any]) -> bool:
    a = atoms(row)
    return all(a[k] for k in GATE_ATOMS)


def floor_over(row: dict[str, Any]) -> bool:
    """OVER line strictly below the L5 min == directional L5 = 5."""
    return bool(atoms(row)["L5=5"])


def usage_share(row: dict[str, Any]) -> float | None:
    for key in ("usage_pct", "target_share_L3", "tgt_share", "rec_tgt_share"):
        v = _num(row.get(key))
        if v is None:
            continue
        return v * 100.0 if v <= 1.5 else v
    rank = _num(row.get("team_top3_rank"))
    if rank is None:
        return None
    # Invert rank so #1 sorts highest. Only used when a real share is missing.
    return 100.0 - rank


def team_share_ranks(rows: Iterable[dict[str, Any]]) -> dict[tuple[str, str, str], int]:
    """(date, team, player_norm) -> rank (1 = highest share)."""
    by: dict[tuple[str, str], dict[str, float]] = {}
    for row in rows:
        share = usage_share(row)
        team = str(row.get("team") or "").strip().upper()
        player = str(row.get("player_norm") or "").strip()
        date = str(row.get("date") or "")[:10]
        if share is None or not team or not player or not date:
            continue
        slot = by.setdefault((date, team), {})
        prev = slot.get(player)
        if prev is None or share > prev:
            slot[player] = share
    ranks: dict[tuple[str, str, str], int] = {}
    for (date, team), pmap in by.items():
        ordered = sorted(pmap.items(), key=lambda kv: kv[1], reverse=True)
        for i, (player, _s) in enumerate(ordered, start=1):
            ranks[(date, team, player)] = i
    return ranks


def top2(row: dict[str, Any], ranks: dict[tuple[str, str, str], int]) -> bool:
    key = (
        str(row.get("date") or "")[:10],
        str(row.get("team") or "").strip().upper(),
        str(row.get("player_norm") or "").strip(),
    )
    rank = ranks.get(key)
    return rank is not None and rank <= RANK_MAX


def wilson_low(hits: int, n: int, z: float = 1.96) -> float | None:
    if n <= 0:
        return None
    p = hits / n
    z2 = z * z
    denom = 1 + z2 / n
    center = p + z2 / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z2 / (4 * n)) / n)
    return round((center - spread) / denom, 4)


def rate(hits: int, n: int) -> float | None:
    if n <= 0:
        return None
    return round(hits / n, 4)


def cell_stats(hits: int, n: int, *, breakeven: float | None) -> dict[str, Any]:
    hr = rate(hits, n)
    wlo = wilson_low(hits, n)
    vs = None
    if hr is not None and breakeven is not None:
        vs = round(hr - breakeven, 4)
    return {
        "hits": hits,
        "n": n,
        "hr": hr,
        "wilson_low": wlo,
        "breakeven": breakeven,
        "hr_minus_breakeven": vs,
        "thin": n < MIN_N,
        "wilson_clears_be": bool(
            wlo is not None and breakeven is not None and n >= MIN_N and wlo >= breakeven
        ),
    }


def binom_p_greater(hits: int, n: int, p0: float) -> float | None:
    if n <= 0 or p0 is None:
        return None
    try:
        from scipy.stats import binomtest

        return float(binomtest(hits, n, p=p0, alternative="greater").pvalue)
    except Exception:
        return None


def bh_keep(pvals: list[float | None], alpha: float = BH_ALPHA) -> set[int]:
    indexed = [(i, p) for i, p in enumerate(pvals) if p is not None]
    m = len(indexed)
    if m == 0:
        return set()
    indexed.sort(key=lambda t: t[1])
    cutoff = 0
    for rank, (_i, p) in enumerate(indexed, 1):
        if p <= (rank / m) * alpha:
            cutoff = rank
    return {indexed[j][0] for j in range(cutoff)}


def oos_split_dates(dates: list[str]) -> tuple[set[str], set[str]]:
    dates = sorted(set(dates))
    if len(dates) <= 1:
        return set(dates), set(dates)
    if len(dates) <= 4:
        return set(dates[:-1]), {dates[-1]}
    cut = max(1, int(round(len(dates) * 0.70)))
    return set(dates[:cut]), set(dates[cut:])


def l5_min(values: list[float | None]) -> float | None:
    nums = [v for v in values if v is not None]
    if len(nums) < L5_GAMES_MIN:
        return None
    return min(nums[:L5_GAMES_MIN])


def persistence_hit(actual: float | None, floor: float | None) -> bool | None:
    if actual is None or floor is None:
        return None
    return actual >= floor
