"""MLB Opp Def axis: hitters vs opponent pitching; pitchers vs opponent offense.

Pitcher Hits Allowed / ERA / Ks are matchups against the other team's bats
(hitting, scoring, and how often they strike out) — not against that team's
pitching. Hitter counting (TB, H+R+RBI, hits) stays vs opponent pitching.

Keep-gate display (same cuts; batting is hitting quality):
  Walks / Pitches: Own pitch Weak|Below
  ERA:             line 0.5 only (Opp scoring shown, not gated)
  Pitcher Ks:      Own pitch Elite|Above
  Hits Allowed:    Opp bats Strong|Above
  Pitching Outs:   Own pitch Elite|Above
  H+R+RBI / Hits / TB: BA>=.275 + Opp pitch Weak|Below
  Hitter Ks:       K%>=28 + Opp pitch Elite|Above
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from utils.defense_tiers import normalize_def_tier_label
from utils.prop_norm import canon_prop

_AXIS_HITS = "opp_offense_hits"
_AXIS_RUNS = "opp_offense_runs"
_AXIS_KS = "opp_offense_ks"
_AXIS_PITCHING = "opp_pitching"

_HITS_PROPS = frozenset({"hits_allowed"})
_RUNS_PROPS = frozenset({"earned_runs", "first_inning_runs"})
_KS_PROPS = frozenset({"pitcher_ks"})

_RANK_TIER = {
    _AXIS_HITS: ("OFF_HITS_RANK", "OFF_HITS_TIER"),
    _AXIS_RUNS: ("OFF_RUNS_RANK", "OFF_RUNS_TIER"),
    _AXIS_KS: ("OFF_SO_RANK", "OFF_SO_TIER"),
}

_D_COLS = (
    "OVERALL_DEF_RANK",
    "DEF_TIER",
    "def_rank",
    "def_tier",
    "opponent_def_rank",
    "opp_def_rank",
    "opponent_def_tier",
)


def _is_hitter(player_type: object) -> bool:
    pt = str(player_type or "").strip().lower()
    return "hitter" in pt or "batter" in pt


def pitcher_offense_axis(prop: object, player_type: object = "") -> str | None:
    """Which opponent-offense axis a pitcher prop uses, else None (use pitching)."""
    if _is_hitter(player_type):
        return None
    canon = canon_prop("MLB", prop)
    if canon in _HITS_PROPS:
        return _AXIS_HITS
    if canon in _RUNS_PROPS:
        return _AXIS_RUNS
    if canon in _KS_PROPS:
        return _AXIS_KS
    return None


def _row_axis(row: pd.Series | dict[str, Any]) -> str | None:
    if isinstance(row, dict):
        prop = row.get("prop_norm") or row.get("prop_type") or row.get("prop")
        ptype = row.get("player_type") or row.get("player_type_norm") or row.get("pos")
    else:
        prop = row.get("prop_norm", row.get("prop_type", row.get("prop", "")))
        ptype = row.get("player_type", row.get("player_type_norm", row.get("pos", "")))
    return pitcher_offense_axis(prop, ptype)


def overlay_pitcher_offense_matchup(df: pd.DataFrame) -> pd.DataFrame:
    """Replace opponent-pitching D with opponent-offense D on pitcher HA / ERA / Ks.

    No-ops when offense rank columns are missing (caller may blank those rows).
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if "def_axis" not in out.columns:
        out["def_axis"] = _AXIS_PITCHING
    else:
        out["def_axis"] = out["def_axis"].fillna(_AXIS_PITCHING)
        out.loc[out["def_axis"].astype(str).str.strip().eq(""), "def_axis"] = _AXIS_PITCHING

    axes = out.apply(_row_axis, axis=1)
    pitcher_mask = axes.notna()
    if not bool(pitcher_mask.any()):
        return out

    have_offense = all(
        rank_col in out.columns
        for rank_col, _tier_col in _RANK_TIER.values()
    )
    if not have_offense:
        for col in _D_COLS:
            if col in out.columns:
                out.loc[pitcher_mask, col] = ""
        out.loc[pitcher_mask, "def_axis"] = axes.loc[pitcher_mask]
        return out

    for axis, (rank_col, tier_col) in _RANK_TIER.items():
        m = pitcher_mask & axes.eq(axis)
        if not bool(m.any()):
            continue
        rank = out.loc[m, rank_col]
        tier = out.loc[m, tier_col] if tier_col in out.columns else ""
        if "OVERALL_DEF_RANK" in out.columns:
            out.loc[m, "OVERALL_DEF_RANK"] = rank
        if "def_rank" in out.columns:
            out.loc[m, "def_rank"] = rank
        if "opponent_def_rank" in out.columns:
            out.loc[m, "opponent_def_rank"] = rank
        if "opp_def_rank" in out.columns:
            out.loc[m, "opp_def_rank"] = rank
        if "DEF_TIER" in out.columns:
            out.loc[m, "DEF_TIER"] = tier
        if "def_tier" in out.columns:
            out.loc[m, "def_tier"] = tier
        if "opponent_def_tier" in out.columns:
            out.loc[m, "opponent_def_tier"] = tier
        out.loc[m, "def_axis"] = axis
    return out


# Batting strength is hitting quality: most hits/G = Strong, fewest = Weak.
# Internal OFF_HITS_TIER is the opposite scale (Weak = most hits, for pitcher OVERs).
_PROD_TO_BATTING = {
    "Weak": "Strong",
    "Below Avg": "Above Avg",
    "Avg": "Avg",
    "Above Avg": "Below Avg",
    "Elite": "Weak",
}
_HITTER_COUNTING = frozenset({"hits", "total_bases", "hits+runs+rbis", "hitter_ks"})


_KS_TO_DISPLAY = {
    "Weak": "High",
    "Below Avg": "Above Avg",
    "Avg": "Avg",
    "Above Avg": "Below Avg",
    "Elite": "Low",
}


def batting_strength_label(off_hits_tier: object) -> str:
    """Hitting quality: lots of hits → Strong, few hits → Weak."""
    t = normalize_def_tier_label(off_hits_tier)
    if not t or t == "N/A":
        return ""
    return _PROD_TO_BATTING.get(t, t)


def k_rate_label(off_so_tier: object) -> str:
    """How often the lineup strikes out: High = K-happy (easier pitcher K OVER)."""
    t = normalize_def_tier_label(off_so_tier)
    if not t or t == "N/A":
        return ""
    return _KS_TO_DISPLAY.get(t, t)


def _rank_suffix(rank: object) -> str:
    if rank is None or rank == "":
        return ""
    try:
        if isinstance(rank, float) and pd.isna(rank):
            return ""
    except (TypeError, ValueError):
        return ""
    try:
        return f"#{int(float(rank))}"
    except (TypeError, ValueError):
        return ""


def mlb_matchup_parts(row: dict[str, Any], *, def_rank: object = None) -> dict[str, str]:
    """Human labels for every MLB keep prop. Pitching stays leaky/stingy."""
    from utils.mlb_keep_gates import (
        opp_off_hits_tier,
        own_off_hits_tier,
        own_pitching_tier,
        opp_pitching_tier,
        player_slash,
    )

    sport = str(row.get("sport") or "").strip().upper()
    if sport and sport not in {"MLB", ""}:
        return {"batting_strength": "", "opp_pitching": "", "text": ""}

    prop = row.get("prop") or row.get("prop_type") or row.get("prop_norm") or ""
    ptype = row.get("player_type") or row.get("player_type_norm") or row.get("pos") or ""
    canon = canon_prop("MLB", prop)
    axis = str(row.get("def_axis") or "").strip() or (
        pitcher_offense_axis(prop, ptype) or _AXIS_PITCHING
    )
    rank = def_rank if def_rank is not None else (
        row.get("def_rank") or row.get("opp_pitching_rank") or row.get("OVERALL_DEF_RANK")
    )
    rk = _rank_suffix(rank)
    d_raw = normalize_def_tier_label(
        row.get("def") or row.get("d") or row.get("def_tier") or row.get("DEF_TIER")
    )
    bat = batting_strength_label(own_off_hits_tier(row))
    opp_bats = batting_strength_label(opp_off_hits_tier(row) or (
        d_raw if axis == _AXIS_HITS else ""
    ))
    pitch = opp_pitching_tier(row)
    own_staff = own_pitching_tier(row)

    batting_strength = ""
    opp_pitching = ""
    bits: list[str] = []

    if canon in _HITTER_COUNTING:
        batting_strength = bat
        opp_pitching = pitch or (d_raw if axis == _AXIS_PITCHING else "")
        slash = player_slash(row)
        if canon == "hitter_ks":
            kr = slash.get("k_rate")
            if kr is not None:
                bits.append(f"K% {100.0 * float(kr):.0f}")
        else:
            avg = slash.get("avg")
            if avg is not None:
                bits.append(f"BA {float(avg):.3f}".replace("0.", "."))
        if batting_strength:
            bits.append(f"Bat {batting_strength}")
        if opp_pitching:
            bits.append(f"Opp pitch {opp_pitching}{rk}")
    elif canon in {"walks_allowed", "pitches_thrown", "pitching_outs"}:
        if own_staff:
            bits.append(f"Own pitch {own_staff}")
    elif canon == "pitcher_ks" or axis == _AXIS_KS:
        if own_staff:
            bits.append(f"Own pitch {own_staff}")
        ks = k_rate_label(d_raw)
        if ks:
            bits.append(f"Opp Ks {ks}{rk}")
    elif canon == "hits_allowed" or axis == _AXIS_HITS:
        bats = opp_bats or batting_strength_label(d_raw)
        if bats:
            bits.append(f"Opp bats {bats}{rk}")
    elif canon in {"earned_runs", "first_inning_runs"} or axis == _AXIS_RUNS:
        scoring = batting_strength_label(d_raw)
        if scoring:
            bits.append(f"Opp scoring {scoring}{rk}")
        if own_staff:
            bits.append(f"Own pitch {own_staff}")
    else:
        if pitch:
            opp_pitching = pitch
            bits.append(f"Opp pitch {pitch}{rk}")
        elif d_raw:
            bits.append(f"{d_raw}{rk}")

    return {
        "batting_strength": batting_strength,
        "opp_pitching": opp_pitching,
        "text": " · ".join(bits),
    }


def format_mlb_matchup(row: dict[str, Any], *, def_rank: object = None) -> str:
    """Compact display: 'Bat Strong · Opp pitch Weak#12'."""
    return mlb_matchup_parts(row, def_rank=def_rank).get("text") or ""
