#!/usr/bin/env python3
"""Best-gate soccer keeps x offense/defense alignment (aligned vs opposing).

Keep / best gates:
  Shots            L5=5 + L10>=8
  Shots On Target  L5>=4 + Off
  Goalie Saves     L5>=4 + own D (live keep: own only, no opp-D fallback)

Attacking: OVER wants leaky opp D + strong own Off
           UNDER wants stingy opp D + weak own Off
Saves:     OVER wants leaky own D + strong opp Off
           UNDER wants stingy own D + weak opp Off

Yellow flag (2026-09-24 rebuilt-history run):
  Saves OVER "best gate" looked ~64%, but own-D-blank rows falling back to
  legacy opp-D were ~70% (n=61) while own-leaky OVER was 5/14=36% and
  ALIGNED own-leaky + opp-Off-strong was 0/7. Do NOT revert the live gate
  yet — most own-D rows are backfilled joins (~6% fill), and very leaky D
  can suppress saves (goals instead of stops). Recheck with:

    py -3.14 scripts/_soccer_best_gate_od_alignment.py --forward-from 2026-09-15 --own-d-only

  (GF/OWN_DEF step3 fill landed ~2026-09-15.) If own-leaky OVER is still
  sub-50% at n>=30-40 on forward-only rows, reconsider Saves signal
  (e.g. shots-faced volume vs raw leaky/stingy).

Watchlist (coincidence until bigger n):
  SOT OVER stingy D + weak team Off 9/13=69% vs stingy D + strong team Off
  1/4=25%. Log only — do not gate on it.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from export_soccer_combo_gates import (  # noqa: E402
    _dir_count,
    _name_key,
    _off_ok,
    _pos_ok,
    _prop_key,
    load_step6,
    load_top3,
)
from export_soccer_hit_rates_excel import _d_pass, _norm_def, load_graded  # noqa: E402
from _soccer_d_off_alignment import (  # noqa: E402
    load_s6_matchup,
    team_profiles,
)

WEAK_D = {"Weak", "Below Avg"}
STINGY_D = {"Elite", "Above Avg"}
STRONG_OFF = {"Elite", "Above Avg"}
WEAK_OFF = {"Weak", "Below Avg"}
# Step3 GF / OWN_DEF_TIER fill landed with the Saves polarity flip.
_DEFAULT_FORWARD_FROM = "2026-09-15"


def _fmt(hits: int, n: int) -> str:
    if n <= 0:
        return "—"
    return f"{hits}/{n}={100 * hits / n:.1f}%"


def _pack(label: str, s: pd.DataFrame) -> tuple[str, int, int, float | None]:
    n = int(len(s))
    hits = int(s["hit"].sum()) if n else 0
    return label, hits, n, (hits / n) if n else None


def build_gate_frame() -> pd.DataFrame:
    print("Loading step6 boards…")
    s6 = load_step6()
    top3 = load_top3()
    print(f"  step6 keys {len(s6):,}  top3 {len(top3):,}")

    print("Loading graded soccer…")
    g = load_graded()
    g["player_key"] = g["player"].map(_name_key)
    g["join_date"] = g["file_date"]
    g["prop_key"] = g["prop"].map(_prop_key).replace({"saves": "goalie saves"})

    merged = g.merge(
        s6.drop(columns=["player"], errors="ignore"),
        on=["player_key", "prop_key", "pick_type", "join_date"],
        how="left",
    )
    matched = (
        merged.get("l10_over_s6", pd.Series(dtype=float)).notna()
        | merged.get("l5_over_s6", pd.Series(dtype=float)).notna()
    ).mean()
    print(f"  graded {len(g):,}  s6 matched {matched:.0%}")

    l5_s6 = _dir_count(merged.get("l5_over_s6"), merged.get("l5_under_s6"), merged["direction"])
    merged["l5_use"] = merged["l5"].where(merged["l5"].notna(), l5_s6)
    merged["l10_use"] = _dir_count(
        merged.get("l10_over_s6"), merged.get("l10_under_s6"), merged["direction"]
    )

    def_s6 = merged.get("def_s6", pd.Series("", index=merged.index)).fillna("")
    def_use = np.where(
        def_s6.astype(str).str.len() > 1,
        def_s6,
        merged["def_tier"].replace("—", ""),
    )
    merged["def_use"] = pd.Series(def_use, index=merged.index).map(_norm_def)
    merged["g_d"] = [
        _d_pass(d, t) for d, t in zip(merged["direction"], merged["def_use"], strict=False)
    ]
    merged["off_top3"] = [
        ((k, p) in top3) for k, p in zip(merged["player_key"], merged["prop_key"], strict=False)
    ]
    merged["g_off"] = _off_ok(merged)
    pos = merged.get("pos_group", pd.Series("UNMAPPED", index=merged.index)).fillna("UNMAPPED")
    merged["g_pos"] = [_pos_ok(pk, p) for pk, p in zip(merged["prop_key"], pos, strict=False)]
    merged["g_l5"] = pd.to_numeric(merged["l5_use"], errors="coerce").ge(4)
    merged["g_l5eq5"] = pd.to_numeric(merged["l5_use"], errors="coerce").eq(5)
    merged["g_l10"] = pd.to_numeric(merged["l10_use"], errors="coerce").ge(8)

    print("Building team O/D profiles…")
    s6m = load_s6_matchup()
    prof = team_profiles(s6m)
    # Prefer step6 team/opp (graded JSON often lacks opp_team).
    team_col = next(
        (c for c in ("team", "team_s6", "Team") if c in merged.columns), None
    )
    opp_col = next(
        (c for c in ("opp_team", "opp_team_s6", "opponent", "Opp") if c in merged.columns),
        None,
    )
    league_col = next((c for c in ("league", "league_s6") if c in merged.columns), None)
    merged["team_key"] = (
        merged[team_col].map(_name_key) if team_col else pd.Series("", index=merged.index)
    )
    merged["opp_key"] = (
        merged[opp_col].map(_name_key) if opp_col else pd.Series("", index=merged.index)
    )
    merged["league"] = (
        merged[league_col].astype(str) if league_col else pd.Series("", index=merged.index)
    )
    # Date+team join (drop league — graded/step6 league labels often disagree).
    own = prof.rename(
        columns={
            "d_tier": "own_d",
            "shots_d": "own_shots_d",
            "off_tier": "own_off",
            "win_pct": "own_win_pct",
        }
    )
    opp = prof.rename(
        columns={
            "team_key": "opp_key",
            "d_tier": "opp_d_prof",
            "shots_d": "opp_shots_d_prof",
            "off_tier": "opp_off",
            "win_pct": "opp_win_pct",
        }
    )
    own_m = (
        own.groupby(["join_date", "team_key"], dropna=False)
        .agg(
            own_d=("own_d", "first"),
            own_shots_d=("own_shots_d", "first"),
            own_off=("own_off", "first"),
            own_win_pct=("own_win_pct", "first"),
        )
        .reset_index()
    )
    opp_m = (
        opp.groupby(["join_date", "opp_key"], dropna=False)
        .agg(
            opp_off=("opp_off", "first"),
            opp_win_pct=("opp_win_pct", "first"),
            opp_d_prof=("opp_d_prof", "first"),
        )
        .reset_index()
    )
    merged = merged.merge(own_m, on=["join_date", "team_key"], how="left")
    merged = merged.merge(opp_m, on=["join_date", "opp_key"], how="left")
    merged["own_d"] = merged.get("own_d", pd.Series("", index=merged.index)).fillna("").map(
        _norm_def
    )
    merged["own_off"] = merged.get("own_off", pd.Series("", index=merged.index)).fillna("").astype(
        str
    )
    merged["opp_off"] = merged.get("opp_off", pd.Series("", index=merged.index)).fillna("").astype(
        str
    )
    shots = merged.get("shots_def_tier", pd.Series("", index=merged.index)).map(_norm_def)
    merged["opp_shots_d"] = np.where(
        shots.astype(str).str.len() > 0, shots, merged["def_use"]
    )
    # Player-usage Off (what SOT keep gate already uses).
    merged["player_off"] = np.where(merged["g_off"], "HIGH", "LOW")

    base = merged[merged["list_shape"]].copy()
    print(
        f"  list-shape pool {len(base):,}  "
        f"team={base['team_key'].astype(str).str.len().gt(0).mean():.0%}  "
        f"opp={base['opp_key'].astype(str).str.len().gt(0).mean():.0%}  "
        f"own_off={(base['own_off'].astype(str).str.len()>0).mean():.0%}  "
        f"opp_off={(base['opp_off'].astype(str).str.len()>0).mean():.0%}  "
        f"own_d={base['own_d'].isin(WEAK_D|STINGY_D|{'Avg'}).mean():.0%}"
    )
    return base


def best_gate_mask(
    df: pd.DataFrame,
    prop_key: str,
    *,
    saves_own_d_only: bool = True,
) -> pd.Series:
    sub = df["prop_key"].eq(prop_key)
    if prop_key == "shots":
        return sub & df["g_l5eq5"] & df["g_l10"]
    if prop_key == "shots on target":
        return sub & df["g_l5"] & df["g_off"]
    if prop_key == "goalie saves":
        own_filled = df["own_d"].isin(WEAK_D | STINGY_D)
        own_ok = pd.Series(
            [
                _d_pass(str(d), t) if t else False
                for d, t in zip(df["direction"], df["own_d"], strict=False)
            ],
            index=df.index,
        )
        if saves_own_d_only:
            # Matches live utils.soccer_keep_gates._saves_own_d_ok (no opp fallback).
            d_ok = own_filled & own_ok
        else:
            # Legacy analysis mask: blank own-D falls back to opp D (g_d).
            d_ok = np.where(own_filled, own_ok, df["g_d"])
        return sub & df["g_l5"] & d_ok
    return sub


def apply_forward_filter(
    df: pd.DataFrame,
    *,
    forward_from: str | None,
    own_d_only_rows: bool,
) -> pd.DataFrame:
    """Restrict to post-GF-fill dates and/or rows with own_d actually filled."""
    out = df
    date_col = "join_date" if "join_date" in out.columns else "file_date"
    if date_col in out.columns and len(out):
        dates = out[date_col].astype(str).str.slice(0, 10)
        print(f"  graded date span on {date_col}: {dates.min()} .. {dates.max()}")
    if forward_from:
        dates = out[date_col].astype(str).str.slice(0, 10)
        out = out[dates >= forward_from].copy()
        print(f"  forward-from {forward_from}: {len(out):,} rows on {date_col}")
        if len(out) == 0:
            print(
                "  NOTE: no graded boards on/after that date yet — "
                "recheck waits until file_date catches up past GF/OWN_DEF fill."
            )
    if own_d_only_rows:
        filled = out["own_d"].isin(WEAK_D | STINGY_D | {"Avg"})
        out = out[filled].copy()
        print(f"  own-d-filled only: {len(out):,} rows")
    return out


def attack_alignment(sub: pd.DataFrame, *, shotish: bool) -> list[tuple]:
    opp_d = pd.Series(
        sub["opp_shots_d"] if shotish else sub["def_use"], index=sub.index
    ).fillna("").astype(str)
    # Team attack proxy (win% tier) when joined; else player usage Off.
    team_off = sub["own_off"].fillna("").astype(str)
    player_off_hi = sub["player_off"].eq("HIGH")
    leaky = opp_d.isin(WEAK_D)
    stingy = opp_d.isin(STINGY_D)
    t_strong = team_off.isin(STRONG_OFF)
    t_weak = team_off.isin(WEAK_OFF)
    rows = [
        _pack("best gate (all)", sub),
        _pack("opp D leaky (Weak|Below)", sub[leaky]),
        _pack("opp D stingy (Elite|Above)", sub[stingy]),
        _pack("player Off HIGH (STARTER/HIGH_VOL/top3)", sub[player_off_hi]),
        _pack("player Off LOW", sub[~player_off_hi]),
        _pack("ALIGNED  leaky D + player Off HIGH", sub[leaky & player_off_hi]),
        _pack("ALIGNED  stingy D + player Off LOW", sub[stingy & ~player_off_hi]),
        _pack("OPPOSING leaky D + player Off LOW", sub[leaky & ~player_off_hi]),
        _pack("OPPOSING stingy D + player Off HIGH", sub[stingy & player_off_hi]),
    ]
    if int(t_strong.sum() + t_weak.sum()) > 0:
        rows.extend(
            [
                _pack("team Off strong (win% tier)", sub[t_strong]),
                _pack("team Off weak (win% tier)", sub[t_weak]),
                _pack("ALIGNED  leaky D + team Off strong", sub[leaky & t_strong]),
                _pack("ALIGNED  stingy D + team Off weak", sub[stingy & t_weak]),
                _pack("OPPOSING leaky D + team Off weak", sub[leaky & t_weak]),
                _pack("OPPOSING stingy D + team Off strong", sub[stingy & t_strong]),
                # Watchlist cell from 2026-09-24 (n=13 vs n=4) — coincidence until bigger.
                _pack(
                    "WATCHLIST SOT-ish stingy D + weak team Off",
                    sub[stingy & t_weak],
                ),
                _pack(
                    "WATCHLIST SOT-ish stingy D + strong team Off",
                    sub[stingy & t_strong],
                ),
            ]
        )
    else:
        rows.append(_pack("team Off (win% tier) - not joined", sub.iloc[0:0]))
    return rows


def saves_alignment(sub: pd.DataFrame) -> list[tuple]:
    own_d = sub["own_d"].fillna("").astype(str)
    opp_off = sub["opp_off"].fillna("").astype(str)
    opp_d = sub["def_use"].fillna("").astype(str)
    own_leaky = own_d.isin(WEAK_D)
    own_stingy = own_d.isin(STINGY_D)
    opp_strong = opp_off.isin(STRONG_OFF)
    opp_weak = opp_off.isin(WEAK_OFF)
    return [
        _pack("best gate (all)", sub),
        _pack("OWN D leaky", sub[own_leaky]),
        _pack("OWN D stingy", sub[own_stingy]),
        _pack("OWN D blank", sub[~own_leaky & ~own_stingy]),
        _pack("opp Off strong", sub[opp_strong]),
        _pack("opp Off weak", sub[opp_weak]),
        _pack("ALIGNED  own leaky + opp Off strong", sub[own_leaky & opp_strong]),
        _pack("ALIGNED  own stingy + opp Off weak", sub[own_stingy & opp_weak]),
        _pack("OPPOSING own stingy + opp Off strong", sub[own_stingy & opp_strong]),
        _pack("OPPOSING own leaky + opp Off weak", sub[own_leaky & opp_weak]),
        _pack("legacy opp D leaky (context only)", sub[opp_d.isin(WEAK_D)]),
        _pack("legacy opp D stingy (context only)", sub[opp_d.isin(STINGY_D)]),
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--forward-from",
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            f"Keep rows with join_date/file_date >= this (default off; "
            f"use {_DEFAULT_FORWARD_FROM} for post GF/OWN_DEF step3 fill)."
        ),
    )
    ap.add_argument(
        "--own-d-only",
        action="store_true",
        help="Drop rows where own_d is blank (Saves recheck on exercised own-D only).",
    )
    ap.add_argument(
        "--saves-legacy-fallback",
        action="store_true",
        help=(
            "Saves best-gate mask: blank own-D falls back to opp D "
            "(rebuilt-history comparison only; live keep does NOT fallback)."
        ),
    )
    args = ap.parse_args()

    g = build_gate_frame()
    g = apply_forward_filter(
        g,
        forward_from=args.forward_from,
        own_d_only_rows=args.own_d_only,
    )
    saves_own_only = not args.saves_legacy_fallback
    keep = [
        ("shots", "Shots", "L5=5+L10", True),
        ("shots on target", "Shots On Target", "L5>=4+Off", True),
        (
            "goalie saves",
            "Goalie Saves",
            (
                "L5>=4+own D (live keep)"
                if saves_own_only
                else "L5>=4+D (legacy opp-D fallback mask)"
            ),
            False,
        ),
    ]
    rows_out: list[dict] = []
    for pk, label, gate_name, shotish in keep:
        print("\n" + "=" * 72)
        print(f"{label}  |  best gate = {gate_name}")
        print("=" * 72)
        base = g[
            best_gate_mask(g, pk, saves_own_d_only=saves_own_only if pk == "goalie saves" else True)
        ]
        print(
            f"best-gate pool: {_fmt(int(base['hit'].sum()), len(base))}  "
            f"(Std+Goblin list-shape)"
        )
        for direction in ("OVER", "UNDER"):
            sub = base[base["direction"] == direction]
            if sub.empty:
                print(f"\n  {direction}: (empty)")
                continue
            print(f"\n  {direction}  n={len(sub)}")
            packs = (
                saves_alignment(sub)
                if pk == "goalie saves"
                else attack_alignment(sub, shotish=shotish)
            )
            for name, hits, n, hr in packs:
                mark = ""
                if name.startswith("ALIGNED"):
                    mark = "  << want"
                elif name.startswith("OPPOSING"):
                    mark = "  << avoid"
                elif name.startswith("WATCHLIST"):
                    mark = "  << watch"
                print(f"    {name:<52} {_fmt(hits, n):>14}{mark}")
                rows_out.append(
                    {
                        "prop": label,
                        "best_gate": gate_name,
                        "direction": direction,
                        "slice": name,
                        "hits": hits,
                        "n": n,
                        "hr": hr,
                        "forward_from": args.forward_from or "",
                        "own_d_only": bool(args.own_d_only),
                        "saves_legacy_fallback": bool(args.saves_legacy_fallback),
                    }
                )

    out = pd.DataFrame(rows_out)
    suffix = []
    if args.forward_from:
        suffix.append(f"fwd{args.forward_from}")
    if args.own_d_only:
        suffix.append("ownd")
    if args.saves_legacy_fallback:
        suffix.append("legacyfb")
    tag = ("_" + "_".join(suffix)) if suffix else ""
    out_path = _REPO / "data" / "reports" / f"soccer_best_gate_od_alignment{tag}.csv"
    out.to_csv(out_path, index=False)
    latest = _REPO / "data" / "reports" / "soccer_best_gate_od_alignment.csv"
    out.to_csv(latest, index=False)
    print(f"\nWrote {out_path}")
    if out_path != latest:
        print(f"Also wrote {latest}")
    print(
        "\nRecheck recipe (leave gate code alone until forward own-leaky OVER "
        "is sub-50% at n>=30-40):\n"
        "  py -3.14 scripts/_soccer_best_gate_od_alignment.py "
        f"--forward-from {_DEFAULT_FORWARD_FROM} --own-d-only"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())