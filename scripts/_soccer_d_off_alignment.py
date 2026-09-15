#!/usr/bin/env python3
"""Test soccer D x offense alignment by direction.

Attacking props (Goals, SOT, G+A, Assists):
  OVER wants leaky opp D (Weak|Below) + strong own attack
  UNDER wants stingy opp D (Elite|Above) + weak own attack

Saves:
  OVER wants leaky *own* D + strong *opp* attack (shots faced)
  UNDER wants stingy own D + weak opp attack

Team attack is not stored (opp_gf_per_game is empty on every board).
Proxy: win% when gp>=5, ranked within slate+league (1 = strongest).
Own D for keepers is inverted from the same board (team as opponent).
"""
from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from export_soccer_combo_gates import _iter_step6, _name_key, _prop_key  # noqa: E402
from export_soccer_hit_rates_excel import _d_pass, _norm_def, load_graded  # noqa: E402
from utils.defense_tiers import def_tier_from_overall_rank  # noqa: E402

ATTACK = {"goals", "shots on target", "goal + assist", "assists"}
SAVES = {"goalie saves"}
FOCUS = ATTACK | SAVES
WEAK_D = {"Weak", "Below Avg"}
STINGY_D = {"Elite", "Above Avg"}
STRONG_OFF = {"Elite", "Above Avg"}
WEAK_OFF = {"Weak", "Below Avg"}

_S6_COLS = {
    "player",
    "prop_type",
    "prop",
    "pick_type",
    "game_date",
    "def_tier",
    "shots_def_tier",
    "team",
    "opp_team",
    "league",
    "gp",
    "wins",
    "draws",
    "losses",
    "starter_tier",
    "shot_volume",
}


def _hr(hits: int, n: int) -> float | None:
    return (hits / n) if n else None


def _fmt(hits: int, n: int) -> str:
    if n <= 0:
        return "—"
    return f"{hits}/{n}={100 * hits / n:.1f}%"


def load_s6_matchup() -> pd.DataFrame:
    frames = []
    for path in _iter_step6():
        try:
            df = pd.read_csv(
                path,
                usecols=lambda c: str(c).strip().lower() in _S6_COLS,
                low_memory=False,
            )
        except Exception:
            continue
        if df.empty:
            continue
        df.columns = [str(c).strip().lower() for c in df.columns]
        df = df.loc[:, ~df.columns.duplicated()].copy()
        frames.append(df)
    if not frames:
        return pd.DataFrame()
    s6 = pd.concat(frames, ignore_index=True)
    s6["player_key"] = s6.get("player", "").map(_name_key)
    s6["prop_key"] = s6.get("prop_type", s6.get("prop", "")).map(_prop_key)
    s6["pick_type"] = (
        s6.get("pick_type", pd.Series("", index=s6.index))
        .astype(str)
        .str.strip()
        .str.lower()
        .replace({"goblin": "Goblin", "standard": "Standard", "demon": "Demon"})
    )
    s6["join_date"] = s6.get("game_date", "").astype(str).str.slice(0, 10)
    s6["team_key"] = s6.get("team", "").map(_name_key)
    s6["opp_key"] = s6.get("opp_team", "").map(_name_key)
    s6["league"] = s6.get("league", "").astype(str)
    s6["opp_d"] = s6.get("def_tier", pd.Series("", index=s6.index)).map(_norm_def)
    shots = s6.get("shots_def_tier", pd.Series("", index=s6.index)).map(_norm_def)
    s6["opp_shots_d"] = np.where(shots.astype(str).str.len() > 0, shots, s6["opp_d"])
    s6["gp_n"] = pd.to_numeric(s6.get("gp"), errors="coerce")
    s6["wins_n"] = pd.to_numeric(s6.get("wins"), errors="coerce")
    keep = [
        "player_key",
        "prop_key",
        "pick_type",
        "join_date",
        "team_key",
        "opp_key",
        "league",
        "opp_d",
        "opp_shots_d",
        "gp_n",
        "wins_n",
        "starter_tier",
        "shot_volume",
    ]
    keep = [c for c in keep if c in s6.columns]
    return s6[keep].drop_duplicates(
        ["player_key", "prop_key", "pick_type", "join_date"], keep="last"
    )


def team_profiles(s6: pd.DataFrame) -> pd.DataFrame:
    """One row per (date, league, team-as-opponent): that team's D and record."""
    src = s6[s6["opp_key"].astype(str).str.len() > 0].copy()
    if src.empty:
        return pd.DataFrame()

    def _mode(s: pd.Series) -> str:
        s = s.astype(str).replace("", np.nan).dropna()
        if s.empty:
            return ""
        return str(s.mode().iloc[0])

    g = src.groupby(["join_date", "league", "opp_key"], dropna=False)
    prof = g.agg(
        d_tier=("opp_d", _mode),
        shots_d=("opp_shots_d", _mode),
        gp=("gp_n", "max"),
        wins=("wins_n", "max"),
    ).reset_index()
    prof = prof.rename(columns={"opp_key": "team_key"})
    prof["win_pct"] = np.where(
        pd.to_numeric(prof["gp"], errors="coerce").ge(5),
        pd.to_numeric(prof["wins"], errors="coerce")
        / pd.to_numeric(prof["gp"], errors="coerce"),
        np.nan,
    )
    # Rank 1 = strongest attack proxy (highest win%).
    off = []
    for _, grp in prof.groupby(["join_date", "league"], dropna=False):
        ok = grp["win_pct"].notna()
        n = int(ok.sum())
        ranks = pd.Series(np.nan, index=grp.index)
        if n >= 5:
            ranks.loc[ok] = grp.loc[ok, "win_pct"].rank(method="min", ascending=False)
        for idx in grp.index:
            r = ranks.loc[idx]
            if pd.isna(r) or n < 5:
                off.append("")
            else:
                off.append(def_tier_from_overall_rank(int(r), n))
    prof["off_tier"] = off
    return prof


def attach_sides(merged: pd.DataFrame, prof: pd.DataFrame) -> pd.DataFrame:
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
    out = merged.merge(
        own[
            [
                "join_date",
                "league",
                "team_key",
                "own_d",
                "own_shots_d",
                "own_off",
                "own_win_pct",
            ]
        ],
        on=["join_date", "league", "team_key"],
        how="left",
    )
    out = out.merge(
        opp[
            [
                "join_date",
                "league",
                "opp_key",
                "opp_d_prof",
                "opp_shots_d_prof",
                "opp_off",
                "opp_win_pct",
            ]
        ],
        on=["join_date", "league", "opp_key"],
        how="left",
    )
    return out


def pack(name: str, s: pd.DataFrame) -> dict:
    n = int(len(s))
    hits = int(s["hit"].sum()) if n else 0
    return {"gate": name, "hits": hits, "n": n, "hr": _hr(hits, n)}


def attack_gates(sub: pd.DataFrame, direction: str, shotish: bool) -> list[dict]:
    opp_d = sub["opp_shots_d"] if shotish else sub["opp_d"]
    opp_d = opp_d.fillna("").astype(str)
    own_off = sub["own_off"].fillna("").astype(str)
    leaky = opp_d.isin(WEAK_D)
    stingy = opp_d.isin(STINGY_D)
    strong = own_off.isin(STRONG_OFF)
    weak = own_off.isin(WEAK_OFF)
    rows = [
        pack("all", sub),
        pack("opp D leaky (Weak|Below)", sub[leaky]),
        pack("opp D stingy (Elite|Above)", sub[stingy]),
        pack("opp D Avg/blank", sub[~leaky & ~stingy]),
        pack("own attack strong (win%)", sub[strong]),
        pack("own attack weak (win%)", sub[weak]),
        pack("ALIGNED leaky D + strong own attack", sub[leaky & strong]),
        pack("MISALIGNED stingy D + weak own attack", sub[stingy & weak]),
        pack("WRONG leaky D + weak own attack", sub[leaky & weak]),
        pack("WRONG stingy D + strong own attack", sub[stingy & strong]),
    ]
    # Directional D as currently coded on the board (opp D).
    d_pass = pd.Series(
        [_d_pass(direction, t) for t in opp_d],
        index=sub.index,
    )
    rows.insert(4, pack("current D gate (opp D, dir-aware)", sub[d_pass]))
    return rows


def saves_gates(sub: pd.DataFrame, direction: str) -> list[dict]:
    own_d = sub["own_d"].fillna("").astype(str)
    opp_d = sub["opp_d"].fillna("").astype(str)
    opp_off = sub["opp_off"].fillna("").astype(str)
    own_leaky = own_d.isin(WEAK_D)
    own_stingy = own_d.isin(STINGY_D)
    opp_leaky = opp_d.isin(WEAK_D)
    opp_stingy = opp_d.isin(STINGY_D)
    opp_strong = opp_off.isin(STRONG_OFF)
    opp_weak = opp_off.isin(WEAK_OFF)
    d_pass_opp = pd.Series([_d_pass(direction, t) for t in opp_d], index=sub.index)
    return [
        pack("all", sub),
        pack("OWN D leaky (correct OVER side)", sub[own_leaky]),
        pack("OWN D stingy (correct UNDER side)", sub[own_stingy]),
        pack("OPP D leaky (current attach)", sub[opp_leaky]),
        pack("OPP D stingy (current attach)", sub[opp_stingy]),
        pack("current D gate (opp D, dir-aware)", sub[d_pass_opp]),
        pack("opp attack strong (win%)", sub[opp_strong]),
        pack("opp attack weak (win%)", sub[opp_weak]),
        pack("ALIGNED own leaky D + strong opp attack", sub[own_leaky & opp_strong]),
        pack("ALIGNED own stingy D + weak opp attack", sub[own_stingy & opp_weak]),
        pack("MISALIGNED own stingy D + strong opp attack", sub[own_stingy & opp_strong]),
        pack("MISALIGNED own leaky D + weak opp attack", sub[own_leaky & opp_weak]),
    ]


def graded_d_only(g: pd.DataFrame) -> pd.DataFrame:
    """Full graded pool — no step6 join required."""
    rows = []
    base = g[g["list_shape"] & g["prop_key"].isin(FOCUS)].copy()
    for prop, sub in base.groupby("prop"):
        for direction in ("OVER", "UNDER"):
            piece = sub[sub["direction"] == direction]
            if piece.empty:
                continue
            tier = piece["def_tier"].fillna("").astype(str)
            leaky = tier.isin(WEAK_D)
            stingy = tier.isin(STINGY_D)
            d_pass = piece["d_gate"].fillna(False).astype(bool)
            for rec in [
                pack("all", piece),
                pack("opp D leaky Weak|Below", piece[leaky]),
                pack("opp D stingy Elite|Above", piece[stingy]),
                pack("opp D Avg/blank", piece[~leaky & ~stingy]),
                pack("current D gate", piece[d_pass]),
                pack("D FAIL (Avg / wrong side / blank)", piece[~d_pass]),
            ]:
                rec.update(
                    {
                        "prop": prop,
                        "direction": direction,
                        "pool": "graded JSON (opp D as attached)",
                    }
                )
                rows.append(rec)
    return pd.DataFrame(rows)


def main() -> int:
    print("Loading graded soccer…")
    g = load_graded()
    g["player_key"] = g["player"].map(_name_key)
    g["join_date"] = g["file_date"]
    d_tbl = graded_d_only(g)

    print("\n========== GRADED ONLY: opp D x direction (Std+Goblin) ==========")
    show = d_tbl[d_tbl["gate"].isin(["all", "opp D leaky Weak|Below", "opp D stingy Elite|Above", "current D gate"])]
    for prop in ["Shots On Target", "Goals", "Goal + Assist", "Assists", "Goalie Saves"]:
        print(f"\n{prop}")
        for direction in ("OVER", "UNDER"):
            piece = show[(show["prop"] == prop) & (show["direction"] == direction)]
            if piece.empty:
                # case variants
                piece = show[
                    (show["prop"].str.casefold() == prop.casefold())
                    & (show["direction"] == direction)
                ]
            if piece.empty:
                continue
            bits = [f"{r.gate} {_fmt(int(r.hits), int(r.n))}" for r in piece.itertuples()]
            print(f"  {direction:5}  " + "  |  ".join(bits))

    print("\nLoading step6 matchup (own D / win% attack)…")
    s6 = load_s6_matchup()
    print(f"  step6 rows {len(s6):,}")
    prof = team_profiles(s6)
    print(
        f"  team profiles {len(prof):,}  "
        f"off_tier filled {(prof['off_tier'].astype(str).str.len()>0).mean():.0%}  "
        f"(needs gp>=5 and 5+ teams on slate)"
    )

    merged = g.merge(s6, on=["player_key", "prop_key", "pick_type", "join_date"], how="left")
    matched = merged["team_key"].astype(str).str.len().gt(1)
    print(f"  graded-s6 team match {matched.mean():.0%}")
    merged = attach_sides(merged, prof)
    play = merged[merged["list_shape"] & merged["prop_key"].isin(FOCUS) & matched].copy()

    combo_rows = []
    print("\n========== JOINED: D + attack proxy (win%), Std+Goblin ==========")
    for prop, sub0 in play.groupby("prop"):
        shotish = _prop_key(prop) == "shots on target"
        is_saves = _prop_key(prop) in SAVES
        print(f"\n{prop}")
        for direction in ("OVER", "UNDER"):
            sub = sub0[sub0["direction"] == direction]
            if sub.empty:
                continue
            gates = saves_gates(sub, direction) if is_saves else attack_gates(sub, direction, shotish)
            print(f"  {direction}")
            for rec in gates:
                rec.update({"prop": prop, "direction": direction, "pool": "step6 join"})
                combo_rows.append(rec)
                if rec["n"] >= 8 or rec["gate"] in {
                    "all",
                    "ALIGNED leaky D + strong own attack",
                    "ALIGNED own leaky D + strong opp attack",
                    "ALIGNED own stingy D + weak opp attack",
                    "current D gate (opp D, dir-aware)",
                }:
                    print(f"    {rec['gate']:<48} {_fmt(rec['hits'], rec['n'])}")

    combo = pd.DataFrame(combo_rows)

    # Compact headline: aligned vs flipped vs all, n>=8.
    print("\n========== HEADLINE (n>=8) ==========")
    print("Attacking OVER should rise on leaky opp D + strong own attack")
    print("Attacking UNDER should rise on stingy opp D + weak own attack")
    print("Saves OVER should rise on leaky OWN D + strong opp attack (not opp D)")

    paths = [
        _REPO / "data" / "reports" / "soccer_combo_gates_latest.xlsx",
        _REPO / "data" / "reports" / "soccer_hit_rates_gates_latest.xlsx",
        _REPO / "outputs" / "2026-09-03" / "soccer_combo_gates.xlsx",
        Path(r"H:\PropORACLE_main_cp") / "outputs" / "2026-09-03" / "soccer" / "soccer_combo_gates.xlsx",
    ]
    for path in paths:
        if not path.exists():
            continue
        try:
            with pd.ExcelWriter(path, engine="openpyxl", mode="a", if_sheet_exists="replace") as w:
                d_tbl.to_excel(w, sheet_name="D x direction graded", index=False)
                combo.to_excel(w, sheet_name="D+attack alignment", index=False)
            print(f"wrote {path}")
        except PermissionError:
            print(f"locked {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
