"""Soft-split: Standard UNDER rushing_yards vs Elite DL definitions.

Compares combined opp_dl_tier vs dl_pass_tier vs dl_run_tier after the
pass/run split. Soft only — does not harden ticket gates.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_espn_context import canon_nfl_abbr
from utils.nfl_player_names import norm_nfl_player_name
from utils.nfl_prop_gate_ledger import dedupe_unique_player_games, prepare_decided, rate

MAIN = Path(r"H:\PropORACLE_main_cp")
REPO = Path(r"H:\PropORACLE")
DATA = REPO / "Sports" / "NFL" / "data"
DATES = [
    "2026-09-09",
    "2026-09-10",
    "2026-09-13",
    "2026-09-14",
    "2026-09-20",
    "2026-09-21",
]
ELITE = {"Elite", "Above Avg", "Above"}


def tier_from_rank(r: object) -> str:
    try:
        v = float(r)
    except (TypeError, ValueError):
        return ""
    if v != v:
        return ""
    if v <= 8:
        return "Elite"
    if v <= 14:
        return "Above"
    if v <= 20:
        return "Avg"
    if v <= 26:
        return "Below"
    return "Weak"


def load(d: str) -> pd.DataFrame | None:
    for root in (MAIN, REPO):
        p = root / "outputs" / d / f"graded_nfl_{d}.xlsx"
        if not p.is_file():
            continue
        df = pd.read_excel(p, sheet_name="Box Raw")
        if df.empty or "result" not in df.columns:
            return None
        if int(df["result"].astype(str).str.upper().isin(["HIT", "MISS"]).sum()) == 0:
            return None
        out = df.copy()
        out["slate"] = d
        return out
    return None


def attach(u: pd.DataFrame) -> pd.DataFrame:
    u = u.copy()
    u["tm"] = u.get("team", pd.Series("", index=u.index)).map(
        lambda x: canon_nfl_abbr(x) or str(x or "").upper()
    )
    u["opp"] = (
        u["opp_team"].map(lambda x: canon_nfl_abbr(x) or str(x or "").upper())
        if "opp_team" in u.columns
        else ""
    )
    u["player_norm"] = u["player"].map(norm_nfl_player_name)

    units = pd.read_csv(DATA / "nfl_team_unit_ranks.csv")
    units["team"] = units["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    units["own_rush_off_tier"] = units["rush_off_rank"].map(tier_from_rank)
    u = u.merge(
        units[["team", "own_rush_off_tier"]].rename(columns={"team": "tm"}),
        on="tm",
        how="left",
    )

    defense = pd.read_csv(DATA / "defense_rankings.csv")
    defense["team"] = defense["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    defense["opp_rush_d_tier"] = defense["rush_def_rank"].map(tier_from_rank)
    u = u.merge(
        defense[["team", "opp_rush_d_tier"]].rename(columns={"team": "opp"}),
        on="opp",
        how="left",
    )

    dl = pd.read_csv(DATA / "nfl_dl_ranks.csv")
    dl["team"] = dl["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    keep = [
        "team",
        "tier",
        "dl_pass_tier",
        "dl_run_tier",
        "pass_rush_rank",
        "run_stop_rank",
    ]
    u = u.merge(
        dl[keep].rename(
            columns={
                "team": "opp",
                "tier": "opp_dl_combined_tier",
                "dl_pass_tier": "opp_dl_pass_tier",
                "dl_run_tier": "opp_dl_run_tier",
            }
        ),
        on="opp",
        how="left",
    )

    leaders = pd.read_csv(DATA / "nfl_stat_leaders.csv")
    rush = leaders[leaders["category"].astype(str).eq("rush_yds")].copy()
    rush["player_norm"] = rush["player_norm"].map(norm_nfl_player_name)
    rush["elite_rb"] = (rush["team_rank"] == 1) | (
        pd.to_numeric(rush["league_rank"], errors="coerce") <= 16
    )
    u = u.merge(rush[["player_norm", "elite_rb"]], on="player_norm", how="left")
    u["elite_rb"] = u["elite_rb"].fillna(False).astype(bool)

    u["not_elite_off"] = ~u["own_rush_off_tier"].isin(ELITE) & u["own_rush_off_tier"].notna()
    u["not_elite_rb"] = ~u["elite_rb"]
    u["elite_rush_d"] = u["opp_rush_d_tier"].isin(ELITE)
    u["elite_dl_combined"] = u["opp_dl_combined_tier"].isin(ELITE)
    u["elite_dl_pass"] = u["opp_dl_pass_tier"].isin(ELITE)
    u["elite_dl_run"] = u["opp_dl_run_tier"].isin(ELITE)
    # Pass-elite but NOT run-elite — the contaminated bucket
    u["elite_pass_only"] = u["elite_dl_pass"] & ~u["elite_dl_run"]
    u["elite_run_only"] = u["elite_dl_run"] & ~u["elite_dl_pass"]
    u["elite_both"] = u["elite_dl_pass"] & u["elite_dl_run"]
    return u


def slice_hr(g: pd.DataFrame, mask: pd.Series) -> tuple[int, int, float | None]:
    sub = g.loc[mask]
    n = len(sub)
    if n == 0:
        return 0, 0, None
    h = int((sub["result"] == "HIT").sum())
    return h, n, rate(h, n)


def show(g: pd.DataFrame, base_hr: float, label: str, mask: pd.Series) -> dict:
    h, n, hr = slice_hr(g, mask)
    if n == 0:
        print(f"  {label:62s}  --")
        return {"label": label, "hits": 0, "n": 0, "hr": None, "lift_pp": None}
    lift = hr - base_hr
    flag = ""
    if lift >= 10 and n >= 5:
        flag = " **LIFT**"
    elif lift <= -10 and n >= 5:
        flag = " **HURT**"
    elif lift >= 5 and n >= 5:
        flag = " *lift*"
    elif lift <= -5 and n >= 5:
        flag = " *hurt*"
    print(f"  {label:62s}  {h:3d}/{n:<3d} = {hr:5.1f}%  ({lift:+.1f}pp){flag}")
    return {"label": label, "hits": h, "n": n, "hr": hr, "lift_pp": lift}


def main() -> None:
    boxes = [b for d in DATES if (b := load(d)) is not None]
    dec = prepare_decided(boxes)
    u = dedupe_unique_player_games(dec)
    u = attach(u)
    g = u[(u.prop == "rushing_yards") & (u.side == "UNDER") & (u.pick == "Standard")].copy()
    base_hr = rate(int((g.result == "HIT").sum()), len(g)) or 0.0
    print(f"Standard UNDER rushing_yards unique-game n={len(g)}  base={base_hr:.1f}%")
    print(
        "DL Elite coverage: "
        f"combined={g.elite_dl_combined.mean()*100:.0f}% "
        f"pass={g.elite_dl_pass.mean()*100:.0f}% "
        f"run={g.elite_dl_run.mean()*100:.0f}% "
        f"pass_only={g.elite_pass_only.mean()*100:.0f}% "
        f"run_only={g.elite_run_only.mean()*100:.0f}%"
    )

    rows: list[dict] = []
    print("\n### Singles — Elite DL definitions")
    for lab, mask in [
        ("Elite combined DL (old)", g["elite_dl_combined"]),
        ("Elite dl_pass_tier", g["elite_dl_pass"]),
        ("Elite dl_run_tier", g["elite_dl_run"]),
        ("Elite pass-only (pass Elite, run NOT)", g["elite_pass_only"]),
        ("Elite run-only (run Elite, pass NOT)", g["elite_run_only"]),
        ("Elite both pass+run", g["elite_both"]),
        ("Elite Rush D (defense_rankings)", g["elite_rush_d"]),
    ]:
        rows.append(show(g, base_hr, lab, mask))

    print("\n### Watch stack × DL split (NOT Elite Off + NOT Elite RB + Elite Rush D)")
    base_watch = g["not_elite_off"] & g["not_elite_rb"] & g["elite_rush_d"]
    rows.append(show(g, base_hr, "Watch (no DL filter)", base_watch))
    for lab, mask in [
        ("Watch + Elite combined DL", base_watch & g["elite_dl_combined"]),
        ("Watch + Elite dl_pass", base_watch & g["elite_dl_pass"]),
        ("Watch + Elite dl_run", base_watch & g["elite_dl_run"]),
        ("Watch + Elite pass-only", base_watch & g["elite_pass_only"]),
        ("Watch + Elite run-only", base_watch & g["elite_run_only"]),
        ("Watch + Elite both", base_watch & g["elite_both"]),
    ]:
        rows.append(show(g, base_hr, lab, mask))

    print("\n### Elite Rush D × DL split alone")
    for lab, mask in [
        ("Elite Rush D + Elite combined DL", g["elite_rush_d"] & g["elite_dl_combined"]),
        ("Elite Rush D + Elite dl_pass", g["elite_rush_d"] & g["elite_dl_pass"]),
        ("Elite Rush D + Elite dl_run", g["elite_rush_d"] & g["elite_dl_run"]),
        ("Elite Rush D + Elite pass-only", g["elite_rush_d"] & g["elite_pass_only"]),
        ("Elite Rush D + Elite run-only", g["elite_rush_d"] & g["elite_run_only"]),
    ]:
        rows.append(show(g, base_hr, lab, mask))

    # Disagreement teams in sample
    disagree = g[g["opp_dl_pass_tier"] != g["opp_dl_run_tier"]]
    print(
        f"\nOpp DL pass!=run in sample: {len(disagree)}/{len(g)} "
        f"({disagree['opp'].nunique()} unique opp teams)"
    )
    if len(disagree):
        print(
            disagree.groupby(["opp", "opp_dl_pass_tier", "opp_dl_run_tier", "opp_dl_combined_tier"])
            .size()
            .reset_index(name="n")
            .sort_values("n", ascending=False)
            .head(12)
            .to_string(index=False)
        )

    out = DATA / "nfl_rush_under_dl_split_soft_week1_2.csv"
    pd.DataFrame(rows).to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
