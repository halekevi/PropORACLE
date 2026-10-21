"""Exhaustive soft-combination grid: Standard OVER rushing_yards.

Factors:
  A = own OL Elite/Above (or run-block Elite/Above)
  B = own Rush Off Elite/Above
  C = elite RB (team RB1 or league top-16 rush yds)
  D = opp Rush D Weak/Below
  E = opp DL / run-stop Weak/Below

Prints singles, all pairs, all triples, 4-ways, full 5-way,
plus the same with L5>=4 and L5=5 overlays.
"""
from __future__ import annotations

from itertools import combinations
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
WEAK = {"Weak", "Below Avg", "Below"}


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

    ol = pd.read_csv(DATA / "nfl_ol_ranks.csv")
    ol["team"] = ol["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    ol["own_run_block_tier"] = ol["run_block_rank"].map(tier_from_rank)
    u = u.merge(
        ol[["team", "tier", "own_run_block_tier"]].rename(
            columns={"team": "tm", "tier": "own_ol_tier"}
        ),
        on="tm",
        how="left",
    )

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
    dl["opp_run_stop_tier"] = dl["run_stop_rank"].map(tier_from_rank)
    u = u.merge(
        dl[["team", "tier", "opp_run_stop_tier"]].rename(
            columns={"team": "opp", "tier": "opp_dl_tier"}
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

    u["A_ol"] = u["own_ol_tier"].isin(ELITE) | u["own_run_block_tier"].isin(ELITE)
    u["B_rush_off"] = u["own_rush_off_tier"].isin(ELITE)
    u["C_elite_rb"] = u["elite_rb"]
    u["D_weak_rush_d"] = u["opp_rush_d_tier"].isin(WEAK)
    u["E_weak_dl"] = u["opp_dl_tier"].isin(WEAK) | u["opp_run_stop_tier"].isin(WEAK)
    return u


FACTORS = [
    ("A", "A_ol", "Elite OL"),
    ("B", "B_rush_off", "Elite Rush Off"),
    ("C", "C_elite_rb", "Elite RB"),
    ("D", "D_weak_rush_d", "Weak Rush D"),
    ("E", "E_weak_dl", "Weak DL"),
]


def slice_hr(g: pd.DataFrame, mask: pd.Series) -> tuple[int, int, float | None]:
    sub = g.loc[mask]
    n = len(sub)
    if n == 0:
        return 0, 0, None
    h = int((sub["result"] == "HIT").sum())
    return h, n, rate(h, n)


def print_grid(g: pd.DataFrame, title: str, base_hr: float) -> list[dict]:
    print(f"\n{'=' * 90}")
    print(f"{title}   base={int((g.result=='HIT').sum())}/{len(g)} = {base_hr:.1f}%")
    print(f"{'=' * 90}")
    rows: list[dict] = []

    # empty = base
    h0, n0, _ = slice_hr(g, pd.Series(True, index=g.index))
    print(f"  {'(none / base)':40s}  {h0:3d}/{n0:<3d} = {base_hr:5.1f}%")

    for k in range(1, len(FACTORS) + 1):
        print(f"\n  --- {k}-factor ---")
        combos = list(combinations(FACTORS, k))
        scored: list[tuple] = []
        for combo in combos:
            mask = pd.Series(True, index=g.index)
            keys = []
            labels = []
            for key, col, lab in combo:
                mask = mask & g[col]
                keys.append(key)
                labels.append(lab)
            h, n, hr = slice_hr(g, mask)
            if n == 0:
                continue
            lift = (hr - base_hr) if hr is not None else float("nan")
            scored.append((lift, hr, h, n, "+".join(keys), " + ".join(labels)))
            rows.append(
                {
                    "window": title,
                    "combo": "+".join(keys),
                    "label": " + ".join(labels),
                    "hits": h,
                    "n": n,
                    "hr": hr,
                    "lift_pp": lift,
                }
            )
        scored.sort(key=lambda x: (-(x[0] if x[0] == x[0] else -999), -x[3]))
        for lift, hr, h, n, keys, labels in scored:
            flag = " **LIFT**" if lift >= 10 and n >= 5 else ""
            if lift >= 5 and n >= 8:
                flag = " *lift*"
            print(f"  {keys:12s}  {labels:55s}  {h:3d}/{n:<3d} = {hr:5.1f}%  ({lift:+.1f}pp){flag}")
    return rows


def main() -> None:
    boxes = [b for d in DATES if (b := load(d)) is not None]
    dec = prepare_decided(boxes)
    u = dedupe_unique_player_games(dec)
    u = attach(u)
    g = u[(u.prop == "rushing_yards") & (u.side == "OVER") & (u.pick == "Standard")].copy()
    print(f"Standard OVER rushing_yards unique-game n={len(g)}")
    print(
        "Factor coverage: "
        f"A={g.A_ol.mean()*100:.0f}% B={g.B_rush_off.mean()*100:.0f}% "
        f"C={g.C_elite_rb.mean()*100:.0f}% D={g.D_weak_rush_d.mean()*100:.0f}% "
        f"E={g.E_weak_dl.mean()*100:.0f}%"
    )
    print("A=Elite OL  B=Elite Rush Off  C=Elite RB  D=Weak Rush D  E=Weak DL")

    all_rows: list[dict] = []
    base_hr = rate(int((g.result == "HIT").sum()), len(g)) or 0.0
    all_rows += print_grid(g, "ALL (no L5 filter)", base_hr)

    g4 = g[g.l5 >= 4]
    if len(g4):
        hr4 = rate(int((g4.result == "HIT").sum()), len(g4)) or 0.0
        all_rows += print_grid(g4, "L5>=4 only", hr4)

    g5 = g[g.l5 == 5]
    if len(g5):
        hr5 = rate(int((g5.result == "HIT").sum()), len(g5)) or 0.0
        all_rows += print_grid(g5, "L5=5 only", hr5)

    # Best lifts summary (n>=5, lift>=5 across ALL window)
    print("\n### Best lifts in ALL window (n>=5, lift>=+5pp)")
    best = [
        r
        for r in all_rows
        if r["window"] == "ALL (no L5 filter)" and r["n"] >= 5 and r["lift_pp"] >= 5
    ]
    best.sort(key=lambda r: (-r["lift_pp"], -r["n"]))
    if not best:
        print("  (none)")
    else:
        for r in best:
            print(
                f"  {r['combo']:12s}  {r['label']:55s}  "
                f"{r['hits']}/{r['n']} = {r['hr']:.1f}%  ({r['lift_pp']:+.1f}pp)"
            )

    print("\n### Worst (n>=5, lift<=-5pp) in ALL window")
    worst = [
        r
        for r in all_rows
        if r["window"] == "ALL (no L5 filter)" and r["n"] >= 5 and r["lift_pp"] <= -5
    ]
    worst.sort(key=lambda r: (r["lift_pp"], -r["n"]))
    for r in worst[:15]:
        print(
            f"  {r['combo']:12s}  {r['label']:55s}  "
            f"{r['hits']}/{r['n']} = {r['hr']:.1f}%  ({r['lift_pp']:+.1f}pp)"
        )

    out = DATA / "nfl_rush_over_combo_grid_week1_2.csv"
    pd.DataFrame(all_rows).to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
