"""Exhaustive soft-combination grid: Standard UNDER rushing_yards.

Mirrors the OVER grid with inverted D/OL polarity for the UNDER thesis:
  Factors kept as raw flags so we can also test the inverse stacks.

  A = own OL Elite/Above
  B = own Rush Off Elite/Above
  C = elite RB
  D = opp Rush D Weak/Below
  E = opp DL Weak/Below

  Also report inverted singles useful for UNDER:
  A_inv = own OL Weak/Below
  B_inv = own Rush Off NOT Elite (Below/Weak/Avg)  — line-inflation filter mirror
  D_inv = opp Rush D Elite/Above
  E_inv = opp DL Elite/Above

Prints full A-E combo grid (same as OVER) plus a focused UNDER thesis block:
  Elite Rush D + Elite DL + Weak OL, and NOT-Elite Rush Off / NOT-Elite RB.
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
    # Prefer CSV split tiers when present; fall back to rank→tier.
    if "dl_run_tier" not in dl.columns:
        dl["dl_run_tier"] = dl["run_stop_rank"].map(tier_from_rank)
    if "dl_pass_tier" not in dl.columns:
        dl["dl_pass_tier"] = dl["pass_rush_rank"].map(tier_from_rank)
    u = u.merge(
        dl[["team", "tier", "dl_pass_tier", "dl_run_tier"]].rename(
            columns={
                "team": "opp",
                "tier": "opp_dl_tier",
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

    # Same polarity flags as OVER grid (for direct comparison)
    u["A_ol_elite"] = u["own_ol_tier"].isin(ELITE) | u["own_run_block_tier"].isin(ELITE)
    u["B_rush_off_elite"] = u["own_rush_off_tier"].isin(ELITE)
    u["C_elite_rb"] = u["elite_rb"]
    u["D_weak_rush_d"] = u["opp_rush_d_tier"].isin(WEAK)
    u["E_weak_dl"] = u["opp_dl_run_tier"].isin(WEAK)
    u["E_weak_dl_combined"] = u["opp_dl_tier"].isin(WEAK)
    u["E_weak_dl_pass"] = u["opp_dl_pass_tier"].isin(WEAK)

    # UNDER thesis polarity (inverted)
    u["A_ol_weak"] = u["own_ol_tier"].isin(WEAK) | u["own_run_block_tier"].isin(WEAK)
    u["B_rush_off_not_elite"] = ~u["B_rush_off_elite"] & u["own_rush_off_tier"].notna()
    u["C_rb_not_elite"] = ~u["C_elite_rb"]
    u["D_elite_rush_d"] = u["opp_rush_d_tier"].isin(ELITE)
    u["E_elite_dl"] = u["opp_dl_run_tier"].isin(ELITE)
    u["E_elite_dl_combined"] = u["opp_dl_tier"].isin(ELITE)
    u["E_elite_dl_pass"] = u["opp_dl_pass_tier"].isin(ELITE)
    return u


# OVER-polarity labels (for mirrored comparison)
OVER_FACTORS = [
    ("A", "A_ol_elite", "Elite OL"),
    ("B", "B_rush_off_elite", "Elite Rush Off"),
    ("C", "C_elite_rb", "Elite RB"),
    ("D", "D_weak_rush_d", "Weak Rush D"),
    ("E", "E_weak_dl", "Weak DL run-stop"),
]

# UNDER thesis factors
UNDER_FACTORS = [
    ("Ainv", "A_ol_weak", "Weak OL"),
    ("Binv", "B_rush_off_not_elite", "NOT Elite Rush Off"),
    ("Cinv", "C_rb_not_elite", "NOT Elite RB"),
    ("Dinv", "D_elite_rush_d", "Elite Rush D"),
    ("Einv", "E_elite_dl", "Elite DL run-stop"),
]


def slice_hr(g: pd.DataFrame, mask: pd.Series) -> tuple[int, int, float | None]:
    sub = g.loc[mask]
    n = len(sub)
    if n == 0:
        return 0, 0, None
    h = int((sub["result"] == "HIT").sum())
    return h, n, rate(h, n)


def print_grid(
    g: pd.DataFrame,
    title: str,
    base_hr: float,
    factors: list[tuple[str, str, str]],
) -> list[dict]:
    print(f"\n{'=' * 90}")
    print(f"{title}   base={int((g.result=='HIT').sum())}/{len(g)} = {base_hr:.1f}%")
    print(f"{'=' * 90}")
    rows: list[dict] = []
    h0, n0, _ = slice_hr(g, pd.Series(True, index=g.index))
    print(f"  {'(none / base)':40s}  {h0:3d}/{n0:<3d} = {base_hr:5.1f}%")

    for k in range(1, len(factors) + 1):
        print(f"\n  --- {k}-factor ---")
        scored: list[tuple] = []
        for combo in combinations(factors, k):
            mask = pd.Series(True, index=g.index)
            keys, labels = [], []
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
            flag = ""
            if lift >= 10 and n >= 5:
                flag = " **LIFT**"
            elif lift >= 5 and n >= 5:
                flag = " *lift*"
            print(
                f"  {keys:18s}  {labels:60s}  {h:3d}/{n:<3d} = {hr:5.1f}%  ({lift:+.1f}pp){flag}"
            )
    return rows


def main() -> None:
    boxes = [b for d in DATES if (b := load(d)) is not None]
    dec = prepare_decided(boxes)
    u = dedupe_unique_player_games(dec)
    u = attach(u)
    g = u[(u.prop == "rushing_yards") & (u.side == "UNDER") & (u.pick == "Standard")].copy()
    print(f"Standard UNDER rushing_yards unique-game n={len(g)}")
    print(
        "Coverage Elite: "
        f"OL={g.A_ol_elite.mean()*100:.0f}% RushOff={g.B_rush_off_elite.mean()*100:.0f}% "
        f"RB={g.C_elite_rb.mean()*100:.0f}% WeakRushD={g.D_weak_rush_d.mean()*100:.0f}% "
        f"WeakDL={g.E_weak_dl.mean()*100:.0f}%"
    )
    print(
        "Coverage UNDER thesis: "
        f"WeakOL={g.A_ol_weak.mean()*100:.0f}% NotEliteOff={g.B_rush_off_not_elite.mean()*100:.0f}% "
        f"NotEliteRB={g.C_rb_not_elite.mean()*100:.0f}% EliteRushD={g.D_elite_rush_d.mean()*100:.0f}% "
        f"EliteDL={g.E_elite_dl.mean()*100:.0f}%"
    )

    all_rows: list[dict] = []
    base_hr = rate(int((g.result == "HIT").sum()), len(g)) or 0.0

    print("\n### Grid 1: SAME polarity as OVER (expect these to HURT on UNDER)")
    all_rows += print_grid(g, "UNDER / OVER-polarity factors", base_hr, OVER_FACTORS)

    print("\n### Grid 2: INVERTED UNDER thesis (Elite Rush D + Elite DL + Weak OL …)")
    all_rows += print_grid(g, "UNDER / inverted thesis", base_hr, UNDER_FACTORS)

    for label, window, subset in [
        ("L5>=4 + inverted thesis", "L5>=4 / inverted", g[g.l5 >= 4]),
        ("L5=5 + inverted thesis", "L5=5 / inverted", g[g.l5 == 5]),
    ]:
        if len(subset) == 0:
            continue
        hr = rate(int((subset.result == "HIT").sum()), len(subset)) or 0.0
        print(f"\n### {label}")
        all_rows += print_grid(subset, window, hr, UNDER_FACTORS)

    print("\n### Best lifts — inverted UNDER thesis (n>=5, lift>=+5pp)")
    best = [
        r
        for r in all_rows
        if r["window"] == "UNDER / inverted thesis"
        and r["n"] >= 5
        and r["lift_pp"] >= 5
    ]
    best.sort(key=lambda r: (-r["lift_pp"], -r["n"]))
    if not best:
        print("  (none at n>=5)")
    for r in best:
        print(
            f"  {r['combo']:18s}  {r['label']:60s}  "
            f"{r['hits']}/{r['n']} = {r['hr']:.1f}%  ({r['lift_pp']:+.1f}pp)"
        )

    print("\n### Worst — OVER-polarity on UNDER (n>=5, lift<=-5pp) = mirror check")
    worst = [
        r
        for r in all_rows
        if r["window"] == "UNDER / OVER-polarity factors"
        and r["n"] >= 5
        and r["lift_pp"] <= -5
    ]
    worst.sort(key=lambda r: (r["lift_pp"], -r["n"]))
    for r in worst[:12]:
        print(
            f"  {r['combo']:18s}  {r['label']:60s}  "
            f"{r['hits']}/{r['n']} = {r['hr']:.1f}%  ({r['lift_pp']:+.1f}pp)"
        )

    # Explicit headline stacks the user named
    print("\n### Headline UNDER stacks (explicit)")
    tests = [
        ("Elite Rush D", g["D_elite_rush_d"]),
        ("Elite DL", g["E_elite_dl"]),
        ("Weak OL", g["A_ol_weak"]),
        ("Elite Rush D + Elite DL", g["D_elite_rush_d"] & g["E_elite_dl"]),
        (
            "Elite Rush D + Elite DL + Weak OL",
            g["D_elite_rush_d"] & g["E_elite_dl"] & g["A_ol_weak"],
        ),
        (
            "Elite Rush D + Elite DL + NOT Elite Rush Off",
            g["D_elite_rush_d"] & g["E_elite_dl"] & g["B_rush_off_not_elite"],
        ),
        (
            "Elite Rush D + Elite DL + NOT Elite RB",
            g["D_elite_rush_d"] & g["E_elite_dl"] & g["C_rb_not_elite"],
        ),
        (
            "FULL UNDER thesis: Elite D/DL + Weak OL + NOT Elite Off + NOT Elite RB",
            g["D_elite_rush_d"]
            & g["E_elite_dl"]
            & g["A_ol_weak"]
            & g["B_rush_off_not_elite"]
            & g["C_rb_not_elite"],
        ),
        # Mirror of OVER's best A+D: Elite OL + Weak Rush D — should HURT on UNDER
        ("OVER-best A+D on UNDER (expect hurt)", g["A_ol_elite"] & g["D_weak_rush_d"]),
        (
            "OVER-best A+D+E on UNDER (expect hurt)",
            g["A_ol_elite"] & g["D_weak_rush_d"] & g["E_weak_dl"],
        ),
    ]
    for lab, mask in tests:
        h, n, hr = slice_hr(g, mask)
        if n == 0:
            print(f"  {lab:70s}  --")
            continue
        lift = hr - base_hr
        print(f"  {lab:70s}  {h:3d}/{n:<3d} = {hr:5.1f}%  ({lift:+.1f}pp)")

    out = DATA / "nfl_rush_under_combo_grid_week1_2.csv"
    pd.DataFrame(all_rows).to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
