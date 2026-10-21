"""Soft combo grids: OL / DL / Secondary / Pass Off / Rush Off on leftover <70% props.

Singles + 2- and 3-factor stacks. Soft only. Fantasy excluded.
Unique-game W1+W2. Polarity flips by OVER vs UNDER.
"""
from __future__ import annotations

from itertools import combinations
from pathlib import Path

import pandas as pd

from utils.nfl_espn_context import canon_nfl_abbr
from utils.nfl_player_names import norm_nfl_player_name
from utils.nfl_prop_gate_ledger import dedupe_unique_player_games, prepare_decided, rate
from utils.nfl_unit_line_ranks import attach_nfl_unit_line_ranks

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

# Leftover under-70% families worth hunting (skip dead 0% FG chips / TD trash)
TARGETS: list[tuple[str, str, str]] = [
    ("receiving_yards", "UNDER", "Standard"),
    ("receiving_yards", "OVER", "Standard"),
    ("receptions", "OVER", "Goblin"),
    ("receptions", "OVER", "Standard"),
    ("longest_reception", "OVER", "Standard"),
    ("longest_rush", "UNDER", "Standard"),
    ("longest_rush", "OVER", "Standard"),
    ("longest_rush", "OVER", "Goblin"),  # lift kept but still <70
    ("sacks_taken", "UNDER", "Standard"),
    ("rushing_yards", "OVER", "Standard"),
    ("rush_attempts", "OVER", "Standard"),
    ("rush_attempts", "OVER", "Goblin"),
    ("passing_yards", "UNDER", "Standard"),
    ("passing_yards", "OVER", "Standard"),
    ("passing_yards", "OVER", "Goblin"),
    ("pass_completions", "UNDER", "Standard"),
    ("pass_completions", "OVER", "Goblin"),
    ("completion_percentage", "UNDER", "Standard"),
    ("rec_targets", "UNDER", "Standard"),
    ("rec_targets", "OVER", "Standard"),
    ("tackles_ast", "OVER", "Goblin"),
    ("tackles_ast", "UNDER", "Standard"),
    ("kicking_points", "OVER", "Goblin"),
    ("kicking_points", "OVER", "Standard"),
    ("pass_rush_yds", "OVER", "Goblin"),
    ("pass_attempts", "OVER", "Goblin"),
]


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


def load_box(d: str) -> pd.DataFrame | None:
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

    defense = pd.read_csv(DATA / "defense_rankings.csv")
    defense["team"] = defense["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    defense["opp_rush_d_tier"] = defense["rush_def_rank"].map(tier_from_rank)
    defense["opp_pass_d_tier"] = defense["pass_def_rank"].map(tier_from_rank)
    u = u.merge(
        defense[["team", "opp_rush_d_tier", "opp_pass_d_tier"]].rename(columns={"team": "opp"}),
        on="opp",
        how="left",
    )

    units = pd.read_csv(DATA / "nfl_team_unit_ranks.csv")
    units["team"] = units["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    units["own_rush_off_tier"] = units["rush_off_rank"].map(tier_from_rank)
    units["own_pass_off_tier"] = units["pass_off_rank"].map(tier_from_rank)
    u = u.merge(
        units[["team", "own_rush_off_tier", "own_pass_off_tier"]].rename(columns={"team": "tm"}),
        on="tm",
        how="left",
    )

    u = attach_nfl_unit_line_ranks(u, REPO, team_col="tm", opp_col="opp")

    # Also own DL pass (for sacks OVER / sacks_taken) — merge dl on tm
    dl = pd.read_csv(DATA / "nfl_dl_ranks.csv")
    dl["team"] = dl["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    keep = ["team", "dl_pass_tier", "dl_run_tier", "tier"]
    u = u.merge(
        dl[keep].rename(
            columns={
                "team": "tm",
                "dl_pass_tier": "own_dl_pass_tier",
                "dl_run_tier": "own_dl_run_tier",
                "tier": "own_dl_tier",
            }
        ),
        on="tm",
        how="left",
    )

    # Flags — OVER polarity (favorable for OVER volume)
    u["OL_E"] = u.get("own_ol_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["OL_W"] = u.get("own_ol_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["SEC_E"] = u.get("opp_secondary_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["SEC_W"] = u.get("opp_secondary_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["DLP_E"] = u.get("opp_dl_pass_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["DLP_W"] = u.get("opp_dl_pass_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["DLR_E"] = u.get("opp_dl_run_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["DLR_W"] = u.get("opp_dl_run_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["OWN_DLP_E"] = u.get("own_dl_pass_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["PASS_OFF_E"] = u["own_pass_off_tier"].isin(ELITE)
    u["PASS_OFF_NE"] = ~u["own_pass_off_tier"].isin(ELITE) & u["own_pass_off_tier"].notna()
    u["RUSH_OFF_E"] = u["own_rush_off_tier"].isin(ELITE)
    u["RUSH_OFF_NE"] = ~u["own_rush_off_tier"].isin(ELITE) & u["own_rush_off_tier"].notna()
    u["PASS_D_E"] = u["opp_pass_d_tier"].isin(ELITE)
    u["PASS_D_W"] = u["opp_pass_d_tier"].isin(WEAK)
    u["RUSH_D_E"] = u["opp_rush_d_tier"].isin(ELITE)
    u["RUSH_D_W"] = u["opp_rush_d_tier"].isin(WEAK)
    return u


def factors_for(prop: str, side: str) -> list[tuple[str, str]]:
    """(col, label) soft factors for this prop family — oriented for the side."""
    p = prop.lower()
    if p in ("receiving_yards", "receptions", "longest_reception", "rec_targets"):
        if side == "UNDER":
            return [
                ("PASS_D_E", "Elite Pass D"),
                ("SEC_E", "Elite Sec"),
                ("PASS_OFF_NE", "NOT Elite Pass Off"),
                ("DLP_E", "Elite opp DL pass"),
                ("OL_W", "Weak OL"),
            ]
        return [
            ("PASS_D_W", "Weak Pass D"),
            ("SEC_W", "Weak Sec"),
            ("PASS_OFF_E", "Elite Pass Off"),
            ("PASS_OFF_NE", "NOT Elite Pass Off"),
            ("DLP_W", "Weak opp DL pass"),
            ("OL_E", "Elite OL"),
        ]

    if p in ("rushing_yards", "rush_attempts", "longest_rush"):
        if side == "UNDER":
            return [
                ("RUSH_D_E", "Elite Rush D"),
                ("DLR_E", "Elite DL run"),
                ("RUSH_OFF_NE", "NOT Elite Rush Off"),
                ("OL_W", "Weak OL"),
                ("SEC_E", "Elite Sec"),  # script / negative game
            ]
        return [
            ("RUSH_D_W", "Weak Rush D"),
            ("DLR_W", "Weak DL run"),
            ("RUSH_OFF_E", "Elite Rush Off"),
            ("RUSH_OFF_NE", "NOT Elite Rush Off"),
            ("OL_E", "Elite OL"),
        ]

    if p in ("passing_yards", "pass_completions", "completion_percentage", "pass_attempts", "pass_rush_yds"):
        if side == "UNDER":
            return [
                ("PASS_D_E", "Elite Pass D"),
                ("DLP_E", "Elite opp DL pass"),
                ("SEC_E", "Elite Sec"),
                ("PASS_OFF_NE", "NOT Elite Pass Off"),
                ("OL_W", "Weak OL"),
            ]
        return [
            ("PASS_D_W", "Weak Pass D"),
            ("DLP_W", "Weak opp DL pass"),
            ("SEC_W", "Weak Sec"),
            ("PASS_OFF_E", "Elite Pass Off"),
            ("PASS_OFF_NE", "NOT Elite Pass Off"),
            ("OL_E", "Elite OL"),
        ]

    if p == "sacks_taken":
        # UNDER = fewer sacks taken → Elite OL / Weak opp pass rush
        if side == "UNDER":
            return [
                ("OL_E", "Elite OL"),
                ("DLP_W", "Weak opp DL pass"),
                ("PASS_OFF_E", "Elite Pass Off"),
                ("PASS_D_W", "Weak Pass D"),
            ]
        return [
            ("OL_W", "Weak OL"),
            ("DLP_E", "Elite opp DL pass"),
            ("OWN_DLP_E", "Elite own DL pass"),
        ]

    if p in ("tackles_ast",):
        if side == "UNDER":
            return [
                ("RUSH_OFF_NE", "Opp weak rush volume proxy NOT Elite Rush Off (own)"),
                ("PASS_OFF_NE", "NOT Elite Pass Off"),
                ("OL_W", "Weak OL"),
            ]
        return [
            ("RUSH_OFF_E", "Elite Rush Off"),
            ("PASS_OFF_E", "Elite Pass Off"),
            ("OL_E", "Elite OL"),
            ("SEC_W", "Weak Sec"),
        ]

    if p in ("kicking_points",):
        if side == "OVER":
            return [
                ("PASS_D_W", "Weak Pass D"),
                ("RUSH_D_W", "Weak Rush D"),
                ("PASS_OFF_E", "Elite Pass Off"),
                ("RUSH_OFF_E", "Elite Rush Off"),
                ("OL_E", "Elite OL"),
            ]
        return [
            ("PASS_D_E", "Elite Pass D"),
            ("RUSH_D_E", "Elite Rush D"),
        ]

    return []


def slice_hr(g: pd.DataFrame, mask: pd.Series) -> tuple[int, int, float | None]:
    sub = g.loc[mask]
    n = len(sub)
    if n == 0:
        return 0, 0, None
    h = int((sub["result"] == "HIT").sum())
    return h, n, rate(h, n)


def main() -> None:
    boxes = [b for d in DATES if (b := load_box(d)) is not None]
    u = attach(dedupe_unique_player_games(prepare_decided(boxes)))

    all_rows: list[dict] = []
    best: list[dict] = []

    for prop, side, pick in TARGETS:
        g = u[(u.prop == prop) & (u.side == side) & (u.pick == pick)].copy()
        if len(g) < 8:
            continue
        base_h = int((g.result == "HIT").sum())
        base_n = len(g)
        base_hr = rate(base_h, base_n) or 0.0
        factors = factors_for(prop, side)
        if not factors:
            continue

        title = f"{pick} {side} {prop}"
        print(f"\n{'=' * 96}")
        print(f"{title}   base={base_h}/{base_n} = {base_hr:.1f}%")
        print(f"{'=' * 96}")

        for k in (1, 2, 3):
            scored: list[tuple] = []
            for combo in combinations(factors, k):
                mask = pd.Series(True, index=g.index)
                keys, labels = [], []
                for col, lab in combo:
                    if col not in g.columns:
                        mask = mask & False
                        continue
                    mask = mask & g[col].fillna(False)
                    keys.append(col)
                    labels.append(lab)
                h, n, hr = slice_hr(g, mask)
                if n < 4:
                    continue
                lift = hr - base_hr
                scored.append((lift, hr, h, n, "+".join(keys), " + ".join(labels)))
                rec = {
                    "pick": pick,
                    "side": side,
                    "prop": prop,
                    "k": k,
                    "combo": "+".join(keys),
                    "label": " + ".join(labels),
                    "hits": h,
                    "n": n,
                    "hr": hr,
                    "base_hr": base_hr,
                    "base_n": base_n,
                    "lift_pp": lift,
                }
                all_rows.append(rec)
                if lift >= 8 and n >= 5:
                    best.append(rec)
            scored.sort(key=lambda x: (-x[0], -x[3]))
            if not scored:
                continue
            print(f"  --- {k}-factor (top 8) ---")
            for lift, hr, h, n, keys, labels in scored[:8]:
                flag = ""
                if lift >= 15 and n >= 5:
                    flag = " **LIFT**"
                elif lift >= 8 and n >= 5:
                    flag = " *lift*"
                elif lift <= -10 and n >= 5:
                    flag = " **HURT**"
                print(
                    f"  {labels:70s}  {h:3d}/{n:<3d} = {hr:5.1f}%  ({lift:+.1f}pp){flag}"
                )

    print("\n" + "=" * 96)
    print("BEST LIFTS across leftovers (lift>=+8pp, n>=5) — soft watch")
    print("=" * 96)
    best.sort(key=lambda r: (-r["lift_pp"], -r["n"]))
    for r in best[:50]:
        clears = " clears70" if r["hr"] >= 70 else ""
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:22s}  k={r['k']}  "
            f"{r['label']:65s}  {r['hits']}/{r['n']} = {r['hr']:.0f}%  "
            f"(base {r['base_hr']:.0f}%, {r['lift_pp']:+.0f}pp){clears}"
        )

    # Per-prop champion
    print("\n" + "=" * 96)
    print("PER-PROP BEST COMBO (max lift, n>=5)")
    print("=" * 96)
    by = {}
    for r in all_rows:
        if r["n"] < 5:
            continue
        key = (r["pick"], r["side"], r["prop"])
        if key not in by or r["lift_pp"] > by[key]["lift_pp"]:
            by[key] = r
    for key, r in sorted(by.items(), key=lambda x: -x[1]["lift_pp"]):
        clears = " clears70" if r["hr"] >= 70 else ""
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:22s}  "
            f"{r['label']:65s}  {r['hits']}/{r['n']} = {r['hr']:.0f}%  "
            f"({r['lift_pp']:+.0f}pp){clears}"
        )

    out = DATA / "nfl_unit_combo_lift_leftovers_week1_2.csv"
    pd.DataFrame(all_rows).to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\nWrote {out} ({len(all_rows)} combos, {len(best)} lifts)")


if __name__ == "__main__":
    main()
