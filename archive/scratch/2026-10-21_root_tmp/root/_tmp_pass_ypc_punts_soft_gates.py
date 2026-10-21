"""Soft gate hunt: pass volume + YPC + punts (the 'doable' PP chips).

Unique-game W1+W2. Soft only. Fantasy / TD / combo noise excluded.
"""
from __future__ import annotations

from itertools import combinations
from pathlib import Path

import pandas as pd

from utils.nfl_context_layers import attach_nfl_context_layers
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

TARGETS = [
    ("passing_yards", "UNDER", "Standard"),
    ("passing_yards", "OVER", "Standard"),
    ("passing_yards", "OVER", "Goblin"),
    ("passing_tds", "UNDER", "Standard"),
    ("passing_tds", "OVER", "Standard"),
    ("passing_tds", "OVER", "Goblin"),
    ("pass_attempts", "UNDER", "Standard"),
    ("pass_attempts", "OVER", "Standard"),
    ("pass_attempts", "OVER", "Goblin"),
    ("pass_completions", "UNDER", "Standard"),
    ("pass_completions", "OVER", "Standard"),
    ("pass_completions", "OVER", "Goblin"),
    ("completion_percentage", "UNDER", "Standard"),
    ("completion_percentage", "OVER", "Standard"),
    ("longest_completion", "OVER", "Standard"),
    ("longest_completion", "OVER", "Goblin"),
    ("longest_completion", "UNDER", "Standard"),
    ("rush_yards_per_carry", "OVER", "Standard"),
    ("rush_yards_per_carry", "UNDER", "Standard"),
    ("punts", "OVER", "Standard"),
    ("punts", "UNDER", "Standard"),
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
    u = attach_nfl_context_layers(u, REPO)

    u["OL_E"] = u.get("own_ol_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["OL_W"] = u.get("own_ol_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["SEC_E"] = u.get("opp_secondary_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["SEC_W"] = u.get("opp_secondary_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["DLP_E"] = u.get("opp_dl_pass_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["DLP_W"] = u.get("opp_dl_pass_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["DLR_E"] = u.get("opp_dl_run_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["DLR_W"] = u.get("opp_dl_run_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["PASS_OFF_E"] = u["own_pass_off_tier"].isin(ELITE)
    u["PASS_OFF_NE"] = ~u["own_pass_off_tier"].isin(ELITE) & u["own_pass_off_tier"].notna()
    u["RUSH_OFF_E"] = u["own_rush_off_tier"].isin(ELITE)
    u["RUSH_OFF_NE"] = ~u["own_rush_off_tier"].isin(ELITE) & u["own_rush_off_tier"].notna()
    u["PASS_D_E"] = u["opp_pass_d_tier"].isin(ELITE)
    u["PASS_D_W"] = u["opp_pass_d_tier"].isin(WEAK)
    u["RUSH_D_E"] = u["opp_rush_d_tier"].isin(ELITE)
    u["RUSH_D_W"] = u["opp_rush_d_tier"].isin(WEAK)
    u["POCKET"] = u.get("qb_mobility_tier", pd.Series("", index=u.index)).astype(str).eq("Pocket")
    u["MOBILE"] = u.get("qb_mobility_tier", pd.Series("", index=u.index)).astype(str).eq("Mobile")
    u["L5_5"] = u["l5"] == 5
    u["L5_GE4"] = u["l5"] >= 4
    return u


def factors(prop: str, side: str) -> list[tuple[str, str]]:
    p = prop.lower()
    if p in (
        "passing_yards",
        "pass_attempts",
        "pass_completions",
        "completion_percentage",
        "longest_completion",
        "passing_tds",
    ):
        if side == "UNDER":
            return [
                ("PASS_D_E", "Elite Pass D"),
                ("SEC_E", "Elite Sec"),
                ("DLP_E", "Elite opp DL pass"),
                ("PASS_OFF_NE", "NOT Elite Pass Off"),
                ("OL_W", "Weak OL"),
                ("POCKET", "Pocket QB"),
                ("L5_5", "L5=5"),
                ("L5_GE4", "L5>=4"),
            ]
        return [
            ("PASS_D_W", "Weak Pass D"),
            ("SEC_W", "Weak Sec"),
            ("DLP_W", "Weak opp DL pass"),
            ("PASS_OFF_E", "Elite Pass Off"),
            ("PASS_OFF_NE", "NOT Elite Pass Off"),
            ("OL_E", "Elite OL"),
            ("MOBILE", "Mobile QB"),
            ("L5_5", "L5=5"),
            ("L5_GE4", "L5>=4"),
        ]
    if p == "rush_yards_per_carry":
        if side == "UNDER":
            return [
                ("RUSH_D_E", "Elite Rush D"),
                ("DLR_E", "Elite DL run"),
                ("RUSH_OFF_NE", "NOT Elite Rush Off"),
                ("OL_W", "Weak OL"),
                ("L5_5", "L5=5"),
            ]
        return [
            ("RUSH_D_W", "Weak Rush D"),
            ("DLR_W", "Weak DL run"),
            ("RUSH_OFF_E", "Elite Rush Off"),
            ("RUSH_OFF_NE", "NOT Elite Rush Off"),
            ("OL_E", "Elite OL"),
            ("L5_5", "L5=5"),
        ]
    if p == "punts":
        # more punts = stalled offense / good opp D / weak own off
        if side == "OVER":
            return [
                ("PASS_D_E", "Elite Pass D (opp)"),
                ("RUSH_D_E", "Elite Rush D (opp)"),
                ("PASS_OFF_NE", "NOT Elite Pass Off"),
                ("RUSH_OFF_NE", "NOT Elite Rush Off"),
                ("OL_W", "Weak OL"),
                ("L5_5", "L5=5"),
            ]
        return [
            ("PASS_D_W", "Weak Pass D"),
            ("RUSH_D_W", "Weak Rush D"),
            ("PASS_OFF_E", "Elite Pass Off"),
            ("RUSH_OFF_E", "Elite Rush Off"),
            ("OL_E", "Elite OL"),
            ("L5_5", "L5=5"),
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
    clears: list[dict] = []

    for prop, side, pick in TARGETS:
        g = u[(u.prop == prop) & (u.side == side) & (u.pick == pick)].copy()
        if len(g) < 5:
            continue
        base_h = int((g.result == "HIT").sum())
        base_n = len(g)
        base_hr = rate(base_h, base_n) or 0.0
        facs = factors(prop, side)
        print(f"\n{'='*92}")
        print(f"{pick} {side} {prop}   base={base_h}/{base_n} = {base_hr:.1f}%")
        print(f"{'='*92}")

        for k in (1, 2, 3):
            scored = []
            for combo in combinations(facs, k):
                mask = pd.Series(True, index=g.index)
                labels = []
                for col, lab in combo:
                    mask = mask & g[col].fillna(False)
                    labels.append(lab)
                h, n, hr = slice_hr(g, mask)
                if n < 4:
                    continue
                lift = hr - base_hr
                scored.append((lift, hr, h, n, " + ".join(labels)))
                rec = {
                    "pick": pick,
                    "side": side,
                    "prop": prop,
                    "k": k,
                    "label": " + ".join(labels),
                    "hits": h,
                    "n": n,
                    "hr": hr,
                    "base_hr": base_hr,
                    "lift_pp": lift,
                }
                all_rows.append(rec)
                if hr >= 70 and n >= 5 and lift >= 0:
                    clears.append(rec)
            scored.sort(key=lambda x: (-x[0], -x[3]))
            print(f"  --- {k}-factor top ---")
            for lift, hr, h, n, lab in scored[:6]:
                flag = ""
                if hr >= 70 and n >= 5 and lift >= 5:
                    flag = " **CLEAR+LIFT**"
                elif hr >= 70 and n >= 5:
                    flag = " *clear*"
                elif lift >= 10 and n >= 5:
                    flag = " *lift*"
                print(f"  {lab:72s}  {h:3d}/{n:<3d} = {hr:5.1f}%  ({lift:+.1f}pp){flag}")

    print("\n" + "=" * 92)
    print("CLEARS >=70% (n>=5, lift>=0) — soft watch candidates")
    print("=" * 92)
    clears.sort(key=lambda r: (-r["hr"], -r["n"], -r["lift_pp"]))
    seen = set()
    for r in clears:
        key = (r["pick"], r["side"], r["prop"], r["label"])
        if key in seen:
            continue
        seen.add(key)
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:24s}  "
            f"{r['label']:70s}  {r['hits']}/{r['n']} = {r['hr']:.0f}%  "
            f"(base {r['base_hr']:.0f}%, {r['lift_pp']:+.0f}pp) k={r['k']}"
        )

    print("\n" + "=" * 92)
    print("PER-PROP BEST (max HR among n>=5; prefer lift)")
    print("=" * 92)
    by = {}
    for r in all_rows:
        if r["n"] < 5:
            continue
        key = (r["pick"], r["side"], r["prop"])
        cur = by.get(key)
        if cur is None or (r["hr"], r["lift_pp"], r["n"]) > (cur["hr"], cur["lift_pp"], cur["n"]):
            by[key] = r
    for key, r in sorted(by.items(), key=lambda x: (-x[1]["hr"], -x[1]["n"])):
        flag = " CLEAR" if r["hr"] >= 70 else " under"
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:24s}  "
            f"{r['label']:70s}  {r['hits']}/{r['n']} = {r['hr']:.0f}%  "
            f"({r['lift_pp']:+.0f}pp){flag}"
        )

    out = DATA / "nfl_pass_ypc_punts_soft_gates_week1_2.csv"
    pd.DataFrame(all_rows).to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\nWrote {out}")


if __name__ == "__main__":
    main()
