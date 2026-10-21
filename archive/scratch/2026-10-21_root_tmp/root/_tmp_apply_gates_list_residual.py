"""Apply best unit-combo / L5=5-lift gates; list what still fails 70%.

Soft reporting only — does not harden ticket code.
"""
from __future__ import annotations

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

# Best soft gate per (pick, side, prop) from prior L5=5 lift + unit combo work.
# Mask uses attached flag columns.
GATES: dict[tuple[str, str, str], tuple[str, str]] = {
    # L5=5 lifts that cleared
    ("Standard", "UNDER", "rushing_yards"): ("L5=5", "l5_eq5"),
    ("Standard", "UNDER", "longest_reception"): ("L5=5", "l5_eq5"),
    ("Goblin", "OVER", "longest_reception"): ("L5=5", "l5_eq5"),
    ("Goblin", "OVER", "receiving_yards"): ("L5=5", "l5_eq5"),
    ("Goblin", "OVER", "rushing_yards"): ("L5=5", "l5_eq5"),
    ("Standard", "OVER", "yards_on_first_rush_attempt"): ("L5=5", "l5_eq5"),
    # Unit combo bests that cleared (or best available)
    ("Standard", "UNDER", "receiving_yards"): (
        "NOT Elite Pass Off + Elite opp DL pass + Weak OL",
        "PASS_OFF_NE & DLP_E & OL_W",
    ),
    ("Standard", "UNDER", "longest_rush"): (
        "Elite Rush D + NOT Elite Rush Off + Weak OL",
        "RUSH_D_E & RUSH_OFF_NE & OL_W",
    ),
    ("Standard", "UNDER", "sacks_taken"): ("Weak opp DL pass", "DLP_W"),
    ("Standard", "UNDER", "passing_yards"): (
        "Elite Pass D + NOT Elite Pass Off",
        "PASS_D_E & PASS_OFF_NE",
    ),
    ("Standard", "UNDER", "pass_completions"): ("Elite Pass D", "PASS_D_E"),
    ("Standard", "UNDER", "completion_percentage"): (
        "Elite Pass D + Elite opp DL pass",
        "PASS_D_E & DLP_E",
    ),
    ("Standard", "UNDER", "rec_targets"): (
        "Elite Pass D + NOT Elite Pass Off",
        "PASS_D_E & PASS_OFF_NE",
    ),
    ("Standard", "UNDER", "tackles_ast"): ("Weak OL", "OL_W"),
    ("Goblin", "OVER", "receptions"): (
        "Weak Pass D + Elite OL",
        "PASS_D_W & OL_E",
    ),
    ("Goblin", "OVER", "longest_rush"): (
        "Weak Rush D + NOT Elite Rush Off",
        "RUSH_D_W & RUSH_OFF_NE",
    ),
    ("Goblin", "OVER", "rush_attempts"): (
        "Weak Rush D + NOT Elite Rush Off",
        "RUSH_D_W & RUSH_OFF_NE",
    ),
    ("Goblin", "OVER", "passing_yards"): (
        "Weak opp DL pass + NOT Elite Pass Off",
        "DLP_W & PASS_OFF_NE",
    ),
    ("Goblin", "OVER", "pass_attempts"): (
        "Weak Pass D + NOT Elite Pass Off",
        "PASS_D_W & PASS_OFF_NE",
    ),
    ("Goblin", "OVER", "pass_completions"): (
        "Weak Pass D + NOT Elite Pass Off",
        "PASS_D_W & PASS_OFF_NE",
    ),
    ("Goblin", "OVER", "pass_rush_yds"): (
        "Weak opp DL pass + NOT Elite Pass Off",
        "DLP_W & PASS_OFF_NE",
    ),
    ("Goblin", "OVER", "tackles_ast"): ("Elite OL + Weak Sec", "OL_E & SEC_W"),
    ("Goblin", "OVER", "kicking_points"): ("Weak Rush D", "RUSH_D_W"),
    ("Standard", "OVER", "receptions"): (
        "Weak Pass D + Weak opp DL pass",
        "PASS_D_W & DLP_W",
    ),
    ("Standard", "OVER", "longest_reception"): (
        "Weak Pass D + Weak Sec + NOT Elite Pass Off",
        "PASS_D_W & SEC_W & PASS_OFF_NE",
    ),
    ("Standard", "OVER", "receiving_yards"): (
        "Weak Pass D + Weak opp DL pass",
        "PASS_D_W & DLP_W",
    ),
    ("Standard", "OVER", "passing_yards"): (
        "Weak Pass D + NOT Elite Pass Off",
        "PASS_D_W & PASS_OFF_NE",
    ),
    ("Standard", "OVER", "longest_rush"): (
        "Weak Rush D + NOT Elite Rush Off",
        "RUSH_D_W & RUSH_OFF_NE",
    ),
    ("Standard", "OVER", "rush_attempts"): (
        "Weak Rush D + NOT Elite Rush Off",
        "RUSH_D_W & RUSH_OFF_NE",
    ),
    ("Standard", "OVER", "rushing_yards"): (
        "Weak DL run + Elite OL",
        "DLR_W & OL_E",
    ),
    ("Standard", "OVER", "yards_on_first_reception"): ("L5=5", "l5_eq5"),
    ("Standard", "OVER", "kicking_points"): ("Weak Pass D", "PASS_D_W"),
    ("Standard", "OVER", "rec_targets"): (
        "Weak Pass D + Weak Sec + Elite Pass Off",
        "PASS_D_W & SEC_W & PASS_OFF_E",
    ),
}


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

    u["l5_eq5"] = u["l5"] == 5
    u["OL_E"] = u.get("own_ol_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["OL_W"] = u.get("own_ol_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["SEC_E"] = u.get("opp_secondary_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["SEC_W"] = u.get("opp_secondary_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["DLP_E"] = u.get("opp_dl_pass_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["DLP_W"] = u.get("opp_dl_pass_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["DLR_W"] = u.get("opp_dl_run_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["PASS_OFF_E"] = u["own_pass_off_tier"].isin(ELITE)
    u["PASS_OFF_NE"] = ~u["own_pass_off_tier"].isin(ELITE) & u["own_pass_off_tier"].notna()
    u["RUSH_OFF_NE"] = ~u["own_rush_off_tier"].isin(ELITE) & u["own_rush_off_tier"].notna()
    u["PASS_D_E"] = u["opp_pass_d_tier"].isin(ELITE)
    u["PASS_D_W"] = u["opp_pass_d_tier"].isin(WEAK)
    u["RUSH_D_E"] = u["opp_rush_d_tier"].isin(ELITE)
    u["RUSH_D_W"] = u["opp_rush_d_tier"].isin(WEAK)
    return u


def eval_expr(g: pd.DataFrame, expr: str) -> pd.Series:
    if expr == "l5_eq5":
        return g["l5_eq5"].fillna(False)
    # "A & B & C" of column names
    mask = pd.Series(True, index=g.index)
    for part in expr.split("&"):
        col = part.strip()
        mask = mask & g[col].fillna(False)
    return mask


def hn(g: pd.DataFrame) -> tuple[int, int, float | None]:
    n = len(g)
    if n == 0:
        return 0, 0, None
    h = int((g["result"] == "HIT").sum())
    return h, n, rate(h, n)


def main() -> None:
    boxes = [b for d in DATES if (b := load_box(d)) is not None]
    u = attach(dedupe_unique_player_games(prepare_decided(boxes)))
    u = u[~u["prop"].astype(str).str.lower().isin({"fantasy_score", "fantasy"})]

    rows: list[dict] = []
    # All Standard/Goblin cells with n>=5
    for (pick, side, prop), g in u.groupby(["pick", "side", "prop"]):
        if pick not in ("Standard", "Goblin"):
            continue
        hb, nb, hrb = hn(g)
        if nb < 5:
            continue
        key = (pick, side, prop)
        if key in GATES:
            label, expr = GATES[key]
            mask = eval_expr(g, expr)
            hg, ng, hrg = hn(g.loc[mask])
            src = label
            gated = True
        else:
            hg, ng, hrg = hb, nb, hrb
            src = "base (no soft gate)"
            gated = False
        rows.append(
            {
                "pick": pick,
                "side": side,
                "prop": prop,
                "base_hits": hb,
                "base_n": nb,
                "base_hr": hrb,
                "hits": hg,
                "n": ng,
                "hr": hrg,
                "gate": src,
                "gated": gated,
            }
        )

    clears = [r for r in rows if r["hr"] is not None and r["n"] >= 4 and r["hr"] >= 70]
    fails = [r for r in rows if r["hr"] is not None and r["n"] >= 4 and r["hr"] < 70]
    empty_gate = [r for r in rows if r["gated"] and r["n"] < 4]

    print(f"Cells n>=5 base: {len(rows)}")
    print(f"Clears >=70% after gates (n>=4): {len(clears)}")
    print(f"Still under 70% (n>=4): {len(fails)}")
    print(f"Gated but n<4 (ignore): {len(empty_gate)}")

    print("\n=== CLEARS (>=70%) with applied gates ===")
    for r in sorted(clears, key=lambda x: (-(x["hr"] or 0), -x["n"])):
        tag = " [gated]" if r["gated"] else ""
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:36s}  "
            f"{r['hits']}/{r['n']} = {r['hr']:.0f}%  "
            f"(base {r['base_hr']:.0f}%)  {r['gate']}{tag}"
        )

    print("\n=== NOT CLEARING (<70%) ===")
    for r in sorted(fails, key=lambda x: (x["pick"], x["side"], x["hr"] or 0, -x["n"])):
        tag = " [gated]" if r["gated"] else " [ungated base]"
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:36s}  "
            f"{r['hits']}/{r['n']} = {r['hr']:.1f}%  "
            f"(base {r['base_hr']:.0f}%)  {r['gate']}{tag}"
        )

    out = DATA / "nfl_gated_residual_under70_week1_2.csv"
    pd.DataFrame(fails).to_csv(out, index=False, encoding="utf-8-sig")
    out2 = DATA / "nfl_gated_all_week1_2.csv"
    pd.DataFrame(rows).to_csv(out2, index=False, encoding="utf-8-sig")
    print(f"\nWrote {out}")
    print(f"Wrote {out2}")


if __name__ == "__main__":
    main()
