"""Soft-attach O/D + WOPR onto W1+W2 graded rows; cross-tab proposed gates.

Does NOT harden nfl_keep_gates — reports lift only.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

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

# Slate date -> NFL week for route/WOPR join
SLATE_WEEK = {
    "2026-09-09": 1,
    "2026-09-10": 1,
    "2026-09-13": 1,
    "2026-09-14": 1,
    "2026-09-20": 2,
    "2026-09-21": 2,
}

ELITE = {"Elite", "Above Avg", "Above"}
WEAK = {"Weak", "Below Avg", "Below"}


def tier_from_rank(r: object, n: int = 32) -> str:
    try:
        v = float(r)
    except (TypeError, ValueError):
        return ""
    if v != v:
        return ""
    # 1–8 Elite · 9–14 Above · 15–20 Avg · 21–26 Below · 27–32 Weak
    if v <= 8:
        return "Elite"
    if v <= 14:
        return "Above Avg"
    if v <= 20:
        return "Avg"
    if v <= 26:
        return "Below Avg"
    return "Weak"


def load_box(d: str) -> pd.DataFrame | None:
    for root in (MAIN, REPO):
        p = root / "outputs" / d / f"graded_nfl_{d}.xlsx"
        if not p.is_file():
            continue
        df = pd.read_excel(p, sheet_name="Box Raw")
        if df.empty or "result" not in df.columns:
            return None
        decided = df["result"].astype(str).str.upper().isin(["HIT", "MISS", "PUSH"])
        if int(decided.sum()) == 0:
            return None
        out = df.copy()
        out["slate"] = d
        return out
    return None


def _team_key(s: object) -> str:
    return str(s or "").strip().upper()


def soft_attach(dec: pd.DataFrame) -> pd.DataFrame:
    out = dec.copy()
    out["opp"] = out["opp_team"].map(_team_key) if "opp_team" in out.columns else ""
    out["tm"] = out["team"].map(_team_key) if "team" in out.columns else ""
    out["player_norm"] = out["player"].map(norm_nfl_player_name)
    out["week"] = out["slate"].map(SLATE_WEEK)

    sec = pd.read_csv(DATA / "nfl_secondary_ranks.csv")
    sec["team"] = sec["team"].map(_team_key)
    air_med = float(pd.to_numeric(sec["air_yards_allowed"], errors="coerce").median())
    sec = sec.rename(
        columns={
            "tier": "opp_secondary_tier",
            "secondary_rank": "opp_secondary_rank",
            "air_yards_allowed": "opp_air_yards_allowed",
        }
    )
    sec["opp_air_yards_low"] = (
        pd.to_numeric(sec["opp_air_yards_allowed"], errors="coerce") < air_med
    )
    out = out.merge(
        sec[
            [
                "team",
                "opp_secondary_tier",
                "opp_secondary_rank",
                "opp_air_yards_allowed",
                "opp_air_yards_low",
            ]
        ].rename(columns={"team": "opp"}),
        on="opp",
        how="left",
    )

    defense = pd.read_csv(DATA / "defense_rankings.csv")
    defense["team"] = defense["team"].map(_team_key)
    defense["opp_rush_d_tier"] = defense["rush_def_rank"].map(tier_from_rank)
    defense["opp_pass_d_tier"] = defense["pass_def_rank"].map(tier_from_rank)
    out = out.merge(
        defense[["team", "opp_rush_d_tier", "opp_pass_d_tier", "rush_def_rank", "pass_def_rank"]].rename(
            columns={"team": "opp", "rush_def_rank": "opp_rush_def_rank", "pass_def_rank": "opp_pass_def_rank"}
        ),
        on="opp",
        how="left",
    )

    # Defensive sacks face the *opponent OL*, not opp DL.
    # Keep both: opp_ol for sacks polarity; opp_dl for user's hypothesized slice.
    dl = pd.read_csv(DATA / "nfl_dl_ranks.csv")
    dl["team"] = dl["team"].map(_team_key)
    out = out.merge(
        dl[["team", "tier", "dl_rank", "pass_rush_rank"]]
        .rename(
            columns={
                "team": "opp",
                "tier": "opp_dl_tier",
                "dl_rank": "opp_dl_rank",
                "pass_rush_rank": "opp_pass_rush_rank",
            }
        ),
        on="opp",
        how="left",
    )
    out = out.merge(
        dl[["team", "tier", "dl_rank", "pass_rush_rank"]]
        .rename(
            columns={
                "team": "tm",
                "tier": "own_dl_tier",
                "dl_rank": "own_dl_rank",
                "pass_rush_rank": "own_pass_rush_rank",
            }
        ),
        on="tm",
        how="left",
    )

    ol = pd.read_csv(DATA / "nfl_ol_ranks.csv")
    ol["team"] = ol["team"].map(_team_key)
    out = out.merge(
        ol[["team", "tier", "ol_rank", "pass_block_rank"]]
        .rename(
            columns={
                "team": "tm",
                "tier": "own_ol_tier",
                "ol_rank": "own_ol_rank",
                "pass_block_rank": "own_pass_block_rank",
            }
        ),
        on="tm",
        how="left",
    )
    out = out.merge(
        ol[["team", "tier", "ol_rank", "pass_block_rank"]]
        .rename(
            columns={
                "team": "opp",
                "tier": "opp_ol_tier",
                "ol_rank": "opp_ol_rank",
                "pass_block_rank": "opp_pass_block_rank",
            }
        ),
        on="opp",
        how="left",
    )
    out["opp_pass_block_tier"] = out["opp_pass_block_rank"].map(tier_from_rank)
    out["own_pass_rush_tier"] = out["own_pass_rush_rank"].map(tier_from_rank)
    units = pd.read_csv(DATA / "nfl_team_unit_ranks.csv")
    units["team"] = units["team"].map(_team_key)
    units["own_rush_off_tier"] = units["rush_off_rank"].map(tier_from_rank)
    out = out.merge(
        units[["team", "own_rush_off_tier", "rush_off_rank"]].rename(
            columns={"team": "tm", "rush_off_rank": "own_rush_off_rank"}
        ),
        on="tm",
        how="left",
    )

    rp = pd.read_csv(DATA / "nfl_route_participation.csv")
    rp["player_norm"] = rp["player"].map(norm_nfl_player_name)
    # Prefer as_of_week matching slate week; fall back to latest.
    rp_use = rp.sort_values("as_of_week")
    # Merge on player_norm + week ≈ as_of_week
    rp_keys = rp_use[
        ["player_norm", "as_of_week", "wopr_L3", "wopr_season"]
    ].copy()
    out = out.merge(
        rp_keys.rename(columns={"as_of_week": "week"}),
        on=["player_norm", "week"],
        how="left",
    )
    # Fill missing WOPR from latest as_of for player
    latest = (
        rp_use.drop_duplicates("player_norm", keep="last")[
            ["player_norm", "wopr_L3", "wopr_season"]
        ].rename(columns={"wopr_L3": "wopr_L3_fb", "wopr_season": "wopr_season_fb"})
    )
    out = out.merge(latest, on="player_norm", how="left")
    out["wopr"] = pd.to_numeric(out["wopr_L3"], errors="coerce")
    out["wopr"] = out["wopr"].fillna(pd.to_numeric(out["wopr_season"], errors="coerce"))
    out["wopr"] = out["wopr"].fillna(pd.to_numeric(out["wopr_L3_fb"], errors="coerce"))
    out["wopr"] = out["wopr"].fillna(pd.to_numeric(out["wopr_season_fb"], errors="coerce"))
    out["wopr_low"] = out["wopr"] < 0.50
    out["wopr_high"] = out["wopr"] >= 0.50  # high_opportunity

    out["opp_sec_elite"] = out["opp_secondary_tier"].isin(ELITE)
    out["opp_rush_elite"] = out["opp_rush_d_tier"].isin(ELITE)
    out["opp_rush_weak"] = out["opp_rush_d_tier"].isin(WEAK)
    out["opp_dl_weak"] = out["opp_dl_tier"].isin(WEAK)
    out["opp_dl_elite"] = out["opp_dl_tier"].isin(ELITE)
    out["own_ol_weak"] = out["own_ol_tier"].isin(WEAK)
    out["opp_ol_elite"] = out["opp_ol_tier"].isin(ELITE)
    out["opp_ol_weak"] = out["opp_ol_tier"].isin(WEAK)
    out["opp_pb_elite"] = out["opp_pass_block_tier"].isin(ELITE)
    out["opp_pb_weak"] = out["opp_pass_block_tier"].isin(WEAK)
    out["own_pr_weak"] = out["own_pass_rush_tier"].isin(WEAK)
    out["own_pr_elite"] = out["own_pass_rush_tier"].isin(ELITE)
    out["own_rush_elite"] = out["own_rush_off_tier"].isin(ELITE)

    return out


def slice_hr(df: pd.DataFrame, mask: pd.Series) -> tuple[int, float | None]:
    g = df.loc[mask]
    n = len(g)
    if n == 0:
        return 0, None
    hits = int((g["result"] == "HIT").sum())
    return n, rate(hits, n)


def report_row(label: str, n: int, hr: float | None, base_hr: float | None) -> str:
    if n == 0 or hr is None:
        return f"  {label:55s}  n=  0  —"
    lift = ""
    if base_hr is not None:
        lift = f"  lift={hr - base_hr:+.1f}pp"
    return f"  {label:55s}  n={n:3d}  hr={hr:5.1f}%{lift}"


def main() -> None:
    boxes = [b for d in DATES if (b := load_box(d)) is not None]
    dec = prepare_decided(boxes)
    print(f"decided rows={len(dec)} slates={sorted(dec['slate'].unique())}")
    dec = soft_attach(dec)
    u = dedupe_unique_player_games(dec)
    print(f"unique-game rows={len(u)}")

    # coverage of soft cols
    print(
        "attach coverage:",
        f"sec={u['opp_secondary_tier'].notna().mean()*100:.0f}%",
        f"rushD={u['opp_rush_d_tier'].notna().mean()*100:.0f}%",
        f"dl={u['opp_dl_tier'].notna().mean()*100:.0f}%",
        f"wopr={u['wopr'].notna().mean()*100:.0f}%",
    )

    def base_l5(mask: pd.Series) -> tuple[pd.Series, int, float | None]:
        m = mask & (u["l5"] == 5)
        n, hr = slice_hr(u, m)
        return m, n, hr

    print("\n========== SOFT O/D CROSS-TABS (unique-game, L5=5 base) ==========")

    # 1) Sacks UNDER (defender prop → face opp OL / own pass-rush)
    m0 = (u["prop"] == "sacks") & (u["side"] == "UNDER") & (u["pick"] == "Standard")
    _, n0, hr0 = base_l5(m0)
    print(f"\n### Sacks UNDER Standard")
    print(report_row("L5=5", n0, hr0, None))
    for label, extra in [
        # Correct polarity for defender sacks UNDER
        ("L5=5 + opp OL Elite/Above (correct)", u["opp_ol_elite"]),
        ("L5=5 + opp pass-block Elite/Above", u["opp_pb_elite"]),
        ("L5=5 + own pass-rush Weak/Below", u["own_pr_weak"]),
        ("L5=5 + opp OL Elite OR own PR Weak", u["opp_ol_elite"] | u["own_pr_weak"]),
        # User hypothesized / wrong-unit slices (for comparison)
        ("L5=5 + opp DL Weak/Below (user hyp)", u["opp_dl_weak"]),
        ("L5=5 + opp DL Elite/Above", u["opp_dl_elite"]),
        ("L5=5 + own OL Weak/Below (user hyp)", u["own_ol_weak"]),
        ("L5=5 + opp OL Weak/Below (anti)", u["opp_ol_weak"]),
        ("L5=5 + opp pass-block Weak (anti)", u["opp_pb_weak"]),
    ]:
        n, hr = slice_hr(u, (u["l5"] == 5) & m0 & extra)
        print(report_row(label, n, hr, hr0))

    # 2) Receptions UNDER
    m0 = (u["prop"] == "receptions") & (u["side"] == "UNDER") & (u["pick"] == "Standard")
    _, n0, hr0 = base_l5(m0)
    print(f"\n### Receptions UNDER Standard")
    print(report_row("L5=5", n0, hr0, None))
    for label, extra in [
        ("L5=5 + opp Sec Elite/Above", u["opp_sec_elite"]),
        ("L5=5 + opp Sec Elite/Above + WOPR Low", u["opp_sec_elite"] & u["wopr_low"]),
        ("L5=5 + WOPR Low only", u["wopr_low"]),
        ("L5=5 + opp Sec Weak/Below (anti)", u["opp_secondary_tier"].isin(WEAK)),
    ]:
        n, hr = slice_hr(u, (u["l5"] == 5) & m0 & extra)
        print(report_row(label, n, hr, hr0))

    # 3) Longest reception UNDER
    m0 = (u["prop"] == "longest_reception") & (u["side"] == "UNDER") & (u["pick"] == "Standard")
    _, n0, hr0 = base_l5(m0)
    print(f"\n### Longest reception UNDER Standard")
    print(report_row("L5=5", n0, hr0, None))
    for label, extra in [
        ("L5=5 + opp Sec Elite/Above", u["opp_sec_elite"]),
        ("L5=5 + air_yards_allowed < median", u["opp_air_yards_low"].fillna(False)),
        ("L5=5 + Sec Elite + air yds low", u["opp_sec_elite"] & u["opp_air_yards_low"].fillna(False)),
        ("L5=5 + opp Sec Weak/Below (anti)", u["opp_secondary_tier"].isin(WEAK)),
    ]:
        n, hr = slice_hr(u, (u["l5"] == 5) & m0 & extra)
        print(report_row(label, n, hr, hr0))

    # 4) Rush yards UNDER
    m0 = (u["prop"] == "rushing_yards") & (u["side"] == "UNDER") & (u["pick"] == "Standard")
    _, n0, hr0 = base_l5(m0)
    print(f"\n### Rushing yards UNDER Standard")
    print(report_row("L5=5", n0, hr0, None))
    for label, extra in [
        ("L5=5 + opp Rush D Elite/Above", u["opp_rush_elite"]),
        ("L5=5 + opp Rush D Weak/Below (anti)", u["opp_rush_weak"]),
    ]:
        n, hr = slice_hr(u, (u["l5"] == 5) & m0 & extra)
        print(report_row(label, n, hr, hr0))

    # 5) Goblin rec yards OVER
    m0 = (u["prop"] == "receiving_yards") & (u["side"] == "OVER") & (u["pick"] == "Goblin")
    _, n0, hr0 = base_l5(m0)
    print(f"\n### Receiving yards Goblin OVER")
    print(report_row("L5=5", n0, hr0, None))
    for label, extra in [
        ("L5=5 + WOPR>=0.50 (high_opportunity)", u["wopr_high"]),
        ("L5=5 + WOPR<0.50 (anti)", u["wopr_low"]),
        ("L5=5 + WOPR missing", u["wopr"].isna()),
    ]:
        n, hr = slice_hr(u, (u["l5"] == 5) & m0 & extra)
        print(report_row(label, n, hr, hr0))

    # 6) Goblin rush yards OVER
    m0 = (u["prop"] == "rushing_yards") & (u["side"] == "OVER") & (u["pick"] == "Goblin")
    _, n0, hr0 = base_l5(m0)
    print(f"\n### Rushing yards Goblin OVER")
    print(report_row("L5=5", n0, hr0, None))
    for label, extra in [
        ("L5=5 + opp Rush D Weak/Below", u["opp_rush_weak"]),
        ("L5=5 + opp Rush D Elite/Above (anti)", u["opp_rush_elite"]),
        ("L5=5 + own Rush Off Elite/Above", u["own_rush_elite"]),
        (
            "L5=5 + opp Rush D Weak + own Rush Off Elite",
            u["opp_rush_weak"] & u["own_rush_elite"],
        ),
    ]:
        n, hr = slice_hr(u, (u["l5"] == 5) & m0 & extra)
        print(report_row(label, n, hr, hr0))

    # Save soft-attached unique rows for reuse
    out_path = DATA / "nfl_od_soft_attach_week1_2.csv"
    u.to_csv(out_path, index=False, encoding="utf-8-sig")
    print(f"\nWrote soft-attach unique rows -> {out_path}")


if __name__ == "__main__":
    main()
