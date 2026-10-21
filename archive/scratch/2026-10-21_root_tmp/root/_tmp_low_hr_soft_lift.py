"""Soft-lift hunt: low-HR W1+W2 props x relevant context stacks.

Soft only — does not harden ticket gates. Focuses on props with base HR < 70%
and n >= 12 (unique-game Standard/Goblin). Factor families match prop_map.
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from utils.nfl_context_layers import attach_nfl_context_layers
from utils.nfl_espn_context import canon_nfl_abbr
from utils.nfl_player_names import norm_nfl_player_name
from utils.nfl_prop_gate_ledger import dedupe_unique_player_games, prepare_decided, rate
from utils.nfl_route_participation_gate import attach_participation
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


def attach_extra(u: pd.DataFrame) -> pd.DataFrame:
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

    # defense_rankings pass/rush
    defense = pd.read_csv(DATA / "defense_rankings.csv")
    defense["team"] = defense["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    defense["opp_rush_d_tier"] = defense["rush_def_rank"].map(tier_from_rank)
    defense["opp_pass_d_tier"] = defense["pass_def_rank"].map(tier_from_rank)
    u = u.merge(
        defense[["team", "opp_rush_d_tier", "opp_pass_d_tier"]].rename(columns={"team": "opp"}),
        on="opp",
        how="left",
    )

    # team unit ranks (rush/pass off)
    units = pd.read_csv(DATA / "nfl_team_unit_ranks.csv")
    units["team"] = units["team"].map(lambda x: canon_nfl_abbr(x) or str(x).upper())
    units["own_rush_off_tier"] = units["rush_off_rank"].map(tier_from_rank)
    units["own_pass_off_tier"] = units["pass_off_rank"].map(tier_from_rank)
    u = u.merge(
        units[["team", "own_rush_off_tier", "own_pass_off_tier"]].rename(columns={"team": "tm"}),
        on="tm",
        how="left",
    )

    # elite RB / WR volume from leaders
    leaders = pd.read_csv(DATA / "nfl_stat_leaders.csv")
    for cat, col in (("rush_yds", "elite_rb"), ("rec_yds", "elite_wr")):
        sub = leaders[leaders["category"].astype(str).eq(cat)].copy()
        sub["player_norm"] = sub["player_norm"].map(norm_nfl_player_name)
        sub[col] = (sub["team_rank"] == 1) | (
            pd.to_numeric(sub["league_rank"], errors="coerce") <= 16
        )
        u = u.merge(sub[["player_norm", col]], on="player_norm", how="left")
        u[col] = u[col].fillna(False).astype(bool)

    # unit line ranks (OL/DL/sec/box/pace) + context layers (mobility/RZ)
    u = attach_nfl_unit_line_ranks(u, REPO, team_col="tm", opp_col="opp")
    u = attach_nfl_context_layers(u, REPO)
    try:
        u = attach_participation(u, REPO)
    except Exception as exc:  # noqa: BLE001
        print(f"[warn] attach_participation: {exc}")

    # WOPR bands
    w = pd.to_numeric(u.get("wopr_L3", u.get("wopr")), errors="coerce")
    if w is None:
        w = pd.Series(float("nan"), index=u.index)
    u["wopr"] = w
    u["wopr_low"] = w < 0.50
    u["wopr_high"] = w >= 0.70
    u["wopr_mid"] = (w >= 0.50) & (w < 0.70)

    # convenience flags
    u["elite_rush_d"] = u["opp_rush_d_tier"].isin(ELITE)
    u["weak_rush_d"] = u["opp_rush_d_tier"].isin(WEAK)
    u["elite_pass_d"] = u["opp_pass_d_tier"].isin(ELITE)
    u["weak_pass_d"] = u["opp_pass_d_tier"].isin(WEAK)
    u["elite_sec"] = u.get("opp_secondary_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["weak_sec"] = u.get("opp_secondary_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["elite_ol"] = u.get("own_ol_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["weak_ol"] = u.get("own_ol_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["elite_dl_pass"] = u.get("opp_dl_pass_tier", pd.Series("", index=u.index)).isin(ELITE)
    u["weak_dl_pass"] = u.get("opp_dl_pass_tier", pd.Series("", index=u.index)).isin(WEAK)
    u["elite_pass_block"] = (
        pd.to_numeric(u.get("own_pass_block_rank"), errors="coerce").le(14).fillna(False)
    )
    u["weak_pass_block"] = (
        pd.to_numeric(u.get("own_pass_block_rank"), errors="coerce").gt(20).fillna(False)
    )
    u["not_elite_rush_off"] = ~u["own_rush_off_tier"].isin(ELITE) & u["own_rush_off_tier"].notna()
    u["elite_rush_off"] = u["own_rush_off_tier"].isin(ELITE)
    u["not_elite_pass_off"] = ~u["own_pass_off_tier"].isin(ELITE) & u["own_pass_off_tier"].notna()
    u["elite_pass_off"] = u["own_pass_off_tier"].isin(ELITE)
    u["fast_pace"] = u.get("own_pace_tier", pd.Series("", index=u.index)).isin(
        {"Fast", "Elite", "Above Avg", "Above"}
    )
    u["pocket"] = u.get("qb_mobility_tier", pd.Series("", index=u.index)).astype(str).eq("Pocket")
    u["mobile"] = u.get("qb_mobility_tier", pd.Series("", index=u.index)).astype(str).eq("Mobile")
    u["high_rz_rush"] = u.get("player_high_rz_rush", pd.Series(False, index=u.index)).fillna(False)
    u["high_rz_tgt"] = u.get("player_high_rz_target", pd.Series(False, index=u.index)).fillna(False)
    u["elite_rz_team"] = u.get("own_rz_td_tier", pd.Series("", index=u.index)).isin(ELITE)
    return u


def slice_hr(g: pd.DataFrame, mask: pd.Series) -> tuple[int, int, float | None]:
    sub = g.loc[mask]
    n = len(sub)
    if n == 0:
        return 0, 0, None
    h = int((sub["result"] == "HIT").sum())
    return h, n, rate(h, n)


def tests_for(prop: str, side: str) -> list[tuple[str, callable]]:
    """Return (label, mask_fn) soft tests for a prop family."""
    p = prop.lower()
    out: list[tuple[str, callable]] = []

    if p in ("receiving_yards", "receptions", "longest_reception", "rec_targets", "yards_on_first_reception"):
        if side == "UNDER":
            out += [
                ("Elite Pass D", lambda g: g["elite_pass_d"]),
                ("Elite Sec", lambda g: g["elite_sec"]),
                ("Elite Pass D + Elite Sec", lambda g: g["elite_pass_d"] & g["elite_sec"]),
                ("WOPR low", lambda g: g["wopr_low"]),
                ("WOPR low + Elite Pass D", lambda g: g["wopr_low"] & g["elite_pass_d"]),
                ("WOPR low + Elite Sec", lambda g: g["wopr_low"] & g["elite_sec"]),
                ("WOPR low + Elite Pass D + Elite Sec", lambda g: g["wopr_low"] & g["elite_pass_d"] & g["elite_sec"]),
                ("NOT Elite WR", lambda g: ~g["elite_wr"]),
                ("NOT Elite WR + Elite Pass D", lambda g: ~g["elite_wr"] & g["elite_pass_d"]),
                ("NOT Elite WR + Elite Sec", lambda g: ~g["elite_wr"] & g["elite_sec"]),
                ("NOT Elite Pass Off + Elite Pass D", lambda g: g["not_elite_pass_off"] & g["elite_pass_d"]),
            ]
        else:  # OVER
            out += [
                ("Weak Pass D", lambda g: g["weak_pass_d"]),
                ("Weak Sec", lambda g: g["weak_sec"]),
                ("Weak Pass D + Weak Sec", lambda g: g["weak_pass_d"] & g["weak_sec"]),
                ("WOPR high", lambda g: g["wopr_high"]),
                ("WOPR high + Weak Pass D", lambda g: g["wopr_high"] & g["weak_pass_d"]),
                ("WOPR high + Weak Sec", lambda g: g["wopr_high"] & g["weak_sec"]),
                ("Elite WR", lambda g: g["elite_wr"]),
                ("Elite WR + Weak Pass D", lambda g: g["elite_wr"] & g["weak_pass_d"]),
                ("Elite Pass Off + Weak Pass D", lambda g: g["elite_pass_off"] & g["weak_pass_d"]),
                ("Fast pace + Weak Pass D", lambda g: g["fast_pace"] & g["weak_pass_d"]),
                ("NOT Elite WR + Weak Pass D (line inflation mirror)", lambda g: ~g["elite_wr"] & g["weak_pass_d"]),
            ]

    elif p in ("rushing_yards", "rush_attempts", "longest_rush", "rush_yards_per_carry", "yards_on_first_rush_attempt"):
        if side == "UNDER":
            out += [
                ("Elite Rush D", lambda g: g["elite_rush_d"]),
                ("NOT Elite Rush Off", lambda g: g["not_elite_rush_off"]),
                ("NOT Elite RB", lambda g: ~g["elite_rb"]),
                ("Watch: NOT Elite Off+RB + Elite Rush D", lambda g: g["not_elite_rush_off"] & ~g["elite_rb"] & g["elite_rush_d"]),
                ("Weak OL + Elite Rush D", lambda g: g["weak_ol"] & g["elite_rush_d"]),
            ]
        else:
            out += [
                ("Weak Rush D", lambda g: g["weak_rush_d"]),
                ("Elite OL", lambda g: g["elite_ol"]),
                ("Elite OL + Weak Rush D", lambda g: g["elite_ol"] & g["weak_rush_d"]),
                ("NOT Elite Rush Off + Weak Rush D", lambda g: g["not_elite_rush_off"] & g["weak_rush_d"]),
                ("NOT Elite RB + Weak Rush D", lambda g: ~g["elite_rb"] & g["weak_rush_d"]),
                ("Elite Rush Off + Weak Rush D", lambda g: g["elite_rush_off"] & g["weak_rush_d"]),
            ]

    elif p in ("passing_yards", "pass_attempts", "pass_completions", "completion_percentage", "longest_completion", "pass_rush_yds"):
        if side == "UNDER":
            out += [
                ("Elite Pass D", lambda g: g["elite_pass_d"]),
                ("Elite DL pass", lambda g: g["elite_dl_pass"]),
                ("Elite Pass D + Elite DL pass", lambda g: g["elite_pass_d"] & g["elite_dl_pass"]),
                ("Weak OL / pass block", lambda g: g["weak_ol"] | g["weak_pass_block"]),
                ("NOT Elite Pass Off + Elite Pass D", lambda g: g["not_elite_pass_off"] & g["elite_pass_d"]),
                ("Pocket QB + Elite DL pass", lambda g: g["pocket"] & g["elite_dl_pass"]),
            ]
        else:
            out += [
                ("Weak Pass D", lambda g: g["weak_pass_d"]),
                ("Elite OL / pass block", lambda g: g["elite_ol"] | g["elite_pass_block"]),
                ("Elite OL + Weak Pass D", lambda g: (g["elite_ol"] | g["elite_pass_block"]) & g["weak_pass_d"]),
                ("Weak DL pass + Weak Pass D", lambda g: g["weak_dl_pass"] & g["weak_pass_d"]),
                ("NOT Elite Pass Off + Weak Pass D", lambda g: g["not_elite_pass_off"] & g["weak_pass_d"]),
                ("Mobile QB + Weak Pass D", lambda g: g["mobile"] & g["weak_pass_d"]),
            ]

    elif p == "sacks_taken":
        if side == "UNDER":
            out += [
                ("Pocket QB", lambda g: g["pocket"]),
                ("Mobile QB", lambda g: g["mobile"]),
                ("Elite pass block", lambda g: g["elite_pass_block"]),
                ("Weak DL pass (opp)", lambda g: g["weak_dl_pass"]),
                ("Pocket + Elite pass block", lambda g: g["pocket"] & g["elite_pass_block"]),
                ("Pocket + Weak DL pass", lambda g: g["pocket"] & g["weak_dl_pass"]),
                ("Elite pass block + Weak DL pass", lambda g: g["elite_pass_block"] & g["weak_dl_pass"]),
            ]
        else:
            out += [
                ("Mobile QB", lambda g: g["mobile"]),
                ("Weak pass block", lambda g: g["weak_pass_block"]),
                ("Elite DL pass", lambda g: g["elite_dl_pass"]),
                ("Weak pass block + Elite DL pass", lambda g: g["weak_pass_block"] & g["elite_dl_pass"]),
            ]

    elif p == "sacks":
        if side == "OVER":
            out += [
                ("Elite DL pass (own via opp OL? use opp weak OL)", lambda g: g["weak_ol"]),  # wrong axis placeholder
                ("Elite own pass rush — skip if missing", lambda g: g["elite_dl_pass"]),  # this is OPP dl
            ]
        # sacks OVER needs own DL — attach own from opp perspective flipped
        # handled specially below if columns exist

    elif p in ("tackles_ast", "tackles_assists", "solo_tackles"):
        out += [
            ("Elite Box (opp? own def)", lambda g: g.get("opp_box_tier", pd.Series("", index=g.index)).isin(ELITE) if "opp_box_tier" in g.columns else pd.Series(False, index=g.index)),
            ("Fast pace opp", lambda g: g.get("opp_pace_tier", pd.Series("", index=g.index)).isin({"Fast", "Elite", "Above Avg", "Above"}) if "opp_pace_tier" in g.columns else pd.Series(False, index=g.index)),
        ]

    elif p in ("player_touchdowns", "rush_tds", "rec_tds", "anytime_tds", "player_touchdowns_combo", "passing_tds"):
        if side == "OVER":
            out += [
                ("High player RZ rush", lambda g: g["high_rz_rush"].astype(bool)),
                ("High player RZ tgt", lambda g: g["high_rz_tgt"].astype(bool)),
                ("Elite team RZ TD", lambda g: g["elite_rz_team"]),
                ("High RZ rush + Weak Rush D", lambda g: g["high_rz_rush"].astype(bool) & g["weak_rush_d"]),
                ("High RZ tgt + Weak Pass D", lambda g: g["high_rz_tgt"].astype(bool) & g["weak_pass_d"]),
            ]
        else:
            out += [
                ("NOT high RZ", lambda g: ~g["high_rz_rush"].astype(bool) & ~g["high_rz_tgt"].astype(bool)),
                ("Elite Rush D (rush TD fade)", lambda g: g["elite_rush_d"]),
                ("Elite Pass D (rec TD fade)", lambda g: g["elite_pass_d"]),
            ]

    elif p in ("fantasy_score", "kicking_points", "fg_made", "punts"):
        if side == "UNDER":
            out += [
                ("Elite Pass D (fantasy)", lambda g: g["elite_pass_d"]),
                ("Elite Rush D (fantasy)", lambda g: g["elite_rush_d"]),
                ("NOT Elite WR", lambda g: ~g["elite_wr"]),
            ]
        else:
            out += [
                ("Weak Pass D", lambda g: g["weak_pass_d"]),
                ("Weak Rush D", lambda g: g["weak_rush_d"]),
                ("WOPR high", lambda g: g["wopr_high"]),
            ]

    return out


# Priority low/mid props to hunt (skip dead 0% FG combos / anytime trash unless tagged)
TARGETS: list[tuple[str, str, str]] = [
    # (prop, side, pick) — pick Standard unless Goblin noted
    ("receiving_yards", "OVER", "Standard"),
    ("receiving_yards", "UNDER", "Standard"),
    ("receiving_yards", "OVER", "Goblin"),
    ("receptions", "OVER", "Standard"),
    ("receptions", "UNDER", "Standard"),
    ("receptions", "OVER", "Goblin"),
    ("longest_reception", "OVER", "Standard"),
    ("longest_reception", "UNDER", "Standard"),
    ("longest_reception", "OVER", "Goblin"),
    ("rec_targets", "OVER", "Standard"),
    ("rec_targets", "UNDER", "Standard"),
    ("rushing_yards", "OVER", "Standard"),
    ("rushing_yards", "OVER", "Goblin"),
    ("rush_attempts", "OVER", "Standard"),
    ("rush_attempts", "UNDER", "Standard"),
    ("rush_attempts", "OVER", "Goblin"),
    ("longest_rush", "OVER", "Standard"),
    ("longest_rush", "UNDER", "Standard"),
    ("longest_rush", "OVER", "Goblin"),
    ("passing_yards", "OVER", "Standard"),
    ("passing_yards", "UNDER", "Standard"),
    ("passing_yards", "OVER", "Goblin"),
    ("pass_attempts", "OVER", "Standard"),
    ("pass_completions", "OVER", "Standard"),
    ("pass_completions", "UNDER", "Standard"),
    ("completion_percentage", "UNDER", "Standard"),
    ("sacks_taken", "UNDER", "Standard"),
    ("sacks", "OVER", "Standard"),
    ("tackles_ast", "OVER", "Standard"),
    ("tackles_ast", "OVER", "Goblin"),
    ("tackles_ast", "UNDER", "Standard"),
    ("player_touchdowns", "OVER", "Standard"),
    ("rush_tds", "OVER", "Standard"),
    ("fantasy_score", "OVER", "Standard"),
    ("fantasy_score", "UNDER", "Standard"),
    ("fantasy_score", "OVER", "Goblin"),
]


def main() -> None:
    boxes = [b for d in DATES if (b := load_box(d)) is not None]
    dec = prepare_decided(boxes)
    u = dedupe_unique_player_games(dec)
    u = attach_extra(u)

    print(
        f"unique={len(u)} wopr_cov={u['wopr'].notna().mean()*100:.0f}% "
        f"mobility={u.get('qb_mobility_tier', pd.Series(dtype=object)).notna().mean()*100:.0f}% "
        f"sec={u.get('opp_secondary_tier', pd.Series(dtype=object)).notna().mean()*100:.0f}%"
    )

    rows: list[dict] = []
    lifts: list[dict] = []

    for prop, side, pick in TARGETS:
        g = u[(u.prop == prop) & (u.side == side) & (u.pick == pick)].copy()
        if len(g) < 8:
            continue
        base_h = int((g.result == "HIT").sum())
        base_n = len(g)
        base_hr = rate(base_h, base_n) or 0.0
        title = f"{pick} {side} {prop}"
        print(f"\n{'='*88}")
        print(f"{title}   base={base_h}/{base_n} = {base_hr:.1f}%")
        print(f"{'='*88}")

        for lab, fn in tests_for(prop, side):
            try:
                mask = fn(g)
            except Exception as exc:  # noqa: BLE001
                print(f"  {lab:55s}  ERR {exc}")
                continue
            if not isinstance(mask, pd.Series):
                mask = pd.Series(bool(mask), index=g.index)
            mask = mask.reindex(g.index).fillna(False).astype(bool)
            h, n, hr = slice_hr(g, mask)
            if n < 4:
                continue
            lift = (hr - base_hr) if hr is not None else float("nan")
            flag = ""
            if lift >= 15 and n >= 5:
                flag = " **LIFT**"
            elif lift >= 8 and n >= 5:
                flag = " *lift*"
            elif lift <= -15 and n >= 5:
                flag = " **HURT**"
            elif lift <= -8 and n >= 5:
                flag = " *hurt*"
            print(f"  {lab:55s}  {h:3d}/{n:<3d} = {hr:5.1f}%  ({lift:+.1f}pp){flag}")
            rec = {
                "prop": prop,
                "side": side,
                "pick": pick,
                "test": lab,
                "hits": h,
                "n": n,
                "hr": hr,
                "base_hr": base_hr,
                "base_n": base_n,
                "lift_pp": lift,
            }
            rows.append(rec)
            if lift >= 8 and n >= 5:
                lifts.append(rec)

    print("\n" + "=" * 88)
    print("TOP LIFTS (lift>=+8pp, n>=5) — soft watch candidates")
    print("=" * 88)
    lifts.sort(key=lambda r: (-r["lift_pp"], -r["n"]))
    for r in lifts[:40]:
        print(
            f"  {r['pick']:8s} {r['side']:5s} {r['prop']:22s}  "
            f"{r['test']:45s}  {r['hits']}/{r['n']} = {r['hr']:.0f}%  "
            f"(base {r['base_hr']:.0f}%, {r['lift_pp']:+.0f}pp)"
        )

    out = DATA / "nfl_low_hr_soft_lift_week1_2.csv"
    pd.DataFrame(rows).to_csv(out, index=False, encoding="utf-8-sig")
    print(f"\nWrote {out} ({len(rows)} rows, {len(lifts)} lifts)")


if __name__ == "__main__":
    main()
