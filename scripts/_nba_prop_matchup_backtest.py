"""Our gates (L5 / L10 / season avg) plus prop-matched team ranks.

For each leg the matchup uses ranks for that leg's own stat, rebuilt as of the
slate date from games strictly before it (no lookahead):
  own_off  = player's team per-game production of that stat (rank 1 = most)
  opp_def  = opponent's per-game allowed of that stat (rank 1 = fewest allowed)
Tiers: Elite 1-6, Above 7-12, Avg 13-18, Below 19-24, Weak 25-30.
Favorable OVER = own_off Elite|Above and opp_def Below|Weak. Favorable UNDER is the reverse.
Turnovers are not inverted here: opp_def rank 1 = forces the fewest turnovers.

Reads the leg CSVs from _nba_std_season_gap_backtest.py.

  py -3.14 scripts/_nba_prop_matchup_backtest.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))
sys.path.insert(0, str(ROOT / "Sports" / "NBA" / "scripts"))
from build_team_prop_ranks import build  # noqa: E402
from defense_report import norm_team_abbr  # noqa: E402
from _nba_matchup_tier_backtest import attach, ranks_asof, team_games, tier, wilson_lo  # noqa: E402

OUT = ROOT / "data" / "reports" / "nba_prop_matchup_backtest.json"
TRAIN_END = "2026-11-12"
MIN_GP = 3
LEG_TO_RANK = {
    "pts": "pts", "reb": "reb", "ast": "ast", "pra": "pra", "pr": "pr", "pa": "pa", "ra": "ra",
    "fga": "fga", "fgm": "fgm", "twopointersattempted": "fg2a", "twopointersmade": "fg2m",
    "3ptattempted": "fg3a", "3ptmade": "fg3m", "freethrowsattempted": "fta", "freethrowsmade": "ftm",
    "stl": "stl", "blk": "blk", "stocks": "bs", "tov": "tov", "fantasy": "fantasy_score",
    "personalfouls": "pf", "offensiverebounds": "oreb", "defensiverebounds": "dreb", "dunks": "dunks",
}
SCORING = {"pts", "pra", "pr", "pa", "fga", "fgm", "twopointersattempted", "twopointersmade", "3ptattempted",
           "3ptmade", "freethrowsattempted", "freethrowsmade", "fantasy", "dunks"}


def prop_ranks_asof(slates: list[str]) -> pd.DataFrame:
    out = []
    for s in slates:
        r = build("2026-27", asof=s)
        r = r[r.gp >= MIN_GP].copy()
        if r.team.nunique() < 20:
            continue
        r["own_off_p"] = r.groupby("prop_key").for_pg.rank(ascending=False, method="min")
        r["opp_def_p"] = r.groupby("prop_key").allowed_pg.rank(ascending=True, method="min")
        r["slate"] = s
        out.append(r[["slate", "team", "prop_key", "own_off_p", "opp_def_p"]])
    return pd.concat(out, ignore_index=True)


def attach_prop(legs: pd.DataFrame, pr: pd.DataFrame) -> pd.DataFrame:
    legs = legs.copy()
    legs["rank_key"] = legs.prop.map(LEG_TO_RANK)
    legs = legs[legs.rank_key.notna()]
    own = pr[["slate", "team", "prop_key", "own_off_p"]].rename(columns={"team": "team_n", "prop_key": "rank_key"})
    opp = pr[["slate", "team", "prop_key", "opp_def_p"]].rename(columns={"team": "opp_n", "prop_key": "rank_key"})
    legs = legs.merge(own, on=["slate", "team_n", "rank_key"], how="inner")
    legs = legs.merge(opp, on=["slate", "opp_n", "rank_key"], how="inner")
    legs["own_off_pt"] = legs.own_off_p.map(tier)
    legs["opp_def_pt"] = legs.opp_def_p.map(tier)
    return legs


def stat(d: pd.DataFrame, m: pd.Series, hit: str, tr: pd.Series) -> dict:
    n, k = int(m.sum()), int(d.loc[m, hit].sum())
    a, b = m & tr, m & ~tr
    return {
        "n": n, "hit": round(100 * k / n, 1) if n else None, "low": round(100 * wilson_lo(k, n), 1) if n else None,
        "early": round(100 * d.loc[a, hit].mean(), 1) if a.sum() else None, "early_n": int(a.sum()),
        "late": round(100 * d.loc[b, hit].mean(), 1) if b.sum() else None, "late_n": int(b.sum()),
    }


def fmt(name: str, s: dict) -> str:
    if not s["n"]:
        return f"  {name:<50} n=    0"
    e = f"{s['early']:5.1f}" if s["early"] is not None else "  -  "
    l_ = f"{s['late']:5.1f}" if s["late"] is not None else "  -  "
    return f"  {name:<50} n={s['n']:5}  hit={s['hit']:5.1f}%  low={s['low']:5.1f}%   early {e}% / late {l_}%"


def main() -> None:
    games = team_games()
    report = {}
    for label, f in (("STANDARD", "nba_std_season_gap_backtest.csv"), ("GOBLIN", "nba_goblin_season_gap_backtest.csv")):
        legs = pd.read_csv(ROOT / "data" / "reports" / f)
        legs = legs[(legs.push == 0) & (legs.l5_n == 5) & (legs.l10_n == 10)]
        slates = sorted(legs.slate.unique())
        d = attach(legs, ranks_asof(games, slates))
        d = attach_prop(d, prop_ranks_asof(slates))
        tr = d.slate <= TRAIN_END
        ov_good_o = d.own_off_t.isin(["Elite", "Above"])
        ov_weak_d = d.opp_def_t.isin(["Below", "Weak"])
        ov_bad_o = d.own_off_t.isin(["Below", "Weak"])
        ov_good_d = d.opp_def_t.isin(["Elite", "Above"])
        p_good_o = d.own_off_pt.isin(["Elite", "Above"])
        p_weak_d = d.opp_def_pt.isin(["Below", "Weak"])
        p_bad_o = d.own_off_pt.isin(["Below", "Weak"])
        p_good_d = d.opp_def_pt.isin(["Elite", "Above"])
        sides = [("OVER", "over_hit", "l5_over", "l10_over", d.season_avg > d.line,
                  ov_good_o & ov_weak_d, p_good_o & p_weak_d, p_weak_d, p_bad_o & p_good_d)]
        if label == "STANDARD":
            sides.append(("UNDER", "under_hit", "l5_under", "l10_under", d.season_avg < d.line,
                          ov_bad_o & ov_good_d, p_bad_o & p_good_d, p_good_d, p_good_o & p_weak_d))
        scoring = d.prop.isin(SCORING)
        for side, hit, l5, l10, szn, fav_overall, fav, fav_d, unfav in sides:
            key = f"{label}_{side}"
            rows = {}
            g5, g10 = d[l5] == 5, d[l10] >= 8
            full = g5 & g10 & szn
            no_l5 = g10 & szn
            base = pd.Series(True, index=d.index)
            rows["all legs"] = stat(d, base, hit, tr)
            for gname, g in (("L5 5/5 + L10 8+ + season", full), ("L10 8+ + season (no L5)", no_l5)):
                rows[gname] = stat(d, g, hit, tr)
                rows[f"  {gname} + overall-rank favorable"] = stat(d, g & fav_overall, hit, tr)
                rows[f"  {gname} + PROP-rank favorable"] = stat(d, g & fav, hit, tr)
                rows[f"  {gname} + PROP-rank defense only"] = stat(d, g & fav_d, hit, tr)
                rows[f"  {gname} + PROP-rank neutral"] = stat(d, g & ~fav & ~unfav, hit, tr)
                rows[f"  {gname} + PROP-rank UNfavorable"] = stat(d, g & unfav, hit, tr)
                rows[f"  {gname} + PROP fav, scoring"] = stat(d, g & fav & scoring, hit, tr)
                rows[f"  {gname} + PROP fav, non-scoring"] = stat(d, g & fav & ~scoring, hit, tr)
            by_prop = {}
            for p, grp in d[no_l5 & fav].groupby("prop"):
                if len(grp) >= 15:
                    by_prop[p] = stat(grp, pd.Series(True, index=grp.index), hit, grp.slate <= TRAIN_END)
            report[key] = {"rows": rows, "by_prop_no_l5_fav": by_prop, "legs": int(len(d))}
            print(f"\n===== {label} {side}  ({len(d)} legs with both rank sets)")
            for name, s in rows.items():
                print(fmt(name, s))
            if by_prop:
                print("  by prop, L10 8+ + season + PROP-rank favorable (n>=15):")
                for p, s in sorted(by_prop.items(), key=lambda kv: -kv[1]["hit"]):
                    print(fmt(f"    {p}", s))
    OUT.write_text(json.dumps(report, indent=1), encoding="utf-8")
    print(f"\nSaved -> {OUT}")


if __name__ == "__main__":
    main()
