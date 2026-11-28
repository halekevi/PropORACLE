#!/usr/bin/env python3
"""Do our own NBA team features (ratings, per-stat opponent defense, pace) add anything to the closing line?

Features are built from box scores strictly before each game date (last N games per team,
preseason excluded): offensive / defensive rating per 100 possessions, pace, and per-stat
matchups (team points / rebounds / assists / threes per game vs what the opponent allows).

Two tests, fit on games before HOLDOUT_START and scored on the holdout:
  1. Model alone: ridge on actual margin / total from our features — error vs the closing line.
  2. Beyond the line: ridge on the line's miss (actual − line) from our features. If the features
     carry information the market missed, holdout residual R² is above 0 and betting the predicted
     side wins at real prices. Thresholds are picked on train and must be profitable there.

Inputs: data/cache/proporacle_ref.db (nba box scores), data/cache/game_lines_history.db.
Outputs: data/reports/nba_own_features_vs_line.json / .md
"""
from __future__ import annotations

import json
import math
import sqlite3
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(REPO / "Sports" / "NBA" / "scripts"))
import backtest_game_lines as bt  # noqa: E402
from defense_report import norm_team_abbr  # noqa: E402

HOLDOUT_START = bt.HOLDOUT_START
LAST_N = 20
MIN_GAMES = 5
RIDGE = 10.0
STATS = ["pts", "reb", "ast", "fg3m", "fga", "fta", "tov", "oreb", "stl", "blk"]
MATCHUP_STATS = ["pts", "reb", "ast", "fg3m"]
REPORTS = REPO / "data" / "reports"


def team_games() -> pd.DataFrame:
    con = sqlite3.connect(REPO / "data" / "cache" / "proporacle_ref.db")
    b = pd.read_sql(
        f"SELECT game_date, event_id, home_team, away_team, team, {', '.join(STATS)} FROM nba "
        "WHERE season_window IS NULL", con)
    con.close()
    t = b.groupby(["game_date", "event_id", "home_team", "away_team", "team"], as_index=False)[STATS].sum()
    t["opp"] = np.where(t["team"] == t["home_team"], t["away_team"], t["home_team"])
    m = t.merge(t[["event_id", "team"] + STATS], left_on=["event_id", "opp"], right_on=["event_id", "team"],
                suffixes=("", "_a"))
    m["poss"] = m.fga + 0.44 * m.fta + m.tov - m.oreb
    m["poss_a"] = m.fga_a + 0.44 * m.fta_a + m.tov_a - m.oreb_a
    m["team"] = m["team"].map(norm_team_abbr)
    m["home_c"] = m["home_team"].map(norm_team_abbr)
    m["away_c"] = m["away_team"].map(norm_team_abbr)
    return m.sort_values("game_date").reset_index(drop=True)


def features_asof(tg: pd.DataFrame, team: str, d: str) -> dict | None:
    g = tg[(tg["team"] == team) & (tg["game_date"] < d)].tail(LAST_N)
    if len(g) < MIN_GAMES:
        return None
    poss = g["poss"].sum()
    poss_a = g["poss_a"].sum()
    f = {
        "ortg": 100 * g["pts"].sum() / poss,
        "drtg": 100 * g["pts_a"].sum() / poss_a,
        "pace": (g["poss"].mean() + g["poss_a"].mean()) / 2,
    }
    for s in MATCHUP_STATS:
        f[f"{s}_for"] = g[s].mean()
        f[f"{s}_allowed"] = g[f"{s}_a"].mean()
    return f


def build(tg: pd.DataFrame, gh: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for _, r in gh.iterrows():
        h, a, d = r["home_c"], r["away_c"], r["game_date"]
        fh, fa = features_asof(tg, h, d), features_asof(tg, a, d)
        if fh is None or fa is None:
            continue
        exp_pace = (fh["pace"] + fa["pace"]) / 2
        exp_h = exp_pace * (fh["ortg"] + fa["drtg"]) / 200
        exp_a = exp_pace * (fa["ortg"] + fh["drtg"]) / 200
        x = {
            "net_rtg_diff": (fh["ortg"] - fh["drtg"]) - (fa["ortg"] - fa["drtg"]),
            "exp_pace": exp_pace,
            "exp_margin_ours": exp_h - exp_a,
            "exp_total_ours": exp_h + exp_a,
        }
        for s in MATCHUP_STATS:
            x[f"home_{s}_matchup"] = (fh[f"{s}_for"] + fa[f"{s}_allowed"]) / 2
            x[f"away_{s}_matchup"] = (fa[f"{s}_for"] + fh[f"{s}_allowed"]) / 2
        rows.append({**r.to_dict(), **x})
    return pd.DataFrame(rows)


MARGIN_FEATS = ["net_rtg_diff", "exp_margin_ours", "home_pts_matchup", "away_pts_matchup",
                "home_reb_matchup", "away_reb_matchup", "home_ast_matchup", "away_ast_matchup",
                "home_fg3m_matchup", "away_fg3m_matchup"]
TOTAL_FEATS = ["exp_pace", "exp_total_ours", "home_pts_matchup", "away_pts_matchup",
               "home_reb_matchup", "away_reb_matchup", "home_ast_matchup", "away_ast_matchup",
               "home_fg3m_matchup", "away_fg3m_matchup"]


def ridge_fit(X: np.ndarray, y: np.ndarray, lam: float = RIDGE):
    mu, sd = X.mean(0), X.std(0) + 1e-9
    Z = (X - mu) / sd
    Z1 = np.c_[np.ones(len(Z)), Z]
    pen = np.r_[0.0, np.full(Z.shape[1], lam)]
    beta = np.linalg.solve(Z1.T @ Z1 + np.diag(pen), Z1.T @ y)
    return lambda Xn: np.c_[np.ones(len(Xn)), (Xn - mu) / sd] @ beta, beta


def r2(y: np.ndarray, yhat: np.ndarray, baseline: np.ndarray) -> float:
    return float(1 - np.sum((y - yhat) ** 2) / np.sum((y - baseline) ** 2))


def bet_table(df: pd.DataFrame, pred_resid: np.ndarray, kind: str, thresholds) -> dict:
    d = df.copy()
    d["pr"] = pred_resid
    if kind == "spread":
        cover = (d["home_score"] - d["away_score"]) + d["spread_close"]
        d["side_home"] = d["pr"] > 0
        d["result"] = np.where(cover == 0, "push", np.where((cover > 0) == d["side_home"], "win", "loss"))
        d["price"] = np.where(d["side_home"], d["home_spread_price_close"], d["away_spread_price_close"])
    else:
        diff = d["home_score"] + d["away_score"] - d["total_close"]
        d["side_over"] = d["pr"] > 0
        d["result"] = np.where(diff == 0, "push", np.where((diff > 0) == d["side_over"], "win", "loss"))
        d["price"] = np.where(d["side_over"], d["over_price_close"], d["under_price_close"])
    d["price"] = pd.Series(d["price"], index=d.index).fillna(-110.0)
    d["abs_edge"] = d["pr"].abs()
    return bt.threshold_table(d, thresholds)


def ml_table(df: pd.DataFrame, pred_resid: np.ndarray, train_mask: np.ndarray) -> dict:
    d = df.copy()
    d["pred_margin"] = -d["spread_close"] + pred_resid
    a, b = bt.fit_win_prob(d[train_mask])
    ml = bt.settle_ml(d, a, b)
    return bt.threshold_table(ml, bt.ML_EV_THRESHOLDS, decimal_col="dec")


def main() -> int:
    tg = team_games()
    gh = bt.load("NBA")
    gh = gh[(gh["odds_status"] == "ok") & gh["spread_close"].notna() & gh["total_close"].notna()].copy()
    gh["home_c"] = gh["home"].map(norm_team_abbr)
    gh["away_c"] = gh["away"].map(norm_team_abbr)
    gh = gh[gh["game_date"] >= tg["game_date"].min()]
    keys = set(zip(tg["game_date"], tg["home_c"], tg["away_c"]))
    gh["box_match"] = [(d, h, a) in keys for d, h, a in zip(gh["game_date"], gh["home_c"], gh["away_c"])]
    match_rate = float(gh["box_match"].mean())
    df = build(tg, gh)
    df = df.dropna(subset=MARGIN_FEATS + TOTAL_FEATS).reset_index(drop=True)
    tr = (df["game_date"] < HOLDOUT_START).to_numpy()
    ho = ~tr

    margin = (df["home_score"] - df["away_score"]).to_numpy(float)
    total = (df["home_score"] + df["away_score"]).to_numpy(float)
    line_margin = (-df["spread_close"]).to_numpy(float)
    line_total = df["total_close"].to_numpy(float)
    Xm = df[MARGIN_FEATS].to_numpy(float)
    Xt = df[TOTAL_FEATS].to_numpy(float)

    out: dict = {"games": int(len(df)), "train_games": int(tr.sum()), "holdout_games": int(ho.sum()),
                 "box_score_match_rate": round(match_rate, 3), "last_n": LAST_N, "ridge": RIDGE,
                 "holdout_start": HOLDOUT_START}

    # 1. Model alone (no line)
    fm, _ = ridge_fit(Xm[tr], margin[tr])
    ft, _ = ridge_fit(Xt[tr], total[tr])
    alone = {}
    for name, mask in (("train", tr), ("holdout", ho)):
        alone[name] = {
            "model_margin_mae": round(float(np.abs(fm(Xm[mask]) - margin[mask]).mean()), 3),
            "line_margin_mae": round(float(np.abs(line_margin[mask] - margin[mask]).mean()), 3),
            "model_total_mae": round(float(np.abs(ft(Xt[mask]) - total[mask]).mean()), 3),
            "line_total_mae": round(float(np.abs(line_total[mask] - total[mask]).mean()), 3),
        }
    out["model_alone"] = alone

    # 2. Beyond the line: predict the line's miss
    rm = margin - line_margin
    rt = total - line_total
    gm, beta_m = ridge_fit(Xm[tr], rm[tr])
    gt, beta_t = ridge_fit(Xt[tr], rt[tr])
    prm, prt = gm(Xm), gt(Xt)
    out["residual_r2"] = {
        "margin": {"train": round(r2(rm[tr], prm[tr], np.zeros(tr.sum())), 4),
                   "holdout": round(r2(rm[ho], prm[ho], np.zeros(ho.sum())), 4)},
        "total": {"train": round(r2(rt[tr], prt[tr], np.zeros(tr.sum())), 4),
                  "holdout": round(r2(rt[ho], prt[ho], np.zeros(ho.sum())), 4)},
    }
    corr = {}
    for f in sorted(set(MARGIN_FEATS + TOTAL_FEATS)):
        x = df[f].to_numpy(float)
        corr[f] = {
            "margin_miss_train": round(float(np.corrcoef(x[tr], rm[tr])[0, 1]), 3),
            "margin_miss_holdout": round(float(np.corrcoef(x[ho], rm[ho])[0, 1]), 3),
            "total_miss_train": round(float(np.corrcoef(x[tr], rt[tr])[0, 1]), 3),
            "total_miss_holdout": round(float(np.corrcoef(x[ho], rt[ho])[0, 1]), 3),
        }
    out["feature_correlation_with_line_miss"] = corr
    out["spread"] = bet_table(df, prm, "spread", (0, 0.5, 1, 1.5, 2, 3))
    out["total"] = bet_table(df, prt, "total", (0, 0.5, 1, 1.5, 2, 3))
    out["moneyline"] = ml_table(df, prm, tr)

    REPORTS.mkdir(parents=True, exist_ok=True)
    (REPORTS / "nba_own_features_vs_line.json").write_text(json.dumps(out, indent=2, default=str), encoding="utf-8")

    md = ["# NBA: do our own features (ratings, per-stat opponent defense, pace) help beyond the line?", "",
          f"Games: {out['games']} (train {out['train_games']} before {HOLDOUT_START}, holdout {out['holdout_games']}). "
          f"Features use each team's last {LAST_N} games before the game date (min {MIN_GAMES}); "
          f"box-score match rate {match_rate:.1%}.", "",
          "## 1. Our model alone vs the closing line (mean absolute error, lower is better)", "",
          "| Split | Our margin | Line margin | Our total | Line total |", "|---|---|---|---|---|"]
    for s in ("train", "holdout"):
        a = alone[s]
        md.append(f"| {s} | {a['model_margin_mae']} | {a['line_margin_mae']} | {a['model_total_mae']} | {a['line_total_mae']} |")
    md += ["", "## 2. Do our features predict the line's miss?", "",
           "R² of predicting (actual − line). Above 0 on holdout means real information the line missed.", "",
           f"- Margin: train {out['residual_r2']['margin']['train']}, holdout {out['residual_r2']['margin']['holdout']}",
           f"- Total: train {out['residual_r2']['total']['train']}, holdout {out['residual_r2']['total']['holdout']}", "",
           "Feature correlation with the line's miss (a real signal keeps its sign on holdout):", "",
           "| Feature | Margin miss train | Margin miss holdout | Total miss train | Total miss holdout |",
           "|---|---|---|---|---|"]
    for f, c in corr.items():
        md.append(f"| {f} | {c['margin_miss_train']} | {c['margin_miss_holdout']} | {c['total_miss_train']} | {c['total_miss_holdout']} |")
    for market in ("spread", "total", "moneyline"):
        tt = out[market]
        md += ["", f"## Betting {market} on the predicted miss", ""]
        if tt["chosen_threshold"] is None:
            md.append("No bet: no threshold made money on train (n ≥ 100).")
        else:
            md.append(f"Chosen on train: threshold {tt['chosen_threshold']} → holdout {bt.fmt(tt['chosen_holdout'])} "
                      f"(train {bt.fmt(tt['chosen_train'])})")
        md += ["", "| Threshold | Train | Holdout |", "|---|---|---|"]
        for r in tt["rows"]:
            md.append(f"| {r['threshold']} | {bt.fmt(r['train'])} | {bt.fmt(r['holdout'])} |")
    (REPORTS / "nba_own_features_vs_line.md").write_text("\n".join(md) + "\n", encoding="utf-8")

    print(f"games={out['games']} train={out['train_games']} holdout={out['holdout_games']} box_match={match_rate:.1%}")
    print("model alone:", json.dumps(alone))
    print("residual R2:", json.dumps(out["residual_r2"]))
    for market in ("spread", "total", "moneyline"):
        tt = out[market]
        print(market, "no bet" if tt["chosen_threshold"] is None else
              f"t={tt['chosen_threshold']} holdout {bt.fmt(tt['chosen_holdout'])}",
              "| all games holdout:", bt.fmt(tt["rows"][0]["holdout"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
