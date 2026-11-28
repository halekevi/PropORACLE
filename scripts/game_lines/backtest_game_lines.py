#!/usr/bin/env python3
"""Walk-forward backtest of game lines (spread, total, moneyline) against ESPN closing numbers.

Model: before each game date, refit team ratings on earlier games only (ridge on scoring margin
with home advantage, and ridge on points scored / allowed for totals), recency-weighted.
Settings per sport are fixed below, not tuned on results.

Split: games before HOLDOUT_START train; the season since then is the holdout. Edge thresholds
are chosen on train (best ROI with n >= MIN_TRAIN_BETS, and that ROI must be positive) and the
holdout number at that threshold is the result. When no threshold made money on train the market
is "no bet" — a holdout row that happens to be positive is not a strategy. Every threshold's
holdout row is also written, labelled, for transparency.

Inputs: data/cache/game_lines_history.db (scripts/game_lines/backfill_game_lines_history.py).
Outputs: data/reports/game_lines_backtest.json / .md, data/reports/game_lines_backtest_predictions.csv
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[2]
DB_PATH = REPO / "data" / "cache" / "game_lines_history.db"
REPORTS = REPO / "data" / "reports"
HOLDOUT_START = "2026-07-01"
MIN_TRAIN_BETS = 100
BREAK_EVEN_110 = 110 / 210


@dataclass(frozen=True)
class SportCfg:
    half_life_days: float
    ridge_margin: float
    ridge_points: float
    window_days: int
    min_team_games: int
    ats_thresholds: tuple[float, ...]
    total_thresholds: tuple[float, ...]


CFG: dict[str, SportCfg] = {
    "NBA": SportCfg(90, 5.0, 5.0, 400, 5, (0, 1, 2, 3, 4, 5, 6), (0, 1, 2, 3, 4, 5, 6)),
    "NHL": SportCfg(120, 10.0, 10.0, 400, 5, (0, 0.1, 0.2, 0.3, 0.4, 0.5), (0, 0.1, 0.2, 0.3, 0.4, 0.5)),
    "NFL": SportCfg(200, 3.0, 3.0, 500, 3, (0, 1, 2, 3, 4, 5), (0, 1, 2, 3, 4, 5)),
    "CFB": SportCfg(200, 2.0, 2.0, 500, 3, (0, 2, 4, 6, 8, 10), (0, 2, 4, 6, 8, 10)),
    "CBB": SportCfg(90, 3.0, 3.0, 400, 4, (0, 1, 2, 3, 4, 5, 6), (0, 1, 2, 3, 4, 5, 6)),
}
ML_EV_THRESHOLDS = (0.0, 0.02, 0.04, 0.06, 0.08, 0.10)


def american_to_decimal(a: float | None) -> float | None:
    if a is None or (isinstance(a, float) and math.isnan(a)) or a == 0:
        return None
    return 1 + (a / 100 if a > 0 else 100 / abs(a))


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if n == 0:
        return (float("nan"), float("nan"))
    p = k / n
    den = 1 + z * z / n
    c = (p + z * z / (2 * n)) / den
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return (c - h, c + h)


def load(sport: str) -> pd.DataFrame:
    con = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM game_history WHERE sport=?", con, params=(sport,))
    con.close()
    df = df[df["home_score"].notna() & df["away_score"].notna()].copy()
    df = df[df["season_type"].fillna(2).astype(int) != 1]  # preseason out of ratings and bets
    df["gd"] = pd.to_datetime(df["game_date"])
    return df.sort_values(["gd", "event_id"]).reset_index(drop=True)


def _ridge(X: np.ndarray, y: np.ndarray, w: np.ndarray, pen: np.ndarray) -> np.ndarray:
    Xw = X * w[:, None]
    return np.linalg.solve(X.T @ Xw + np.diag(pen), Xw.T @ y)


def walk_forward(df: pd.DataFrame, cfg: SportCfg) -> pd.DataFrame:
    teams = sorted(set(df["home_id"]) | set(df["away_id"]))
    idx = {t: i for i, t in enumerate(teams)}
    nt = len(teams)
    hi = df["home_id"].map(idx).to_numpy()
    ai = df["away_id"].map(idx).to_numpy()
    hs = df["home_score"].to_numpy(float)
    as_ = df["away_score"].to_numpy(float)
    home_flag = 1.0 - df["neutral_site"].fillna(0).to_numpy(float)
    days = (df["gd"] - df["gd"].min()).dt.days.to_numpy()
    scored = ~(np.isnan(hs) | np.isnan(as_))

    pred_margin = np.full(len(df), np.nan)
    pred_total = np.full(len(df), np.nan)
    for d in np.unique(days):
        cur = np.where(days == d)[0]
        prior = np.where((days < d) & (days >= d - cfg.window_days) & scored)[0]
        if len(prior) < 50:
            continue
        gp = np.bincount(hi[prior], minlength=nt) + np.bincount(ai[prior], minlength=nt)
        w = 0.5 ** ((d - days[prior]) / cfg.half_life_days)

        # Margin: y = hfa*home_flag + r_home - r_away
        Xm = np.zeros((len(prior), nt + 1))
        Xm[np.arange(len(prior)), hi[prior]] = 1
        Xm[np.arange(len(prior)), ai[prior]] -= 1
        Xm[:, nt] = home_flag[prior]
        pen = np.r_[np.full(nt, cfg.ridge_margin), 1e-6]
        bm = _ridge(Xm, hs[prior] - as_[prior], w, pen)

        # Points: score = mu + off[team] + def[opp] + hfa_pts*home_flag (home row only)
        n = len(prior)
        Xp = np.zeros((2 * n, 2 * nt + 2))
        r = np.arange(n)
        Xp[r, hi[prior]] = 1
        Xp[r, nt + ai[prior]] = 1
        Xp[r, 2 * nt] = 1
        Xp[r, 2 * nt + 1] = home_flag[prior]
        Xp[n + r, ai[prior]] = 1
        Xp[n + r, nt + hi[prior]] = 1
        Xp[n + r, 2 * nt] = 1
        yp = np.r_[hs[prior], as_[prior]]
        wp = np.r_[w, w]
        penp = np.r_[np.full(2 * nt, cfg.ridge_points), 1e-6, 1e-6]
        bp = _ridge(Xp, yp, wp, penp)

        for g in cur:
            if gp[hi[g]] < cfg.min_team_games or gp[ai[g]] < cfg.min_team_games:
                continue
            pred_margin[g] = bm[nt] * home_flag[g] + bm[hi[g]] - bm[ai[g]]
            h_pts = bp[2 * nt] + bp[hi[g]] + bp[nt + ai[g]] + bp[2 * nt + 1] * home_flag[g]
            a_pts = bp[2 * nt] + bp[ai[g]] + bp[nt + hi[g]]
            pred_total[g] = h_pts + a_pts
    out = df.copy()
    out["pred_margin"] = pred_margin
    out["pred_total"] = pred_total
    return out


def settle_ats(p: pd.DataFrame) -> pd.DataFrame:
    p = p[p["spread_close"].notna() & p["pred_margin"].notna()].copy()
    margin = p["home_score"] - p["away_score"]
    p["cover_home"] = margin + p["spread_close"]  # >0 home covers
    p["edge"] = p["pred_margin"] + p["spread_close"]
    p["side"] = np.where(p["edge"] > 0, "home", "away")
    p["result"] = np.where(p["cover_home"] == 0, "push",
                           np.where((p["cover_home"] > 0) == (p["side"] == "home"), "win", "loss"))
    price = np.where(p["side"] == "home", p["home_spread_price_close"], p["away_spread_price_close"])
    p["price"] = pd.Series(price, index=p.index).fillna(-110.0)
    p["abs_edge"] = p["edge"].abs()
    return p


def settle_total(p: pd.DataFrame) -> pd.DataFrame:
    p = p[p["total_close"].notna() & p["pred_total"].notna()].copy()
    actual = p["home_score"] + p["away_score"]
    p["edge"] = p["pred_total"] - p["total_close"]
    p["side"] = np.where(p["edge"] > 0, "over", "under")
    diff = actual - p["total_close"]
    p["result"] = np.where(diff == 0, "push", np.where((diff > 0) == (p["side"] == "over"), "win", "loss"))
    price = np.where(p["side"] == "over", p["over_price_close"], p["under_price_close"])
    p["price"] = pd.Series(price, index=p.index).fillna(-110.0)
    p["abs_edge"] = p["edge"].abs()
    return p


def fit_win_prob(train: pd.DataFrame) -> tuple[float, float]:
    """Logistic P(home win) = 1/(1+exp(-(a + b*pred_margin))) fit on train predictions only."""
    t = train[train["pred_margin"].notna() & (train["home_score"] != train["away_score"])]
    x = t["pred_margin"].to_numpy(float)
    y = (t["home_score"] > t["away_score"]).to_numpy(float)
    a, b = 0.0, 0.1
    for _ in range(100):
        z = np.clip(a + b * x, -30, 30)
        pr = 1 / (1 + np.exp(-z))
        g = np.array([np.sum(y - pr), np.sum((y - pr) * x)])
        wv = pr * (1 - pr)
        H = np.array([[np.sum(wv), np.sum(wv * x)], [np.sum(wv * x), np.sum(wv * x * x)]]) + 1e-9 * np.eye(2)
        step = np.linalg.solve(H, g)
        a, b = a + step[0], b + step[1]
        if np.abs(step).max() < 1e-8:
            break
    return float(a), float(b)


def settle_ml(p: pd.DataFrame, a: float, b: float) -> pd.DataFrame:
    p = p[p["home_ml_close"].notna() & p["away_ml_close"].notna() & p["pred_margin"].notna()].copy()
    p = p[p["home_score"] != p["away_score"]]
    p["p_home"] = 1 / (1 + np.exp(-(a + b * p["pred_margin"])))
    dh = p["home_ml_close"].map(american_to_decimal)
    da = p["away_ml_close"].map(american_to_decimal)
    p = p[dh.notna() & da.notna()].copy()
    dh, da = dh[p.index].astype(float), da[p.index].astype(float)
    ih, ia = 1 / dh, 1 / da
    p["mkt_p_home"] = ih / (ih + ia)
    ev_home = p["p_home"] * dh - 1
    ev_away = (1 - p["p_home"]) * da - 1
    p["side"] = np.where(ev_home >= ev_away, "home", "away")
    p["ev"] = np.where(p["side"] == "home", ev_home, ev_away)
    p["dec"] = np.where(p["side"] == "home", dh, da)
    home_won = p["home_score"] > p["away_score"]
    p["result"] = np.where(home_won == (p["side"] == "home"), "win", "loss")
    p["abs_edge"] = p["ev"]
    return p


def summarize(bets: pd.DataFrame, decimal_col: str | None = None) -> dict:
    graded = bets[bets["result"] != "push"]
    n = len(graded)
    k = int((graded["result"] == "win").sum())
    if decimal_col:
        dec = graded[decimal_col].astype(float)
    else:
        dec = graded["price"].map(american_to_decimal).astype(float)
    profit = np.where(graded["result"] == "win", dec - 1, -1.0)
    lo, hi = wilson(k, n)
    return {
        "n": n, "wins": k, "pushes": int((bets["result"] == "push").sum()),
        "win_rate": round(k / n, 4) if n else None,
        "wilson_lo": round(lo, 4) if n else None, "wilson_hi": round(hi, 4) if n else None,
        "roi": round(float(profit.mean()), 4) if n else None,
        "units": round(float(profit.sum()), 2) if n else None,
        "avg_price": round(float(graded["price"].mean()), 1) if (n and "price" in graded) else None,
    }


def threshold_table(settled: pd.DataFrame, thresholds, decimal_col=None) -> dict:
    tr = settled[settled["game_date"] < HOLDOUT_START]
    ho = settled[settled["game_date"] >= HOLDOUT_START]
    rows = []
    for t in thresholds:
        rows.append({"threshold": t,
                     "train": summarize(tr[tr["abs_edge"] >= t], decimal_col),
                     "holdout": summarize(ho[ho["abs_edge"] >= t], decimal_col)})
    eligible = [r for r in rows if r["train"]["n"] >= MIN_TRAIN_BETS and (r["train"]["roi"] or 0) > 0]
    chosen = max(eligible, key=lambda r: r["train"]["roi"]) if eligible else None
    return {"rows": rows, "chosen_threshold": chosen["threshold"] if chosen else None,
            "chosen_holdout": chosen["holdout"] if chosen else None,
            "chosen_train": chosen["train"] if chosen else None}


def baselines(p: pd.DataFrame) -> dict:
    ho = p[p["game_date"] >= HOLDOUT_START]
    out = {}
    ats = ho[ho["spread_close"].notna()].copy()
    if len(ats):
        cov = (ats["home_score"] - ats["away_score"]) + ats["spread_close"]
        for name, home_side in (("always_home_ats", pd.Series(True, index=ats.index)),
                                ("always_favorite_ats", ats["spread_close"] < 0)):
            res = np.where(cov == 0, "push", np.where((cov > 0) == home_side, "win", "loss"))
            price = np.where(home_side, ats["home_spread_price_close"], ats["away_spread_price_close"])
            b = pd.DataFrame({"result": res, "price": pd.Series(price, index=ats.index).fillna(-110.0)})
            out[name] = summarize(b)
    tot = ho[ho["total_close"].notna()].copy()
    if len(tot):
        diff = tot["home_score"] + tot["away_score"] - tot["total_close"]
        res = np.where(diff == 0, "push", np.where(diff < 0, "win", "loss"))
        out["always_under"] = summarize(pd.DataFrame({"result": res, "price": tot["under_price_close"].fillna(-110.0)}))
    ml = ho[ho["home_ml_close"].notna() & ho["away_ml_close"].notna() & (ho["home_score"] != ho["away_score"])].copy()
    if len(ml):
        fav_home = ml["home_ml_close"] <= ml["away_ml_close"]
        res = np.where((ml["home_score"] > ml["away_score"]) == fav_home, "win", "loss")
        price = np.where(fav_home, ml["home_ml_close"], ml["away_ml_close"])
        out["always_favorite_ml"] = summarize(pd.DataFrame({"result": res, "price": price}))
    return out


def accuracy(p: pd.DataFrame) -> dict:
    out = {}
    for label, part in (("train", p[p["game_date"] < HOLDOUT_START]), ("holdout", p[p["game_date"] >= HOLDOUT_START])):
        m = part[part["pred_margin"].notna() & part["spread_close"].notna()]
        t = part[part["pred_total"].notna() & part["total_close"].notna()]
        margin = m["home_score"] - m["away_score"]
        tot = t["home_score"] + t["away_score"]
        out[label] = {
            "games_margin": int(len(m)),
            "model_margin_mae": round(float((m["pred_margin"] - margin).abs().mean()), 3) if len(m) else None,
            "line_margin_mae": round(float((-m["spread_close"] - margin).abs().mean()), 3) if len(m) else None,
            "games_total": int(len(t)),
            "model_total_mae": round(float((t["pred_total"] - tot).abs().mean()), 3) if len(t) else None,
            "line_total_mae": round(float((t["total_close"] - tot).abs().mean()), 3) if len(t) else None,
        }
    return out


def fmt(s: dict | None) -> str:
    if not s or not s.get("n"):
        return "n=0"
    return (f"{s['wins']}/{s['n']} = {s['win_rate']:.1%} (95% {s['wilson_lo']:.1%}–{s['wilson_hi']:.1%}), "
            f"ROI {s['roi']:+.1%}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--sports", nargs="+", default=["NBA", "NFL", "CFB", "NHL"], choices=sorted(CFG))
    args = ap.parse_args()
    REPORTS.mkdir(parents=True, exist_ok=True)

    report: dict = {"holdout_start": HOLDOUT_START, "min_train_bets": MIN_TRAIN_BETS,
                    "break_even_at_minus110": round(BREAK_EVEN_110, 4), "sports": {}}
    preds_all = []
    md = ["# Game lines backtest (spread / total / moneyline vs ESPN closing lines)", "",
          f"Train: games before {HOLDOUT_START}. Holdout: {HOLDOUT_START} onward. "
          f"Thresholds chosen on train (best ROI, n ≥ {MIN_TRAIN_BETS}); the holdout row at that "
          f"threshold is the result. Break-even at -110 is {BREAK_EVEN_110:.1%}. ROI uses each game's "
          "actual closing price.", ""]
    for sport in args.sports:
        df = load(sport)
        df = df[df["odds_status"] == "ok"].copy() if "odds_status" in df else df
        if len(df) < 200:
            print(f"[{sport}] only {len(df)} games with odds — skipped")
            continue
        cfg = CFG[sport]
        p = walk_forward(df, cfg)
        ats = settle_ats(p)
        tot = settle_total(p)
        a, b = fit_win_prob(p[p["game_date"] < HOLDOUT_START])
        ml = settle_ml(p, a, b)
        brier = {}
        for split, part in (("train", ml[ml["game_date"] < HOLDOUT_START]),
                            ("holdout", ml[ml["game_date"] >= HOLDOUT_START])):
            y = (part["home_score"] > part["away_score"]).astype(float)
            brier[split] = {
                "games": int(len(part)),
                "model": round(float(((part["p_home"] - y) ** 2).mean()), 4) if len(part) else None,
                "market_no_vig": round(float(((part["mkt_p_home"] - y) ** 2).mean()), 4) if len(part) else None,
            }
        res = {
            "win_prob_brier": brier,
            "games": int(len(df)),
            "date_range": [df["game_date"].min(), df["game_date"].max()],
            "holdout_games": int((df["game_date"] >= HOLDOUT_START).sum()),
            "accuracy": accuracy(p),
            "spread": threshold_table(ats, cfg.ats_thresholds),
            "total": threshold_table(tot, cfg.total_thresholds),
            "moneyline": threshold_table(ml, ML_EV_THRESHOLDS, decimal_col="dec"),
            "win_prob_logistic": {"a": round(a, 4), "b": round(b, 4)},
            "baselines_holdout": baselines(p),
        }
        report["sports"][sport] = res
        keep = ["sport", "event_id", "game_date", "away", "home", "away_score", "home_score",
                "spread_close", "total_close", "home_ml_close", "away_ml_close", "pred_margin", "pred_total"]
        preds_all.append(p[keep])

        acc = res["accuracy"]
        md += [f"## {sport}", "",
               f"Games with odds: {res['games']} ({res['date_range'][0]} to {res['date_range'][1]}); "
               f"holdout games: {res['holdout_games']}.", "",
               "Prediction error (mean absolute error, lower is better):", "",
               "| Split | Model margin | Closing spread | Model total | Closing total |",
               "|---|---|---|---|---|"]
        for split in ("train", "holdout"):
            s = acc[split]
            md.append(f"| {split} | {s['model_margin_mae']} | {s['line_margin_mae']} | "
                      f"{s['model_total_mae']} | {s['line_total_mae']} |")
        if sport == "NHL":
            md.append("")
            md.append("NHL 'closing spread' is the ±1.5 puck line, not an expected margin, so its margin "
                      "error above is not comparable. Use the win-probability Brier row below.")
        md += ["", "Win probability (Brier score, lower is better):", "",
               "| Split | Games | Model | Market (no-vig close) |", "|---|---|---|---|"]
        for split in ("train", "holdout"):
            s = res["win_prob_brier"][split]
            md.append(f"| {split} | {s['games']} | {s['model']} | {s['market_no_vig']} |")
        md.append("")
        for market in ("spread", "total", "moneyline"):
            tt = res[market]
            if tt["chosen_threshold"] is None:
                md.append(f"**{market.title()}** — no bet: no threshold made money on train "
                          f"(n ≥ {MIN_TRAIN_BETS}).")
            else:
                md.append(f"**{market.title()}** — chosen threshold {tt['chosen_threshold']} → holdout "
                          f"{fmt(tt['chosen_holdout'])} (train {fmt(tt['chosen_train'])})")
            md.append("")
            md.append("| Threshold | Train | Holdout |")
            md.append("|---|---|---|")
            for r in tt["rows"]:
                md.append(f"| {r['threshold']} | {fmt(r['train'])} | {fmt(r['holdout'])} |")
            md.append("")
        md.append("Holdout baselines:")
        md.append("")
        for name, s in res["baselines_holdout"].items():
            md.append(f"- {name}: {fmt(s)}")
        md.append("")
        def _short(m: str) -> str:
            tt = res[m]
            return "no bet" if tt["chosen_threshold"] is None else f"t={tt['chosen_threshold']} {fmt(tt['chosen_holdout'])}"

        print(f"[{sport}] games={res['games']} holdout={res['holdout_games']} "
              f"spread: {_short('spread')} | total: {_short('total')} | ML: {_short('moneyline')}")

    (REPORTS / "game_lines_backtest.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    (REPORTS / "game_lines_backtest.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    if preds_all:
        pd.concat(preds_all).to_csv(REPORTS / "game_lines_backtest_predictions.csv", index=False)
    print("wrote", REPORTS / "game_lines_backtest.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
