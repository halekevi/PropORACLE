#!/usr/bin/env python3
"""Open → close / CLV tests on ESPN game lines (spread, total, moneyline).

Uses only open and close already stored in data/cache/game_lines_history.db.
No team ratings. Two questions, same freeze date as the ratings backtest:

  A. Closing-line value (CLV): if you bet the *open* number (home side always, or
     the side the market later moves toward / against), what is the close-relative
     edge and the ROI at the *open* price? Beating the open is a different bar
     than beating the close.

  B. Does open→close movement predict the close's miss? If the line moved, bet
     with steam (same side as the move) or fade the move, against the *closing*
     number at closing prices. Thresholds chosen on train must be profitable
     there; holdout is the result.

Outputs: data/reports/game_lines_open_close_clv.json / .md
"""
from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import backtest_game_lines as bt  # noqa: E402

REPO = HERE.parents[1]
REPORTS = REPO / "data" / "reports"
HOLDOUT = bt.HOLDOUT_START
MOVE_THRESHOLDS = {
    "NBA": (0.5, 1.0, 1.5, 2.0, 2.5, 3.0),
    "NFL": (0.5, 1.0, 1.5, 2.0, 2.5, 3.0),
    "CFB": (1.0, 2.0, 3.0, 4.0, 5.0, 6.0),
    "NHL": (0.0, 0.0),  # puck line rarely moves in our feed; still score ML/total
}
TOTAL_MOVE = {
    "NBA": (0.5, 1.0, 1.5, 2.0, 2.5, 3.0),
    "NFL": (0.5, 1.0, 1.5, 2.0, 2.5, 3.0),
    "CFB": (1.0, 1.5, 2.0, 2.5, 3.0, 4.0),
    "NHL": (0.5, 1.0),
}
ML_MOVE_PP = (1.0, 2.0, 3.0, 4.0, 5.0)  # percentage-point move in no-vig home win%


def no_vig_home(hml, aml):
    dh, da = bt.american_to_decimal(hml), bt.american_to_decimal(aml)
    if not dh or not da:
        return np.nan
    return (1 / dh) / (1 / dh + 1 / da)


def juice(series: pd.Series, *, lo: float = -150, hi: float = 150, fill: float = -110.0) -> pd.Series:
    """Standard ATS/total juice only. Outliers (CFB garbage, NHL puck-line alts) → fill."""
    s = pd.to_numeric(series, errors="coerce")
    return s.where(s.between(lo, hi), other=np.nan).fillna(fill)


def load_sport(sport: str) -> pd.DataFrame:
    df = bt.load(sport)
    df = df[df["odds_status"] == "ok"].copy()
    df = df[df["spread_open"].notna() & df["spread_close"].notna()]
    df["spread_move"] = df["spread_close"] - df["spread_open"]  # + = home gets worse (more points)
    df["total_move"] = df["total_close"] - df["total_open"]
    df["mkt_home_open"] = [no_vig_home(h, a) for h, a in zip(df["home_ml_open"], df["away_ml_open"])]
    df["mkt_home_close"] = [no_vig_home(h, a) for h, a in zip(df["home_ml_close"], df["away_ml_close"])]
    df["ml_move_pp"] = 100 * (df["mkt_home_close"] - df["mkt_home_open"])
    return df


def clv_spread_home(df: pd.DataFrame) -> dict:
    """Always bet home at the open spread / open price; grade vs open; report close CLV."""
    d = df.dropna(subset=["home_spread_price_close", "spread_open", "spread_close"]).copy()
    # Open ATS juice is not stored; ROI uses close juice clamped to standard -110 band.
    d["price"] = juice(d["home_spread_price_close"])
    cover = (d["home_score"] - d["away_score"]) + d["spread_open"]
    d["result"] = np.where(cover == 0, "push", np.where(cover > 0, "win", "loss"))
    # Home bettor CLV in points: open − close (open -3, close -2 → +1 better number).
    d["clv_pts"] = d["spread_open"] - d["spread_close"]
    out = {}
    for label, part in (("train", d[d["game_date"] < HOLDOUT]), ("holdout", d[d["game_date"] >= HOLDOUT])):
        s = bt.summarize(part)
        s["avg_clv_pts"] = round(float(part["clv_pts"].mean()), 3) if len(part) else None
        s["pct_positive_clv"] = round(float((part["clv_pts"] > 0).mean()), 4) if len(part) else None
        out[label] = s
    return out


def clv_total_over(df: pd.DataFrame) -> dict:
    d = df.dropna(subset=["total_open", "total_close"]).copy()
    d["price"] = juice(d["over_price_close"])
    diff = d["home_score"] + d["away_score"] - d["total_open"]
    d["result"] = np.where(diff == 0, "push", np.where(diff > 0, "win", "loss"))
    # Over bettor CLV: open total - close total (open 220, close 222 → over got worse number)
    d["clv_pts"] = d["total_open"] - d["total_close"]
    out = {}
    for label, part in (("train", d[d["game_date"] < HOLDOUT]), ("holdout", d[d["game_date"] >= HOLDOUT])):
        s = bt.summarize(part)
        s["avg_clv_pts"] = round(float(part["clv_pts"].mean()), 3) if len(part) else None
        s["pct_positive_clv"] = round(float((part["clv_pts"] > 0).mean()), 4) if len(part) else None
        out[label] = s
    return out


def move_predicts_miss_spread(df: pd.DataFrame, thresholds) -> dict:
    """Bet with or fade the open→close move; grade vs *close* at close prices."""
    d = df.dropna(subset=["spread_open", "spread_close"]).copy()
    # cover vs close
    cover = (d["home_score"] - d["away_score"]) + d["spread_close"]
    move = d["spread_move"]  # close - open; + means home spread rose (home less favored / more points)

    def side_table(mode: str) -> dict:
        # with_steam: bet the side the market moved toward
        #   move > 0 (home spread rose, e.g. -3 → -2 or +2 → +3): market moved toward away → bet away
        #   move < 0 (home spread fell, e.g. -3 → -4): market moved toward home → bet home
        if mode == "with_steam":
            side_home = move < 0
        else:  # fade
            side_home = move > 0
        b = d.copy()
        b["abs_edge"] = move.abs()
        b["result"] = np.where(cover == 0, "push", np.where((cover > 0) == side_home, "win", "loss"))
        b["price"] = juice(pd.Series(
            np.where(side_home, b["home_spread_price_close"], b["away_spread_price_close"]), index=b.index))
        b = b[b["abs_edge"] > 0]
        return bt.threshold_table(b, thresholds)

    return {"with_steam": side_table("with_steam"), "fade_move": side_table("fade")}


def move_predicts_miss_total(df: pd.DataFrame, thresholds) -> dict:
    d = df.dropna(subset=["total_open", "total_close"]).copy()
    diff = d["home_score"] + d["away_score"] - d["total_close"]
    move = d["total_move"]

    def side_table(mode: str) -> dict:
        # move > 0: total went up → steam on over
        if mode == "with_steam":
            side_over = move > 0
        else:
            side_over = move < 0
        b = d.copy()
        b["abs_edge"] = move.abs()
        b["result"] = np.where(diff == 0, "push", np.where((diff > 0) == side_over, "win", "loss"))
        b["price"] = juice(pd.Series(
            np.where(side_over, b["over_price_close"], b["under_price_close"]), index=b.index))
        b = b[b["abs_edge"] > 0]
        return bt.threshold_table(b, thresholds)

    return {"with_steam": side_table("with_steam"), "fade_move": side_table("fade")}


def move_predicts_miss_ml(df: pd.DataFrame) -> dict:
    d = df.dropna(subset=["mkt_home_open", "mkt_home_close"]).copy()
    d = d[d["home_score"] != d["away_score"]]
    move = d["ml_move_pp"]  # + = home win% rose

    def side_table(mode: str) -> dict:
        if mode == "with_steam":
            side_home = move > 0
        else:
            side_home = move < 0
        b = d.copy()
        b["abs_edge"] = move.abs()
        home_won = b["home_score"] > b["away_score"]
        b["result"] = np.where(home_won == side_home, "win", "loss")
        price = np.where(side_home, b["home_ml_close"], b["away_ml_close"])
        b["price"] = price
        b["dec"] = pd.Series(price, index=b.index).map(bt.american_to_decimal)
        b = b[b["abs_edge"] > 0].dropna(subset=["dec"])
        return bt.threshold_table(b, ML_MOVE_PP, decimal_col="dec")

    return {"with_steam": side_table("with_steam"), "fade_move": side_table("fade")}


def short(tt: dict) -> str:
    if tt.get("chosen_threshold") is None:
        return "no bet"
    return f"t={tt['chosen_threshold']} holdout {bt.fmt(tt['chosen_holdout'])}"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--sports", nargs="+", default=["NBA", "NFL", "CFB", "NHL"])
    args = ap.parse_args()
    REPORTS.mkdir(parents=True, exist_ok=True)

    report = {"holdout_start": HOLDOUT, "note": (
        "CLV rows bet the open number. Move→miss rows bet against the closing number at close prices. "
        "A strategy needs positive train ROI (n≥100) before its holdout number counts."
    ), "sports": {}}
    md = ["# Open → close / CLV game-lines tests", "",
          f"Train before {HOLDOUT}; holdout after. No team ratings — only ESPN open and close.",
          "",
          "**CLV:** bet the open number (always home ATS / always over). "
          "`avg_clv_pts` > 0 means the close moved in your favor on average.",
          "",
          "**Move → miss:** after the line moves, bet with steam or fade, graded vs the **close**. "
          "Thresholds must make money on train.", ""]

    for sport in args.sports:
        df = load_sport(sport)
        if len(df) < 200:
            print(f"[{sport}] skip n={len(df)}")
            continue
        sp_th = MOVE_THRESHOLDS.get(sport, (1.0, 2.0))
        tot_th = TOTAL_MOVE.get(sport, (1.0, 2.0))
        # NHL spread move thresholds: many zeros; use small set
        if sport == "NHL":
            sp_th = (0.5, 1.0)

        res = {
            "games": int(len(df)),
            "holdout_games": int((df["game_date"] >= HOLDOUT).sum()),
            "clv_always_home_spread": clv_spread_home(df),
            "clv_always_over": clv_total_over(df[df["total_open"].notna() & df["total_close"].notna()]),
            "spread_move": move_predicts_miss_spread(df, sp_th),
            "total_move": move_predicts_miss_total(df[df["total_open"].notna()], tot_th),
            "ml_move": move_predicts_miss_ml(df),
        }
        report["sports"][sport] = res

        md += [f"## {sport}", "",
               f"Games with open+close: {res['games']} (holdout {res['holdout_games']}).", ""]
        for name, block in (("CLV always home ATS (bet open)", res["clv_always_home_spread"]),
                            ("CLV always over (bet open)", res["clv_always_over"])):
            md.append(f"**{name}**")
            md.append("")
            md.append("| Split | Record / ROI | Avg CLV pts | % positive CLV |")
            md.append("|---|---|---|---|")
            for split in ("train", "holdout"):
                s = block[split]
                md.append(f"| {split} | {bt.fmt(s)} | {s.get('avg_clv_pts')} | {s.get('pct_positive_clv')} |")
            md.append("")

        for market, key in (("Spread", "spread_move"), ("Total", "total_move"), ("Moneyline", "ml_move")):
            md.append(f"**{market} move → close miss**")
            md.append("")
            for mode in ("with_steam", "fade_move"):
                tt = res[key][mode]
                md.append(f"- {mode}: {short(tt)}")
                md.append("")
                md.append("| Threshold | Train | Holdout |")
                md.append("|---|---|---|")
                for r in tt["rows"]:
                    md.append(f"| {r['threshold']} | {bt.fmt(r['train'])} | {bt.fmt(r['holdout'])} |")
                md.append("")

        print(f"[{sport}] n={res['games']} CLV home holdout {bt.fmt(res['clv_always_home_spread']['holdout'])} "
              f"clv={res['clv_always_home_spread']['holdout'].get('avg_clv_pts')} | "
              f"spread steam {short(res['spread_move']['with_steam'])} fade {short(res['spread_move']['fade_move'])} | "
              f"total steam {short(res['total_move']['with_steam'])} fade {short(res['total_move']['fade_move'])}")

    (REPORTS / "game_lines_open_close_clv.json").write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    (REPORTS / "game_lines_open_close_clv.md").write_text("\n".join(md) + "\n", encoding="utf-8")
    print("wrote", REPORTS / "game_lines_open_close_clv.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())
