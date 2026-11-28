#!/usr/bin/env python3
"""Info-only Game Lines board: tonight's spread / total / moneyline next to the ratings model.

No picks. The backtest (scripts/game_lines/backtest_game_lines.py) found the model less accurate
than the closing line in every sport, so the board shows the numbers side by side with that record.

Steps: top up data/cache/game_lines_history.db for recent days, fit ratings on games before the
slate date, pull tonight's ESPN odds, write ui_runner/templates/game_lines_latest.json.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import backfill_game_lines_history as bf  # noqa: E402
import backtest_game_lines as bt  # noqa: E402

REPO = HERE.parents[1]
OUT_PATHS = (REPO / "ui_runner" / "templates" / "game_lines_latest.json",)
BACKTEST_JSON = REPO / "data" / "reports" / "game_lines_backtest.json"
BOARD_SPORTS = ("NBA", "NFL", "CFB", "NHL")


def slate_date_default() -> str:
    for p in (REPO / "ui_runner" / "templates" / "slate_display_date.json",
              REPO / "ui_runner" / "runtime" / "slate_display_date.json"):
        try:
            d = str(json.loads(p.read_text(encoding="utf-8")).get("date") or "")[:10]
            if d:
                return d
        except Exception:
            continue
    return datetime.now(bf.ET).date().isoformat()


def top_up_history(sport: str, slate: date, days_back: int = 10) -> None:
    conn = sqlite3.connect(bf.DB_PATH)
    conn.executescript(bf.SCHEMA)
    have = {r[0] for r in conn.execute(
        "SELECT event_id FROM game_history WHERE sport=? AND odds_status='ok'", (sport,))}
    stamp = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for i in range(days_back, 0, -1):
        for g in bf.scoreboard_games(sport, slate - timedelta(days=i)):
            if g["event_id"] in have:
                continue
            odds, status = bf.fetch_odds(sport, g["event_id"])
            row = dict(g)
            for k in ("provider", "spread_open", "spread_close", "home_spread_price_close",
                      "away_spread_price_close", "total_open", "total_close", "over_price_close",
                      "under_price_close", "home_ml_open", "away_ml_open", "home_ml_close", "away_ml_close"):
                row[k] = odds.get(k)
            row["odds_status"] = status
            row["fetched_at"] = stamp
            bf.upsert(conn, row)
    conn.commit()
    conn.close()


def upcoming_games(sport: str, slate: date) -> list[dict[str, Any]]:
    sp, lg, group = bf.SPORTS[sport]
    url = (f"https://site.api.espn.com/apis/site/v2/sports/{sp}/{lg}/scoreboard"
           f"?dates={slate:%Y%m%d}&limit=500")
    if group:
        url += f"&groups={group}"
    payload = bf._get(url) or {}
    out = []
    for ev in payload.get("events", []):
        comp = (ev.get("competitions") or [{}])[0]
        state = ((comp.get("status") or {}).get("type") or {}).get("state")
        if state not in ("pre", "in"):
            continue
        if ((ev.get("season") or {}).get("type")) == 1:
            continue
        teams = {c.get("homeAway"): c for c in comp.get("competitors", [])}
        home, away = teams.get("home"), teams.get("away")
        if not home or not away:
            continue
        out.append({
            "sport": sport, "event_id": str(ev.get("id")), "game_date": slate.isoformat(),
            "start_utc": ev.get("date"), "state": state,
            "neutral_site": 1 if comp.get("neutralSite") else 0,
            "home": (home.get("team") or {}).get("abbreviation"),
            "away": (away.get("team") or {}).get("abbreviation"),
            "home_id": str((home.get("team") or {}).get("id") or ""),
            "away_id": str((away.get("team") or {}).get("id") or ""),
            "home_score": np.nan, "away_score": np.nan, "season_type": (ev.get("season") or {}).get("type"),
        })
    return out


def _no_vig_home(hml: float | None, aml: float | None) -> float | None:
    dh, da = bt.american_to_decimal(hml), bt.american_to_decimal(aml)
    if not dh or not da:
        return None
    return (1 / dh) / (1 / dh + 1 / da)


def _r(x: Any, nd: int = 1) -> float | None:
    if x is None:
        return None
    try:
        f = float(x)
    except (TypeError, ValueError):
        return None
    return None if math.isnan(f) else round(f, nd)


def build_sport(sport: str, slate: date, backtest: dict) -> dict[str, Any]:
    games = upcoming_games(sport, slate)
    rec: dict[str, Any] = {"games": [], "backtest": backtest.get(sport)}
    if not games:
        return rec
    hist = bt.load(sport)
    hist = hist[(hist["odds_status"] == "ok") & (hist["game_date"] < slate.isoformat())]
    tonight = pd.DataFrame(games)
    tonight["gd"] = pd.to_datetime(tonight["game_date"])
    both = pd.concat([hist, tonight], ignore_index=True, sort=False).sort_values(["gd", "event_id"])
    both = both.reset_index(drop=True)
    pred = bt.walk_forward(both, bt.CFG[sport])
    a, b = bt.fit_win_prob(pred[pred["home_score"].notna()])
    pred = pred[pred["event_id"].isin(tonight["event_id"])]

    for _, g in pred.iterrows():
        odds, status = bf.fetch_odds(sport, g["event_id"])
        pm = g["pred_margin"]
        p_home = None if pd.isna(pm) else 1 / (1 + math.exp(-(a + b * pm)))
        mkt_home = _no_vig_home(odds.get("home_ml_close"), odds.get("away_ml_close"))
        spread = odds.get("spread_close")
        total = odds.get("total_close")
        rec["games"].append({
            "event_id": g["event_id"], "start_utc": g["start_utc"], "state": g.get("state"),
            "away": g["away"], "home": g["home"], "neutral_site": int(g["neutral_site"] or 0),
            "odds_status": status, "provider": odds.get("provider"),
            "market": {
                "home_spread": _r(spread), "home_spread_price": _r(odds.get("home_spread_price_close"), 0),
                "away_spread_price": _r(odds.get("away_spread_price_close"), 0),
                "total": _r(total), "over_price": _r(odds.get("over_price_close"), 0),
                "under_price": _r(odds.get("under_price_close"), 0),
                "home_ml": _r(odds.get("home_ml_close"), 0), "away_ml": _r(odds.get("away_ml_close"), 0),
                "home_win_pct_no_vig": _r(None if mkt_home is None else 100 * mkt_home),
            },
            "model": {
                "home_margin": _r(pm), "home_spread_equiv": _r(None if pd.isna(pm) else -pm),
                "total": _r(g["pred_total"]),
                "home_win_pct": _r(None if p_home is None else 100 * p_home),
            },
            "model_minus_market": {
                "spread_pts": _r(None if (pd.isna(pm) or spread is None) else (-pm) - spread),
                "total_pts": _r(None if (pd.isna(g["pred_total"]) or total is None) else g["pred_total"] - total),
                "home_win_pct": _r(None if (p_home is None or mkt_home is None) else 100 * (p_home - mkt_home)),
            },
        })
    rec["games"].sort(key=lambda r: (r["start_utc"] or "", r["home"] or ""))
    return rec


def backtest_summary() -> dict[str, Any]:
    try:
        rep = json.loads(BACKTEST_JSON.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out = {}
    for sport, d in (rep.get("sports") or {}).items():
        ho = (d.get("accuracy") or {}).get("holdout") or {}
        br = (d.get("win_prob_brier") or {}).get("holdout") or {}
        markets = {}
        for m in ("spread", "total", "moneyline"):
            row0 = ((d.get(m) or {}).get("rows") or [{}])[0].get("holdout") or {}
            markets[m] = {"n": row0.get("n"), "win_rate": row0.get("win_rate"), "roi": row0.get("roi"),
                          "train_profitable_rule": (d.get(m) or {}).get("chosen_threshold") is not None,
                          "rule_holdout_roi": ((d.get(m) or {}).get("chosen_holdout") or {}).get("roi")}
        out[sport] = {
            "holdout_games": d.get("holdout_games"),
            "model_margin_mae": ho.get("model_margin_mae"), "line_margin_mae": ho.get("line_margin_mae"),
            "model_total_mae": ho.get("model_total_mae"), "line_total_mae": ho.get("line_total_mae"),
            "model_brier": br.get("model"), "market_brier": br.get("market_no_vig"),
            "markets": markets,
        }
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--date", default="", help="slate date YYYY-MM-DD (default: slate_display_date.json)")
    ap.add_argument("--sports", nargs="+", default=list(BOARD_SPORTS), choices=list(BOARD_SPORTS))
    ap.add_argument("--no-top-up", action="store_true")
    args = ap.parse_args()
    slate = date.fromisoformat(args.date or slate_date_default())
    summary = backtest_summary()
    payload: dict[str, Any] = {
        "date": slate.isoformat(),
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "info_only": True,
        "note": ("Info only — no picks. In the walk-forward backtest the model was less accurate than "
                 "the closing line in every sport, and no betting rule held up on the 2026-27 holdout."),
        "sports": {},
    }
    for sport in args.sports:
        try:
            if not args.no_top_up:
                top_up_history(sport, slate)
            payload["sports"][sport] = build_sport(sport, slate, summary)
            print(f"[{sport}] {len(payload['sports'][sport]['games'])} games on {slate}")
        except Exception as e:  # one sport failing must not blank the others
            print(f"[{sport}] FAILED: {e}")
            payload["sports"][sport] = {"games": [], "backtest": summary.get(sport), "error": str(e)[:200]}
    text = json.dumps(payload, indent=1, default=str)
    for p in OUT_PATHS:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text, encoding="utf-8")
        print("Saved", p)
    return 0


if __name__ == "__main__":
    sys.exit(main())
