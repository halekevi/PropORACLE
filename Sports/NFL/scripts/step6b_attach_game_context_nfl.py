#!/usr/bin/env python3
"""NFL step6b — Vegas totals/spreads + ESPN/Open-Meteo weather.

Attaches game_total, spread, implied_team_total, blowout_risk, wind_mph,
weather_flag, indoor, game_context_score. Odds API is preferred (ODDS_API_KEY);
HTTP 401 OUT_OF_USAGE_CREDITS falls back to ESPN scoreboard totals so implied
totals stay live. Weather always tries ESPN scoreboard + Open-Meteo for outdoor
stadiums. Blank all-fallback caches are ignored.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

_SCRIPT_DIR = Path(__file__).resolve().parent
_NFL_ROOT = _SCRIPT_DIR.parent
_REPO_ROOT = _SCRIPT_DIR.parents[2]
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from _nfl_pipeline_active import require_nfl_pipeline_active_or_exit
from utils.nfl_espn_context import (
    BLOWOUT_THRESHOLD,
    LOW_TOTAL_THRESH,
    ODDS_TEAM_KEYWORDS,
    attach_weather_details,
    canon_nfl_abbr,
    fetch_espn_nfl_lines,
    fetch_espn_nfl_scoreboard,
    game_context_score,
    iso_to_et_date,
    pace_tier,
    save_json,
    weather_lookup_by_team,
)
from utils.nfl_prop_defense import fill_opp_team_from_game, prop_def_axis

ODDS_API_BASE = "https://api.the-odds-api.com/v4"
ODDS_SPORT = "americanfootball_nfl"
WEATHER_CACHE = _NFL_ROOT / "data" / "nfl_weather_cache.json"

ODDS_COLS = (
    "game_total",
    "spread",
    "implied_team_total",
    "blowout_risk",
    "low_total_flag",
    "pace_tier",
    "odds_source",
    "wind_mph",
    "temp_f",
    "precip_mm",
    "weather_flag",
    "indoor",
    "venue_name",
    "game_context_score",
    "ctx_adj",
)


def _dates_from_df(df: pd.DataFrame, date_str: str) -> list[str]:
    dates: list[str] = []
    if date_str:
        dates.append(str(date_str)[:10])
    if "start_time" in df.columns:
        dates.extend(iso_to_et_date(x) for x in df["start_time"].tolist())
    if "fetch_date" in df.columns:
        dates.extend(str(x)[:10] for x in df["fetch_date"].tolist())
    return [d for d in dict.fromkeys(dates) if d and len(d) == 10]


def _bootstrap_odds_key() -> str:
    key = (os.environ.get("ODDS_API_KEY") or "").strip()
    if key:
        return key
    env_path = _REPO_ROOT / ".env"
    if not env_path.is_file():
        return ""
    try:
        for raw in env_path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            name, _, val = line.partition("=")
            if name.strip() == "ODDS_API_KEY":
                return val.strip().strip('"').strip("'")
    except OSError:
        return ""
    return ""


def fetch_odds(api_key: str) -> list[dict] | None:
    params = {
        "apiKey": api_key,
        "regions": "us",
        "markets": "totals,spreads",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }
    url = f"{ODDS_API_BASE}/sports/{ODDS_SPORT}/odds?{urlencode(params)}"
    try:
        req = Request(url, headers={"User-Agent": "PropOracle/1.0"})
        with urlopen(req, timeout=20) as resp:
            remaining = resp.headers.get("x-requests-remaining", "?")
            print(f"[NFL step6b] Odds API remaining={remaining}")
            data = json.loads(resp.read().decode("utf-8"))
            return data if isinstance(data, list) else None
    except HTTPError as exc:
        remaining = exc.headers.get("x-requests-remaining", "?") if exc.headers else "?"
        body = ""
        try:
            body = exc.read().decode("utf-8", errors="replace")[:180]
        except Exception:
            body = ""
        print(f"[NFL step6b] Odds API HTTP {exc.code} remaining={remaining} {body}")
        return None
    except Exception as exc:
        print(f"[NFL step6b] Odds API failed: {exc}")
        return None


def parse_odds_lookup(games: list[dict]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    priority = ["draftkings", "fanduel", "betmgm", "caesars"]

    def bm_score(bm: dict) -> int:
        k = str(bm.get("key") or "").lower()
        for i, p in enumerate(priority):
            if p in k:
                return i
        return 99

    for game in games:
        home = str(game.get("home_team") or "")
        away = str(game.get("away_team") or "")
        game_total = None
        spread_home = None
        spread_away = None
        for bm in sorted(game.get("bookmakers") or [], key=bm_score):
            for market in bm.get("markets") or []:
                mkey = market.get("key")
                for o in market.get("outcomes") or []:
                    name = str(o.get("name") or "")
                    pt = o.get("point")
                    try:
                        ptf = float(pt) if pt is not None else None
                    except (TypeError, ValueError):
                        ptf = None
                    if mkey == "totals" and name == "Over" and game_total is None:
                        game_total = ptf
                    elif mkey == "spreads" and ptf is not None:
                        if home.lower() in name.lower() and spread_home is None:
                            spread_home = ptf
                        elif away.lower() in name.lower() and spread_away is None:
                            spread_away = ptf
            if game_total is not None and spread_home is not None:
                break
        rec = {
            "game_total": game_total,
            "spread_home": spread_home,
            "spread_away": spread_away,
            "home_team_full": home,
            "away_team_full": away,
        }
        lookup[(home.lower(), away.lower())] = rec
        lookup[(away.lower(), home.lower())] = {
            **rec,
            "spread_home": spread_away,
            "spread_away": spread_home,
        }
    return lookup


def find_odds(team: str, opp: str, lookup: dict) -> dict | None:
    ta = canon_nfl_abbr(team)
    oa = canon_nfl_abbr(opp)
    if ta and oa:
        rec = lookup.get((ta.lower(), oa.lower()))
        if rec and rec.get("game_total") is not None:
            return {"game_total": rec.get("game_total"), "spread": rec.get("spread_home")}
    tkw = ODDS_TEAM_KEYWORDS.get(ta, team).lower()
    okw = ODDS_TEAM_KEYWORDS.get(oa, opp).lower()
    if not tkw or not okw:
        return None
    for (h, a), rec in lookup.items():
        if tkw in h and okw in a:
            return {"game_total": rec.get("game_total"), "spread": rec.get("spread_home")}
        if tkw in a and okw in h:
            return {"game_total": rec.get("game_total"), "spread": rec.get("spread_away")}
    return None


def espn_lines_lookup(rows: list[dict]) -> dict[tuple[str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str], dict[str, Any]] = {}
    for rec in rows:
        home = canon_nfl_abbr(rec.get("home"))
        away = canon_nfl_abbr(rec.get("away"))
        total = rec.get("game_total")
        home_spread = rec.get("home_spread")
        if not home or not away or total is None:
            continue
        try:
            total_f = float(total)
        except (TypeError, ValueError):
            continue
        try:
            hs = float(home_spread) if home_spread is not None else 0.0
        except (TypeError, ValueError):
            hs = 0.0
        packed = {
            "game_total": total_f,
            "spread_home": hs,
            "spread_away": -hs,
        }
        lookup[(home.lower(), away.lower())] = packed
        lookup[(away.lower(), home.lower())] = {
            "game_total": total_f,
            "spread_home": -hs,
            "spread_away": hs,
        }
    return lookup


def odds_cache_usable(df: pd.DataFrame | None) -> bool:
    """Skip all-fallback caches (blank implied totals from Odds API 401)."""
    if df is None or df.empty:
        return False
    totals = pd.to_numeric(df.get("game_total"), errors="coerce") if "game_total" in df.columns else pd.Series(dtype=float)
    if totals.notna().any():
        return True
    implied = pd.to_numeric(df.get("implied_team_total"), errors="coerce") if "implied_team_total" in df.columns else pd.Series(dtype=float)
    return bool(implied.notna().any())


def load_odds_cache(path: Path) -> pd.DataFrame | None:
    if path.is_file():
        try:
            df = pd.read_csv(path)
        except Exception as exc:
            print(f"[NFL step6b] odds cache read failed: {exc}")
            return None
        if odds_cache_usable(df):
            return df
        print(f"[NFL step6b] odds cache unusable (blank totals) {path}")
    return None


def main() -> int:
    require_nfl_pipeline_active_or_exit()
    ap = argparse.ArgumentParser(description="NFL step6b game context")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--api_key", default="")
    ap.add_argument("--cache", default="")
    ap.add_argument("--date", default="")
    args = ap.parse_args()

    df = pd.read_csv(args.input, encoding="utf-8-sig") if Path(args.input).is_file() else pd.DataFrame()
    date_str = args.date or datetime.now().strftime("%Y-%m-%d")
    cache_path = Path(args.cache) if args.cache else _NFL_ROOT / "data" / f"game_context_cache_{date_str}.csv"

    if df.empty:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"[NFL step6b] empty -> {out}")
        return 0

    df = fill_opp_team_from_game(df)
    dates = _dates_from_df(df, date_str)
    events: list[dict] = []
    for d in dates:
        events.extend(fetch_espn_nfl_scoreboard(d))
    espn_map = {e["home"]: e["away"] for e in events}
    espn_map.update({e["away"]: e["home"] for e in events})
    if espn_map:
        df = fill_opp_team_from_game(df, espn_opp_map=espn_map)
    events = attach_weather_details(events)
    wx_by_team = weather_lookup_by_team(events)
    save_json(
        WEATHER_CACHE,
        {
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
            "dates": dates,
            "games": {
                f"{e['away']}|{e['home']}": {
                    "home": e["home"],
                    "away": e["away"],
                    "indoor": e.get("indoor"),
                    "wind_mph": e.get("wind_mph"),
                    "temp_f": e.get("temp_f"),
                    "weather_flag": e.get("weather_flag"),
                    "venue_name": e.get("venue_name"),
                }
                for e in events
            },
        },
    )

    api_key = (args.api_key or "").strip() or _bootstrap_odds_key()
    cached = load_odds_cache(cache_path)
    odds_lookup: dict = {}
    if cached is not None and not cached.empty:
        print(f"[NFL step6b] using odds cache {cache_path}")
        game_odds = cached
    else:
        if not api_key:
            print("[NFL step6b] ODDS_API_KEY missing — ESPN totals fallback")
        raw = fetch_odds(api_key) if api_key else None
        if raw:
            odds_lookup = parse_odds_lookup(raw)
        source_tag = "live"
        if not odds_lookup:
            espn_rows: list[dict] = []
            for d in dates:
                espn_rows.extend(fetch_espn_nfl_lines(date_iso=d))
            odds_lookup = espn_lines_lookup(espn_rows)
            if odds_lookup:
                source_tag = "espn"
                print(f"[NFL step6b] ESPN lines fallback games={len(espn_rows)}")
            else:
                print("[NFL step6b] no Odds API or ESPN totals")
        pairs = df[["team", "opp_team"]].drop_duplicates() if "opp_team" in df.columns else df[["team"]].drop_duplicates()
        if "opp_team" not in pairs.columns:
            pairs["opp_team"] = ""
        rows = []
        for _, prow in pairs.iterrows():
            team = canon_nfl_abbr(prow.get("team"))
            opp = canon_nfl_abbr(prow.get("opp_team"))
            rec = find_odds(team, opp, odds_lookup) if odds_lookup else None
            if rec and rec.get("game_total"):
                gt = float(rec["game_total"])
                spread = float(rec["spread"]) if rec.get("spread") is not None else 0.0
                itotal = round((gt / 2.0) - (spread / 2.0), 1)
                source = source_tag
            else:
                gt = spread = itotal = None
                source = "fallback"
            rows.append(
                {
                    "team": prow.get("team"),
                    "opp_team": prow.get("opp_team"),
                    "game_total": gt,
                    "spread": spread,
                    "implied_team_total": itotal,
                    "blowout_risk": bool(spread is not None and abs(float(spread or 0)) > BLOWOUT_THRESHOLD),
                    "low_total_flag": bool(gt is not None and gt < LOW_TOTAL_THRESH),
                    "pace_tier": pace_tier(gt),
                    "odds_source": source,
                }
            )
        game_odds = pd.DataFrame(rows)
        try:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            game_odds.to_csv(cache_path, index=False)
        except Exception as exc:
            print(f"[NFL step6b] odds cache write failed: {exc}")

    df = df.drop(columns=[c for c in ODDS_COLS if c in df.columns], errors="ignore")
    merge_cols = [c for c in ("team", "opp_team") if c in df.columns and c in game_odds.columns]
    if merge_cols:
        df = df.merge(game_odds, on=merge_cols, how="left")
    else:
        for c in ODDS_COLS:
            if c not in df.columns:
                df[c] = pd.NA

    prop_col = next(
        (c for c in ("prop_type_normalized", "prop_norm", "prop_type", "stat_type") if c in df.columns),
        None,
    )
    ctx = []
    for idx, row in df.iterrows():
        team = canon_nfl_abbr(row.get("team"))
        wx = wx_by_team.get(team) or {}
        wind = wx.get("wind_mph")
        if wind is None:
            wind = row.get("wind_mph")
        df.at[idx, "wind_mph"] = wind
        df.at[idx, "temp_f"] = wx.get("temp_f", row.get("temp_f"))
        df.at[idx, "precip_mm"] = wx.get("precip_mm")
        df.at[idx, "weather_flag"] = wx.get("weather_flag") or row.get("weather_flag") or "unknown"
        df.at[idx, "indoor"] = bool(wx.get("indoor")) if wx else row.get("indoor")
        df.at[idx, "venue_name"] = wx.get("venue_name") or row.get("venue_name") or ""
        axis = prop_def_axis(row.get(prop_col) if prop_col else "")
        try:
            spread = float(row.get("spread")) if pd.notna(row.get("spread")) else None
        except (TypeError, ValueError):
            spread = None
        blowout = bool(row.get("blowout_risk"))
        low_tot = bool(row.get("low_total_flag"))
        score = game_context_score(
            axis=axis,
            wind_mph=float(wind) if wind is not None else None,
            weather_flag_val=str(df.at[idx, "weather_flag"] or ""),
            blowout=blowout,
            low_total=low_tot,
            spread=spread,
        )
        df.at[idx, "game_context_score"] = score
        ctx.append(score)
    df["ctx_adj"] = ctx
    df["blowout_risk"] = df["blowout_risk"].fillna(False)
    df["low_total_flag"] = df["low_total_flag"].fillna(False)
    df["odds_source"] = df["odds_source"].fillna("fallback")

    live = int((game_odds["odds_source"] == "live").sum()) if "odds_source" in game_odds.columns else 0
    print(
        f"[NFL step6b] odds_live={live}/{len(game_odds)} weather_games={len(events)} "
        f"ctx<0={(df['ctx_adj'] < 0).sum()}"
    )
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")
    print(f"[NFL step6b] rows={len(df)} -> {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
