"""CFB implied team totals + observe-only blowout / D-veto labels.

Spread + total → implied_team_total uses the same formula as NFL step6b:
    implied = game_total/2 - team_spread/2
    projected_margin = -team_spread   # positive = this team favored

Does not change ticket gates. ESPN scoreboard (FBS group 80) is the source so
this does not spend Odds API quota. pp_vegas_gap stays player-prop cover only.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from utils.cfb_clock_script import RUN_HEAVY
from utils.cfb_opp_fill import _get_json, dates_from_start_times
from utils.cfb_playoff_metadata import cfb_abbr_lookup_keys, norm_cfb_team_abbr
from utils.defense_tiers import d_aligned

BLOWOUT_THRESHOLDS: tuple[float, ...] = (14.0, 17.0, 21.0)
DEFAULT_BLOWOUT = 17.0

CFB_SCOREBOARD_FBS = (
    "https://site.api.espn.com/apis/site/v2/sports/football/college-football"
    "/scoreboard?dates={dates}&groups=80&limit=300"
)
CFB_CORE_ODDS = (
    "https://sports.core.api.espn.com/v2/sports/football/leagues/"
    "college-football/events/{eid}/competitions/{eid}/odds"
)

_REPO = Path(__file__).resolve().parents[1]
_CACHE_DIR = _REPO / "Sports" / "CFB" / "data" / "cache"
_PASS_YARDS_KEY = re.compile(r"[^a-z0-9]+")


def implied_team_total(game_total: float, team_spread: float) -> float:
    """team_spread is this team's line (negative = favorite)."""
    return round(float(game_total) / 2.0 - float(team_spread) / 2.0, 1)


def projected_margin(team_spread: float) -> float:
    return round(-float(team_spread), 1)


def _norm_token(s: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def is_pass_yards_prop(prop: object) -> bool:
    p = _PASS_YARDS_KEY.sub("", str(prop or "").lower())
    return p in {"passyds", "passingyards", "passyards"}


def _is_goblin_over(pick_type: object, direction: object) -> bool:
    pt = str(pick_type or "").strip().lower()
    side = str(direction or "").strip().upper()
    return pt == "goblin" and side in {"OVER", "O", "HIGHER", ""}


def _name_hit(label: str, names: list[str]) -> bool:
    lab = _norm_token(label)
    if not lab:
        return False
    for n in names:
        tok = _norm_token(n)
        if tok and (tok in lab or lab in tok):
            return True
    return False


def home_spread_from_odds(
    odd: dict[str, Any] | None,
    *,
    home_names: list[str],
    away_names: list[str],
) -> float | None:
    """Return the home team's spread (negative = home favored)."""
    if not isinstance(odd, dict):
        return None
    details = str(odd.get("details") or "").strip()
    m = re.search(r"^(.*?)([+-]?\d+(?:\.\d+)?)\s*$", details)
    if m:
        label, num = m.group(1).strip(), float(m.group(2))
        if _name_hit(label, home_names):
            return num
        if _name_hit(label, away_names):
            return -num
    sp = odd.get("spread")
    try:
        if sp is not None:
            return float(sp)
    except (TypeError, ValueError):
        return None
    return None


def _game_total_from_odds(odd: dict[str, Any] | None) -> float | None:
    if not isinstance(odd, dict):
        return None
    ou = odd.get("overUnder")
    try:
        if ou is not None:
            return float(ou)
    except (TypeError, ValueError):
        return None
    return None


def _pick_odds(odds_list: object) -> dict[str, Any] | None:
    if not isinstance(odds_list, list):
        return None
    for o in odds_list:
        if isinstance(o, dict) and (
            o.get("spread") is not None or o.get("overUnder") is not None or o.get("details")
        ):
            return o
    for o in odds_list:
        if isinstance(o, dict):
            return o
    return None


def _competitor_names(c: dict[str, Any]) -> list[str]:
    team = c.get("team") or {}
    out = [
        team.get("abbreviation"),
        team.get("displayName"),
        team.get("shortDisplayName"),
        team.get("name"),
        team.get("nickname"),
        team.get("location"),
    ]
    return [str(x).strip() for x in out if str(x or "").strip()]


def _fetch_core_odds(event_id: str) -> dict[str, Any] | None:
    eid = str(event_id or "").strip()
    if not eid:
        return None
    try:
        payload = _get_json(CFB_CORE_ODDS.format(eid=eid))
    except Exception:
        return None
    items = payload.get("items") if isinstance(payload, dict) else None
    if not isinstance(items, list) or not items:
        return None
    first = items[0] if isinstance(items[0], dict) else None
    return first


def parse_scoreboard_games(
    payload: dict[str, Any],
    *,
    fetch_missing_core: bool = True,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ev in payload.get("events") or []:
        comps = (ev.get("competitions") or [None])[0]
        if not isinstance(comps, dict):
            continue
        home = away = ""
        home_names: list[str] = []
        away_names: list[str] = []
        for c in comps.get("competitors") or []:
            if not isinstance(c, dict):
                continue
            names = _competitor_names(c)
            abbr = str((c.get("team") or {}).get("abbreviation") or "").strip().upper()
            ha = str(c.get("homeAway") or "").lower()
            if ha == "home":
                home = abbr
                home_names = names
            elif ha == "away":
                away = abbr
                away_names = names
        if not home or not away:
            continue
        odd = _pick_odds(comps.get("odds"))
        if odd is None and fetch_missing_core:
            eid = str(ev.get("id") or comps.get("id") or "").strip()
            core = _fetch_core_odds(eid)
            if core:
                odd = {
                    "spread": core.get("spread"),
                    "overUnder": core.get("overUnder"),
                    "details": core.get("details"),
                }
        home_spread = home_spread_from_odds(odd, home_names=home_names, away_names=away_names)
        total = _game_total_from_odds(odd)
        home_n = norm_cfb_team_abbr(home) or home
        away_n = norm_cfb_team_abbr(away) or away
        rows.append(
            {
                "home": home_n,
                "away": away_n,
                "home_spread": home_spread,
                "game_total": total,
                "home_keys": {k.upper() for k in cfb_abbr_lookup_keys(home) | {home, home_n}},
                "away_keys": {k.upper() for k in cfb_abbr_lookup_keys(away) | {away, away_n}},
                "source": "espn",
            }
        )
    return rows


def _cache_path(day: str) -> Path:
    ds = str(day or "").strip()[:10].replace("-", "")
    return _CACHE_DIR / f"cfb_implied_lines_{ds}.json"


def _load_cache(day: str) -> list[dict[str, Any]] | None:
    path = _cache_path(day)
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    games = payload.get("games") if isinstance(payload, dict) else None
    if not isinstance(games, list):
        return None
    out: list[dict[str, Any]] = []
    for g in games:
        if not isinstance(g, dict):
            continue
        rec = dict(g)
        rec["home_keys"] = {str(x).upper() for x in (rec.get("home_keys") or [])}
        rec["away_keys"] = {str(x).upper() for x in (rec.get("away_keys") or [])}
        out.append(rec)
    return out


def _save_cache(day: str, games: list[dict[str, Any]]) -> None:
    path = _cache_path(day)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        serial = []
        for g in games:
            serial.append(
                {
                    "home": g.get("home"),
                    "away": g.get("away"),
                    "home_spread": g.get("home_spread"),
                    "game_total": g.get("game_total"),
                    "home_keys": sorted(g.get("home_keys") or []),
                    "away_keys": sorted(g.get("away_keys") or []),
                    "source": g.get("source") or "espn",
                }
            )
        path.write_text(
            json.dumps(
                {
                    "date": str(day)[:10],
                    "fetched_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "games": serial,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
    except Exception:
        pass


def fetch_cfb_game_lines(dates: list[str], *, use_cache: bool = True) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for day in dates:
        ds = str(day or "").strip()[:10]
        if len(ds) < 10:
            continue
        ymd = ds.replace("-", "")
        cached = _load_cache(ds) if use_cache else None
        games = cached
        if games is None:
            try:
                payload = _get_json(CFB_SCOREBOARD_FBS.format(dates=ymd))
            except Exception as exc:
                print(f"[CFB implied] scoreboard {ymd} failed: {exc}")
                games = []
            else:
                games = parse_scoreboard_games(payload if isinstance(payload, dict) else {})
                _save_cache(ds, games)
        for g in games or []:
            key = (str(g.get("home") or ""), str(g.get("away") or ""))
            if key in seen:
                continue
            seen.add(key)
            merged.append(g)
    return merged


def lookup_team_line(team: object, games: list[dict[str, Any]]) -> dict[str, Any] | None:
    keys = {k.upper() for k in cfb_abbr_lookup_keys(team)}
    raw = str(team or "").strip().upper()
    if raw:
        keys.add(raw)
    if not keys:
        return None
    for g in games:
        home_keys = g.get("home_keys") or set()
        away_keys = g.get("away_keys") or set()
        hs = g.get("home_spread")
        tot = g.get("game_total")
        if hs is None or tot is None:
            continue
        try:
            hs_f = float(hs)
            tot_f = float(tot)
        except (TypeError, ValueError):
            continue
        if keys & set(home_keys):
            return {
                "spread": hs_f,
                "game_total": tot_f,
                "implied_team_total": implied_team_total(tot_f, hs_f),
                "opp_implied_total": implied_team_total(tot_f, -hs_f),
                "projected_margin": projected_margin(hs_f),
                "home": g.get("home"),
                "away": g.get("away"),
            }
        if keys & set(away_keys):
            ts = -hs_f
            return {
                "spread": ts,
                "game_total": tot_f,
                "implied_team_total": implied_team_total(tot_f, ts),
                "opp_implied_total": implied_team_total(tot_f, -ts),
                "projected_margin": projected_margin(ts),
                "home": g.get("home"),
                "away": g.get("away"),
            }
    return None


def blowout_flags(margin: float | None) -> dict[str, bool]:
    out: dict[str, bool] = {}
    if margin is None:
        for t in BLOWOUT_THRESHOLDS:
            k = str(int(t))
            out[f"blowout_{k}"] = False
            out[f"trail_{k}"] = False
        out["blowout_flag"] = False
        return out
    m = float(margin)
    for t in BLOWOUT_THRESHOLDS:
        k = str(int(t))
        out[f"blowout_{k}"] = m >= t
        out[f"trail_{k}"] = m <= -t
    out["blowout_flag"] = m >= DEFAULT_BLOWOUT
    return out


def would_veto_pass_yards_goblin_d(
    *,
    blowout: bool,
    run_heavy: bool,
    d_ok: bool,
    prop: object,
    pick_type: object,
    direction: object,
) -> bool:
    """Hypothetical D discount. Observe-only — does not change ticket_70.

    Week 1 cell is the 9/05 trio only (Weigman HIT, Becht HIT, Hawkins
    MISS) = 2/3 unique. 9/12 had run-heavy blow-fav Pass Yards Goblin
    OVER (Becht, Stockton; both HIT, both D-fail) and zero D+ rows, so
    the fav×RH×D+ intersection was not re-tested. Leave logged; do not
    graduate.
    """
    if not (blowout and run_heavy and d_ok):
        return False
    if not is_pass_yards_prop(prop):
        return False
    return _is_goblin_over(pick_type, direction)


def _run_heavy(rate: object) -> bool:
    try:
        v = float(rate)
    except (TypeError, ValueError):
        return False
    if v != v:
        return False
    # Step8 prints Team Rush Rate as 0-1; tolerate a 0-100 display.
    if v > 1.5:
        v = v / 100.0
    return v >= RUN_HEAVY


def _share_summary(series: pd.Series) -> dict[str, float | int | None]:
    s = pd.to_numeric(series, errors="coerce").dropna()
    if s.empty:
        return {"n": 0, "p25": None, "p50": None, "p75": None}
    vals = s.astype(float)
    # Display columns are 0-100; keep them as percent for the log.
    if vals.max() <= 1.5:
        vals = vals * 100.0
    q = np.nanpercentile(vals.to_numpy(), [25, 50, 75])
    return {
        "n": int(len(vals)),
        "p25": round(float(q[0]), 1),
        "p50": round(float(q[1]), 1),
        "p75": round(float(q[2]), 1),
    }


def attach_implied_totals(
    df: pd.DataFrame,
    *,
    games: list[dict[str, Any]] | None = None,
    slate_date: str = "",
    team_col: str = "Team",
    time_col: str = "Game Time",
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Add observe-only script labels. Never mutates rank/ticket columns."""
    out = df.copy()
    label_cols = [
        "Game Total",
        "Spread",
        "Implied Team Total",
        "Opp Implied Total",
        "Projected Margin",
        "Blowout 14",
        "Blowout 17",
        "Blowout 21",
        "Blowout Flag",
        "Pass Yards D Veto",
    ]
    for c in label_cols:
        if c not in out.columns:
            out[c] = pd.NA

    dates: list[str] = []
    if slate_date:
        dates.append(str(slate_date).strip()[:10])
    if time_col in out.columns:
        dates.extend(dates_from_start_times(out[time_col].tolist()))
    dates = [d for d in dict.fromkeys(dates) if d and len(d) >= 10]

    if games is None:
        games = fetch_cfb_game_lines(dates) if dates else []

    n_hit = 0
    for idx, row in out.iterrows():
        rec = lookup_team_line(row.get(team_col), games)
        if not rec:
            continue
        n_hit += 1
        margin = rec["projected_margin"]
        flags = blowout_flags(margin)
        out.at[idx, "Game Total"] = rec["game_total"]
        out.at[idx, "Spread"] = rec["spread"]
        out.at[idx, "Implied Team Total"] = rec["implied_team_total"]
        out.at[idx, "Opp Implied Total"] = rec["opp_implied_total"]
        out.at[idx, "Projected Margin"] = margin
        out.at[idx, "Blowout 14"] = bool(flags["blowout_14"])
        out.at[idx, "Blowout 17"] = bool(flags["blowout_17"])
        out.at[idx, "Blowout 21"] = bool(flags["blowout_21"])
        out.at[idx, "Blowout Flag"] = bool(flags["blowout_flag"])
        d_ok = d_aligned("CFB", str(row.get("Direction") or ""), row.get("Def Tier"), str(row.get("Prop") or ""))
        veto = would_veto_pass_yards_goblin_d(
            blowout=bool(flags["blowout_flag"]),
            run_heavy=_run_heavy(row.get("Team Rush Rate")),
            d_ok=d_ok,
            prop=row.get("Prop"),
            pick_type=row.get("Pick Type"),
            direction=row.get("Direction"),
        )
        if is_pass_yards_prop(row.get("Prop")) and _is_goblin_over(row.get("Pick Type"), row.get("Direction")):
            out.at[idx, "Pass Yards D Veto"] = "Y" if veto else "N"

    meta = {
        "observe_only": True,
        "slate_date": (slate_date or (dates[0] if dates else "")),
        "dates": dates,
        "games_fetched": len(games or []),
        "rows_with_line": int(n_hit),
        "rows_total": int(len(out)),
        "source": "espn",
        "default_blowout": DEFAULT_BLOWOUT,
        "run_heavy": RUN_HEAVY,
    }
    return out, meta


def _kickoff_dates(series: pd.Series) -> list[str]:
    return dates_from_start_times(series.tolist()) if len(series) else []


def _identity_block(df: pd.DataFrame) -> dict[str, Any]:
    players = sorted({str(x).strip() for x in df.get("Player", pd.Series(dtype=str)).tolist() if str(x).strip()})
    game_ids = []
    if "Game Id" in df.columns:
        game_ids = sorted({str(x).strip() for x in df["Game Id"].tolist() if str(x).strip() and str(x).lower() != "nan"})
    kickoffs = _kickoff_dates(df["Game Time"]) if "Game Time" in df.columns else []
    return {
        "unique_players": players,
        "unique_player_n": len(players),
        "unique_game_ids": game_ids,
        "unique_game_n": len(game_ids),
        "kickoff_dates": kickoffs,
    }


def pass_yards_goblin_d_veto_report(df: pd.DataFrame) -> dict[str, Any]:
    """Sweep 14/17/21 on Pass Yards Goblin OVER rows that currently pass D."""
    empty = {"n": 0, "d_pass": 0, "by_threshold": {}, **_identity_block(df.iloc[0:0])}
    if df.empty:
        return empty
    pt = df.get("Pick Type", pd.Series("", index=df.index)).astype(str).str.strip().str.lower()
    side = df.get("Direction", pd.Series("", index=df.index)).astype(str).str.upper().str.strip()
    prop_ok = df.get("Prop", pd.Series("", index=df.index)).map(is_pass_yards_prop)
    goblin_over = pt.eq("goblin") & side.isin({"OVER", "O", "HIGHER", ""}) & prop_ok
    sub = df.loc[goblin_over].copy()
    n = int(len(sub))
    if not n:
        return empty

    d_ok = []
    run_heavy = []
    for _, row in sub.iterrows():
        d_ok.append(
            d_aligned("CFB", str(row.get("Direction") or ""), row.get("Def Tier"), str(row.get("Prop") or ""))
        )
        run_heavy.append(_run_heavy(row.get("Team Rush Rate")))
    d_ok_s = pd.Series(d_ok, index=sub.index)
    rh_s = pd.Series(run_heavy, index=sub.index)
    d_pass = int(d_ok_s.sum())

    by_threshold: dict[str, dict[str, int | float]] = {}
    for t in BLOWOUT_THRESHOLDS:
        col = f"Blowout {int(t)}"
        if col in sub.columns:
            blow = sub[col].fillna(False).astype(bool)
        else:
            margin = pd.to_numeric(sub.get("Projected Margin"), errors="coerce")
            blow = margin >= t
        veto = blow & rh_s & d_ok_s
        n_veto = int(veto.sum())
        rec = {
            "n_veto": n_veto,
            "d_pass": d_pass,
            "rate_of_d_pass": round(n_veto / d_pass, 3) if d_pass else None,
            "rate_of_goblin_over": round(n_veto / n, 3) if n else None,
        }
        if int(t) == int(DEFAULT_BLOWOUT):
            rec["identity"] = _identity_block(sub.loc[veto])
        by_threshold[str(int(t))] = rec
    ident = _identity_block(sub)
    return {"n": n, "d_pass": d_pass, "by_threshold": by_threshold, **ident}


def usage_share_observe(df: pd.DataFrame) -> dict[str, Any]:
    """Percentile sanity check for later floors. Not a gate."""
    if df.empty:
        return {}
    pt = df.get("Pick Type", pd.Series("", index=df.index)).astype(str).str.strip().str.lower()
    side = df.get("Direction", pd.Series("", index=df.index)).astype(str).str.upper().str.strip()
    prop = df.get("Prop", pd.Series("", index=df.index)).astype(str)
    over = side.isin({"OVER", "O", "HIGHER", ""})
    rush_like = prop.str.lower().str.replace(r"[^a-z0-9]+", "", regex=True).str.contains(
        r"rushyd|rushingyd|rushatt", regex=True
    )
    rec_like = prop.str.lower().str.replace(r"[^a-z0-9]+", "", regex=True).str.contains(
        r"recyd|receiving|reception", regex=True
    )
    pass_like = prop.map(is_pass_yards_prop)
    return {
        "rush_share_rush_over": _share_summary(df.loc[over & rush_like, "Rush Share"] if "Rush Share" in df.columns else pd.Series(dtype=float)),
        "tgt_share_rec_over": _share_summary(df.loc[over & rec_like, "Tgt Share"] if "Tgt Share" in df.columns else pd.Series(dtype=float)),
        "pass_share_pass_over": _share_summary(
            df.loc[over & pass_like, "Pass Share"] if "Pass Share" in df.columns else pd.Series(dtype=float)
        ),
        "proposed_floors_not_applied": {
            "rush": 0.40,
            "pass": 0.70,
            "rec": 0.18,
            "status": "provisional_unvalidated",
            "note": "Asserted before share distributions existed. Do not live-gate until hit rate by bucket is derived.",
        },
    }


def write_observe_report(path: Path, meta: dict[str, Any], df: pd.DataFrame) -> None:
    payload = {
        **meta,
        "pass_yards_goblin_d_veto": pass_yards_goblin_d_veto_report(df),
        "usage_share": usage_share_observe(df),
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def log_observe_overlay(meta: dict[str, Any], df: pd.DataFrame) -> dict[str, Any]:
    veto = pass_yards_goblin_d_veto_report(df)
    usage = usage_share_observe(df)
    print(
        f"[CFB step8] implied totals observe-only: "
        f"{meta.get('rows_with_line', 0)}/{meta.get('rows_total', 0)} rows, "
        f"{meta.get('games_fetched', 0)} games, source={meta.get('source')}"
    )
    if veto.get("n"):
        bits = []
        for t, rec in (veto.get("by_threshold") or {}).items():
            bits.append(f"{t}pt {rec.get('n_veto')}/{veto.get('d_pass')} D-pass")
        ident17 = ((veto.get("by_threshold") or {}).get("17") or {}).get("identity") or {}
        print(
            f"[CFB step8] Pass Yards Goblin D-veto observe: "
            f"n={veto['n']} D-pass={veto['d_pass']} ({', '.join(bits)}); "
            f"veto games={ident17.get('unique_game_n', 0)} "
            f"players={ident17.get('unique_player_n', 0)} "
            f"kickoffs={ident17.get('kickoff_dates') or veto.get('kickoff_dates')}"
        )
        slate = str(meta.get("slate_date") or "")[:10]
        kicks = list(ident17.get("kickoff_dates") or veto.get("kickoff_dates") or [])
        if slate and kicks and slate not in kicks:
            print(
                f"[CFB step8] WARN report slate_date={slate} is not a veto kickoff date {kicks} "
                f"— do not count this as a separate weekend"
            )
    else:
        print("[CFB step8] Pass Yards Goblin D-veto observe: no Goblin OVER pass_yds rows")
    tgt = (usage.get("tgt_share_rec_over") or {})
    rush = (usage.get("rush_share_rush_over") or {})
    if rush.get("n") or tgt.get("n"):
        print(
            f"[CFB step8] usage-share observe (not a gate): "
            f"rush OVER n={rush.get('n')} p50={rush.get('p50')}; "
            f"rec OVER tgt n={tgt.get('n')} p50={tgt.get('p50')}"
        )
    return {"meta": meta, "pass_yards_goblin_d_veto": veto, "usage_share": usage}
