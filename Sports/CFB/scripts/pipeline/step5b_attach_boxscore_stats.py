#!/usr/bin/env python3
"""
cfb_step5b_attach_boxscore_stats.py  (upgraded)
------------------------------------------------
Mirrors NBA step4 logic exactly.

Improvements over original:
- stat_season_avg added (all games in window)
- stat_last10_avg already present, now also stat_last5_avg
- line_hit_rate_over_ou_5  (last 5 vs line, excl push)
- line_hit_rate_over_ou_10 (last 10 vs line, excl push)  ← NEW
- line_hit_rate_over_5 / line_hit_rate_under_5
- MIN averages: min_last5_avg, min_season_avg
- Matches by espn_athlete_id first, then player_norm fallback

Input : step2_normalized_cfb.csv  (or step3_cfb.csv)
Output: step5b_with_stats_cfb.csv
"""

from __future__ import annotations

import argparse
import datetime as dt
import random
import re
import sys
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Tuple, Optional

import pandas as pd
import requests

# Ensure <repo>/PropOracle is on sys.path so we can import PropOracle-level helpers.
_PROPORACLE_ROOT = Path(__file__).resolve().parents[4]
if str(_PROPORACLE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROPORACLE_ROOT))

from scripts.db_utils import log_pipeline_health
from utils.dnp_zero_void import is_dnp_zero_actual
from utils.nfl_unsupported_props import is_football_period_split_prop
from utils.stat_attach_guard import decide_attach

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/plain, */*"}
ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/football/{league}/scoreboard"
ESPN_SUMMARY_URL    = "https://site.web.api.espn.com/apis/site/v2/sports/football/{league}/summary"
ESPN_LEAGUE = "college-football"


def norm(s: str) -> str:
    """Canonical player name normalizer — matches cbb_step2_normalize.norm_str()."""
    s = (s or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def request_json(url, params=None, max_tries=5, backoff=1.4, sleep=0.0):
    for i in range(1, max_tries + 1):
        try:
            if sleep: time.sleep(sleep)
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff ** (i - 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(backoff ** (i - 1))
    log_pipeline_health(
        "cfb.step5b_attach_boxscore_stats",
        "request_json_failed",
        extra={"url": url, "params": params, "max_tries": max_tries},
        start=Path(__file__),
    )
    return None


def date_range(end_date: dt.date, days_back: int) -> List[str]:
    return [(end_date - dt.timedelta(days=i)).strftime("%Y%m%d") for i in range(days_back + 1)]


def pull_scoreboard(d: str) -> dict:
    # Do NOT pass groups=50 — ESPN treats it as a tiny subset filter on dated
    # scoreboards (~4–6 games/day). Omit groups to get the full slate (~50–60).
    params = {"dates": d, "limit": "500"}
    return request_json(ESPN_SCOREBOARD_URL.format(league=ESPN_LEAGUE), params=params, sleep=0.10) or {}


def _nfl_abbr_to_team_id() -> dict[str, str]:
    """ESPN team abbreviation -> team id (NFL scoreboard filter)."""
    data = request_json(
        "https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams",
        params={"limit": "50"},
        sleep=0.05,
    ) or {}
    out: dict[str, str] = {}
    for ent in (data.get("sports") or [{}])[0].get("leagues", [{}])[0].get("teams", []) or []:
        t = ent.get("team") or {}
        ab = str(t.get("abbreviation") or "").strip().upper()
        tid = str(t.get("id") or "").strip()
        if ab and tid:
            out[ab] = tid
    _SLATE = {"LA": "LAR", "WAS": "WSH", "JAC": "JAX"}
    for k, v in _SLATE.items():
        if v in out and k not in out:
            out[k] = out[v]
    return out


def extract_events(sb: dict) -> List[Tuple[str, str, str, str]]:
    """Return list of (eid, team1_id, team2_id, date_str YYYYMMDD)."""
    out = []
    for ev in sb.get("events", []) or []:
        eid = str(ev.get("id", "")).strip()
        if not eid:
            continue
        date_str = str(ev.get("date", ""))[:10].replace("-", "")
        comps = ev.get("competitions", []) or []
        if not comps:
            continue
        competitors = comps[0].get("competitors", []) or []
        tids = []
        for c in competitors:
            tid = str((c.get("team") or {}).get("id", "")).strip()
            if tid:
                tids.append(tid)
        if len(tids) >= 2:
            out.append((eid, tids[0], tids[1], date_str))
    return out


def pull_summary(eid: str) -> dict:
    return request_json(ESPN_SUMMARY_URL.format(league=ESPN_LEAGUE), params={"event": eid}, sleep=0.08) or {}


def parse_min(x) -> float:
    try:
        s = str(x).strip()
        if s in ("", "--", "nan", "None"): return 0.0
        if ":" in s:
            mm, ss = s.split(":", 1)
            return float(mm) + float(ss) / 60.0
        return float(s)
    except Exception:
        return 0.0


def parse_players(
    summary: dict,
    game_date: str = "",
    event_id: str = "",
    team_ids: Optional[Tuple[str, str]] = None,
) -> List[dict]:
    """Parse CFB boxscore rows (passing / rushing / receiving)."""
    if not game_date:
        hdr = summary.get("header", {}) or {}
        comps = hdr.get("competitions", [{}])
        game_date = str(comps[0].get("date", "") if comps else "")[:10].replace("-", "")

    box = summary.get("boxscore", {}) or {}
    blocks = box.get("players", []) or []
    by_ath: Dict[str, dict] = {}

    def _f(val) -> float:
        try:
            s = str(val).strip()
            if s in ("", "--", "nan", "None"):
                return 0.0
            if "/" in s:
                s = s.split("/", 1)[0]
            return float(s)
        except Exception:
            return 0.0

    for tb in blocks:
        team_id = str((tb.get("team") or {}).get("id", "")).strip()
        if not team_id:
            continue
        opp_id = ""
        if team_ids and team_id in team_ids:
            opp_id = team_ids[1] if team_id == team_ids[0] else team_ids[0]
        for grp in tb.get("statistics", []) or []:
            if not isinstance(grp, dict):
                continue
            cat = str(grp.get("name", "")).strip().lower()
            labels = [str(x).upper() for x in (grp.get("labels") or [])]
            athletes = grp.get("athletes") or []
            if not labels or not athletes:
                continue

            def idx(lbl: str):
                return labels.index(lbl) if lbl in labels else None

            def _f_opt(val) -> Optional[float]:
                try:
                    s = str(val).strip()
                    if s in ("", "--", "nan", "None"):
                        return None
                    if "/" in s:
                        return None
                    return float(s)
                except Exception:
                    return None

            def _cmp_att(val) -> Tuple[Optional[float], Optional[float]]:
                s = str(val).strip()
                if s in ("", "--", "nan", "None"):
                    return None, None
                if "/" in s:
                    a, b = s.split("/", 1)
                    try:
                        return float(a.strip()), float(b.strip())
                    except (TypeError, ValueError):
                        return None, None
                return _f_opt(s), None

            for a in athletes:
                ath = a.get("athlete", {}) or {}
                aid = str(ath.get("id", "")).strip()
                pn = norm(ath.get("displayName") or ath.get("fullName") or "")
                if not pn:
                    continue
                st = a.get("stats", []) or []
                key = aid or f"{pn}|{team_id}"
                row = by_ath.setdefault(
                    key,
                    {
                        "team_id": team_id,
                        "player_norm": pn,
                        "espn_athlete_id": aid,
                        "game_date": game_date,
                        "event_id": event_id,
                        "opp_team_id": opp_id,
                    },
                )
                if opp_id and not str(row.get("opp_team_id") or "").strip():
                    row["opp_team_id"] = opp_id
                if cat == "passing":
                    y = idx("YDS")
                    td = idx("TD")
                    cmp_i = idx("C")
                    if cmp_i is None:
                        cmp_i = idx("CMP")
                    catt = idx("C/ATT")
                    att_i = idx("ATT")
                    int_i = idx("INT")
                    lng = idx("LONG")
                    if lng is None:
                        lng = idx("LNG")
                    if y is not None and y < len(st):
                        row["PASS_YDS"] = _f(st[y])
                    if td is not None and td < len(st):
                        row["PASS_TD"] = _f(st[td])
                    if catt is not None and catt < len(st):
                        cmp_v, att_v = _cmp_att(st[catt])
                        if cmp_v is not None:
                            row["PASS_CMP"] = cmp_v
                        if att_v is not None:
                            row["PASS_ATT"] = att_v
                    else:
                        if cmp_i is not None and cmp_i < len(st):
                            row["PASS_CMP"] = _f(st[cmp_i])
                        if att_i is not None and att_i < len(st):
                            v = _f_opt(st[att_i])
                            if v is not None:
                                row["PASS_ATT"] = v
                    if int_i is not None and int_i < len(st):
                        row["PASS_INT"] = _f(st[int_i])
                    if lng is not None and lng < len(st):
                        v = _f_opt(st[lng])
                        if v is not None:
                            row["PASS_LONG"] = v
                    psack = idx("SACKS")
                    if psack is None:
                        psack = idx("SACK")
                    if psack is not None and psack < len(st):
                        v = _f_opt(st[psack])
                        if v is not None:
                            row["PASS_SACKS"] = v
                elif cat == "rushing":
                    y = idx("YDS")
                    td = idx("TD")
                    car = idx("CAR")
                    if car is None:
                        car = idx("ATT")
                    lng = idx("LONG")
                    if lng is None:
                        lng = idx("LNG")
                    if y is not None and y < len(st):
                        row["RUSH_YDS"] = _f(st[y])
                    if td is not None and td < len(st):
                        row["RUSH_TD"] = _f(st[td])
                    if car is not None and car < len(st):
                        v = _f_opt(st[car])
                        if v is not None:
                            row["RUSH_ATT"] = v
                    if lng is not None and lng < len(st):
                        v = _f_opt(st[lng])
                        if v is not None:
                            row["RUSH_LONG"] = v
                elif cat == "receiving":
                    rec = idx("REC")
                    y = idx("YDS")
                    td = idx("TD")
                    lng = idx("LONG")
                    if lng is None:
                        lng = idx("LNG")
                    tgt = idx("TGTS")
                    if tgt is None:
                        tgt = idx("TGT")
                    if rec is not None and rec < len(st):
                        row["REC"] = _f(st[rec])
                    if y is not None and y < len(st):
                        row["REC_YDS"] = _f(st[y])
                    if td is not None and td < len(st):
                        row["REC_TD"] = _f(st[td])
                    if lng is not None and lng < len(st):
                        v = _f_opt(st[lng])
                        if v is not None:
                            row["REC_LONG"] = v
                    if tgt is not None and tgt < len(st):
                        v = _f_opt(st[tgt])
                        if v is not None:
                            row["REC_TGT"] = v
                elif cat == "defensive":
                    tot = idx("TOT")
                    sack = idx("SACK")
                    if sack is None:
                        sack = idx("SACKS")
                    dint = idx("INT")
                    if tot is not None and tot < len(st):
                        row["TACK_TOT"] = _f(st[tot])
                    if sack is not None and sack < len(st):
                        row["SACK"] = _f(st[sack])
                    if dint is not None and dint < len(st):
                        row["DEF_INT"] = _f(st[dint])
                elif cat == "kicking":
                    pts = idx("PTS")
                    fg = idx("FG")
                    if fg is None:
                        fg = idx("FGM")
                    xp = idx("XP")
                    if xp is None:
                        xp = idx("XPM")
                    if pts is not None and pts < len(st):
                        row["KICK_PTS"] = _f(st[pts])
                    if fg is not None and fg < len(st):
                        raw = str(st[fg]).strip()
                        if "/" in raw:
                            try:
                                row["KICK_FG"] = float(raw.split("/", 1)[0])
                            except (TypeError, ValueError):
                                pass
                        else:
                            v = _f_opt(st[fg])
                            if v is not None:
                                row["KICK_FG"] = v
                    if xp is not None and xp < len(st):
                        raw = str(st[xp]).strip()
                        if "/" in raw:
                            try:
                                row["KICK_XP"] = float(raw.split("/", 1)[0])
                            except (TypeError, ValueError):
                                pass
                        else:
                            v = _f_opt(st[xp])
                            if v is not None:
                                row["KICK_XP"] = v

    _stat_keys = (
        "PASS_YDS", "RUSH_YDS", "REC_YDS", "REC", "PASS_TD", "RUSH_TD", "REC_TD",
        "PASS_CMP", "PASS_ATT", "PASS_INT", "PASS_SACKS", "SACK", "TACK_TOT",
        "KICK_PTS", "KICK_FG", "KICK_XP", "DEF_INT",
        "PASS_LONG", "RUSH_LONG", "REC_LONG", "REC_TGT", "RUSH_ATT",
    )
    _optional_absent = {
        "PASS_LONG", "RUSH_LONG", "REC_LONG", "REC_TGT", "RUSH_ATT",
        "PASS_ATT", "PASS_CMP", "PASS_SACKS", "KICK_FG", "KICK_XP",
    }
    rows: List[dict] = []
    for row in by_ath.values():
        if not any(row.get(k) not in (None, "") for k in _stat_keys):
            continue
        for k in _stat_keys:
            if k in _optional_absent:
                continue
            row.setdefault(k, 0.0)
        row["MIN"] = 1.0
        rows.append(row)
    return rows


def game_played_for_prop(g: dict, prop: str) -> bool:
    """True when this cached game should count toward L5/L10 (prop stat exists or any CFB stat)."""
    if prop and prop_value(prop, g) is not None:
        return True
    return any(
        float(g.get(k, 0) or 0) != 0
        for k in (
            "PASS_YDS", "RUSH_YDS", "REC_YDS", "REC", "PASS_TD", "RUSH_TD", "REC_TD",
            "PASS_CMP", "PASS_INT", "SACK", "TACK_TOT", "KICK_PTS",
        )
    )


def fantasy(r: dict) -> float:
    return (
        0.04 * r.get("PASS_YDS", 0)
        + 4 * r.get("PASS_TD", 0)
        - 1 * r.get("INT", 0)
        + 0.1 * r.get("RUSH_YDS", 0)
        + 6 * r.get("RUSH_TD", 0)
        + 0.1 * r.get("REC_YDS", 0)
        + 6 * r.get("REC_TD", 0)
        + r.get("REC", 0)
    )


def _box_float(r: dict, key: str) -> Optional[float]:
    raw = r.get(key)
    if raw is None or raw == "":
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    if v != v:
        return None
    return v


def _pass_count(r: dict, key: str) -> Optional[float]:
    """Completions or attempts. A 0 beside real pass yards is a blank, not a game."""
    v = _box_float(r, key)
    yds = _box_float(r, "PASS_YDS")
    if v == 0 and yds is not None and yds > 0:
        return None
    return v


def _rush_yards_per_carry(r: dict) -> Optional[float]:
    yds = _box_float(r, "RUSH_YDS")
    att = _box_float(r, "RUSH_ATT")
    if yds is None or att is None or att <= 0:
        return None
    return round(float(yds) / float(att), 1)


def _completion_percentage(r: dict) -> Optional[float]:
    cmp_v = _pass_count(r, "PASS_CMP")
    att = _pass_count(r, "PASS_ATT")
    if cmp_v is None or att is None or att <= 0:
        return None
    return round(100.0 * float(cmp_v) / float(att), 1)


def _stat_num(r: dict, key: str) -> float:
    try:
        v = r.get(key)
        if v is None or v == "":
            return 0.0
        return float(v)
    except (TypeError, ValueError):
        return 0.0


def prop_value(prop_norm: str, r: dict) -> Optional[float]:
    p = str(prop_norm or "").strip().lower()
    p = re.sub(r"[^a-z0-9]+", "_", p).strip("_")
    # Period / first-N boards: never fuzzy-map onto full-game box columns.
    if is_football_period_split_prop(p):
        return None
    m = {
        "pass_yds": r.get("PASS_YDS"),
        "rush_yds": r.get("RUSH_YDS"),
        "rec_yds": r.get("REC_YDS"),
        "pass_td": r.get("PASS_TD"),
        "rush_td": r.get("RUSH_TD"),
        "rec_td": r.get("REC_TD"),
        "rec": r.get("REC"),
        "int": r.get("PASS_INT") or r.get("DEF_INT"),
        "passing_yards": r.get("PASS_YDS"),
        "rushing_yards": r.get("RUSH_YDS"),
        "receiving_yards": r.get("REC_YDS"),
        "passing_tds": r.get("PASS_TD"),
        "rushing_tds": r.get("RUSH_TD"),
        "receiving_tds": r.get("REC_TD"),
        "receptions": r.get("REC"),
        "pass_completions": _pass_count(r, "PASS_CMP"),
        "pass_cmp": _pass_count(r, "PASS_CMP"),
        "completions": _pass_count(r, "PASS_CMP"),
        "pass_att": _pass_count(r, "PASS_ATT"),
        "pass_attempts": _pass_count(r, "PASS_ATT"),
        "rush_att": r.get("RUSH_ATT"),
        "rush_attempts": r.get("RUSH_ATT"),
        "interceptions_thrown": r.get("PASS_INT"),
        "interceptions": r.get("PASS_INT") or r.get("DEF_INT"),
        "sacks": r.get("SACK"),
        "sacks_taken": r.get("PASS_SACKS"),
        "tackles": r.get("TACK_TOT"),
        "tackles_assists": r.get("TACK_TOT"),
        "tackles_ast": r.get("TACK_TOT"),
        "kicking_points": r.get("KICK_PTS"),
        "kick_pts": r.get("KICK_PTS"),
        "pat_made": r.get("KICK_XP"),
        "fg_made": r.get("KICK_FG"),
        "extra_points": r.get("KICK_XP"),
        "field_goals": r.get("KICK_FG"),
        "player_td": _stat_num(r, "PASS_TD") + _stat_num(r, "RUSH_TD") + _stat_num(r, "REC_TD"),
        "player_tds": _stat_num(r, "PASS_TD") + _stat_num(r, "RUSH_TD") + _stat_num(r, "REC_TD"),
        "pass_rush_yds": _stat_num(r, "PASS_YDS") + _stat_num(r, "RUSH_YDS"),
        "rush_rec_yds": _stat_num(r, "RUSH_YDS") + _stat_num(r, "REC_YDS"),
        "pass_long": r.get("PASS_LONG"),
        "rush_long": r.get("RUSH_LONG"),
        "rec_long": r.get("REC_LONG"),
        "longest_reception": r.get("REC_LONG"),
        "longest_completion": r.get("PASS_LONG"),
        "longest_rush": r.get("RUSH_LONG"),
        "rec_tgt": r.get("REC_TGT"),
        "fantasy": fantasy(r),
        "rush_yards_per_carry": _rush_yards_per_carry(r),
        "yards_per_carry": _rush_yards_per_carry(r),
        "ypc": _rush_yards_per_carry(r),
        "completion_percentage": _completion_percentage(r),
        "completion_pct": _completion_percentage(r),
        "comp_pct": _completion_percentage(r),
    }
    if p in m and m[p] is not None:
        return m[p]
    if "fantasy" in p:
        return fantasy(r)
    if "longest" in p and "completion" in p:
        return r.get("PASS_LONG")
    if "longest" in p and "rush" in p:
        return r.get("RUSH_LONG")
    if "longest" in p and ("rec" in p or "reception" in p):
        return r.get("REC_LONG")
    if "target" in p:
        return r.get("REC_TGT")
    if "rush" in p and ("att" in p or "attempt" in p):
        return r.get("RUSH_ATT")
    if "pass" in p and ("att" in p or "attempt" in p) and "td" not in p:
        return _pass_count(r, "PASS_ATT")
    if "rush" in p and "rec" in p and "yd" in p:
        return m["rush_rec_yds"]
    if "pass" in p and "rush" in p and "yd" in p:
        return m["pass_rush_yds"]
    if "pass" in p and "yard" in p:
        return r.get("PASS_YDS")
    if (
        "rush" in p
        and "yard" in p
        and "per" not in p
        and "avg" not in p
        and "ypc" not in p
    ):
        return r.get("RUSH_YDS")
    if ("rec" in p or "receiv" in p) and "yard" in p:
        return r.get("REC_YDS")
    if "reception" in p or p == "rec":
        return r.get("REC")
    if (
        ("completion" in p or p in ("pass_cmp", "cmp"))
        and "percent" not in p
        and "pct" not in p
    ):
        return _pass_count(r, "PASS_CMP")
    if "sack" in p and "taken" in p:
        return r.get("PASS_SACKS")
    if p == "sacks" or ("sack" in p and "taken" not in p):
        return r.get("SACK")
    if "kick" in p and "point" in p:
        return r.get("KICK_PTS")
    if p in ("pat_made", "extra_points") or ("pat" in p and "made" in p):
        return r.get("KICK_XP")
    if p in ("fg_made", "field_goals") or ("field" in p and "goal" in p):
        return r.get("KICK_FG")
    if "fg" in p and "made" in p:
        return r.get("KICK_FG")
    if "tackle" in p:
        return r.get("TACK_TOT")
    if "touchdown" in p or p in ("player_td", "player_tds", "tds"):
        return m["player_td"]
    return m.get(p)


def hit_rates(vals: List[float], line: float, n: int):
    """Compute hit rate over/under/push for last n games, excl push."""
    sub = vals[:n]
    over = sum(1 for v in sub if v > line)
    under = sum(1 for v in sub if v < line)
    push  = sum(1 for v in sub if v == line)
    denom_ou = len(sub) - push
    hr_over_ou  = over  / denom_ou if denom_ou > 0 else None
    hr_under_ou = under / denom_ou if denom_ou > 0 else None
    hr_over     = over  / len(sub) if sub else None
    hr_under    = under / len(sub) if sub else None
    return over, under, push, hr_over, hr_under, hr_over_ou, hr_under_ou


def _fetch_one_event_cfb(
    eid: str,
    t1: str,
    t2: str,
    slate_ids: set,
    date_str: str = "",   # FIX: was missing, causing TypeError
) -> Tuple[str, List[dict]]:
    """Fetch and parse a single CFB ESPN event. Returns (eid, player_rows)."""
    if slate_ids and t1 not in slate_ids and t2 not in slate_ids:
        return eid, []
    try:
        time.sleep(random.uniform(0.05, 0.25))
        summ = pull_summary(eid)
        return eid, parse_players(
            summ, game_date=date_str, event_id=eid, team_ids=(str(t1), str(t2))
        )
    except Exception as e:
        print(f"  [WARN] CFB summary failed event={eid}: {e}")
        log_pipeline_health(
            "cfb.step5b_attach_boxscore_stats",
            "event_summary_failed",
            extra={"event_id": eid, "error": f"{type(e).__name__}: {e}"},
            start=Path(__file__),
        )
        return eid, []


def _events_cache_path(cache_path: str) -> str:
    p = Path(cache_path)
    return str(p.with_name(p.stem + "_events.csv"))


def _load_scoreboard_events(path: str) -> List[Tuple[str, str, str, str]]:
    if not path or not Path(path).is_file():
        return []
    try:
        df = pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return []
    out: List[Tuple[str, str, str, str]] = []
    for rec in df.to_dict("records"):
        eid = str(rec.get("event_id", "")).strip()
        date_str = str(rec.get("game_date", "")).strip().replace("-", "")[:8]
        t1 = str(rec.get("team1_id", "")).strip()
        t2 = str(rec.get("team2_id", "")).strip()
        if eid and date_str and t1 and t2:
            out.append((eid, t1, t2, date_str))
    return out


def _seed_events_from_box_rows(rows: List[dict]) -> List[Tuple[str, str, str, str]]:
    grouped: dict = {}
    for rr in rows:
        eid = str(rr.get("event_id") or "").strip()
        tid = str(rr.get("team_id") or "").strip()
        date_str = str(rr.get("game_date") or "").strip().replace("-", "")[:8]
        if not eid or not date_str:
            continue
        slot = grouped.setdefault(eid, {"date": date_str, "teams": []})
        if tid and tid not in slot["teams"]:
            slot["teams"].append(tid)
    out: List[Tuple[str, str, str, str]] = []
    for eid, slot in grouped.items():
        teams = slot["teams"]
        if len(teams) >= 2:
            out.append((eid, teams[0], teams[1], slot["date"]))
    return out


def _scanned_dates_path(cache_path: str) -> str:
    p = Path(cache_path)
    return str(p.with_name(p.stem + "_scoreboard_dates.txt"))


def _load_scanned_dates(path: str) -> set:
    if not path or not Path(path).is_file():
        return set()
    return {ln.strip() for ln in Path(path).read_text(encoding="utf-8").splitlines() if ln.strip()}


def _save_scanned_dates(path: str, dates: set) -> None:
    if not path:
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text("\n".join(sorted(dates)) + ("\n" if dates else ""), encoding="utf-8")


def _save_scoreboard_events(path: str, events: List[Tuple[str, str, str, str]]) -> None:
    if not path:
        return
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {"event_id": eid, "team1_id": t1, "team2_id": t2, "game_date": date_str}
            for eid, t1, t2, date_str in events
        ]
    ).to_csv(path, index=False)


def build_player_histories(
    days: int,
    slate_ids: set,
    workers: int = 4,
    cache_path: str = "",
    tid_to_abbr: dict = None,
    end_date: Optional[dt.date] = None,
) -> Tuple[Dict, Dict]:
    """
    Parallelized boxscore fetch for CFB with persistent cache + deterministic ordering.
    Returns (hist_aid, hist_name) sorted newest-first per player.
    """
    import os

    # Phase 0: load cache
    cached_rows: List[dict] = []
    cached_eids: set = set()
    if cache_path and os.path.exists(cache_path):
        try:
            cache_df = pd.read_csv(cache_path, dtype=str).fillna("")
            cached_rows = cache_df.to_dict("records")
            for rr in cached_rows:
                for col in (
                    "PASS_YDS", "RUSH_YDS", "REC_YDS", "REC", "PASS_TD", "RUSH_TD", "REC_TD",
                    "PASS_CMP", "PASS_INT", "SACK", "TACK_TOT", "KICK_PTS", "DEF_INT",
                ):
                    if col in rr:
                        try:
                            rr[col] = float(rr[col])
                        except Exception:
                            rr[col] = 0.0
            print(f"  [CACHE] Loaded {len(cached_rows)} rows (opponent filled after scoreboard scan)")
        except Exception as e:
            print(f"  [CACHE] Load failed ({e}) — full refresh")
            cached_rows, cached_eids = [], set()
    # Phase 1: scoreboards. Completed dates stay on disk; only the last few days are live.
    events_path = _events_cache_path(cache_path) if cache_path else ""
    dates_path = _scanned_dates_path(cache_path) if cache_path else ""
    stored_events = _load_scoreboard_events(events_path)
    if not stored_events and cached_rows:
        stored_events = _seed_events_from_box_rows(cached_rows)
        if stored_events:
            print(f"  [CACHE] Seeded {len(stored_events)} scoreboard events from box-score rows")
    scanned_dates = _load_scanned_dates(dates_path)
    by_date: dict = {}
    for ev in stored_events:
        by_date.setdefault(ev[3], []).append(ev)
    anchor = end_date or dt.date.today()
    dates = date_range(anchor, days)
    newest = ""
    for rr in cached_rows:
        gd = str(rr.get("game_date") or "").strip().replace("-", "")[:8]
        if len(gd) == 8 and gd > newest:
            newest = gd
    if not newest and stored_events:
        newest = max(ev[3] for ev in stored_events)
    if newest and not scanned_dates:
        oldest = min((ev[3] for ev in stored_events), default=newest)
        for d in dates:
            if oldest <= d < newest:
                scanned_dates.add(d)
        print(
            f"  [CACHE] Marked {len(scanned_dates)} completed scoreboard days "
            f"before {newest}; live scan starts at the cache's newest date"
        )
    if ESPN_LEAGUE == "nfl" and cached_rows:
        season_start = f"{anchor.year if anchor.month >= 9 else anchor.year - 1}0901"
        n_current = 0
        for rr in cached_rows:
            gd = str(rr.get("game_date") or "").strip().replace("-", "")[:8]
            if len(gd) == 8 and gd >= season_start:
                n_current += 1
        print(
            f"  [CACHE] current-season box rows={n_current} "
            f"(game_date>={season_start}; these rows feed games_used)"
        )
    all_events: List[Tuple[str, str, str, str]] = []
    seen_eids: set = set()
    live_dates: List[str] = []
    cached_dates = 0
    for d in dates:
        if newest and d < newest and (d in scanned_dates or by_date.get(d)):
            cached_dates += 1
            for ev in by_date.get(d, []):
                if ev[0] not in seen_eids:
                    seen_eids.add(ev[0])
                    all_events.append(ev)
        else:
            live_dates.append(d)
    print(
        f"-> Scoreboards: {cached_dates} completed days from cache, "
        f"{len(live_dates)} days live from {newest or 'the start of the window'} ({ESPN_LEAGUE})"
    )
    try:
        from tqdm import tqdm as _tqdm
    except ImportError:
        import subprocess as _sp, sys as _sys
        _sp.check_call([_sys.executable, "-m", "pip", "install", "tqdm", "--break-system-packages", "-q"])
        from tqdm import tqdm as _tqdm
    if live_dates:
        for d in _tqdm(live_dates, desc="Scanning scoreboards", unit="day"):
            sb = pull_scoreboard(d)
            day_events = extract_events(sb)
            by_date[d] = day_events
            scanned_dates.add(d)
            for ev in day_events:
                if ev[0] not in seen_eids:
                    seen_eids.add(ev[0])
                    all_events.append(ev)
    if dates_path:
        _save_scanned_dates(dates_path, scanned_dates)
    if events_path:
        merged: List[Tuple[str, str, str, str]] = []
        seen_save: set = set()
        for d in dates:
            for ev in by_date.get(d, []):
                if ev[0] not in seen_save:
                    seen_save.add(ev[0])
                    merged.append(ev)
        _save_scoreboard_events(events_path, merged)

    def _row_has_opp(rr: dict) -> bool:
        abbr = str(rr.get("opp_team_abbr", "")).strip()
        oid = str(rr.get("opp_team_id", "")).strip()
        return abbr not in ("", "nan") or oid not in ("", "nan")

    event_teams = {eid: (t1, t2) for eid, t1, t2, _ds in all_events}
    filled_opp = 0
    still_missing: set = set()
    for rr in cached_rows:
        if _row_has_opp(rr):
            continue
        eid = str(rr.get("event_id", "")).strip()
        teams = event_teams.get(eid)
        tid = str(rr.get("team_id", "")).strip()
        if teams and tid in teams:
            rr["opp_team_id"] = teams[1] if tid == teams[0] else teams[0]
            filled_opp += 1
        elif eid:
            still_missing.add(eid)
    if filled_opp:
        print(f"  [CACHE] Filled opponent on {filled_opp} cached rows from scoreboards")
    if still_missing:
        cached_rows = [rr for rr in cached_rows if str(rr.get("event_id", "")) not in still_missing]
        print(f"  [CACHE] Dropped {len(still_missing)} events still missing opponent — will refetch")
    cached_eids = {str(rr.get("event_id", "")) for rr in cached_rows if rr.get("event_id")}

    if slate_ids:
        all_events = [(e, t1, t2, ds) for e, t1, t2, ds in all_events
                      if t1 in slate_ids or t2 in slate_ids]

    pending = [(eid, t1, t2, ds) for eid, t1, t2, ds in all_events
               if eid not in cached_eids]

    print(f"-> {len(all_events)} total | {len(cached_eids)} cached | "
          f"{len(pending)} new ({workers} workers)...")

    # Phase 2: parallel fetch
    new_rows: List[dict] = []
    if pending:
        from concurrent.futures import ThreadPoolExecutor, as_completed as _ac
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_fetch_one_event_cfb, eid, t1, t2, slate_ids, ds): eid
                for eid, t1, t2, ds in pending
            }
            fetched = 0
            with _tqdm(total=len(pending), desc="Fetching games", unit="game") as pbar:
                for future in _ac(futures):
                    eid, rows = future.result()
                    if rows:
                        fetched += 1
                        new_rows.extend(rows)
                    pbar.update(1)
        print(f"  [FETCH] Got {fetched} new games with player data")

    # Phase 3: update cache
    all_rows = cached_rows + new_rows
    # Resolve opp_team_id -> opp_team_abbr using slate map (for H2H matching in step6)
    if tid_to_abbr:
        for rr in all_rows:
            if not rr.get("opp_team_abbr"):
                opp_id = str(rr.get("opp_team_id", "")).strip()
                rr["opp_team_abbr"] = tid_to_abbr.get(opp_id, opp_id)
    if cache_path and (new_rows or filled_opp):
        try:
            pd.DataFrame(all_rows).to_csv(cache_path, index=False)
            print(f"  [CACHE] Saved {len(all_rows)} rows -> {cache_path}")
        except Exception as e:
            print(f"  [CACHE] Save failed: {e}")

    # Phase 4: build sorted, deduplicated histories (newest-first)
    raw_by_aid:  Dict[Tuple[str,str], List[dict]] = {}
    raw_by_name: Dict[Tuple[str,str], List[dict]] = {}
    for rr in all_rows:
        tid = str(rr.get("team_id","")).strip()
        aid = str(rr.get("espn_athlete_id","")).strip()
        pn  = str(rr.get("player_norm","")).strip()
        if tid and aid: raw_by_aid.setdefault((tid, aid), []).append(rr)
        if tid and pn:  raw_by_name.setdefault((tid, pn), []).append(rr)

    def dedup_sort(game_list):
        sorted_games = sorted(game_list, key=lambda r: r.get("game_date",""), reverse=True)
        seen, out = set(), []
        for g in sorted_games:
            eid = str(g.get("event_id",""))
            if eid and eid in seen: continue
            if eid: seen.add(eid)
            out.append(g)
        return out

    hist_aid  = {k: dedup_sort(v) for k, v in raw_by_aid.items()}
    hist_name = {k: dedup_sort(v) for k, v in raw_by_name.items()}

    print(f"-> {ESPN_LEAGUE} histories built | by_id={len(hist_aid)} | by_name={len(hist_name)}")
    return hist_aid, hist_name


def main():
    try:
        from tqdm import tqdm as _tqdm
    except ImportError:
        import subprocess as _sp, sys as _sys
        _sp.check_call([_sys.executable, "-m", "pip", "install", "tqdm", "--break-system-packages", "-q"])
        from tqdm import tqdm as _tqdm

    ap = argparse.ArgumentParser()
    ap.add_argument("--input",    required=True)
    ap.add_argument("--output",   default="step5b_with_stats_cfb.csv")
    ap.add_argument("--days",     type=int, default=180,
                    help="Days of history to scan backward from --date (default 180 for cross-season L5)")
    ap.add_argument("--date",     default="",
                    help="Anchor date YYYY-MM-DD for scoreboard scan (default: today)")
    ap.add_argument("--n",        type=int, default=10)
    ap.add_argument("--workers",  type=int, default=4)
    ap.add_argument("--cache",    default="cfb_boxscore_cache.csv",
                    help="Persistent cache CSV path (default: cfb_boxscore_cache.csv)")
    ap.add_argument(
        "--league",
        default="auto",
        choices=["auto", "college-football", "wocollege-football", "nfl"],
        help="ESPN league slug for scoreboard/summary fetches (default: auto by wcbb in paths).",
    )
    ap.add_argument("--no_cache", action="store_true",
                    help="Ignore existing cache and force full refresh")
    args = ap.parse_args()
    cache_path = "" if args.no_cache else args.cache

    global ESPN_LEAGUE
    if args.league == "auto":
        hint = f"{args.input} {args.output} {cache_path}".lower()
        if "nfl" in hint:
            ESPN_LEAGUE = "nfl"
        else:
            ESPN_LEAGUE = "wocollege-football" if "wcbb" in hint else "college-football"
    else:
        ESPN_LEAGUE = args.league
    print(f"-> ESPN league: {ESPN_LEAGUE}")

    print("→ Loading:", args.input)
    try:
        df = pd.read_csv(args.input, dtype=str).fillna("")
    except Exception as e:
        log_pipeline_health(
            "cfb.step5b_attach_boxscore_stats",
            "read_failed",
            extra={"input": args.input, "error": f"{type(e).__name__}: {e}"},
            start=Path(__file__),
        )
        raise
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    if "player_norm" not in df.columns:
        pname_col = next((c for c in ("player", "player_name", "pp_player") if c in df.columns), None)
        if pname_col:
            df["player_norm"] = df[pname_col].astype(str).apply(norm)
        else:
            df["player_norm"] = ""
    if ESPN_LEAGUE == "nfl":
        if "player" not in df.columns and "player_name" in df.columns:
            df["player"] = df["player_name"]
        if "team_abbr" not in df.columns and "team" in df.columns:
            df["team_abbr"] = df["team"].astype(str).str.strip().str.upper()
        abbr_map = _nfl_abbr_to_team_id()
        if "team_id" not in df.columns:
            df["team_id"] = ""
        if abbr_map and "team_abbr" in df.columns:
            df["team_id"] = df["team_abbr"].map(abbr_map).fillna(df["team_id"]).astype(str)
        ref_map = _PROPORACLE_ROOT / "data" / "reference" / "pp_to_espn_id_map_nfl.csv"
        if ref_map.is_file() and "espn_athlete_id" not in df.columns:
            try:
                ref = pd.read_csv(ref_map, dtype=str).fillna("")
                if "player_name" in ref.columns and "espn_athlete_id" in ref.columns:
                    pn = df.get("player_name", df.get("player", "")).astype(str).str.strip()
                    m = ref.drop_duplicates("player_name").set_index("player_name")["espn_athlete_id"]
                    df["espn_athlete_id"] = pn.map(m).fillna("")
            except Exception:
                pass
    if "team_id" not in df.columns:
        df["team_id"] = ""
    if "espn_athlete_id" not in df.columns:
        df["espn_athlete_id"] = ""

    # ── Flag 2nd-half props before any stat attachment ────────────────────────
    # ESPN boxscores are full-game only. Props with duration="2nd Half" need a
    # separate data source — attaching full-game stats produces wrong averages
    # and hit rates (~2x the actual 2H numbers). Mark UNSUPPORTED_2H so step6
    # excludes them from scoring entirely rather than using corrupted stats.
    if "duration" in df.columns:
        h2_mask = df["duration"].str.lower().str.contains("2nd|half", na=False)
        n_h2 = int(h2_mask.sum())
        if n_h2:
            print(f"  ⚠️  {n_h2} 2nd-half props tagged UNSUPPORTED_2H — full-game ESPN stats invalid for 2H lines")
            df_2h = df[h2_mask].copy()
            df_2h["stat_status"] = "UNSUPPORTED_2H"
            df = df[~h2_mask].copy()
        else:
            df_2h = pd.DataFrame()
    else:
        df_2h = pd.DataFrame()

    # use prop_norm if available, else prop_type
    if ESPN_LEAGUE == "nfl" and "prop_type_normalized" in df.columns:
        prop_col = "prop_type_normalized"
    else:
        prop_col = "prop_norm" if "prop_norm" in df.columns else "prop_type"

    # ── Bouncer (slate-level) ───────────────────────────────────────────────
    before = len(df)
    df = df[df.get("player", "").astype(str).str.strip() != ""].copy()
    if "pp_team" in df.columns:
        df = df[df["pp_team"].astype(str).str.strip() != ""].copy()
    df = df[df[prop_col].astype(str).str.strip() != ""].copy()
    # Line must be numeric & non-negative for hit-rate math
    df = df[df["line"].notna() & (df["line"] >= 0)].copy()
    bounced = before - len(df)
    if bounced:
        print(f"  🧹 Bouncer: removed {bounced} junk slate rows")
        log_pipeline_health(
            "cfb.step5b_attach_boxscore_stats",
            "bouncer_removed_slate_rows",
            extra={"removed": bounced, "before": before, "after": len(df)},
            start=Path(__file__),
        )

    slate_ids = {x for x in df["team_id"].astype(str).str.strip() if x and x != "nan"}
    print("→ Slate team_ids:", len(slate_ids))

    # If NO_MATCH rows have blank team_id but we know the team from team_abbr,
    # fetch all games rather than filtering — the team_id filter is an optimization
    # but it silently drops players whose ESPN ID wasn't found. If >10% of rows
    # have no team_id, disable the slate_ids filter entirely for safety.
    no_team_id = (df["team_id"].astype(str).str.strip().isin(["", "nan"])).sum()
    if no_team_id > 0:
        print(f"→ {no_team_id} rows have no team_id — fetching all {ESPN_LEAGUE} games (no team filter)")
        slate_ids = set()  # empty set = fetch all games

    # Build team_id -> team_abbr reverse map from slate for opp resolution in cache
    tid_to_abbr: dict = {}
    for _, r in df.iterrows():
        tid  = str(r.get("team_id", "")).strip()
        abbr = str(r.get("team_abbr", "")).strip()
        if tid and abbr and tid != "nan":
            tid_to_abbr[tid] = abbr

    end_d = dt.date.today()
    if str(args.date or "").strip():
        end_d = dt.datetime.strptime(str(args.date).strip()[:10], "%Y-%m-%d").date()

    # ── Parallelized fetch ────────────────────────────────────────────────────
    hist_aid, hist_name = build_player_histories(
        args.days,
        slate_ids,
        workers=args.workers,
        cache_path=cache_path,
        tid_to_abbr=tid_to_abbr,
        end_date=end_d,
    )
    if not hist_aid and not hist_name:
        log_pipeline_health(
            "cfb.step5b_attach_boxscore_stats",
            "no_histories_built",
            extra={"days": args.days, "workers": args.workers},
            start=Path(__file__),
        )

    out_rows, stat_status = [], []

    for _, row in _tqdm(df.iterrows(), total=len(df), desc="Attaching stats", unit="prop"):
        tid  = str(row.get("team_id",       "")).strip()
        pn   = str(row.get("player_norm",   "")).strip()
        aid  = str(row.get("espn_athlete_id","")).strip()
        prop = str(row.get(prop_col,        "")).strip()
        line = row.get("line", None)

        unsup = 0
        if "unsupported_prop" in row.index:
            try:
                unsup = int(float(row.get("unsupported_prop") or 0))
            except (TypeError, ValueError):
                unsup = 0
        if unsup == 1 or is_football_period_split_prop(prop):
            stat_status.append("UNSUPPORTED_PROP")
            out_rows.append({})
            continue

        # When team_id is missing, fall through to ID-only then name-only matching.
        # Handles CBB props where ESPN team_id was never resolved by step5a,
        # and also covers slates processed by attach_cbb_athlete_ids (espn_id present
        # but team_id blank).
        if not tid:
            games = []
            # Strategy A: espn_athlete_id across all teams (fastest, most accurate)
            if aid:
                for (t, a), g in hist_aid.items():
                    if a == aid:
                        games.extend(g)
            # Strategy B: player_norm across all teams (name-only fallback)
            if not games and pn:
                for (t, p), g in hist_name.items():
                    if p == pn:
                        games.extend(g)
            if not games:
                stat_status.append("NO_BOX_HISTORY"); out_rows.append({}); continue
        else:
            games = (hist_aid.get((tid, aid), []) if aid else []) or hist_name.get((tid, pn), [])
        if not games:
            stat_status.append("NO_BOX_HISTORY"); out_rows.append({}); continue

        played = [g for g in games if game_played_for_prop(g, prop)]
        raw_vals = []
        kept_games = []
        for g in played:
            v = prop_value(prop, g)
            if v is None:
                continue
            try:
                fv = float(v)
            except (TypeError, ValueError):
                continue
            if is_dnp_zero_actual(fv, g):
                continue
            raw_vals.append(fv)
            kept_games.append(g)

        if not raw_vals:
            stat_status.append("UNSUPPORTED_PROP")
            out_rows.append({})
            continue

        role = str(row.get("position_group") or row.get("pos") or row.get("role") or "")
        vals, attach_st = decide_attach(
            raw_vals, prop, line, role=role, min_games=5, games=kept_games
        )
        if vals is None or attach_st != "OK":
            extra = {"games_used": len(raw_vals)} if raw_vals else {}
            stat_status.append(attach_st)
            out_rows.append(extra)
            continue
        played = kept_games[: len(vals)]

        # vals is now newest-first (already sorted from dedup_sort)

        # FIX: preserve full season history BEFORE truncating to --n games
        season_vals = vals[:]           # full window
        game_log    = vals[:args.n]     # truncated for g1..gN columns
        last5       = game_log[:5]
        last10      = game_log[:10]

        o = {"games_used": len(season_vals)}   # true season game count
        for k in range(1, args.n + 1):
            o[f"stat_g{k}"] = game_log[k-1] if k-1 < len(game_log) else ""

        o["stat_last5_avg"]  = round(sum(last5)       / len(last5),       3) if last5       else ""
        o["stat_last10_avg"] = round(sum(last10)      / len(last10),      3) if last10      else ""
        o["stat_season_avg"] = round(sum(season_vals) / len(season_vals), 3) if season_vals else ""

        # minutes averages (CFB: participation flag only)
        min_vals = [float(g.get("MIN", 1) or 1) for g in played]
        min5 = min_vals[:5]
        o["min_last5_avg"]   = round(sum(min5)    / len(min5),    1) if min5    else ""
        o["min_season_avg"]  = round(sum(min_vals) / len(min_vals), 1) if min_vals else ""

        # hit rates vs line
        if pd.notna(line):
            ln = float(line)

            over5, under5, push5, hr_ov5, hr_un5, hr_ov_ou5, hr_un_ou5 = hit_rates(game_log, ln, 5)
            o["line_hits_over_5"]         = over5
            o["line_hits_under_5"]        = under5
            o["line_hits_push_5"]         = push5
            o["line_hit_rate_over_5"]     = round(hr_ov5,    3) if hr_ov5    is not None else ""
            o["line_hit_rate_under_5"]    = round(hr_un5,    3) if hr_un5    is not None else ""
            o["line_hit_rate_over_ou_5"]  = round(hr_ov_ou5, 3) if hr_ov_ou5 is not None else ""
            o["line_hit_rate_under_ou_5"] = round(hr_un_ou5, 3) if hr_un_ou5 is not None else ""

            over10, under10, push10, hr_ov10, hr_un10, hr_ov_ou10, hr_un_ou10 = hit_rates(game_log, ln, 10)
            o["line_hits_over_10"]         = over10
            o["line_hits_under_10"]        = under10
            o["line_hits_push_10"]         = push10
            o["line_hit_rate_over_10"]     = round(hr_ov10,    3) if hr_ov10    is not None else ""
            o["line_hit_rate_under_10"]    = round(hr_un10,    3) if hr_un10    is not None else ""
            o["line_hit_rate_over_ou_10"]  = round(hr_ov_ou10, 3) if hr_ov_ou10 is not None else ""
            o["line_hit_rate_under_ou_10"] = round(hr_un_ou10, 3) if hr_un_ou10 is not None else ""

            o["model_dir_5"] = "OVER" if over5 >= under5 else "UNDER"

        stat_status.append("OK")
        out_rows.append(o)

    stats_df = pd.DataFrame(out_rows).fillna("")
    df["stat_status"] = stat_status
    out = pd.concat([df.reset_index(drop=True), stats_df], axis=1)

    # Re-attach the 2H rows (they carry UNSUPPORTED_2H status, no stat columns)
    if not df_2h.empty:
        out = pd.concat([out, df_2h], ignore_index=True, sort=False).fillna("")

    out.to_csv(args.output, index=False)

    print(f"✅ Saved → {args.output} | rows={len(out)}")
    print("stat_status breakdown:")
    print(out["stat_status"].value_counts().to_string())




if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_pipeline_health(
            "cfb.step5b_attach_boxscore_stats",
            "run_failed",
            extra={"error": f"{type(e).__name__}: {e}"},
            start=Path(__file__),
        )
        print(f"❌ CBB step5b failed (logged). {type(e).__name__}: {e}")
