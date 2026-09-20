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
import importlib.util
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
from scripts.espn_boxscore_cache import load_boxscore_cache, save_boxscore_cache, sport_key_from_espn_league
from utils.nfl_unsupported_props import is_football_period_split_prop
from utils.combo_actuals import split_combo_players, strip_combo_prop_token

_PIPELINE_DIR = Path(__file__).resolve().parent
if str(_PIPELINE_DIR) not in sys.path:
    sys.path.insert(0, str(_PIPELINE_DIR))
from cfb_1h_pbp import parse_1h_players

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


def multiplayer_combo_arm_names(player: object) -> list[str]:
    """PrizePicks ``A + B`` labels → individual arm names (empty if not a duo)."""
    parts = split_combo_players(player)
    return parts if len(parts) >= 2 else []


def base_prop_for_combo(prop: object) -> str:
    """Strip trailing ``(Combo)`` / ``_combo`` so prop_value hits solo box columns."""
    raw = str(prop or "").strip()
    if not raw:
        return ""
    stripped = strip_combo_prop_token(raw)
    return stripped or raw


def sum_multiplayer_combo_game_vals(
    arm_val_series: list[list[float]],
) -> list[float] | None:
    """Pair each arm's newest-first game series by index and sum (cross-team L5)."""
    if len(arm_val_series) < 2:
        return None
    if any(not s for s in arm_val_series):
        return None
    n = min(len(s) for s in arm_val_series)
    if n <= 0:
        return None
    return [float(sum(s[i] for s in arm_val_series)) for i in range(n)]


# PrizePicks often uses the legal first name; ESPN boxscores use the nickname.
_NFL_FIRST_ALIASES = {
    "cameron": ("cam",),
    "cam": ("cameron",),
    "nicholas": ("nick",),
    "nick": ("nicholas",),
    "drew": ("andrew",),
    "andrew": ("drew",),
    "joshua": ("josh",),
    "josh": ("joshua",),
    "matthew": ("matt",),
    "matt": ("matthew",),
    "michael": ("mike",),
    "mike": ("michael",),
    "christopher": ("chris",),
    "chris": ("christopher",),
    "william": ("will",),
    "will": ("william",),
    "joseph": ("joe",),
    "joe": ("joseph",),
    "benjamin": ("ben",),
    "ben": ("benjamin",),
    "samuel": ("sam",),
    "sam": ("samuel",),
    "alexander": ("alex",),
    "alex": ("alexander",),
    "anthony": ("tony",),
    "tony": ("anthony",),
}


def _parse_box_date(value) -> Optional[dt.date]:
    """Parse cache game_date values stored as YYYY-MM-DD or YYYYMMDD."""
    s = str(value or "").strip()
    if not s or s.lower() in ("nan", "none"):
        return None
    if len(s) == 8 and s.isdigit():
        try:
            return dt.date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        except ValueError:
            return None
    try:
        return dt.date.fromisoformat(s[:10])
    except ValueError:
        return None


def _cfb_season_start(slate: dt.date) -> dt.date:
    """Start of the fall CFB season that contains ``slate``.

    Aug–Dec → Aug 1 of slate.year. Jan–Jul (bowls / offseason) → Aug 1 of
    the prior calendar year. Flags / games_used / l5_sample_n stop here.
    Face L5/L10 may include this player's prior-season boxes only in weeks 0–3.
    """
    if slate.month >= 8:
        return dt.date(slate.year, 8, 1)
    return dt.date(slate.year - 1, 8, 1)


def _split_cfb_by_season(
    games: List[dict],
    slate: dt.date,
) -> tuple[List[dict], List[dict]]:
    """Split newest-first box rows into current-season vs prior (Aug 1)."""
    start = _cfb_season_start(slate)
    current: List[dict] = []
    prior: List[dict] = []
    for g in games:
        d = _parse_box_date(g.get("game_date"))
        if d is None:
            continue
        if d >= start:
            current.append(g)
        else:
            prior.append(g)
    return current, prior


def _cfb_week0_saturday(season_start: dt.date) -> dt.date:
    """First Saturday on or after Aug 23 (typical CFB Week 0)."""
    d = dt.date(season_start.year, 8, 23)
    return d + dt.timedelta(days=(5 - d.weekday()) % 7)


def _cfb_allow_prior_season_fallback(slate: dt.date) -> bool:
    """Weeks 0–3 of the fall season, through Sunday of Week 3.

    The Jones-fix fill window was "first ~3 weeks," not Aug 1 + 21 days
    (that cutoff is ~Aug 22, Week 0 only, and dropped Week 3). Week 0
    Saturday + 22 days is Sunday of Week 3 (2026-09-20). Week 4 is outside.
    """
    start = _cfb_season_start(slate)
    week3_sunday = _cfb_week0_saturday(start) + dt.timedelta(days=22)
    return slate <= week3_sunday


def _cfb_face_games(
    current: List[dict],
    prior: List[dict],
    *,
    allow_fill: bool,
) -> List[dict]:
    """This player's boxes only: current season first, then prior if filling.

    Never a position/team average — callers must pass one athlete's games.
    After Week 3, current only.
    """
    if allow_fill and len(current) < 5 and prior:
        return list(current) + list(prior)
    return list(current)


def _cfb_face_vals(
    current_vals: List[float],
    prior_vals: List[float],
    slate: dt.date,
) -> tuple[List[float], str]:
    """Return (face_vals, season_l5_flag) for this player's own series.

    PRIOR_SEASON_FILL — current games exist; last year mixed into L5/L10.
    PRIOR_SEASON_L5  — no current games; window is last year only (weeks 0–3).
    NO_CURRENT_SEASON — no current games and fill window closed / no prior.
    ''               — current-only face (full sample or post-window thin).
    """
    cur = list(current_vals)
    pri = list(prior_vals)
    allow = _cfb_allow_prior_season_fallback(slate)
    if not cur:
        if allow and pri:
            return pri, "PRIOR_SEASON_L5"
        return [], "NO_CURRENT_SEASON"
    if allow and len(cur) < 5 and pri:
        return cur + pri, "PRIOR_SEASON_FILL"
    return cur, ""


def _cfb_season_flag(current: List[dict], prior: List[dict]) -> str:
    if current:
        return ""
    if prior:
        return "PRIOR_SEASON_L5"
    return "NO_CURRENT_SEASON"


def _cfb_thin_sample_flag(
    *,
    n: int,
    overs: int,
    unders: int,
    pushes: int = 0,
) -> str:
    """Split early-season thin samples into risk profiles.

    Strict L5=5 / L5>=4 gates still fail on all of these (n < 5). The label
    only separates:

      THIN_CLEAR_OVER  — every decisive current-season game clears OVER
      THIN_CLEAR_UNDER — every decisive current-season game clears UNDER
      THIN_MIXED       — sample exists but sides disagree (Jones-type risk:
                         average can look clean while the window is split)
      THIN_SEASON      — thin with no usable line / no decisive games

    CLEAR ≠ ticket-eligible. It means "directionally consistent on the real
    games that exist," not "full-confidence L5."
    """
    if n >= 5:
        return ""
    decisive = int(overs) + int(unders)
    if decisive <= 0:
        return "THIN_SEASON"
    if int(overs) > 0 and int(unders) == 0:
        return "THIN_CLEAR_OVER"
    if int(unders) > 0 and int(overs) == 0:
        return "THIN_CLEAR_UNDER"
    return "THIN_MIXED"


def _filter_cfb_current_season(
    games: List[dict],
    *,
    slate: dt.date,
    allow_prior_fallback: bool = False,
) -> tuple[List[dict], str]:
    """Current-season slice for flags / games_used (not the L5 face window).

    Returns (games, flag) where flag is:
      ''                 — ≥1 current-season game (may still be thin)
      'PRIOR_SEASON_L5'  — no current-season games; prior kept in weeks 0–3
      'NO_CURRENT_SEASON'— no current-season games and fill window closed

    Attach uses ``_cfb_face_vals`` so this player's last-year boxes fill
    L5/L10 in weeks 0–3, tagged PRIOR_SEASON_FILL (mixed) or PRIOR_SEASON_L5
    (prior-only). games_used / l5_sample_n stay current-season counts.
    """
    current, prior = _split_cfb_by_season(games, slate)
    if current:
        return current, ""
    if allow_prior_fallback and prior and _cfb_allow_prior_season_fallback(slate):
        return prior, "PRIOR_SEASON_L5"
    return [], "NO_CURRENT_SEASON"


def _nfl_season_start(slate: dt.date) -> dt.date:
    """Start of the NFL regular season that contains ``slate``.

    Sep–Dec → Sep 1 of slate.year. Jan–Feb (playoffs) → Sep 1 of the prior
    calendar year. Mar–Aug (offseason / NFLP) → Sep 1 of the prior year so
    NFLP L5 still reads last year's regular-season logs, not August preseason.
    """
    if slate.month >= 9:
        return dt.date(slate.year, 9, 1)
    if slate.month <= 2:
        return dt.date(slate.year - 1, 9, 1)
    return dt.date(slate.year - 1, 9, 1)


def _nfl_week1_thursday(season_start: dt.date) -> dt.date:
    """First Thursday on or after Sep 4 (typical NFL Week 1 openers)."""
    d = dt.date(season_start.year, 9, 4)
    return d + dt.timedelta(days=(3 - d.weekday()) % 7)


def _nfl_allow_prior_season_fallback(slate: dt.date) -> bool:
    """Weeks 1–3 of the NFL regular season: allow prior boxes to fill thin L5.

    Mar–Aug already treats prior Sep 1 as season start (NFLP) — no pad needed.
    Oct+ / playoffs stay current-only. Through Sunday of Week 3 (Week 1 Thu + 17d).

    Face L5/L10 may mix last year (PRIOR_SEASON_FILL); ``l5_sample_n`` /
    ``games_used`` stay current-season counts. Ticket Gate70 still requires
    current-season n>=5 (see ``utils.nfl_keep_gates.nfl_current_season_l5_ok``).
    """
    if 3 <= slate.month <= 8:
        return False
    start = _nfl_season_start(slate)
    week3_sunday = _nfl_week1_thursday(start) + dt.timedelta(days=17)
    return slate <= week3_sunday


def _split_nfl_by_season(
    games: List[dict],
    slate: dt.date,
) -> tuple[List[dict], List[dict]]:
    """Split newest-first box rows into current-season vs prior (Sep 1)."""
    start = _nfl_season_start(slate)
    current: List[dict] = []
    prior: List[dict] = []
    for g in games:
        d = _parse_box_date(g.get("game_date"))
        if d is None:
            continue
        if d >= start:
            current.append(g)
        else:
            prior.append(g)
    return current, prior


def _nfl_face_vals(
    current_vals: List[float],
    prior_vals: List[float],
    slate: dt.date,
) -> tuple[List[float], str]:
    """Same contract as ``_cfb_face_vals`` for NFL weeks 1–3."""
    cur = list(current_vals)
    pri = list(prior_vals)
    allow = _nfl_allow_prior_season_fallback(slate)
    if not cur:
        if allow and pri:
            return pri, "PRIOR_SEASON_L5"
        return [], "NO_CURRENT_SEASON"
    if allow and len(cur) < 5 and pri:
        return cur + pri, "PRIOR_SEASON_FILL"
    return cur, ""


def _filter_nfl_current_season(
    games: List[dict],
    *,
    slate: dt.date,
    allow_prior_fallback: bool = False,
) -> tuple[List[dict], str]:
    """Keep newest-first games from the current NFL season only.

    Same flag contract as ``_filter_cfb_current_season``. When
    ``allow_prior_fallback`` and weeks 1–3, prior-only rows return as
    PRIOR_SEASON_L5. Prefer ``_nfl_face_vals`` on the attach path for mixed fill.
    """
    current, prior = _split_nfl_by_season(games, slate)
    if current:
        return current, ""
    if allow_prior_fallback and prior and _nfl_allow_prior_season_fallback(slate):
        return prior, "PRIOR_SEASON_L5"
    return [], "NO_CURRENT_SEASON"


def _is_nfl_regular_season(d: dt.date) -> bool:
    """NFL regular season + playoffs (Sep–Feb). August is preseason — never L5."""
    return d.month >= 9 or d.month <= 2


def _nfl_name_keys(pn: str) -> set[str]:
    """Name keys for NFL matching: nickname aliases + collapsed apostrophe names."""
    p = norm(pn)
    if not p:
        return set()
    keys = {p, p.replace(" ", "")}
    parts = p.split()
    if not parts:
        return keys
    first, rest = parts[0], parts[1:]
    for alt in _NFL_FIRST_ALIASES.get(first, ()):
        keys.add(" ".join([alt, *rest]))
        keys.add("".join([alt, *rest]))
    if first == "wandale":
        keys.add(" ".join(["wan", "dale", *rest]))
    if first == "wan" and rest and rest[0] == "dale":
        keys.add(" ".join(["wandale", *rest[1:]]))
        keys.add("wandale" + "".join(rest[1:]))
    return {k for k in keys if k}


def _lookup_nfl_box_games(
    pn: str,
    aid: str,
    hist_aid: Dict[Tuple[str, str], List[dict]],
    hist_name: Dict[Tuple[str, str], List[dict]],
) -> List[dict]:
    """NFL/NFLP L5 is prior NFL boxscores for the player, regardless of current team."""
    games: List[dict] = []
    if aid:
        for (_t, a), g in hist_aid.items():
            if a == aid:
                games.extend(g)
        if games:
            return games
    keys = _nfl_name_keys(pn)
    collapsed = {k.replace(" ", "") for k in keys}
    for (_t, p), g in hist_name.items():
        if p in keys or p.replace(" ", "") in collapsed:
            games.extend(g)
    return games


def _nfl_regular_season_games(games: List[dict]) -> List[dict]:
    """Newest-first NFL regular-season/playoff games; drops August preseason."""
    out: List[dict] = []
    seen: set[str] = set()
    for g in games:
        d = _parse_box_date(g.get("game_date"))
        if d is None or not _is_nfl_regular_season(d):
            continue
        eid = str(g.get("event_id") or "").strip()
        if eid and eid in seen:
            continue
        if eid:
            seen.add(eid)
        row = dict(g)
        row["_dt"] = d
        out.append(row)
    out.sort(key=lambda r: r.get("_dt") or dt.date.min, reverse=True)
    return out


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
    dates = [(end_date - dt.timedelta(days=i)).strftime("%Y%m%d") for i in range(days_back + 1)]
    if ESPN_LEAGUE == "nfl":
        # NFLP and NFL share the same boxscores. Skip Mar–Aug so L5 cannot
        # pick up last year's (or this year's) preseason games.
        dates = [d for d in dates if d[4:6] not in ("03", "04", "05", "06", "07", "08")]
    return dates


def pull_scoreboard(d: str) -> dict:
    params = {"dates": d, "limit": "500"}
    if ESPN_LEAGUE == "college-football":
        params["groups"] = "80"  # FBS
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


_PASS_COMPLETE_TYPES = frozenset({"pass reception", "passing touchdown"})
_RUSH_TYPES = frozenset({"rush", "rushing touchdown", "run"})
_PASSER_COMPLETE_RE = re.compile(
    r"#(\d+)\s+(?:[A-Z]\.\s*)?([A-Za-z'\-]+(?:\s+(?:Jr|Sr|II|III|IV)\.)?)\s+pass complete",
    re.I,
)
_RUSHER_RE = re.compile(
    r"#(\d+)\s+(?:[A-Z]\.\s*)?([A-Za-z'\-]+(?:\s+(?:Jr|Sr|II|III|IV)\.)?)\s+(?:run|rush)",
    re.I,
)
_TARGET_RE = re.compile(
    r"(?:to|intended for)\s+#(\d+)\s+(?:([A-Z])\.)?\s*([A-Za-z'\-]+)?",
    re.I,
)
_TARGET_FULL_RE = re.compile(
    r"(?i:to|intended for)\s+([A-Z][a-zA-Z'\-]+(?:\s+[A-Z][a-zA-Z'\-]+)+)",
)
_OPTIONAL_ABSENT = (
    "PASS_LONG", "RUSH_LONG", "REC_LONG", "REC_TGT", "RUSH_ATT", "PASS_ATT",
)


def _norm_jersey(v) -> str:
    s = str(v or "").strip()
    if not s or s.lower() in ("nan", "none"):
        return ""
    s = re.sub(r"^0+", "", s)
    return s or "0"


def parse_pass_long_from_drives(summary: dict) -> dict[tuple[str, str], float]:
    """Longest completion per (offense team_id, jersey) from ESPN drive PBP.

    CFB passing boxscore has no LONG. The same summary payload includes
    drives.previous[].plays[] with type Pass Reception / Passing Touchdown
    and statYardage. Match passers by jersey (#9 S.Locklear).
    """
    drives = summary.get("drives") or {}
    items: List[dict] = []
    if isinstance(drives, dict):
        prev = drives.get("previous") or []
        if isinstance(prev, list):
            items.extend(x for x in prev if isinstance(x, dict))
        cur = drives.get("current")
        if isinstance(cur, dict):
            items.append(cur)
        elif isinstance(cur, list):
            items.extend(x for x in cur if isinstance(x, dict))
    longs: dict[tuple[str, str], float] = {}
    for dr in items:
        for p in dr.get("plays") or []:
            if not isinstance(p, dict) or p.get("isPenalty"):
                continue
            typ = str((p.get("type") or {}).get("text") or "").strip().lower()
            if typ not in _PASS_COMPLETE_TYPES:
                continue
            m = _PASSER_COMPLETE_RE.search(str(p.get("text") or ""))
            if not m:
                continue
            jersey = _norm_jersey(m.group(1))
            if not jersey:
                continue
            try:
                raw = p.get("statYardage")
                yds = float(raw) if raw is not None and raw != "" else 0.0
            except (TypeError, ValueError):
                continue
            off = ""
            for tp in p.get("teamParticipants") or []:
                if str(tp.get("type") or "").strip().lower() == "offense":
                    off = str(tp.get("id") or "").strip()
                    break
            key = (off, jersey)
            prev_yds = longs.get(key)
            if prev_yds is None or yds > prev_yds:
                longs[key] = yds
    return longs


def parse_rec_long_from_drives(summary: dict) -> dict[tuple[str, str], float]:
    """Longest reception per (offense team_id, receiver jersey) from drive PBP."""
    drives = summary.get("drives") or {}
    items: List[dict] = []
    if isinstance(drives, dict):
        prev = drives.get("previous") or []
        if isinstance(prev, list):
            items.extend(x for x in prev if isinstance(x, dict))
        cur = drives.get("current")
        if isinstance(cur, dict):
            items.append(cur)
        elif isinstance(cur, list):
            items.extend(x for x in cur if isinstance(x, dict))
    longs: dict[tuple[str, str], float] = {}
    for dr in items:
        for p in dr.get("plays") or []:
            if not isinstance(p, dict) or p.get("isPenalty"):
                continue
            typ = str((p.get("type") or {}).get("text") or "").strip().lower()
            if typ not in _PASS_COMPLETE_TYPES:
                continue
            text = str(p.get("text") or "")
            m = _TARGET_RE.search(text)
            if not m:
                continue
            jersey = _norm_jersey(m.group(1))
            if not jersey:
                continue
            try:
                raw = p.get("statYardage")
                yds = float(raw) if raw is not None and raw != "" else 0.0
            except (TypeError, ValueError):
                continue
            off = ""
            for tp in p.get("teamParticipants") or []:
                if str(tp.get("type") or "").strip().lower() == "offense":
                    off = str(tp.get("id") or "").strip()
                    break
            key = (off, jersey)
            prev_yds = longs.get(key)
            if prev_yds is None or yds > prev_yds:
                longs[key] = yds
    return longs


def parse_rush_long_from_drives(summary: dict) -> dict[tuple[str, str], float]:
    """Longest rush per (offense team_id, rusher jersey) from drive PBP."""
    drives = summary.get("drives") or {}
    items: List[dict] = []
    if isinstance(drives, dict):
        prev = drives.get("previous") or []
        if isinstance(prev, list):
            items.extend(x for x in prev if isinstance(x, dict))
        cur = drives.get("current")
        if isinstance(cur, dict):
            items.append(cur)
        elif isinstance(cur, list):
            items.extend(x for x in cur if isinstance(x, dict))
    longs: dict[tuple[str, str], float] = {}
    for dr in items:
        for p in dr.get("plays") or []:
            if not isinstance(p, dict) or p.get("isPenalty"):
                continue
            typ = str((p.get("type") or {}).get("text") or "").strip().lower()
            text = str(p.get("text") or "")
            text_l = text.lower()
            if typ not in _RUSH_TYPES and " run for " not in f" {text_l} " and " rush for " not in f" {text_l} ":
                if "run for" not in text_l and "rush for" not in text_l:
                    continue
            if "kneel" in text_l or "sack" in text_l:
                continue
            m = _RUSHER_RE.search(text)
            if not m:
                continue
            jersey = _norm_jersey(m.group(1))
            if not jersey:
                continue
            try:
                raw = p.get("statYardage")
                yds = float(raw) if raw is not None and raw != "" else 0.0
            except (TypeError, ValueError):
                continue
            off = ""
            for tp in p.get("teamParticipants") or []:
                if str(tp.get("type") or "").strip().lower() == "offense":
                    off = str(tp.get("id") or "").strip()
                    break
            key = (off, jersey)
            prev_yds = longs.get(key)
            if prev_yds is None or yds > prev_yds:
                longs[key] = yds
    return longs


def _load_player_volume_mod():
    path = Path(__file__).resolve().parent / "build_cfb_player_volume.py"
    spec = importlib.util.spec_from_file_location("cfb_player_volume", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def parse_rec_target_plays(summary: dict) -> List[Tuple[str, str, str, str]]:
    """PBP target plays as (offense team_id, jersey, last_name, full_norm)."""
    drives = summary.get("drives") or {}
    items: List[dict] = []
    if isinstance(drives, dict):
        prev = drives.get("previous") or []
        if isinstance(prev, list):
            items.extend(x for x in prev if isinstance(x, dict))
        cur = drives.get("current")
        if isinstance(cur, dict):
            items.append(cur)
        elif isinstance(cur, list):
            items.extend(x for x in cur if isinstance(x, dict))
    plays: List[Tuple[str, str, str, str]] = []
    for dr in items:
        for p in dr.get("plays") or []:
            if not isinstance(p, dict) or p.get("isPenalty"):
                continue
            text = str(p.get("text") or "")
            text_l = text.lower()
            if "pass" not in text_l or "sack" in text_l:
                continue
            m = _TARGET_RE.search(text)
            jersey = last = full = ""
            if m:
                jersey = _norm_jersey(m.group(1))
                last = str(m.group(3) or "").strip().lower().strip(".,")
            else:
                fm = _TARGET_FULL_RE.search(text)
                if not fm:
                    continue
                full_raw = str(fm.group(1) or "").strip()
                full = norm(full_raw)
                parts = full_raw.replace(".", " ").split()
                last = parts[-1].lower().strip(".,") if parts else ""
            off = ""
            for tp in p.get("teamParticipants") or []:
                if str(tp.get("type") or "").strip().lower() == "offense":
                    off = str(tp.get("id") or "").strip()
                    break
            plays.append((off, jersey, last, full))
    return plays


def parse_rec_targets_from_drives(summary: dict) -> dict[tuple[str, str], int]:
    """Reception targets per (offense team_id, jersey) from ESPN drive PBP."""
    counts: dict[tuple[str, str], int] = {}
    for off, jersey, _last, _full in parse_rec_target_plays(summary):
        if not jersey:
            continue
        key = (off, jersey)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _cmp_att(val) -> Tuple[Optional[float], Optional[float]]:
    """Split ESPN ``C/ATT`` (e.g. ``22/39``) into completions and attempts."""
    s = str(val).strip()
    if s in ("", "--", "nan", "None"):
        return None, None
    if "/" in s:
        a, b = s.split("/", 1)

        def _one(x: str) -> Optional[float]:
            t = str(x).strip()
            if t in ("", "--", "nan", "None"):
                return None
            try:
                return float(t)
            except (TypeError, ValueError):
                return None

        return _one(a), _one(b)
    try:
        return float(s), None
    except (TypeError, ValueError):
        return None, None


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


def parse_players(summary: dict, game_date: str = "", event_id: str = "") -> List[dict]:
    """Parse CFB boxscore rows (passing / rushing / receiving)."""
    if not game_date:
        hdr = summary.get("header", {}) or {}
        comps = hdr.get("competitions", [{}])
        game_date = str(comps[0].get("date", "") if comps else "")[:10].replace("-", "")

    box = summary.get("boxscore", {}) or {}
    blocks = box.get("players", []) or []
    by_ath: Dict[str, dict] = {}
    passing_slots: List[Tuple[str, str, str]] = []  # (row_key, team_id, jersey)
    jersey_keys: Dict[Tuple[str, str], List[str]] = {}  # (team_id, jersey) -> row keys
    last_keys: Dict[Tuple[str, str], List[str]] = {}  # (team_id, last_name) -> row keys
    name_keys: Dict[Tuple[str, str], List[str]] = {}  # (team_id, player_norm) -> row keys

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
                    },
                )
                jer = _norm_jersey(ath.get("jersey"))
                if jer and not row.get("jersey"):
                    row["jersey"] = jer
                    jersey_keys.setdefault((team_id, jer), []).append(key)
                ln = str(ath.get("lastName") or "").strip()
                if not ln:
                    parts = str(ath.get("displayName") or ath.get("fullName") or "").replace(".", " ").split()
                    ln = parts[-1] if parts else ""
                ln_n = ln.lower().strip(".,")
                if ln_n and not row.get("last_name"):
                    row["last_name"] = ln_n
                    last_keys.setdefault((team_id, ln_n), []).append(key)
                if pn:
                    nk = name_keys.setdefault((team_id, pn), [])
                    if key not in nk:
                        nk.append(key)
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
                            row["PASS_ATT"] = _f(st[att_i])
                    if int_i is not None and int_i < len(st):
                        row["PASS_INT"] = _f(st[int_i])
                    if lng is not None and lng < len(st):
                        row["PASS_LONG"] = _f(st[lng])
                    passing_slots.append((key, team_id, jer))
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
                        row["RUSH_ATT"] = _f(st[car])
                    if lng is not None and lng < len(st):
                        row["RUSH_LONG"] = _f(st[lng])
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
                    if tgt is None:
                        tgt = idx("TARGETS")
                    if rec is not None and rec < len(st):
                        row["REC"] = _f(st[rec])
                    if y is not None and y < len(st):
                        row["REC_YDS"] = _f(st[y])
                    if td is not None and td < len(st):
                        row["REC_TD"] = _f(st[td])
                    if lng is not None and lng < len(st):
                        row["REC_LONG"] = _f(st[lng])
                    if tgt is not None and tgt < len(st):
                        row["REC_TGT"] = _f(st[tgt])
                elif cat == "defensive":
                    tot = idx("TOT")
                    sack = idx("SACK")
                    if sack is None:
                        sack = idx("SACKS")
                    dint = idx("INT")
                    tfl = idx("TFL")
                    if tot is not None and tot < len(st):
                        row["TACK_TOT"] = _f(st[tot])
                    if sack is not None and sack < len(st):
                        row["SACK"] = _f(st[sack])
                    if dint is not None and dint < len(st):
                        row["DEF_INT"] = _f(st[dint])
                    if tfl is not None and tfl < len(st):
                        row["TFL"] = _f(st[tfl])
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
                        row["KICK_FG"] = _f(st[fg])
                    if xp is not None and xp < len(st):
                        row["KICK_XP"] = _f(st[xp])

    pbp_longs = parse_pass_long_from_drives(summary)
    if pbp_longs:
        jersey_n: Dict[str, int] = {}
        for _, _, jer in passing_slots:
            if jer:
                jersey_n[jer] = jersey_n.get(jer, 0) + 1
        for key, tid, jer in passing_slots:
            row = by_ath.get(key)
            if not row:
                continue
            if row.get("PASS_LONG") not in (None, ""):
                continue
            val = pbp_longs.get((tid, jer)) if jer else None
            if val is None and jer and jersey_n.get(jer) == 1:
                val = pbp_longs.get(("", jer))
            if val is not None:
                row["PASS_LONG"] = val
            else:
                row["PASS_LONG"] = 0.0

    pbp_rec_longs = parse_rec_long_from_drives(summary)
    if pbp_rec_longs:
        rec_jersey_n: Dict[str, int] = {}
        for key, row in by_ath.items():
            jer = _norm_jersey(row.get("jersey"))
            if jer:
                rec_jersey_n[jer] = rec_jersey_n.get(jer, 0) + 1
        for key, row in by_ath.items():
            if row.get("REC_LONG") not in (None, ""):
                continue
            jer = _norm_jersey(row.get("jersey"))
            tid = str(row.get("team_id") or "").strip()
            val = pbp_rec_longs.get((tid, jer)) if jer else None
            if val is None and jer and rec_jersey_n.get(jer) == 1:
                val = pbp_rec_longs.get(("", jer))
            if val is not None:
                row["REC_LONG"] = val

    pbp_rush_longs = parse_rush_long_from_drives(summary)
    if pbp_rush_longs:
        rush_jersey_n: Dict[str, int] = {}
        for key, row in by_ath.items():
            jer = _norm_jersey(row.get("jersey"))
            if jer:
                rush_jersey_n[jer] = rush_jersey_n.get(jer, 0) + 1
        for key, row in by_ath.items():
            if row.get("RUSH_LONG") not in (None, ""):
                continue
            jer = _norm_jersey(row.get("jersey"))
            tid = str(row.get("team_id") or "").strip()
            val = pbp_rush_longs.get((tid, jer)) if jer else None
            if val is None and jer and rush_jersey_n.get(jer) == 1:
                val = pbp_rush_longs.get(("", jer))
            if val is not None:
                row["RUSH_LONG"] = val

    pbp_plays = parse_rec_target_plays(summary)
    if pbp_plays:
        tgt_n: Dict[str, int] = {}
        for tid, jer, last, full in pbp_plays:
            keys = jersey_keys.get((tid, jer)) or [] if jer else []
            if len(keys) != 1 and full:
                keys = name_keys.get((tid, full)) or []
            if len(keys) != 1 and last:
                keys = last_keys.get((tid, last)) or []
            if len(keys) != 1:
                continue
            tgt_n[keys[0]] = tgt_n.get(keys[0], 0) + 1
        for key, count in tgt_n.items():
            row = by_ath.get(key)
            if not row:
                continue
            if row.get("REC_TGT") not in (None, ""):
                continue
            row["REC_TGT"] = float(count)

    _stat_keys = (
        "PASS_YDS", "RUSH_YDS", "REC_YDS", "REC", "PASS_TD", "RUSH_TD", "REC_TD",
        "PASS_CMP", "PASS_ATT", "PASS_INT", "SACK", "TACK_TOT", "KICK_PTS", "KICK_FG", "KICK_XP", "DEF_INT",
        "PASS_LONG", "RUSH_LONG", "REC_LONG", "REC_TGT", "RUSH_ATT", "TFL",
    )
    # Do not zero-fill LONG / TGTS / ATT: missing means ESPN did not publish
    # the stat (or this cache row predates the parser). Zero would fake L5 unders.
    _zero_fill = tuple(k for k in _stat_keys if k not in _OPTIONAL_ABSENT)
    rows: List[dict] = []
    for row in by_ath.values():
        if not any(row.get(k) for k in _stat_keys):
            continue
        for k in _zero_fill:
            row.setdefault(k, 0.0)
        # CFB boxscores have no MIN column; mark participation for rolling-window filters.
        # Volume: RUSH_ATT from CAR, PASS_ATT from C/ATT, REC_TGT from drive PBP
        # (to #N / intended for #N). Snap % still CANNOT-OBTAIN from ESPN CFB.
        # Optional: CFBD /player/usage → data/cache/cfb_usage_cache.json via
        # build_cfb_usage_cache.py when CFBD_API_KEY is set (usage share → minutes_tier).
        # Do not invent minutes_tier from MIN=1.
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
            "PASS_CMP", "PASS_ATT", "PASS_INT", "SACK", "TACK_TOT", "KICK_PTS", "KICK_FG", "KICK_XP",
            "RUSH_LONG", "REC_LONG", "PASS_LONG", "REC_TGT", "RUSH_ATT", "TFL",
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
        "pass_completions": r.get("PASS_CMP"),
        "pass_cmp": r.get("PASS_CMP"),
        "completions": r.get("PASS_CMP"),
        "pass_att": r.get("PASS_ATT"),
        "pass_attempts": r.get("PASS_ATT"),
        "rush_att": r.get("RUSH_ATT"),
        "rush_attempts": r.get("RUSH_ATT"),
        "interceptions_thrown": r.get("PASS_INT"),
        "interceptions": r.get("PASS_INT") or r.get("DEF_INT"),
        "sacks": r.get("SACK"),
        "tackles": r.get("TACK_TOT"),
        "tackles_assists": r.get("TACK_TOT"),
        "tfl": r.get("TFL"),
        "kicking_points": r.get("KICK_PTS"),
        "kick_pts": r.get("KICK_PTS"),
        "pat_made": r.get("KICK_XP"),
        "fg_made": r.get("KICK_FG"),
        "extra_points": r.get("KICK_XP"),
        "field_goals": r.get("KICK_FG"),
        "player_td": _stat_num(r, "PASS_TD") + _stat_num(r, "RUSH_TD") + _stat_num(r, "REC_TD"),
        "player_tds": _stat_num(r, "PASS_TD") + _stat_num(r, "RUSH_TD") + _stat_num(r, "REC_TD"),
        "player_touchdowns": _stat_num(r, "PASS_TD") + _stat_num(r, "RUSH_TD") + _stat_num(r, "REC_TD"),
        "pass_rush_yds": _stat_num(r, "PASS_YDS") + _stat_num(r, "RUSH_YDS"),
        "rush_rec_yds": _stat_num(r, "RUSH_YDS") + _stat_num(r, "REC_YDS"),
        "pass_rush_rec_td": _stat_num(r, "PASS_TD") + _stat_num(r, "RUSH_TD") + _stat_num(r, "REC_TD"),
        "pass_long": r.get("PASS_LONG"),
        "rush_long": r.get("RUSH_LONG"),
        "rec_long": r.get("REC_LONG"),
        "rec_tgt": r.get("REC_TGT"),
        "fantasy": fantasy(r),
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
        return r.get("PASS_ATT")
    if "pass" in p and "rush" in p and "rec" in p and "td" in p:
        return m["pass_rush_rec_td"]
    if "rush" in p and "rec" in p and "yd" in p:
        return m["rush_rec_yds"]
    if "pass" in p and "rush" in p and "yd" in p:
        return m["pass_rush_yds"]
    if "pass" in p and "yard" in p:
        return r.get("PASS_YDS")
    if "rush" in p and "yard" in p:
        return r.get("RUSH_YDS")
    if ("rec" in p or "receiv" in p) and "yard" in p:
        return r.get("REC_YDS")
    if "reception" in p or p == "rec":
        return r.get("REC")
    if "completion" in p or p in ("pass_cmp", "cmp"):
        return r.get("PASS_CMP")
    if "sack" in p:
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
    if "player" in p and "td" in p:
        return m["player_td"]
    return m.get(p)


def _cfb_played_vals(games: List[dict], prop: str) -> tuple[List[dict], List[float]]:
    played = [g for g in games if game_played_for_prop(g, prop)]
    vals = [float(v) for g in played if (v := prop_value(prop, g)) is not None]
    return played, vals


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
    date_str: str = "",
    segment: str = "",
) -> Tuple[str, List[dict]]:
    """Fetch and parse a single CFB ESPN event. Returns (eid, player_rows)."""
    if slate_ids and (t1 or t2) and t1 not in slate_ids and t2 not in slate_ids:
        return eid, []
    try:
        time.sleep(random.uniform(0.05, 0.25))
        summ = pull_summary(eid)
        if str(segment or "").strip().upper() == "1H":
            return eid, parse_1h_players(summ, game_date=date_str, event_id=eid)
        return eid, parse_players(summ, game_date=date_str, event_id=eid)
    except Exception as e:
        print(f"  [WARN] CFB summary failed event={eid}: {e}")
        log_pipeline_health(
            "cfb.step5b_attach_boxscore_stats",
            "event_summary_failed",
            extra={"event_id": eid, "error": f"{type(e).__name__}: {e}"},
            start=Path(__file__),
        )
        return eid, []


def build_player_histories(
    days: int,
    slate_ids: set,
    workers: int = 4,
    cache_path: str = "",
    tid_to_abbr: dict = None,
    end_date: Optional[dt.date] = None,
    force_refetch_keys: Optional[set] = None,
    slate_athlete_ids: Optional[set] = None,
    volume_team_ids: Optional[set] = None,
    cache_only: bool = False,
    segment: str = "",
    full_game_cache: str = "",
) -> Tuple[Dict, Dict]:
    """
    Parallelized boxscore fetch for CFB with persistent cache + deterministic ordering.
    Returns (hist_aid, hist_name) sorted newest-first per player.
    """
    import os

    # Phase 0: load cache (SQLite first; CSV is backfill only)
    cached_rows: List[dict] = []
    cached_eids: set = set()
    cache_source = "empty"
    sport_key = sport_key_from_espn_league(ESPN_LEAGUE, segment)
    if cache_path:
        try:
            cached_rows, cache_source = load_boxscore_cache(sport_key, cache_path)
            stale_eids: set = set()
            for rr in cached_rows:
                has_opp = (
                    ("opp_team_abbr" in rr and str(rr.get("opp_team_abbr","")).strip() not in ("", "nan"))
                    or ("opp_team_id" in rr and str(rr.get("opp_team_id","")).strip() not in ("", "nan"))
                )
                for col in (
                    "PASS_YDS", "RUSH_YDS", "REC_YDS", "REC", "PASS_TD", "RUSH_TD", "REC_TD",
                    "PASS_CMP", "PASS_ATT", "PASS_INT", "SACK", "TACK_TOT", "KICK_PTS", "KICK_FG", "KICK_XP", "DEF_INT",
                    "PASS_LONG", "RUSH_LONG", "REC_LONG", "REC_TGT", "RUSH_ATT", "TFL",
                ):
                    if col not in rr:
                        continue
                    raw = str(rr[col]).strip()
                    if raw in ("", "nan", "None"):
                        # Missing LONG/TGTS/ATT stay absent so L5 is not invented as 0.
                        rr[col] = None if col in _OPTIONAL_ABSENT else 0.0
                        continue
                    try:
                        rr[col] = float(raw)
                    except Exception:
                        rr[col] = None if col in _OPTIONAL_ABSENT else 0.0
                if not has_opp and str(segment or "").strip().upper() != "1H":
                    stale_eids.add(str(rr.get("event_id","")))
            if stale_eids:
                if cache_only:
                    # Missing opp breaks H2H matching, but L5 still needs these rows.
                    # Dropping them under --cache-only wiped current-season games
                    # (CSV/DB often lack opp on fresh 2026 events) and left only
                    # prior-year tape — exactly the Jones cross-season bug path.
                    print(
                        f"  [CACHE] Keeping {len(stale_eids)} events missing opponent "
                        f"(cache-only — L5 kept; H2H opp blank)"
                    )
                else:
                    cached_rows = [rr for rr in cached_rows if str(rr.get("event_id","")) not in stale_eids]
                    print(f"  [CACHE] Dropped {len(stale_eids)} stale events missing opponent — will refetch")
            cached_eids = {str(rr.get("event_id","")) for rr in cached_rows if rr.get("event_id")}
            if force_refetch_keys:
                drop_eids: set[str] = set()
                for rr in cached_rows:
                    pn = str(rr.get("player_norm") or "").strip()
                    if not (_nfl_name_keys(pn) & set(force_refetch_keys)):
                        continue
                    if str(rr.get("KICK_FG", "")).strip() in ("", "nan"):
                        drop_eids.add(str(rr.get("event_id", "")).strip())
                drop_eids.discard("")
                if drop_eids:
                    cached_rows = [rr for rr in cached_rows if str(rr.get("event_id", "")).strip() not in drop_eids]
                    cached_eids -= drop_eids
                    print(f"  [CACHE] Refetch {len(drop_eids)} events to split FG/XP kicking stats")
            print(f"  [CACHE] Loaded {len(cached_rows)} rows ({len(cached_eids)} events)")
        except Exception as e:
            print(f"  [CACHE] Load failed ({e}) — full refresh")
            cached_rows, cached_eids = [], set()
            cache_source = "empty"
    if cache_only and str(segment or "").strip().upper() == "1H" and not cached_rows:
        print("  [CFB1H] --cache-only ignored (empty first-half PBP cache); fetching ESPN summaries")
        cache_only = False
    if cache_only:
        print(
            f"  [CACHE] cache-only — weeks 0–3 fill L5 from this player's last "
            f"year ({len(cached_rows)} rows, skip ESPN)"
        )
        all_rows = list(cached_rows)
        if tid_to_abbr:
            for rr in all_rows:
                if not rr.get("opp_team_abbr"):
                    opp_id = str(rr.get("opp_team_id", "")).strip()
                    rr["opp_team_abbr"] = tid_to_abbr.get(opp_id, opp_id)
        raw_by_aid: Dict[Tuple[str, str], List[dict]] = {}
        raw_by_name: Dict[Tuple[str, str], List[dict]] = {}
        for rr in all_rows:
            tid = str(rr.get("team_id", "")).strip()
            aid = str(rr.get("espn_athlete_id", "")).strip()
            pn = str(rr.get("player_norm", "")).strip()
            if tid and aid:
                raw_by_aid.setdefault((tid, aid), []).append(rr)
            if tid and pn:
                raw_by_name.setdefault((tid, pn), []).append(rr)

        def _dedup_sort_cache(game_list):
            def _key(r):
                d = _parse_box_date(r.get("game_date"))
                return d or dt.date.min
            sorted_games = sorted(game_list, key=_key, reverse=True)
            seen, out = set(), []
            for g in sorted_games:
                eid = str(g.get("event_id", ""))
                if eid and eid in seen:
                    continue
                if eid:
                    seen.add(eid)
                out.append(g)
            return out

        hist_aid = {k: _dedup_sort_cache(v) for k, v in raw_by_aid.items()}
        hist_name = {k: _dedup_sort_cache(v) for k, v in raw_by_name.items()}
        print(f"-> {ESPN_LEAGUE} histories built | by_id={len(hist_aid)} | by_name={len(hist_name)}")
        return hist_aid, hist_name
    # Phase 1: scoreboards
    all_events: List[Tuple[str, str, str, str]] = []
    seen_eids: set = set()
    print(f"-> Scanning {days + 1} days of {ESPN_LEAGUE} scoreboards...")
    try:
        from tqdm import tqdm as _tqdm
    except ImportError:
        import subprocess as _sp, sys as _sys
        _sp.check_call([_sys.executable, "-m", "pip", "install", "tqdm", "--break-system-packages", "-q"])
        from tqdm import tqdm as _tqdm
    anchor = end_date or dt.date.today()
    for d in _tqdm(date_range(anchor, days), desc="Scanning scoreboards", unit="day"):
        sb = pull_scoreboard(d)
        for eid, t1, t2, date_str in extract_events(sb):
            if eid not in seen_eids:
                seen_eids.add(eid)
                all_events.append((eid, t1, t2, date_str))

    if slate_ids:
        all_events = [(e, t1, t2, ds) for e, t1, t2, ds in all_events
                      if t1 in slate_ids or t2 in slate_ids]

    pending = [(eid, t1, t2, ds) for eid, t1, t2, ds in all_events
               if eid not in cached_eids]

    def _cached_has_rush_long(rr: dict) -> bool:
        v = rr.get("RUSH_LONG")
        if v is None:
            return False
        s = str(v).strip()
        return s not in ("", "nan", "None")

    def _cached_has_pass_long(rr: dict) -> bool:
        v = rr.get("PASS_LONG")
        if v is None:
            return False
        s = str(v).strip()
        return s not in ("", "nan", "None")

    if cached_rows:
        scan_eids = {str(eid) for eid, _, _, _ in all_events}
        scan_cached = [
            rr for rr in cached_rows
            if str(rr.get("event_id", "")).strip() in scan_eids
        ]
        need_long = bool(scan_cached) and (
            not any(_cached_has_rush_long(rr) for rr in scan_cached)
            or not any(_cached_has_pass_long(rr) for rr in scan_cached)
        )
        if need_long:
            cached_rows = [
                rr for rr in cached_rows
                if str(rr.get("event_id", "")).strip() not in scan_eids
            ]
            cached_eids -= scan_eids
            pending = [(eid, t1, t2, ds) for eid, t1, t2, ds in all_events]
            print(f"  [CACHE] Refetch {len(pending)} events to add rush/rec LONG + pass LONG (PBP) + SACKS")

    def _cached_has_vol(rr: dict) -> bool:
        for k in ("RUSH_ATT", "PASS_ATT"):
            v = rr.get(k)
            if v is None:
                continue
            s = str(v).strip()
            if s not in ("", "nan", "None"):
                return True
        return False

    def _cached_has_num(rr: dict, col: str) -> bool:
        v = rr.get(col)
        if v is None:
            return False
        s = str(v).strip()
        if s in ("", "nan", "None"):
            return False
        try:
            return float(s) > 0
        except (TypeError, ValueError):
            return False

    # 2025 games sit outside the ~200-day scoreboard window. Refetch ANY
    # cached event still missing CAR / C/ATT / PBP targets — including
    # teams not on today's slate — so they are ready when they post.
    opp_restore: dict[Tuple[str, str], Tuple[str, str]] = {}
    if ESPN_LEAGUE in ("college-football", "nfl") and cached_rows and str(segment or "").strip().upper() != "1H":
        eids_with_vol = {
            str(rr.get("event_id") or "").strip()
            for rr in cached_rows
            if _cached_has_vol(rr)
        }
        eids_with_vol.discard("")
        eids_with_tgt = {
            str(rr.get("event_id") or "").strip()
            for rr in cached_rows
            if _cached_has_num(rr, "REC_TGT")
        }
        eids_with_tgt.discard("")
        eids_with_rec = {
            str(rr.get("event_id") or "").strip()
            for rr in cached_rows
            if _cached_has_num(rr, "REC")
        }
        eids_with_rec.discard("")
        candidate: set[str] = set()
        eid_meta: dict[str, Tuple[str, str, str]] = {}
        for rr in cached_rows:
            eid = str(rr.get("event_id") or "").strip()
            tid = str(rr.get("team_id") or "").strip()
            if not eid:
                continue
            ds = str(rr.get("game_date") or "").strip().replace("-", "")[:8]
            opp = str(rr.get("opp_team_id") or "").strip()
            opp_ab = str(rr.get("opp_team_abbr") or "").strip()
            if eid not in eid_meta:
                eid_meta[eid] = (tid, opp, ds)
            if tid and (opp or opp_ab):
                opp_restore[(eid, tid)] = (opp, opp_ab)
            candidate.add(eid)
        need_vol = {e for e in candidate if e not in eids_with_vol}
        need_tgt = {e for e in candidate if e in eids_with_rec and e not in eids_with_tgt}
        need_vol |= need_tgt
        if need_vol:
            cached_rows = [
                rr for rr in cached_rows
                if str(rr.get("event_id") or "").strip() not in need_vol
            ]
            cached_eids -= need_vol
            pending_eids = {p[0] for p in pending}
            for eid in sorted(need_vol):
                if eid in pending_eids:
                    continue
                t1, t2, ds = eid_meta.get(eid, ("", "", ""))
                # Empty t1/t2 bypasses the slate-team skip so non-slate
                # 2025 games still refetch.
                pending.append((eid, "", "", ds))
                pending_eids.add(eid)
            print(
                f"  [CACHE] Refetch {len(need_vol)} events to add RUSH_ATT / PASS_ATT / PBP REC_TGT"
                + (f" (incl {len(need_tgt)} missing targets)" if need_tgt else "")
            )

    if str(segment or "").strip().upper() == "1H":
        full_path = str(full_game_cache or "").strip()
        if not full_path and cache_path:
            p = Path(cache_path)
            cand = p.with_name("cfb_boxscore_cache.csv")
            full_path = str(cand) if cand.is_file() else str(p)
        if full_path:
            try:
                full_rows, _src = load_boxscore_cache("cfb", full_path)
            except Exception:
                full_rows = []
            pending_eids = {p[0] for p in pending}
            seeded = 0
            for rr in full_rows:
                eid = str(rr.get("event_id") or "").strip()
                if not eid or eid in cached_eids or eid in pending_eids:
                    continue
                ds = str(rr.get("game_date") or "").strip().replace("-", "")[:8]
                pending.append((eid, "", "", ds))
                pending_eids.add(eid)
                seeded += 1
                tid = str(rr.get("team_id") or "").strip()
                opp = str(rr.get("opp_team_id") or "").strip()
                opp_ab = str(rr.get("opp_team_abbr") or "").strip()
                if tid and (opp or opp_ab):
                    opp_restore[(eid, tid)] = (opp, opp_ab)
            if seeded:
                print(f"  [CFB1H] Seed {seeded} full-game events for first-half PBP (never use full-game yards)")

    print(f"-> {len(all_events)} total | {len(cached_eids)} cached | "
          f"{len(pending)} new ({workers} workers)...")

    # Phase 2: parallel fetch
    new_rows: List[dict] = []
    if pending:
        from concurrent.futures import ThreadPoolExecutor, as_completed as _ac
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(_fetch_one_event_cfb, eid, t1, t2, slate_ids, ds, segment): eid
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
        if opp_restore:
            for rr in new_rows:
                key = (
                    str(rr.get("event_id") or "").strip(),
                    str(rr.get("team_id") or "").strip(),
                )
                if key not in opp_restore:
                    continue
                if str(rr.get("opp_team_id") or "").strip():
                    continue
                oid, oab = opp_restore[key]
                if oid:
                    rr["opp_team_id"] = oid
                if oab:
                    rr["opp_team_abbr"] = oab

    # Phase 3: update cache
    all_rows = cached_rows + new_rows
    # Resolve opp_team_id -> opp_team_abbr using slate map (for H2H matching in step6)
    if tid_to_abbr:
        for rr in all_rows:
            if not rr.get("opp_team_abbr"):
                opp_id = str(rr.get("opp_team_id", "")).strip()
                rr["opp_team_abbr"] = tid_to_abbr.get(opp_id, opp_id)
    if cache_path and all_rows:
        try:
            save_boxscore_cache(sport_key, all_rows, cache_path, cache_source)
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
        def _key(r):
            d = _parse_box_date(r.get("game_date"))
            return d or dt.date.min
        sorted_games = sorted(game_list, key=_key, reverse=True)
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
                    help="Days of history to scan/cache (fetch window). CFB L5/L10 "
                         "may include this player's last-year boxes in weeks 0–3 "
                         "(PRIOR_SEASON_FILL); games_used / l5_sample_n stay current-season.")
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
        help="ESPN league slug (auto: nfl / college-football). CFB paths never match wcbb; that sniff is a known-dead CBB leftover.",
    )
    ap.add_argument("--no_cache", action="store_true",
                    help="Ignore existing cache and force full refresh")
    ap.add_argument(
        "--cache-only",
        action="store_true",
        help="No ESPN fetch. Weeks 0–3: this player's last-year boxes fill "
             "L5/L10 as PRIOR_SEASON_FILL (or PRIOR_SEASON_L5 if no 2026 games). "
             "games_used / l5_sample_n stay current-season. After Week 3, current only.",
    )
    ap.add_argument(
        "--segment",
        default="auto",
        choices=["auto", "full", "1H"],
        help="full = full-game boxscore L5; 1H = first-half PBP only (CFB1H). auto from path/sport.",
    )
    ap.add_argument(
        "--full-game-cache",
        default="",
        help="Full-game CFB cache used to seed CFB1H event ids (never as 1H stat values).",
    )
    args = ap.parse_args()
    cache_path = "" if args.no_cache else args.cache

    global ESPN_LEAGUE
    if args.league == "auto":
        hint = f"{args.input} {args.output} {cache_path}".lower()
        if "nfl" in hint:
            ESPN_LEAGUE = "nfl"
        else:
            # Known-dead CBB leftover: CFB input/output/cache paths never contain
            # "wcbb", so this always resolves to college-football on the live CFB
            # path. A wcbb grep hit here is not a live ESPN-league bug.
            ESPN_LEAGUE = "wocollege-football" if "wcbb" in hint else "college-football"
    else:
        ESPN_LEAGUE = args.league
    print(f"-> ESPN league: {ESPN_LEAGUE}")
    hint = f"{args.input} {args.output} {cache_path}".lower()
    segment = str(args.segment or "auto").strip().upper()
    if segment == "AUTO":
        if "cfb1h" in hint or "cfb_1h" in hint:
            segment = "1H"
        else:
            segment = "FULL"
    if segment == "1H":
        print("-> Stat window: FIRST HALF (ESPN PBP periods 1–2). Full-game box yards are not used.")
        if not args.no_cache:
            c = str(args.cache or "").replace("\\", "/").lower()
            if "cfb1h" not in c:
                cache_path = "data/cache/cfb1h_boxscore_cache.csv"

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
    # Do NOT match "1st Half" (the substring "half" would tag every CFB1H row).
    if "sport" in df.columns and df["sport"].astype(str).str.upper().str.contains("CFB1H", na=False).any():
        segment = "1H"
        if not args.no_cache and "cfb1h" not in str(cache_path).lower():
            cache_path = "data/cache/cfb1h_boxscore_cache.csv"
            print("-> Stat window: FIRST HALF (sport=CFB1H). Full-game box yards are not used.")
    if "duration" in df.columns and segment != "1H":
        h2_mask = df["duration"].str.contains(r"2nd\s*(?:Half|Quarter)|2H", case=False, na=False)
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
    orig_slate_ids = set(slate_ids)
    no_team_id = (df["team_id"].astype(str).str.strip().isin(["", "nan"])).sum()
    if no_team_id > 0:
        print(f"→ {no_team_id} rows have no team_id — scanning all CFB games (no team filter)")
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

    kick_refetch_keys: set[str] = set()
    if ESPN_LEAGUE == "nfl":
        kick_props = {"fg_made", "pat_made", "field_goals", "extra_points"}
        for _, r in df.iterrows():
            if str(r.get(prop_col, "")).strip().lower() in kick_props:
                kick_refetch_keys |= _nfl_name_keys(str(r.get("player_norm", "")))

    def _aid_token(v) -> str:
        s = str(v or "").strip()
        if s.lower() in ("", "nan", "none"):
            return ""
        if s.endswith(".0") and s[:-2].isdigit():
            return s[:-2]
        return s

    slate_aids = {_aid_token(x) for x in df.get("espn_athlete_id", pd.Series(dtype=str))}
    slate_aids.discard("")

    # ── Parallelized fetch ────────────────────────────────────────────────────
    hist_aid, hist_name = build_player_histories(
        args.days,
        slate_ids,
        workers=args.workers,
        cache_path=cache_path,
        tid_to_abbr=tid_to_abbr,
        end_date=end_d,
        force_refetch_keys=kick_refetch_keys or None,
        slate_athlete_ids=slate_aids or None,
        volume_team_ids=orig_slate_ids or None,
        cache_only=bool(args.cache_only),
        segment=segment,
        full_game_cache=str(args.full_game_cache or ""),
    )
    if not hist_aid and not hist_name:
        log_pipeline_health(
            "cfb.step5b_attach_boxscore_stats",
            "no_histories_built",
            extra={"days": args.days, "workers": args.workers},
            start=Path(__file__),
        )

    if ESPN_LEAGUE == "nfl":
        name_to_aid: dict[str, str] = {}
        for (_t, a), glist in hist_aid.items():
            if not a or not glist:
                continue
            pn0 = str(glist[0].get("player_norm") or "").strip()
            for k in _nfl_name_keys(pn0):
                name_to_aid.setdefault(k, a)
        filled = 0
        new_aids = []
        for _, row in df.iterrows():
            cur = str(row.get("espn_athlete_id") or "").strip()
            if cur:
                new_aids.append(cur)
                continue
            found = ""
            for k in _nfl_name_keys(str(row.get("player_norm") or "")):
                if k in name_to_aid:
                    found = name_to_aid[k]
                    filled += 1
                    break
            new_aids.append(found)
        df["espn_athlete_id"] = new_aids
        if filled:
            print(f"  [NFL] Filled {filled} espn_athlete_id values from boxscore cache")

    out_rows, stat_status = [], []

    def _clean_id(v) -> str:
        s = str(v or "").strip()
        if s.lower() in ("", "nan", "none"):
            return ""
        if s.endswith(".0") and s[:-2].isdigit():
            return s[:-2]
        return s

    def _games_any_team(aid: str, pn: str) -> list:
        found: list = []
        if aid:
            for (t, a), g in hist_aid.items():
                if _clean_id(a) == aid:
                    found.extend(g)
        if found:
            return found
        if pn:
            pn_n = str(pn).strip().lower()
            for (t, p), g in hist_name.items():
                if str(p).strip().lower() == pn_n:
                    found.extend(g)
        return found

    for _, row in _tqdm(df.iterrows(), total=len(df), desc="Attaching stats", unit="prop"):
        tid  = _clean_id(row.get("team_id",       ""))
        pn   = str(row.get("player_norm",   "")).strip()
        aid  = _clean_id(row.get("espn_athlete_id",""))
        prop = str(row.get(prop_col,        "")).strip()
        line = row.get("line", None)

        # Honor step2 unsupported_prop (NFL period/first-N) before any box lookup.
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

        # Multi-player combos (A + B): sum each arm's newest-first game series by index.
        # Do not look up collapsed player_norm ("aj surace mason mckenzie") as one athlete.
        combo_arms = multiplayer_combo_arm_names(row.get("player", ""))
        vals: List[float] = []
        played: List[dict] = []
        current_vals: List[float] = []

        if combo_arms:
            base_prop = base_prop_for_combo(prop)
            arm_series: List[List[float]] = []
            arm_cur_series: List[List[float]] = []
            season_flag = ""
            for arm_name in combo_arms:
                arm_pn = norm(arm_name)
                if ESPN_LEAGUE == "nfl":
                    arm_games = _lookup_nfl_box_games(arm_pn, "", hist_aid, hist_name)
                    arm_games = _nfl_regular_season_games(arm_games)
                    arm_cur_g, arm_pri_g = _split_nfl_by_season(arm_games, end_d)
                    _, arm_cur = _cfb_played_vals(arm_cur_g, base_prop)
                    _, arm_pri = _cfb_played_vals(arm_pri_g, base_prop)
                    arm_vals, arm_flag = _nfl_face_vals(arm_cur, arm_pri, end_d)
                    if arm_flag == "PRIOR_SEASON_FILL":
                        season_flag = arm_flag
                    elif arm_flag and not season_flag:
                        season_flag = arm_flag
                else:
                    # This athlete only (espn id, then exact player_norm) — never
                    # a position or team average. Weeks 0–3 may mix last year.
                    arm_games = _games_any_team("", arm_pn)
                    current, prior = _split_cfb_by_season(arm_games, end_d)
                    _, arm_cur = _cfb_played_vals(current, base_prop)
                    _, arm_pri = _cfb_played_vals(prior, base_prop)
                    arm_vals, arm_flag = _cfb_face_vals(arm_cur, arm_pri, end_d)
                    if arm_flag == "PRIOR_SEASON_FILL":
                        season_flag = arm_flag
                    elif not season_flag:
                        season_flag = arm_flag
                if not arm_vals:
                    arm_series = []
                    arm_cur_series = []
                    break
                arm_series.append(arm_vals)
                arm_cur_series.append(arm_cur)
            combo_vals = sum_multiplayer_combo_game_vals(arm_series) if arm_series else None
            if not combo_vals:
                stat_status.append(season_flag or "NO_BOX_HISTORY")
                out_rows.append({})
                continue
            vals = combo_vals
            played = [{"MIN": 1, "game_date": ""} for _ in vals]
            if (
                arm_cur_series
                and len(arm_cur_series) == len(arm_series)
                and all(arm_cur_series)
            ):
                current_vals = sum_multiplayer_combo_game_vals(arm_cur_series) or []
            else:
                current_vals = list(vals) if ESPN_LEAGUE == "nfl" else []
        else:
            season_flag = ""
            # NFL / NFLP: regular-season/playoff boxscores only (drop August),
            # then current-season boundary (Sep 1). Nickname aliases cover
            # Cam/Cameron etc. Mar–Aug NFLP still sees last year's Sep–Feb.
            if ESPN_LEAGUE == "nfl":
                games = _lookup_nfl_box_games(pn, aid, hist_aid, hist_name)
                games = _nfl_regular_season_games(games)
                current, prior = _split_nfl_by_season(games, end_d)
                cur_played, cur_vals = _cfb_played_vals(current, prop)
                pri_played, pri_vals = _cfb_played_vals(prior, prop)
                vals, season_flag = _nfl_face_vals(cur_vals, pri_vals, end_d)
                if not vals:
                    stat_status.append(season_flag or "NO_BOX_HISTORY")
                    out_rows.append({})
                    continue
                # Rebuild played rows to match face vals length (current then prior).
                played = list(cur_played) + list(pri_played)
                if season_flag == "PRIOR_SEASON_L5":
                    played = list(pri_played)
                elif season_flag != "PRIOR_SEASON_FILL":
                    played = list(cur_played)
                current_vals = list(cur_vals)
            else:
                # This athlete only: espn_athlete_id (all teams — transfers stay
                # the same person), then same-team name, then exact player_norm.
                # Never position/team-average fill.
                games = _games_any_team(aid, "") if aid else []
                if not games and tid and pn:
                    games = hist_name.get((tid, pn), [])
                if not games:
                    games = _games_any_team("", pn)
                current, prior = _split_cfb_by_season(games, end_d)
                cur_played, cur_vals = _cfb_played_vals(current, prop)
                pri_played, pri_vals = _cfb_played_vals(prior, prop)
                vals, season_flag = _cfb_face_vals(cur_vals, pri_vals, end_d)
                if not vals:
                    stat_status.append(season_flag or "NO_BOX_HISTORY")
                    out_rows.append({})
                    continue
                if season_flag in ("PRIOR_SEASON_FILL", "PRIOR_SEASON_L5"):
                    played = cur_played + pri_played if cur_played else pri_played
                else:
                    played = cur_played
                current_vals = cur_vals

        if not vals:
            if season_flag:
                stat_status.append(season_flag)
            else:
                stat_status.append("UNSUPPORTED_PROP")
            out_rows.append({})
            continue
        # CFB: weeks 0–3 face window may include this player's last year.
        # l5_sample_n / games_used stay current-season so a filled n=5 is not
        # presented as a full 2026 sample.
        # CFB + NFL weeks 1–3: face window may include this player's last year.
        # l5_sample_n / games_used stay current-season so a filled n=5 is not
        # presented as a full current sample.
        is_face_fill = True  # both leagues use current_vals honesty now
        if is_face_fill:
            thin_pending = (
                len(current_vals) < 5
                and season_flag
                not in ("PRIOR_SEASON_L5", "PRIOR_SEASON_FILL", "NO_CURRENT_SEASON")
            )
            season_vals = list(current_vals)
        else:
            thin_pending = (
                len(vals) < 5
                and season_flag
                not in ("PRIOR_SEASON_L5", "NO_CURRENT_SEASON")
            )
            season_vals = vals[:]

        # vals is now newest-first (already sorted from dedup_sort)

        # Face window truncated for g1..gN / L5 / L10
        game_log    = vals[:args.n]
        last5       = game_log[:5]
        last10      = game_log[:10]
        l5_n_current = min(5, len(current_vals)) if is_face_fill else len(last5)

        o = {
            "games_used": len(season_vals),   # current-season game count (CFB)
            "l5_sample_n": l5_n_current,       # honest current n, not padded 5
            "season_l5_flag": "",  # filled after hit-rate thin classify
        }
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

        over5 = under5 = push5 = 0
        # hit rates vs line — CFB L5/L10 use the face window (current + last year)
        if pd.notna(line):
            ln = float(line)

            over5, under5, push5, hr_ov5, hr_un5, hr_ov_ou5, hr_un_ou5 = hit_rates(game_log, ln, 5)
            o["line_hits_over_5"]         = over5
            o["line_hits_under_5"]        = under5
            o["line_hits_push_5"]         = push5
            o["last5_over"]               = over5
            o["last5_under"]              = under5
            o["l5_over"]                  = over5
            o["l5_under"]                 = under5
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

        # Season / thin flags — never present a filled L5 as a clean 2026 sample.
        # PRIOR_SEASON_FILL (mixed) / PRIOR_SEASON_L5 (prior-only) win over THIN_*.
        thin_overs, thin_unders, thin_pushes = over5, under5, push5
        thin_n = len(last5)
        if is_face_fill:
            thin_n = len(current_vals)
            if current_vals and pd.notna(line):
                thin_overs, thin_unders, thin_pushes, *_ = hit_rates(
                    current_vals, float(line), 5
                )
        if season_flag in ("PRIOR_SEASON_FILL", "PRIOR_SEASON_L5", "NO_CURRENT_SEASON"):
            final_flag = season_flag
        elif is_face_fill:
            if len(current_vals) < 5:
                final_flag = _cfb_thin_sample_flag(
                    n=thin_n,
                    overs=int(thin_overs or 0),
                    unders=int(thin_unders or 0),
                    pushes=int(thin_pushes or 0),
                )
            else:
                final_flag = ""
        elif thin_pending or len(last5) < 5:
            final_flag = _cfb_thin_sample_flag(
                n=len(last5),
                overs=int(over5 or 0),
                unders=int(under5 or 0),
                pushes=int(push5 or 0),
            )
        else:
            final_flag = ""
        o["season_l5_flag"] = final_flag
        # CFB: attach OK even when filled/thin — Jones tag is season_l5_flag.
        # NFL keeps THIN_* / PRIOR on stat_status (step6 eligibility).
        if is_face_fill:
            stat_status.append("OK")
        elif final_flag:
            stat_status.append(final_flag)
        else:
            stat_status.append("OK")
        out_rows.append(o)

    stats_df = pd.DataFrame(out_rows).fillna("")
    df["stat_status"] = stat_status
    out = pd.concat([df.reset_index(drop=True), stats_df], axis=1)

    # Re-attach the 2H rows (they carry UNSUPPORTED_2H status, no stat columns)
    if not df_2h.empty:
        out = pd.concat([out, df_2h], ignore_index=True, sort=False).fillna("")

    if ESPN_LEAGUE == "college-football":
        try:
            vol_mod = _load_player_volume_mod()
            flat = vol_mod.flatten_histories(hist_aid, hist_name)
            vol_df = vol_mod.aggregate_player_volume(flat)
            if tid_to_abbr and not vol_df.empty and "team_id" in vol_df.columns:
                mapped = vol_df["team_id"].astype(str).map(tid_to_abbr)
                blank = vol_df["team_abbr"].astype(str).str.strip().isin(("", "nan"))
                vol_df.loc[blank, "team_abbr"] = mapped[blank].fillna("")
            vol_path = Path(__file__).resolve().parents[2] / "data" / "reference" / "cfb_player_volume.csv"
            if not vol_df.empty:
                vol_path.parent.mkdir(parents=True, exist_ok=True)
                vol_df.to_csv(vol_path, index=False)
                n_rush = int(pd.to_numeric(vol_df["rush_att_pg"], errors="coerce").notna().sum())
                n_tgt = int(pd.to_numeric(vol_df["rec_tgt_pg"], errors="coerce").notna().sum())
                print(f"  [VOLUME] {vol_path.name} players={len(vol_df)} rush_att={n_rush} rec_tgt={n_tgt}")
            out = vol_mod.attach_player_volume(out, vol_df)
            n_pg = int(pd.to_numeric(out.get("rush_att_pg"), errors="coerce").notna().sum()) if "rush_att_pg" in out.columns else 0
            n_tg = int(pd.to_numeric(out.get("rec_tgt_pg"), errors="coerce").notna().sum()) if "rec_tgt_pg" in out.columns else 0
            print(f"  [VOLUME] attached rush_att_pg={n_pg} rec_tgt_pg={n_tg}")
        except Exception as exc:
            print(f"  [VOLUME] skip ({exc})")

    out.to_csv(args.output, index=False)

    print(f"✅ Saved → {args.output} | rows={len(out)}")
    print("stat_status breakdown:")
    print(out["stat_status"].value_counts().to_string())
    if ESPN_LEAGUE == "college-football" and "season_l5_flag" in out.columns:
        flags = out["season_l5_flag"].replace("", "(current)")
        print("season_l5_flag breakdown:")
        print(flags.value_counts().to_string())




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
        print(f"❌ CFB step5b failed (logged). {type(e).__name__}: {e}")
        raise
