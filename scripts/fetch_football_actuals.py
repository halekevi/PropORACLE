#!/usr/bin/env python3
"""
Fetch NFL or CFB player stat actuals from ESPN box scores for slate grading.

  py -3.14 scripts/fetch_football_actuals.py --league nfl --date 2025-09-07
  py -3.14 scripts/fetch_football_actuals.py --league cfb --date 2025-11-15

ESPN often 403s bare site.api calls; we send a browser Referer and fall back to
site.web.api (same pattern as CFB boxscore engine / fetch_actuals soccer).
"""

from __future__ import annotations

import argparse
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import requests

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.actuals_file_guard import write_actuals_csv  # noqa: E402
from utils.eastern_slate_date import event_start_raw, keep_event_on_slate  # noqa: E402

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.espn.com/",
    "Origin": "https://www.espn.com",
}

LEAGUE_PATH = {
    "nfl": "football/nfl",
    "cfb": "football/college-football",
}

# ESPN stat group label -> (prop_type, stat label in group)
_STAT_ROWS: list[tuple[str, str, str]] = [
    ("passing", "YDS", "passing_yards"),
    ("passing", "TD", "passing_tds"),
    ("passing", "C", "completions"),
    ("passing", "CMP", "completions"),
    ("passing", "ATT", "pass_attempts"),
    ("passing", "INT", "interceptions"),
    ("passing", "LONG", "longest_completion"),
    ("passing", "LNG", "longest_completion"),
    ("passing", "SACKS", "sacks_taken"),
    ("rushing", "YDS", "rushing_yards"),
    ("rushing", "TD", "rushing_tds"),
    ("rushing", "CAR", "rushing_attempts"),
    ("rushing", "ATT", "rushing_attempts"),
    ("rushing", "LONG", "longest_rush"),
    ("rushing", "LNG", "longest_rush"),
    ("rushing", "AVG", "rush_yards_per_carry"),
    ("receiving", "YDS", "receiving_yards"),
    ("receiving", "REC", "receptions"),
    ("receiving", "TD", "receiving_tds"),
    ("receiving", "TGTS", "targets"),
    ("receiving", "TGT", "targets"),
    ("receiving", "LONG", "longest_reception"),
    ("receiving", "LNG", "longest_reception"),
    ("defensive", "TOT", "tackles_assists"),
    ("defensive", "SACK", "sacks"),
    ("defensive", "SACKS", "sacks"),
    ("defensive", "INT", "defensive_interceptions"),
    ("kicking", "PTS", "kicking_points"),
    ("kicking", "FG", "fg_made"),
    ("kicking", "XP", "pat_made"),
    ("kicking", "LONG", "longest_fg_made"),
    ("punting", "NO", "punts"),
    ("punting", "PUNTS", "punts"),
    ("punting", "TB", "punt_touchbacks"),
    ("punting", "IN 20", "punts_inside_20"),
]

_NAME_TOKEN_RE = re.compile(r"\b([A-Z])\.([A-Za-z][A-Za-z'\-]*)")
_PASS_COMPLETE_TYPES = frozenset({"pass reception", "passing touchdown"})
_PASS_ATTEMPT_TYPES = frozenset(
    {"pass reception", "passing touchdown", "pass incompletion", "interception return"}
)
_RUSH_TYPES = frozenset({"rush", "rushing touchdown"})
_FG_GOOD_TYPES = frozenset({"field goal good"})
_NAME_SUFFIXES = frozenset({"jr", "sr", "ii", "iii", "iv", "v"})

# Categories whose TD count toward anytime / player_touchdowns (not passing TDs).
_SCORING_TD_CATS = frozenset(
    {"rushing", "receiving", "kickreturns", "puntreturns", "defensive", "interceptions"}
)

# Return-only box appearances (e.g. Marvin Mims in puntReturns with 0 targets).
# Detected via ``_is_return_cat``.

# Offensive counting props to emit as 0 when a player played but has no box line
# for that group (returner-only, or active roster with zero offensive snaps).
_OFFENSE_ZERO_PROPS: tuple[str, ...] = (
    "rushing_yards",
    "rushing_attempts",
    "longest_rush",
    "rushing_tds",
    "receiving_yards",
    "receptions",
    "longest_reception",
    "targets",
    "receiving_tds",
)

_PASS_ZERO_PROPS: tuple[str, ...] = (
    "pass_attempts",
    "completions",
    "longest_completion",
    "interceptions",
    "sacks_taken",
    "passing_tds",
)

_DEFENSE_ZERO_PROPS: tuple[str, ...] = (
    "sacks",
    "tackles_assists",
)

_ATHLETE_NAME_CACHE: dict[tuple[int, int], str] = {}
_ATHLETE_NAME_DISK = _REPO / "Sports" / "NFL" / "data" / "cache" / "espn_athlete_names.json"
_athlete_disk_loaded = False


def _load_athlete_name_disk() -> None:
    global _athlete_disk_loaded
    if _athlete_disk_loaded:
        return
    _athlete_disk_loaded = True
    if not _ATHLETE_NAME_DISK.is_file():
        return
    try:
        import json

        raw = json.loads(_ATHLETE_NAME_DISK.read_text(encoding="utf-8"))
        if isinstance(raw, dict):
            for k, v in raw.items():
                parts = str(k).split(":", 1)
                if len(parts) != 2:
                    continue
                try:
                    _ATHLETE_NAME_CACHE[(int(parts[0]), int(parts[1]))] = str(v or "")
                except ValueError:
                    continue
    except Exception:
        pass


def _save_athlete_name_disk() -> None:
    try:
        import json

        _ATHLETE_NAME_DISK.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            f"{pid}:{season}": name
            for (pid, season), name in _ATHLETE_NAME_CACHE.items()
            if name
        }
        _ATHLETE_NAME_DISK.write_text(
            json.dumps(payload, indent=0, sort_keys=True), encoding="utf-8"
        )
    except Exception:
        pass


def _num(x) -> float | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s or s in ("-", "--"):
        return None
    # FG / XP often arrive as "1/2" (made/att) — take the made half.
    if "/" in s:
        left = s.split("/", 1)[0].strip()
        try:
            return float(left)
        except ValueError:
            pass
    # Sacks taken arrive as "6-38" (sacks-yards) — take the left half.
    if re.fullmatch(r"\d+(?:\.\d+)?-\d+(?:\.\d+)?", s):
        try:
            return float(s.split("-", 1)[0])
        except ValueError:
            pass
    try:
        return float(s)
    except ValueError:
        m = re.search(r"-?\d+(?:\.\d+)?", s)
        return float(m.group(0)) if m else None


def _period_num(play: dict) -> int:
    per = play.get("period")
    if isinstance(per, dict):
        try:
            return int(per.get("number") or 0)
        except (TypeError, ValueError):
            return 0
    try:
        return int(per or 0)
    except (TypeError, ValueError):
        return 0


def _name_last(display: str) -> str:
    parts = [p for p in str(display or "").replace(".", " ").split() if p]
    if not parts:
        return ""
    last = parts[-1]
    if last.lower().rstrip(".") in _NAME_SUFFIXES and len(parts) >= 2:
        last = parts[-2]
    return last.lower().rstrip(".")


def _athlete_token_index(summary: dict) -> dict[tuple[str, str], list[tuple[str, str]]]:
    """Map (first_initial, last_name) -> [(displayName, team_abbr), ...]."""
    idx: dict[tuple[str, str], list[tuple[str, str]]] = {}
    box = summary.get("boxscore") or {}
    for team_block in box.get("players") or []:
        team = str((team_block.get("team") or {}).get("abbreviation") or "").strip().upper()
        if not team:
            continue
        for group in team_block.get("statistics") or []:
            for athlete in group.get("athletes") or []:
                ath = athlete.get("athlete") or {}
                name = str(ath.get("displayName") or ath.get("shortName") or "").strip()
                if not name:
                    continue
                parts = [p for p in name.replace(".", " ").split() if p]
                if not parts:
                    continue
                first = parts[0]
                last = _name_last(name)
                if not first or not last:
                    continue
                key = (first[0].lower(), last)
                bucket = idx.setdefault(key, [])
                if (name, team) not in bucket:
                    bucket.append((name, team))
    return idx


def _resolve_token(
    idx: dict[tuple[str, str], list[tuple[str, str]]],
    initial: str,
    last: str,
    *,
    prefer_team: str = "",
) -> tuple[str, str] | None:
    key = (str(initial or "").lower()[:1], str(last or "").lower().rstrip("."))
    cands = idx.get(key) or []
    if not cands:
        # Fallback: last-name only when unique across box.
        last_only = [
            (n, t)
            for (ini, ln), rows in idx.items()
            if ln == key[1]
            for (n, t) in rows
        ]
        # Dedup
        uniq: dict[tuple[str, str], None] = {}
        for row in last_only:
            uniq[row] = None
        cands = list(uniq.keys())
        if len(cands) != 1:
            return None
    if prefer_team:
        team_hit = [c for c in cands if c[1] == prefer_team]
        if len(team_hit) == 1:
            return team_hit[0]
        if len(team_hit) > 1:
            return team_hit[0]
    return cands[0] if len(cands) == 1 else (cands[0] if cands else None)


def _offense_team(play: dict) -> str:
    for key in ("teamParticipants", "participants"):
        for part in play.get(key) or []:
            if not isinstance(part, dict):
                continue
            team = part.get("team") or part
            abbr = str(
                (team.get("abbreviation") if isinstance(team, dict) else "") or ""
            ).strip().upper()
            if abbr:
                return abbr
    start = play.get("start") or {}
    if isinstance(start, dict):
        team = start.get("team") or {}
        if isinstance(team, dict):
            abbr = str(team.get("abbreviation") or "").strip().upper()
            if abbr:
                return abbr
    return ""


def _parse_drives_micro(summary: dict) -> list[dict]:
    """Drive PBP micros: longest completion, first-N, halves/quarters, FG yards."""
    idx = _athlete_token_index(summary)
    if not idx:
        return []

    # Per (name, team) bags
    rush_yds: dict[tuple[str, str], list[tuple[int, float]]] = {}
    rec_yds: dict[tuple[str, str], list[tuple[int, float]]] = {}
    pass_comp_yds: dict[tuple[str, str], list[tuple[int, float]]] = {}
    pass_att_flags: dict[tuple[str, str], list[tuple[int, int]]] = {}  # (period, completed)
    fg_made_yds: dict[tuple[str, str], list[float]] = {}

    drives = (summary.get("drives") or {}).get("previous") or []
    for drive in drives:
        for play in drive.get("plays") or []:
            if play.get("isPenalty"):
                continue
            typ = str(((play.get("type") or {}).get("text") or "")).strip().lower()
            text = str(play.get("text") or "")
            if not typ or not text:
                continue
            period = _period_num(play)
            yards = play.get("statYardage")
            try:
                yds_f = float(yards) if yards is not None else None
            except (TypeError, ValueError):
                yds_f = None
            offense = _offense_team(play)
            tokens = _NAME_TOKEN_RE.findall(text)

            if typ in _RUSH_TYPES and tokens:
                # Rusher is typically the first Initial.Last before paren tacklers.
                head = text.split("(", 1)[0]
                head_toks = _NAME_TOKEN_RE.findall(head)
                pick = head_toks[0] if head_toks else tokens[0]
                resolved = _resolve_token(idx, pick[0], pick[1], prefer_team=offense)
                if resolved and yds_f is not None:
                    rush_yds.setdefault(resolved, []).append((period, yds_f))

            elif typ in _PASS_COMPLETE_TYPES and tokens:
                # Passer first; receiver after " to ".
                passer = _resolve_token(idx, tokens[0][0], tokens[0][1], prefer_team=offense)
                receiver = None
                m_to = re.search(
                    r"\bto\s+([A-Z])\.([A-Za-z][A-Za-z'\-]*)",
                    text,
                )
                if m_to:
                    receiver = _resolve_token(
                        idx, m_to.group(1), m_to.group(2), prefer_team=offense
                    )
                if passer and yds_f is not None:
                    pass_comp_yds.setdefault(passer, []).append((period, yds_f))
                    pass_att_flags.setdefault(passer, []).append((period, 1))
                if receiver and yds_f is not None:
                    rec_yds.setdefault(receiver, []).append((period, yds_f))

            elif typ in _PASS_ATTEMPT_TYPES and tokens:
                passer = _resolve_token(idx, tokens[0][0], tokens[0][1], prefer_team=offense)
                if passer:
                    completed = 1 if typ in _PASS_COMPLETE_TYPES else 0
                    # Completes already recorded above; skip double-count.
                    if typ not in _PASS_COMPLETE_TYPES:
                        pass_att_flags.setdefault(passer, []).append((period, completed))

            elif typ in _FG_GOOD_TYPES and tokens and yds_f is not None:
                # "J.Moody 29 yard field goal"
                kicker = _resolve_token(idx, tokens[0][0], tokens[0][1], prefer_team=offense)
                if kicker:
                    fg_made_yds.setdefault(kicker, []).append(yds_f)

    rows: list[dict] = []

    def _emit(key: tuple[str, str], prop: str, val: float) -> None:
        rows.append(
            {
                "player": key[0],
                "team": key[1],
                "prop_type": prop,
                "actual": float(val),
            }
        )

    for key, seq in pass_comp_yds.items():
        longs = [y for _, y in seq]
        if longs:
            _emit(key, "longest_completion", max(longs))
        by_q: dict[int, float] = {}
        by_h: dict[int, float] = {}
        for per, y in seq:
            by_q[per] = by_q.get(per, 0.0) + y
            half = 1 if per <= 2 else 2
            by_h[half] = by_h.get(half, 0.0) + y
        _emit(key, "halves_with_100_pass_yards", sum(1 for v in by_h.values() if v >= 100))
        _emit(key, "quarters_with_25_pass_yards", sum(1 for v in by_q.values() if v >= 25))

    for key, flags in pass_att_flags.items():
        first10 = flags[:10]
        if first10:
            _emit(
                key,
                "completions_in_first_10_pass_attempts",
                float(sum(c for _, c in first10)),
            )

    for key, seq in rush_yds.items():
        if seq:
            _emit(key, "yards_on_first_rush_attempt", seq[0][1])
            first5 = seq[:5]
            _emit(key, "rush_yards_in_first_5_attempts", sum(y for _, y in first5))
        by_q: dict[int, float] = {}
        by_h: dict[int, float] = {}
        for per, y in seq:
            by_q[per] = by_q.get(per, 0.0) + y
            half = 1 if per <= 2 else 2
            by_h[half] = by_h.get(half, 0.0) + y
        _emit(key, "halves_with_25_rush_yards", sum(1 for v in by_h.values() if v >= 25))
        _emit(key, "quarters_with_5_rush_yards", sum(1 for v in by_q.values() if v >= 5))

    for key, seq in rec_yds.items():
        if seq:
            _emit(key, "yards_on_first_reception", seq[0][1])
            first2 = seq[:2]
            _emit(
                key,
                "receiving_yards_in_first_2_receptions",
                sum(y for _, y in first2),
            )
        by_q_yds: dict[int, float] = {}
        by_q_rec: dict[int, int] = {}
        by_h: dict[int, float] = {}
        for per, y in seq:
            by_q_yds[per] = by_q_yds.get(per, 0.0) + y
            by_q_rec[per] = by_q_rec.get(per, 0) + 1
            half = 1 if per <= 2 else 2
            by_h[half] = by_h.get(half, 0.0) + y
        _emit(
            key,
            "halves_with_25_receiving_yards",
            sum(1 for v in by_h.values() if v >= 25),
        )
        _emit(
            key,
            "quarters_with_5_receiving_yards",
            sum(1 for v in by_q_yds.values() if v >= 5),
        )
        _emit(
            key,
            "quarters_with_1_reception",
            sum(1 for v in by_q_rec.values() if v >= 1),
        )

    for key, made in fg_made_yds.items():
        if not made:
            continue
        _emit(key, "field_goal_yards", float(sum(made)))
        _emit(key, "longest_fg_made", float(max(made)))
        _emit(key, "shortest_fg_made", float(min(made)))

    return rows


def _scoreboard_urls(league_path: str, date_str: str) -> list[str]:
    d = date_str.replace("-", "")
    q = f"{league_path}/scoreboard?dates={d}&limit=500"
    # FBS only — bare CFB scoreboard often returns 0 events (Labor Day / midweek).
    if "college-football" in league_path:
        q = f"{q}&groups=80"
    return [
        f"https://site.api.espn.com/apis/site/v2/sports/{q}",
        f"https://site.web.api.espn.com/apis/site/v2/sports/{q}",
    ]


def _summary_urls(league_path: str, event_id: str) -> list[str]:
    q = f"{league_path}/summary?event={event_id}"
    return [
        f"https://site.web.api.espn.com/apis/site/v2/sports/{q}",
        f"https://site.api.espn.com/apis/site/v2/sports/{q}",
    ]


def _espn_get_json(urls: list[str]) -> dict | None:
    last_err: Exception | str | None = None
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=25)
            if r.status_code == 403:
                last_err = f"403 from {url.split('//', 1)[-1].split('/', 1)[0]}"
                continue
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, dict):
                return payload
            last_err = "non-dict JSON"
        except Exception as exc:
            last_err = exc
            continue
    print(f"  ERROR ESPN GET: {last_err}")
    return None


def _is_return_cat(cat: str) -> bool:
    c = re.sub(r"[^a-z0-9]+", "", str(cat or "").lower())
    return c in {"kickreturns", "puntreturns"}


def _event_id_and_competitors(summary: dict) -> tuple[str, list[tuple[str, str]]]:
    """Return (event_id, [(competitor_id, team_abbr), ...]) from a summary payload."""
    header = summary.get("header") or {}
    comps = header.get("competitions") or []
    if not comps:
        return "", []
    comp = comps[0] if isinstance(comps[0], dict) else {}
    eid = str(comp.get("id") or header.get("id") or "").strip()
    out: list[tuple[str, str]] = []
    for c in comp.get("competitors") or []:
        if not isinstance(c, dict):
            continue
        cid = str(c.get("id") or "").strip()
        abbr = str((c.get("team") or {}).get("abbreviation") or "").strip().upper()
        if cid and abbr:
            out.append((cid, abbr))
    return eid, out


def _athlete_display_name(player_id: int, season: int) -> str:
    _load_athlete_name_disk()
    key = (int(player_id), int(season))
    if key in _ATHLETE_NAME_CACHE:
        return _ATHLETE_NAME_CACHE[key]
    url = (
        "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/"
        f"seasons/{season}/athletes/{player_id}"
    )
    name = ""
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        if r.ok:
            payload = r.json()
            if isinstance(payload, dict):
                name = str(
                    payload.get("displayName") or payload.get("fullName") or ""
                ).strip()
    except Exception:
        name = ""
    _ATHLETE_NAME_CACHE[key] = name
    return name


def _nfl_game_roster_players(
    summary: dict, *, season: int
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """Return (active_players, dnp_players) as (displayName, team_abbr) lists.

    Active (didNotPlay=False): dressed players — zero-fill counting props when
    they have no box line (or only fumble/return crumbs).
    DNP (didNotPlay=True): emit a ``dnp`` actual so the grader voids as
    ESPN_ROSTER_DNP instead of opaque NO_ACTUAL.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    eid, competitors = _event_id_and_competitors(summary)
    if not eid or not competitors:
        return [], []
    jobs: list[tuple[int, str, bool]] = []
    for cid, abbr in competitors:
        url = (
            "https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/"
            f"events/{eid}/competitions/{eid}/competitors/{cid}/roster"
        )
        payload = _espn_get_json([url])
        if not payload:
            continue
        for entry in payload.get("entries") or []:
            if not isinstance(entry, dict):
                continue
            pid = entry.get("playerId")
            try:
                pid_i = int(pid)
            except (TypeError, ValueError):
                continue
            jobs.append((pid_i, abbr, bool(entry.get("didNotPlay") is True)))

    actives: list[tuple[str, str]] = []
    dnps: list[tuple[str, str]] = []
    if not jobs:
        return actives, dnps

    def _one(pid_i: int, abbr: str, is_dnp: bool) -> tuple[str, str, bool]:
        return _athlete_display_name(pid_i, season), abbr, is_dnp

    with ThreadPoolExecutor(max_workers=12) as pool:
        futs = [pool.submit(_one, pid_i, abbr, is_dnp) for pid_i, abbr, is_dnp in jobs]
        for fut in as_completed(futs):
            try:
                name, abbr, is_dnp = fut.result()
            except Exception:
                continue
            if not name:
                continue
            if is_dnp:
                dnps.append((name, abbr))
            else:
                actives.append((name, abbr))
    _save_athlete_name_disk()
    return actives, dnps


def _active_nfl_roster_players(
    summary: dict, *, season: int
) -> list[tuple[str, str]]:
    """Backward-compatible wrapper — active (non-DNP) roster only."""
    actives, _dnps = _nfl_game_roster_players(summary, season=season)
    return actives


def _emit_zero(
    rows: list[dict],
    bag: dict[tuple[str, str], dict[str, float]],
    key: tuple[str, str],
    prop: str,
) -> None:
    name, team = key
    rows.append(
        {
            "player": name,
            "team": team,
            "prop_type": prop,
            "actual": 0.0,
        }
    )
    bag.setdefault(key, {})[prop] = 0.0


def _parse_box(summary: dict, *, season: int | None = None) -> list[dict]:
    rows: list[dict] = []
    # Scoring TDs (rush/rec/return/def) for player_touchdowns / anytime_td.
    # Every athlete who appears in the box gets a TD total (0 if none) so OVER 0.5
    # props grade as MISS instead of NO_ACTUAL when the player played but did not score.
    td_totals: dict[tuple[str, str], float] = {}
    seen_athletes: set[tuple[str, str]] = set()
    return_athletes: set[tuple[str, str]] = set()
    def_adjacent_athletes: set[tuple[str, str]] = set()
    # Per-player counting bag for derived combos / fantasy.
    bag: dict[tuple[str, str], dict[str, float]] = {}

    def _bump(key: tuple[str, str], prop: str, val: float) -> None:
        bag.setdefault(key, {})[prop] = float(val)

    box = summary.get("boxscore") or {}
    for team_block in box.get("players") or []:
        team = str((team_block.get("team") or {}).get("abbreviation") or "").strip().upper()
        if not team:
            continue
        for group in team_block.get("statistics") or []:
            cat = str(group.get("name") or group.get("displayName") or "").strip().lower()
            labels = [str(x).strip().upper() for x in (group.get("labels") or [])]
            for athlete in group.get("athletes") or []:
                ath = athlete.get("athlete") or {}
                name = str(ath.get("displayName") or ath.get("shortName") or "").strip()
                if not name:
                    continue
                key = (name, team)
                seen_athletes.add(key)
                if _is_return_cat(cat):
                    return_athletes.add(key)
                cat_compact = re.sub(r"[^a-z0-9]+", "", cat)
                if cat_compact.startswith("defensive") or cat_compact.startswith("fum"):
                    def_adjacent_athletes.add(key)
                stats = athlete.get("stats") or []
                stat_map = {labels[i]: stats[i] for i in range(min(len(labels), len(stats)))}

                # Completions / attempts may only appear inside C/ATT.
                if "C/ATT" in stat_map:
                    raw = str(stat_map.get("C/ATT") or "").strip()
                    if "/" in raw:
                        left, right = raw.split("/", 1)
                        cmp_v, att_v = _num(left), _num(right)
                    else:
                        cmp_v, att_v = _num(raw), None
                    if cmp_v is not None and "C" not in stat_map and "CMP" not in stat_map:
                        rows.append(
                            {
                                "player": name,
                                "team": team,
                                "prop_type": "completions",
                                "actual": cmp_v,
                            }
                        )
                        _bump(key, "completions", cmp_v)
                    if att_v is not None and "ATT" not in stat_map:
                        rows.append(
                            {
                                "player": name,
                                "team": team,
                                "prop_type": "pass_attempts",
                                "actual": att_v,
                            }
                        )
                        _bump(key, "pass_attempts", att_v)
                    if cmp_v is not None and att_v and att_v > 0:
                        pct = round(100.0 * float(cmp_v) / float(att_v), 1)
                        rows.append(
                            {
                                "player": name,
                                "team": team,
                                "prop_type": "completion_percentage",
                                "actual": pct,
                            }
                        )
                        _bump(key, "completion_percentage", pct)

                for gcat, label, prop_type in _STAT_ROWS:
                    if gcat not in cat:
                        continue
                    val = _num(stat_map.get(label.upper()))
                    if val is None:
                        continue
                    rows.append(
                        {
                            "player": name,
                            "team": team,
                            "prop_type": prop_type,
                            "actual": val,
                        }
                    )
                    _bump(key, prop_type, val)

                if any(c in cat for c in _SCORING_TD_CATS):
                    td_val = _num(stat_map.get("TD"))
                    if td_val is not None:
                        td_totals[key] = td_totals.get(key, 0.0) + float(td_val)

    # Active game-roster players with no box line (dressed, 0 snaps).
    # DNP inactives get a marker row so the grader voids as ESPN_ROSTER_DNP.
    force_zero_counting: set[tuple[str, str]] = set()
    if season is not None:
        actives, dnps = _nfl_game_roster_players(summary, season=int(season))
        for name, team in actives:
            key = (name, team)
            if key not in seen_athletes:
                seen_athletes.add(key)
                force_zero_counting.add(key)
            elif key in def_adjacent_athletes and not any(
                p in (bag.get(key) or {}) for p in _DEFENSE_ZERO_PROPS
            ):
                # Fumble-only / empty defensive crumbs (e.g. Keion White).
                force_zero_counting.add(key)
        for name, team in dnps:
            rows.append(
                {
                    "player": name,
                    "team": team,
                    "prop_type": "dnp",
                    "actual": 1.0,
                }
            )

    for name, team in seen_athletes:
        key = (name, team)
        td = float(td_totals.get(key, 0.0))
        rows.append(
            {
                "player": name,
                "team": team,
                "prop_type": "player_touchdowns",
                "actual": td,
            }
        )
        rows.append(
            {
                "player": name,
                "team": team,
                "prop_type": "anytime_td",
                "actual": 1.0 if td >= 1 else 0.0,
            }
        )
        b = bag.get(key) or {}
        py = float(b.get("passing_yards") or 0.0)
        ry = float(b.get("rushing_yards") or 0.0)
        rcy = float(b.get("receiving_yards") or 0.0)
        ptd = float(b.get("passing_tds") or 0.0)
        rtd = float(b.get("rushing_tds") or 0.0)
        rectd = float(b.get("receiving_tds") or 0.0)
        rec = float(b.get("receptions") or 0.0)
        ints = float(b.get("interceptions") or 0.0)
        rush_att = float(b.get("rushing_attempts") or 0.0)
        fg_made = float(b.get("fg_made") or 0.0)
        kick_pts = float(b.get("kicking_points") or 0.0)
        # Zero-fill missing complementary offensive counting stats when the
        # player already appears in an offensive box group (WR with 0 rushes),
        # return groups only (punt returner with 0 targets), or active roster
        # with no box line (dressed skill player, 0 snaps).
        has_off = any(
            k in b
            for k in (
                "passing_yards",
                "rushing_yards",
                "receiving_yards",
                "receptions",
                "rushing_attempts",
                "pass_attempts",
                "completions",
            )
        )
        if key in return_athletes or key in force_zero_counting:
            has_off = True
        if has_off:
            for prop in _OFFENSE_ZERO_PROPS:
                if prop not in b:
                    _emit_zero(rows, bag, key, prop)
            # Refresh locals after possible zero-fills.
            ry = float((bag.get(key) or {}).get("rushing_yards") or 0.0)
            rcy = float((bag.get(key) or {}).get("receiving_yards") or 0.0)
            rec = float((bag.get(key) or {}).get("receptions") or 0.0)
            rush_att = float((bag.get(key) or {}).get("rushing_attempts") or 0.0)
            rtd = float((bag.get(key) or {}).get("rushing_tds") or 0.0)
            rectd = float((bag.get(key) or {}).get("receiving_tds") or 0.0)
            b = bag.get(key) or {}

        has_pass = any(
            k in b
            for k in ("passing_yards", "pass_attempts", "completions", "passing_tds")
        )
        if has_pass or key in force_zero_counting:
            for prop in _PASS_ZERO_PROPS:
                if prop not in b:
                    _emit_zero(rows, bag, key, prop)
            py = float((bag.get(key) or {}).get("passing_yards") or 0.0)
            ptd = float((bag.get(key) or {}).get("passing_tds") or 0.0)
            ints = float((bag.get(key) or {}).get("interceptions") or 0.0)
            b = bag.get(key) or {}

        # Defense zeros: dressed ghosts + fumble/defensive-adjacent with no tackles/sacks.
        if key in force_zero_counting or key in def_adjacent_athletes:
            for prop in _DEFENSE_ZERO_PROPS:
                if prop not in b:
                    _emit_zero(rows, bag, key, prop)
            b = bag.get(key) or {}

        if ry or rcy or ("rushing_yards" in b and "receiving_yards" in b):
            rows.append(
                {
                    "player": name,
                    "team": team,
                    "prop_type": "rush_rec_yds",
                    "actual": ry + rcy,
                }
            )
        if py or ry:
            rows.append(
                {
                    "player": name,
                    "team": team,
                    "prop_type": "pass_rush_yds",
                    "actual": py + ry,
                }
            )
        # Pass+Rush+Rec TDs (PP combo chip) — sum component TD bags.
        if any(k in b for k in ("passing_tds", "rushing_tds", "receiving_tds")) or has_off or has_pass:
            rows.append(
                {
                    "player": name,
                    "team": team,
                    "prop_type": "pass_rush_rec_tds",
                    "actual": float(ptd + rtd + rectd),
                }
            )
        # Zero-volume micros so UNDER / 0-line props grade instead of VOID.
        if "receptions" in b and rec <= 0:
            rows.append(
                {
                    "player": name,
                    "team": team,
                    "prop_type": "yards_on_first_reception",
                    "actual": 0.0,
                }
            )
            rows.append(
                {
                    "player": name,
                    "team": team,
                    "prop_type": "receiving_yards_in_first_2_receptions",
                    "actual": 0.0,
                }
            )
        if "rushing_attempts" in b and rush_att <= 0:
            rows.append(
                {
                    "player": name,
                    "team": team,
                    "prop_type": "yards_on_first_rush_attempt",
                    "actual": 0.0,
                }
            )
            rows.append(
                {
                    "player": name,
                    "team": team,
                    "prop_type": "rush_yards_in_first_5_attempts",
                    "actual": 0.0,
                }
            )
        if "fg_made" in b and fg_made <= 0:
            rows.append(
                {
                    "player": name,
                    "team": team,
                    "prop_type": "field_goal_yards",
                    "actual": 0.0,
                }
            )
            # Do NOT emit shortest/longest FG yards as 0 when there were no makes.
            # Those props are undefined without a make — grader voids as NO_FG_MADE
            # (MISS would deflate kicker ledger HR). Combo arms missing shortest
            # also void the duo rather than summing a fake 0.
        # PrizePicks-style NFL fantasy approximation (enough to grade Standard FS).
        if any(
            k in b
            for k in ("passing_yards", "rushing_yards", "receiving_yards", "receptions")
        ):
            fs = (
                0.04 * py
                + 4.0 * ptd
                - 2.0 * ints
                + 0.1 * ry
                + 6.0 * rtd
                + 0.1 * rcy
                + 1.0 * rec
                + 6.0 * rectd
            )
            rows.append(
                {
                    "player": name,
                    "team": team,
                    "prop_type": "fantasy_score",
                    "actual": round(fs, 2),
                }
            )
        elif "kicking_points" in b:
            # Kicker FS ≈ kick points on PP boards.
            rows.append(
                {
                    "player": name,
                    "team": team,
                    "prop_type": "fantasy_score",
                    "actual": kick_pts,
                }
            )
    return rows


def fetch_football_actuals(league: str, date_str: str) -> pd.DataFrame:
    league = league.lower().strip()
    if league not in LEAGUE_PATH:
        raise ValueError(f"Unknown league: {league}")
    path = LEAGUE_PATH[league]
    print(f"\n=== {league.upper()} actuals for {date_str} ===\n")
    # Evening Eastern kickoffs can sit on the next UTC scoreboard page.
    query_dates = [
        date_str,
        (date.fromisoformat(str(date_str)[:10]) + timedelta(days=1)).isoformat(),
    ]
    events = []
    seen_ids: set[str] = set()
    for qday in query_dates:
        urls = _scoreboard_urls(path, qday)
        print(f"  Scoreboard: {urls[0]}")
        payload = _espn_get_json(urls)
        if not payload:
            continue
        for event in payload.get("events") or []:
            eid = str(event.get("id") or "")
            if eid and eid in seen_ids:
                continue
            if not keep_event_on_slate(event_start_raw(event), date_str, qday):
                continue
            if eid:
                seen_ids.add(eid)
            events.append(event)

    print(f"  Events: {len(events)}")
    all_rows: list[dict] = []
    for event in events:
        state = ((event.get("status") or {}).get("type") or {}).get("state", "")
        completed = ((event.get("status") or {}).get("type") or {}).get("completed", False)
        if state != "post" and not completed:
            print(f"  Skip {event.get('shortName', '')} — not final")
            continue
        eid = str(event.get("id") or "")
        if not eid:
            continue
        print(f"  Grading: {event.get('shortName', eid)}")
        summary = _espn_get_json(_summary_urls(path, eid))
        if not summary:
            print("    ERROR: summary fetch failed")
            continue
        season = int(str(date_str)[:4]) if str(date_str)[:4].isdigit() else None
        # NFL: also zero-fill active roster ghosts (dressed, no box line).
        parsed = _parse_box(
            summary, season=season if league == "nfl" else None
        )
        micro = _parse_drives_micro(summary)
        print(f"    -> {len(parsed)} box + {len(micro)} drive-micro rows")
        all_rows.extend(parsed)
        all_rows.extend(micro)
        time.sleep(0.2)

    if not all_rows:
        return pd.DataFrame(columns=["player", "team", "prop_type", "actual"])
    df = pd.DataFrame(all_rows)
    df["actual"] = pd.to_numeric(df["actual"], errors="coerce")
    return df.dropna(subset=["actual"]).drop_duplicates(
        subset=["player", "team", "prop_type"], keep="last"
    )


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--league", required=True, choices=["nfl", "cfb"])
    ap.add_argument("--date", default="", help="YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--output", default="")
    ap.add_argument(
        "--force",
        action="store_true",
        help="Replace an existing file after a dated backup. Default is append-only.",
    )
    args = ap.parse_args()

    if not args.date:
        args.date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    if not args.output:
        args.output = str(_REPO / "outputs" / args.date / f"actuals_{args.league}_{args.date}.csv")

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    df = fetch_football_actuals(args.league, args.date)
    if df.empty:
        df = pd.DataFrame(columns=["player", "team", "prop_type", "actual"])
    how = write_actuals_csv(out, df, force=args.force)
    print(f"\n{how} -> {out}  ({len(df)} fetched rows)")
    if not df.empty and "prop_type" in df.columns:
        print(f"Prop types: {sorted(df['prop_type'].unique().tolist())}")


if __name__ == "__main__":
    main()
