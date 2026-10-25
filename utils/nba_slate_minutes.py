"""Tonight's NBA starter status + slate minutes (pre-tip).

Historical ``starter_flag`` in ``nba_minutes_role`` is season MPG / ESPN
starter *rate*. This module is the missing nightly input: ESPN depth-chart
starters, injury status, and a transparent minutes number that can haircut
B2B / questionable / OUT and bump remaining starters when a star is out.

Does not gate tickets. Attach is observe / rank context. LightGBM minutes
comes later once these labels exist on graded slates.

Sources
-------
- ESPN team depth charts — rank-1 at PG/SG/SF/PF/C = projected starter
- ESPN core roster ``starter=true`` ~T−30 — official starters while slips
  can still change. After tip the boxscore starter is grading-only.
- ESPN injuries endpoint — OUT / doubtful / questionable (step4d only kept OUT)

Each attach appends ``Sports/NBA/data/cache/nba_slate_minutes_snapshots.jsonl``
with ``as_of`` and ``pre_tip``. Train the minutes model only on ``pre_tip``.
Post-tip confirmed values leak the lineup. Scout joins ``slate_minutes``
only through ``apply_pre_tip_slate_minutes``.
"""
from __future__ import annotations

import json
import time
import unicodedata
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import requests

_REPO = Path(__file__).resolve().parents[1]
CACHE_DIR = _REPO / "Sports" / "NBA" / "data" / "cache"
SNAPSHOT_LOG = CACHE_DIR / "nba_slate_minutes_snapshots.jsonl"
CACHE_HOURS = 2.0
TIMEOUT = 12
TMINUS_MINUTES = 35
HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; PropORACLE/1.0)"}

ESPN_SCOREBOARD = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
)
ESPN_SUMMARY = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary"
)
ESPN_DEPTH = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{tid}/depthcharts"
)
ESPN_INJURIES = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries"
)
ESPN_CORE_ROSTER = (
    "https://sports.core.api.espn.com/v2/sports/basketball/leagues/nba"
    "/events/{eid}/competitions/{eid}/competitors/{tid}/roster?limit=50"
)
ESPN_ATHLETE = (
    "https://site.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{aid}"
)

# ESPN site team id → board abbr
ESPN_TEAM_ID: dict[str, str] = {
    "1": "ATL",
    "2": "BOS",
    "3": "NOP",
    "4": "CHI",
    "5": "CLE",
    "6": "DAL",
    "7": "DEN",
    "8": "DET",
    "9": "GSW",
    "10": "HOU",
    "11": "IND",
    "12": "LAC",
    "13": "LAL",
    "14": "MIA",
    "15": "MIL",
    "16": "MIN",
    "17": "BKN",
    "18": "NYK",
    "19": "ORL",
    "20": "PHI",
    "21": "PHX",
    "22": "POR",
    "23": "SAC",
    "24": "SAS",
    "25": "OKC",
    "26": "UTA",
    "27": "WAS",
    "28": "TOR",
    "29": "MEM",
    "30": "CHA",
}

_TEAM_ALIAS = {
    "BRK": "BKN",
    "BK": "BKN",
    "GS": "GSW",
    "NO": "NOP",
    "NOR": "NOP",
    "NY": "NYK",
    "SA": "SAS",
    "PHO": "PHX",
    "WSH": "WAS",
    "UTAH": "UTA",
}

OUT_STATUS = frozenset({"OUT", "O", "INACTIVE", "SUSPENDED", "IR"})
DOUBTFUL_STATUS = frozenset({"DOUBTFUL", "D"})
QUESTIONABLE_STATUS = frozenset(
    {"QUESTIONABLE", "Q", "GTD", "DAY-TO-DAY", "DAY TO DAY", "DAY_TO_DAY"}
)

SLATE_MINUTES_COLS = (
    "tonight_starter",
    "tonight_starter_source",
    "player_injury_status",
    "slate_minutes",
    "slate_minutes_lo",
    "slate_minutes_hi",
    "minutes_vs_role",
    "slate_minutes_note",
    "slate_as_of",
    "slate_pre_tip",
    "game_window",
    "tip_utc",
)

STARTER_MPG_FLOOR = 26.0
STARTER_MAX = 38.0
BENCH_CAP = 24.0
B2B_STARTER = 0.90
B2B_BENCH = 0.92
Q_HAIRCUT = 0.80
DOUBTFUL_HAIRCUT = 0.35
STAR_OUT_BUMP = 3.0


_SUFFIX = (" jr", " jr.", " sr", " sr.", " iii", " ii", " iv", " junior")


def _norm_name(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or "").strip().lower())
    t = "".join(c for c in t if not unicodedata.combining(c))
    for suf in _SUFFIX:
        if t.endswith(suf):
            t = t[: -len(suf)].strip()
    return t


def _norm_team(v: object) -> str:
    t = str(v or "").strip().upper()
    if not t or t in {"NAN", "NONE", "NULL"}:
        return ""
    return _TEAM_ALIAS.get(t, t)


def _num(v: object) -> float | None:
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x:  # NaN
        return None
    return x


def classify_injury_status(raw: object) -> str:
    s = str(raw or "").strip().upper()
    if not s or s in {"NAN", "NONE", "HEALTHY", "ACTIVE"}:
        return ""
    if s in OUT_STATUS or "OUT" in s:
        return "OUT"
    if s in DOUBTFUL_STATUS:
        return "DOUBTFUL"
    if s in QUESTIONABLE_STATUS:
        return "QUESTIONABLE"
    return s


def project_slate_minutes(
    *,
    role_mpg: float | None,
    tonight: str,
    injury: str,
    b2b: bool = False,
    teammate_star_out: bool = False,
) -> tuple[float, float, float, str]:
    """Return (minutes, lo, hi, note). Transparent — not a learned model."""
    notes: list[str] = []
    inj = classify_injury_status(injury)
    role = tonight.strip().lower()
    base = role_mpg if role_mpg is not None and role_mpg > 0 else 20.0

    if inj == "OUT" or role == "out":
        return 0.0, 0.0, 0.0, "out"

    if role in {"confirmed", "projected"}:
        minutes = max(base, STARTER_MPG_FLOOR) if base < STARTER_MPG_FLOOR else base
        notes.append(role)
    elif role == "bench":
        minutes = min(base, BENCH_CAP)
        notes.append("bench")
    else:
        minutes = base
        notes.append("unknown_role")

    if inj == "DOUBTFUL":
        minutes *= DOUBTFUL_HAIRCUT
        notes.append("doubtful")
    elif inj == "QUESTIONABLE":
        minutes *= Q_HAIRCUT
        notes.append("questionable")

    if b2b:
        minutes *= B2B_STARTER if role in {"confirmed", "projected"} else B2B_BENCH
        notes.append("b2b")

    if teammate_star_out and role in {"confirmed", "projected"} and inj != "DOUBTFUL":
        minutes = min(STARTER_MAX, minutes + STAR_OUT_BUMP)
        notes.append("star_out_bump")

    minutes = max(0.0, min(STARTER_MAX, minutes))
    lo = round(minutes * 0.85, 1)
    hi = round(min(STARTER_MAX, minutes * 1.10), 1)
    return round(minutes, 1), lo, hi, "|".join(notes)


def game_window(tip_utc: datetime | None, now: datetime | None = None) -> str:
    """early | tminus30 | live. Official NBA starters post ~30 min before tip."""
    if tip_utc is None:
        return "unknown"
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    if tip_utc.tzinfo is None:
        tip_utc = tip_utc.replace(tzinfo=timezone.utc)
    if now >= tip_utc:
        return "live"
    if now >= tip_utc - timedelta(minutes=TMINUS_MINUTES):
        return "tminus30"
    return "early"


def _parse_tip(raw: object) -> datetime | None:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _athlete_name(athlete_id: str, cache: dict[str, str]) -> str:
    if athlete_id in cache:
        return cache[athlete_id]
    data = _get_json(ESPN_ATHLETE.format(aid=athlete_id))
    nm = str(
        (data.get("athlete") or {}).get("displayName")
        or data.get("displayName")
        or ""
    ).strip()
    cache[athlete_id] = nm
    return nm


def _parse_core_roster_starters(payload: dict, name_cache: dict[str, str]) -> list[dict[str, str]]:
    """Official starters when ESPN sets starter=true (~T−30)."""
    out: list[dict[str, str]] = []
    for entry in payload.get("entries") or []:
        if not entry.get("starter"):
            continue
        aid = str(entry.get("playerId") or "").strip()
        nm = _athlete_name(aid, name_cache) if aid else ""
        if nm or aid:
            out.append({"id": aid, "name": nm})
    return out


def parse_scoreboard_games(scoreboard: dict, *, now: datetime | None = None) -> list[dict[str, Any]]:
    now = now or datetime.now(timezone.utc)
    games: list[dict[str, Any]] = []
    for ev in scoreboard.get("events") or []:
        eid = str(ev.get("id") or "")
        tip = _parse_tip(ev.get("date"))
        status = str((ev.get("status") or {}).get("type", {}).get("name") or "")
        home = away = ""
        home_id = away_id = ""
        for c in (ev.get("competitions") or [{}])[0].get("competitors") or []:
            abbr = _norm_team((c.get("team") or {}).get("abbreviation", ""))
            tid = str((c.get("team") or {}).get("id") or c.get("id") or "")
            if str(c.get("homeAway") or "") == "home":
                home, home_id = abbr, tid
            else:
                away, away_id = abbr, tid
        games.append(
            {
                "event_id": eid,
                "tip_utc": tip.isoformat() if tip else "",
                "home": home,
                "away": away,
                "home_id": home_id,
                "away_id": away_id,
                "status": status,
                "window": game_window(tip, now),
            }
        )
    return games


def _get_json(url: str, params: dict | None = None) -> dict:
    r = requests.get(url, headers=HEADERS, params=params or {}, timeout=TIMEOUT)
    if r.status_code != 200:
        return {}
    try:
        data = r.json()
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _cache_path(slate: date) -> Path:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    return CACHE_DIR / f"nba_slate_minutes_{slate.isoformat()}.json"


def _cache_fresh(path: Path) -> bool:
    if not path.is_file():
        return False
    age = datetime.now(timezone.utc).timestamp() - path.stat().st_mtime
    return age < CACHE_HOURS * 3600


def fetch_injuries() -> dict[str, dict[str, str]]:
    """team_abbr → {player_norm: status}."""
    data = _get_json(ESPN_INJURIES)
    out: dict[str, dict[str, str]] = {}
    for team_block in data.get("injuries", []) or []:
        abbr = _norm_team(team_block.get("abbreviation", ""))
        if not abbr:
            name = str(team_block.get("displayName", "")).strip().upper()
            for token, mapped in (
                ("LAKERS", "LAL"),
                ("CLIPPERS", "LAC"),
                ("KNICKS", "NYK"),
                ("NETS", "BKN"),
                ("THUNDER", "OKC"),
                ("76ERS", "PHI"),
                ("SIXERS", "PHI"),
                ("JAZZ", "UTA"),
                ("SPURS", "SAS"),
                ("WARRIORS", "GSW"),
                ("PELICANS", "NOP"),
                ("WIZARDS", "WAS"),
            ):
                if token in name:
                    abbr = mapped
                    break
        if not abbr:
            continue
        bucket = out.setdefault(abbr, {})
        for inj in team_block.get("injuries", []) or []:
            athlete = inj.get("athlete") or {}
            pname = str(athlete.get("displayName", inj.get("name", ""))).strip()
            status = classify_injury_status(inj.get("status", inj.get("type", "")))
            if pname and status:
                bucket[_norm_name(pname)] = status
    return out


def _parse_depth_starters(payload: dict) -> list[str]:
    charts = payload.get("depthchart") or []
    if not charts:
        return []
    positions = (charts[0] or {}).get("positions") or {}
    names: list[str] = []
    for pos in ("pg", "sg", "sf", "pf", "c"):
        athletes = (positions.get(pos) or {}).get("athletes") or []
        if not athletes:
            continue
        nm = str((athletes[0] or {}).get("displayName") or "").strip()
        if nm:
            names.append(nm)
    return names


def _parse_confirmed_starters(summary: dict) -> dict[str, list[str]]:
    """team_abbr → starter display names from a tipped boxscore."""
    by_team: dict[str, list[str]] = {}
    box = summary.get("boxscore") or {}
    for tblock in box.get("players") or []:
        team = _norm_team((tblock.get("team") or {}).get("abbreviation", ""))
        if not team:
            continue
        names: list[str] = []
        for stat_group in tblock.get("statistics") or []:
            for ath in stat_group.get("athletes") or []:
                if not ath.get("starter"):
                    continue
                nm = str((ath.get("athlete") or {}).get("displayName") or ath.get("displayName") or "").strip()
                if nm:
                    names.append(nm)
        if names:
            by_team[team] = names
    return by_team


def fetch_event_confirmed(
    game: dict[str, Any],
    *,
    name_cache: dict[str, str] | None = None,
) -> dict[str, list[str]]:
    """Official starters for one game. Empty until ESPN posts (~T−30)."""
    name_cache = name_cache if name_cache is not None else {}
    confirmed: dict[str, list[str]] = {}
    eid = str(game.get("event_id") or "")
    if not eid:
        return confirmed
    window = str(game.get("window") or "")
    if window == "early":
        return confirmed
    for abbr, tid in ((game.get("home"), game.get("home_id")), (game.get("away"), game.get("away_id"))):
        if not abbr or not tid:
            continue
        time.sleep(0.2)
        payload = _get_json(ESPN_CORE_ROSTER.format(eid=eid, tid=tid))
        rows = _parse_core_roster_starters(payload, name_cache)
        names = [r["name"] for r in rows if r.get("name")]
        if len(names) >= 4:
            confirmed[_norm_team(abbr)] = names
    if confirmed:
        return confirmed
    # After tip the boxscore starter flag is the grading source.
    time.sleep(0.2)
    summary = _get_json(ESPN_SUMMARY, {"event": eid})
    for team, names in _parse_confirmed_starters(summary).items():
        if names:
            confirmed[team] = names
    return confirmed


def fetch_slate_snapshot(
    slate: date,
    *,
    teams: list[str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Live ESPN snapshot for one slate date."""
    now = now or datetime.now(timezone.utc)
    want = {_norm_team(t) for t in (teams or []) if _norm_team(t)}
    scoreboard = _get_json(ESPN_SCOREBOARD, {"dates": slate.strftime("%Y%m%d")})
    games = parse_scoreboard_games(scoreboard, now=now)
    slate_teams: set[str] = set()
    for g in games:
        if g.get("home"):
            slate_teams.add(g["home"])
        if g.get("away"):
            slate_teams.add(g["away"])
    yday = _get_json(ESPN_SCOREBOARD, {"dates": (slate - timedelta(days=1)).strftime("%Y%m%d")})
    yday_teams: set[str] = set()
    for ev in yday.get("events") or []:
        for comp in (ev.get("competitions") or [{}])[0].get("competitors") or []:
            abbr = _norm_team((comp.get("team") or {}).get("abbreviation", ""))
            if abbr:
                yday_teams.add(abbr)
    if want:
        slate_teams = {t for t in slate_teams if t in want} or slate_teams
        games = [g for g in games if g.get("home") in want or g.get("away") in want] or games

    confirmed: dict[str, list[str]] = {}
    name_cache: dict[str, str] = {}
    for g in games:
        if g.get("window") == "early":
            continue
        for team, names in fetch_event_confirmed(g, name_cache=name_cache).items():
            if not want or team in want:
                confirmed[team] = names

    projected: dict[str, list[str]] = {}
    id_by_abbr = {v: k for k, v in ESPN_TEAM_ID.items()}
    for abbr in sorted(slate_teams or want):
        tid = id_by_abbr.get(abbr)
        if not tid:
            continue
        time.sleep(0.25)
        payload = _get_json(ESPN_DEPTH.format(tid=tid))
        names = _parse_depth_starters(payload)
        if names:
            projected[abbr] = names

    injuries = fetch_injuries()
    if want:
        injuries = {t: v for t, v in injuries.items() if t in want}

    return {
        "slate": slate.isoformat(),
        "fetched_at": now.isoformat(),
        "confirmed": confirmed,
        "projected": projected,
        "injuries": injuries,
        "slate_teams": sorted(slate_teams),
        "b2b_teams": sorted(slate_teams & yday_teams),
        "games": games,
    }


def load_or_fetch_snapshot(
    slate: date,
    *,
    teams: list[str] | None = None,
    refresh: bool = False,
) -> dict[str, Any]:
    path = _cache_path(slate)
    if not refresh and _cache_fresh(path):
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            pass
    snap = fetch_slate_snapshot(slate, teams=teams)
    path.write_text(json.dumps(snap, indent=2, sort_keys=True), encoding="utf-8")
    return snap


def _team_tip(snap: dict[str, Any], team: str) -> datetime | None:
    tm = _norm_team(team)
    for g in snap.get("games") or []:
        if tm in {_norm_team(g.get("home")), _norm_team(g.get("away"))}:
            return _parse_tip(g.get("tip_utc"))
    return None


def append_snapshot_log(
    df: pd.DataFrame,
    snap: dict[str, Any],
    *,
    slate: date,
    as_of: datetime | None = None,
) -> int:
    """Append one row per player. Train later only where slate_pre_tip is true."""
    as_of = as_of or datetime.now(timezone.utc)
    if as_of.tzinfo is None:
        as_of = as_of.replace(tzinfo=timezone.utc)
    player_col = "player" if "player" in df.columns else ("Player" if "Player" in df.columns else None)
    team_col = "team" if "team" in df.columns else ("Team" if "Team" in df.columns else None)
    if player_col is None or team_col is None:
        return 0
    SNAPSHOT_LOG.parent.mkdir(parents=True, exist_ok=True)
    seen: set[tuple[str, str]] = set()
    n = 0
    with SNAPSHOT_LOG.open("a", encoding="utf-8") as fh:
        for _, row in df.iterrows():
            key = (_norm_name(row.get(player_col)), _norm_team(row.get(team_col)))
            if not key[0] or key in seen:
                continue
            seen.add(key)
            tip = _team_tip(snap, key[1])
            pre_tip = bool(tip is not None and as_of < tip)
            rec = {
                "as_of": as_of.isoformat(),
                "slate": slate.isoformat(),
                "pre_tip": pre_tip,
                "player": str(row.get(player_col) or ""),
                "player_norm": key[0],
                "team": key[1],
                "tonight_starter": str(row.get("tonight_starter") or ""),
                "tonight_starter_source": str(row.get("tonight_starter_source") or ""),
                "player_injury_status": str(row.get("player_injury_status") or ""),
                "slate_minutes": _num(row.get("slate_minutes")),
                "slate_minutes_lo": _num(row.get("slate_minutes_lo")),
                "slate_minutes_hi": _num(row.get("slate_minutes_hi")),
                "tip_utc": tip.isoformat() if tip else "",
                "game_window": game_window(tip, as_of),
            }
            fh.write(json.dumps(rec, sort_keys=True) + "\n")
            n += 1
    return n


def load_pre_tip_lookup(path: Path | None = None) -> dict[tuple[str, str, str], dict[str, Any]]:
    """Latest pre-tip snapshot per (slate, team, player_norm). Post-tip rows dropped."""
    src = path or SNAPSHOT_LOG
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    if not src.is_file():
        return out
    with src.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not rec.get("pre_tip"):
                continue
            key = (
                str(rec.get("slate") or "")[:10],
                _norm_team(rec.get("team")),
                _norm_name(rec.get("player_norm") or rec.get("player")),
            )
            if not key[0] or not key[2]:
                continue
            prev = out.get(key)
            if prev is None or str(rec.get("as_of") or "") >= str(prev.get("as_of") or ""):
                out[key] = rec
    return out


def apply_pre_tip_slate_minutes(df: pd.DataFrame, lookup: dict[tuple[str, str, str], dict[str, Any]] | None = None) -> pd.DataFrame:
    """Rescale proj_mu / P(over) only when a pre-tip snapshot exists.

    Post-tip confirmed minutes are never applied — that would leak the lineup.
    """
    out = df.copy()
    out["slate_minutes_used"] = 0
    if lookup is None:
        lookup = load_pre_tip_lookup()
    if not lookup:
        return out
    try:
        from utils.stat_distributions import line_probs
    except Exception:
        return out

    date_col = next((c for c in ("file_date", "game_date", "board_date") if c in out.columns), None)
    player_col = next((c for c in ("player", "player_name", "Player") if c in out.columns), None)
    team_col = next((c for c in ("team", "Team") if c in out.columns), None)
    if not date_col or not player_col:
        return out

    dirs = (
        out["direction"].astype(str).str.upper()
        if "direction" in out.columns
        else pd.Series("OVER", index=out.index)
    )
    for idx, row in out.iterrows():
        key = (
            str(row.get(date_col) or "")[:10],
            _norm_team(row.get(team_col)) if team_col else "",
            _norm_name(row.get(player_col)),
        )
        rec = lookup.get(key)
        if rec is None:
            matches = [v for k, v in lookup.items() if k[0] == key[0] and k[2] == key[2]]
            rec = matches[0] if len(matches) == 1 else None
        if rec is None:
            continue
        slate_m = _num(rec.get("slate_minutes"))
        status = str(rec.get("tonight_starter") or "")
        inj = str(rec.get("player_injury_status") or "")
        if status == "out" or inj == "OUT" or slate_m == 0:
            out.at[idx, "proj_hold"] = 1.0
            out.at[idx, "slate_minutes_used"] = 1
            out.at[idx, "slate_minutes"] = 0.0
            continue
        if slate_m is None or slate_m <= 0:
            continue
        proj_m = _num(row.get("proj_minutes"))
        mu = _num(row.get("proj_mu"))
        if proj_m is None or proj_m <= 0 or mu is None:
            continue
        new_mu = mu * (slate_m / proj_m)
        try:
            line_f = float(row.get("line"))
        except (TypeError, ValueError):
            continue
        var = _num(row.get("proj_var"))
        if var is None:
            var = max(new_mu, 1.0)
        p_over, p_under, p_push, _ = line_probs(new_mu, line_f, var=var, n=_num(row.get("proj_n")) or 10)
        play = p_under if str(dirs.loc[idx]).upper() == "UNDER" else p_over
        br = _num(row.get("base_rate")) or 0.5
        out.at[idx, "proj_mu"] = new_mu
        out.at[idx, "proj_minutes"] = slate_m
        out.at[idx, "proj_p_over"] = p_over
        out.at[idx, "proj_p_under"] = p_under
        out.at[idx, "proj_p_push"] = p_push
        out.at[idx, "proj_p_play"] = play
        out.at[idx, "proj_edge"] = play - br
        out.at[idx, "slate_minutes_used"] = 1
        out.at[idx, "slate_minutes"] = slate_m
    return out


def _lookup_status(snap: dict[str, Any], team: str, player: str) -> tuple[str, str, str]:
    """Return (tonight_starter, source, injury)."""
    pn = _norm_name(player)
    tm = _norm_team(team)
    inj = ((snap.get("injuries") or {}).get(tm) or {}).get(pn, "")
    confirmed = {_norm_name(n) for n in (snap.get("confirmed") or {}).get(tm) or []}
    projected = {_norm_name(n) for n in (snap.get("projected") or {}).get(tm) or []}
    if inj == "OUT":
        return "out", "espn_injury", inj
    if pn in confirmed:
        return "confirmed", "espn_lineup", inj
    if pn in projected:
        return "projected", "espn_depth", inj
    if projected:
        return "bench", "espn_depth", inj
    return "unknown", "none", inj


def attach_slate_minutes_columns(
    df: pd.DataFrame,
    *,
    as_of: date | None = None,
    snapshot: dict[str, Any] | None = None,
    refresh: bool = False,
    write_log: bool = True,
) -> pd.DataFrame:
    """Add tonight_starter / slate_minutes next to historical starter_flag."""
    out = df.copy()
    for col in SLATE_MINUTES_COLS:
        if col not in out.columns:
            out[col] = pd.NA

    player_col = "player" if "player" in out.columns else ("Player" if "Player" in out.columns else None)
    team_col = "team" if "team" in out.columns else ("Team" if "Team" in out.columns else None)
    if player_col is None or team_col is None:
        return out

    if isinstance(as_of, datetime):
        slate = as_of.date()
    elif isinstance(as_of, date):
        slate = as_of
    elif as_of:
        try:
            slate = date.fromisoformat(str(as_of)[:10])
        except ValueError:
            slate = date.today()
    else:
        slate = date.today()
    teams = sorted({_norm_team(t) for t in out[team_col].dropna().astype(str) if _norm_team(t)})
    snap = snapshot if snapshot is not None else load_or_fetch_snapshot(
        slate, teams=teams, refresh=refresh
    )

    star_out_teams = set()
    for tm, bucket in (snap.get("injuries") or {}).items():
        if any(st == "OUT" for st in bucket.values()):
            # Usage of the OUT player is not always here; treat any OUT on a
            # depth-chart starter as a star-out candidate.
            proj = {_norm_name(n) for n in (snap.get("projected") or {}).get(tm) or []}
            conf = {_norm_name(n) for n in (snap.get("confirmed") or {}).get(tm) or []}
            if any(pn in proj or pn in conf for pn, st in bucket.items() if st == "OUT"):
                star_out_teams.add(tm)

    b2b_col = next((c for c in ("b2b_flag", "B2B", "is_back_to_back") if c in out.columns), None)
    role_col = next((c for c in ("role_mpg", "min_season_avg", "min_last5_avg") if c in out.columns), None)
    hist_col = "starter_flag" if "starter_flag" in out.columns else None

    tonight: list[str] = []
    source: list[str] = []
    injury: list[str] = []
    mins: list[Any] = []
    lo_s: list[Any] = []
    hi_s: list[Any] = []
    vs: list[Any] = []
    notes: list[str] = []

    for _, row in out.iterrows():
        tm = _norm_team(row.get(team_col))
        role, src, inj = _lookup_status(snap, tm, row.get(player_col))
        if role == "unknown" and hist_col:
            hist = str(row.get(hist_col) or "").strip().lower()
            if hist in {"starter", "bench"}:
                role = "projected" if hist == "starter" else "bench"
                src = "historical"
        role_mpg = _num(row.get(role_col)) if role_col else None
        if role_mpg is None:
            role_mpg = _num(row.get("min_last5_avg"))
        b2b_raw = row.get(b2b_col) if b2b_col else False
        try:
            b2b = bool(b2b_raw) and str(b2b_raw).strip().lower() not in {"0", "false", "nan", ""}
        except (TypeError, ValueError):
            b2b = False
        if not b2b and tm in set(snap.get("b2b_teams") or []):
            b2b = True
        star = tm in star_out_teams
        m, lo, hi, note = project_slate_minutes(
            role_mpg=role_mpg,
            tonight=role,
            injury=inj,
            b2b=b2b,
            teammate_star_out=star,
        )
        tonight.append(role)
        source.append(src)
        injury.append(inj or "healthy")
        mins.append(m)
        lo_s.append(lo)
        hi_s.append(hi)
        vs.append(round(m - role_mpg, 1) if role_mpg is not None else pd.NA)
        notes.append(note)

    as_of_now = datetime.now(timezone.utc)
    fetched = _parse_tip(snap.get("fetched_at")) or as_of_now
    tips: list[str] = []
    windows: list[str] = []
    pre_flags: list[bool] = []
    for _, row in out.iterrows():
        tip = _team_tip(snap, row.get(team_col))
        tips.append(tip.isoformat() if tip else "")
        windows.append(game_window(tip, fetched))
        pre_flags.append(bool(tip is not None and fetched < tip))

    out["tonight_starter"] = tonight
    out["tonight_starter_source"] = source
    out["player_injury_status"] = injury
    out["slate_minutes"] = mins
    out["slate_minutes_lo"] = lo_s
    out["slate_minutes_hi"] = hi_s
    out["minutes_vs_role"] = vs
    out["slate_minutes_note"] = notes
    out["slate_as_of"] = fetched.isoformat()
    out["slate_pre_tip"] = pre_flags
    out["game_window"] = windows
    out["tip_utc"] = tips
    if write_log:
        try:
            append_snapshot_log(out, snap, slate=slate, as_of=fetched)
        except OSError:
            pass
    return out
