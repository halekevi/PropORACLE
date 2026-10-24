"""Publish-time gate: every ticketed game must be on that day's ESPN scoreboard.

Load-path date filters miss leftover boards whose game_time is only ``8:00 PM``.
This runs at write-web / publish: if the pairing is not on the ESPN schedule
for the slate date, the slip is blocked and the leg is logged.

Fail closed: missing team/opp, unresolvable sport, empty scoreboard, or fetch
error all block. ``PROPORACLE_SKIP_SCHEDULE_GATE=1`` disables (emergency only).
"""

from __future__ import annotations

import json
import os
import re
import sys
import urllib.error
import urllib.request
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable, Iterable

from pathlib import Path

from utils.eastern_slate_date import ET, eastern_slate_date, event_start_raw, keep_event_on_slate

_REPO = Path(__file__).resolve().parents[1]

# Family key → ESPN site.v2 sports path (no leading slash).
_TEAM_SPORT_PATH: dict[str, str] = {
    "NBA": "basketball/nba",
    "NBA1Q": "basketball/nba",
    "NBA1H": "basketball/nba",
    "WNBA": "basketball/wnba",
    "WNBA1Q": "basketball/wnba",
    "WNBA1H": "basketball/wnba",
    "NHL": "hockey/nhl",
    "MLB": "baseball/mlb",
    "NFL": "football/nfl",
    "NFLP": "football/nfl",
    "CFB": "football/college-football",
    "CFB1H": "football/college-football",
    "CBB": "basketball/mens-college-basketball",
    "WCBB": "basketball/womens-college-basketball",
}

_TENNIS = frozenset({"TENNIS"})
_GOLF = frozenset({"GOLF"})
_SOCCER = frozenset({"SOCCER", "SOC"})
_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def sport_family(sport: object) -> str:
    return str(sport or "").strip().upper()


def _sport_canon_key(sport: str) -> str:
    sp = sport_family(sport)
    if sp in {"NBA", "NBA1Q", "NBA1H"}:
        return "NBA"
    if sp in {"WNBA", "WNBA1Q", "WNBA1H"}:
        return "WNBA"
    if sp in _SOCCER:
        return "SOCCER"
    if sp in {"NFLP"}:
        return "NFL"
    if sp in {"CFB1H"}:
        return "CFB"
    return sp


_SOCCER_ALIAS_CACHE: dict[str, str] | None = None


def _soccer_alias_map() -> dict[str, str]:
    global _SOCCER_ALIAS_CACHE
    if _SOCCER_ALIAS_CACHE is not None:
        return _SOCCER_ALIAS_CACHE
    out: dict[str, str] = {}
    csv_path = _REPO / "Sports" / "Soccer" / "data" / "soccer_team_aliases.csv"
    if csv_path.is_file():
        try:
            import csv

            with csv_path.open(newline="", encoding="utf-8-sig") as fh:
                for rec in csv.DictReader(fh):
                    src = str(rec.get("pp_name") or "").strip().upper()
                    dst = str(rec.get("canonical_name") or "").strip().upper()
                    if src and dst:
                        out[src] = dst
        except Exception:
            pass
    try:
        scripts = str(_REPO / "scripts")
        if scripts not in sys.path:
            sys.path.insert(0, scripts)
        from soccer_grader_advanced import _SOCCER_TEAM_ALIASES  # type: ignore

        for k, v in (_SOCCER_TEAM_ALIASES or {}).items():
            out.setdefault(str(k).strip().upper(), str(v).strip().upper())
    except Exception:
        pass
    _SOCCER_ALIAS_CACHE = out
    return out


def _fold_team_token(raw: object) -> str:
    s = str(raw or "").strip()
    if not s or s.upper() in {"NAN", "NONE", "NAT", "TBD", "NULL"}:
        return ""
    s = s.replace("@", " ").replace("VS.", " ").replace("VS", " ")
    return " ".join(s.split())


def canon_team(raw: object, sport: str = "") -> str:
    """Fold ESPN / PrizePicks team tokens through the existing sport canon map."""
    tok = _fold_team_token(raw)
    if not tok:
        return ""
    if "/" in tok or " + " in tok or "+" in tok:
        tok = re.split(r"\s*\+\s*|\s*/\s*", tok, maxsplit=1)[0].strip()
        if not tok:
            return ""
    sp = _sport_canon_key(sport)
    if sp == "SOCCER":
        key = tok.upper()
        return _soccer_alias_map().get(key, key)
    try:
        from espn_injuries import canon_team_abbr

        mapped = canon_team_abbr(sp if sp else "NBA", tok)
        if mapped:
            return mapped
    except Exception:
        pass
    try:
        from utils.roster_change import _NBA_TEAM_ALIASES, norm_nba_team

        t = norm_nba_team(tok)
        if t:
            return t
        u = tok.upper()
        return _NBA_TEAM_ALIASES.get(u, u)
    except Exception:
        return tok.upper()


def combo_team_tokens(raw: object, sport: str = "") -> list[str]:
    """DET/BOS or Caufield-style team combos → two (or more) canon tokens."""
    s = _fold_team_token(raw)
    if not s:
        return []
    if "/" in s or re.search(r"\s\+\s", s) or (s.count("+") == 1 and " " not in s):
        parts = [p.strip() for p in re.split(r"\s*\+\s*|\s*/\s*", s) if p.strip()]
        out = [canon_team(p, sport) for p in parts]
        return [t for t in out if t]
    t = canon_team(s, sport)
    return [t] if t else []


def _fold_player(raw: object) -> str:
    try:
        from player_name_norm import fold_player_name

        return fold_player_name(raw)
    except Exception:
        return " ".join(str(raw or "").strip().lower().split())


def _leg_team_opp(leg: dict[str, Any], sport: str = "") -> tuple[str, str]:
    sp = sport or sport_family(leg.get("sport") or "")
    raw_team = leg.get("team") or leg.get("Team") or ""
    raw_opp = (
        leg.get("opp")
        or leg.get("opp_team")
        or leg.get("board_opponent")
        or leg.get("opponent")
        or leg.get("Opp")
        or ""
    )
    team_toks = combo_team_tokens(raw_team, sp)
    opp_toks = combo_team_tokens(raw_opp, sp)
    # Duo / two-team face (DET/BOS): the combo *is* the pairing.
    if len(team_toks) >= 2 and not opp_toks:
        return team_toks[0], team_toks[1]
    team = team_toks[0] if team_toks else ""
    opp = opp_toks[0] if opp_toks else ""
    matchup = str(leg.get("matchup") or "")
    if (not team or not opp) and matchup:
        s = matchup.split("(")[0].strip()
        parts = re.split(r"\s+vs\.?\s+|\s+@\s+|@", s, flags=re.I)
        if len(parts) == 2:
            a, b = canon_team(parts[0], sp), canon_team(parts[1], sp)
            team = team or a
            opp = opp or b
    return team, opp


def _competitor_tokens(comp: dict[str, Any], sport: str) -> set[str]:
    team = comp.get("team") if isinstance(comp.get("team"), dict) else {}
    out: set[str] = set()
    for raw in (
        team.get("abbreviation"),
        comp.get("abbreviation"),
        team.get("shortDisplayName"),
        team.get("displayName"),
        team.get("name"),
        team.get("location"),
    ):
        for tok in combo_team_tokens(raw, sport) or ([canon_team(raw, sport)] if raw else []):
            if tok:
                out.add(tok)
    return out


def _parse_event_start(raw: object) -> datetime | None:
    s = str(raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    text = s.replace("Z", "+00:00")
    if re.match(r"^\d{4}-\d{2}-\d{2} ", text):
        text = text.replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(text[:32])
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def parse_loose_game_time(raw: object, slate_date: str = "") -> datetime | None:
    """Parse a ticket/board game_time (ISO, ``7:00 PM``, or ``10/22 12:45 PM``)."""
    s = str(raw or "").strip()
    if not s or s.lower() in {"nan", "none", "null"}:
        return None
    iso = _parse_event_start(s)
    if iso is not None:
        return iso
    ds = str(slate_date or "")[:10]
    m = re.match(r"^(\d{1,2}:\d{2}\s*[AP]M)$", s, flags=re.I)
    if m and _DATE_RE.fullmatch(ds):
        try:
            dt = datetime.strptime(f"{ds} {m.group(1).upper()}", "%Y-%m-%d %I:%M %p")
            return dt.replace(tzinfo=ET)
        except ValueError:
            pass
    m = re.match(r"^(\d{1,2})/(\d{1,2})\s+(\d{1,2}:\d{2}\s*[AP]M)$", s, flags=re.I)
    if m:
        year = int(ds[:4]) if _DATE_RE.fullmatch(ds) else datetime.now(ET).year
        try:
            dt = datetime.strptime(
                f"{year}-{int(m.group(1)):02d}-{int(m.group(2)):02d} {m.group(3).upper()}",
                "%Y-%m-%d %I:%M %p",
            )
            return dt.replace(tzinfo=ET)
        except ValueError:
            pass
    return None


def _aware_now(now: datetime | None = None) -> datetime:
    if now is None:
        return datetime.now(timezone.utc)
    if now.tzinfo is None:
        return now.replace(tzinfo=ET)
    return now


def _event_pair(event: dict[str, Any], sport: str = "") -> frozenset[str] | None:
    """All canon tokens for both sides (abbr + display names) so SAS/SA match."""
    comps = event.get("competitions") or []
    if not comps or not isinstance(comps[0], dict):
        return None
    sides: list[set[str]] = []
    for c in comps[0].get("competitors") or []:
        if not isinstance(c, dict):
            continue
        toks = _competitor_tokens(c, sport)
        if toks:
            sides.append(toks)
    if len(sides) < 2:
        return None
    return frozenset().union(*sides[:2])


def _event_players(event: dict[str, Any]) -> set[str]:
    names: set[str] = set()
    comps_iter: list[dict[str, Any]] = []
    groupings = event.get("groupings") or []
    if groupings:
        for grp in groupings:
            if isinstance(grp, dict):
                comps_iter.extend(x for x in (grp.get("competitions") or []) if isinstance(x, dict))
    else:
        comps_iter = [x for x in (event.get("competitions") or []) if isinstance(x, dict)]
    for comp in comps_iter:
        for c in comp.get("competitors") or []:
            if not isinstance(c, dict):
                continue
            athlete = c.get("athlete") if isinstance(c.get("athlete"), dict) else {}
            raw = athlete.get("displayName") or c.get("displayName") or ""
            for nm in _combo_player_names(raw):
                if nm:
                    names.add(nm)
            folded = _fold_player(raw)
            if folded:
                names.add(folded)
    return names


def _name_on_board(nm: str, board: set[str]) -> bool:
    if not nm:
        return False
    if nm in board:
        return True
    last = nm.split()[-1] if nm.split() else ""
    if not last:
        return False
    hits = [b for b in board if (b.split()[-1] if b.split() else "") == last]
    return len(hits) == 1


def _combo_player_names(raw: object) -> list[str]:
    """Split doubles / combo legs ('Arevalo / Pavic', 'Caufield + Slafkovský')."""
    s = str(raw or "").strip()
    if not s:
        return []
    if re.search(r"\s\+\s|\s/\s|/", s):
        parts = [p.strip() for p in re.split(r"\s*\+\s*|\s*/\s*", s) if p.strip()]
        return [n for n in (_fold_player(p) for p in parts) if n]
    n = _fold_player(s)
    return [n] if n else []


def _http_json(url: str, timeout: int = 20) -> dict[str, Any]:
    req = urllib.request.Request(url, headers={"User-Agent": _UA, "Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8", errors="replace") or "{}")


def fetch_espn_events(path: str, day: str) -> list[dict[str, Any]]:
    """One ESPN scoreboard page (site.api, then site.web.api)."""
    ymd = str(day or "").replace("-", "")
    suffix = f"{path}/scoreboard?dates={ymd}&limit=100"
    urls = (
        f"https://site.api.espn.com/apis/site/v2/sports/{suffix}",
        f"https://site.web.api.espn.com/apis/site/v2/sports/{suffix}",
    )
    last: Exception | None = None
    for url in urls:
        try:
            data = _http_json(url)
            events = data.get("events") or []
            return [e for e in events if isinstance(e, dict)]
        except (urllib.error.URLError, TimeoutError, json.JSONDecodeError, OSError, ValueError) as exc:
            last = exc
            continue
    if last:
        raise last
    return []


def _queried_days(slate: str, sport: str) -> list[str]:
    """Slate day plus the next UTC page (evening ET tips). Tennis keeps +2."""
    ds = str(slate or "")[:10]
    if not _DATE_RE.fullmatch(ds):
        return []
    d0 = date.fromisoformat(ds)
    extra = 2 if sport in _TENNIS else 1
    return [(d0 + timedelta(days=i)).isoformat() for i in range(extra + 1)]


def events_on_slate(
    events: Iterable[dict[str, Any]],
    slate: str,
    queried_day: str = "",
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ev in events:
        if not isinstance(ev, dict):
            continue
        if keep_event_on_slate(event_start_raw(ev), slate, queried_day or eastern_slate_date(event_start_raw(ev))):
            out.append(ev)
    return out


FetchFn = Callable[[str, str], list[dict[str, Any]]]


def _remember_start(
    starts: dict[frozenset[str], datetime], pair: frozenset[str], event: dict[str, Any]
) -> None:
    st = _parse_event_start(event_start_raw(event))
    if st is None:
        return
    prev = starts.get(pair)
    if prev is None or st < prev:
        starts[pair] = st


def load_team_board(
    sport: str,
    slate: str,
    *,
    fetch: FetchFn | None = None,
) -> tuple[set[frozenset[str]], dict[frozenset[str], datetime], str]:
    """Pairings and earliest ESPN start on the Eastern slate."""
    fetch = fetch or fetch_espn_events
    path = _TEAM_SPORT_PATH.get(sport)
    if not path:
        return set(), {}, "unmapped_sport"
    pairs: set[frozenset[str]] = set()
    starts: dict[frozenset[str], datetime] = {}
    err = ""
    for qday in _queried_days(slate, sport):
        try:
            raw = fetch(path, qday)
        except Exception as exc:
            err = f"fetch_failed:{exc}"
            continue
        for ev in events_on_slate(raw, slate, qday):
            pair = _event_pair(ev, sport)
            if pair:
                pairs.add(pair)
                _remember_start(starts, pair, ev)
    if not pairs and err:
        return set(), starts, err
    return pairs, starts, err


def load_team_games(
    sport: str,
    slate: str,
    *,
    fetch: FetchFn | None = None,
) -> tuple[set[frozenset[str]], str]:
    """Return pairings on the Eastern slate, plus an error tag (empty if ok)."""
    pairs, _starts, err = load_team_board(sport, slate, fetch=fetch)
    return pairs, err


def _soccer_league_slugs() -> list[str]:
    scripts = os.path.join(os.path.dirname(__file__), "..", "scripts")
    if scripts not in sys.path:
        sys.path.insert(0, os.path.abspath(scripts))
    try:
        from fetch_actuals import SOCCER_LEAGUES  # type: ignore

        return [str(slug) for slug, _name in SOCCER_LEAGUES]
    except Exception:
        return ["eng.1", "usa.1", "esp.1", "ger.1", "ita.1", "fra.1", "mex.1"]


def load_soccer_board(
    slate: str,
    *,
    fetch: FetchFn | None = None,
) -> tuple[set[frozenset[str]], dict[frozenset[str], datetime], str]:
    fetch = fetch or fetch_espn_events
    pairs: set[frozenset[str]] = set()
    starts: dict[frozenset[str], datetime] = {}
    err = ""
    for slug in _soccer_league_slugs():
        path = f"soccer/{slug}"
        for qday in _queried_days(slate, "SOCCER"):
            try:
                raw = fetch(path, qday)
            except Exception as exc:
                err = f"fetch_failed:{exc}"
                continue
            for ev in events_on_slate(raw, slate, qday):
                pair = _event_pair(ev, "SOCCER")
                if pair:
                    pairs.add(pair)
                    _remember_start(starts, pair, ev)
    if not pairs and err:
        return set(), starts, err
    return pairs, starts, err


def load_soccer_games(
    slate: str,
    *,
    fetch: FetchFn | None = None,
) -> tuple[set[frozenset[str]], str]:
    pairs, _starts, err = load_soccer_board(slate, fetch=fetch)
    return pairs, err


def load_name_board_with_starts(
    sport: str,
    slate: str,
    *,
    fetch: FetchFn | None = None,
) -> tuple[set[str], dict[str, datetime], str]:
    """Tennis / golf: folded player names and earliest ESPN start."""
    fetch = fetch or fetch_espn_events
    names: set[str] = set()
    starts: dict[str, datetime] = {}
    err = ""
    if sport in _TENNIS:
        paths = ("tennis/atp", "tennis/wta")
    elif sport in _GOLF:
        paths = ("golf/pga",)
    else:
        return set(), {}, "unmapped_sport"
    allow = set(_queried_days(slate, sport))
    for path in paths:
        for qday in allow:
            try:
                raw = fetch(path, qday)
            except Exception as exc:
                err = f"fetch_failed:{exc}"
                continue
            for ev in raw:
                if not isinstance(ev, dict):
                    continue
                et = eastern_slate_date(event_start_raw(ev))
                if sport in _TENNIS:
                    if et and et not in allow:
                        continue
                    if not et and qday not in allow:
                        continue
                players = _event_players(ev)
                names |= players
                st = _parse_event_start(event_start_raw(ev))
                if st is None:
                    continue
                for nm in players:
                    prev = starts.get(nm)
                    if prev is None or st < prev:
                        starts[nm] = st
    if not names and err:
        return set(), starts, err
    return names, starts, err


def load_name_board(
    sport: str,
    slate: str,
    *,
    fetch: FetchFn | None = None,
) -> tuple[set[str], str]:
    """Tennis / golf: folded player names on the scoreboard."""
    names, _starts, err = load_name_board_with_starts(sport, slate, fetch=fetch)
    return names, err


def leg_schedule_reason(
    leg: dict[str, Any],
    *,
    slate: str,
    group_sport: str = "",
    team_games: dict[str, tuple[set[frozenset[str]], str]] | None = None,
    name_boards: dict[str, tuple[set[str], str]] | None = None,
) -> str | None:
    """None if the game is on the ESPN slate; else a short block reason."""
    sport = sport_family(leg.get("sport") or group_sport)
    if not sport:
        return "no_sport"
    team_games = team_games or {}
    name_boards = name_boards or {}
    if sport in _TENNIS or sport in _GOLF:
        board, err = name_boards.get(sport, (set(), "missing_board"))
        player_names = _combo_player_names(leg.get("player"))
        opp_names = _combo_player_names(
            leg.get("opp")
            or leg.get("board_opponent")
            or leg.get("opponent")
            or ""
        )
        if err and not board:
            return err
        if not player_names:
            return "no_player"
        missing = [n for n in player_names if not _name_on_board(n, board)]
        if missing:
            return f"player_not_on_scoreboard:{missing[0]}"
        missing_opp = [n for n in opp_names if not _name_on_board(n, board)]
        if missing_opp:
            return f"opponent_not_on_scoreboard:{missing_opp[0]}"
        return None
    if sport in _SOCCER:
        key = "SOCCER"
        games, err = team_games.get(key, (set(), "missing_board"))
    elif sport in _TEAM_SPORT_PATH:
        key = sport
        games, err = team_games.get(sport, (set(), "missing_board"))
        if not games and sport in {"NBA1Q", "NBA1H"}:
            games, err = team_games.get("NBA", (games, err))
        if not games and sport in {"WNBA1Q", "WNBA1H"}:
            games, err = team_games.get("WNBA", (games, err))
    else:
        return "unmapped_sport"
    if err and not games:
        return err
    if not games:
        return "empty_scoreboard"
    team, opp = _leg_team_opp(leg, sport)
    if not team:
        return "no_team"
    if opp and any(team in g and opp in g for g in games):
        return None
    combo = combo_team_tokens(leg.get("team") or leg.get("Team") or "", sport)
    if len(combo) >= 2 and any(all(t in g for t in combo[:2]) for g in games):
        return None
    if not opp and any(team in g for g in games):
        return "no_opp"
    if any(team in g for g in games) and opp:
        return f"matchup_not_on_scoreboard:{team}@{opp}"
    return f"team_not_on_scoreboard:{team}"


def _pair_start(
    team: str,
    opp: str,
    starts: dict[frozenset[str], datetime],
) -> datetime | None:
    for pair, dt in starts.items():
        if team and team in pair and (not opp or opp in pair):
            return dt
    return None


def leg_start_dt(
    leg: dict[str, Any],
    *,
    slate: str = "",
    group_sport: str = "",
    team_starts: dict[str, dict[frozenset[str], datetime]] | None = None,
    name_starts: dict[str, dict[str, datetime]] | None = None,
) -> datetime | None:
    raw = (
        leg.get("start_time")
        or leg.get("game_time")
        or leg.get("commence_time")
        or leg.get("tip")
    )
    parsed = parse_loose_game_time(raw, slate)
    if parsed is not None:
        return parsed
    sport = sport_family(leg.get("sport") or group_sport)
    team_starts = team_starts or {}
    name_starts = name_starts or {}
    if sport in _TENNIS or sport in _GOLF:
        board = name_starts.get(sport) or {}
        for nm in _combo_player_names(leg.get("player")):
            if nm in board:
                return board[nm]
        return None
    if sport in _SOCCER:
        starts = team_starts.get("SOCCER") or {}
    else:
        fam = "NBA" if sport in {"NBA", "NBA1Q", "NBA1H"} else (
            "WNBA" if sport in {"WNBA", "WNBA1Q", "WNBA1H"} else sport
        )
        starts = team_starts.get(sport) or team_starts.get(fam) or {}
    team, opp = _leg_team_opp(leg, sport)
    if not team:
        return None
    return _pair_start(team, opp, starts)


def ticket_started_reason(
    ticket: dict[str, Any],
    *,
    slate: str = "",
    group_sport: str = "",
    team_starts: dict[str, dict[frozenset[str], datetime]] | None = None,
    name_starts: dict[str, dict[str, datetime]] | None = None,
    now: datetime | None = None,
) -> str | None:
    """``game_started:<player>`` if any leg's tip is at or before now."""
    clock = _aware_now(now)
    for leg in ticket.get("legs") or []:
        if not isinstance(leg, dict):
            continue
        dt = leg_start_dt(
            leg,
            slate=slate,
            group_sport=group_sport,
            team_starts=team_starts,
            name_starts=name_starts,
        )
        if dt is not None and dt <= clock:
            player = str(leg.get("player") or "").strip() or "unknown"
            return f"game_started:{player}"
    return None


def filter_payload_to_espn_schedule(
    payload: dict[str, Any] | None,
    *,
    fetch: FetchFn | None = None,
    skip: bool | None = None,
    now: datetime | None = None,
) -> tuple[dict[str, Any], list[dict[str, str]]]:
    """Drop slips off the ESPN scoreboard or whose game has already started."""
    if not isinstance(payload, dict):
        return {}, []
    if skip is None:
        flag = str(os.environ.get("PROPORACLE_SKIP_SCHEDULE_GATE") or "").strip().lower()
        skip = flag in {"1", "true", "yes", "on"}
    if skip:
        print("[schedule-gate] skipped (PROPORACLE_SKIP_SCHEDULE_GATE)")
        return payload, []
    slate = str(payload.get("date") or payload.get("slate_date") or "")[:10]
    if not _DATE_RE.fullmatch(slate):
        print("[schedule-gate] no slate date on payload — blocking all slips")
        blocked: list[dict[str, str]] = []
        for grp in payload.get("groups") or []:
            if not isinstance(grp, dict):
                continue
            for t in grp.get("tickets") or []:
                if isinstance(t, dict):
                    blocked.append(
                        {
                            "ticket_id": str(t.get("ticket_id") or t.get("id") or ""),
                            "reason": "no_slate_date",
                            "player": "",
                            "sport": "",
                        }
                    )
            grp["tickets"] = []
        payload["schedule_gate_blocked"] = len(blocked)
        return payload, blocked

    sports_needed: set[str] = set()
    for grp in payload.get("groups") or []:
        if not isinstance(grp, dict):
            continue
        gsp = sport_family(grp.get("sport") or grp.get("group_name") or "")
        for t in grp.get("tickets") or []:
            if not isinstance(t, dict):
                continue
            for leg in t.get("legs") or []:
                if isinstance(leg, dict):
                    sports_needed.add(sport_family(leg.get("sport") or gsp))

    team_games: dict[str, tuple[set[frozenset[str]], str]] = {}
    name_boards: dict[str, tuple[set[str], str]] = {}
    team_starts: dict[str, dict[frozenset[str], datetime]] = {}
    name_starts: dict[str, dict[str, datetime]] = {}
    seen_family: set[str] = set()
    for sp in sorted(sports_needed):
        if not sp:
            continue
        if sp in _TENNIS or sp in _GOLF:
            if sp not in name_boards:
                names, starts, err = load_name_board_with_starts(sp, slate, fetch=fetch)
                name_boards[sp] = (names, err)
                name_starts[sp] = starts
            continue
        if sp in _SOCCER:
            if "SOCCER" not in team_games:
                pairs, starts, err = load_soccer_board(slate, fetch=fetch)
                team_games["SOCCER"] = (pairs, err)
                team_starts["SOCCER"] = starts
            continue
        fam = "NBA" if sp in {"NBA", "NBA1Q", "NBA1H"} else (
            "WNBA" if sp in {"WNBA", "WNBA1Q", "WNBA1H"} else sp
        )
        if fam in seen_family:
            team_games[sp] = team_games.get(fam, (set(), "missing_board"))
            team_starts[sp] = team_starts.get(fam, {})
            continue
        seen_family.add(fam)
        pairs, starts, err = load_team_board(
            fam if fam in _TEAM_SPORT_PATH else sp, slate, fetch=fetch
        )
        team_games[fam] = (pairs, err)
        team_games[sp] = team_games[fam]
        team_starts[fam] = starts
        team_starts[sp] = starts

    espn_by_sport = _espn_board_labels(team_games, name_boards)
    for sp, labels in espn_by_sport.items():
        if not labels:
            print(f"  [schedule-gate] {sp} ESPN games: (none)")
            continue
        shown = ", ".join(labels[:16])
        extra = "" if len(labels) <= 16 else f" (+{len(labels) - 16})"
        print(f"  [schedule-gate] {sp} ESPN games: {shown}{extra}")

    blocked_rows: list[dict[str, str]] = []
    n_drop = 0
    for grp in payload.get("groups") or []:
        if not isinstance(grp, dict):
            continue
        gname = str(grp.get("group_name") or "")
        gsp = sport_family(grp.get("sport") or gname)
        kept: list[dict[str, Any]] = []
        for t in grp.get("tickets") or []:
            if not isinstance(t, dict):
                continue
            hit: str | None = None
            hit_leg: dict[str, Any] = {}
            for leg in t.get("legs") or []:
                if not isinstance(leg, dict):
                    continue
                reason = leg_schedule_reason(
                    leg,
                    slate=slate,
                    group_sport=gsp,
                    team_games=team_games,
                    name_boards=name_boards,
                )
                if reason:
                    hit = reason
                    hit_leg = leg
                    break
            if not hit:
                hit = ticket_started_reason(
                    t,
                    slate=slate,
                    group_sport=gsp,
                    team_starts=team_starts,
                    name_starts=name_starts,
                    now=now,
                )
                if hit:
                    for leg in t.get("legs") or []:
                        if isinstance(leg, dict):
                            hit_leg = leg
                            break
            if hit:
                n_drop += 1
                rec_sport = sport_family(hit_leg.get("sport") or gsp)
                rec = {
                    "ticket_id": str(t.get("ticket_id") or t.get("id") or ""),
                    "group": gname,
                    "sport": rec_sport,
                    "player": str(hit_leg.get("player") or ""),
                    "team": str(hit_leg.get("team") or ""),
                    "opp": str(hit_leg.get("opp") or hit_leg.get("board_opponent") or ""),
                    "reason": hit,
                    "slate": slate,
                    "espn_games": espn_by_sport.get(rec_sport)
                    or espn_by_sport.get("NBA")
                    or espn_by_sport.get("SOCCER")
                    or [],
                }
                blocked_rows.append(rec)
                board_s = ", ".join((rec["espn_games"] or [])[:8])
                print(
                    f"  [schedule-gate] blocked {rec['ticket_id'] or '(no id)'} "
                    f"{rec['sport']} {rec['player']} {rec['team']} vs {rec['opp']}: {hit}"
                    f" | ESPN: {board_s or '(none)'}"
                )
                continue
            kept.append(t)
        grp["tickets"] = kept
    payload["schedule_gate_blocked"] = n_drop
    if n_drop:
        print(f"[schedule-gate] blocked {n_drop} slip(s) not on ESPN {slate} scoreboard")
        _log_blocked_by_sport(slate, blocked_rows, espn_by_sport)
    else:
        print(f"[schedule-gate] all slips on ESPN {slate} scoreboard")
    return payload, blocked_rows


def _game_labels(games: set[frozenset[str]]) -> list[str]:
    labels: list[str] = []
    for g in games:
        abbrs = sorted(t for t in g if 2 <= len(str(t)) <= 4)
        if len(abbrs) >= 2:
            labels.append(f"{abbrs[0]}@{abbrs[1]}")
        elif g:
            labels.append("@".join(sorted(str(x) for x in g)[:2]))
    return sorted(set(labels))


def _espn_board_labels(
    team_games: dict[str, tuple[set[frozenset[str]], str]],
    name_boards: dict[str, tuple[set[str], str]],
) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for sp, (games, _err) in team_games.items():
        out[sp] = _game_labels(games)
    for sp, (names, _err) in name_boards.items():
        out[sp] = sorted(names)[:40]
    return out


def _log_blocked_by_sport(
    slate: str,
    blocked: list[dict[str, str]],
    espn_by_sport: dict[str, list[str]],
) -> None:
    by_sport: dict[str, dict[str, Any]] = {}
    for rec in blocked:
        sp = str(rec.get("sport") or "?")
        bucket = by_sport.setdefault(sp, {"n": 0, "reasons": {}, "rows": []})
        bucket["n"] += 1
        reason = str(rec.get("reason") or "?")
        key = reason.split(":")[0]
        bucket["reasons"][key] = int(bucket["reasons"].get(key) or 0) + 1
        slim = {k: rec[k] for k in rec if k != "espn_games"}
        bucket["rows"].append(slim)
    for sp, bucket in sorted(by_sport.items()):
        reasons = ", ".join(f"{k}={v}" for k, v in sorted(bucket["reasons"].items()))
        print(f"  [schedule-gate] {sp}: {bucket['n']} blocked ({reasons})")
    dest = _REPO / "data" / "reports" / f"schedule_gate_blocked_{slate}.json"
    try:
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(
            json.dumps(
                {
                    "slate": slate,
                    "n_blocked": len(blocked),
                    "by_sport": by_sport,
                    "espn_games": espn_by_sport,
                },
                indent=2,
            ),
            encoding="utf-8",
        )
        print(f"  [schedule-gate] wrote {dest}")
    except OSError as exc:
        print(f"  [schedule-gate] blocked-log write failed: {exc}")
