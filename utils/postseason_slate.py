"""League season-type ticket gate.

Cells were fit on regular-season rows. Preseason and an unknown game type
stay off tickets and do not fall back to the 0.763 floor.

MLB and WNBA postseason (ESPN season type 3, including WNBA period slates)
is allowed. The price is the regular-season cell minus 3 points, tagged
``postseason=1``, graded on ``gated_goblin_postseason``, and kept out of
the 71/127 book. One such slip per day until 30 are graded. NFL, CFB, NBA,
NHL, and CBB postseason stay blocked.

The key is the ESPN season type on the event, not a calendar date. Type 1
is preseason. Type 2 / slug ``regular-season`` is the regular season.
Type 3 is postseason. The scoreboard lookup merges season types 2 and 3
so a postseason event is not missing from the join.

Tennis, soccer, and golf are not this gate.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.request import Request, urlopen

_ROOT = Path(__file__).resolve().parents[1]
_CACHE = _ROOT / "data" / "cache" / "espn_season_type"

# League families whose hit-rate cells are regular-season only.
_FAMILY = {
    "MLB": "MLB",
    "WNBA": "WNBA",
    "WNBA1H": "WNBA",
    "WNBA1Q": "WNBA",
    "NBA": "NBA",
    "NBA1H": "NBA",
    "NBA1Q": "NBA",
    "NBAP": "NBA",
    "NHL": "NHL",
    "NFL": "NFL",
    "NFLP": "NFL",
    "CFB": "CFB",
    "CFB1H": "CFB",
}

_ESPN_PATH = {
    "MLB": "baseball/mlb",
    "WNBA": "basketball/wnba",
    "NBA": "basketball/nba",
    "NHL": "hockey/nhl",
    "NFL": "football/nfl",
    "CFB": "football/college-football",
}

_FETCH: Callable[[str], dict] | None = None

# Families whose postseason legs can ticket. Everyone else stays blocked.
POSTSEASON_TICKET_FAMILIES = frozenset({"MLB", "WNBA"})
POSTSEASON_HAIRCUT = 0.03
POSTSEASON_PLACE_HAIRCUT = 0.02
POSTSEASON_DAILY_CAP = 1
POSTSEASON_GRADED_BEFORE_OPEN = 30
POSTSEASON_TRACK = "gated_goblin_postseason"
# Median scraped all-Goblin Power quotes. N-correct, not 1st place.
MEDIAN_ALL_GOBLIN_3 = 2.2
MEDIAN_ALL_GOBLIN_2 = 1.40


def slate_date(row: dict[str, Any] | None) -> date | None:
    """Board date from the row. PrizePicks start_time is often the slate stamp."""
    if not isinstance(row, dict):
        return None
    for key in ("start_time", "game_date", "board_date", "slate_date", "date"):
        raw = row.get(key)
        if raw is None or raw == "":
            continue
        text = str(raw).strip()
        if len(text) >= 10 and text[4] == "-" and text[7] == "-":
            try:
                return date.fromisoformat(text[:10])
            except ValueError:
                continue
    return None


def _explicit_class(row: dict[str, Any]) -> str | None:
    lg = str(row.get("league") or row.get("League") or "").strip().upper()
    lid = str(row.get("league_id") or "").strip()
    # PrizePicks preseason tabs. Keep in sync with Sports/NBA/prizepicks_league_ids.py
    # (NBAP=237) and Sports/NFL/prizepicks_league_ids.py (NFLP=44).
    if lg in {"NBAP", "NBA PRESEASON", "NBA-PRESEASON"} or lid == "237":
        return "preseason"
    if lg in {"NFLP", "NFL PRESEASON", "NFL-PRESEASON"} or lid == "44":
        return "preseason"
    raw = str(
        row.get("season_type")
        or row.get("espn_season_type")
        or row.get("espn_season_slug")
        or ""
    ).strip().lower()
    if not raw:
        return None
    if raw in {"2", "regular", "regular-season", "reg", "r"}:
        return "regular"
    if raw in {"1", "pre", "preseason", "pre-season"}:
        return "preseason"
    if raw in {"3", "post", "postseason", "post-season", "playoff"}:
        return "postseason"
    return None


def _class_from_season(season: dict) -> str | None:
    slug = str(season.get("slug") or "").strip().lower()
    if slug == "regular-season":
        return "regular"
    if "pre" in slug:
        return "preseason"
    if "post" in slug or "playoff" in slug:
        return "postseason"
    try:
        code = int(season.get("type"))
    except (TypeError, ValueError):
        return None
    return {1: "preseason", 2: "regular", 3: "postseason"}.get(code)


# PrizePicks abbreviations that ESPN spells differently.
_ABBR_ALIAS = {
    "CWS": "CHW",
    "CHW": "CWS",
    "WSH": "WSN",
    "WSN": "WSH",
    # PrizePicks uses WAS for the Commanders. ESPN's abbreviation is WSH.
    "WAS": "WSH",
    "ATH": "OAK",
    "OAK": "ATH",
    "SDP": "SD",
    "SD": "SDP",
    "SFG": "SF",
    "SF": "SFG",
    "TBR": "TB",
    "TB": "TBR",
    "KCR": "KC",
    "KC": "KCR",
    "ARI": "AZ",
    "AZ": "ARI",
    # WNBA PrizePicks abbreviations. ESPN uses NY, LV, GS.
    "NYL": "NY",
    "LVA": "LV",
    "GSV": "GS",
}


def _norm_token(text: object) -> str:
    return "".join(ch for ch in str(text or "").upper() if ch.isalnum())


def _expand_abbr(token: str) -> set[str]:
    out = {token} if token else set()
    alias = _ABBR_ALIAS.get(token)
    if alias:
        out.add(alias)
    return out


def _side_tokens(raw: object) -> set[str]:
    text = str(raw or "").strip()
    if not text or text.lower() in {"nan", "none"}:
        return set()
    tokens: set[str] = set()
    whole = _norm_token(text)
    if len(whole) >= 2:
        tokens |= _expand_abbr(whole)
    for part in text.replace("@", " ").replace("/", " ").split():
        token = _norm_token(part)
        if len(token) >= 2:
            tokens |= _expand_abbr(token)
    return tokens


def _row_sides(row: dict[str, Any]) -> tuple[set[str], set[str]]:
    team = set()
    for key in ("team", "Team", "home_team", "away_team"):
        team |= _side_tokens(row.get(key))
    opp = set()
    for key in ("opp_team", "opponent", "opp", "Opp"):
        opp |= _side_tokens(row.get(key))
    return team, opp


def _event_tokens(event: dict) -> set[str]:
    tokens = set()
    comps = (event.get("competitions") or [{}])[0].get("competitors") or []
    for comp in comps:
        team = comp.get("team") or {}
        for key in ("abbreviation", "shortDisplayName", "displayName", "name", "location"):
            token = _norm_token(team.get(key))
            if len(token) >= 2:
                tokens.add(token)
    return tokens


def _fetch_json(url: str) -> dict:
    if _FETCH is not None:
        return _FETCH(url)
    req = Request(url, headers={"User-Agent": "PropORACLE"})
    with urlopen(req, timeout=25) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else {}


def _family_of(sport: str) -> str | None:
    return _FAMILY.get(sport)


def postseason_ticket_sport(sport: object) -> bool:
    return _family_of(str(sport or "").strip().upper()) in POSTSEASON_TICKET_FAMILIES


def _scoreboard(family: str, day: date) -> list[dict]:
    """Regular-season and postseason events for this date.

    A dates-only scoreboard omits the other season type. Both type 2 and
    type 3 are merged. A cache written before that merge is refetched.
    """
    path = _ESPN_PATH.get(family)
    if not path:
        return []
    stamp = day.strftime("%Y%m%d")
    _CACHE.mkdir(parents=True, exist_ok=True)
    cache_path = _CACHE / f"{family}_{stamp}.json"
    if cache_path.is_file():
        try:
            cached = json.loads(cache_path.read_text(encoding="utf-8"))
            if (
                isinstance(cached, dict)
                and cached.get("seasontypes") == [2, 3]
                and isinstance(cached.get("events"), list)
            ):
                return cached["events"]
        except Exception:
            pass
    events_by_id: dict[str, dict] = {}
    fetched = False
    for seasontype in (2, 3):
        url = (
            "https://site.api.espn.com/apis/site/v2/sports/"
            f"{path}/scoreboard?dates={stamp}&seasontype={seasontype}"
        )
        try:
            payload = _fetch_json(url)
        except Exception:
            continue
        fetched = True
        events = payload.get("events") if isinstance(payload, dict) else None
        if not isinstance(events, list):
            continue
        for event in events:
            if not isinstance(event, dict):
                continue
            eid = str(event.get("id") or "") or str(id(event))
            events_by_id[eid] = event
    events = list(events_by_id.values())
    if fetched:
        try:
            cache_path.write_text(
                json.dumps({"seasontypes": [2, 3], "events": events}),
                encoding="utf-8",
            )
        except Exception:
            pass
    return events


def espn_season_class(row: dict[str, Any]) -> str | None:
    """Match the row to one ESPN event and return preseason, regular, or postseason."""
    sport = str(row.get("sport") or "").strip().upper()
    family = _FAMILY.get(sport)
    played = slate_date(row)
    if family is None or played is None:
        return None
    team_tokens, opp_tokens = _row_sides(row)
    if not team_tokens and not opp_tokens:
        return None
    matches: list[str] = []
    for delta in (0, -1, 1):
        for event in _scoreboard(family, played + timedelta(days=delta)):
            if not isinstance(event, dict):
                continue
            names = _event_tokens(event)
            if not names:
                continue
            team_hit = bool(team_tokens & names) if team_tokens else True
            opp_hit = bool(opp_tokens & names) if opp_tokens else True
            if not (team_hit and opp_hit):
                continue
            kind = _class_from_season(event.get("season") or {})
            if kind:
                matches.append(kind)
        if matches:
            break
    kinds = set(matches)
    if len(kinds) == 1:
        return next(iter(kinds))
    return None


def season_class(row: dict[str, Any] | None) -> str | None:
    if not isinstance(row, dict):
        return None
    explicit = _explicit_class(row)
    if explicit:
        return explicit
    return espn_season_class(row)


def is_regular_season_row(row: dict[str, Any] | None) -> bool:
    return season_class(row) == "regular"


def season_blocks_ticket(row: dict[str, Any] | None) -> bool:
    """Block preseason, unknown game type, and postseason outside MLB/WNBA.

    MLB and WNBA postseason is allowed. It is priced separately and is not
    a regular-season leg.
    """
    if not isinstance(row, dict):
        return False
    sport = str(row.get("sport") or "").strip().upper()
    if sport not in _FAMILY:
        return False
    kind = season_class(row)
    if kind == "regular":
        return False
    if kind == "postseason" and postseason_ticket_sport(sport):
        return False
    return True


def postseason_blocks_ticket(row: dict[str, Any] | None) -> bool:
    """Backward-compatible name for the season block."""
    return season_blocks_ticket(row)


def is_postseason_row(row: dict[str, Any] | None) -> bool:
    return season_class(row) == "postseason"


def is_postseason_ticket_leg(row: dict[str, Any] | None) -> bool:
    """MLB or WNBA leg whose event is postseason."""
    if not isinstance(row, dict):
        return False
    sport = str(row.get("sport") or "").strip().upper()
    return postseason_ticket_sport(sport) and is_postseason_row(row)


def apply_postseason_price(row: dict[str, Any], cell_hr: float | None) -> bool:
    """Price a postseason leg at the cell minus 3 points.

    A missing cell does not fall back to 0.763. Returns True when the leg
    keeps a price.
    """
    row["postseason"] = 1
    row["grade_track"] = POSTSEASON_TRACK
    try:
        cell = float(cell_hr) if cell_hr is not None else None
    except (TypeError, ValueError):
        cell = None
    if cell is None or cell != cell or cell <= POSTSEASON_HAIRCUT or cell > 1:
        row["p"] = None
        row["p_cell_hr"] = None
        row["p_source"] = "postseason_no_cell"
        return False
    row["p_cell_hr"] = cell
    row["p"] = cell - POSTSEASON_HAIRCUT
    row["p_source"] = "postseason_cell_minus_3"
    return True


def median_all_hit_x(n_legs: int, n_goblin: int) -> float | None:
    """Median scraped N-correct quote for the shortlist test."""
    if n_legs == 3 and n_goblin == 3:
        return MEDIAN_ALL_GOBLIN_3
    if n_legs == 2 and n_goblin == 2:
        return MEDIAN_ALL_GOBLIN_2
    if n_goblin == 0 and n_legs == 2:
        return 3.0
    if n_goblin == 0 and n_legs == 3:
        return 6.0
    return None


def power_ev(ps: list[float], payout: float) -> float | None:
    if payout < 1.4 or not ps:
        return None
    p_all = 1.0
    for p in ps:
        if p <= 0 or p > 1:
            return None
        p_all *= p
    return p_all * payout - 1.0


def postseason_graded_count() -> int:
    path = _ROOT / "data" / "reports" / "gated_goblin_postseason_graded.json"
    if not path.is_file():
        return 0
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return 0
    try:
        return int(payload.get("graded") or 0)
    except (TypeError, ValueError):
        return 0


def postseason_daily_cap() -> int:
    """One slip a day until 30 postseason slips are graded."""
    if postseason_graded_count() >= POSTSEASON_GRADED_BEFORE_OPEN:
        return 99
    return POSTSEASON_DAILY_CAP


def shortlist_postseason(legs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Groups with EV>0 at the median quote after the cell-minus-3 price.

    Placement is separate: EV>0 at a scraped payout with every leg another
    2 points under that already-haircut cell. No scrape means not placed.
    """
    from itertools import combinations

    rows = [r for r in legs if r.get("p") is not None and r.get("postseason") == 1]
    found: list[dict[str, Any]] = []
    for n in (2, 3):
        if len(rows) < n:
            continue
        for combo in combinations(rows, n):
            ids = []
            ok = True
            for leg in combo:
                player = str(leg.get("player") or "").strip().lower()
                prop = str(leg.get("prop") or leg.get("prop_type") or "").strip().lower()
                ident = f"{player}|{prop}"
                if not player or ident in ids:
                    ok = False
                    break
                ids.append(ident)
            if not ok:
                continue
            n_g = sum(1 for leg in combo if "goblin" in str(leg.get("pick_type") or "").lower())
            quote = median_all_hit_x(n, n_g)
            if quote is None:
                continue
            try:
                ps = [float(leg["p"]) for leg in combo]
            except (KeyError, TypeError, ValueError):
                continue
            ev = power_ev(ps, quote)
            if ev is None or ev <= 0:
                continue
            found.append(
                {
                    "n_legs": n,
                    "n_goblin": n_g,
                    "n_standard": n - n_g,
                    "median_x": quote,
                    "ev": ev,
                    "placed": False,
                    "postseason": 1,
                    "ticket_track": POSTSEASON_TRACK,
                    "legs": list(combo),
                }
            )
    found.sort(key=lambda item: item["ev"], reverse=True)
    return found


def mark_placed(group: dict[str, Any], scraped_x: float | None) -> bool:
    """R3: place only when EV stays positive 2 points under the haircut cell."""
    try:
        payout = float(scraped_x) if scraped_x is not None else None
    except (TypeError, ValueError):
        payout = None
    if payout is None:
        group["placed"] = False
        return False
    ps = []
    for leg in group.get("legs") or []:
        try:
            ps.append(float(leg["p"]) - POSTSEASON_PLACE_HAIRCUT)
        except (KeyError, TypeError, ValueError):
            group["placed"] = False
            return False
    ev = power_ev(ps, payout)
    group["placement_ev"] = ev
    group["scraped_x"] = payout
    group["placed"] = bool(ev is not None and ev > 0)
    return bool(group["placed"])


def counts_toward_gated_book(slip: dict[str, Any] | None) -> bool:
    """False for the postseason track. Those slips stay out of the 71/127 book."""
    if not isinstance(slip, dict):
        return False
    if slip.get("postseason") in {1, "1", True}:
        return False
    track = str(slip.get("ticket_track") or slip.get("grade_track") or "")
    if track == POSTSEASON_TRACK:
        return False
    return True
