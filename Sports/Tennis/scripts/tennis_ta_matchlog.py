"""Tennis Abstract match-log fill for serve stats and games/sets.

Jeff Sackmann's GitHub CSVs (and ESPN scoreboards) do not carry live per-match
serve/return stats. Tennis Abstract embeds `matchmx` with aces, DFs, and
break-point saved/faced through the current week. Score strings also fill
games/sets when ESPN omits Challenger matches.

ATP: `player-classic.cgi` often inlines the live log. Do not prefer
`jsmatches/{slug}.js` for ATP — that file can be an old career dump.

WTA: use `jsmatches/{slug}.js` (and `wplayer-classic.cgi` as fallback).
Never fetch a WTA name from ATP `player-classic.cgi` — that page collides
(Iga Swiatek → Benoit Paire).

TA score strings mix player-first (`4-6 6-3 3-6` with L) and winner-first
(`6-2 6-3` with L). Games/sets use W/L vs set counts — never assume the
first number in each set is this player.

Missing cells stay None (never coerced to 0). Walkovers and in-progress rows
have empty aces/DFs and are skipped. Qualifying rounds are tagged so step4's
existing L5 filter drops them.
"""
from __future__ import annotations

import json
import random
import re
import sys
import time
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import quote

_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from tennis_shared import (  # noqa: E402
    _ROUND_ORDER,
    collect_history_values,
    norm_key,
    ta_player_side_from_score,
)

CACHE_PATH = Path(__file__).resolve().parents[1] / "data" / "cache" / "tennis_abstract_matchlog.json"
TA_ATP_URL = "https://www.tennisabstract.com/cgi-bin/player-classic.cgi?p={slug}"
TA_WTA_URL = "https://www.tennisabstract.com/cgi-bin/wplayer-classic.cgi?p={slug}"
TA_JSMATCHES_URL = "https://www.tennisabstract.com/jsmatches/{slug}.js"
TA_HEADERS = {
    "Accept": "text/html,application/javascript,text/javascript,*/*",
    "Referer": "https://www.tennisabstract.com/",
}
TA_PROP_KEYS = frozenset({"aces", "double_faults", "break_points_won"})
TA_GAME_KEYS = frozenset({
    "games_won",
    "match_total_games",
    "sets_won",
    "total_sets",
    "total_tie_breaks",
})
TA_FILL_KEYS = TA_PROP_KEYS | TA_GAME_KEYS
# Challenger / ITF often skip a week. 14d dropped Aug-10 logs on an Aug-25 slate.
TA_PLAYER_STALE_DAYS = 21
_CACHE_MAX_AGE_H = 12.0
_WO_SCORES = frozenset({"w/o", "wo", "ret", "retired", "def", "default", "walkover"})

# Tennis Abstract matchhead (ATP player-classic / WTA wplayer / jsmatches).
DEFAULT_MATCHHEAD: tuple[str, ...] = (
    "date",
    "tourn",
    "surf",
    "level",
    "wl",
    "rank",
    "seed",
    "entry",
    "round",
    "score",
    "max",
    "opp",
    "orank",
    "oseed",
    "oentry",
    "ohand",
    "obday",
    "oht",
    "ocountry",
    "oactive",
    "time",
    "aces",
    "dfs",
    "pts",
    "firsts",
    "fwon",
    "swon",
    "games",
    "saved",
    "chances",
    "oaces",
    "odfs",
    "opts",
    "ofirsts",
    "ofwon",
    "oswon",
    "ogames",
    "osaved",
    "ochances",
    "obackhand",
    "chartlink",
    "pslink",
    "whserver",
    "matchid",
    "wh",
    "roundnum",
    "matchnum",
)


def needs_ta_matchlog(prop_norm: str, prop_type: str = "") -> bool:
    for raw in (prop_norm, prop_type):
        p = str(raw or "").lower().strip()
        compact = p.replace("-", "").replace("_", "").replace(" ", "")
        if p in TA_PROP_KEYS or compact in {
            "aces",
            "ace",
            "doublefaults",
            "doublefault",
            "breakpointswon",
        }:
            return True
        if "double fault" in p or p == "break points won":
            return True
    return False


def _ta_name_parts(name: str) -> list[str]:
    text = unicodedata.normalize("NFKD", str(name or ""))
    text = "".join(c for c in text if not unicodedata.combining(c))
    return [p for p in re.findall(r"[A-Za-z0-9]+", text) if p]


def _ta_join_slug(parts: list[str]) -> str:
    return "".join(p[:1].upper() + p[1:] for p in parts if p)


def ta_slug(name: str) -> str:
    """Tennis Abstract player-classic `p=` token: FirstLast, no punctuation."""
    return _ta_join_slug(_ta_name_parts(name))


def ta_slug_candidates(name: str) -> list[str]:
    """Given-family and family-given slugs (ESPN/PP often flip East Asian names)."""
    parts = _ta_name_parts(name)
    out: list[str] = []
    primary = _ta_join_slug(parts)
    if primary:
        out.append(primary)
    if len(parts) >= 2:
        flipped = _ta_join_slug([parts[-1], *parts[:-1]])
        if flipped and flipped not in out:
            out.append(flipped)
    return out


def normalize_tour(tour: str) -> str:
    t = str(tour or "").strip().upper()
    return "WTA" if t == "WTA" else "ATP"


def ta_index_key(name: str, tour: str = "ATP") -> str:
    return f"{normalize_tour(tour)}:{norm_key(name)}"


def ta_page_url(slug: str, tour: str = "ATP") -> str:
    token = quote(slug or "", safe="")
    if normalize_tour(tour) == "WTA":
        return TA_WTA_URL.format(slug=token)
    return TA_ATP_URL.format(slug=token)


def ta_jsmatches_url(slug: str) -> str:
    return TA_JSMATCHES_URL.format(slug=quote(slug or "", safe=""))


def parse_js_quoted_array(src: str, var_name: str) -> list[str]:
    """Read `var matchhead = ["date", ..., 'games', ...]` (mixed quotes)."""
    m = re.search(rf"var\s+{re.escape(var_name)}\s*=\s*", src)
    if not m:
        return []
    chunk = src[m.end() :]
    end = re.search(r"\]\s*", chunk)
    if end:
        chunk = chunk[: end.end()]
    else:
        chunk = chunk[:4000]
    return re.findall(r"""['"]([^'"]*)['"]""", chunk)


def parse_matchmx(src: str) -> list[list[Any]]:
    m = re.search(r"var\s+matchmx\s*=\s*", src)
    if not m:
        return []
    try:
        obj, _ = json.JSONDecoder().raw_decode(src[m.end() :])
    except json.JSONDecodeError:
        return []
    if not isinstance(obj, list):
        return []
    return [row for row in obj if isinstance(row, list)]


def _cell_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    text = str(val).strip()
    if not text or text.lower() in {"none", "nan", "null"}:
        return None
    try:
        return float(text)
    except (TypeError, ValueError):
        return None


def _row_get(row: list[Any], head: list[str], name: str) -> Any:
    try:
        i = head.index(name)
    except ValueError:
        return None
    if i >= len(row):
        return None
    return row[i]


def parse_ta_date(raw: object) -> Optional[date]:
    s = str(raw or "").strip()
    if len(s) >= 8 and s[:8].isdigit():
        try:
            return date(int(s[:4]), int(s[4:6]), int(s[6:8]))
        except ValueError:
            return None
    return None


def rows_from_matchmx(head: list[str], matchmx: list[list[Any]]) -> list[dict[str, Any]]:
    """Map Tennis Abstract match rows → step4 history records (newest first)."""
    if not head or not matchmx:
        return []
    out: list[dict[str, Any]] = []
    for raw in matchmx:
        score = str(_row_get(raw, head, "score") or "").strip()
        rnd = str(_row_get(raw, head, "round") or "").strip()
        date_s = str(_row_get(raw, head, "date") or "").strip()
        try:
            match_num = int(float(_row_get(raw, head, "matchnum") or 0))
        except (TypeError, ValueError):
            match_num = 0
        aces = _cell_float(_row_get(raw, head, "aces"))
        dfs = _cell_float(_row_get(raw, head, "dfs"))
        pts = _cell_float(_row_get(raw, head, "pts"))
        firsts = _cell_float(_row_get(raw, head, "firsts"))
        fwon = _cell_float(_row_get(raw, head, "fwon"))
        swon = _cell_float(_row_get(raw, head, "swon"))
        sv_games = _cell_float(_row_get(raw, head, "games"))
        # Own BP saved/faced live under saved/chances; opponent under osaved/ochances.
        own_saved = _cell_float(_row_get(raw, head, "saved"))
        own_chances = _cell_float(_row_get(raw, head, "chances"))
        osaved = _cell_float(_row_get(raw, head, "osaved"))
        ochances = _cell_float(_row_get(raw, head, "ochances"))
        bp = None
        if osaved is not None and ochances is not None:
            bp = ochances - osaved

        def _rate(num: float | None, den: float | None) -> float | None:
            if num is None or den is None or den <= 0:
                return None
            return round(100.0 * float(num) / float(den), 2)

        second_in = None
        if pts is not None and firsts is not None:
            second_in = pts - firsts
        rec = {
            "date": date_s,
            "match_num": match_num,
            "round": rnd,
            "score": score,
            "wl": str(_row_get(raw, head, "wl") or "").strip().upper(),
            "tourn": str(_row_get(raw, head, "tourn") or ""),
            "level": str(_row_get(raw, head, "level") or ""),
            "surface": str(_row_get(raw, head, "surf") or "").strip() or None,
            "opp": str(_row_get(raw, head, "opp") or ""),
            "opp_hand": str(_row_get(raw, head, "ohand") or "").strip() or None,
            "rank": _cell_float(_row_get(raw, head, "rank")),
            "opp_rank": _cell_float(_row_get(raw, head, "orank")),
            "aces": aces,
            "double_faults": dfs,
            "break_points_won": bp,
            "sv_pts": pts,
            "first_in_pct": _rate(firsts, pts),
            "first_won_pct": _rate(fwon, firsts),
            "second_won_pct": _rate(swon, second_in),
            "bp_saved_pct": _rate(own_saved, own_chances),
            "bp_faced": own_chances,
            "service_games": sv_games,
            "is_qualifying": rnd.upper() in {"Q", "Q1", "Q2", "Q3", "Q1ST", "Q2ND", "Q3RD", "QUAL"},
        }
        parsed = ta_player_side_from_score(score, rec["wl"])
        if parsed:
            rec["match_total_games"] = parsed["match_total_games"]
            rec["total_sets"] = parsed["total_sets"]
            rec["total_tie_breaks"] = parsed["total_tie_breaks"]
            rec["games_won"] = parsed.get("games_won")
            rec["sets_won"] = parsed.get("sets_won")
        if score.lower() in _WO_SCORES:
            rec["aces"] = None
            rec["double_faults"] = None
            rec["break_points_won"] = None
            rec["match_total_games"] = None
            rec["games_won"] = None
            rec["sets_won"] = None
            rec["total_sets"] = None
            rec["total_tie_breaks"] = None
        out.append(rec)
    out.sort(
        key=lambda x: (
            str(x.get("date") or ""),
            int(x.get("match_num") or 0),
            _ROUND_ORDER.get(str(x.get("round") or "").upper(), 0),
        ),
        reverse=True,
    )
    return out


def parse_player_page(html: str) -> list[dict[str, Any]]:
    head = parse_js_quoted_array(html, "matchhead") or list(DEFAULT_MATCHHEAD)
    mx = parse_matchmx(html)
    return rows_from_matchmx(head, mx)


def backfill_score_fields(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Fill games/sets from stored TA score strings (older cache rows lack them)."""
    out: list[dict[str, Any]] = []
    for rec in rows:
        if not isinstance(rec, dict):
            continue
        r = dict(rec)
        parsed = ta_player_side_from_score(str(r.get("score") or ""), r.get("wl"))
        if parsed:
            r["match_total_games"] = parsed["match_total_games"]
            r["total_sets"] = parsed["total_sets"]
            r["total_tie_breaks"] = parsed["total_tie_breaks"]
            r["games_won"] = parsed.get("games_won")
            r["sets_won"] = parsed.get("sets_won")
        if str(r.get("score") or "").strip().lower() in _WO_SCORES:
            for k in (
                "aces",
                "double_faults",
                "break_points_won",
                "match_total_games",
                "games_won",
                "sets_won",
                "total_sets",
                "total_tie_breaks",
            ):
                r[k] = None
        out.append(r)
    return out


_CACHED_TA_INDEX: dict[str, list[dict[str, Any]]] | None = None


def load_cached_ta_player_index(
    path: Path | None = None,
) -> dict[str, list[dict[str, Any]]]:
    """Disk cache → norm_key(player) lists for serve-stat lookup (no network)."""
    global _CACHED_TA_INDEX
    if _CACHED_TA_INDEX is not None and path is None:
        return _CACHED_TA_INDEX
    cache = _load_cache(path or CACHE_PATH)
    players = cache.get("players") or {}
    index: dict[str, list[dict[str, Any]]] = {}
    for key, rec in players.items():
        if not isinstance(rec, dict):
            continue
        rows = backfill_score_fields(list(rec.get("rows") or []))
        name = str(key).split(":", 1)[-1]
        pk = norm_key(name)
        if pk and rows:
            index.setdefault(pk, []).extend(rows)
    if path is None:
        _CACHED_TA_INDEX = index
    return index


def newest_row_date(rows: list[dict[str, Any]]) -> Optional[date]:
    for rec in rows:
        parsed = parse_ta_date(rec.get("date") if isinstance(rec, dict) else None)
        if parsed is not None:
            return parsed
    return None


def rows_are_fresh(
    rows: list[dict[str, Any]],
    slate: date,
    max_age_days: int = TA_PLAYER_STALE_DAYS,
) -> bool:
    newest = newest_row_date(rows)
    if newest is None:
        return False
    return (slate - newest).days <= max(1, int(max_age_days))


def ta_player_fresh(
    index: dict[str, list[dict[str, Any]]],
    player_name: str,
    slate: date,
    *,
    tour: str = "ATP",
    max_age_days: int = TA_PLAYER_STALE_DAYS,
) -> bool:
    rows = index.get(ta_index_key(player_name, tour)) or []
    return rows_are_fresh(rows, slate, max_age_days=max_age_days)


def _load_cache(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {"players": {}}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"players": {}}
    if not isinstance(data, dict):
        return {"players": {}}
    data.setdefault("players", {})
    return data


def _save_cache(path: Path, cache: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    cache["fetched_at"] = datetime.now(timezone.utc).isoformat()
    path.write_text(json.dumps(cache), encoding="utf-8")


def _cache_fresh(entry: dict[str, Any]) -> bool:
    raw = str(entry.get("fetched_at") or "")
    if not raw:
        return False
    try:
        ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return False
    age_h = (datetime.now(timezone.utc) - ts).total_seconds() / 3600.0
    return age_h <= _CACHE_MAX_AGE_H and bool(entry.get("rows"))


def _make_session():
    from utils.prizepicks_http import curl_impersonate, ensure_chrome131

    ensure_chrome131()
    from curl_cffi.requests import Session as CurlSession

    session = CurlSession(impersonate=curl_impersonate())
    session.headers.update(TA_HEADERS)
    return session


def _http_get(session, url: str, retries: int = 4) -> tuple[str, str]:
    last_err = ""
    for attempt in range(1, retries + 1):
        try:
            time.sleep(0.35 + random.uniform(0, 0.25))
            resp = session.get(url, timeout=30)
            if resp.status_code == 429:
                last_err = "HTTP 429"
                time.sleep(6.0 * attempt)
                continue
            if resp.status_code != 200:
                last_err = f"HTTP {resp.status_code}"
                if resp.status_code in {404, 403, 410}:
                    break
                continue
            return resp.text or "", ""
        except Exception as exc:
            last_err = str(exc)
            time.sleep(0.6 * attempt)
    return "", last_err


def fetch_player_rows(
    slug: str,
    session=None,
    retries: int = 4,
    *,
    tour: str = "ATP",
) -> list[dict[str, Any]]:
    if not slug:
        return []
    sess = session or _make_session()
    tour_n = normalize_tour(tour)
    last_err = ""

    if tour_n == "WTA":
        # Live WTA log is jsmatches. ATP player-classic collides on names.
        text, last_err = _http_get(sess, ta_jsmatches_url(slug), retries=retries)
        rows = parse_player_page(text) if text else []
        if rows:
            return rows
        text, html_err = _http_get(sess, ta_page_url(slug, "WTA"), retries=retries)
        rows = parse_player_page(text) if text else []
        if rows:
            return rows
        last_err = last_err or html_err or "no matchmx"
    else:
        # ATP inlined HTML first. jsmatches can be a stale career dump (2018).
        text, last_err = _http_get(sess, ta_page_url(slug, "ATP"), retries=retries)
        rows = parse_player_page(text) if text else []
        if rows:
            return rows
        text, js_err = _http_get(sess, ta_jsmatches_url(slug), retries=retries)
        rows = parse_player_page(text) if text else []
        if rows:
            return rows
        last_err = last_err or js_err or "no matchmx"

    if last_err:
        print(f"  [ta] {tour_n} {slug}: {last_err}")
    return []


def _coerce_player_spec(item: str | tuple[str, str] | list[str]) -> tuple[str, str]:
    if isinstance(item, str):
        return item, "ATP"
    if not item:
        return "", "ATP"
    name = str(item[0] or "")
    tour = str(item[1] or "ATP") if len(item) > 1 else "ATP"
    return name, tour


def _fetch_best_ta_rows(
    slugs: list[str],
    tour: str,
    session,
    slate: Optional[date],
) -> tuple[list[dict[str, Any]], str, str]:
    """Try given/family slugs; if ATP is empty or stale vs slate, try WTA."""
    tours_to_try = [tour]
    if tour == "ATP":
        tours_to_try.append("WTA")
    best: list[dict[str, Any]] = []
    used_slug = slugs[0]
    used_tour = tour
    for cand_tour in tours_to_try:
        for cand in slugs:
            rows = fetch_player_rows(cand, session=session, tour=cand_tour)
            if not rows:
                continue
            used_slug, used_tour, best = cand, cand_tour, rows
            if slate is None or rows_are_fresh(rows, slate):
                return rows, cand, cand_tour
    return best, used_slug, used_tour


def build_ta_player_index(
    players: Iterable[str | tuple[str, str] | list[str]],
    *,
    cache_path: Optional[Path] = None,
    session=None,
    force: bool = False,
    slate: Optional[date] = None,
) -> dict[str, list[dict[str, Any]]]:
    """Fetch (or cache) Tennis Abstract logs keyed by `ATP|WTA:norm_key(name)`."""
    path = Path(cache_path) if cache_path else CACHE_PATH
    cache = _load_cache(path)
    cache_players = cache.setdefault("players", {})
    index: dict[str, list[dict[str, Any]]] = {}
    unique: list[tuple[str, list[str], str]] = []
    seen: set[str] = set()
    for item in players:
        name, tour = _coerce_player_spec(item)
        pk = ta_index_key(name, tour)
        slugs = ta_slug_candidates(name)
        if not name or not slugs or pk in seen:
            continue
        seen.add(pk)
        unique.append((pk, slugs, normalize_tour(tour)))

    def _cached_fresh_enough(entry: dict[str, Any]) -> bool:
        if force or not _cache_fresh(entry):
            return False
        rows = backfill_score_fields(list(entry.get("rows") or []))
        if slate is None:
            return True
        return rows_are_fresh(rows, slate)

    need_net = [row for row in unique if not _cached_fresh_enough(cache_players.get(row[0]) or {})]
    sess = None
    if need_net:
        try:
            sess = session or _make_session()
        except ImportError:
            print("[ta] curl_cffi not installed — skip Tennis Abstract fill")
            need_net = []
        print(
            f"[ta] Tennis Abstract matchlog for {len(need_net)} players "
            f"({len(unique) - len(need_net)} cached)"
        )

    for pk, slugs, tour in unique:
        entry = cache_players.get(pk) or {}
        cached_rows = backfill_score_fields(list(entry.get("rows") or []))
        if not force and _cache_fresh(entry) and (slate is None or rows_are_fresh(cached_rows, slate)):
            index[pk] = cached_rows
            continue
        rows: list[dict[str, Any]] = []
        used_slug = slugs[0]
        used_tour = tour
        if sess is not None:
            rows, used_slug, used_tour = _fetch_best_ta_rows(slugs, tour, sess, slate)
        if rows and (slate is None or rows_are_fresh(rows, slate) or not cached_rows):
            cache_players[pk] = {
                "slug": used_slug,
                "tour": used_tour,
                "fetched_at": datetime.now(timezone.utc).isoformat(),
                "rows": rows,
            }
            index[pk] = rows
        elif cached_rows:
            index[pk] = cached_rows
        elif rows:
            index[pk] = rows

    try:
        _save_cache(path, cache)
    except Exception as exc:
        print(f"[ta] cache save failed: {exc}")
    return index


def ta_history(
    index: dict[str, list[dict[str, Any]]],
    player_name: str,
    prop_norm: str,
    last_n: int = 10,
    *,
    line: object = None,
    tour: str = "ATP",
) -> list[float]:
    if prop_norm not in TA_FILL_KEYS:
        return []
    rows = index.get(ta_index_key(player_name, tour)) or []
    return collect_history_values(rows, prop_norm, last_n, line=line)
