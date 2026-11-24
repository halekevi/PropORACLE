"""Add NFL games that ESPN box scores leave out (player appeared, recorded nothing).

ESPN box scores omit players with no stats, while ESPN's per-player game log and the
PrizePicks card count those games as 0. Without them L5/L10 skip the zero and averages
and Over hit rates read too high. For each board athlete this pulls
``site.web.api.espn.com/.../athletes/<id>/gamelog`` and returns a box-cache row for every
listed game missing from the cache, filled from the game log's own stat line (all zeros
when he recorded nothing). Rows carry ``SOURCE=espn_gamelog``.

A game log is re-pulled only when the box cache has a completed game newer than the
last pull; past seasons are pulled once.
"""
from __future__ import annotations

import json
import ssl
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "Sports" / "NFL" / "data" / "cache" / "gamelog"
URL = "https://site.web.api.espn.com/apis/common/v3/sports/football/nfl/athletes/{aid}/gamelog?season={season}"
ET = ZoneInfo("America/New_York")

STAT_COLS = (
    "PASS_YDS", "PASS_TD", "PASS_INT", "RUSH_YDS", "RUSH_TD", "REC_YDS", "REC", "REC_TD",
    "PASS_CMP", "SACK", "TACK_TOT", "KICK_PTS", "DEF_INT", "PASS_ATT", "RUSH_ATT",
    "RUSH_LONG", "REC_LONG", "REC_TGT", "KICK_FG", "KICK_XP",
)
COMMON = {
    "passingYards": "PASS_YDS", "passingTouchdowns": "PASS_TD", "completions": "PASS_CMP",
    "passingAttempts": "PASS_ATT", "rushingAttempts": "RUSH_ATT", "rushingYards": "RUSH_YDS",
    "rushingTouchdowns": "RUSH_TD", "longRushing": "RUSH_LONG", "receptions": "REC",
    "receivingTargets": "REC_TGT", "receivingYards": "REC_YDS", "receivingTouchdowns": "REC_TD",
    "longReception": "REC_LONG", "totalTackles": "TACK_TOT", "totalKickingPoints": "KICK_PTS",
    "fieldGoalsMade-fieldGoalAttempts": "KICK_FG", "extraPointsMade-extraPointAttempts": "KICK_XP",
}


def _ctx() -> ssl.SSLContext:
    try:
        import certifi

        return ssl.create_default_context(cafile=certifi.where())
    except Exception:
        return ssl.create_default_context()


def _num(raw) -> float:
    s = str(raw or "").strip()
    if "-" in s[1:]:
        s = s.split("-")[0] if not s.startswith("-") else s
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return 0.0


def _season_of(game_date: str) -> int:
    ds = str(game_date).replace("-", "")[:8]
    y, m = int(ds[:4]), int(ds[4:6])
    return y - 1 if m <= 7 else y


def _fetch(aid: str, season: int, ctx: ssl.SSLContext) -> dict | None:
    req = urllib.request.Request(URL.format(aid=aid, season=season), headers={"User-Agent": "Mozilla/5.0"})
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, context=ctx, timeout=30) as r:
                return json.load(r)
        except Exception:
            time.sleep(1.5 * (attempt + 1))
    return None


def _load(aid: str, season: int, newest_box: str, ctx: ssl.SSLContext, current_season: int) -> dict | None:
    path = CACHE_DIR / f"{aid}_{season}.json"
    if path.is_file():
        try:
            cached = json.loads(path.read_text(encoding="utf-8"))
            pulled = str(cached.get("_pulled_date", ""))
            if season < current_season or pulled > newest_box:
                return cached
        except Exception:
            pass
    data = _fetch(aid, season, ctx)
    if data is None:
        return None
    data["_pulled_date"] = datetime.now(ET).strftime("%Y%m%d")
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data), encoding="utf-8")
    return data


def _rows_from_gamelog(aid: str, data: dict, have: set, player_norm: str, season: int) -> list[dict]:
    names = list(data.get("names") or [])
    mapping = dict(COMMON)
    if "completions" in names:
        mapping.update({"sacks": "", "interceptions": "PASS_INT"})
    else:
        mapping.update({"sacks": "SACK", "interceptions": "DEF_INT"})
    if not any(mapping.get(n) for n in names):
        # Punters / long snappers: no tracked stat, so every game would read as a fake zero.
        return []
    events = data.get("events") or {}
    out: list[dict] = []
    for st in data.get("seasonTypes") or []:
        label = str(st.get("displayName") or "")
        if "Preseason" in label:
            continue
        for cat in st.get("categories") or []:
            for ev in cat.get("events") or []:
                eid = str(ev.get("eventId") or "")
                if not eid or eid in have:
                    continue
                meta = events.get(eid) or {}
                try:
                    gd = datetime.fromisoformat(str(meta.get("gameDate")).replace("Z", "+00:00"))
                except ValueError:
                    continue
                if gd > datetime.now(timezone.utc):
                    continue
                row = {c: 0.0 for c in STAT_COLS}
                for name, raw in zip(names, ev.get("stats") or []):
                    col = mapping.get(name)
                    if col:
                        row[col] = _num(raw)
                team = meta.get("team") or {}
                opp = meta.get("opponent") or {}
                row.update({
                    "team_id": str(team.get("id") or ""),
                    "player_norm": player_norm,
                    "espn_athlete_id": aid,
                    "game_date": gd.astimezone(ET).strftime("%Y%m%d"),
                    "event_id": eid,
                    "opp_team_abbr": str(opp.get("abbreviation") or ""),
                    "opp_team_id": str(opp.get("id") or ""),
                    "SEASON": str(season),
                    "MIN": 1.0,
                    "SOURCE": "espn_gamelog",
                })
                have.add(eid)
                out.append(row)
    return out


def zero_stat_rows(
    cached_rows: list[dict],
    board_aids: set[str],
    *,
    end_date: str,
    workers: int = 6,
) -> list[dict]:
    """Rows for games in each board athlete's ESPN game log that the box cache lacks."""
    aids = {str(a).strip() for a in board_aids if str(a).strip() not in ("", "nan", "None")}
    if not aids:
        return []
    have: dict[str, set] = {}
    names: dict[str, str] = {}
    seasons: dict[str, set] = {}
    newest_box = ""
    for r in cached_rows:
        gd = str(r.get("game_date", "")).replace("-", "")[:8]
        if len(gd) == 8 and gd > newest_box:
            newest_box = gd
        aid = str(r.get("espn_athlete_id", "")).strip()
        if aid not in aids:
            continue
        have.setdefault(aid, set()).add(str(r.get("event_id", "")))
        names[aid] = str(r.get("player_norm", "")) or names.get(aid, "")
        if len(gd) == 8:
            seasons.setdefault(aid, set()).add(_season_of(gd))
    current = _season_of(str(end_date).replace("-", "")[:8])
    ctx = _ctx()

    def one(aid: str) -> list[dict]:
        rows: list[dict] = []
        for season in sorted(seasons.get(aid, set()) | {current}):
            if season < current - 1:
                continue
            data = _load(aid, season, newest_box, ctx, current)
            if data:
                rows += _rows_from_gamelog(aid, data, have.setdefault(aid, set()), names.get(aid, ""), season)
        return rows

    out: list[dict] = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for rows in ex.map(one, sorted(aids)):
            out += rows
    return out
