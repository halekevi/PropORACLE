"""Saves pairs, stored identity, and ESPN box for the board opponent's game.

Sep 1–27 step8 Saves / Shots / SOT. A stored actual is compared with the
ESPN summary only when the event's opponent matches the board opponent.
"""
from __future__ import annotations

import json
import sys
import time
import unicodedata
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from fetch_actuals import (  # noqa: E402
    HEADERS,
    SOCCER_SCOREBOARD_BASE,
    SOCCER_SCOREBOARD_BASE_WEB,
    SOCCER_SUMMARY_BASE,
    SOCCER_SUMMARY_BASE_WEB,
    _espn_soccer_get,
    parse_soccer_boxscore,
)
from utils.prop_norm import canon_prop  # noqa: E402

CACHE = ROOT / "data" / "reports" / "_espn_soccer_cache"
PROPS = {"saves", "shots", "sog"}


def _p(*a):
    print(*a, flush=True)


def _norm(name: object) -> str:
    raw = unicodedata.normalize("NFKD", str(name or ""))
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    return " ".join(raw.casefold().split())


def _last(name: str) -> str:
    parts = _norm(name).split()
    return parts[-1] if parts else ""


def _team_match(board: str, espn_names: list[str]) -> bool:
    nb = _norm(board)
    if not nb:
        return False
    for raw in espn_names:
        ne = _norm(raw)
        if not ne:
            continue
        if nb == ne or nb in ne or ne in nb:
            return True
        lb, le = _last(board), _last(raw)
        if lb and le and lb == le and len(lb) >= 4:
            return True
    return False


def _player_match(a: str, b: str) -> bool:
    na, nb = _norm(a), _norm(b)
    if not na or not nb:
        return False
    if na == nb or na in nb or nb in na:
        return True
    la, lb = _last(a), _last(b)
    return bool(la and lb and la == lb and len(la) >= 4)


def _get(url_api: str, url_web: str, cache_name: str):
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / cache_name
    if path.is_file():
        return json.loads(path.read_text(encoding="utf-8"))
    r = _espn_soccer_get(url_api, url_web)
    data = r.json()
    path.write_text(json.dumps(data), encoding="utf-8")
    time.sleep(0.15)
    return data


def _load_actuals(start: str, end: str):
    """player-day-prop -> actual. player-day -> league, team. day-team -> players' SOT and goals."""
    actual: dict[tuple[str, str, str], float] = {}
    league_of: dict[tuple[str, str], str] = {}
    team_of: dict[tuple[str, str], str] = {}
    player_sot: dict[tuple[str, str], float] = {}
    player_g: dict[tuple[str, str], float] = {}
    root = MAIN / "outputs"
    day = date.fromisoformat(start)
    stop = date.fromisoformat(end)
    while day <= stop:
        stamp = day.isoformat()
        path = root / stamp / f"actuals_soccer_{stamp}.csv"
        day += timedelta(days=1)
        if not path.is_file():
            continue
        df = pd.read_csv(path, dtype=str)
        for _, r in df.iterrows():
            player = _norm(r.get("player"))
            team = str(r.get("team") or "").strip().upper()
            if player and team:
                team_of[(player, stamp)] = team
            league = str(r.get("league") or "").strip()
            if player and league:
                league_of[(player, stamp)] = league
            try:
                val = float(r.get("actual"))
            except (TypeError, ValueError):
                val = None
            prop = canon_prop("SOCCER", str(r.get("prop_type") or ""))
            if player and prop in PROPS and val is not None:
                actual[(player, stamp, prop)] = val
            if player and team:
                try:
                    player_sot[(player, stamp)] = float(r.get("SOT"))
                except (TypeError, ValueError):
                    pass
                try:
                    player_g[(player, stamp)] = float(r.get("G"))
                except (TypeError, ValueError):
                    pass
    return actual, league_of, team_of, player_sot, player_g


def _load_step8(start: str, end: str):
    rows = []
    day = date.fromisoformat(start)
    stop = date.fromisoformat(end)
    while day <= stop:
        stamp = day.isoformat()
        path = MAIN / "outputs" / stamp / "soccer" / "step8_soccer_direction.csv"
        if not path.is_file():
            path = ROOT / "outputs" / stamp / "soccer" / "step8_soccer_direction.csv"
        day += timedelta(days=1)
        if not path.is_file():
            continue
        df = pd.read_csv(path, dtype=str, encoding_errors="replace")
        for _, r in df.iterrows():
            prop = canon_prop("SOCCER", str(r.get("prop_type") or ""))
            if prop not in PROPS:
                continue
            player = _norm(r.get("player"))
            if not player:
                continue
            try:
                line = float(r.get("line"))
            except (TypeError, ValueError):
                line = None
            pick = str(r.get("pick_type") or "").lower()
            if "goblin" in pick:
                kind = "Goblin"
            elif "standard" in pick:
                kind = "Standard"
            else:
                continue
            rows.append(
                {
                    "day": stamp,
                    "player": player,
                    "player_raw": str(r.get("player") or ""),
                    "prop": prop,
                    "kind": kind,
                    "line": line,
                    "team": str(r.get("team") or "").strip(),
                    "opp": str(r.get("opp_team") or "").strip(),
                }
            )
    return rows


def _pairs(rows, actual) -> None:
    legs: dict[tuple[str, str], list] = defaultdict(list)
    for r in rows:
        if r["prop"] != "saves" or r["line"] is None:
            continue
        legs[(r["player"], r["day"])].append(r)
    both = decided = 0
    g_hits = s_hits = 0
    gaps = []
    for key, items in legs.items():
        g_lines = [it["line"] for it in items if it["kind"] == "Goblin"]
        s_lines = [it["line"] for it in items if it["kind"] == "Standard"]
        if not g_lines or not s_lines:
            continue
        both += 1
        g, s = min(g_lines), min(s_lines)
        act = actual.get((key[0], key[1], "saves"))
        if act is None:
            continue
        decided += 1
        g_hits += int(act > g)
        s_hits += int(act > s)
        gaps.append(s - g)
    _p(f"pairs with both lines: {both}")
    _p(f"decided pairs: {decided}")
    if decided:
        _p(f"  Goblin hit {g_hits}/{decided} ({g_hits / decided:.1%})")
        _p(f"  Standard hit {s_hits}/{decided} ({s_hits / decided:.1%})")
        _p(f"  mean Standard−Goblin line gap {sum(gaps) / len(gaps):.2f}")


def _identity(rows, actual, team_of, player_sot, player_g) -> None:
    """Stored saves vs opponent SOT minus opponent goals, same actuals file."""
    # one value per player-day
    by_team: dict[tuple[str, str], dict[str, tuple[float, float]]] = defaultdict(dict)
    for (player, day), team in team_of.items():
        sot = player_sot.get((player, day))
        goals = player_g.get((player, day))
        if sot is None and goals is None:
            continue
        by_team[(day, team)][player] = (sot or 0.0, goals or 0.0)
    name_to_code: dict[tuple[str, str], str] = {}
    for r in rows:
        code = team_of.get((r["player"], r["day"]), "")
        if code and r["team"]:
            name_to_code[(r["day"], r["team"].strip().upper())] = code
    diffs = []
    saves_vals = []
    ident_vals = []
    seen = set()
    for r in rows:
        if r["prop"] != "saves" or r["kind"] != "Goblin":
            continue
        key = (r["player"], r["day"])
        if key in seen:
            continue
        seen.add(key)
        sv = actual.get((r["player"], r["day"], "saves"))
        if sv is None:
            continue
        own = team_of.get(key, "")
        opp_code = name_to_code.get((r["day"], r["opp"].strip().upper()), "")
        if not opp_code or opp_code == own:
            continue
        players = by_team.get((r["day"], opp_code)) or {}
        if not players:
            continue
        sot = sum(v[0] for v in players.values())
        goals = sum(v[1] for v in players.values())
        ident = sot - goals
        diffs.append(sv - ident)
        saves_vals.append(sv)
        ident_vals.append(ident)
    n = len(diffs)
    _p(f"stored identity (saves vs opp SOT − opp goals): n={n}")
    if n:
        _p(f"  mean stored saves {sum(saves_vals) / n:.2f}")
        _p(f"  mean opp SOT − goals {sum(ident_vals) / n:.2f}")
        _p(f"  mean stored − identity {sum(diffs) / n:.2f}")
        low = sum(1 for d in diffs if d <= -1)
        _p(f"  stored at least 1 below identity: {low}/{n}")


def _espn(rows, actual, league_of) -> None:
    # One comparison row per player-day-prop with a stored actual and a board opponent.
    want = []
    seen = set()
    for r in rows:
        if not r["opp"]:
            continue
        act = actual.get((r["player"], r["day"], r["prop"]))
        if act is None:
            continue
        key = (r["player"], r["day"], r["prop"])
        if key in seen:
            continue
        seen.add(key)
        league = league_of.get((r["player"], r["day"]), "")
        if not league or league.upper() == "SOCCER":
            continue
        want.append({**r, "actual": act, "league": league})
    _p(f"rows with stored actual, board opponent, and a league: {len(want)}")
    by_league_day: dict[tuple[str, str], list] = defaultdict(list)
    for r in want:
        by_league_day[(r["day"], r["league"])].append(r)

    # event_id -> parsed rows
    boxes: dict[str, list] = {}
    matched = defaultdict(list)
    no_event = 0
    ambiguous = 0
    no_player = 0
    for (day, league), group in sorted(by_league_day.items()):
        date_espn = day.replace("-", "")
        try:
            payload = _get(
                SOCCER_SCOREBOARD_BASE.format(league=league, date_espn=date_espn),
                SOCCER_SCOREBOARD_BASE_WEB.format(league=league, date_espn=date_espn),
                f"sb_{league}_{date_espn}.json",
            )
        except Exception as exc:
            _p(f"  scoreboard fail {day} {league}: {exc}")
            no_event += len(group)
            continue
        events = payload.get("events") or []
        for r in group:
            hits = []
            for ev in events:
                comps = (ev.get("competitions") or [{}])[0].get("competitors") or []
                names = []
                for c in comps:
                    team = c.get("team") or {}
                    names.extend(
                        [
                            team.get("displayName") or "",
                            team.get("shortDisplayName") or "",
                            team.get("abbreviation") or "",
                            team.get("name") or "",
                        ]
                    )
                if _team_match(r["opp"], names):
                    hits.append(ev.get("id"))
            hits = [h for h in hits if h]
            if not hits:
                no_event += 1
                continue
            if len(set(hits)) > 1:
                ambiguous += 1
                continue
            eid = hits[0]
            if eid not in boxes:
                try:
                    summary = _get(
                        SOCCER_SUMMARY_BASE.format(league=league, event_id=eid),
                        SOCCER_SUMMARY_BASE_WEB.format(league=league, event_id=eid),
                        f"sum_{league}_{eid}.json",
                    )
                    boxes[eid] = parse_soccer_boxscore(summary, league)
                except Exception as exc:
                    _p(f"  summary fail {eid}: {exc}")
                    boxes[eid] = []
            found = None
            for er in boxes[eid]:
                if canon_prop("SOCCER", er.get("prop_type") or "") != r["prop"]:
                    continue
                if _player_match(r["player_raw"], er.get("player") or ""):
                    found = er
                    break
            if found is None:
                no_player += 1
                continue
            matched[r["prop"]].append((r["actual"], float(found["actual"]), eid, r))
        _p(f"  {day} {league}: compared running {sum(len(v) for v in matched.values())}")

    _p(
        f"ESPN board-opponent: no event {no_event}, ambiguous {ambiguous}, "
        f"player missing {no_player}"
    )
    for prop in ("saves", "shots", "sog"):
        pairs = matched.get(prop) or []
        if not pairs:
            _p(f"  {prop}: n=0")
            continue
        stored = [a for a, _, _, _ in pairs]
        espn = [b for _, b, _, _ in pairs]
        diff = [a - b for a, b, _, _ in pairs]
        agree = sum(1 for d in diff if abs(d) < 0.51)
        low = sum(1 for d in diff if d <= -1)
        _p(
            f"  {prop}: n={len(pairs)} mean stored {sum(stored)/len(pairs):.2f} "
            f"mean ESPN {sum(espn)/len(pairs):.2f} mean stored−ESPN {sum(diff)/len(diff):.2f} "
            f"agree {agree}/{len(pairs)} stored≤ESPN−1 {low}/{len(pairs)}"
        )

    # Saves vs ESPN opponent SOT − goals on the matched event.
    ident = []
    for stored_sv, espn_sv, eid, r in matched.get("saves") or []:
        box = boxes.get(eid) or []
        # Opponent side: players whose team is not the keeper's ESPN team.
        keeper_team = ""
        for er in box:
            if _player_match(r["player_raw"], er.get("player") or "") and er.get("team"):
                keeper_team = str(er.get("team") or "")
                break
        if not keeper_team:
            continue
        seen_p = set()
        sot = goals = 0.0
        for er in box:
            if str(er.get("team") or "") == keeper_team:
                continue
            who = _norm(er.get("player"))
            if who in seen_p:
                continue
            seen_p.add(who)
            try:
                sot += float(er.get("SOT") or 0)
            except (TypeError, ValueError):
                pass
            try:
                goals += float(er.get("G") or 0)
            except (TypeError, ValueError):
                pass
        ident.append((stored_sv, espn_sv, sot - goals))
    if ident:
        _p(f"saves vs ESPN (opp SOT − goals) on matched events: n={len(ident)}")
        _p(f"  mean stored saves {sum(a for a, _, _ in ident) / len(ident):.2f}")
        _p(f"  mean ESPN saves {sum(b for _, b, _ in ident) / len(ident):.2f}")
        _p(f"  mean ESPN opp SOT − goals {sum(c for _, _, c in ident) / len(ident):.2f}")


def main() -> None:
    start, end = "2026-09-01", "2026-09-27"
    actual, league_of, team_of, player_sot, player_g = _load_actuals(start, end)
    rows = _load_step8(start, end)
    _p(f"step8 rows {len(rows)} stored prop cells {len(actual)}")
    _pairs(rows, actual)
    _identity(rows, actual, team_of, player_sot, player_g)
    _espn(rows, actual, league_of)


if __name__ == "__main__":
    main()
