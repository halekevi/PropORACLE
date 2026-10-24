"""Grade 2026-10-21 Standards L5=5 slip as source standards_manual.

Uses PrizePicks/step8 board teams (not last-season boxes) to find the ESPN
game. Logs played + model faces. Appends grade_history track standards_manual.
"""
from __future__ import annotations

import json
import math
import sqlite3
import sys
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.nba_board_team import (  # noqa: E402
    annotate_board_team_changed,
    board_vs_last_box_mismatches,
    last_box_team,
    load_board_player_teams,
    team_changed_status,
)
from utils.nba_minutes_role import _norm_name  # noqa: E402
from utils.prop_face_gate import prop_key  # noqa: E402
from utils.roster_change import norm_nba_team  # noqa: E402

SLATE = "2026-10-21"
TRACK = "standards_manual"
OUT = ROOT / "data" / "reports" / f"standards_manual_grade_{SLATE}.json"
MISMATCH_OUT = ROOT / "data" / "reports" / f"nba_board_team_mismatch_{SLATE}.json"
GRADE_HISTORY = ROOT / "data" / "grade_history.json"

LEGS: list[dict[str, Any]] = [
    {
        "sport": "WNBA",
        "player": "Kaitlyn Chen",
        "play_prop": "Points",
        "play_line": 4.5,
        "play_side": "OVER",
        "model_prop": "Pts+Asts",
        "model_line": 5.5,
        "model_side": "OVER",
        "pick_type": "Standard",
        "face_swap": True,
    },
    {
        "sport": "WNBA",
        "player": "Veronica Burton",
        "play_prop": "Pts+Asts",
        "play_line": 13.5,
        "play_side": "UNDER",
        "model_prop": "Pts+Asts",
        "model_line": 13.5,
        "model_side": "UNDER",
        "pick_type": "Standard",
        "face_swap": False,
    },
    {
        "sport": "NBA",
        "player": "Isaiah Jackson",
        "play_prop": "Rebounds",
        "play_line": 7.5,
        "play_side": "UNDER",
        "model_prop": "Rebounds",
        "model_line": 7.5,
        "model_side": "UNDER",
        "pick_type": "Standard",
        "face_swap": False,
    },
    {
        "sport": "NBA",
        "player": "Darius Garland",
        "play_prop": "Points",
        "play_line": 23.5,
        "play_side": "UNDER",
        "model_prop": "Pts+Asts",
        "model_line": None,
        "model_side": "UNDER",
        "pick_type": "Standard",
        "face_swap": True,
        "note": "Pts+Asts thesis; Points face not re-gated",
    },
    {
        "sport": "NBA",
        "player": "Franz Wagner",
        "play_prop": "Pts+Rebs+Asts",
        "play_line": 32.5,
        "play_side": "UNDER",
        "model_prop": "Pts+Rebs",
        "model_line": 27.5,
        "model_side": "UNDER",
        "pick_type": "Standard",
        "face_swap": True,
    },
    {
        "sport": "NBA",
        "player": "Dejounte Murray",
        "play_prop": "Pts+Rebs+Asts",
        "play_line": 30.0,
        "play_side": "UNDER",
        "model_prop": "Pts+Rebs+Asts",
        "model_line": 30.0,
        "model_side": "UNDER",
        "pick_type": "Standard",
        "face_swap": False,
    },
]

PROP_TO_COLS = {
    "points": ("pts",),
    "rebounds": ("reb",),
    "assists": ("ast",),
    "pts+asts": ("pa", "pts", "ast"),
    "pts+rebs": ("pr", "pts", "reb"),
    "pts+rebs+asts": ("pra", "pts", "reb", "ast"),
}


def _grade_val(actual: float | None, line: float, side: str) -> str:
    if actual is None or (isinstance(actual, float) and math.isnan(actual)):
        return "UNGRADED"
    side_u = side.upper()
    if side_u == "OVER":
        if actual > line:
            return "HIT"
        if actual < line:
            return "MISS"
        return "PUSH"
    if actual < line:
        return "HIT"
    if actual > line:
        return "MISS"
    return "PUSH"


def _stat_from_row(row: dict[str, Any], prop: str) -> float | None:
    key = prop_key(prop)
    cols = PROP_TO_COLS.get(key)
    if not cols:
        return None
    primary = cols[0]
    if primary in row and row[primary] is not None:
        try:
            return float(row[primary])
        except (TypeError, ValueError):
            pass
    parts = []
    for c in cols[1:]:
        if c not in row or row[c] is None:
            return None
        try:
            parts.append(float(row[c]))
        except (TypeError, ValueError):
            return None
    return sum(parts) if parts else None


def _normalize_espn_row(name: str, keys: list[str], vals: list[Any], *, league: str) -> dict[str, Any]:
    row: dict[str, Any] = {"player": name, "source": f"espn_{league}"}
    if not vals:
        row.update(
            {
                "pts": 0.0,
                "reb": 0.0,
                "ast": 0.0,
                "pra": 0.0,
                "pr": 0.0,
                "pa": 0.0,
                "minutes": 0.0,
                "dnp_or_inactive": True,
            }
        )
        return row
    for i, k in enumerate(keys):
        if i >= len(vals):
            break
        raw = vals[i]
        try:
            row[k] = float(raw)
        except (TypeError, ValueError):
            if k in {"minutes", "min"} and isinstance(raw, str) and ":" in raw:
                mm, ss = raw.split(":", 1)
                row["minutes"] = float(mm) + float(ss) / 60.0
            continue
    if "points" in row:
        row["pts"] = float(row["points"])
    elif "pts" in row:
        row["pts"] = float(row["pts"])
    if "rebounds" in row:
        row["reb"] = float(row["rebounds"])
    elif "reb" in row:
        row["reb"] = float(row["reb"])
    if "assists" in row:
        row["ast"] = float(row["assists"])
    elif "ast" in row:
        row["ast"] = float(row["ast"])
    if all(k in row for k in ("pts", "reb", "ast")):
        row["pra"] = row["pts"] + row["reb"] + row["ast"]
        row["pr"] = row["pts"] + row["reb"]
        row["pa"] = row["pts"] + row["ast"]
    return row


def _espn_scoreboard(day: str, league: str) -> dict[str, Any]:
    ymd = day.replace("-", "")
    url = (
        f"https://site.api.espn.com/apis/site/v2/sports/basketball/{league}/"
        f"scoreboard?dates={ymd}"
    )
    with urllib.request.urlopen(url, timeout=45) as resp:
        return json.loads(resp.read())


def _espn_boxes_by_player_and_team(
    day: str, league: str
) -> tuple[dict[str, dict[str, Any]], dict[str, list[dict[str, Any]]], dict[str, dict[str, Any]]]:
    """Return (by_player, by_team_list, events_meta).

    by_team_list lets us confirm a player is on the board team’s game even when
    last-season boxes pointed at a different club.
    """
    board = _espn_scoreboard(day, league)
    by_player: dict[str, dict[str, Any]] = {}
    by_team: dict[str, list[dict[str, Any]]] = {}
    events_meta: dict[str, dict[str, Any]] = {}

    for ev in board.get("events") or []:
        st = ((ev.get("status") or {}).get("type") or {})
        eid = str(ev.get("id") or "")
        comps = (ev.get("competitions") or [{}])[0]
        teams = comps.get("competitors") or []
        abbrs = [norm_nba_team((t.get("team") or {}).get("abbreviation")) for t in teams]
        events_meta[eid] = {
            "status": st.get("name"),
            "completed": bool(st.get("completed")),
            "teams": abbrs,
            "label": " vs ".join(a for a in abbrs if a),
        }
        if st.get("name") in {"STATUS_SCHEDULED", "STATUS_POSTPONED"}:
            continue
        if not eid:
            continue
        sum_url = (
            f"https://site.api.espn.com/apis/site/v2/sports/basketball/{league}/"
            f"summary?event={eid}"
        )
        try:
            with urllib.request.urlopen(sum_url, timeout=45) as resp:
                summary = json.loads(resp.read())
        except Exception as exc:  # noqa: BLE001
            print(f"  warn summary {league} {eid}: {exc}")
            continue
        for team_block in (summary.get("boxscore") or {}).get("players") or []:
            team_abbr = norm_nba_team((team_block.get("team") or {}).get("abbreviation"))
            for stat_group in team_block.get("statistics") or []:
                keys = [str(k).lower() for k in (stat_group.get("keys") or [])]
                for ath in stat_group.get("athletes") or []:
                    name = ((ath.get("athlete") or {}).get("displayName") or "").strip()
                    if not name:
                        continue
                    row = _normalize_espn_row(
                        name, keys, ath.get("stats") or [], league=league
                    )
                    row["team"] = team_abbr
                    row["event_id"] = eid
                    row["event_status"] = st.get("name")
                    row["event_completed"] = bool(st.get("completed"))
                    pn = _norm_name(name)
                    by_player[pn] = row
                    if team_abbr:
                        by_team.setdefault(team_abbr, []).append(row)
    return by_player, by_team, events_meta


def _resolve_nba_row(
    player: str,
    board_team: str,
    by_player: dict[str, dict[str, Any]],
    events_meta: dict[str, dict[str, Any]],
) -> tuple[dict[str, Any] | None, str]:
    """Find the player’s box on the board-team game; else explain status."""
    pn = _norm_name(player)
    row = by_player.get(pn)
    if row is not None:
        # Prefer the live box even if team string differs slightly.
        return row, "found_in_box"

    # Game for board team not tipped / not found.
    team = norm_nba_team(board_team)
    for meta in events_meta.values():
        if team in (meta.get("teams") or []):
            status = str(meta.get("status") or "")
            if status in {"STATUS_SCHEDULED", "STATUS_POSTPONED"}:
                return None, f"game_scheduled:{meta.get('label')}"
            return None, f"not_in_box:{meta.get('label')}:{status}"
    return None, f"no_espn_game_for_board_team:{team}"


def _load_wnba_boxes(day: str) -> dict[str, dict[str, Any]]:
    by_player, _by_team, _meta = _espn_boxes_by_player_and_team(day, "wnba")
    if by_player:
        return by_player
    out: dict[str, dict[str, Any]] = {}
    db = ROOT / "data" / "cache" / "proporacle_ref.db"
    if not db.exists():
        return out
    con = sqlite3.connect(str(db))
    try:
        cur = con.execute(
            "SELECT player, pts, reb, ast, minutes FROM wnba "
            "WHERE substr(cast(game_date as text),1,10)=?",
            (day,),
        )
        for player, pts, reb, ast, minutes in cur.fetchall():
            row = {
                "player": player,
                "pts": pts,
                "reb": reb,
                "ast": ast,
                "minutes": minutes,
                "source": "db",
            }
            if pts is not None and reb is not None and ast is not None:
                row["pra"] = float(pts) + float(reb) + float(ast)
                row["pr"] = float(pts) + float(reb)
                row["pa"] = float(pts) + float(ast)
            out[_norm_name(player)] = row
    finally:
        con.close()
    return out


def grade_legs() -> dict[str, Any]:
    board_teams = load_board_player_teams(SLATE)
    nba_by_player, _nba_by_team, events_meta = _espn_boxes_by_player_and_team(SLATE, "nba")
    wnba = _load_wnba_boxes(SLATE)
    print(
        f"board_players={len(board_teams)} espn_nba_boxes={len(nba_by_player)} "
        f"wnba={len(wnba)} events={len(events_meta)}"
    )

    graded = []
    for leg in LEGS:
        sport = leg["sport"]
        player = leg["player"]
        pn = _norm_name(player)
        board_info = board_teams.get(pn) or {}
        board_team = board_info.get("team") or ""
        board_opp = board_info.get("opp") or ""
        box_team = last_box_team(player, as_of=SLATE)
        changed_st = (
            team_changed_status(player, board_team, slate=SLATE)
            if sport == "NBA" and board_team
            else {}
        )

        if sport == "WNBA":
            row = wnba.get(pn)
            resolve = "wnba_box" if row else "wnba_missing"
        else:
            row, resolve = _resolve_nba_row(player, board_team, nba_by_player, events_meta)

        play_actual = _stat_from_row(row or {}, leg["play_prop"]) if row else None
        model_actual = (
            _stat_from_row(row, leg["model_prop"])
            if row and leg.get("model_prop")
            else None
        )

        if row is None:
            if resolve.startswith("game_scheduled"):
                play_grade = "UNGRADED"
                model_grade = "UNGRADED" if leg.get("model_prop") else "N/A"
            elif resolve.startswith("no_espn_game"):
                play_grade = "NO_GAME"
                model_grade = "NO_GAME" if leg.get("model_prop") else "N/A"
            else:
                play_grade = "UNGRADED"
                model_grade = "UNGRADED" if leg.get("model_prop") else "N/A"
        else:
            play_grade = _grade_val(play_actual, float(leg["play_line"]), leg["play_side"])
            model_grade = "N/A"
            if leg.get("model_line") is not None and leg.get("model_prop"):
                model_grade = _grade_val(
                    model_actual, float(leg["model_line"]), str(leg["model_side"])
                )

        rec = {
            **leg,
            "board_team": board_team,
            "board_opp": board_opp,
            "last_box_team": box_team,
            "team_mismatch": bool(board_team and box_team and board_team != box_team),
            "team_changed": changed_st.get("team_changed"),
            "games_with_new_team": changed_st.get("games_with_new_team"),
            "power_l5_blocked": changed_st.get("power_l5_blocked"),
            "resolve": resolve,
            "play_actual": play_actual,
            "model_actual": model_actual,
            "play_grade": play_grade,
            "model_grade": model_grade,
            "actual_source": (row or {}).get("source"),
            "event_status": (row or {}).get("event_status"),
            "dnp_or_inactive": bool((row or {}).get("dnp_or_inactive")),
        }
        graded.append(rec)
        swap = " SWAP" if leg.get("face_swap") else ""
        mm = f" board={board_team}/{board_opp} last_box={box_team or '-'}"
        print(
            f"  {player:<18} play {leg['play_side'][0]}{leg['play_prop']} "
            f"{leg['play_line']} actual={play_actual} -> {play_grade}{swap} "
            f"[{resolve}]{mm}"
        )
        if leg.get("face_swap"):
            print(
                f"  {'':18} model {leg.get('model_side', '?')[0]}{leg.get('model_prop')} "
                f"{leg.get('model_line')} actual={model_actual} -> {model_grade}"
            )

    play_grades = [g["play_grade"] for g in graded]
    hits = sum(1 for g in play_grades if g == "HIT")
    misses = sum(1 for g in play_grades if g == "MISS")
    pushes = sum(1 for g in play_grades if g == "PUSH")
    no_game = sum(1 for g in play_grades if g == "NO_GAME")
    ungraded = sum(1 for g in play_grades if g == "UNGRADED")
    if ungraded or no_game:
        slip_result = "UNGRADED"
    elif misses:
        slip_result = "LOSS"
    elif hits >= 2:
        slip_result = "WIN"
    else:
        slip_result = "UNGRADED"

    swap_legs = [
        g
        for g in graded
        if g.get("face_swap")
        and g["play_grade"] in {"HIT", "MISS"}
        and g["model_grade"] in {"HIT", "MISS"}
    ]
    swap_cost = {
        "n": len(swap_legs),
        "play_hits": sum(1 for g in swap_legs if g["play_grade"] == "HIT"),
        "model_hits": sum(1 for g in swap_legs if g["model_grade"] == "HIT"),
        "play_miss_model_hit": [
            g["player"]
            for g in swap_legs
            if g["play_grade"] == "MISS" and g["model_grade"] == "HIT"
        ],
        "play_hit_model_miss": [
            g["player"]
            for g in swap_legs
            if g["play_grade"] == "HIT" and g["model_grade"] == "MISS"
        ],
    }

    return {
        "date": SLATE,
        "track": TRACK,
        "ticket_track": TRACK,
        "source": "Standards build, manual",
        "label": "Standards L5=5 manual 6-pick (played + model faces)",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "product": "Power",
        "n_legs": len(graded),
        "legs": graded,
        "hits": hits,
        "misses": misses,
        "pushes": pushes,
        "no_game": no_game,
        "ungraded": ungraded,
        "slip_result": slip_result,
        "face_swap_cost": swap_cost,
        "exclude_from_winrate": True,
        "note": "Board team from step8; re-run after LAC@SAC finals",
    }


def append_grade_history(payload: dict[str, Any]) -> None:
    runs: list[Any] = []
    if GRADE_HISTORY.exists():
        try:
            runs = json.loads(GRADE_HISTORY.read_text(encoding="utf-8"))
            if not isinstance(runs, list):
                runs = []
        except json.JSONDecodeError:
            runs = []
    ds = payload["date"]
    track = payload["track"]
    runs = [
        r
        for r in runs
        if not (
            isinstance(r, dict)
            and str(r.get("date", ""))[:10] == ds
            and str(r.get("track") or r.get("ticket_track") or "").strip().lower()
            == track
        )
    ]
    hist = {
        "date": ds,
        "track": track,
        "ticket_track": track,
        "source": payload["source"],
        "label": payload["label"],
        "generated_at": payload["generated_at"],
        "n_tickets": 1,
        "n_legs": payload["n_legs"],
        "hits": payload["hits"],
        "misses": payload["misses"],
        "pushes": payload["pushes"],
        "no_game": payload.get("no_game"),
        "ungraded": payload["ungraded"],
        "slip_result": payload["slip_result"],
        "face_swap_cost": payload["face_swap_cost"],
        "exclude_from_winrate": True,
        "detail_path": str(OUT.relative_to(ROOT)).replace("\\", "/"),
    }
    runs.append(hist)
    GRADE_HISTORY.parent.mkdir(parents=True, exist_ok=True)
    GRADE_HISTORY.write_text(
        json.dumps(runs, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(f"appended grade_history track={track} date={ds}")


def main() -> None:
    mismatch = board_vs_last_box_mismatches(SLATE)
    changed = annotate_board_team_changed(SLATE)
    mismatch["team_changed"] = {
        "team_changed_n": changed["team_changed_n"],
        "power_l5_blocked_n": changed["power_l5_blocked_n"],
        "min_new_team_games": changed["min_new_team_games"],
        "power_l5_blocked": changed["power_l5_blocked"],
    }
    MISMATCH_OUT.parent.mkdir(parents=True, exist_ok=True)
    MISMATCH_OUT.write_text(
        json.dumps(mismatch, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    print(
        f"team_changed={changed['team_changed_n']} "
        f"power_l5_blocked={changed['power_l5_blocked_n']} "
        f"(need {changed['min_new_team_games']} games on new club)"
    )
    latest = mismatch["vs_latest_box"]
    prior = mismatch["vs_prior_season_box"]
    print(
        f"board vs latest box mismatches: {latest['mismatch_n']}/{mismatch['board_players']} "
        f"| vs prior-RS box: {prior['mismatch_n']}/{mismatch['board_players']} "
        f"({100*prior['mismatch_rate']:.1f}%) opener={mismatch.get('opener')} -> {MISMATCH_OUT}"
    )
    for row in prior["mismatches"][:15]:
        print(
            f"  {row['player']:<22} board={row['board_team']:<3} "
            f"prior_box={row['last_box_team']:<3} opp={row['board_opp']}"
        )
    if prior["mismatch_n"] > 15:
        print(f"  ... +{prior['mismatch_n'] - 15} more")

    payload = grade_legs()
    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"wrote {OUT}")
    print(
        f"slip_result={payload['slip_result']} hits={payload['hits']} "
        f"misses={payload['misses']} ungraded={payload['ungraded']} "
        f"no_game={payload['no_game']}"
    )
    append_grade_history(payload)


if __name__ == "__main__":
    main()
