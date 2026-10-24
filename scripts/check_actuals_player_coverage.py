"""Alert when started-slate players are missing from that night's actuals.

Only players whose game had started on the Eastern slate day, in a covered
competition, enter the denominator. Day-ahead board rows and uncovered cups
do not count, so the alarm does not fire every night on soccer cups.
"""
from __future__ import annotations

import argparse
import sys
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from utils.eastern_slate_date import eastern_slate_date, event_start_raw, keep_event_on_slate  # noqa: E402
from utils.soccer_board_coverage import covered_events, event_matches_sides, match_sides  # noqa: E402

ALERT_SHARE = 0.03
ET = ZoneInfo("America/New_York")

FOOTBALL = {
    "nfl": "football/nfl",
    "cfb": "football/college-football",
}


def _name(raw: object) -> str:
    text = unicodedata.normalize("NFKD", str(raw or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return " ".join(text.casefold().split())


def _read_frame(path: Path) -> pd.DataFrame:
    if not path.is_file():
        return pd.DataFrame()
    try:
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        if path.suffix.lower() == ".csv":
            return pd.read_csv(path, dtype=str, encoding_errors="replace")
        if path.suffix.lower() in {".xlsx", ".xls"}:
            return pd.read_excel(path)
    except Exception:
        return pd.DataFrame()
    return pd.DataFrame()


def _actuals_players(path: Path) -> set[str]:
    df = _read_frame(path)
    cols = {str(c).lower(): c for c in df.columns}
    player_c = cols.get("player") or cols.get("player_name")
    if not player_c:
        return set()
    out = set()
    for raw in df[player_c]:
        name = _name(raw)
        if name and " + " not in name:
            out.add(name)
    return out


def _slate_files(day_dir: Path, sport: str) -> list[Path]:
    patterns = [f"step8*{sport}*", f"{sport}/step8*", f"step8_{sport}*"]
    found: list[Path] = []
    seen: set[str] = set()
    for pattern in patterns:
        for path in day_dir.glob(pattern):
            if path.suffix.lower() not in {".parquet", ".csv", ".xlsx", ".xls"}:
                continue
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            found.append(path)
    return found


def _parse_start(raw: object) -> datetime | None:
    text = str(raw or "").strip()
    if not text or text.lower() == "nan":
        return None
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(ET)


def _started_on_slate(start_raw: object, slate_date: str, *, now: datetime | None = None) -> bool:
    """True when the start is this Eastern day and tip/kick has passed."""
    day = eastern_slate_date(start_raw)
    if day != slate_date:
        return False
    dt = _parse_start(start_raw)
    if dt is None:
        return False
    clock = now or datetime.now(ET)
    # Past days are fully started. Same day needs tip in the past.
    if day < clock.date().isoformat():
        return True
    return dt <= clock


def _football_started_teams(league: str, slate_date: str) -> set[str]:
    from fetch_football_actuals import _espn_get_json, _scoreboard_urls

    path = FOOTBALL[league]
    teams: set[str] = set()
    for qday in (slate_date, (date.fromisoformat(slate_date) + timedelta(days=1)).isoformat()):
        payload = _espn_get_json(_scoreboard_urls(path, qday)) or {}
        for event in payload.get("events") or []:
            if not keep_event_on_slate(event_start_raw(event), slate_date, qday):
                continue
            status = (event.get("status") or {}).get("type") or {}
            state = str(status.get("state") or "")
            if state not in {"in", "post"} and not status.get("completed"):
                # pre: not started
                continue
            comps = event.get("competitions") or []
            comp = comps[0] if comps and isinstance(comps[0], dict) else {}
            for competitor in comp.get("competitors") or []:
                abbr = str(((competitor or {}).get("team") or {}).get("abbreviation") or "").strip().upper()
                if abbr:
                    teams.add(abbr)
    return teams


def _eligible_slate_players(day_dir: Path, sport: str, slate_date: str) -> set[str]:
    """Players on the slate whose game started that Eastern day in coverage."""
    players: set[str] = set()
    now = datetime.now(ET)
    football_teams = (
        _football_started_teams(sport, slate_date) if sport in FOOTBALL else set()
    )
    soccer_events = covered_events(slate_date) if sport == "soccer" else None

    for path in _slate_files(day_dir, sport):
        df = _read_frame(path)
        if df.empty:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        player_c = cols.get("player") or cols.get("player_name")
        if not player_c:
            continue
        start_c = cols.get("start_time") or cols.get("game_time") or cols.get("game time")
        team_c = cols.get("team")
        for _, row in df.iterrows():
            name = _name(row.get(player_c))
            if not name or " + " in name:
                continue
            start = row.get(start_c) if start_c else None
            if sport in FOOTBALL:
                team = str(row.get(team_c) or "").strip().upper() if team_c else ""
                if team and team in football_teams:
                    players.add(name)
                elif start is not None and _started_on_slate(start, slate_date, now=now):
                    players.add(name)
                continue
            if sport == "soccer":
                if not _started_on_slate(start, slate_date, now=now):
                    continue
                sides = match_sides(
                    {
                        "sport": "SOCCER",
                        "matchup": row.get(cols.get("matchup") or "") if cols.get("matchup") else "",
                        "team": row.get(team_c) if team_c else "",
                        "opp_team": row.get(cols.get("opp_team") or cols.get("opponent") or "")
                        if (cols.get("opp_team") or cols.get("opponent"))
                        else "",
                        "start_time": start,
                    }
                )
                if sides is None or not soccer_events:
                    continue
                if any(event_matches_sides(sides[0], sides[1], ev) for ev in soccer_events):
                    players.add(name)
                continue
            # Tennis and other sports: Eastern start on this day, tip passed.
            if _started_on_slate(start, slate_date, now=now):
                players.add(name)
    return players


def _soccer_game_gaps(day_dir: Path, slate_date: str, have: set[str]) -> list[str]:
    """Alert when a covered started game has players on the board and zero actuals.

    Same shape as a missing NFL box (Rams at Denver): the slate share can look
    like a 30% gap when one match failed upstream.
    """
    lines: list[str] = []
    soccer_events = covered_events(slate_date) or ()
    if not soccer_events:
        return lines
    now = datetime.now(ET)
    # event_id -> {label, players}
    by_event: dict[str, dict] = {}
    for path in _slate_files(day_dir, "soccer"):
        df = _read_frame(path)
        if df.empty:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        player_c = cols.get("player") or cols.get("player_name")
        if not player_c:
            continue
        start_c = cols.get("start_time") or cols.get("game_time") or cols.get("game time")
        team_c = cols.get("team")
        for _, row in df.iterrows():
            name = _name(row.get(player_c))
            if not name or " + " in name:
                continue
            start = row.get(start_c) if start_c else None
            if not _started_on_slate(start, slate_date, now=now):
                continue
            sides = match_sides(
                {
                    "sport": "SOCCER",
                    "matchup": row.get(cols.get("matchup") or "") if cols.get("matchup") else "",
                    "team": row.get(team_c) if team_c else "",
                    "opp_team": row.get(cols.get("opp_team") or cols.get("opponent") or "")
                    if (cols.get("opp_team") or cols.get("opponent"))
                    else "",
                    "start_time": start,
                }
            )
            if sides is None:
                continue
            for ev in soccer_events:
                if not event_matches_sides(sides[0], sides[1], ev):
                    continue
                eid = str(ev.get("id") or "")
                label = str(ev.get("name") or ev.get("shortName") or eid)
                bucket = by_event.setdefault(
                    eid, {"label": label, "players": set(), "starters": set()}
                )
                bucket["players"].add(name)
                starter = str(
                    row.get(cols.get("starter_tier") or "") if cols.get("starter_tier") else ""
                ).upper()
                if "STARTER" in starter or "ROTATION" in starter:
                    bucket["starters"].add(name)
                break
    for eid, info in sorted(by_event.items(), key=lambda kv: kv[1]["label"]):
        players = info["players"]
        if not players:
            continue
        missing = sorted(p for p in players if p not in have)
        present = len(players) - len(missing)
        if present == 0 and missing:
            lines.append(
                f"ALERT [player-coverage] soccer {slate_date} game={info['label']}: "
                f"{len(missing)}/{len(players)} started covered players have no actual "
                f"(zero rows for this match)"
            )
        elif missing and len(missing) / len(players) > ALERT_SHARE:
            lines.append(
                f"ALERT [player-coverage] soccer {slate_date} game={info['label']}: "
                f"{len(missing)}/{len(players)} started covered players have no actual"
            )
    return lines


def sport_gaps(day_dir: Path, threshold: float = ALERT_SHARE) -> list[str]:
    lines: list[str] = []
    if not day_dir.is_dir():
        return lines
    day = day_dir.name
    for actuals in sorted(day_dir.glob(f"actuals_*_{day}.csv")):
        parts = actuals.name.split("_")
        if len(parts) < 3:
            continue
        sport = parts[1]
        slate_players = _eligible_slate_players(day_dir, sport, day)
        if len(slate_players) < 10:
            lines.append(
                f"[player-coverage] {sport} {day}: "
                f"{len(slate_players)} started covered players (skip; need 10)"
            )
            continue
        have = _actuals_players(actuals)
        missing = sorted(name for name in slate_players if name not in have)
        share = len(missing) / len(slate_players)
        label = (
            f"{len(missing)}/{len(slate_players)} started covered players "
            f"({share:.1%}) have no actual row"
        )
        if share > threshold:
            lines.append(f"ALERT [player-coverage] {sport} {day}: {label}")
        else:
            lines.append(f"[player-coverage] {sport} {day}: {label}")
        if sport == "soccer":
            lines.extend(_soccer_game_gaps(day_dir, day, have))
    return lines


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True)
    ap.add_argument("--outputs", default="")
    ap.add_argument("--threshold", type=float, default=ALERT_SHARE)
    args = ap.parse_args()
    day_dir = Path(args.outputs) if args.outputs else ROOT / "outputs" / args.date
    lines = sport_gaps(day_dir, args.threshold)
    if not lines:
        print(f"[player-coverage] {args.date}: no actuals files")
        return
    for line in lines:
        print(line)


if __name__ == "__main__":
    main()
