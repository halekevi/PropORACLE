"""Flag legs whose L5 window predates a roster / team change.

Detects:
  1. **player_team_mismatch** — player's L5 boxes were for a different team
     than tonight's slate team (trade / FA signing).
  2. **team_churn** — tonight's team lost a prior top scorer or added players
     whose L5 was elsewhere. Flags *every* player on that team so shared
     usage (Tatum without Brown, Queta with Robinson, etc.) is visible.

Never drops a leg. Attaches columns + a ``roster_changed`` flag for grading.
"""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Mapping

import pandas as pd

# ESPN / box DB often uses NY; PrizePicks slate uses NYK.
_NBA_TEAM_ALIASES: dict[str, str] = {
    "NY": "NYK",
    "NO": "NOP",
    "SA": "SAS",
    "GS": "GSW",
    "PHO": "PHX",
    "WSH": "WAS",
    "UTAH": "UTA",
    "BRK": "BKN",
    "BK": "BKN",
}

# How many prior-season scoring leaders to treat as "stars" for churn.
_TOP_SCORER_N = 3
# Prior window ends just before the new season tip (offseason trades land).
_DEFAULT_SEASON_CUTOVER = "-10-01"


@dataclass
class RosterChangeHit:
    changed: bool = False
    reasons: list[str] = field(default_factory=list)
    note: str = ""
    l5_teams: tuple[str, ...] = ()
    departed: tuple[str, ...] = ()
    arrived: tuple[str, ...] = ()
    new_teammates: tuple[str, ...] = ()


def norm_nba_team(raw: object) -> str:
    s = str(raw or "").strip().upper()
    if not s or s in {"NAN", "NONE", "NAT"}:
        return ""
    # Combo rows like DET/BOS — take first token only at caller when needed.
    if "/" in s:
        s = s.split("/", 1)[0].strip()
    return _NBA_TEAM_ALIASES.get(s, s)


def norm_player(raw: object) -> str:
    return " ".join(str(raw or "").strip().lower().split())


def default_nba_db() -> Path:
    here = Path(__file__).resolve()
    candidates = [
        here.parents[1] / "Sports" / "NBA" / "data" / "cache" / "proporacle_ref.db",
        here.parents[1] / "data" / "cache" / "proporacle_ref.db",
    ]
    for p in candidates:
        if p.is_file():
            return p
    return candidates[0]


def season_cutover_ymd(slate_date: str) -> str:
    """Return YYYY-10-01 for the slate's calendar year (NBA season open)."""
    y = str(slate_date or "").strip()[:4]
    if not y.isdigit():
        return f"2026{_DEFAULT_SEASON_CUTOVER}"
    return f"{y}{_DEFAULT_SEASON_CUTOVER}"


def _player_l5_teams(
    con: sqlite3.Connection,
    player: str,
    *,
    before_ymd: str,
    n: int = 5,
) -> list[tuple[str, str]]:
    """Return [(game_date, team_abbr), ...] most recent n boxes before cutover/slate."""
    rows = con.execute(
        """
        SELECT game_date, team
        FROM nba
        WHERE player = ?
          AND game_date < ?
          AND team IS NOT NULL
          AND TRIM(team) != ''
        ORDER BY game_date DESC
        LIMIT ?
        """,
        (player, before_ymd, int(n)),
    ).fetchall()
    out: list[tuple[str, str]] = []
    for gd, team in rows:
        ab = norm_nba_team(team)
        if ab:
            out.append((str(gd)[:10], ab))
    return out


def _prior_team_scorers(
    con: sqlite3.Connection,
    team: str,
    *,
    season_end_ymd: str,
    lookback_start: str,
) -> list[tuple[str, float]]:
    """Prior-season points leaders for team (name, sum_pts) descending."""
    team_u = norm_nba_team(team)
    if not team_u:
        return []
    # Match both slate abbr and DB alias (NYK vs NY).
    aliases = {team_u}
    for raw, canon in _NBA_TEAM_ALIASES.items():
        if canon == team_u:
            aliases.add(raw)
    placeholders = ",".join("?" for _ in aliases)
    rows = con.execute(
        f"""
        SELECT player, SUM(pts) AS s
        FROM nba
        WHERE team IN ({placeholders})
          AND game_date >= ?
          AND game_date < ?
        GROUP BY player
        HAVING s IS NOT NULL
        ORDER BY s DESC
        LIMIT 20
        """,
        (*sorted(aliases), lookback_start, season_end_ymd),
    ).fetchall()
    return [(str(p), float(s or 0)) for p, s in rows if str(p or "").strip()]


def _prior_team_roster(
    con: sqlite3.Connection,
    team: str,
    *,
    season_end_ymd: str,
    lookback_start: str,
) -> set[str]:
    """Every player who logged a box for the team in the prior window."""
    team_u = norm_nba_team(team)
    if not team_u:
        return set()
    aliases = {team_u}
    for raw, canon in _NBA_TEAM_ALIASES.items():
        if canon == team_u:
            aliases.add(raw)
    placeholders = ",".join("?" for _ in aliases)
    rows = con.execute(
        f"""
        SELECT DISTINCT player
        FROM nba
        WHERE team IN ({placeholders})
          AND game_date >= ?
          AND game_date < ?
        """,
        (*sorted(aliases), lookback_start, season_end_ymd),
    ).fetchall()
    return {norm_player(p) for (p,) in rows if str(p or "").strip()}


def attach_roster_change_columns(
    df: pd.DataFrame,
    *,
    sport: str = "NBA",
    slate_date: str | None = None,
    db_path: Path | str | None = None,
) -> pd.DataFrame:
    """Add roster_changed / roster_note / new_teammates columns. Idempotent."""
    if df is None or getattr(df, "empty", True):
        return df
    sport_u = str(sport or "").strip().upper()
    if sport_u not in {"NBA", "NBAP"}:
        # Other sports: leave columns blank; flag path is NBA-first.
        out = df.copy()
        if "roster_changed" not in out.columns:
            out["roster_changed"] = 0
            out["roster_note"] = ""
            out["new_teammates"] = ""
            out["roster_change_reason"] = ""
        return out

    out = df.copy()
    date = str(
        slate_date
        or (out["game_date"].iloc[0] if "game_date" in out.columns else "")
        or (out["board_date"].iloc[0] if "board_date" in out.columns else "")
    ).strip()[:10]
    if not date:
        date = "2026-10-20"
    cut = season_cutover_ymd(date)
    # Prior season sample: Jan 1 of cutover year through cutover.
    y = int(cut[:4])
    lookback = f"{y}-01-01"

    db = Path(db_path) if db_path else default_nba_db()
    if not db.is_file():
        out["roster_changed"] = 0
        out["roster_note"] = ""
        out["new_teammates"] = ""
        out["roster_change_reason"] = ""
        return out

    player_col = "player" if "player" in out.columns else ("Player" if "Player" in out.columns else None)
    team_col = "team" if "team" in out.columns else ("Team" if "Team" in out.columns else None)
    if not player_col or not team_col:
        out["roster_changed"] = 0
        out["roster_note"] = ""
        out["new_teammates"] = ""
        out["roster_change_reason"] = ""
        return out

    # Current slate roster by team (skip combo rows).
    slate_roster: dict[str, set[str]] = defaultdict(set)
    slate_names: dict[str, str] = {}  # norm -> display
    for _, row in out.iterrows():
        pname = str(row.get(player_col) or "").strip()
        if not pname or "+" in pname:
            continue
        team = norm_nba_team(row.get(team_col))
        if not team:
            continue
        key = norm_player(pname)
        slate_roster[team].add(key)
        slate_names[key] = pname

    con = sqlite3.connect(str(db))
    try:
        # Per-player L5 teams + mismatch
        player_hits: dict[str, RosterChangeHit] = {}
        arrivals_by_team: dict[str, list[str]] = defaultdict(list)

        unique_players = sorted(
            {
                str(r.get(player_col) or "").strip()
                for _, r in out.iterrows()
                if str(r.get(player_col) or "").strip() and "+" not in str(r.get(player_col) or "")
            }
        )
        for pname in unique_players:
            l5 = _player_l5_teams(con, pname, before_ymd=date, n=5)
            teams = tuple(dict.fromkeys(t for _, t in l5))  # order-preserving unique
            hit = RosterChangeHit(l5_teams=teams)
            # slate team for this player (first non-combo row)
            slate_team = ""
            for _, r in out.iterrows():
                if str(r.get(player_col) or "").strip() == pname:
                    slate_team = norm_nba_team(r.get(team_col))
                    if slate_team:
                        break
            if teams and slate_team and any(t != slate_team for t in teams):
                hit.changed = True
                hit.reasons.append("player_team_mismatch")
                prior = ",".join(teams)
                hit.note = f"L5 on {prior}; now {slate_team}"
                arrivals_by_team[slate_team].append(pname)
            player_hits[norm_player(pname)] = hit

        # Team-level churn: departed prior top scorers + notable arrivals
        team_churn: dict[str, RosterChangeHit] = {}
        for team, current in slate_roster.items():
            prior_scorers = _prior_team_scorers(
                con, team, season_end_ymd=cut, lookback_start=lookback
            )
            prior_roster = {norm_player(p) for p, _ in prior_scorers}
            top = prior_scorers[:_TOP_SCORER_N]
            departed = [
                slate_names.get(norm_player(p), p)
                for p, _ in top
                if norm_player(p) not in current
            ]
            # Arrivals: on slate, not in prior roster (or already flagged mismatch)
            arrived_names = []
            for key in sorted(current):
                disp = slate_names.get(key, key)
                if key not in prior_roster:
                    arrived_names.append(disp)
                elif key in player_hits and "player_team_mismatch" in player_hits[key].reasons:
                    if disp not in arrived_names:
                        arrived_names.append(disp)
            # Also include mismatch arrivals we tracked
            for a in arrivals_by_team.get(team, []):
                if a not in arrived_names:
                    arrived_names.append(a)

            if departed or arrived_names:
                # Only treat as churn when a top scorer left OR 2+ arrivals
                if departed or len(arrived_names) >= 2 or (
                    arrived_names and any(
                        "player_team_mismatch" in player_hits.get(norm_player(a), RosterChangeHit()).reasons
                        for a in arrived_names
                    )
                ):
                    bits = []
                    if departed:
                        bits.append("out " + ", ".join(departed))
                    if arrived_names:
                        bits.append("in " + ", ".join(arrived_names[:6]))
                    team_churn[team] = RosterChangeHit(
                        changed=True,
                        reasons=["team_churn"],
                        note=f"{team}: " + "; ".join(bits),
                        departed=tuple(departed),
                        arrived=tuple(arrived_names[:8]),
                        new_teammates=tuple(arrived_names[:8]),
                    )
    finally:
        con.close()

    # Write columns
    changed_col: list[int] = []
    note_col: list[str] = []
    mates_col: list[str] = []
    reason_col: list[str] = []

    for _, row in out.iterrows():
        pname = str(row.get(player_col) or "").strip()
        team = norm_nba_team(row.get(team_col))
        key = norm_player(pname.split("+")[0].strip()) if pname else ""
        notes: list[str] = []
        reasons: list[str] = []
        mates: list[str] = []
        ph = player_hits.get(key)
        if ph and ph.changed:
            notes.append(ph.note)
            reasons.extend(ph.reasons)
        th = team_churn.get(team)
        if th and th.changed:
            notes.append(th.note)
            reasons.extend(th.reasons)
            # Arrivals on this team, excluding the player himself.
            mates = [
                a
                for a in (th.arrived or th.new_teammates or ())
                if norm_player(a) != key
            ]
        # Dedupe reasons/notes
        reasons = list(dict.fromkeys(reasons))
        notes = list(dict.fromkeys(n for n in notes if n))
        changed_col.append(1 if reasons else 0)
        note_col.append(" | ".join(notes))
        mates_col.append(", ".join(mates))
        reason_col.append("|".join(reasons))

    out["roster_changed"] = changed_col
    out["roster_note"] = note_col
    out["new_teammates"] = mates_col
    out["roster_change_reason"] = reason_col
    return out


def roster_change_from_row(row: Mapping[str, Any]) -> bool:
    v = row.get("roster_changed")
    if v is None:
        return False
    if isinstance(v, (int, float)):
        return int(v) == 1
    return str(v).strip().lower() in {"1", "true", "yes"}
