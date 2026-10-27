"""Join injuries_latest.csv onto step8 / ranker boards (player + team)."""
from __future__ import annotations

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[1]
_SCRIPTS = _REPO / "scripts"
for _p in (_REPO, _SCRIPTS):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

from espn_injuries import canon_team_abbr  # noqa: E402
from player_name_norm import fold_player_name  # noqa: E402
from utils.injury_collect import load_latest_rows, latest_csv_path  # noqa: E402
from utils.injury_gate import (  # noqa: E402
    injury_snapshot_stale,
    is_injury_out,
    is_injury_soft_flag,
    normalize_inj_status,
)

TOP_N_TEAMMATES = 2


def _is_gleague_out(name: str, index: dict[tuple[str, str, str], dict[str, Any]], sport: str, team: str) -> bool:
    """G-League two-way / assignment outs do not open starter minutes."""
    fold = fold_player_name(name)
    lit = sport.upper()
    tm = canon_team_abbr(lit, team)
    for (sp, pl, t), rec in index.items():
        if sp != lit or pl != fold:
            continue
        if tm and t and t != tm:
            continue
        blob = f"{rec.get('injury') or ''} {rec.get('status') or ''}".lower()
        if "gleague" in blob.replace(" ", "") or "g-league" in blob or "two-way" in blob:
            return True
        if "on assignment" in blob or "onassignment" in blob.replace(" ", ""):
            return True
    return False


def _sport_of(row: dict[str, Any]) -> str:
    return str(row.get("sport") or row.get("Sport") or row.get("league") or "").strip().upper()


def _player_of(row: dict[str, Any]) -> str:
    for k in ("player", "player_name", "Player", "Name"):
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _team_of(row: dict[str, Any]) -> str:
    for k in ("team", "team_abbr", "Team", "Team Abbr"):
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def _opp_of(row: dict[str, Any]) -> str:
    for k in ("opponent", "opp", "Opponent", "opp_abbr", "Opp"):
        v = row.get(k)
        if v is not None and str(v).strip():
            return str(v).strip()
    return ""


def index_injuries(
    rows: list[dict[str, Any]] | None = None,
    *,
    root: Path | None = None,
) -> dict[tuple[str, str, str], dict[str, Any]]:
    """(sport, fold_player, team_canon) -> injury row (worst status wins)."""
    src = rows if rows is not None else load_latest_rows(root=root)
    rank = {"OUT": 0, "Doubtful": 1, "Questionable": 2, "GTD": 3, "Probable": 4}
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for r in src:
        sport = str(r.get("sport") or "").strip().upper()
        player = fold_player_name(r.get("player", ""))
        team = canon_team_abbr(sport, r.get("team", ""))
        if not player:
            continue
        key = (sport, player, team)
        status = normalize_inj_status(r.get("status"))
        prev = out.get(key)
        if prev is None:
            out[key] = dict(r)
            continue
        if rank.get(status, 9) < rank.get(normalize_inj_status(prev.get("status")), 9):
            out[key] = dict(r)
    return out


def team_out_players(
    index: dict[tuple[str, str, str], dict[str, Any]],
    sport: str,
    team: str,
) -> list[str]:
    lit = sport.upper()
    tm = canon_team_abbr(lit, team)
    names: list[str] = []
    for (sp, _pl, t), rec in index.items():
        if sp != lit or t != tm:
            continue
        if is_injury_out(rec.get("status")):
            names.append(str(rec.get("player") or "").strip())
    return sorted({n for n in names if n})


def attach_injury_columns(
    board: list[dict[str, Any]],
    *,
    root: Path | None = None,
    injuries: list[dict[str, Any]] | None = None,
    now: datetime | None = None,
    stale_hours: float = 6.0,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Add inj_status, inj_captured_at, team_out_list, opp_out_list. Log unmatched."""
    index = index_injuries(injuries, root=root)
    unmatched: list[dict[str, str]] = []
    matched = 0
    stale_n = 0
    out_n = 0
    soft_n = 0
    ref = now or datetime.now(timezone.utc)

    # Precompute out lists per sport/team
    teams_seen: set[tuple[str, str]] = set()
    for row in board:
        sp = _sport_of(row)
        tm = canon_team_abbr(sp, _team_of(row))
        if sp and tm:
            teams_seen.add((sp, tm))
        opp = canon_team_abbr(sp, _opp_of(row))
        if sp and opp:
            teams_seen.add((sp, opp))
    out_lists = {(sp, tm): team_out_players(index, sp, tm) for sp, tm in teams_seen}

    # Top-2 board usage proxy per team (avg_l5 / season avg) — who leaving opens minutes.
    team_top2: dict[tuple[str, str], list[str]] = {}
    ranked_avg: dict[tuple[str, str], dict[str, float]] = {}
    for row in board:
        sp = _sport_of(row)
        tm = canon_team_abbr(sp, _team_of(row))
        pl = fold_player_name(_player_of(row))
        if not sp or not tm or not pl:
            continue
        avg = None
        for k in ("avg_l5", "stat_last5_avg", "l5_avg", "season_avg", "avg"):
            try:
                avg = float(row.get(k))  # type: ignore[arg-type]
                break
            except (TypeError, ValueError):
                continue
        if avg is None:
            continue
        slot = ranked_avg.setdefault((sp, tm), {})
        prev = slot.get(pl)
        if prev is None or avg > prev:
            slot[pl] = avg
    for key, pmap in ranked_avg.items():
        ordered = sorted(pmap.items(), key=lambda kv: kv[1], reverse=True)
        team_top2[key] = [name for name, _a in ordered[:2]]

    enriched: list[dict[str, Any]] = []
    minutes_opp_n = 0
    for row in board:
        rec = dict(row)
        sport = _sport_of(rec)
        player = fold_player_name(_player_of(rec))
        team = canon_team_abbr(sport, _team_of(rec))
        opp = canon_team_abbr(sport, _opp_of(rec))
        hit = index.get((sport, player, team))
        if hit is None and player:
            # name-only fallback within sport
            cands = [v for (sp, pl, _t), v in index.items() if sp == sport and pl == player]
            hit = cands[0] if len(cands) == 1 else None
        if hit is None:
            if player and sport:
                unmatched.append(
                    {
                        "sport": sport,
                        "player": _player_of(rec),
                        "team": _team_of(rec),
                        "reason": "no_injury_row",
                    }
                )
            rec["inj_status"] = rec.get("inj_status") or normalize_inj_status(
                rec.get("injury_status") or rec.get("Injury Status") or ""
            )
            rec["inj_captured_at"] = rec.get("inj_captured_at") or ""
        else:
            matched += 1
            status = normalize_inj_status(hit.get("status"))
            rec["inj_status"] = status
            rec["inj_captured_at"] = str(hit.get("captured_at") or "")
            # Keep legacy columns in sync for ticket gates / ranker
            rec["injury_status"] = status or rec.get("injury_status") or ""
            if is_injury_out(status):
                out_n += 1
            elif is_injury_soft_flag(status):
                soft_n += 1
        if injury_snapshot_stale(rec.get("inj_captured_at"), now=ref, max_hours=stale_hours):
            if rec.get("inj_status") or index:
                stale_n += 1
                rec["inj_stale"] = 1
            else:
                rec["inj_stale"] = 0
        else:
            rec["inj_stale"] = 0
        team_outs = [n for n in out_lists.get((sport, team), []) if fold_player_name(n) != player]
        opp_outs = out_lists.get((sport, opp), []) if opp else []
        # G-League two-ways don't free starter minutes; ignore for opportunity flag.
        meaningful_outs = [
            n for n in team_outs if not _is_gleague_out(n, index, sport, team)
        ]
        rec["team_out_list"] = ",".join(team_outs)
        rec["opp_out_list"] = ",".join(opp_outs)

        # Flag-only: available board legs when a real (non-G-League) teammate is OUT
        # → possible minutes / usage bump. Prefer top-2-by-avg when that OUT is still
        # on the board; otherwise any Injury/Illness OUT counts. Never a ticket gate.
        top2 = team_top2.get((sport, team), [])
        top2_out = [
            n
            for n in meaningful_outs
            if fold_player_name(n) in {fold_player_name(t) for t in top2}
        ]
        self_out = is_injury_out(rec.get("inj_status") or rec.get("injury_status"))
        if meaningful_outs and not self_out:
            rec["minutes_opp"] = 1
            rec["minutes_opp_from"] = ",".join(top2_out or meaningful_outs[:4])
            rec["backup_spot"] = 1
            minutes_opp_n += 1
        elif team_outs and not self_out:
            rec["minutes_opp"] = 0
            rec["minutes_opp_from"] = ""
            rec["backup_spot"] = 1  # only G-League teammates out
        else:
            rec["minutes_opp"] = 0
            rec["minutes_opp_from"] = ""
            rec["backup_spot"] = 0
        enriched.append(rec)

    stats = {
        "n_board": len(board),
        "n_matched": matched,
        "n_unmatched_log": len(unmatched),
        "n_out": out_n,
        "n_soft_flag": soft_n,
        "n_stale": stale_n,
        "n_minutes_opp": minutes_opp_n,
        "injuries_csv": str(latest_csv_path(root=root)),
        "unmatched_sample": unmatched[:40],
    }
    return enriched, stats


def write_unmatched_log(stats: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = stats.get("unmatched_sample") or []
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["sport", "player", "team", "reason"])
        w.writeheader()
        for r in rows:
            w.writerow(r)
