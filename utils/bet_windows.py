"""Slate-day bet windows: line pulls + payout scrapes with timestamps.

Used to see when lines and N-correct floors moved (9PM day-ahead / 1AM / 5AM /
8AM / 9AM / 10:30 / 1PM / 4:30; 9:45 kept as a historical label only). Never uses 1st-place multipliers.
"""
from __future__ import annotations

import json
import math
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
ET = ZoneInfo("America/New_York")
LINE_DB = ROOT / "data" / "line_history.db"
REPORTS = ROOT / "data" / "reports"
CACHE_DIR = ROOT / "data" / "cache"
LINE_WINDOW_CACHE = CACHE_DIR / "last_line_window.json"
STAMP_TEMPLATES = ROOT / "ui_runner" / "templates" / "last_fetch_window.json"
STAMP_RUNTIME = ROOT / "ui_runner" / "runtime" / "last_fetch_window.json"

WINDOWS: tuple[tuple[int, int, str], ...] = (
    (1, 0, "1AM"),
    (5, 0, "5AM"),
    (8, 0, "8AM"),
    (9, 0, "9AM"),
    (9, 45, "9:45"),
    (10, 30, "10:30"),
    (13, 0, "1PM"),
    (16, 30, "4:30"),
    (21, 0, "9PM"),
)

# Scheduled-job labels (1AM / 8AM included) so a long run still buckets correctly.
WINDOW_ALIASES: dict[str, str] = {
    "1AM": "1AM",
    "DAILY1AM": "1AM",
    "5AM": "5AM",
    "8AM": "8AM",
    "9PM": "9PM",
    "21:00": "9PM",
    "DAYAHEAD": "9PM",
    "DAY_AHEAD": "9PM",
    "DAILY8AM": "8AM",
    "9AM": "9AM",
    "945AM": "9:45",
    "9:45": "9:45",
    "1030AM": "10:30",
    "10:30": "10:30",
    "1PM": "1PM",
    "13:00": "1PM",
    "430PM": "4:30",
    "4:30": "4:30",
}


def captured_hhmm(raw: object) -> str:
    """Eastern clock time HH:MM from a scrape stamp. Empty when it will not parse."""
    dt = parse_et(raw)
    if dt is None:
        return ""
    return f"{dt.hour:02d}:{dt.minute:02d}"


def _name_key(raw: object) -> str:
    return "".join(ch for ch in str(raw or "").casefold() if ch.isalnum())


def slip_share_flags(legs: list[dict[str, Any]]) -> dict[str, Any]:
    """Whether legs share a game or a team.

    A game key is game_id when the leg has one, otherwise sport plus the two
    team names. A flag stays null when fewer than two legs carry that field,
    so a missing team is not logged as a different game.
    """
    game_keys: list[tuple] = []
    teams: list[str] = []
    for leg in legs or []:
        if not isinstance(leg, dict):
            continue
        gid = str(leg.get("game_id") or leg.get("pp_game_id") or "").strip()
        team = _name_key(leg.get("team"))
        opp = _name_key(leg.get("opp") or leg.get("opponent") or leg.get("opp_team"))
        sport = _name_key(leg.get("sport"))
        if gid:
            game_keys.append(("id", gid))
        elif team and opp:
            game_keys.append(("pair", sport, tuple(sorted((team, opp)))))
        if team:
            teams.append(team)
    shared_game = None
    if len(game_keys) >= 2:
        shared_game = len(game_keys) != len(set(game_keys))
    shared_team = None
    if len(teams) >= 2:
        shared_team = len(teams) != len(set(teams))
    return {"shared_game": shared_game, "shared_team": shared_team}


def now_et_iso() -> str:
    return datetime.now(ET).isoformat(timespec="seconds")


def parse_et(raw: object) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ET)
    return dt.astimezone(ET)


def normalize_window_label(raw: object) -> str:
    key = str(raw or "").strip().upper().replace(" ", "").replace("-", "")
    return WINDOW_ALIASES.get(key, "")


def window_label(raw: object) -> str:
    dt = parse_et(raw)
    if dt is None:
        return "other"
    mins = dt.hour * 60 + dt.minute
    best = min(WINDOWS, key=lambda w: abs((w[0] * 60 + w[1]) - mins))
    if abs((best[0] * 60 + best[1]) - mins) > 50:
        return dt.strftime("%H:%M")
    return best[2]


def job_window_label(captured_at: object = None, *, explicit: object = None) -> str:
    """Prefer the scheduled job (1AM / 8AM / …) over clock rounding."""
    import os

    for cand in (explicit, os.environ.get("PROPORACLE_BET_WINDOW")):
        named = normalize_window_label(cand)
        if named:
            return named
    return window_label(captured_at)


def scrape_log_path(date: str) -> Path:
    return REPORTS / f"payout_scrape_log_{date}.jsonl"


def append_payout_scrape_log(
    date: str,
    captured: list[dict[str, Any]],
    *,
    captured_at: str | None = None,
) -> int:
    """Append one JSONL row per ok/partial slip. Returns rows written."""
    date_s = str(date or "")[:10]
    if not date_s:
        return 0
    stamp = captured_at or now_et_iso()
    REPORTS.mkdir(parents=True, exist_ok=True)
    n = 0
    with scrape_log_path(date_s).open("a", encoding="utf-8") as fh:
        for rec in captured or []:
            if not isinstance(rec, dict):
                continue
            if str(rec.get("status") or "").lower() not in {"ok", "partial"}:
                continue
            try:
                px = float(rec.get("power_min_x") or rec.get("power_payout_x") or 0)
            except (TypeError, ValueError):
                continue
            if not (px > 0):
                continue
            at = str(rec.get("captured_at") or stamp)
            win = job_window_label(at, explicit=rec.get("window"))
            legs = rec.get("legs") if isinstance(rec.get("legs"), list) else []
            lines = []
            for leg in legs:
                if not isinstance(leg, dict):
                    continue
                src = str(leg.get("standard_line_source") or "").strip().lower()
                # Only a posted Standard sibling is a payout gap. A blank source
                # can be an old offset estimate that was copied onto the ticket
                # without its label. An explicit gap from step1 is trusted.
                trusted_src = src in {"sibling", "sibling_or_pp", "pp_or_step1"}
                gap = leg.get("gap")
                if gap in (None, "", 0, 0.0):
                    gap = None
                if gap is None and trusted_src:
                    gap = (
                        leg.get("line_distance")
                        if leg.get("line_distance") not in (None, "", 0, 0.0)
                        else leg.get("delta")
                    )
                    if gap in (None, "", 0, 0.0):
                        try:
                            gap = abs(float(leg.get("line")) - float(leg.get("standard_line")))
                            if gap <= 0:
                                gap = None
                        except (TypeError, ValueError):
                            gap = None
                logged_std = leg.get("standard_line") if (gap is not None or trusted_src) else None
                lines.append(
                    {
                        "player": str(leg.get("player") or "").strip(),
                        "sport": str(leg.get("sport") or "").strip().upper(),
                        "prop": str(leg.get("prop_type") or leg.get("prop") or "").strip(),
                        "line": leg.get("line"),
                        "side": str(leg.get("direction") or leg.get("side") or "").strip().upper(),
                        "pick_type": str(leg.get("pick_type") or "").strip(),
                        "standard_line": logged_std,
                        "gap": gap,
                        "team": str(leg.get("team") or "").strip(),
                        "opp": str(leg.get("opp") or leg.get("opponent") or leg.get("opp_team") or "").strip(),
                        "game_id": str(leg.get("game_id") or leg.get("pp_game_id") or "").strip(),
                        "start_time": str(
                            leg.get("start_time")
                            or leg.get("event_start_time")
                            or leg.get("game_start_time")
                            or ""
                        ).strip(),
                    }
                )
            share = slip_share_flags(lines)
            row = {
                "captured_at": at,
                "captured_time_et": captured_hhmm(at),
                "window": win,
                "ticket_id": str(rec.get("ticket_id") or ""),
                "n_legs": rec.get("n_legs") or len(lines),
                "power_min_x": px,
                "flex_n_correct": rec.get("flex_n_correct") or {},
                "shared_game": share["shared_game"],
                "shared_team": share["shared_team"],
                "lines": lines,
                "source": "live_cdp",
            }
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
            n += 1
    return n


def _line_pulls(date: str) -> list[dict[str, Any]]:
    if not LINE_DB.is_file():
        return []
    pulls: list[dict[str, Any]] = []
    try:
        with sqlite3.connect(LINE_DB) as conn:
            conn.row_factory = sqlite3.Row
            has_game = any(
                r[1] == "game_date"
                for r in conn.execute("PRAGMA table_info(line_history)").fetchall()
            )
            if has_game:
                q = (
                    "SELECT fetched_at, COUNT(*) AS n FROM line_history "
                    "WHERE substr(CAST(fetched_at AS TEXT),1,10)=? OR CAST(game_date AS TEXT)=? "
                    "GROUP BY fetched_at ORDER BY fetched_at"
                )
                fetch_rows = conn.execute(q, (date, date)).fetchall()
            else:
                fetch_rows = conn.execute(
                    "SELECT fetched_at, COUNT(*) AS n FROM line_history "
                    "WHERE substr(CAST(fetched_at AS TEXT),1,10)=? "
                    "GROUP BY fetched_at ORDER BY fetched_at",
                    (date,),
                ).fetchall()
            moved: dict[str, int] = {}
            try:
                for r in conn.execute(
                    "SELECT fetched_at, COUNT(*) AS n FROM line_events "
                    "WHERE event='moved' AND (game_date=? OR substr(CAST(fetched_at AS TEXT),1,10)=?) "
                    "GROUP BY fetched_at",
                    (date, date),
                ):
                    moved[str(r["fetched_at"])] = int(r["n"] or 0)
            except sqlite3.OperationalError:
                moved = {}
            for r in fetch_rows:
                ts = str(r["fetched_at"] or "")
                pulls.append(
                    {
                        "fetched_at": ts,
                        "window": window_label(ts),
                        "n_props": int(r["n"] or 0),
                        "n_moved": int(moved.get(ts) or 0),
                    }
                )
    except sqlite3.Error:
        return []
    return pulls


def _payout_windows(date: str) -> list[dict[str, Any]]:
    path = scrape_log_path(date)
    if not path.is_file():
        return []
    by_win: dict[str, dict[str, Any]] = {}
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(rec, dict):
                continue
            win = str(rec.get("window") or window_label(rec.get("captured_at")))
            bucket = by_win.setdefault(
                win,
                {
                    "window": win,
                    "n_scrapes": 0,
                    "power_xs": [],
                    "first_captured_at": rec.get("captured_at"),
                    "last_captured_at": rec.get("captured_at"),
                },
            )
            bucket["n_scrapes"] += 1
            try:
                bucket["power_xs"].append(float(rec.get("power_min_x")))
            except (TypeError, ValueError):
                pass
            at = str(rec.get("captured_at") or "")
            if at and (
                not bucket.get("first_captured_at")
                or at < str(bucket.get("first_captured_at") or "")
            ):
                bucket["first_captured_at"] = at
            if at and at > str(bucket.get("last_captured_at") or ""):
                bucket["last_captured_at"] = at
    out = []
    for win, b in by_win.items():
        xs = [x for x in b["power_xs"] if math.isfinite(x) and x > 0]
        xs.sort()
        med = xs[len(xs) // 2] if xs else None
        out.append(
            {
                "window": win,
                "n_scrapes": b["n_scrapes"],
                "median_power_x": med,
                "min_power_x": xs[0] if xs else None,
                "max_power_x": xs[-1] if xs else None,
                "first_captured_at": b["first_captured_at"],
                "last_captured_at": b["last_captured_at"],
            }
        )
    order = {w[2]: i for i, w in enumerate(WINDOWS)}
    out.sort(key=lambda r: (order.get(str(r["window"]), 99), str(r.get("first_captured_at") or "")))
    return out


def rebuild_bet_windows(date: str | None = None) -> dict[str, Any]:
    date_s = str(date or datetime.now(ET).strftime("%Y-%m-%d"))[:10]
    payload = {
        "date": date_s,
        "generated_at": now_et_iso(),
        "line_pulls": _line_pulls(date_s),
        "payout_scrapes": _payout_windows(date_s),
        "note": "N-correct / To Win only. Line pulls from line_history.db; payouts from CDP jsonl.",
    }
    REPORTS.mkdir(parents=True, exist_ok=True)
    dated = REPORTS / f"bet_windows_{date_s}.json"
    latest = REPORTS / "bet_windows_latest.json"
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    dated.write_text(text, encoding="utf-8")
    latest.write_text(text, encoding="utf-8")
    return payload


#: A sport that gains this many props it did not have in an earlier fetch of the same
#: slate day (or any prop type it did not have) triggers a per-sport rebuild.
NEW_ROWS_MIN_DEFAULT = 25
#: Sports allowed to run the per-sport rebuild. Tennis first; extend only after the
#: ``logs/late_fetch_sport_rebuilds.jsonl`` log has been watched for a couple of days.
SPORT_REBUILD_DEFAULT = "TENNIS"
SPORT_REBUILD_LOG = ROOT / "logs" / "late_fetch_sport_rebuilds.jsonl"


def sport_rebuild_enabled_sports() -> list[str]:
    """Sports enabled for the per-sport late-fetch rebuild.

    ``PROPORACLE_LATE_SPORT_REBUILD`` is a comma list (``TENNIS,SOCCER``). Set it to an
    empty string or ``off`` to switch the feature off. Unset means ``TENNIS``.
    """
    import os

    raw = os.environ.get("PROPORACLE_LATE_SPORT_REBUILD")
    if raw is None:
        raw = SPORT_REBUILD_DEFAULT
    raw = raw.strip()
    if not raw or raw.lower() in {"0", "off", "none", "false"}:
        return []
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def new_rows_min() -> int:
    import os

    try:
        return max(1, int(str(os.environ.get("PROPORACLE_LATE_SPORT_NEW_ROWS_MIN", "")).strip()))
    except ValueError:
        return NEW_ROWS_MIN_DEFAULT


def _new_rows_by_sport(date: str, start_ts: str, end_ts: str) -> dict[str, dict[str, Any]]:
    """Props each sport gained in ``[start_ts, end_ts]`` that no earlier fetch of ``date`` had.

    ``new_rows`` counts distinct props (projection id, else player+prop+line+pick) absent from
    every earlier fetch of the slate day. ``new_prop_types`` are prop types the sport had
    not posted in any earlier fetch. Empty before the day's first fetch.
    """
    if not start_ts or not LINE_DB.is_file():
        return {}
    key = (
        "COALESCE(NULLIF(CAST(projection_id AS TEXT),''), "
        "player||'|'||prop_type||'|'||CAST(line AS TEXT)||'|'||COALESCE(pick_type,''))"
    )
    out: dict[str, dict[str, Any]] = {}
    try:
        with sqlite3.connect(LINE_DB) as conn:
            prior_keys: dict[str, set[str]] = {}
            prior_types: dict[str, set[str]] = {}
            seen_now: dict[str, set[str]] = {}
            n_earlier = 0
            rows = conn.execute(
                f"SELECT UPPER(COALESCE(sport,'')), {key}, COALESCE(prop_type,''), fetched_at "
                "FROM line_history WHERE CAST(game_date AS TEXT)=? AND fetched_at<=?",
                (date, end_ts),
            )
            for sport, k, pt, fa in rows:
                if not sport:
                    continue
                if fa < start_ts:
                    n_earlier += 1
                    prior_keys.setdefault(sport, set()).add(k)
                    prior_types.setdefault(sport, set()).add(pt)
                    continue
                # in-window rows are compared against everything strictly earlier; the scan
                # order is not time-sorted, so collect and resolve after the loop.
                seen_now.setdefault(sport, set()).add((k, pt))
            if not n_earlier:
                return {}
            for sport, pairs in seen_now.items():
                for k, pt in pairs:
                    if k in prior_keys.get(sport, ()):
                        continue
                    ent = out.setdefault(sport, {"new_rows": 0, "new_prop_types": set()})
                    ent["new_rows"] += 1
                    if pt and pt not in prior_types.get(sport, ()):
                        ent["new_prop_types"].add(pt)
    except sqlite3.Error:
        return {}
    return {
        s: {"new_rows": int(v["new_rows"]), "new_prop_types": sorted(v["new_prop_types"])}
        for s, v in out.items()
    }


def sports_needing_rebuild(new_by_sport: dict[str, dict[str, Any]]) -> list[str]:
    """Sports that gained >= NEW_ROWS_MIN new props or any new prop type."""
    need = new_rows_min()
    return sorted(
        s
        for s, v in (new_by_sport or {}).items()
        if int(v.get("new_rows") or 0) >= need or (v.get("new_prop_types") and int(v.get("new_rows") or 0) > 0)
    )


def append_sport_rebuild_log(
    *,
    date: str,
    window: str,
    mode: str,
    triggered: list[str],
    rebuilt: list[str],
    new_by_sport: dict[str, Any] | None = None,
    outcome: str = "",
    path: Path | None = None,
) -> Path:
    """One JSON line per late-fetch window: which sports triggered / rebuilt."""
    dest = path or SPORT_REBUILD_LOG
    dest.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "at": now_et_iso(),
        "date": date,
        "window": window,
        "mode": mode,
        "triggered_sports": triggered,
        "rebuilt_sports": rebuilt,
        "enabled_sports": sport_rebuild_enabled_sports(),
        "new_rows_min": new_rows_min(),
        "new_by_sport": new_by_sport or {},
        "outcome": outcome,
    }
    with dest.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    return dest


def summarize_fetch_window(
    date: str | None = None,
    window: str | None = None,
) -> dict[str, Any]:
    """Did this scheduled window stamp lines, and should payout Force run?

    Line timestamps are recorded on every fetch (including n_moved=0).
    Force payout / rebuild tickets when this is the day's first stamp, or this
    window moved lines vs the previous pull. Empty payout jsonl alone does not
    Force — Auto still fills missing live_cdp.
    """
    date_s = str(date or datetime.now(ET).strftime("%Y-%m-%d"))[:10]
    win = normalize_window_label(window) or job_window_label()
    pulls = _line_pulls(date_s)
    this = [p for p in pulls if str(p.get("window") or "") == win]
    n_moved_this = sum(int(p.get("n_moved") or 0) for p in this)
    initial = pulls[0] if pulls else None
    latest = pulls[-1] if pulls else None
    is_initial = False
    if this and pulls:
        is_initial = str(pulls[0].get("fetched_at") or "") == str(this[0].get("fetched_at") or "")
    elif pulls and not this:
        is_initial = False
    else:
        is_initial = True
    n_moved_from_initial = 0
    if pulls and not is_initial:
        n_moved_from_initial = sum(int(p.get("n_moved") or 0) for p in pulls[1:])
    no_pulls = not pulls
    if not this and pulls:
        this = [p for p in pulls if str(p.get("fetched_at") or "") == str(pulls[-1].get("fetched_at") or "")]
        n_moved_this = sum(int(p.get("n_moved") or 0) for p in this)
        is_initial = len({str(p.get("fetched_at") or "") for p in pulls}) == 1
    # Force full CDP only when this is the day's first stamp or lines moved.
    # Do NOT Force just because payout_scrape_log_*.jsonl is empty — mid-day Auto
    # still fills missing live_cdp / changed slips. Empty jsonl was forcing ~1h
    # Force rescrapes on quiet 8AM/9AM boards.
    force_payout = bool(no_pulls or is_initial or n_moved_this > 0)
    rebuild_tickets = bool(no_pulls or is_initial or n_moved_this > 0)

    # Newly posted props never count as "moved". Per sport, props (or prop types) that no
    # earlier fetch of the slate day had trigger a narrower per-sport rebuild when the
    # full rebuild is not already running.
    new_by_sport: dict[str, dict[str, Any]] = {}
    if this and not (no_pulls or is_initial):
        stamps = sorted(str(p.get("fetched_at") or "") for p in this if p.get("fetched_at"))
        # A window label also matches the same slot on the day before (slate-day rows are
        # fetched at D-1 1AM too), so only the latest run (fetches within 3h of the last
        # stamp) counts as "this window".
        if stamps:
            last_dt = parse_et(stamps[-1])
            if last_dt is not None:
                stamps = [
                    s
                    for s in stamps
                    if (pdt := parse_et(s)) is not None and (last_dt - pdt).total_seconds() <= 3 * 3600
                ] or stamps[-1:]
        if stamps:
            new_by_sport = _new_rows_by_sport(date_s, stamps[0], stamps[-1])
    triggered = sports_needing_rebuild(new_by_sport)
    enabled = sport_rebuild_enabled_sports()
    rebuild_sports = [] if rebuild_tickets else [s for s in triggered if s in enabled]
    return {
        "date": date_s,
        "window": win or "other",
        "generated_at": now_et_iso(),
        "n_pulls_this_window": len(this),
        "n_moved_this_window": n_moved_this,
        "n_moved_from_initial": n_moved_from_initial,
        "is_initial_stamp": is_initial or no_pulls,
        "force_payout": force_payout,
        "rebuild_tickets": rebuild_tickets,
        "new_by_sport": new_by_sport,
        "sport_rebuild_triggered": triggered,
        "sport_rebuild_enabled": enabled,
        "rebuild_sports": rebuild_sports,
        "latest_fetched_at": str((latest or {}).get("fetched_at") or ""),
        "initial_fetched_at": str((initial or {}).get("fetched_at") or ""),
    }


def write_fetch_window_stamp(summary: dict[str, Any] | None = None) -> dict[str, Any]:
    """Persist last_line_window.json + last_fetch_window.json for live publish."""
    payload = dict(summary or summarize_fetch_window())
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    LINE_WINDOW_CACHE.write_text(text, encoding="utf-8")
    for dest in (STAMP_TEMPLATES, STAMP_RUNTIME):
        dest.parent.mkdir(parents=True, exist_ok=True)
        dest.write_text(text, encoding="utf-8")
    return payload
