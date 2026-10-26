"""Shared observe engine for the 2026-10-25 volume-floor family.

NBA rebounds / assists / attempts and NHL SOG each have their own track
module and prereg. This file is the common floor + rank + log. It does
not import or edit the frozen NFL Demon loggers.
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import date as _date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from utils.comparison_tracks import wilson_band
from utils.nfl_std_under_shadow import game_date_et
from utils.prop_norm import canon_prop
from utils.scraped_quote import is_real_scraped_quote, real_scraped_payout_x

_ROOT = Path(__file__).resolve().parents[1]
_LOG = _ROOT / "data" / "reports" / "track_comparison.jsonl"
_TEMPLATES = _ROOT / "ui_runner" / "templates"
L5_GAMES_MIN = 5
RANK_MAX = 2
BAR_DECIDED = 100
FACES = frozenset({"standard", "demon"})


@dataclass(frozen=True)
class FloorSpec:
    track: str
    sport: str
    prereg_date: str
    props: frozenset[str]
    share_keys: tuple[str, ...]
    minutes_keys: tuple[str, ...]
    backup_minutes_below: float
    id_prefix: str
    prereg_json: Path


def _f(raw: object) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _fold(s: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def _pick(raw: object) -> str:
    k = str(raw or "").strip().lower()
    if "gob" in k:
        return "goblin"
    if "dem" in k:
        return "demon"
    if k in {"standard", "std"} or "standard" in k:
        return "standard"
    return ""


def _side(row: dict[str, Any]) -> str:
    raw = str(row.get("side") or row.get("direction") or "").strip().upper()
    if raw in {"U", "UNDER", "LESS"}:
        return "UNDER"
    if raw in {"O", "OVER", "MORE"}:
        return "OVER"
    return raw


def share_pp(row: dict[str, Any], keys: Iterable[str]) -> float | None:
    for key in keys:
        v = _f(row.get(key))
        if v is None:
            continue
        return v * 100.0 if v <= 1.5 else v
    return None


def minutes_or_snap(row: dict[str, Any], keys: Iterable[str]) -> float | None:
    return share_pp(row, keys)


def l5_games(row: dict[str, Any]) -> list[float]:
    out: list[float] = []
    for prefix in ("current_stat_g", "Current G", "stat_g", "Stat G"):
        found = [_f(row.get(f"{prefix}{i}")) for i in range(1, 6)]
        vals = [v for v in found if v is not None]
        if len(vals) >= L5_GAMES_MIN:
            return vals[:L5_GAMES_MIN]
        if vals and not out:
            out = vals
    return out


def l5_current_season_n(row: dict[str, Any]) -> int:
    """How many of the five L5 slots are current-season. Log-only.

    Counts numeric ``current_stat_g1..g5``. If those are empty, counts ``R``
    in ``window_mix`` (current RS). Playoff ``O`` and preseason ``P`` are
    not this season. Does not change who qualifies.
    """
    n = 0
    for i in range(1, 6):
        if _f(row.get(f"current_stat_g{i}")) is not None:
            n += 1
        elif _f(row.get(f"Current G{i}")) is not None:
            n += 1
    if n:
        return min(5, n)
    mix = str(row.get("window_mix") or row.get("windowMix") or "").upper()
    if mix:
        return min(5, mix.count("R"))
    sample = _f(row.get("l5_sample_n") or row.get("L5 Sample N"))
    if sample is not None:
        return int(min(5, max(0, sample)))
    return 0


def l5_floor(row: dict[str, Any]) -> float | None:
    games = l5_games(row)
    if len(games) < L5_GAMES_MIN:
        l5 = _f(row.get("l5_over") or row.get("L5 Over") or row.get("last5_over"))
        if l5 == 5:
            line = _f(row.get("line"))
            return None if line is None else line + 0.5
        return None
    return min(games)


def team_ranks(
    board: list[dict[str, Any]],
    spec: FloorSpec,
) -> dict[str, dict[str, int]]:
    by_team: dict[str, dict[str, float]] = {}
    for row in board:
        if str(row.get("sport") or "").strip().upper() != spec.sport:
            continue
        share = share_pp(row, spec.share_keys)
        if share is None:
            share = minutes_or_snap(row, spec.minutes_keys)
        team = _fold(row.get("team"))
        player = _fold(row.get("player"))
        if share is None or not team or not player:
            continue
        slot = by_team.setdefault(team, {})
        prev = slot.get(player)
        if prev is None or share > prev:
            slot[player] = share
    ranks: dict[str, dict[str, int]] = {}
    for team, pmap in by_team.items():
        ordered = sorted(pmap.items(), key=lambda kv: kv[1], reverse=True)
        ranks[team] = {name: i + 1 for i, (name, _s) in enumerate(ordered)}
    return ranks


def role_tag(rank: int | None, minutes: float | None, *, backup_below: float) -> str:
    """Log-only. Does not decide eligibility."""
    low = minutes is not None and minutes < backup_below
    if rank == 1 and not low:
        return "lead"
    if rank == 2 and not low:
        return "secondary"
    return "backup"


def face_verified_step1(root: Path, slate_date: str, folder: str) -> bool:
    path = root / "outputs" / slate_date / folder / "step1_pp_props_today.csv"
    if not path.is_file():
        return False
    try:
        import csv

        with path.open("r", encoding="utf-8-sig", errors="ignore", newline="") as fh:
            reader = csv.DictReader(fh)
            if not reader.fieldnames or "pick_type" not in reader.fieldnames:
                return False
            for row in reader:
                pt = str(row.get("pick_type") or "").strip().lower()
                if pt and pt != "standard":
                    return True
    except OSError:
        return False
    return False


def load_sport_board(root: Path, date: str, sport: str) -> list[dict[str, Any]]:
    """Load step8 even when G70 tickets for that sport are held."""
    import sys

    scripts = _ROOT / "scripts"
    if str(scripts) not in sys.path:
        sys.path.insert(0, str(scripts))
    import rank_best_props_today as R  # noqa: E402

    loader = {"NBA": R.load_nba, "NHL": R.load_nhl}.get(sport)
    if loader is None:
        return []
    df = loader(root, date)
    if df is None or df.empty:
        return []
    return R.recs(df)


def assert_spec(spec: FloorSpec) -> None:
    raw = json.loads(spec.prereg_json.read_text(encoding="utf-8"))
    assert raw["track"] == spec.track
    assert raw["prereg_date"] == spec.prereg_date
    assert raw.get("amendments_closed") is True
    assert sorted(raw["props_in"]) == sorted(spec.props)
    assert raw["sport"] == spec.sport


def _read_log(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if not path.is_file():
        return rows
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict):
            rows.append(rec)
    return rows


def _write_log(rows: list[dict[str, Any]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows),
        encoding="utf-8",
    )


def leg_qualifies(
    row: dict[str, Any],
    spec: FloorSpec,
    *,
    face_ok: bool,
    ranks: dict[str, dict[str, int]],
) -> tuple[bool, str]:
    if str(row.get("sport") or "").strip().upper() != spec.sport:
        return False, "wrong_sport"
    league = str(row.get("league") or row.get("sport") or "").strip().upper()
    if league.endswith(("1H", "1Q", "2H", "4Q")):
        return False, "not_full_game"
    pick = _pick(row.get("pick_type"))
    if pick not in FACES:
        return False, "not_std_or_demon"
    if not face_ok:
        return False, "face_unverified_step1_no_pick_type"
    if _side(row) == "UNDER":
        return False, "under_out"
    if _side(row) not in {"", "OVER"}:
        return False, "not_over"
    prop = canon_prop(spec.sport, row.get("prop") or row.get("prop_type"))
    if prop not in spec.props:
        return False, "prop_not_listed"
    line = _f(row.get("line"))
    floor = l5_floor(row)
    if line is None:
        return False, "no_line"
    if floor is None:
        return False, "l5_floor_unknown"
    if not (floor > line):
        return False, "line_not_below_l5_floor"
    team = _fold(row.get("team"))
    player = _fold(row.get("player"))
    rank = (ranks.get(team) or {}).get(player)
    if rank is None:
        return False, "rank_unavailable"
    if rank > RANK_MAX:
        return False, "not_top2"
    gd = game_date_et(row)
    if not gd:
        return False, "no_game_date"
    if gd <= spec.prereg_date:
        return False, "game_before_prereg"
    return True, f"rank_{rank}"


def _leg_record(
    row: dict[str, Any],
    spec: FloorSpec,
    *,
    slate_date: str,
    index: int,
    role_source: str,
    ranks: dict[str, dict[str, int]],
) -> dict[str, Any]:
    pick = _pick(row.get("pick_type"))
    face = "Demon" if pick == "demon" else "Standard"
    rank = (ranks.get(_fold(row.get("team"))) or {}).get(_fold(row.get("player")))
    minutes = minutes_or_snap(row, spec.minutes_keys)
    tier = str(row.get("minutes_tier") or "").strip().upper()
    if minutes is None and tier in {"LOW", "1", "2"}:
        minutes = spec.backup_minutes_below - 1
    if minutes is None and tier in {"HIGH", "STARTER"}:
        minutes = spec.backup_minutes_below + 10
    return {
        "date": slate_date,
        "game_date": game_date_et(row),
        "id": f"{spec.id_prefix}-leg-{slate_date}-{index}",
        "track": spec.track,
        "kind": "leg",
        "arm": face.lower(),
        "placed": False,
        "ticket_track": "",
        "postseason": 0,
        "composition": "1S" if face == "Standard" else "1D",
        "n_legs": 1,
        "n_standard": int(face == "Standard"),
        "n_goblin": 0,
        "n_demon": int(face == "Demon"),
        "player": row.get("player"),
        "team": row.get("team"),
        "opponent": row.get("opp_team") or row.get("opp"),
        "sport": spec.sport,
        "prop": row.get("prop"),
        "prop_canon": canon_prop(spec.sport, row.get("prop") or row.get("prop_type")),
        "pick_type": face,
        "pick_type_raw": str(row.get("pick_type") or "").strip(),
        "face_verified": True,
        "direction": "OVER",
        "line": _f(row.get("line")),
        "l5_floor": l5_floor(row),
        "l5_games": l5_games(row),
        "share_pp": share_pp(row, spec.share_keys),
        "minutes_or_snap": minutes,
        "rank": rank,
        "role_tag": role_tag(rank, minutes, backup_below=spec.backup_minutes_below),
        "role_source": role_source,
        "l5_current_season_n": (
            l5_current_season_n(row) if spec.sport == "NBA" else None
        ),
        "start_time": row.get("start_time") or row.get("game_time"),
        "p": None,
        "p_source": "unpriced_observe",
        "quote": None,
        "real_quote": False,
        "ev_label": "observe",
        "stake": 20,
        "result": None,
        "exclude_from_book": True,
        "exclude_from_forward": False,
        "prereg": spec.prereg_date,
    }


def record_floor_shadow(
    spec: FloorSpec,
    board: list[dict[str, Any]],
    *,
    date: str,
    root: Path | None = None,
    path: Path | None = None,
    now: datetime | None = None,
    folder: str,
) -> dict[str, Any]:
    assert_spec(spec)
    root = root or _ROOT
    log_path = path or _LOG
    rows = [r for r in board if str(r.get("sport") or "").strip().upper() == spec.sport]
    if not rows:
        rows = load_sport_board(root, date, spec.sport)
    face_ok = face_verified_step1(root, date, folder)
    ranks = team_ranks(rows, spec)
    reasons: dict[str, int] = {}
    legs: list[dict[str, Any]] = []
    now_dt = now or datetime.now(timezone(timedelta(hours=-4)))
    seen: set[tuple[str, str, float | None]] = set()
    for row in rows:
        ok, why = leg_qualifies(row, spec, face_ok=face_ok, ranks=ranks)
        if not ok:
            if why not in {"wrong_sport", "not_std_or_demon", "prop_not_listed", "not_over", "under_out"}:
                reasons[why] = reasons.get(why, 0) + 1
            continue
        st = str(row.get("start_time") or row.get("game_time") or "")
        try:
            tip = datetime.fromisoformat(st.replace("Z", "+00:00"))
            if tip.tzinfo is not None and tip <= now_dt:
                reasons["already_started"] = reasons.get("already_started", 0) + 1
                continue
        except ValueError:
            pass
        key = (_fold(row.get("player")), spec.track, _f(row.get("line")))
        if key in seen:
            continue
        seen.add(key)
        legs.append(
            _leg_record(
                row,
                spec,
                slate_date=date,
                index=len(legs) + 1,
                role_source=why,
                ranks=ranks,
            )
        )
    kept = [r for r in _read_log(log_path) if not (r.get("track") == spec.track and r.get("date") == date)]
    _write_log(kept + legs, log_path)
    return {
        "track": spec.track,
        "placed": False,
        "face_verified_board": face_ok,
        "n_legs": len(legs),
        "skipped": reasons,
        "legs": legs,
    }


def _load_graded_faces(templates: Path, game_date: str, sport: str) -> dict[tuple, dict[str, Any]]:
    out: dict[tuple, dict[str, Any]] = {}
    for cand in (
        templates / f"graded_props_{game_date}.json",
        _ROOT / "data" / "graded" / f"graded_props_{game_date}.json",
    ):
        if not cand.is_file():
            continue
        try:
            payload = json.loads(cand.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for r in payload.get("props") or []:
            if not isinstance(r, dict):
                continue
            if str(r.get("sport") or "").strip().upper() != sport:
                continue
            side = str(r.get("direction") or r.get("over_under") or "").upper()
            if "OVER" not in side and "MORE" not in side:
                continue
            line = _f(r.get("line"))
            if line is None:
                continue
            prop = canon_prop(sport, r.get("prop") or r.get("prop_type"))
            key = (_fold(r.get("player")), prop, round(line, 2))
            out[key] = r
        if out:
            break
    return out


def grade_pending(
    spec: FloorSpec,
    *,
    path: Path | None = None,
    templates: Path | None = None,
) -> dict[str, int]:
    log_path = path or _LOG
    tpl = templates or _TEMPLATES
    rows = _read_log(log_path)
    cache: dict[str, dict] = {}
    counts = {"graded": 0, "no_face_on_board": 0, "pending": 0}
    for r in rows:
        if r.get("track") != spec.track or r.get("kind") != "leg":
            continue
        if r.get("result") in {"HIT", "MISS", "VOID", "NOT_ON_BOARD"}:
            continue
        gd = str(r.get("game_date") or "")
        if not gd:
            continue
        if gd not in cache:
            cache[gd] = _load_graded_faces(tpl, gd, spec.sport)
        faces = cache[gd]
        if not faces:
            counts["pending"] += 1
            continue
        line = _f(r.get("line"))
        key = (
            _fold(r.get("player")),
            str(r.get("prop_canon") or ""),
            round(line, 2) if line is not None else None,
        )
        g = faces.get(key)
        want = str(r.get("pick_type") or "").strip().lower()
        if g is None:
            r["result"] = "NOT_ON_BOARD"
            r["grade_note"] = "no graded OVER face at this line"
            counts["no_face_on_board"] += 1
        else:
            label = _pick(g.get("pick_type"))
            res = str(g.get("result") or "").upper()
            if label != want:
                r["result"] = "NOT_ON_BOARD"
                r["grade_note"] = f"graded face is {label or 'blank'}, not {want}"
                counts["no_face_on_board"] += 1
            elif res in {"HIT", "MISS"}:
                r["result"] = res
                r["actual"] = g.get("actual_value", g.get("actual"))
                counts["graded"] += 1
            elif res in {"VOID", "PUSH", "DNP"}:
                r["result"] = "VOID"
                r["grade_note"] = res
            else:
                counts["pending"] += 1
        r["graded_at"] = datetime.now(timezone.utc).isoformat()
    _write_log(rows, log_path)
    return counts


def summarize(spec: FloorSpec, *, path: Path | None = None) -> dict[str, Any]:
    rows = _read_log(path or _LOG)
    legs = [r for r in rows if r.get("track") == spec.track and r.get("kind") == "leg"]
    forward = [l for l in legs if str(l.get("game_date") or "") > spec.prereg_date]
    arms: dict[str, dict[str, Any]] = {}
    for arm in ("standard", "demon", "combined"):
        pool = forward if arm == "combined" else [l for l in forward if l.get("arm") == arm]
        real_quotes = [
            real_scraped_payout_x(l)
            for l in pool
            if is_real_scraped_quote(l.get("payout_source"), l.get("captured_at"))
        ]
        real_quotes = [q for q in real_quotes if q]
        median_quote = sorted(real_quotes)[len(real_quotes) // 2] if real_quotes else None
        be = (median_quote ** (-1.0 / 3.0)) if median_quote else None
        decided = [l for l in pool if l.get("result") in {"HIT", "MISS"}]
        hits = sum(1 for l in decided if l["result"] == "HIT")
        band = wilson_band(hits, len(decided))
        tags = {}
        for l in pool:
            tag = str(l.get("role_tag") or "unknown")
            tags[tag] = tags.get(tag, 0) + 1
        arms[arm] = {
            "legs": len(pool),
            "decided": len(decided),
            "hits": hits,
            "hit_rate": (hits / len(decided)) if decided else None,
            "wilson_low": band[0] if band else None,
            "wilson_high": band[1] if band else None,
            "not_on_board": sum(1 for l in pool if l.get("result") == "NOT_ON_BOARD"),
            "role_tags": tags,
            "bar_met": bool(
                len(decided) >= BAR_DECIDED and band and be is not None and band[0] >= be
            ),
            "bar_note": "" if median_quote is not None else "needs a real scraped N-correct",
        }
    return {
        "track": spec.track,
        "prereg": spec.prereg_date,
        "bar": {
            "decided_min": BAR_DECIDED,
            "quote_basis": "real_scraped_n_correct",
        },
        "arms": arms,
    }
