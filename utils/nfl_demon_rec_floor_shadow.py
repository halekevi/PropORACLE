"""NFL Demon receptions L5-floor observe. Registered 2026-10-25. Logged only.

Sibling of the frozen lead-back / target-leader track. Do not edit
``utils/nfl_demon_volume_shadow.py``. This one takes a top-2 target
whose Demon receptions line sits under his L5 minimum.

See ``ml/prereg/nfl_demon_rec_floor_2026-10-25.md``.
"""
from __future__ import annotations

import json
from datetime import date as _date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from utils.comparison_tracks import wilson_band
from utils.nfl_demon_volume_shadow import (
    _f,
    _fold,
    _pick_raw,
    _side,
    injury_blocks,
    rec_tgt_share,
    tgt_share,
    volume_prop,
)
from utils.nfl_std_under_shadow import game_date_et, step1_has_pick_type
from utils.scraped_quote import is_real_scraped_quote, real_scraped_payout_x

TRACK = "nfl_demon_rec_floor_shadow"
PREREG_DATE = "2026-10-25"
PROPS_IN = frozenset({"receptions"})
RANK_MAX = 2
MIN_TGT_SHARE_PP = 15.0
L5_GAMES_MIN = 5
BAR_DECIDED = 100
AMENDMENTS_CLOSED = True

_ROOT = Path(__file__).resolve().parents[1]
_LOG = _ROOT / "data" / "reports" / "track_comparison.jsonl"
_TEMPLATES = _ROOT / "ui_runner" / "templates"
_PREREG_JSON = _ROOT / "ml" / "prereg" / "nfl_demon_rec_floor_2026-10-25.json"


def frozen_spec() -> dict[str, Any]:
    return {
        "track": TRACK,
        "prereg_date": PREREG_DATE,
        "props_in": sorted(PROPS_IN),
        "rank_max": RANK_MAX,
        "min_tgt_share_pp": MIN_TGT_SHARE_PP,
        "l5_games_min": L5_GAMES_MIN,
        "bar_decided": BAR_DECIDED,
        "amendments_closed": AMENDMENTS_CLOSED,
    }


def assert_frozen_spec(*, path: Path | None = None) -> None:
    dest = path or _PREREG_JSON
    spec = json.loads(dest.read_text(encoding="utf-8"))
    assert spec["track"] == TRACK
    assert spec["prereg_date"] == PREREG_DATE
    assert spec.get("amendments_closed") is True
    assert sorted(spec["props_in"]) == sorted(PROPS_IN)
    assert int(spec["role"]["rank_max"]) == RANK_MAX
    assert float(spec["role"]["min_tgt_share_pp"]) == MIN_TGT_SHARE_PP
    assert int(spec["gate"]["l5_games_min"]) == L5_GAMES_MIN
    assert int(spec["bar"]["decided_min"]) == BAR_DECIDED


def _share_pp(val: float | None) -> float | None:
    if val is None:
        return None
    return val * 100.0 if val <= 1.5 else val


def player_tgt_share_pp(row: dict[str, Any]) -> float | None:
    return _share_pp(tgt_share(row) if tgt_share(row) is not None else rec_tgt_share(row))


def l5_games(row: dict[str, Any]) -> list[float]:
    """Newest-first L5 actuals. Prefer current-season slots."""
    out: list[float] = []
    for prefix in ("current_stat_g", "Current G", "stat_g", "Stat G"):
        found = [_f(row.get(f"{prefix}{i}")) for i in range(1, 6)]
        vals = [v for v in found if v is not None]
        if len(vals) >= L5_GAMES_MIN:
            return vals[:L5_GAMES_MIN]
        if vals and not out:
            out = vals
    return out


def l5_floor(row: dict[str, Any]) -> float | None:
    games = l5_games(row)
    if len(games) < L5_GAMES_MIN:
        return None
    return min(games)


def team_tgt_ranks(board: list[dict[str, Any]]) -> dict[str, dict[str, int]]:
    """player-fold → rank (1 = highest share) per team."""
    by_team: dict[str, dict[str, float]] = {}
    for row in board:
        if str(row.get("sport") or "").strip().upper() != "NFL":
            continue
        team = _fold(row.get("team"))
        player = _fold(row.get("player"))
        share = player_tgt_share_pp(row)
        if not team or not player or share is None:
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


def role_ok(row: dict[str, Any], ranks: dict[str, dict[str, int]]) -> tuple[bool, str]:
    share = player_tgt_share_pp(row)
    if share is None:
        return False, "share_unavailable"
    if share + 1e-9 < MIN_TGT_SHARE_PP:
        return False, "share_below_floor"
    team = _fold(row.get("team"))
    player = _fold(row.get("player"))
    rank = (ranks.get(team) or {}).get(player)
    if rank is None:
        return False, "rank_unavailable"
    if rank > RANK_MAX:
        return False, "not_top2_target"
    return True, f"tgt_rank_{rank}"


def floor_ok(row: dict[str, Any]) -> tuple[bool, str]:
    line = _f(row.get("line"))
    floor = l5_floor(row)
    if line is None:
        return False, "no_line"
    if floor is None:
        return False, "l5_floor_unknown"
    if floor > line:
        return True, "below_l5_floor"
    return False, "line_not_below_l5_floor"


def leg_qualifies(
    row: dict[str, Any],
    *,
    face_verified: bool,
    ranks: dict[str, dict[str, int]] | None = None,
) -> tuple[bool, str]:
    if str(row.get("sport") or "").strip().upper() != "NFL":
        return False, "not_nfl"
    if str(row.get("league") or "").strip().upper() in {
        "NFLP",
        "NFLSZN",
        "NFL1H",
        "NFL2H",
        "NFL1Q",
        "NFL4Q",
    }:
        return False, "not_full_game_regular"
    try:
        from utils.postseason_slate import season_class

        sc = season_class(row)
    except Exception:
        sc = None
    if sc not in (None, "regular"):
        return False, f"season_{sc}"
    if _pick_raw(row).lower() != "demon":
        return False, "not_literal_demon"
    if not face_verified:
        return False, "face_unverified_step1_no_pick_type"
    if _side(row) == "UNDER":
        return False, "demon_under_invalid"
    if _side(row) != "OVER":
        return False, "not_over"
    if volume_prop(row.get("prop") or row.get("prop_type")) not in PROPS_IN:
        return False, "prop_not_listed"
    from utils.stat_attach_guard import attach_status_blocks_l5

    if attach_status_blocks_l5(row.get("stat_status")):
        return False, "stat_status_not_ok"
    ok_role, role_why = role_ok(row, ranks or {})
    if not ok_role:
        return False, role_why
    if injury_blocks(row):
        return False, "injury_taking_targets"
    ok_floor, floor_why = floor_ok(row)
    if not ok_floor:
        return False, floor_why
    gd = game_date_et(row)
    if not gd:
        return False, "no_game_date"
    if gd <= PREREG_DATE:
        return False, "game_before_prereg"
    return True, role_why


def _leg_record(row: dict[str, Any], *, slate_date: str, index: int, role_source: str) -> dict[str, Any]:
    return {
        "date": slate_date,
        "game_date": game_date_et(row),
        "id": f"NFLDF-leg-{slate_date}-{index}",
        "track": TRACK,
        "kind": "leg",
        "arm": "receptions",
        "placed": False,
        "ticket_track": "",
        "postseason": 0,
        "composition": "1D",
        "n_legs": 1,
        "n_standard": 0,
        "n_goblin": 0,
        "n_demon": 1,
        "player": row.get("player"),
        "team": row.get("team"),
        "opponent": row.get("opp_team") or row.get("opp"),
        "sport": "NFL",
        "prop": row.get("prop"),
        "prop_canon": "receptions",
        "pick_type": "Demon",
        "pick_type_raw": _pick_raw(row),
        "face_verified": True,
        "direction": "OVER",
        "line": _f(row.get("line")),
        "target_share_L3": tgt_share(row),
        "rec_tgt_share": rec_tgt_share(row),
        "tgt_share_pp": player_tgt_share_pp(row),
        "l5_floor": l5_floor(row),
        "l5_games": l5_games(row),
        "role_source": role_source,
        "start_time": row.get("start_time"),
        "p": None,
        "p_source": "unpriced_observe",
        "quote": None,
        "real_quote": False,
        "ev_label": "observe",
        "stake": 20,
        "result": None,
        "exclude_from_book": True,
        "exclude_from_forward": False,
        "prereg": PREREG_DATE,
    }


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


def record_nfl_demon_rec_floor_shadow(
    board: list[dict[str, Any]],
    *,
    date: str,
    root: Path | None = None,
    path: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    assert_frozen_spec()
    root = root or _ROOT
    log_path = path or _LOG
    face_ok = step1_has_pick_type(root, date)
    ranks = team_tgt_ranks(board)
    reasons: dict[str, int] = {}
    legs: list[dict[str, Any]] = []
    now_dt = now or datetime.now(timezone(timedelta(hours=-4)))
    seen: set[tuple[str, float | None]] = set()
    for row in board:
        ok, why = leg_qualifies(row, face_verified=face_ok, ranks=ranks)
        if not ok:
            if why not in {"not_nfl", "not_literal_demon", "not_over", "prop_not_listed"}:
                reasons[why] = reasons.get(why, 0) + 1
            continue
        st = str(row.get("start_time") or "")
        try:
            tip = datetime.fromisoformat(st.replace("Z", "+00:00"))
            if tip.tzinfo is not None and tip <= now_dt:
                reasons["already_started"] = reasons.get("already_started", 0) + 1
                continue
        except ValueError:
            pass
        key = (_fold(row.get("player")), _f(row.get("line")))
        if key in seen:
            continue
        seen.add(key)
        legs.append(_leg_record(row, slate_date=date, index=len(legs) + 1, role_source=why))
    kept = [r for r in _read_log(log_path) if not (r.get("track") == TRACK and r.get("date") == date)]
    _write_log(kept + legs, log_path)
    return {
        "track": TRACK,
        "placed": False,
        "face_verified_board": face_ok,
        "n_legs": len(legs),
        "skipped": reasons,
        "legs": legs,
    }


def _load_graded_faces(templates: Path, game_date: str) -> dict[tuple[str, str, float], dict[str, Any]]:
    from utils.nfl_demon_volume_shadow import _load_graded_faces as _load

    return _load(templates, game_date)


def grade_pending(*, path: Path | None = None, templates: Path | None = None) -> dict[str, int]:
    log_path = path or _LOG
    tpl = templates or _TEMPLATES
    rows = _read_log(log_path)
    cache: dict[str, dict] = {}
    counts = {"graded": 0, "no_face_on_board": 0, "pending": 0}
    for r in rows:
        if r.get("track") != TRACK or r.get("kind") != "leg":
            continue
        if r.get("result") in {"HIT", "MISS", "VOID", "NOT_ON_BOARD"}:
            continue
        gd = str(r.get("game_date") or "")
        if not gd:
            continue
        if gd not in cache:
            cache[gd] = _load_graded_faces(tpl, gd)
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
        if g is None:
            r["result"] = "NOT_ON_BOARD"
            r["grade_note"] = "no graded OVER face at this line"
            counts["no_face_on_board"] += 1
        else:
            label = str(g.get("pick_type") or "").strip().lower()
            res = str(g.get("result") or "").upper()
            if label != "demon":
                r["result"] = "NOT_ON_BOARD"
                r["grade_note"] = f"graded face is {label or 'blank'}, not Demon"
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


def summarize(*, path: Path | None = None, today: str | None = None) -> dict[str, Any]:
    rows = _read_log(path or _LOG)
    today = today or _date.today().isoformat()
    legs = [r for r in rows if r.get("track") == TRACK and r.get("kind") == "leg"]
    forward = [l for l in legs if str(l.get("game_date") or "") > PREREG_DATE]
    real_quotes = [
        real_scraped_payout_x(l)
        for l in forward
        if is_real_scraped_quote(l.get("payout_source"), l.get("captured_at"))
    ]
    real_quotes = [q for q in real_quotes if q]
    median_quote = sorted(real_quotes)[len(real_quotes) // 2] if real_quotes else None
    be = (median_quote ** (-1.0 / 3.0)) if median_quote else None
    decided = [l for l in forward if l.get("result") in {"HIT", "MISS"}]
    hits = sum(1 for l in decided if l["result"] == "HIT")
    band = wilson_band(hits, len(decided))
    return {
        "track": TRACK,
        "prereg": PREREG_DATE,
        "bar": {
            "decided_min": BAR_DECIDED,
            "breakeven": None if be is None else round(be, 4),
            "quote_basis": "real_scraped_demon_n_correct" if median_quote else "unverified_needs_demon_n_correct",
            "real_quotes_n": len(real_quotes),
        },
        "arms": {
            "receptions": {
                "legs": len(forward),
                "decided": len(decided),
                "hits": hits,
                "hit_rate": (hits / len(decided)) if decided else None,
                "wilson_low": band[0] if band else None,
                "wilson_high": band[1] if band else None,
                "not_on_board": sum(1 for l in forward if l.get("result") == "NOT_ON_BOARD"),
                "bar_met": bool(
                    len(decided) >= BAR_DECIDED and band and be is not None and band[0] >= be
                ),
                "bar_note": "" if median_quote is not None else "needs a real scraped Demon N-correct",
            }
        },
    }
