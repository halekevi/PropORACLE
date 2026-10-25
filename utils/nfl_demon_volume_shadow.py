"""NFL Demon volume observe. Pre-registered 2026-10-25. Logged only.

Rush-attempt Demon OVER for the lead back, and reception Demon OVER for
the target leader, when that team is favored or the game is close, and
the posted line still sits under recent volume. Not a lock.

Pre-registration (frozen — see ml/prereg/nfl_demon_volume_2026-10-25.md):

  props   rush_attempts, receptions. Everything else is out.
  face    literal ``Demon`` pick_type on a board whose step1 stored
          pick_type. Default labels do not qualify. Demon UNDER is invalid.
  role    lead back / target leader by rank. Committee (runner-up within
          15 share points) or an injury on the candidate skips.
  script  team spread <= +3.5 (favored or one-score). Bigger dogs fail.
  line    L5 average minus line >= 1. Compare the live Demon to recent
          carries / catches, not last Sunday's spike.
  timing  game_date after 2026-10-25; log before kickoff.
  bar     >= 100 decided legs, Wilson lower bound vs a real scraped
          Demon N-correct (never 1st-place). No guessed multiple meets it.

Legs go to data/reports/track_comparison.jsonl with ``placed`` false and
``exclude_from_book`` true. Skattebo 2026-10-25 is the motivating example
and is excluded by the date cut. A hit there is a small-stake candidate
until the forward log says otherwise.
"""
from __future__ import annotations

import json
import math
import re
from datetime import date as _date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from utils.comparison_tracks import wilson_band
from utils.nfl_std_under_shadow import game_date_et, step1_has_pick_type
from utils.prop_norm import canon_prop
from utils.scraped_quote import is_real_scraped_quote, real_scraped_payout_x

TRACK = "nfl_demon_volume_shadow"
PREREG_DATE = "2026-10-25"
PROPS_IN = frozenset({"rush_attempts", "receptions"})
ARMS = ("rush_attempts", "receptions", "combined")
BAR_DECIDED = 100
# Frozen 2026-10-25. Do not edit after the first forward leg.
# Wrong rule → new prereg / new track, not a patch here.
CLOSE_MAX_DOG = 3.5  # favored or one-score; first draft was spread < 0
COMMITTEE_GAP_PP = 15.0
MIN_LINE_CUSHION = 1.0
AMENDMENTS_CLOSED = True
_EPS = 1e-9
_INJURY_BLOCKS = frozenset(
    {
        "out",
        "doubtful",
        "questionable",
        "ir",
        "injuredreserve",
        "suspended",
        "pup",
    }
)

_RUSH_ATT = frozenset(
    {
        "rush_attempts",
        "rushattempts",
        "rushingattempts",
        "rushing_attempts",
        "carries",
        "rush_att",
        "rushatts",
    }
)
_RECEPTIONS = frozenset({"receptions", "recs", "reception", "rec"})

_ROOT = Path(__file__).resolve().parents[1]
_LOG = _ROOT / "data" / "reports" / "track_comparison.jsonl"
_TEMPLATES = _ROOT / "ui_runner" / "templates"
_PREREG_JSON = _ROOT / "ml" / "prereg" / "nfl_demon_volume_2026-10-25.json"


def frozen_spec() -> dict[str, Any]:
    """Locked thresholds. Edit = new prereg, not a patch."""
    return {
        "track": TRACK,
        "prereg_date": PREREG_DATE,
        "props_in": sorted(PROPS_IN),
        "close_max_dog": CLOSE_MAX_DOG,
        "committee_gap_pp": COMMITTEE_GAP_PP,
        "line_cushion_min": MIN_LINE_CUSHION,
        "bar_decided": BAR_DECIDED,
        "amendments_closed": AMENDMENTS_CLOSED,
    }


def assert_frozen_spec(*, path: Path | None = None) -> None:
    """Constants must still match the frozen prereg JSON."""
    dest = path or _PREREG_JSON
    spec = json.loads(dest.read_text(encoding="utf-8"))
    assert spec["track"] == TRACK
    assert spec["prereg_date"] == PREREG_DATE
    assert spec.get("amendments_closed") is True
    assert sorted(spec["props_in"]) == sorted(PROPS_IN)
    assert float(spec["script"]["close_max_dog"]) == CLOSE_MAX_DOG
    assert float(spec["role"]["committee_gap_pp"]) == COMMITTEE_GAP_PP
    assert float(spec["gate"]["line_cushion_min"]) == MIN_LINE_CUSHION
    assert int(spec["bar"]["decided_min"]) == BAR_DECIDED


def spread_coverage(board: list[dict[str, Any]]) -> dict[str, int]:
    """Demon rush-attempt / reception rows that can even see condition 2."""
    n = missing = 0
    for row in board:
        if str(row.get("sport") or "").strip().upper() != "NFL":
            continue
        if _pick_raw(row).lower() != "demon":
            continue
        if volume_prop(row.get("prop") or row.get("prop_type")) not in PROPS_IN:
            continue
        n += 1
        if team_spread(row) is None:
            missing += 1
    return {"candidates": n, "spread_missing": missing}


def _f(raw: object) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _fold(s: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def _first_f(row: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        if key in row and row.get(key) not in (None, ""):
            val = _f(row.get(key))
            if val is not None:
                return val
    return None


def _side(row: dict[str, Any]) -> str:
    raw = str(row.get("side") or row.get("direction") or "").strip().upper()
    if raw in {"U", "UNDER", "LESS"}:
        return "UNDER"
    if raw in {"O", "OVER", "MORE"}:
        return "OVER"
    return raw


def _pick_raw(row: dict[str, Any]) -> str:
    return str(row.get("pick_type") or "").strip()


def volume_prop(raw: object) -> str:
    """Fold board labels onto rush_attempts / receptions when they match."""
    folded = _fold(raw)
    if folded in _RUSH_ATT:
        return "rush_attempts"
    if folded in _RECEPTIONS:
        return "receptions"
    c = canon_prop("NFL", raw)
    if c in _RUSH_ATT or _fold(c) in _RUSH_ATT:
        return "rush_attempts"
    if c in _RECEPTIONS or _fold(c) in _RECEPTIONS:
        return "receptions"
    return c


def team_spread(row: dict[str, Any]) -> float | None:
    return _first_f(row, "spread", "Spread", "team_spread")


def snap_share(row: dict[str, Any]) -> float | None:
    return _first_f(row, "snap_pct_L3", "Snap L3", "snap_pct_season")


def rush_att_share(row: dict[str, Any]) -> float | None:
    return _first_f(row, "rush_att_share", "rush_att_share_pct", "Rush Att Share")


def tgt_share(row: dict[str, Any]) -> float | None:
    return _first_f(row, "target_share_L3", "Tgt Share L3", "target_share_season")


def rec_tgt_share(row: dict[str, Any]) -> float | None:
    return _first_f(row, "rec_tgt_share", "rec_tgt_share_pct", "Rec Tgt Share")


def team_role_index(board: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    """Per-team maxima and runner-up for role rank.

    Lead-back snap is taken only from rush-attempt rows so a wideout's
    snap share cannot beat the workhorse. Target share is team-wide.
    One player on many props counts once.
    """
    by_team: dict[str, dict[str, dict[str, float]]] = {}
    for row in board:
        if str(row.get("sport") or "").strip().upper() != "NFL":
            continue
        team = _fold(row.get("team"))
        player = _fold(row.get("player"))
        if not team or not player:
            continue
        pmap = by_team.setdefault(team, {}).setdefault(player, {})
        prop = volume_prop(row.get("prop") or row.get("prop_type"))
        updates: list[tuple[str, float | None]] = [
            ("rush_att_share", rush_att_share(row)),
            ("tgt", tgt_share(row)),
            ("rec_tgt", rec_tgt_share(row)),
        ]
        if prop == "rush_attempts":
            updates.append(("rb_snap", snap_share(row)))
        for key, val in updates:
            if val is None:
                continue
            prev = pmap.get(key)
            if prev is None or val > prev:
                pmap[key] = val
    out: dict[str, dict[str, float]] = {}
    pairs = (
        ("rush_att_share", "max_rush_att_share", "second_rush_att_share"),
        ("rb_snap", "max_rb_snap", "second_rb_snap"),
        ("tgt", "max_tgt", "second_tgt"),
        ("rec_tgt", "max_rec_tgt", "second_rec_tgt"),
    )
    for team, pmap in by_team.items():
        slot: dict[str, float] = {}
        for key, max_k, second_k in pairs:
            vals = sorted((m[key] for m in pmap.values() if key in m), reverse=True)
            if vals:
                slot[max_k] = vals[0]
            if len(vals) > 1:
                slot[second_k] = vals[1]
        out[team] = slot
    return out


def _is_team_max(val: float | None, mx: float | None) -> bool:
    return val is not None and mx is not None and val + _EPS >= mx


def _gap_pp(top: float | None, second: float | None) -> float | None:
    """Gap in percentage points. Shares stored as 0–1 or 0–100 both work."""
    if top is None or second is None:
        return None
    raw = abs(top - second)
    return raw if max(top, second) > 1.5 else raw * 100.0


def committee(prop: str, roles: dict[str, dict[str, float]], team: str) -> bool:
    slot = roles.get(team) or {}
    if prop == "rush_attempts":
        if slot.get("max_rush_att_share") is not None:
            gap = _gap_pp(slot.get("max_rush_att_share"), slot.get("second_rush_att_share"))
        else:
            gap = _gap_pp(slot.get("max_rb_snap"), slot.get("second_rb_snap"))
    elif prop == "receptions":
        if slot.get("max_tgt") is not None:
            gap = _gap_pp(slot.get("max_tgt"), slot.get("second_tgt"))
        else:
            gap = _gap_pp(slot.get("max_rec_tgt"), slot.get("second_rec_tgt"))
    else:
        return False
    return gap is not None and gap <= COMMITTEE_GAP_PP + _EPS


def injury_status(row: dict[str, Any]) -> str:
    raw = str(
        row.get("injury_status")
        or row.get("Injury")
        or row.get("injury")
        or row.get("Injury Status")
        or ""
    ).strip()
    return raw


def injury_blocks(row: dict[str, Any]) -> bool:
    folded = _fold(injury_status(row))
    if not folded or folded in {"healthy", "active", "probable"}:
        return False
    return any(tag in folded for tag in _INJURY_BLOCKS)


def recent_avg(row: dict[str, Any]) -> float | None:
    return _first_f(row, "avg_l5", "avg_L5", "Avg L5")


def line_cushion(row: dict[str, Any]) -> float | None:
    line = _f(row.get("line"))
    avg = recent_avg(row)
    if line is None or avg is None:
        return None
    return avg - line


def script_ok(spread: float) -> tuple[bool, str]:
    if spread <= CLOSE_MAX_DOG:
        return True, "favored" if spread < 0 else "close"
    return False, "not_favored_or_close"


def role_ok(row: dict[str, Any], prop: str, roles: dict[str, dict[str, float]]) -> tuple[bool, str]:
    team = _fold(row.get("team"))
    slot = roles.get(team) or {}
    if prop == "rush_attempts":
        ras = rush_att_share(row)
        if ras is not None or slot.get("max_rush_att_share") is not None:
            if ras is None:
                return False, "role_unavailable"
            if _is_team_max(ras, slot.get("max_rush_att_share")):
                return True, "rush_att_share"
            return False, "not_lead_back"
        snap = snap_share(row)
        if snap is None:
            return False, "role_unavailable"
        if _is_team_max(snap, slot.get("max_rb_snap")):
            return True, "snap_l3"
        return False, "not_lead_back"
    if prop == "receptions":
        tgt = tgt_share(row)
        if tgt is not None:
            if _is_team_max(tgt, slot.get("max_tgt")):
                return True, "tgt_share_l3"
            return False, "not_target_leader"
        recs = rec_tgt_share(row)
        if recs is None:
            return False, "role_unavailable"
        if _is_team_max(recs, slot.get("max_rec_tgt")):
            return True, "rec_tgt_share"
        return False, "not_target_leader"
    return False, "prop_not_listed"


def leg_qualifies(
    row: dict[str, Any],
    *,
    face_verified: bool,
    roles: dict[str, dict[str, float]] | None = None,
) -> tuple[bool, str]:
    if str(row.get("sport") or "").strip().upper() != "NFL":
        return False, "not_nfl"
    if str(row.get("league") or "").strip().upper() in {"NFLP", "NFLSZN", "NFL1H", "NFL2H", "NFL1Q", "NFL4Q"}:
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
    prop = volume_prop(row.get("prop") or row.get("prop_type"))
    if prop not in PROPS_IN:
        return False, "prop_not_listed"
    from utils.stat_attach_guard import attach_status_blocks_l5

    if attach_status_blocks_l5(row.get("stat_status")):
        return False, "stat_status_not_ok"
    spread = team_spread(row)
    if spread is None:
        return False, "spread_unknown"
    ok_script, script_why = script_ok(spread)
    if not ok_script:
        return False, script_why
    ok_role, role_why = role_ok(row, prop, roles or {})
    if not ok_role:
        return False, role_why
    if committee(prop, roles or {}, _fold(row.get("team"))):
        return False, "committee"
    if injury_blocks(row):
        return False, "injury_taking_carries"
    cov = line_cushion(row)
    if cov is None:
        return False, "line_vs_recent_unknown"
    if cov < MIN_LINE_CUSHION:
        return False, "line_not_low_vs_recent"
    gd = game_date_et(row)
    if not gd:
        return False, "no_game_date"
    if gd <= PREREG_DATE:
        return False, "game_before_prereg"
    return True, role_why


def _leg_record(row: dict[str, Any], *, slate_date: str, index: int, role_source: str) -> dict[str, Any]:
    prop = volume_prop(row.get("prop") or row.get("prop_type"))
    return {
        "date": slate_date,
        "game_date": game_date_et(row),
        "id": f"NFLD-leg-{slate_date}-{index}",
        "track": TRACK,
        "kind": "leg",
        "arm": prop,
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
        "prop_canon": prop,
        "pick_type": "Demon",
        "pick_type_raw": _pick_raw(row),
        "face_verified": True,
        "direction": "OVER",
        "line": _f(row.get("line")),
        "spread": team_spread(row),
        "snap_pct_L3": snap_share(row),
        "target_share_L3": tgt_share(row),
        "rush_att_share": rush_att_share(row),
        "rec_tgt_share": rec_tgt_share(row),
        "avg_l5": recent_avg(row),
        "line_cushion": line_cushion(row),
        "script_band": script_ok(team_spread(row) or 0)[1] if team_spread(row) is not None else "",
        "injury_status": injury_status(row) or "",
        "role_source": role_source,
        "start_time": row.get("start_time"),
        "p": None,
        "p_source": "unpriced_observe",
        "quote": None,
        "payout_source": None,
        "captured_at": None,
        "real_quote": False,
        "scraped_payout": None,
        "ev": None,
        "dollar_test": False,
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
    path.write_text("".join(json.dumps(r, ensure_ascii=False) + "\n" for r in rows), encoding="utf-8")


def record_nfl_demon_volume_shadow(
    board: list[dict[str, Any]],
    *,
    date: str,
    root: Path | None = None,
    path: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Log today's qualifying Demon volume legs. Replaces this slate's prior rows."""
    assert_frozen_spec()
    root = root or _ROOT
    log_path = path or _LOG
    face_ok = step1_has_pick_type(root, date)
    roles = team_role_index(board)
    reasons: dict[str, int] = {}
    legs: list[dict[str, Any]] = []
    now_dt = now or datetime.now(timezone(timedelta(hours=-4)))
    seen: set[tuple[str, str, float | None]] = set()
    for row in board:
        ok, why = leg_qualifies(row, face_verified=face_ok, roles=roles)
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
        key = (_fold(row.get("player")), volume_prop(row.get("prop")), _f(row.get("line")))
        if key in seen:
            continue
        seen.add(key)
        legs.append(_leg_record(row, slate_date=date, index=len(legs) + 1, role_source=why))
    kept = [r for r in _read_log(log_path) if not (r.get("track") == TRACK and r.get("date") == date)]
    _write_log(kept + legs, log_path)
    cov = spread_coverage(board)
    return {
        "track": TRACK,
        "placed": False,
        "face_verified_board": face_ok,
        "n_legs": len(legs),
        "n_rush_attempts": sum(1 for l in legs if l["prop_canon"] == "rush_attempts"),
        "n_receptions": sum(1 for l in legs if l["prop_canon"] == "receptions"),
        "spread_candidates": cov["candidates"],
        "spread_missing": cov["spread_missing"],
        "needs_odds_spread": bool(cov["candidates"] and cov["spread_missing"] == cov["candidates"]),
        "skipped": reasons,
        "legs": legs,
    }


def _load_graded_faces(templates: Path, game_date: str) -> dict[tuple[str, str, float], dict[str, Any]]:
    """Graded NFL OVER rows keyed by player/prop/line. Only literal Demon faces."""
    out: dict[tuple[str, str, float], dict[str, Any]] = {}
    for cand in (templates / f"graded_props_{game_date}.json", _ROOT / "data" / "graded" / f"graded_props_{game_date}.json"):
        if not cand.is_file():
            continue
        try:
            payload = json.loads(cand.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        for r in payload.get("props") or []:
            if not isinstance(r, dict) or str(r.get("sport") or "").upper() != "NFL":
                continue
            side = str(r.get("direction") or r.get("over_under") or "").upper()
            if "OVER" not in side and "MORE" not in side:
                continue
            line = _f(r.get("line"))
            if line is None:
                continue
            prop = volume_prop(r.get("prop") or r.get("prop_type"))
            key = (_fold(r.get("player")), prop, round(line, 2))
            out[key] = r
        if out:
            break
    return out


def grade_pending(*, path: Path | None = None, templates: Path | None = None) -> dict[str, int]:
    """Fill result on logged legs whose game has a graded_props file. Idempotent."""
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
        key = (_fold(r.get("player")), str(r.get("prop_canon") or ""), round(line, 2) if line is not None else None)
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
    """Forward counters per arm against the pre-registered bar."""
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
    # Per-leg breakeven for a 3-leg all-hit multiple when a scrape exists.
    be = (median_quote ** (-1.0 / 3.0)) if median_quote else None
    out: dict[str, Any] = {
        "track": TRACK,
        "prereg": PREREG_DATE,
        "bar": {
            "decided_min": BAR_DECIDED,
            "breakeven": None if be is None else round(be, 4),
            "quote_basis": "real_scraped_demon_n_correct" if median_quote else "unverified_needs_demon_n_correct",
            "real_quotes_n": len(real_quotes),
        },
        "arms": {},
    }
    for arm in ARMS:
        pool = forward if arm == "combined" else [l for l in forward if l.get("prop_canon") == arm]
        decided = [l for l in pool if l.get("result") in {"HIT", "MISS"}]
        hits = sum(1 for l in decided if l["result"] == "HIT")
        band = wilson_band(hits, len(decided))
        not_on_board = sum(1 for l in pool if l.get("result") == "NOT_ON_BOARD")
        out["arms"][arm] = {
            "legs": len(pool),
            "decided": len(decided),
            "hits": hits,
            "hit_rate": (hits / len(decided)) if decided else None,
            "wilson_low": band[0] if band else None,
            "wilson_high": band[1] if band else None,
            "not_on_board": not_on_board,
            "bar_met": bool(
                len(decided) >= BAR_DECIDED and band and be is not None and band[0] >= be
            ),
            "bar_note": "" if median_quote is not None else "needs a real scraped Demon N-correct",
        }
    return out
