"""NFL Standard UNDER forward shadow. Pre-registered 2026-10-24. Logged only.

Why a shadow and not a gate: the 2026-09-09 → 10-22 backtest cell
(`L10>=8 + cover>=1` 82.9% n=375) is face-contaminated. NFL step1 did not
persist ``pick_type`` before 2026-10-23, step7 defaulted blank to Standard,
and 77% of the graded sacks / tackles UNDER legs sat on a ladder (several
lines for the same player-prop-date). Most of those were Goblin / Demon
rungs where UNDER was never offered. The backtest number is not a price.

Pre-registration (frozen — see ml/prereg/nfl_standard_under_2026-10-24.md):

  props   tackles only (PrizePicks "Tackles + Ast" canons to tackles).
          Sacks dropped 2026-10-25 (attach/all-zero contamination).
          Everything else is out; pass completions explicitly out.
  face    literal ``Standard`` pick_type on a board whose step1 stored
          pick_type. Default labels do not qualify.
  gate    directional L10 >= 8 (l10_under) AND cover >= 1 where cover is
          line - L5 average (directional, not |cover|).
  shape   disjoint 2-leg Power, flat 3x, breakeven 57.7%/leg. Pairs priced
          as p^2 (tested independent after date matching).
  arms    rule4_applied: drop lines in {0.5, 2.5, 4.5}. NFL step8 has no
          usable role field (Usage Role / Minutes Tier are blank), so the
          role half of rule 4 cannot be evaluated; the arm is line-only
          and treats every player as a role player.
          rule4_waived: keep them.
  bar     >= 100 decided legs with game date after 2026-10-24, Wilson
          lower bound >= breakeven of a real scraped quote
          (utils.scraped_quote). With no real scrape the flat 3x bar is
          shown and flagged, not counted as met.

Legs and slips go to data/reports/track_comparison.jsonl with
``placed`` false and ``exclude_from_book`` true. Nothing here is on /tickets.
"""
from __future__ import annotations

import json
import math
import re
from datetime import date as _date, datetime, timedelta, timezone
from itertools import combinations
from pathlib import Path
from typing import Any

from utils.comparison_tracks import wilson_band
from utils.prop_norm import canon_prop
from utils.scraped_quote import is_real_scraped_quote, real_scraped_payout_x

TRACK = "nfl_std_under_shadow"
PREREG_DATE = "2026-10-24"
PROPS_IN = frozenset({"tackles"})
PROPS_OUT_EXPLICIT = frozenset({"passcompletions", "sacks"})
RULE4_LINES = frozenset({0.5, 2.5, 4.5})
ARMS = ("rule4_applied", "rule4_waived")
FLAT_POWER2_X = 3.0
BREAKEVEN_FLAT = FLAT_POWER2_X ** -0.5  # 0.5774
BAR_DECIDED = 100
MIN_L10 = 8
MIN_COVER = 1.0

_ROOT = Path(__file__).resolve().parents[1]
_LOG = _ROOT / "data" / "reports" / "track_comparison.jsonl"
_TEMPLATES = _ROOT / "ui_runner" / "templates"


def _f(raw: object) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


def _fold(s: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def _side(row: dict[str, Any]) -> str:
    raw = str(row.get("side") or row.get("direction") or "").strip().upper()
    if raw in {"U", "UNDER", "LESS"}:
        return "UNDER"
    if raw in {"O", "OVER", "MORE"}:
        return "OVER"
    return raw


def _pick_raw(row: dict[str, Any]) -> str:
    return str(row.get("pick_type") or "").strip()


def game_date_et(row: dict[str, Any]) -> str:
    """ET calendar date of the tip from start_time / game_time."""
    raw = str(row.get("start_time") or row.get("game_time") or row.get("game_date") or "").strip()
    if not raw or raw.lower() in {"none", "nan", "nat"}:
        return ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return raw[:10] if re.match(r"^\d{4}-\d{2}-\d{2}", raw) else ""
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone(timedelta(hours=-4)))
    return dt.date().isoformat()


def step1_has_pick_type(root: Path, slate_date: str) -> bool:
    """A board can only verify a Standard face if step1 stored pick_type.

    The column alone is not enough: a default-filled column is 100% Standard,
    which is the Week-1 hole again. Require at least one Goblin / Demon row.
    """
    path = root / "outputs" / slate_date / "nfl" / "step1_pp_props_today.csv"
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


def directional_cover(row: dict[str, Any]) -> float | None:
    """UNDER cover = line - L5 average. Positive means the average sits under the line."""
    line = _f(row.get("line"))
    avg = _f(row.get("avg_l5"))
    if avg is None:
        cov = _f(row.get("cover"))
        if cov is None:
            return None
        # Board 'cover' is avg - line; flip for UNDER.
        return -cov
    if line is None:
        return None
    return line - avg


def rule4_blocked(row: dict[str, Any]) -> bool:
    line = _f(row.get("line"))
    return line is not None and any(abs(line - x) < 1e-9 for x in RULE4_LINES)


def leg_qualifies(row: dict[str, Any], *, face_verified: bool) -> tuple[bool, str]:
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
    if _pick_raw(row).lower() != "standard":
        return False, "not_literal_standard"
    if not face_verified:
        return False, "face_unverified_step1_no_pick_type"
    if _side(row) != "UNDER":
        return False, "not_under"
    prop = canon_prop("NFL", row.get("prop") or row.get("prop_type"))
    if prop in PROPS_OUT_EXPLICIT:
        return False, "prop_excluded"
    if prop not in PROPS_IN:
        return False, "prop_not_listed"
    from utils.stat_attach_guard import (
        attach_status_blocks_l5,
        is_all_zero_history,
        row_qualifies_for_list,
    )

    if attach_status_blocks_l5(row.get("stat_status")):
        return False, "stat_status_not_ok"
    ok_list, why_list = row_qualifies_for_list(row)
    if not ok_list and why_list in {
        "all_zero_mapping_failure",
        "implausible_ypc_scale",
        "implausible_longest_scale",
        "implausible_comp_pct_scale",
        "l5_hits_no_games",
        "unsupported_period_prop",
    }:
        return False, why_list
    hist = []
    for i in range(1, 11):
        hist.append(row.get(f"stat_g{i}"))
    avg = _f(row.get("avg_l5"))
    if is_all_zero_history(hist) or avg == 0.0:
        return False, "all_zero_history"
    l10 = _f(row.get("l10_under"))
    if l10 is None or l10 < MIN_L10:
        return False, "l10_under_lt_8"
    cov = directional_cover(row)
    if cov is None or cov < MIN_COVER:
        return False, "cover_lt_1"
    gd = game_date_et(row)
    if not gd:
        return False, "no_game_date"
    if gd <= PREREG_DATE:
        return False, "game_before_prereg"
    return True, ""


def _leg_record(row: dict[str, Any], *, slate_date: str, index: int) -> dict[str, Any]:
    prop = canon_prop("NFL", row.get("prop") or row.get("prop_type"))
    return {
        "date": slate_date,
        "game_date": game_date_et(row),
        "id": f"NFLU-leg-{slate_date}-{index}",
        "track": TRACK,
        "kind": "leg",
        "placed": False,
        "ticket_track": "",
        "postseason": 0,
        "composition": "1S+0G",
        "n_legs": 1,
        "n_standard": 1,
        "n_goblin": 0,
        "player": row.get("player"),
        "team": row.get("team"),
        "opponent": row.get("opp_team") or row.get("opp"),
        "sport": "NFL",
        "prop": row.get("prop"),
        "prop_canon": prop,
        "pick_type": "Standard",
        "pick_type_raw": _pick_raw(row),
        "face_verified": True,
        "direction": "UNDER",
        "line": _f(row.get("line")),
        "l10_under": _f(row.get("l10_under")),
        "l5_under": _f(row.get("l5_under")),
        "avg_l5": _f(row.get("avg_l5")),
        "cover_dir": directional_cover(row),
        "rule4_blocked": rule4_blocked(row),
        "rule4_role_source": "unavailable_line_only",
        "start_time": row.get("start_time"),
        "p": None,
        "p_source": "unpriced_backtest_face_contaminated",
        "quote": None,
        "payout_source": None,
        "captured_at": None,
        "real_quote": False,
        "scraped_payout": None,
        "ev": None,
        "dollar_test": False,
        "ev_label": "shadow",
        "stake": 20,
        "result": None,
        "exclude_from_book": True,
        "exclude_from_forward": False,
        "prereg": PREREG_DATE,
    }


def _slips_for_arm(legs: list[dict[str, Any]], arm: str, *, slate_date: str) -> list[dict[str, Any]]:
    pool = [l for l in legs if not (arm == "rule4_applied" and l["rule4_blocked"])]
    # Highest L10 under then cover; disjoint players; one slip per player.
    pool.sort(key=lambda l: (-(l["l10_under"] or 0), -(l["cover_dir"] or 0)))
    used: set[str] = set()
    out = []
    idx = 1
    for a, b in combinations(pool, 2):
        ka, kb = _fold(a["player"]), _fold(b["player"])
        if not ka or not kb or ka == kb or ka in used or kb in used:
            continue
        used.update({ka, kb})
        out.append(
            {
                "date": slate_date,
                "game_date": max(a["game_date"], b["game_date"]),
                "id": f"NFLU2-{arm}-{slate_date}-{idx}",
                "track": TRACK,
                "arm": arm,
                "kind": "slip",
                "placed": False,
                "ticket_track": "",
                "postseason": 0,
                "composition": "2S+0G",
                "n_legs": 2,
                "n_standard": 2,
                "n_goblin": 0,
                "same_game_pair": bool(
                    a.get("team") and b.get("team") and (
                        {a.get("team"), a.get("opponent")} == {b.get("team"), b.get("opponent")}
                    )
                ),
                "p_win": None,
                "quote": FLAT_POWER2_X,
                "payout_source": "standard_flat",
                "captured_at": None,
                "real_quote": False,
                "scraped_payout": None,
                "ev": None,
                "dollar_test": False,
                "ev_label": "shadow",
                "stake": 20,
                "result": None,
                "sports": ["NFL"],
                "standard_sports": ["NFL"],
                "legs": [
                    {
                        "id": l["id"],
                        "player": l["player"],
                        "sport": "NFL",
                        "prop": l["prop"],
                        "prop_canon": l["prop_canon"],
                        "pick_type": "Standard",
                        "direction": "UNDER",
                        "line": l["line"],
                        "rule4_blocked": l["rule4_blocked"],
                    }
                    for l in (a, b)
                ],
                "exclude_from_book": True,
                "prereg": PREREG_DATE,
            }
        )
        idx += 1
    return out


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


def record_nfl_std_under_shadow(
    board: list[dict[str, Any]],
    *,
    date: str,
    root: Path | None = None,
    path: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Log today's qualifying legs and both slip arms. Replaces this slate's prior rows."""
    root = root or _ROOT
    log_path = path or _LOG
    face_ok = step1_has_pick_type(root, date)
    reasons: dict[str, int] = {}
    legs: list[dict[str, Any]] = []
    now_dt = now or datetime.now(timezone(timedelta(hours=-4)))
    seen: set[tuple[str, str, float | None]] = set()
    for row in board:
        ok, why = leg_qualifies(row, face_verified=face_ok)
        if not ok:
            if why not in {"not_nfl", "not_literal_standard", "not_under", "prop_not_listed"}:
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
        key = (_fold(row.get("player")), canon_prop("NFL", row.get("prop")), _f(row.get("line")))
        if key in seen:
            continue
        seen.add(key)
        legs.append(_leg_record(row, slate_date=date, index=len(legs) + 1))
    slips = []
    for arm in ARMS:
        slips.extend(_slips_for_arm(legs, arm, slate_date=date))
    kept = [r for r in _read_log(log_path) if not (r.get("track") == TRACK and r.get("date") == date)]
    _write_log(kept + legs + slips, log_path)
    return {
        "track": TRACK,
        "placed": False,
        "face_verified_board": face_ok,
        "n_legs": len(legs),
        "n_rule4_blocked": sum(1 for l in legs if l["rule4_blocked"]),
        "n_slips": {arm: sum(1 for s in slips if s["arm"] == arm) for arm in ARMS},
        "skipped": reasons,
        "legs": legs,
    }


# ---------------------------------------------------------------- grading


def _load_graded_faces(templates: Path, game_date: str) -> dict[tuple[str, str, float], dict[str, Any]]:
    """Graded NFL UNDER rows keyed by player/prop/line. Only literal Standard faces."""
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
            if "UNDER" not in side and "LESS" not in side:
                continue
            line = _f(r.get("line"))
            if line is None:
                continue
            key = (_fold(r.get("player")), canon_prop("NFL", r.get("prop") or r.get("prop_type")), round(line, 2))
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
    legs_by_id: dict[str, dict[str, Any]] = {}
    for r in rows:
        if r.get("track") != TRACK or r.get("kind") != "leg":
            continue
        if r.get("result") in {"HIT", "MISS", "VOID", "NOT_ON_BOARD"}:
            legs_by_id[r["id"]] = r
            continue
        gd = str(r.get("game_date") or "")
        if not gd:
            continue
        if gd not in cache:
            cache[gd] = _load_graded_faces(tpl, gd)
        faces = cache[gd]
        if not faces:
            counts["pending"] += 1
            legs_by_id[r["id"]] = r
            continue
        line = _f(r.get("line"))
        key = (_fold(r.get("player")), str(r.get("prop_canon") or ""), round(line, 2) if line is not None else None)
        g = faces.get(key)
        if g is None:
            r["result"] = "NOT_ON_BOARD"
            r["grade_note"] = "no graded UNDER face at this line"
            counts["no_face_on_board"] += 1
        else:
            label = str(g.get("pick_type") or "").strip().lower()
            res = str(g.get("result") or "").upper()
            if label != "standard":
                r["result"] = "NOT_ON_BOARD"
                r["grade_note"] = f"graded face is {label or 'blank'}, not Standard"
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
        legs_by_id[r["id"]] = r
    for r in rows:
        if r.get("track") != TRACK or r.get("kind") != "slip":
            continue
        results = [legs_by_id.get(l.get("id"), {}).get("result") for l in r.get("legs") or []]
        if any(x in {None, "VOID", "NOT_ON_BOARD"} for x in results):
            if any(x == "NOT_ON_BOARD" for x in results):
                r["result"] = "NOT_ON_BOARD"
            continue
        r["result"] = "WIN" if all(x == "HIT" for x in results) else "LOSS"
    _write_log(rows, log_path)
    return counts


def summarize(*, path: Path | None = None, today: str | None = None) -> dict[str, Any]:
    """Forward counters per arm against the pre-registered bar."""
    rows = _read_log(path or _LOG)
    today = today or _date.today().isoformat()
    legs = [r for r in rows if r.get("track") == TRACK and r.get("kind") == "leg"]
    slips = [r for r in rows if r.get("track") == TRACK and r.get("kind") == "slip"]
    forward = [l for l in legs if str(l.get("game_date") or "") > PREREG_DATE]
    real_quotes = [real_scraped_payout_x(s) for s in slips if is_real_scraped_quote(s.get("payout_source"), s.get("captured_at"))]
    real_quotes = [q for q in real_quotes if q]
    median_quote = sorted(real_quotes)[len(real_quotes) // 2] if real_quotes else None
    be = (median_quote ** -0.5) if median_quote else BREAKEVEN_FLAT
    out: dict[str, Any] = {
        "track": TRACK,
        "prereg": PREREG_DATE,
        "bar": {
            "decided_min": BAR_DECIDED,
            "breakeven": round(be, 4),
            "quote_basis": "real_scraped_median" if median_quote else "flat_3x_unverified",
            "real_quotes_n": len(real_quotes),
        },
        "arms": {},
    }
    for arm in ARMS:
        pool = [l for l in forward if not (arm == "rule4_applied" and l.get("rule4_blocked"))]
        decided = [l for l in pool if l.get("result") in {"HIT", "MISS"}]
        hits = sum(1 for l in decided if l["result"] == "HIT")
        band = wilson_band(hits, len(decided))
        not_on_board = sum(1 for l in pool if l.get("result") == "NOT_ON_BOARD")
        arm_slips = [s for s in slips if s.get("arm") == arm and str(s.get("game_date") or "") > PREREG_DATE]
        slip_dec = [s for s in arm_slips if s.get("result") in {"WIN", "LOSS"}]
        out["arms"][arm] = {
            "legs": len(pool),
            "decided": len(decided),
            "hits": hits,
            "hit_rate": (hits / len(decided)) if decided else None,
            "wilson_low": band[0] if band else None,
            "wilson_high": band[1] if band else None,
            "not_on_board": not_on_board,
            "slips": len(arm_slips),
            "slips_decided": len(slip_dec),
            "slip_wins": sum(1 for s in slip_dec if s["result"] == "WIN"),
            "bar_met": bool(
                len(decided) >= BAR_DECIDED and band and band[0] >= be and median_quote is not None
            ),
            "bar_note": (
                "needs a real scraped quote" if median_quote is None else ""
            ),
        }
    return out
