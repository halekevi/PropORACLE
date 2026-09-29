"""Observe-only ledger for WNBA Goblin PRA Off-without-S/A.

Live tickets still require Off + prop-tier S/A. This logs the sleeve that
would ticket if S/A were dropped (L5=5+L10>=8+D+cover+Off). Result is filled
from graded_props_{date}.json; do not pack these legs onto Goblin-70.
"""
from __future__ import annotations

import argparse
import json
import math
import re
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[1]
LEDGER = _REPO / "data" / "reports" / "wnba_pra_off_observe.jsonl"
LATEST = _REPO / "data" / "reports" / "wnba_pra_off_observe_latest.json"
SUMMARY = _REPO / "data" / "reports" / "wnba_pra_off_observe_summary.json"
# Pre-declared live flip. proposed = Off-without-S/A and not already live.
# 39/50 at 78% has Wilson 95% LB ~64%, so headline 78% at n=50 still fails.
GO_LIVE_MIN_N = 50
GO_LIVE_WILSON_LB = 65.0
WILSON_Z = 1.96

from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import (  # noqa: E402
    directional_l10,
    directional_l5,
    goblin_70_eligible,
    wnba_pra_off_observe_eligible,
)


def _fold_player(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.casefold()
    t = re.sub(r"\b(jr|sr|iii|ii|iv)\b\.?", " ", t)
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return " ".join(t.split())


def _line_key(line: object) -> str:
    try:
        return f"{float(line):.1f}"
    except (TypeError, ValueError):
        return str(line or "")


def _prop_key(prop: object) -> str:
    return canon_prop("WNBA", str(prop or "")) or str(prop or "").strip().casefold()


def _cover(r: dict[str, Any]):
    for k in ("cover", "dist_l5"):
        v = r.get(k)
        if v is None or v == "":
            continue
        try:
            return float(v)
        except (TypeError, ValueError):
            continue
    return None


def _row_key(date: str, r: dict[str, Any]) -> tuple[str, str, str, str]:
    """Same join key as live PRA legs: date + folded player + canon prop + line."""
    return (
        str(date)[:10],
        _fold_player(r.get("player")),
        _prop_key(r.get("prop") or r.get("prop_type")),
        _line_key(r.get("line")),
    )


def _grade_key(date: str, r: dict[str, Any]) -> tuple[str, str, str, str]:
    return _row_key(date, r)


def snapshot_row(date: str, r: dict[str, Any]) -> dict[str, Any]:
    live = goblin_70_eligible(r)
    return {
        "slate_date": date,
        "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "player": r.get("player"),
        "prop": r.get("prop"),
        "side": r.get("side") or "OVER",
        "pick_type": r.get("pick_type") or "Goblin",
        "line": r.get("line"),
        "l5": directional_l5(r),
        "l10": directional_l10(r),
        "cover": _cover(r),
        "def": r.get("def") or r.get("def_tier") or r.get("opp_def_tier"),
        "usage_tier": r.get("usage_tier") or r.get("Usage Tier") or r.get("star_tier"),
        "minutes_tier": r.get("minutes_tier") or r.get("min_tier"),
        "prop_tier": r.get("prop_tier"),
        "matchup": r.get("matchup") or r.get("game"),
        "live_eligible": live,
        "observe_eligible": True,
        "observe_reason": "off_without_sa",
        "result": None,
        "actual_value": None,
        "graded_at": None,
    }


def _read_jsonl() -> list[dict[str, Any]]:
    if not LEDGER.is_file():
        return []
    rows: list[dict[str, Any]] = []
    for line in LEDGER.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict):
            rows.append(rec)
    return rows


def _write_jsonl(rows: list[dict[str, Any]]) -> None:
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    with LEDGER.open("w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def _prior_results(date: str) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for old in _read_jsonl():
        if str(old.get("slate_date") or "")[:10] != date:
            continue
        key = _row_key(date, old)
        if old.get("result"):
            out[key] = old
    return out


def _carry_grade(dst: dict[str, Any], src: dict[str, Any]) -> None:
    dst["result"] = src.get("result")
    dst["actual_value"] = src.get("actual_value")
    dst["graded_at"] = src.get("graded_at")


def _write_date_snapshot(date: str, rows: list[dict[str, Any]], extra: dict[str, Any] | None = None) -> dict[str, Any]:
    live_n = sum(1 for x in rows if x.get("live_eligible"))
    payload = {
        "slate_date": date,
        "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n": len(rows),
        "n_already_live": live_n,
        "n_proposed": len(rows) - live_n,
        "note": (
            "Observe-only. Goblin-70 still requires PRA Off+S/A. "
            "These rows would ticket if S/A were dropped."
        ),
        "rows": rows,
    }
    if extra:
        payload.update(extra)
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    dated = LEDGER.parent / f"wnba_pra_off_observe_{date}.json"
    dated.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def log_board(date: str, board: list[dict[str, Any]] | None) -> dict[str, Any]:
    """Replace this slate-date's observe rows. Keeps HIT/MISS already joined."""
    prior = _prior_results(date)
    seen: set[tuple[str, str, str, str]] = set()
    rows: list[dict[str, Any]] = []
    for r in board or []:
        if not wnba_pra_off_observe_eligible(r):
            continue
        key = _row_key(date, r)
        if key in seen:
            continue
        seen.add(key)
        rec = snapshot_row(date, r)
        if key in prior:
            _carry_grade(rec, prior[key])
        rows.append(rec)
    payload = _write_date_snapshot(date, rows)
    LATEST.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    others = [x for x in _read_jsonl() if str(x.get("slate_date") or "")[:10] != date]
    _write_jsonl(others + rows)
    return payload


def _sport_ok(raw: object) -> bool:
    return str(raw or "").strip().upper() == "WNBA"


def _goblin_over(r: dict[str, Any]) -> bool:
    pt = str(r.get("pick_type") or "").strip().lower()
    if "goblin" not in pt:
        return False
    side = str(r.get("direction") or r.get("over_under") or r.get("side") or "").strip().upper()
    return side in {"O", "OVER"}


def _result_of(r: dict[str, Any]) -> str | None:
    res = str(r.get("result") or "").strip().upper()
    if res in {"HIT", "MISS", "VOID"}:
        return res
    return None


def _actual_of(r: dict[str, Any]):
    v = r.get("actual_value")
    if v is None or v == "":
        v = r.get("actual")
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return v


def index_pra_grades(graded_props: list[dict[str, Any]], date: str) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    """WNBA Goblin OVER PRA rows from graded_props, keyed like observe rows."""
    idx: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for r in graded_props or []:
        if not isinstance(r, dict):
            continue
        if not _sport_ok(r.get("sport")):
            continue
        if not _goblin_over(r):
            continue
        if _prop_key(r.get("prop") or r.get("prop_type")) != "pra":
            continue
        key = _grade_key(date, r)
        if not key[1]:
            continue
        res = _result_of(r)
        prev = idx.get(key)
        if prev is None:
            idx[key] = r
            continue
        prev_res = _result_of(prev)
        if prev_res not in {"HIT", "MISS"} and res in {"HIT", "MISS"}:
            idx[key] = r
    return idx


def apply_grades(
    observe_rows: list[dict[str, Any]],
    graded_props: list[dict[str, Any]],
    date: str,
) -> tuple[list[dict[str, Any]], dict[str, int]]:
    """Fill result on observe rows. Does not change live ticket eligibility."""
    idx = index_pra_grades(graded_props, date)
    stats = {"matched": 0, "pending": 0, "unmatched_grades": 0, "hit": 0, "miss": 0, "void": 0}
    used: set[tuple[str, str, str, str]] = set()
    out: list[dict[str, Any]] = []
    for rec in observe_rows:
        row = dict(rec)
        key = _row_key(date, row)
        src = idx.get(key)
        if src is not None:
            used.add(key)
            res = _result_of(src)
            if res:
                row["result"] = res
                row["actual_value"] = _actual_of(src)
                row["graded_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
                stats["matched"] += 1
        out.append(row)
        res = str(row.get("result") or "").upper()
        if res == "HIT":
            stats["hit"] += 1
        elif res == "MISS":
            stats["miss"] += 1
        elif res == "VOID":
            stats["void"] += 1
        else:
            stats["pending"] += 1
    stats["unmatched_grades"] = len(idx) - len(used)
    return out, stats


def graded_props_path(date: str) -> Path | None:
    name = f"graded_props_{date}.json"
    sibling = _REPO.parent / "PropORACLE_main_cp"
    for folder in (
        _REPO / "ui_runner" / "templates",
        sibling / "ui_runner" / "templates",
        _REPO / "mobile" / "www",
        sibling / "mobile" / "www",
    ):
        p = folder / name
        if p.is_file():
            return p
    return None


def _load_graded_props(path: Path) -> list[dict[str, Any]]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        props = raw.get("props") or raw.get("rows") or []
        return props if isinstance(props, list) else []
    return raw if isinstance(raw, list) else []


def _hr(hits: int, n: int) -> float | None:
    if n <= 0:
        return None
    return round(100.0 * hits / n, 1)


def wilson_ci(hits: int, n: int, z: float = WILSON_Z) -> tuple[float | None, float | None, float | None]:
    """Wilson score 95% CI as percents. (hr, lo, hi)."""
    if n <= 0:
        return None, None, None
    p = hits / n
    z2 = z * z
    den = 1.0 + z2 / n
    center = (p + z2 / (2.0 * n)) / den
    half = (z / den) * math.sqrt(p * (1.0 - p) / n + z2 / (4.0 * n * n))
    lo = max(0.0, center - half)
    hi = min(1.0, center + half)
    return round(100.0 * p, 1), round(100.0 * lo, 1), round(100.0 * hi, 1)


def _player_game_key(r: dict[str, Any]) -> tuple[str, str]:
    return (str(r.get("slate_date") or "")[:10], _fold_player(r.get("player")))


def _cluster(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Independence check: repeating stars inflate n without new player-games."""
    players = Counter(_fold_player(r.get("player")) for r in rows)
    games = {_player_game_key(r) for r in rows}
    n = len(rows)
    top_name, top_n = ("", 0)
    if players:
        top_name, top_n = players.most_common(1)[0]
    top_share = round(top_n / n, 3) if n else None
    unique_ratio = round(len(games) / n, 3) if n else None
    return {
        "n": n,
        "distinct_players": len(players),
        "distinct_player_games": len(games),
        "unique_game_ratio": unique_ratio,
        "top_player": top_name,
        "top_player_n": top_n,
        "top_player_share": top_share,
        "top3": [{"player": p, "n": c} for p, c in players.most_common(3)],
        "repeat_heavy": bool(n and ((unique_ratio or 0) < 0.7 or (top_share or 0) > 0.25)),
    }


def _go_live(proposed_n: int, wilson_lo: float | None) -> dict[str, Any]:
    reasons: list[str] = []
    if proposed_n < GO_LIVE_MIN_N:
        reasons.append(f"proposed_decided {proposed_n} < {GO_LIVE_MIN_N}")
    if wilson_lo is None:
        reasons.append("no Wilson LB (n=0)")
    elif wilson_lo < GO_LIVE_WILSON_LB:
        reasons.append(f"Wilson LB {wilson_lo}% < {GO_LIVE_WILSON_LB}%")
    ok = not reasons
    return {
        "pass": ok,
        "rule": (
            f"Flip goblin_70_eligible only if proposed_decided>={GO_LIVE_MIN_N} "
            f"and Wilson 95% LB>={GO_LIVE_WILSON_LB}% on the not-already-live sleeve. "
            "Clustering is diagnostic, not a gate."
        ),
        "reasons": reasons,
        "verdict": "PASS" if ok else "FAIL",
    }


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    hit = miss = void = pending = 0
    hit_prop = miss_prop = 0
    proposed_decided_rows: list[dict[str, Any]] = []
    for r in rows:
        res = str(r.get("result") or "").upper()
        proposed = not r.get("live_eligible")
        if res == "HIT":
            hit += 1
            if proposed:
                hit_prop += 1
                proposed_decided_rows.append(r)
        elif res == "MISS":
            miss += 1
            if proposed:
                miss_prop += 1
                proposed_decided_rows.append(r)
        elif res == "VOID":
            void += 1
        else:
            pending += 1
    decided = hit + miss
    decided_prop = hit_prop + miss_prop
    _hr_all, lo_all, hi_all = wilson_ci(hit, decided)
    phr, plo, phi = wilson_ci(hit_prop, decided_prop)
    go = _go_live(decided_prop, plo)
    cluster = _cluster(proposed_decided_rows)
    return {
        "n": len(rows),
        "decided": decided,
        "hit": hit,
        "miss": miss,
        "void": void,
        "pending": pending,
        "hr": _hr(hit, decided),
        "wilson_lo": lo_all,
        "wilson_hi": hi_all,
        "proposed_decided": decided_prop,
        "proposed_hit": hit_prop,
        "proposed_hr": phr,
        "proposed_wilson_lo": plo,
        "proposed_wilson_hi": phi,
        "cluster": cluster,
        "go_live": go,
        "go_live_min_n": GO_LIVE_MIN_N,
        "go_live_wilson_lb": GO_LIVE_WILSON_LB,
        "note": (
            "In-sample Off-without-S/A was 78.1% n=32. "
            f"Live flip only if proposed_decided>={GO_LIVE_MIN_N} and "
            f"Wilson 95% LB>={GO_LIVE_WILSON_LB}% (not-already-live sleeve)."
        ),
    }


def join_grades(date: str | None = None, *, graded_file: Path | None = None) -> dict[str, Any]:
    """Match graded_props onto the jsonl. Live ticket path is unchanged."""
    all_rows = _read_jsonl()
    dates = sorted(
        {
            str(r.get("slate_date") or "")[:10]
            for r in all_rows
            if str(r.get("slate_date") or "")[:10]
        }
    )
    if date:
        dates = [date]
    by_date: dict[str, list[dict[str, Any]]] = {}
    for r in all_rows:
        d = str(r.get("slate_date") or "")[:10]
        by_date.setdefault(d, []).append(r)
    joined: list[dict[str, Any]] = []
    per_date: dict[str, dict[str, int]] = {}
    missing: list[str] = []
    for d in dates:
        rows = by_date.get(d) or []
        path = graded_file if graded_file is not None and date == d else graded_props_path(d)
        if path is None or not Path(path).is_file():
            missing.append(d)
            joined.extend(rows)
            continue
        graded = _load_graded_props(path)
        updated, stats = apply_grades(rows, graded, d)
        per_date[d] = stats
        _write_date_snapshot(d, updated, extra={"join": stats, "graded_file": str(path)})
        joined.extend(updated)
    leftovers = [r for r in all_rows if str(r.get("slate_date") or "")[:10] not in set(dates)]
    out_rows = leftovers + joined
    _write_jsonl(out_rows)
    summary = summarize([r for r in out_rows])
    summary["per_date"] = per_date
    summary["missing_graded"] = missing
    summary["joined_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
    SUMMARY.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main() -> int:
    ap = argparse.ArgumentParser(description="Join graded_props HIT/MISS onto WNBA PRA Off observe ledger")
    ap.add_argument("--date", default="", help="Slate date (default: every date in the jsonl)")
    ap.add_argument("--graded-file", default="", help="Override graded_props JSON path")
    args = ap.parse_args()
    graded = Path(args.graded_file) if args.graded_file else None
    summary = join_grades(args.date or None, graded_file=graded)
    hr = summary.get("hr")
    phr = summary.get("proposed_hr")
    plo = summary.get("proposed_wilson_lo")
    phi = summary.get("proposed_wilson_hi")
    go = summary.get("go_live") or {}
    cl = summary.get("cluster") or {}
    print(
        f"  [observe-join] WNBA PRA Off: decided {summary['decided']}/{summary['n']} "
        f"HIT {summary['hit']} MISS {summary['miss']} pending {summary['pending']}"
        + (f" hr={hr}%" if hr is not None else "")
    )
    wilson_s = ""
    if plo is not None and phi is not None:
        wilson_s = f" Wilson95 [{plo}, {phi}]"
    print(
        f"  [observe-join] proposed {summary['proposed_hit']}/{summary['proposed_decided']}"
        + (f" hr={phr}%" if phr is not None else "")
        + wilson_s
        + f"  go_live {go.get('verdict') or 'FAIL'}"
    )
    if cl:
        top = cl.get("top_player") or "—"
        print(
            f"  [observe-join] cluster unique_pg={cl.get('distinct_player_games')}/"
            f"{cl.get('n')} players={cl.get('distinct_players')} "
            f"top={top} {cl.get('top_player_n')} "
            f"({round(100 * (cl.get('top_player_share') or 0))}%)"
            + (" REPEAT_HEAVY" if cl.get("repeat_heavy") else "")
        )
    if go.get("reasons"):
        print("  [observe-join] go_live FAIL:", "; ".join(go["reasons"]))
    if summary.get("missing_graded"):
        print("  [observe-join] no graded_props for", ", ".join(summary["missing_graded"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
