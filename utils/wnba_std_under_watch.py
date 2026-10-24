"""Watch ledger: WNBA Standard UNDER that already tickets G70 Flex.

Not a proposed flip — this sleeve is live on the generic L5=5+L10>=8+D+cover
stack with no per-prop keep pass. Same n>=50 / Wilson 95% LB>=65% bar as
PRA Off so the mixed 76.5% n=34 cannot sit unmonitored.
"""
from __future__ import annotations

import json
import re
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[1]
LEDGER = _REPO / "data" / "reports" / "wnba_std_under_watch.jsonl"
LATEST = _REPO / "data" / "reports" / "wnba_std_under_watch_latest.json"

from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import (  # noqa: E402
    directional_l10,
    directional_l5,
    standard_ticket_eligible,
)
from utils.wnba_pra_off_observe import wilson_ci  # noqa: E402

WATCH_MIN_N = 50
WATCH_WILSON_LB = 65.0


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


def _row_key(date: str, r: dict[str, Any]) -> tuple[str, str, str, str]:
    return (
        str(date)[:10],
        _fold_player(r.get("player")),
        _prop_key(r.get("prop") or r.get("prop_type")),
        _line_key(r.get("line")),
    )


def wnba_std_under_watch_eligible(r: dict[str, Any]) -> bool:
    if str(r.get("sport") or "").strip().upper() != "WNBA":
        return False
    return bool(standard_ticket_eligible(r)) and str(
        r.get("side") or r.get("direction") or ""
    ).strip().upper() in {"U", "UNDER"}


def snapshot_row(date: str, r: dict[str, Any]) -> dict[str, Any]:
    return {
        "slate_date": date,
        "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "player": r.get("player"),
        "prop": r.get("prop") or r.get("prop_type"),
        "side": "UNDER",
        "pick_type": "Standard",
        "line": r.get("line"),
        "l5": directional_l5(r),
        "l10": directional_l10(r),
        "cover": r.get("cover") if r.get("cover") is not None else r.get("dist_l5"),
        "def": r.get("def") or r.get("def_tier"),
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


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    hit = miss = pending = 0
    by_prop: dict[str, list[int]] = {}
    for r in rows:
        res = str(r.get("result") or "").strip().upper()
        prop = _prop_key(r.get("prop"))
        bucket = by_prop.setdefault(prop, [0, 0, 0])
        if res == "HIT":
            hit += 1
            bucket[0] += 1
        elif res == "MISS":
            miss += 1
            bucket[1] += 1
        else:
            pending += 1
            bucket[2] += 1
    decided = hit + miss
    hr, lo, _hi = wilson_ci(hit, decided)
    reasons: list[str] = []
    if decided < WATCH_MIN_N:
        reasons.append(f"decided {decided} < {WATCH_MIN_N}")
    if lo is None:
        reasons.append("no Wilson LB (n=0)")
    elif lo < WATCH_WILSON_LB:
        reasons.append(f"Wilson LB {lo}% < {WATCH_WILSON_LB}%")
    per_prop = {}
    for prop, (h, m, p) in sorted(by_prop.items()):
        n = h + m
        phr, plo, _ = wilson_ci(h, n)
        per_prop[prop] = {"hit": h, "miss": m, "pending": p, "hr": phr, "wilson_lo": plo}
    return {
        "n": len(rows),
        "decided": decided,
        "hit": hit,
        "miss": miss,
        "pending": pending,
        "hr": hr,
        "wilson_lo": lo,
        "watch": {
            "pass": not reasons,
            "verdict": "PASS" if not reasons else "HOLD",
            "reasons": reasons,
            "rule": (
                f"Treat WNBA Standard UNDER G70 as a real keep only if decided>="
                f"{WATCH_MIN_N} and Wilson 95% LB>={WATCH_WILSON_LB}% overall, "
                "then still split by prop before promoting any one market."
            ),
        },
        "per_prop": per_prop,
        "distinct_players": len({_fold_player(r.get("player")) for r in rows}),
        "note": (
            "Live Flex sleeve. Generic L5=5+L10>=8+D+cover, mixed props. "
            "Not a keep-list pass. Do not read the aggregate as one market."
        ),
    }


def log_board(date: str, board: list[dict[str, Any]] | None) -> dict[str, Any]:
    prior = _prior_results(date)
    seen: set[tuple[str, str, str, str]] = set()
    rows: list[dict[str, Any]] = []
    for r in board or []:
        rec = dict(r)
        rec.setdefault("side", rec.get("direction") or "UNDER")
        if not wnba_std_under_watch_eligible(rec):
            continue
        key = _row_key(date, rec)
        if key in seen:
            continue
        seen.add(key)
        snap = snapshot_row(date, rec)
        old = prior.get(key)
        if old:
            snap["result"] = old.get("result")
            snap["actual_value"] = old.get("actual_value")
            snap["graded_at"] = old.get("graded_at")
        rows.append(snap)
    payload = {
        "date": date,
        "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        **summarize(rows),
        "rows": rows,
    }
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    dated = LEDGER.parent / f"wnba_std_under_watch_{date}.json"
    dated.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    LATEST.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    others = [x for x in _read_jsonl() if str(x.get("slate_date") or "")[:10] != date]
    _write_jsonl(others + rows)
    return payload
