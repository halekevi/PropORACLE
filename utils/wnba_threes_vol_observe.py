"""Observe-only ledger: WNBA Goblin 3PM L5=5 + 5+ looks + 3P%>=33 (replace L10+D).

Live goblin_70_eligible is unchanged. Flip only after proposed_decided>=50
and Wilson 95% LB>=65% on the not-already-live sleeve (same bar as PRA Off).
"""
from __future__ import annotations

import json
import math
import re
import unicodedata
from collections import Counter
from datetime import datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[1]
LEDGER = _REPO / "data" / "reports" / "wnba_threes_vol_observe.jsonl"
LATEST = _REPO / "data" / "reports" / "wnba_threes_vol_observe_latest.json"
SUMMARY = _REPO / "data" / "reports" / "wnba_threes_vol_observe_summary.json"
CACHE = _REPO / "Sports" / "WNBA" / "wnba_espn_cache.csv"
GO_LIVE_MIN_N = 50
GO_LIVE_WILSON_LB = 65.0
WILSON_Z = 1.96

from utils.player_name_utils import normalize_player_name  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import (  # noqa: E402
    directional_l10,
    directional_l5,
    goblin_70_eligible,
    wnba_threes_vol_observe_eligible,
)


def _norm_name(name: str) -> str:
    base = normalize_player_name(name).lower()
    s = re.sub(r"[^a-z0-9\s]", " ", base)
    s = re.sub(r"\s+", " ", s).strip()
    return " ".join(p for p in s.split() if p not in {"jr", "sr", "ii", "iii", "iv", "v"})


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


@lru_cache(maxsize=1)
def _prior_index() -> dict[tuple[str, str], dict[str, Any]]:
    """(player_norm, date) -> entering L10 3PA/g and season 3P%."""
    import pandas as pd

    if not CACHE.is_file():
        return {}
    df = pd.read_csv(CACHE, low_memory=False)
    df["game_date"] = df["game_date"].astype(str).str[:10]
    pn = df.get("PLAYER_NORM")
    if pn is None:
        df["PLAYER_NORM"] = df["PLAYER_NAME"].map(_norm_name)
    else:
        df["PLAYER_NORM"] = pn.fillna("").astype(str)
        empty = df["PLAYER_NORM"] == ""
        if empty.any():
            df.loc[empty, "PLAYER_NORM"] = df.loc[empty, "PLAYER_NAME"].map(_norm_name)
    df["FG3M"] = pd.to_numeric(df["FG3M"], errors="coerce").fillna(0.0)
    df["FG3A"] = pd.to_numeric(df["FG3A"], errors="coerce").fillna(0.0)
    df["MIN"] = pd.to_numeric(df.get("MIN"), errors="coerce").fillna(0.0)
    df = df.sort_values(["PLAYER_NORM", "game_date"])
    out: dict[tuple[str, str], dict[str, Any]] = {}
    running: dict[str, list] = {}
    for rec in df.itertuples(index=False):
        name = rec.PLAYER_NORM
        d = rec.game_date
        games = running.setdefault(name, [])
        last10 = games[-10:]
        s3a = sum(g[1] for g in games)
        s3m = sum(g[0] for g in games)
        l10_3pa = sum(g[1] for g in last10)
        out[(name, d)] = {
            "l10_3pa_g": (l10_3pa / len(last10)) if last10 else None,
            "season_3pp": (s3m / s3a) if s3a else None,
            "season_3pa": s3a,
        }
        if float(rec.MIN) >= 1.0:
            games.append((float(rec.FG3M), float(rec.FG3A)))
    return out


def attach_vol(r: dict[str, Any], date: str) -> dict[str, Any]:
    rec = dict(r)
    st = _prior_index().get((_norm_name(str(r.get("player") or "")), str(date)[:10])) or {}
    if rec.get("l10_3pa_g") in (None, ""):
        rec["l10_3pa_g"] = st.get("l10_3pa_g")
    if rec.get("season_3pp") in (None, ""):
        rec["season_3pp"] = st.get("season_3pp")
    if rec.get("season_3pa") in (None, ""):
        rec["season_3pa"] = st.get("season_3pa")
    return rec


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
        "l10_3pa_g": r.get("l10_3pa_g"),
        "season_3pp": r.get("season_3pp"),
        "season_3pa": r.get("season_3pa"),
        "cover": r.get("cover") if r.get("cover") is not None else r.get("dist_l5"),
        "def": r.get("def") or r.get("def_tier"),
        "live_eligible": live,
        "observe_eligible": True,
        "observe_reason": "threes_vol_replace_l10_d",
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


def log_board(date: str, board: list[dict[str, Any]] | None) -> dict[str, Any]:
    prior = _prior_results(date)
    seen: set[tuple[str, str, str, str]] = set()
    rows: list[dict[str, Any]] = []
    for r in board or []:
        rec = attach_vol(r, date)
        if not wnba_threes_vol_observe_eligible(rec):
            continue
        key = _row_key(date, rec)
        if key in seen:
            continue
        seen.add(key)
        snap = snapshot_row(date, rec)
        if key in prior:
            snap["result"] = prior[key].get("result")
            snap["actual_value"] = prior[key].get("actual_value")
            snap["graded_at"] = prior[key].get("graded_at")
        rows.append(snap)
    live_n = sum(1 for x in rows if x.get("live_eligible"))
    payload = {
        "slate_date": date,
        "logged_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "n": len(rows),
        "n_already_live": live_n,
        "n_proposed": len(rows) - live_n,
        "note": (
            "Observe-only. Goblin-70 still uses L5=5+L10>=8+D+cover for 3PM. "
            "These rows are the replace sleeve (L5=5 + 5+ L10 3PA/g + season 3P%>=33)."
        ),
        "rows": rows,
        "go_live_min_n": GO_LIVE_MIN_N,
        "go_live_wilson_lb": GO_LIVE_WILSON_LB,
    }
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    dated = LEDGER.parent / f"wnba_threes_vol_observe_{date}.json"
    dated.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    LATEST.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    others = [x for x in _read_jsonl() if str(x.get("slate_date") or "")[:10] != date]
    _write_jsonl(others + rows)
    return payload


def wilson_ci(hits: int, n: int, z: float = WILSON_Z) -> tuple[float | None, float | None, float | None]:
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


def summarize(rows: list[dict[str, Any]]) -> dict[str, Any]:
    hit = miss = pending = 0
    hit_prop = miss_prop = 0
    for r in rows:
        res = str(r.get("result") or "").upper()
        proposed = not r.get("live_eligible")
        if res == "HIT":
            hit += 1
            if proposed:
                hit_prop += 1
        elif res == "MISS":
            miss += 1
            if proposed:
                miss_prop += 1
        else:
            pending += 1
    decided = hit + miss
    decided_prop = hit_prop + miss_prop
    phr, plo, _phi = wilson_ci(hit_prop, decided_prop)
    reasons: list[str] = []
    if decided_prop < GO_LIVE_MIN_N:
        reasons.append(f"proposed_decided {decided_prop} < {GO_LIVE_MIN_N}")
    if plo is None:
        reasons.append("no Wilson LB (n=0)")
    elif plo < GO_LIVE_WILSON_LB:
        reasons.append(f"Wilson LB {plo}% < {GO_LIVE_WILSON_LB}%")
    players = Counter(_fold_player(r.get("player")) for r in rows if not r.get("live_eligible"))
    return {
        "n": len(rows),
        "decided": decided,
        "hit": hit,
        "miss": miss,
        "pending": pending,
        "proposed_decided": decided_prop,
        "proposed_hit": hit_prop,
        "proposed_hr": phr,
        "proposed_wilson_lo": plo,
        "go_live": {
            "pass": not reasons,
            "verdict": "PASS" if not reasons else "FAIL",
            "reasons": reasons,
            "rule": (
                f"Flip goblin_70_eligible only if proposed_decided>={GO_LIVE_MIN_N} "
                f"and Wilson 95% LB>={GO_LIVE_WILSON_LB}% on the not-already-live sleeve."
            ),
        },
        "distinct_proposed_players": len(players),
        "note": (
            "Replace-version 3PM sleeve. In-sample B was 89.8% n=88 (L5=5+vol); "
            "A∩B 94.3% n=35; B-only 86.8% n=53. Live path unchanged."
        ),
    }
