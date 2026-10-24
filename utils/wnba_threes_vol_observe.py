"""Observe-only ledger: WNBA Goblin 3PM L5=5 + 5+ looks + 3P%>=33 (replace L10+D).

Live goblin_70_eligible is unchanged. Flip only after proposed_decided>=50
and Wilson 95% LB>=65% on the not-already-live sleeve (same bar as PRA Off).
"""
from __future__ import annotations

import argparse
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
        if float(rec.MIN) < 1.0:
            continue
        name = rec.PLAYER_NORM
        d = rec.game_date
        games = running.setdefault(name, [])
        games.append((float(rec.FG3M), float(rec.FG3A)))
        last10 = games[-10:]
        s3a = sum(g[1] for g in games)
        s3m = sum(g[0] for g in games)
        l10_3pa = sum(g[1] for g in last10)
        # Post-game closing stats for this box. Board lookup uses the last
        # closing date strictly before the slate — never the on-slate row.
        out[(name, d)] = {
            "l10_3pa_g": (l10_3pa / len(last10)) if last10 else None,
            "season_3pp": (s3m / s3a) if s3a else None,
            "season_3pa": s3a,
        }
    return out


def clear_vol_cache() -> None:
    _prior_index.cache_clear()
    _boxes_by_player.cache_clear()


@lru_cache(maxsize=1)
def _boxes_by_player() -> dict[str, tuple[tuple[str, dict[str, Any]], ...]]:
    by: dict[str, list[tuple[str, dict[str, Any]]]] = {}
    for (name, d), st in _prior_index().items():
        by.setdefault(name, []).append((d, st))
    return {k: tuple(sorted(v, key=lambda x: x[0])) for k, v in by.items()}


def vol_before_slate(player: object, slate_date: object) -> dict[str, Any]:
    """Entering-game 3PA/3P% from the last completed box strictly before slate."""
    d = str(slate_date or "")[:10]
    if len(d) != 10:
        return {}
    name = _norm_name(str(player or ""))
    chosen: dict[str, Any] = {}
    for box_d, st in _boxes_by_player().get(name) or ():
        if box_d < d:
            chosen = st
        else:
            break
    return chosen


def attach_vol(r: dict[str, Any], date: str) -> dict[str, Any]:
    rec = dict(r)
    st = vol_before_slate(r.get("player"), date)
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


def index_threes_grades(
    graded_props: list[dict[str, Any]], date: str
) -> dict[tuple[str, str, str, str], dict[str, Any]]:
    idx: dict[tuple[str, str, str, str], dict[str, Any]] = {}
    for r in graded_props or []:
        if not isinstance(r, dict):
            continue
        if not _sport_ok(r.get("sport")):
            continue
        if not _goblin_over(r):
            continue
        if _prop_key(r.get("prop") or r.get("prop_type")) != "threes":
            continue
        key = _row_key(date, r)
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
    idx = index_threes_grades(graded_props, date)
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


def _write_date_snapshot(date: str, rows: list[dict[str, Any]], extra: dict[str, Any] | None = None) -> dict[str, Any]:
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
    if extra:
        payload.update(extra)
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    dated = LEDGER.parent / f"wnba_threes_vol_observe_{date}.json"
    dated.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return payload


def join_grades(date: str | None = None, *, graded_file: Path | None = None) -> dict[str, Any]:
    """Match graded_props onto the 3PM volume jsonl. Live ticket path is unchanged."""
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
    ap = argparse.ArgumentParser(description="Join graded_props HIT/MISS onto WNBA 3PM volume observe ledger")
    ap.add_argument("--date", default="", help="Slate date (default: every date in the jsonl)")
    ap.add_argument("--graded-file", default="", help="Override graded_props JSON path")
    args = ap.parse_args()
    graded = Path(args.graded_file) if args.graded_file else None
    summary = join_grades(args.date or None, graded_file=graded)
    go = summary.get("go_live") or {}
    print(
        f"  [observe-join] WNBA 3PM vol: decided {summary['decided']}/{summary['n']} "
        f"HIT {summary['hit']} MISS {summary['miss']} pending {summary['pending']}"
    )
    print(
        f"  [observe-join] proposed {summary['proposed_hit']}/{summary['proposed_decided']}"
        + (f" hr={summary.get('proposed_hr')}%" if summary.get("proposed_hr") is not None else "")
        + f"  go_live {go.get('verdict') or 'FAIL'}"
    )
    if go.get("reasons"):
        print("  [observe-join] go_live FAIL:", "; ".join(go["reasons"]))
    if summary.get("missing_graded"):
        print("  [observe-join] no graded_props for", ", ".join(summary["missing_graded"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
