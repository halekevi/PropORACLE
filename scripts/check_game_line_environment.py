#!/usr/bin/env python3
"""Robustness checks for NBA game-line environment. Not a gate.

Prop-type slices are intentionally absent. Cells from a search over props
are not leads. MLB, WNBA, NFL, and CFB are not scored here; their buckets
are declared in data/reports/game_line_environment.md and stay unused
until those lines exist.
"""
from __future__ import annotations

import bisect
import json
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

GRADED = _REPO / "ui_runner" / "templates"
LINES_DB = _REPO / "data" / "cache" / "game_lines.db"
LOGS_DB = _REPO / "data" / "cache" / "historical_actuals.db"

# Confirmatory contrasts from the first pass. Do not add prop types.
CONTRASTS = (
    ("over_fav_7_9", "OVER", "fav79"),
    ("over_dog_10", "OVER", "dog10"),
    ("under_fav_10", "UNDER", "fav10"),
)
REGULAR_SEASON_END = "2026-04-12"
POSTSEASON_START = "2026-04-14"
MARCH_APRIL_END = "2026-04-30"


def _cluster_effect(y: np.ndarray, treat: np.ndarray, groups: list[str]) -> tuple[float, float, int]:
    """OLS coefficient on treat, cluster-robust SE by game. Returns (effect, se, n_games)."""
    x = np.column_stack([np.ones(len(y)), treat.astype(float)])
    xtx = x.T @ x
    beta = np.linalg.solve(xtx, x.T @ y)
    resid = y - x @ beta
    buckets: dict[str, list[int]] = defaultdict(list)
    for i, g in enumerate(groups):
        buckets[g].append(i)
    meat = np.zeros((2, 2))
    for idxs in buckets.values():
        score = x[idxs].T @ resid[idxs]
        meat += np.outer(score, score)
    g_n = len(buckets)
    n, k = x.shape
    if g_n <= k:
        return float(beta[1]), float("nan"), g_n
    scale = (g_n / (g_n - 1)) * ((n - 1) / (n - k))
    var = scale * (np.linalg.inv(xtx) @ meat @ np.linalg.inv(xtx))
    return float(beta[1]), float(np.sqrt(max(var[1, 1], 0.0))), g_n


def _flag(spread: float | None, code: str) -> bool:
    if spread is None:
        return False
    if code == "fav79":
        return -9.5 <= spread <= -7.0
    if code == "dog10":
        return spread >= 10.0
    if code == "fav10":
        return spread <= -10.0
    raise KeyError(code)


def _spread_band(team_spread: float | None) -> str:
    """Player-team spread bands. Negative = that team favored."""
    if team_spread is None:
        return "missing"
    s = float(team_spread)
    a = abs(s)
    if a < 3:
        return "pk"
    if a < 7:
        band = "3-6.5"
    elif a < 10:
        band = "7-9.5"
    else:
        band = "10+"
    if s < 0:
        return f"fav {band}"
    if s > 0:
        return f"dog {band}"
    return "pk"


def _rate_cluster(rows: list[dict]) -> tuple[float, int, int, int]:
    if not rows:
        return 0.0, 0, 0, 0
    hits = sum(r["hit"] for r in rows)
    n = len(rows)
    games = len({r["game"] for r in rows})
    return hits / n, hits, n, games


def _load_lines() -> dict[tuple[str, str], dict]:
    conn = sqlite3.connect(LINES_DB)
    try:
        rows = conn.execute(
            """
            SELECT game_date, home_team, away_team, home_team_id, away_team_id,
                   spread, total, home_win_prob, away_win_prob, event_id,
                   provider, is_closing
            FROM game_lines WHERE sport = 'NBA'
            """
        ).fetchall()
        xw = conn.execute(
            "SELECT abbr, espn_team_id FROM team_crosswalk WHERE sport = 'NBA'"
        ).fetchall()
    finally:
        conn.close()
    by_team_date: dict[tuple[str, str], dict] = {}
    for date, home, away, hid, aid, spread, total, hwp, awp, event_id, provider, is_closing in rows:
        rec = {
            "date": date,
            "home": str(home or "").upper(),
            "away": str(away or "").upper(),
            "hid": str(hid or ""),
            "aid": str(aid or ""),
            "spread": None if spread is None else float(spread),
            "total": None if total is None else float(total),
            "hwp": hwp,
            "awp": awp,
            "event_id": str(event_id or ""),
            "provider": str(provider or "").strip(),
            "is_closing": is_closing,
            "has_close_meta": bool(str(provider or "").strip()) and is_closing is not None,
        }
        if rec["hid"]:
            by_team_date[(date, rec["hid"])] = rec
        if rec["aid"]:
            by_team_date[(date, rec["aid"])] = rec
        by_team_date[(date, "abbr:" + rec["home"])] = rec
        by_team_date[(date, "abbr:" + rec["away"])] = rec
    abbr_to_id: dict[str, str] = {}
    ambiguous: set[str] = set()
    for abbr, tid in xw:
        ab = str(abbr or "").upper()
        if ab in abbr_to_id and abbr_to_id[ab] != str(tid):
            ambiguous.add(ab)
        abbr_to_id[ab] = str(tid)
    for ab in ambiguous:
        abbr_to_id.pop(ab, None)
    return {"by_team_date": by_team_date, "abbr_to_id": abbr_to_id}


def _lookup(bundle: dict, date: str, team: str) -> tuple[dict | None, bool | None]:
    ab = str(team or "").strip().upper()
    tid = bundle["abbr_to_id"].get(ab, "")
    rec = None
    if tid:
        rec = bundle["by_team_date"].get((date, tid))
    if rec is None:
        rec = bundle["by_team_date"].get((date, "abbr:" + ab))
    if rec is None:
        return None, None
    player_is_home = (tid and tid == rec["hid"]) or ab == rec["home"]
    if not player_is_home and not ((tid and tid == rec["aid"]) or ab == rec["away"]):
        return None, None
    return rec, bool(player_is_home)


def _norm_name(name: str) -> str:
    raw = str(name or "").lower().replace(".", " ")
    keep = []
    for ch in raw:
        keep.append(ch if ch.isalpha() or ch.isspace() else " ")
    parts = [p for p in "".join(keep).split() if p not in {"jr", "sr", "ii", "iii", "iv"}]
    return " ".join(parts)


def _load_minutes() -> dict[str, tuple[list[str], list[float], list[float]]]:
    conn = sqlite3.connect(LOGS_DB)
    try:
        rows = conn.execute(
            """
            SELECT player_name, game_date, minutes
            FROM player_game_logs
            WHERE sport = 'NBA' AND game_date >= '2025-10-01' AND game_date <= '2026-06-13'
              AND minutes IS NOT NULL
            """
        ).fetchall()
    finally:
        conn.close()
    grouped: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for name, date, minutes in rows:
        grouped[_norm_name(name)].append((str(date), float(minutes)))
    out = {}
    for name, pairs in grouped.items():
        pairs.sort()
        dates = [d for d, _ in pairs]
        mins = [m for _, m in pairs]
        prefix = []
        running = 0.0
        for m in mins:
            running += m
            prefix.append(running)
        out[name] = (dates, mins, prefix)
    return out


def _role(logs: dict, name: str, prop_date: str) -> str:
    packed = logs.get(_norm_name(name))
    if not packed:
        return "unknown"
    dates, _, prefix = packed
    idx = bisect.bisect_left(dates, prop_date)
    if idx <= 0:
        return "unknown"
    mean = prefix[idx - 1] / idx
    if idx >= 15 and mean >= 28:
        return "starter"
    if idx >= 10 and mean < 16:
        return "bench"
    return "rotation"


def _fmt(effect: float, se: float, games: int, hits: int, n: int) -> str:
    if n == 0 or games == 0:
        return "n=0"
    rate = hits / n
    lo = effect - 1.96 * se
    hi = effect + 1.96 * se
    return (
        f"rows {hits}/{n} = {rate:.1%}  games {games}  "
        f"lift {effect:+.1%}  cluster SE {se:.1%}  95% CI [{lo:+.1%}, {hi:+.1%}]"
    )


def _effect(rows: list[dict], code: str) -> tuple[float, float, int, int, int]:
    if not rows:
        return 0.0, float("nan"), 0, 0, 0
    y = np.array([r["hit"] for r in rows], dtype=float)
    treat = np.array([1.0 if _flag(r["team_spread"], code) else 0.0 for r in rows])
    if treat.sum() == 0 or treat.sum() == len(treat):
        return 0.0, float("nan"), len({r["game"] for r in rows}), int(y.sum()), len(rows)
    effect, se, _ = _cluster_effect(y, treat, [r["game"] for r in rows])
    in_bucket = treat.astype(bool)
    games = len({r["game"] for r, flag in zip(rows, in_bucket) if flag})
    return effect, se, games, int(y[in_bucket].sum()), int(in_bucket.sum())


def main() -> None:
    bundle = _load_lines()
    logs = _load_minutes()
    rows_by_dir: dict[str, list[dict]] = {"OVER": [], "UNDER": []}
    seen = set()
    decided = matched = 0
    for path in sorted(GRADED.glob("graded_props_*.json")):
        date = path.name[len("graded_props_") : -5]
        if date < "2026-03-06" or date > "2026-06-13":
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        for row in data.get("props") or []:
            if str(row.get("sport") or "") not in ("NBA", "NBA1Q", "NBA1H"):
                continue
            result = str(row.get("result") or "").upper()
            if result not in ("HIT", "MISS"):
                continue
            direction = str(row.get("direction") or row.get("over_under") or "").upper()
            if direction not in ("OVER", "UNDER"):
                continue
            team = str(row.get("team") or "")
            if not team or "/" in team:
                continue
            key = (date, row.get("player"), row.get("prop"), row.get("line"), direction, team)
            if key in seen:
                continue
            seen.add(key)
            decided += 1
            rec, is_home = _lookup(bundle, date, team)
            if rec is None or is_home is None or rec["spread"] is None:
                continue
            matched += 1
            team_spread = rec["spread"] if is_home else -rec["spread"]
            game = rec["event_id"] or f"{date}|{rec['hid']}|{rec['aid']}|{rec['home']}|{rec['away']}"
            rows_by_dir[direction].append(
                {
                    "hit": 1 if result == "HIT" else 0,
                    "team_spread": team_spread,
                    "game": game,
                    "date": date,
                    "role": _role(logs, str(row.get("player") or ""), date),
                    "band": _spread_band(team_spread),
                    "has_close_meta": bool(rec.get("has_close_meta")),
                }
            )

    print(f"NBA decided rows in window: {decided}")
    print(f"matched on team id with a spread: {matched} ({matched / decided:.1%})" if decided else "none")
    with_meta = sum(1 for d in ("OVER", "UNDER") for r in rows_by_dir[d] if r["has_close_meta"])
    if matched:
        print(f"rows with provider + is_closing filled: {with_meta}/{matched} ({with_meta / matched:.1%})")
    print("Older rows without close meta are not closing-line tests.")
    print("Role is entering-game minutes, not minutes in that game.")
    print("Starter: mean >= 28 before the date, >= 15 games. Bench: mean < 16, >= 10 games.")
    print()

    overs = rows_by_dir["OVER"]
    print("== postseason over base rates by spread band ==")
    print("If every band rises in the postseason, the fav 7-9.5 cell is playoff minutes, not the spread.")
    bands = sorted({r["band"] for r in overs})
    for band in bands:
        reg = [r for r in overs if r["band"] == band and r["date"] <= REGULAR_SEASON_END]
        post = [r for r in overs if r["band"] == band and r["date"] >= POSTSEASON_START]
        rr, rh, rn, rg = _rate_cluster(reg)
        pr, ph, pn, pg = _rate_cluster(post)
        if rn < 50 and pn < 50:
            continue
        delta = pr - rr if rn and pn else float("nan")
        print(
            f"  {band:12} regular {rh}/{rn} = {rr:5.1%} ({rg}g)  "
            f"post {ph}/{pn} = {pr:5.1%} ({pg}g)  delta {delta:+.1%}"
        )
    all_reg = [r for r in overs if r["date"] <= REGULAR_SEASON_END]
    all_post = [r for r in overs if r["date"] >= POSTSEASON_START]
    rr, rh, rn, rg = _rate_cluster(all_reg)
    pr, ph, pn, pg = _rate_cluster(all_post)
    print(
        f"  {'ALL OVERS':12} regular {rh}/{rn} = {rr:5.1%} ({rg}g)  "
        f"post {ph}/{pn} = {pr:5.1%} ({pg}g)  delta {pr - rr:+.1%}"
    )
    print()

    for label, direction, code in CONTRASTS:
        pool = rows_by_dir[direction]
        effect, se, games, hits, n = _effect(pool, code)
        print(f"== {label} ==")
        print("  all     " + _fmt(effect, se, games, hits, n))

        for role in ("starter", "bench"):
            sub = [r for r in pool if r["role"] == role]
            e2, s2, g2, h2, n2 = _effect(sub, code)
            print(f"  {role:8}" + _fmt(e2, s2, g2, h2, n2))

        for name, pred in (
            ("regular", lambda d: d <= REGULAR_SEASON_END),
            ("postseason", lambda d: d >= POSTSEASON_START),
            ("mar_apr", lambda d: d <= MARCH_APRIL_END),
            ("may_jun", lambda d: d > MARCH_APRIL_END),
        ):
            sub = [r for r in pool if pred(r["date"])]
            e2, s2, g2, h2, n2 = _effect(sub, code)
            print(f"  {name:12}" + _fmt(e2, s2, g2, h2, n2))

        by_date: dict[str, list[dict]] = defaultdict(list)
        for r in pool:
            by_date[r["date"]].append(r)
        lifts = []
        for hold in by_date:
            kept = [r for d, group in by_date.items() if d != hold for r in group]
            e2, _, g2, _, n2 = _effect(kept, code)
            if n2 and g2:
                lifts.append(e2)
        if lifts:
            arr = np.array(lifts)
            print(
                f"  leave-one-date-out lifts: min {arr.min():+.1%}  median {np.median(arr):+.1%}  "
                f"max {arr.max():+.1%}  positive {int((arr > 0).sum())}/{len(arr)}"
            )
        print()


if __name__ == "__main__":
    main()
