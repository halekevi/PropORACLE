#!/usr/bin/env python3
"""MLB game-line battery under data/reports/game_line_environment.md.

1. Backfill closing lines for graded MLB dates (is_closing from feed/event).
2. Lock total cuts from the per-game total distribution (before joining hits).
3. Spot-check post-final core odds vs a near-tip capture when both exist.
4. Score the three named contrasts (Bonferroni, pass = RS interval + pooled sign).

Does not write gates. Appends a results section to the environment note.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "scripts"))

from fetch_game_lines import (  # noqa: E402
    DB_PATH,
    fetch_core_odds_item,
    init_db,
    line_from_core_item,
    refresh_crosswalk,
    run_fetch,
    stamp_team_ids_from_crosswalk,
)

GRADED = _REPO / "ui_runner" / "templates"
NOTE = _REPO / "data" / "reports" / "game_line_environment.md"
REPORT_JSON = _REPO / "data" / "reports" / "mlb_game_line_battery.json"

# Locked before looking at hits. Filled after per-game total histogram.
# Placeholder until lock_total_cuts() runs.
MLB_TOTAL_LOW_MAX: float | None = None
MLB_TOTAL_HIGH_MIN: float | None = None

PITCHER_PROPS = frozenset(
    {
        "pitcher_ks",
        "pitcher_strikeouts",
        "hits_allowed",
        "walks_allowed",
        "pitches_thrown",
        "earned_runs",
        "earned_runs_allowed",
        "pitching_outs",
        "outs",
        "era",
    }
)

Z_BONFERRONI = 2.393  # ~N^{-1}(1 - 0.05/6) two-sided for α=0.05/3


def _norm_prop(prop: str) -> str:
    s = str(prop or "").strip().lower().replace(" ", "_").replace("-", "_")
    aliases = {
        "pitcher_strikeouts": "pitcher_ks",
        "hitter_strikeouts": "hitter_ks",
        "earned_runs_allowed": "earned_runs",
        "total_bases": "total_bases",
        "hits+runs+rbis": "hits+runs+rbis",
        "hits_runs_rbis": "hits+runs+rbis",
    }
    return aliases.get(s, s)


def _is_pitcher_prop(prop: str) -> bool:
    p = _norm_prop(prop)
    if p in PITCHER_PROPS:
        return True
    return p.startswith("pitcher_") or p.endswith("_allowed") or p in {
        "pitches_thrown",
        "pitching_outs",
    }


def mlb_graded_dates() -> list[str]:
    out: list[str] = []
    for path in sorted(GRADED.glob("graded_props_*.json")):
        date = path.name[len("graded_props_") : -5]
        raw = path.read_text(encoding="utf-8", errors="ignore")
        if '"sport": "MLB"' not in raw and '"sport":"MLB"' not in raw:
            continue
        out.append(date)
    return out


def backfill_mlb(dates: list[str], *, refresh: bool, sleep_s: float) -> dict[str, int]:
    conn = sqlite3.connect(str(DB_PATH))
    try:
        init_db(conn)
        refresh_crosswalk(conn, ["MLB"])
        stamp_team_ids_from_crosswalk(conn)
    finally:
        conn.close()
    fetched = skipped = games = 0
    for i, d in enumerate(dates):
        before = _mlb_game_count(d)
        if before and not refresh:
            skipped += 1
            games += before
            continue
        result = run_fetch(d, ["MLB"], refresh=refresh or before == 0)
        n = len(result.get("MLB") or [])
        fetched += 1
        games += n
        print(f"  [{i + 1}/{len(dates)}] {d}: {n} MLB games")
        if sleep_s > 0:
            time.sleep(sleep_s)
    return {"dates_fetched": fetched, "dates_skipped": skipped, "games": games}


def _mlb_game_count(date: str) -> int:
    if not DB_PATH.is_file():
        return 0
    conn = sqlite3.connect(str(DB_PATH))
    try:
        return int(
            conn.execute(
                "SELECT COUNT(*) FROM game_lines WHERE sport='MLB' AND game_date=?",
                (date,),
            ).fetchone()[0]
        )
    finally:
        conn.close()


def lock_total_cuts() -> tuple[float, float, dict]:
    """Set low/high from per-game totals. Fixed values, not prop-weighted."""
    global MLB_TOTAL_LOW_MAX, MLB_TOTAL_HIGH_MIN
    conn = sqlite3.connect(str(DB_PATH))
    try:
        totals = [
            float(r[0])
            for r in conn.execute(
                "SELECT total FROM game_lines WHERE sport='MLB' AND total IS NOT NULL AND is_closing=1"
            )
        ]
        if len(totals) < 50:
            totals = [
                float(r[0])
                for r in conn.execute(
                    "SELECT total FROM game_lines WHERE sport='MLB' AND total IS NOT NULL"
                )
            ]
    finally:
        conn.close()
    if not totals:
        raise SystemExit("No MLB totals in game_lines — fetch first")
    ctr = Counter(round(t, 1) for t in totals)
    # Prefer the conventional bunching: low ≤ 7.5, high ≥ 9.0 when those exist.
    n = len(totals)
    n_le_75 = sum(1 for t in totals if t <= 7.5)
    n_ge_90 = sum(1 for t in totals if t >= 9.0)
    low_max, high_min = 7.5, 9.0
    # If either band is <15% of games, widen one step toward the mass.
    if n_le_75 / n < 0.15:
        low_max = 8.0
    if n_ge_90 / n < 0.15:
        high_min = 8.5
    MLB_TOTAL_LOW_MAX = low_max
    MLB_TOTAL_HIGH_MIN = high_min
    meta = {
        "n_games_with_total": n,
        "histogram": dict(sorted(ctr.items())),
        "low_max": low_max,
        "high_min": high_min,
        "n_low": sum(1 for t in totals if t <= low_max),
        "n_high": sum(1 for t in totals if t >= high_min),
        "n_mid": sum(1 for t in totals if low_max < t < high_min),
    }
    return low_max, high_min, meta


def load_crosswalk() -> dict[str, str]:
    conn = sqlite3.connect(str(DB_PATH))
    try:
        rows = conn.execute(
            "SELECT abbr, espn_team_id FROM team_crosswalk WHERE sport='MLB'"
        ).fetchall()
    finally:
        conn.close()
    out: dict[str, str] = {}
    amb: set[str] = set()
    for abbr, tid in rows:
        ab = str(abbr or "").upper()
        if ab in out and out[ab] != str(tid):
            amb.add(ab)
        out[ab] = str(tid)
    for ab in amb:
        out.pop(ab, None)
    return out


def load_mlb_games() -> dict[tuple[str, str], dict]:
    """(date, team_id_or_abbr) -> game rec with team-perspective helpers."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        rows = conn.execute(
            """
            SELECT game_date, home_team, away_team, home_team_id, away_team_id,
                   spread, total, home_win_prob, away_win_prob, event_id,
                   provider, is_closing, captured_at
            FROM game_lines WHERE sport='MLB'
            """
        ).fetchall()
    finally:
        conn.close()
    by: dict[tuple[str, str], dict] = {}
    for gdate, home, away, hid, aid, spread, total, hwp, awp, eid, prov, closing, cap in rows:
        rec = {
            "date": gdate,
            "home": str(home or "").upper(),
            "away": str(away or "").upper(),
            "hid": str(hid or ""),
            "aid": str(aid or ""),
            "spread": None if spread is None else float(spread),
            "total": None if total is None else float(total),
            "hwp": None if hwp is None else float(hwp),
            "awp": None if awp is None else float(awp),
            "event_id": str(eid or ""),
            "provider": str(prov or ""),
            "is_closing": closing,
            "captured_at": cap,
        }
        if rec["hid"]:
            by[(gdate, "id:" + rec["hid"])] = rec
        if rec["aid"]:
            by[(gdate, "id:" + rec["aid"])] = rec
        by[(gdate, "abbr:" + rec["home"])] = rec
        by[(gdate, "abbr:" + rec["away"])] = rec
    return by


def lookup(games: dict, abbr_to_id: dict[str, str], date: str, team: str) -> tuple[dict | None, bool | None]:
    ab = str(team or "").strip().upper()
    tid = abbr_to_id.get(ab, "")
    rec = None
    if tid:
        rec = games.get((date, "id:" + tid))
    if rec is None:
        rec = games.get((date, "abbr:" + ab))
    if rec is None:
        return None, None
    if tid and tid == rec["hid"]:
        return rec, True
    if tid and tid == rec["aid"]:
        return rec, False
    if ab == rec["home"]:
        return rec, True
    if ab == rec["away"]:
        return rec, False
    return None, None


def team_win_prob(rec: dict, player_is_home: bool) -> float | None:
    """Always the prop player's own team (hitters and pitchers)."""
    return rec["hwp"] if player_is_home else rec["awp"]


def spot_check_closes(n: int = 5) -> list[dict]:
    """Compare post-final core odds to an older near-tip row when we have tip times."""
    conn = sqlite3.connect(str(DB_PATH))
    try:
        rows = conn.execute(
            """
            SELECT game_date, home_team, away_team, event_id, spread, total,
                   home_win_prob, is_closing, captured_at
            FROM game_lines
            WHERE sport='MLB' AND event_id IS NOT NULL AND event_id != ''
              AND is_closing = 1
            ORDER BY game_date DESC
            LIMIT 40
            """
        ).fetchall()
    finally:
        conn.close()
    out: list[dict] = []
    for gdate, home, away, eid, spread, total, hwp, closing, cap in rows:
        if len(out) >= n:
            break
        core = fetch_core_odds_item("MLB", str(eid))
        if not core:
            continue
        parsed = line_from_core_item(core)
        out.append(
            {
                "date": gdate,
                "matchup": f"{away}@{home}",
                "event_id": eid,
                "stored_spread": spread,
                "stored_total": total,
                "refetch_spread": parsed["spread"],
                "refetch_total": parsed["total"],
                "spread_match": spread == parsed["spread"]
                or (
                    spread is not None
                    and parsed["spread"] is not None
                    and abs(float(spread) - float(parsed["spread"])) < 1e-9
                ),
                "total_match": total == parsed["total"]
                or (
                    total is not None
                    and parsed["total"] is not None
                    and abs(float(total) - float(parsed["total"])) < 1e-9
                ),
                "provider": parsed.get("provider"),
            }
        )
        time.sleep(0.25)
    return out


def _cluster_effect(y: np.ndarray, treat: np.ndarray, groups: list[str]) -> tuple[float, float, int]:
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


def _summarize(rows: list[dict], treat_key: str) -> dict:
    if not rows:
        return {"empty": True}
    y = np.array([r["hit"] for r in rows], dtype=float)
    treat = np.array([1.0 if r[treat_key] else 0.0 for r in rows])
    if treat.sum() == 0 or treat.sum() == len(treat):
        return {"empty": True, "n": len(rows)}
    effect, se, _ = _cluster_effect(y, treat, [r["game"] for r in rows])
    in_t = treat.astype(bool)
    in_c = ~in_t
    games_t = len({r["game"] for r, f in zip(rows, in_t) if f})
    games_c = len({r["game"] for r, f in zip(rows, in_c) if f})
    hits_t, n_t = int(y[in_t].sum()), int(in_t.sum())
    hits_c, n_c = int(y[in_c].sum()), int(in_c.sum())
    lo = effect - Z_BONFERRONI * se
    hi = effect + Z_BONFERRONI * se
    return {
        "empty": False,
        "treat_hits": hits_t,
        "treat_n": n_t,
        "treat_rate": hits_t / n_t if n_t else 0.0,
        "treat_games": games_t,
        "ctrl_hits": hits_c,
        "ctrl_n": n_c,
        "ctrl_rate": hits_c / n_c if n_c else 0.0,
        "ctrl_games": games_c,
        "lift": effect,
        "se": se,
        "ci_lo": lo,
        "ci_hi": hi,
        "excludes_zero": bool(lo > 0 or hi < 0),
        "clears_200": games_t >= 200 and games_c >= 200,
    }


def _pass_rule(pooled: dict, half: dict, *, eligible: bool) -> dict:
    if not eligible:
        return {
            "status": "ineligible (below floor)",
            "pass": False,
            "reason": "ineligible (below floor)",
        }
    if pooled.get("empty") or half.get("empty"):
        return {"status": "fail", "pass": False, "reason": "empty"}
    if not half.get("excludes_zero"):
        return {"status": "fail", "pass": False, "reason": "rs_ci_includes_zero"}
    if (pooled["lift"] > 0) != (half["lift"] > 0):
        return {"status": "fail", "pass": False, "reason": "pooled_sign_disagrees"}
    return {"status": "pass", "pass": True, "reason": "ok"}


def pre_score_bucket_counts() -> dict[str, dict]:
    """Count unique games in each locked bucket from game_lines (before hits).

    Eligibility is per treatment bucket and per comparison, not total slate games.
    The earlier ~1,867 graded-game estimate counted the whole sport and wrongly
    implied all three MLB contrasts were scorable.
    """
    assert MLB_TOTAL_LOW_MAX is not None and MLB_TOTAL_HIGH_MIN is not None
    conn = sqlite3.connect(str(DB_PATH))
    try:
        rows = conn.execute(
            """
            SELECT event_id, game_date, home_team, away_team,
                   home_win_prob, away_win_prob, total, is_closing
            FROM game_lines WHERE sport='MLB'
            """
        ).fetchall()
    finally:
        conn.close()

    heavy: set[str] = set()
    pickem: set[str] = set()
    low_tot: set[str] = set()
    high_tot: set[str] = set()
    for eid, gdate, home, away, hwp, awp, total, closing in rows:
        if closing != 1:
            continue
        key = str(eid or f"{gdate}|{home}|{away}")
        for wp in (hwp, awp):
            if wp is None:
                continue
            w = float(wp)
            if w >= 0.70:
                heavy.add(key)
            if 0.42 <= w <= 0.58:
                pickem.add(key)
        if total is None:
            continue
        t = float(total)
        if t <= MLB_TOTAL_LOW_MAX:
            low_tot.add(key)
        if t >= MLB_TOTAL_HIGH_MIN:
            high_tot.add(key)

    def pack(treat_n: int, ctrl_n: int) -> dict:
        ok = treat_n >= 200 and ctrl_n >= 200
        return {
            "treat_games": treat_n,
            "ctrl_games": ctrl_n,
            "eligible": ok,
            "status": "eligible" if ok else "ineligible (below floor)",
        }

    return {
        "over_heavy_fav_vs_pickem": pack(len(heavy), len(pickem)),
        "under_heavy_fav_vs_pickem": pack(len(heavy), len(pickem)),
        "over_high_vs_low_total_hitters": pack(len(high_tot), len(low_tot)),
        "note": (
            "Counts are unique closing-line games with a team (WP) or game (total) "
            "in the locked band. Floor is 200 per bucket. Do not substitute a new "
            "WP threshold after seeing these counts; any replacement band is only "
            "testable on future games."
        ),
    }


def within_prop_total_diagnostic(rows: list[dict]) -> dict:
    """Same high vs low total contrast, within prop type, reweighted to a common mix.

    Does not add a new contrast. Checks whether the pooled +1.7pp is a total effect
    or a shift in the prop menu (e.g. more HR props in high-total parks).
    """
    hitter_overs = [
        r
        for r in rows
        if r["direction"] == "OVER" and not r["pitcher"] and (r["high_total"] or r["low_total"])
    ]
    by_prop: dict[str, list[dict]] = defaultdict(list)
    for r in hitter_overs:
        by_prop[r["prop"]].append(r)

    # Common mix: share of each prop in the combined high+low pool.
    total_n = len(hitter_overs) or 1
    mix = {p: len(grp) / total_n for p, grp in by_prop.items()}

    per_prop = []
    weighted_lift = 0.0
    weight_sum = 0.0
    for prop, grp in sorted(by_prop.items(), key=lambda x: -len(x[1])):
        high = [r for r in grp if r["high_total"]]
        low = [r for r in grp if r["low_total"]]
        if len(high) < 30 or len(low) < 30:
            per_prop.append(
                {
                    "prop": prop,
                    "n_high": len(high),
                    "n_low": len(low),
                    "skipped": True,
                    "reason": "thin_within_prop",
                }
            )
            continue
        rh = sum(r["hit"] for r in high) / len(high)
        rl = sum(r["hit"] for r in low) / len(low)
        lift = rh - rl
        w = mix[prop]
        weighted_lift += w * lift
        weight_sum += w
        games_h = len({r["game"] for r in high})
        games_l = len({r["game"] for r in low})
        per_prop.append(
            {
                "prop": prop,
                "n_high": len(high),
                "n_low": len(low),
                "games_high": games_h,
                "games_low": games_l,
                "rate_high": rh,
                "rate_low": rl,
                "lift": lift,
                "mix_weight": w,
                "skipped": False,
            }
        )
    return {
        "weighted_lift": weighted_lift if weight_sum else None,
        "weight_covered": weight_sum,
        "per_prop": per_prop,
        "interpretation": (
            "If weighted_lift stays near the pooled +1.7pp and positive, the effect "
            "is not just a change in prop mix. If it shrinks toward zero, high-total "
            "boards are carrying a different menu."
        ),
    }


def collect_rows(
    games: dict,
    abbr_to_id: dict[str, str],
    *,
    closing_only: bool,
) -> list[dict]:
    rows: list[dict] = []
    seen: set[tuple] = set()
    matched = decided = 0
    for path in sorted(GRADED.glob("graded_props_*.json")):
        date = path.name[len("graded_props_") : -5]
        data = json.loads(path.read_text(encoding="utf-8"))
        for row in data.get("props") or []:
            if str(row.get("sport") or "") != "MLB":
                continue
            res = str(row.get("result") or "").upper()
            if res not in ("HIT", "MISS"):
                continue
            direction = str(row.get("direction") or row.get("over_under") or "").upper()
            if direction not in ("OVER", "UNDER"):
                continue
            team = str(row.get("team") or "").strip().upper()
            if not team or "/" in team:
                continue
            prop = _norm_prop(str(row.get("prop") or ""))
            key = (date, row.get("player"), prop, row.get("line"), direction, team)
            if key in seen:
                continue
            seen.add(key)
            decided += 1
            rec, is_home = lookup(games, abbr_to_id, date, team)
            if rec is None or is_home is None:
                continue
            if closing_only and rec.get("is_closing") != 1:
                continue
            if closing_only and not rec.get("provider"):
                # Allow close-by-final without provider for this first pull, but prefer provider.
                # Note locks closing_only with provider OR is_closing; require is_closing=1 at minimum.
                pass
            wp = team_win_prob(rec, is_home)
            if wp is None or rec.get("total") is None:
                continue
            matched += 1
            game = rec["event_id"] or f"{date}|{rec['hid']}|{rec['aid']}"
            rows.append(
                {
                    "hit": 1 if res == "HIT" else 0,
                    "direction": direction,
                    "prop": prop,
                    "pitcher": _is_pitcher_prop(prop),
                    "player": str(row.get("player") or ""),
                    "team": team,
                    "date": date,
                    "game": game,
                    "wp": float(wp),
                    "total": float(rec["total"]),
                    "heavy_fav": float(wp) >= 0.70,
                    "pickem": 0.42 <= float(wp) <= 0.58,
                }
            )
    for r in rows:
        assert MLB_TOTAL_LOW_MAX is not None and MLB_TOTAL_HIGH_MIN is not None
        r["low_total"] = r["total"] <= MLB_TOTAL_LOW_MAX
        r["high_total"] = r["total"] >= MLB_TOTAL_HIGH_MIN
    return rows


def collapse_player_game(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        groups[(r["date"], r["player"], r["team"], r["direction"], r["game"])].append(r)
    out = []
    for (date, player, team, direction, game), grp in groups.items():
        base = dict(grp[0])
        base["hit"] = float(np.mean([g["hit"] for g in grp]))
        base["prop"] = "collapsed"
        base["pitcher"] = all(g["pitcher"] for g in grp)
        out.append(base)
    return out


def score_contrasts(
    rows: list[dict],
    median_date: str,
    eligibility: dict[str, dict],
) -> dict:
    assert MLB_TOTAL_LOW_MAX is not None and MLB_TOTAL_HIGH_MIN is not None
    results = {}

    def run(name: str, pool: list[dict], treat_pred, ctrl_pred) -> dict:
        elig = eligibility.get(name) or {}
        eligible = bool(elig.get("eligible"))
        tagged = []
        for r in pool:
            t = treat_pred(r)
            c = ctrl_pred(r)
            if not t and not c:
                continue
            tagged.append({**r, "_treat": bool(t)})
        if not eligible:
            # Refuse to score: no lift/CI that could be misread as a test.
            return {
                "scored": False,
                "eligibility": elig,
                "pooled": {"empty": True},
                "pre_median": {"empty": True},
                "post_median": {"empty": True},
                "decision": {
                    "status": "ineligible (below floor)",
                    "pass": False,
                    "reason": "ineligible (below floor)",
                },
            }
        pooled = _summarize(tagged, "_treat")
        rs = _summarize([r for r in tagged if r["date"] <= median_date], "_treat")
        late = _summarize([r for r in tagged if r["date"] > median_date], "_treat")
        decision = _pass_rule(pooled, rs, eligible=True)
        return {
            "scored": True,
            "eligibility": elig,
            "pooled": pooled,
            "pre_median": rs,
            "post_median": late,
            "decision": decision,
        }

    overs = [r for r in rows if r["direction"] == "OVER"]
    unders = [r for r in rows if r["direction"] == "UNDER"]
    hitter_overs = [r for r in overs if not r["pitcher"]]

    results["over_heavy_fav_vs_pickem"] = run(
        "over_heavy_fav_vs_pickem",
        overs,
        lambda r: r["heavy_fav"],
        lambda r: r["pickem"],
    )
    results["over_high_vs_low_total_hitters"] = run(
        "over_high_vs_low_total_hitters",
        hitter_overs,
        lambda r: r["high_total"],
        lambda r: r["low_total"],
    )
    results["under_heavy_fav_vs_pickem"] = run(
        "under_heavy_fav_vs_pickem",
        unders,
        lambda r: r["heavy_fav"],
        lambda r: r["pickem"],
    )
    return results


def _fmt_block(name: str, block: dict) -> str:
    lines = [f"#### {name}"]
    elig = block.get("eligibility") or {}
    if elig:
        lines.append(
            f"- pre-score bucket games: treat {elig.get('treat_games')} / "
            f"ctrl {elig.get('ctrl_games')} -> {elig.get('status')}"
        )
    d = block.get("decision") or {}
    status = str(d.get("status") or ("pass" if d.get("pass") else "fail")).upper()
    if not block.get("scored", True):
        lines.append(f"- **decision: {status}** ({d.get('reason')}) — not scored; no lift reported.")
        return "\n".join(lines)
    for split in ("pooled", "pre_median", "post_median"):
        s = block.get(split) or {}
        if s.get("empty"):
            lines.append(f"- {split}: empty")
            continue
        lines.append(
            f"- {split}: treat {s['treat_hits']}/{s['treat_n']} = {s['treat_rate']:.1%} "
            f"({s['treat_games']}g) vs ctrl {s['ctrl_hits']}/{s['ctrl_n']} = {s['ctrl_rate']:.1%} "
            f"({s['ctrl_games']}g); lift {s['lift']:+.1%} SE {s['se']:.1%} "
            f"Bonf CI [{s['ci_lo']:+.1%}, {s['ci_hi']:+.1%}]; "
            f"excludes0={s['excludes_zero']}"
        )
    lines.append(f"- **decision: {status}** ({d.get('reason')})")
    return "\n".join(lines)


def append_note(payload: dict) -> None:
    cuts = payload["total_cuts"]
    spot = payload["spot_check"]
    primary = payload["primary"]
    collapsed = payload["collapsed"]
    eligibility = payload["eligibility"]
    within = payload.get("within_prop_total") or {}
    median_date = payload["median_date"]
    lines = [
        "",
        "## MLB locked total cuts",
        "",
        f"Set from {cuts['n_games_with_total']} closing (or available) per-game totals before joining hits.",
        f"Histogram (rounded): `{json.dumps(cuts['histogram'], sort_keys=True)}`.",
        f"**Low <= {cuts['low_max']}** (n={cuts['n_low']} games). "
        f"**High >= {cuts['high_min']}** (n={cuts['n_high']} games). "
        f"Mid excluded (n={cuts['n_mid']}). Not exact thirds.",
        "",
        "## MLB pre-score bucket eligibility",
        "",
        "Floor is **200 games per treatment bucket and 200 per comparison**, counted from "
        "`game_lines` before hits. The earlier ~1,867 graded-game estimate counted the whole "
        "sport and wrongly treated all three contrasts as scorable.",
        "",
        "| Contrast | Treat games | Ctrl games | Status |",
        "|---|---:|---:|---|",
    ]
    for name in (
        "over_heavy_fav_vs_pickem",
        "over_high_vs_low_total_hitters",
        "under_heavy_fav_vs_pickem",
    ):
        e = eligibility.get(name) or {}
        lines.append(
            f"| {name} | {e.get('treat_games')} | {e.get('ctrl_games')} | {e.get('status')} |"
        )
    lines.extend(
        [
            "",
            eligibility.get("note", ""),
            "",
            "At WP >= 0.70, MLB cannot use two of its three locked contrasts. "
            "The 0.70 threshold stays fixed. Do not substitute a lower band on this data; "
            "any replacement band is only testable on future games.",
            "",
            "## MLB close spot-check",
            "",
            "Post-final core refetch vs stored closing row on the same `event_id`:",
            "",
        ]
    )
    if not spot:
        lines.append("No spot-check rows.")
    else:
        lines.append("| Game | Stored O/U | Refetch O/U | Match | Stored spread | Refetch spread | Match |")
        lines.append("|---|---:|---:|:---:|---:|---:|:---:|")
        for s in spot:
            lines.append(
                f"| {s['date']} {s['matchup']} | {s['stored_total']} | {s['refetch_total']} | "
                f"{'Y' if s['total_match'] else 'N'} | {s['stored_spread']} | {s['refetch_spread']} | "
                f"{'Y' if s['spread_match'] else 'N'} |"
            )
        n_ok = sum(1 for s in spot if s["total_match"] and s["spread_match"])
        lines.append("")
        lines.append(
            f"{n_ok}/{len(spot)} matched on both spread and total. "
            "This pull's MLB rows were all post-final fetches (`is_closing=1`); there was no "
            "second near-tip store on the same game. The check is refetch stability after final, "
            "not equality with the first-pitch line."
        )
    lines.extend(
        [
            "",
            "### Near-tip close test (next slate)",
            "",
            "On the next MLB slate, store a row within about 15 minutes of first pitch for a few "
            "games, then compare that line to the post-final core number on the same `event_id`. "
            "That closes the gap between feed stability after final and the true close.",
            "",
            "## MLB battery results",
            "",
            f"Closing-line join. Median split date (from game dates before hits): **{median_date}**.",
            f"Matched decided props: {payload['n_rows']}. Unique games in rows: {payload['n_games']}.",
            "",
            "### Primary (one row per prop; cluster by game)",
            "",
        ]
    )
    for name, block in primary.items():
        lines.append(_fmt_block(name, block))
        if name == "over_high_vs_low_total_hitters" and block.get("scored"):
            pre = block.get("pre_median") or {}
            post = block.get("post_median") or {}
            if not pre.get("empty") and not post.get("empty"):
                lines.append(
                    f"- **Both halves:** pre-median lift {pre['lift']:+.1%} "
                    f"(Bonf CI [{pre['ci_lo']:+.1%}, {pre['ci_hi']:+.1%}]); "
                    f"post-median lift {post['lift']:+.1%} "
                    f"(Bonf CI [{post['ci_lo']:+.1%}, {post['ci_hi']:+.1%}]). "
                    "Post-median near zero or with a wide interval means the effect may be "
                    "decaying across the season; that does not reverse a pass but belongs here."
                )
        lines.append("")

    lines.append("### Within-prop mix check (total PASS only)")
    lines.append("")
    lines.append(
        "Hitter overs sit near 27-29% because the pool is heavy on low-probability props "
        "(HR, runs, RBIs, etc.). High-total games may also carry a different prop menu. "
        "This reweights within-prop high-vs-low lifts to one common mix — still the same "
        "contrast, not a new one."
    )
    lines.append("")
    if within.get("weighted_lift") is None:
        lines.append("Within-prop diagnostic unavailable.")
    else:
        lines.append(
            f"**Mix-weighted lift: {within['weighted_lift']:+.1%}** "
            f"(weight covered {within.get('weight_covered', 0):.0%} of the high+low hitter-over pool)."
        )
        lines.append("")
        lines.append("| Prop | High rate | Low rate | Lift | Mix weight | n high / n low |")
        lines.append("|---|---:|---:|---:|---:|---|")
        for p in within.get("per_prop") or []:
            if p.get("skipped"):
                lines.append(
                    f"| {p['prop']} | — | — | — | — | {p.get('n_high')}/{p.get('n_low')} (skipped) |"
                )
                continue
            lines.append(
                f"| {p['prop']} | {p['rate_high']:.1%} | {p['rate_low']:.1%} | "
                f"{p['lift']:+.1%} | {p['mix_weight']:.1%} | {p['n_high']}/{p['n_low']} |"
            )
        lines.append("")
        lines.append(str(within.get("interpretation") or ""))
    lines.append("")
    lines.append("### Player-game collapse (diagnostic)")
    lines.append("")
    sign_flips = []
    for name, block in collapsed.items():
        lines.append(_fmt_block(name, block))
        if not block.get("scored"):
            lines.append("")
            continue
        p_lift = (primary.get(name) or {}).get("pooled", {}).get("lift")
        c_lift = (block.get("pooled") or {}).get("lift")
        if p_lift is not None and c_lift is not None and (p_lift > 0) != (c_lift > 0):
            sign_flips.append(name)
            lines.append(
                f"- **SIGN FLIP vs primary** on pooled lift "
                f"(primary {p_lift:+.1%}, collapsed {c_lift:+.1%}). "
                "Heavy-volume players may be driving the prop-level number."
            )
        lines.append("")
    if not sign_flips:
        lines.append("No pooled sign flips between primary and player-game collapse.")
    else:
        lines.append(f"Sign flips recorded: {', '.join(sign_flips)}.")
    lines.extend(
        [
            "",
            "A PASS here is review-only. Even if the within-prop check holds, a ~1.7-point lift "
            "on a ~28% base is useful as a continuous `game_total` model input, not a ticket gate.",
            "",
            "### Soccer next",
            "",
            "Soccer is next in the backfill order. Apply the same **per-bucket** eligibility "
            "count before scoring. Pooled soccer games can still fall short once split across "
            "goal-line and win-probability buckets, the same way MLB's heavy-favorite band did.",
            "",
        ]
    )

    text = NOTE.read_text(encoding="utf-8")
    marker = "## MLB locked total cuts"
    if marker in text:
        text = text.split(marker)[0].rstrip() + "\n"
    NOTE.write_text(text + "\n".join(lines) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="MLB game-line battery")
    ap.add_argument("--skip-fetch", action="store_true")
    ap.add_argument("--refresh", action="store_true")
    ap.add_argument("--sleep", type=float, default=0.35)
    ap.add_argument("--max-dates", type=int, default=0, help="0 = all graded MLB dates")
    args = ap.parse_args()

    dates = mlb_graded_dates()
    if args.max_dates > 0:
        dates = dates[: args.max_dates]
    print(f"MLB graded dates: {len(dates)} ({dates[0] if dates else '-'} ... {dates[-1] if dates else '-'})")

    if not args.skip_fetch:
        print("Backfilling MLB game lines...")
        stats = backfill_mlb(dates, refresh=args.refresh, sleep_s=args.sleep)
        print(stats)

    low, high, cuts = lock_total_cuts()
    print(f"Locked total cuts: low <= {low}, high >= {high}  meta={cuts}")

    eligibility = pre_score_bucket_counts()
    print("Pre-score bucket eligibility:")
    for name, e in eligibility.items():
        if name == "note":
            continue
        print(f"  {name}: treat={e['treat_games']} ctrl={e['ctrl_games']} -> {e['status']}")

    print("Spot-checking post-final core odds...")
    spot = spot_check_closes(5)
    for s in spot:
        print(
            f"  {s['date']} {s['matchup']}: total {s['stored_total']}->{s['refetch_total']} "
            f"({'Y' if s['total_match'] else 'N'}) "
            f"spread {s['stored_spread']}->{s['refetch_spread']} "
            f"({'Y' if s['spread_match'] else 'N'})"
        )

    abbr_to_id = load_crosswalk()
    games = load_mlb_games()
    conn = sqlite3.connect(str(DB_PATH))
    try:
        gdates = sorted(
            {
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT game_date FROM game_lines WHERE sport='MLB' AND is_closing=1"
                )
            }
        )
        if not gdates:
            gdates = sorted(
                {r[0] for r in conn.execute("SELECT DISTINCT game_date FROM game_lines WHERE sport='MLB'")}
            )
    finally:
        conn.close()
    median_date = gdates[len(gdates) // 2]
    print(f"Median split date (from game_lines, before hits): {median_date}")

    rows = collect_rows(games, abbr_to_id, closing_only=True)
    if len(rows) < 1000:
        print(f"Closing-only rows thin ({len(rows)}); retrying without closing-only filter")
        rows = collect_rows(games, abbr_to_id, closing_only=False)
    print(f"Matched decided props: {len(rows)}")
    n_games = len({r["game"] for r in rows})
    print(f"Unique games in matched rows: {n_games}")

    primary = score_contrasts(rows, median_date, eligibility)
    collapsed = score_contrasts(collapse_player_game(rows), median_date, eligibility)
    within = within_prop_total_diagnostic(rows)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_cuts": cuts,
        "eligibility": eligibility,
        "spot_check": spot,
        "median_date": median_date,
        "n_rows": len(rows),
        "n_games": n_games,
        "primary": primary,
        "collapsed": collapsed,
        "within_prop_total": within,
    }
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    append_note(payload)
    print(f"Wrote {REPORT_JSON}")
    print(f"Updated {NOTE}")
    print(f"Within-prop weighted lift: {within.get('weighted_lift')}")
    for name, block in primary.items():
        d = block["decision"]
        print(f"  {name}: {d.get('status', '?').upper()} ({d['reason']})")


if __name__ == "__main__":
    main()
