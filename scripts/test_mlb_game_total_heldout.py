#!/usr/bin/env python3
"""Held-out test: does continuous MLB game_total earn a model slot?

Per data/reports/game_line_environment.md (Model, not a gate):
1. Train through a cutoff; score later games.
2. Compare log loss / Brier with and without game_total.
3. One game-clustered interval on the difference.

Does not write gates. Appends a short section to the environment note.
"""
from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
from sklearn.linear_model import LogisticRegression

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
sys.path.insert(0, str(_REPO / "scripts"))

from fetch_game_lines import DB_PATH  # noqa: E402
from score_mlb_game_line_battery import (  # noqa: E402
    GRADED,
    _norm_prop,
    load_crosswalk,
    load_mlb_games,
    lock_total_cuts,
    lookup,
    team_win_prob,
)

NOTE = _REPO / "data" / "reports" / "game_line_environment.md"
REPORT_JSON = _REPO / "data" / "reports" / "mlb_game_total_heldout.json"


def collect_mlb_model_rows(*, closing_only: bool = True) -> list[dict]:
    lock_total_cuts()
    from score_mlb_game_line_battery import MLB_TOTAL_HIGH_MIN, MLB_TOTAL_LOW_MAX

    assert MLB_TOTAL_LOW_MAX is not None and MLB_TOTAL_HIGH_MIN is not None
    games = load_mlb_games()
    abbr_to_id = load_crosswalk()
    rows: list[dict] = []
    seen: set[tuple] = set()
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
            try:
                mlp = float(row.get("ml_prob"))
            except (TypeError, ValueError):
                continue
            if not (0.0 < mlp < 1.0):
                continue
            prop = _norm_prop(str(row.get("prop") or ""))
            key = (date, row.get("player"), prop, row.get("line"), direction, team)
            if key in seen:
                continue
            seen.add(key)
            rec, is_home = lookup(games, abbr_to_id, date, team)
            if rec is None or is_home is None:
                continue
            if closing_only and rec.get("is_closing") != 1:
                continue
            if rec.get("total") is None:
                continue
            wp = team_win_prob(rec, is_home)
            if wp is None:
                continue
            game = rec["event_id"] or f"{date}|{rec['hid']}|{rec['aid']}"
            rows.append(
                {
                    "hit": 1 if res == "HIT" else 0,
                    "direction": direction,
                    "prop": prop,
                    "player": str(row.get("player") or ""),
                    "team": team,
                    "date": date,
                    "game": game,
                    "ml_prob": mlp,
                    "total": float(rec["total"]),
                    "wp": float(wp),
                }
            )
    return rows


def _fit_predict(X_tr: np.ndarray, y_tr: np.ndarray, X_te: np.ndarray) -> np.ndarray:
    lr = LogisticRegression(C=1.0, max_iter=2000, random_state=42, solver="lbfgs")
    lr.fit(X_tr, y_tr)
    return lr.predict_proba(X_te)[:, 1]


def _logloss(y: np.ndarray, p: np.ndarray) -> float:
    p = np.clip(p, 1e-6, 1.0 - 1e-6)
    return float(-np.mean(y * np.log(p) + (1.0 - y) * np.log(1.0 - p)))


def _brier(y: np.ndarray, p: np.ndarray) -> float:
    return float(np.mean((p - y) ** 2))


def _per_game_logloss(
    y: np.ndarray, p0: np.ndarray, p1: np.ndarray, games: list[str]
) -> tuple[np.ndarray, np.ndarray]:
    buckets: dict[str, list[int]] = defaultdict(list)
    for i, g in enumerate(games):
        buckets[g].append(i)
    keys = sorted(buckets.keys())
    ll0 = np.zeros(len(keys))
    ll1 = np.zeros(len(keys))
    for j, g in enumerate(keys):
        idxs = buckets[g]
        yy = y[idxs]
        ll0[j] = _logloss(yy, p0[idxs])
        ll1[j] = _logloss(yy, p1[idxs])
    return ll0, ll1


def _clustered_diff_ci(
    per_game_base: np.ndarray, per_game_full: np.ndarray, *, n_boot: int = 2000
) -> dict:
    """Delta = mean(full) - mean(base); negative = full is better (lower log loss)."""
    delta_games = per_game_full - per_game_base
    n = len(delta_games)
    point = float(np.mean(delta_games))
    rng = np.random.default_rng(42)
    boots = np.empty(n_boot)
    for b in range(n_boot):
        sample = rng.choice(delta_games, size=n, replace=True)
        boots[b] = float(np.mean(sample))
    lo, hi = np.quantile(boots, [0.025, 0.975])
    se = float(np.std(delta_games, ddof=1) / math.sqrt(n)) if n > 1 else float("nan")
    return {
        "delta_logloss": point,
        "se": se,
        "ci95": [float(lo), float(hi)],
        "excludes_zero": bool(lo > 0 or hi < 0),
        "n_games": n,
        "improves": bool(point < 0 and hi < 0),
    }


def run_heldout(rows: list[dict], cutoff: str) -> dict:
    train = [r for r in rows if r["date"] <= cutoff]
    test = [r for r in rows if r["date"] > cutoff]
    if len(train) < 500 or len(test) < 200:
        raise SystemExit(f"Thin split train={len(train)} test={len(test)} cutoff={cutoff}")

    y_tr = np.array([r["hit"] for r in train], dtype=float)
    y_te = np.array([r["hit"] for r in test], dtype=float)
    ml_tr = np.array([r["ml_prob"] for r in train], dtype=float).reshape(-1, 1)
    ml_te = np.array([r["ml_prob"] for r in test], dtype=float).reshape(-1, 1)
    tot_tr = np.array([r["total"] for r in train], dtype=float)
    tot_te = np.array([r["total"] for r in test], dtype=float)
    mu, sig = float(tot_tr.mean()), float(tot_tr.std() or 1.0)
    tot_tr_z = ((tot_tr - mu) / sig).reshape(-1, 1)
    tot_te_z = ((tot_te - mu) / sig).reshape(-1, 1)

    p0 = _fit_predict(ml_tr, y_tr, ml_te)
    p1 = _fit_predict(np.hstack([ml_tr, tot_tr_z]), y_tr, np.hstack([ml_te, tot_te_z]))

    games = [r["game"] for r in test]
    ll0_g, ll1_g = _per_game_logloss(y_te, p0, p1, games)
    br0 = _brier(y_te, p0)
    br1 = _brier(y_te, p1)
    ci = _clustered_diff_ci(ll0_g, ll1_g)

    return {
        "cutoff": cutoff,
        "n_train": len(train),
        "n_test": len(test),
        "n_test_games": len(set(games)),
        "base": {"features": ["ml_prob"], "logloss": _logloss(y_te, p0), "brier": br0},
        "with_total": {
            "features": ["ml_prob", "game_total_z"],
            "logloss": _logloss(y_te, p1),
            "brier": br1,
        },
        "delta_logloss": ci,
        "delta_brier": br1 - br0,
        "decision": (
            "include in continuous model review"
            if ci["improves"]
            else "exclude from model — held-out log-loss improvement does not clear a "
            "game-clustered 95% interval"
        ),
    }


def append_note(result: dict) -> None:
    d = result["delta_logloss"]
    lines = [
        "",
        "## MLB game_total held-out model test",
        "",
        "Continuous `game_total` must earn a model slot via held-out games (train through "
        "a cutoff; score later dates) log loss / Brier with a **game-clustered** interval "
        "on the difference. Base model: logistic on PrizePicks `ml_prob`. Comparison: "
        "same + standardized closing `game_total`.",
        "",
        f"- Cutoff (train through): **{result['cutoff']}**",
        f"- Train props: {result['n_train']}. Test props: {result['n_test']} "
        f"({result['n_test_games']} games).",
        f"- Base log loss: {result['base']['logloss']:.4f}. "
        f"With total: {result['with_total']['logloss']:.4f}.",
        f"- Base Brier: {result['base']['brier']:.4f}. "
        f"With total: {result['with_total']['brier']:.4f} "
        f"(delta {result['delta_brier']:+.4f}).",
        f"- Delta log loss (with − base; negative = better): "
        f"{d['delta_logloss']:+.4f} "
        f"(game-clustered 95% CI [{d['ci95'][0]:+.4f}, {d['ci95'][1]:+.4f}]; "
        f"n_games={d['n_games']}; excludes0={d['excludes_zero']}).",
        f"- **Decision: {result['decision']}**",
        "",
        "Backfill order after this: **WNBA / CFB** once per-bucket counts clear. "
        "Near-tip close check remains on the next MLB slate.",
        "",
    ]
    text = NOTE.read_text(encoding="utf-8")
    marker = "## MLB game_total held-out model test"
    if marker in text:
        text = text.split(marker)[0].rstrip() + "\n"
    NOTE.write_text(text.rstrip() + "\n" + "\n".join(lines), encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="MLB game_total held-out model test")
    ap.add_argument(
        "--cutoff",
        default="",
        help="YYYY-MM-DD train-through (default: median closing game_date)",
    )
    args = ap.parse_args()

    print("Collecting MLB rows with ml_prob + closing game_total...")
    rows = collect_mlb_model_rows(closing_only=True)
    if len(rows) < 1000:
        print(f"Closing-only thin ({len(rows)}); including non-closing")
        rows = collect_mlb_model_rows(closing_only=False)
    print(f"Matched rows: {len(rows)}")

    conn = sqlite3.connect(str(DB_PATH))
    try:
        gdates = sorted(
            {
                r[0]
                for r in conn.execute(
                    "SELECT DISTINCT game_date FROM game_lines "
                    "WHERE sport='MLB' AND is_closing=1"
                )
            }
        )
    finally:
        conn.close()
    cutoff = args.cutoff.strip() or gdates[len(gdates) // 2]
    print(f"Cutoff: {cutoff}")

    result = run_heldout(rows, cutoff)
    result["generated_at"] = datetime.now(timezone.utc).isoformat()
    REPORT_JSON.parent.mkdir(parents=True, exist_ok=True)
    REPORT_JSON.write_text(json.dumps(result, indent=2), encoding="utf-8")
    append_note(result)
    print(json.dumps({k: result[k] for k in result if k != "generated_at"}, indent=2))
    print(f"Wrote {REPORT_JSON}")
    print(f"Updated {NOTE}")


if __name__ == "__main__":
    main()
