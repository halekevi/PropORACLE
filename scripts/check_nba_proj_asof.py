"""Isolation test: February proj_edge must not change when later boxes are dropped.

  py -3.14 scripts/check_nba_proj_asof.py --date 2026-02-19
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from score_nba_stat_projections import (  # noqa: E402
    _attach_gate,
    _load_graded,
    attach_nba_projections,
)
from utils.expanding_marginals import walk_forward_lookups_by_date  # noqa: E402
from utils.nba_player_projection import DEFAULT_DB, NbaBoxIndex  # noqa: E402

COLS = ("proj_mu", "proj_edge", "proj_minutes", "proj_hold", "base_rate", "proj_p_play")


def _score(day: pd.DataFrame, index: NbaBoxIndex, lookup: dict, as_of: str) -> pd.DataFrame:
    out = attach_nba_projections(day, as_of=as_of, index=index, lookup=lookup)
    out["_gate"] = _attach_gate(out, index, as_of)
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default="2026-02-19")
    ap.add_argument("--db", default=str(DEFAULT_DB))
    ap.add_argument("--templates", default=str(_REPO / "ui_runner" / "templates"))
    args = ap.parse_args()
    as_of = str(args.date)[:10]

    graded = _load_graded(Path(args.templates))
    sport = graded.get("sport", pd.Series("", index=graded.index)).astype(str).str.strip().str.upper()
    nba = graded.loc[sport.eq("NBA")].copy()
    day = nba.loc[nba["file_date"].astype(str).str[:10].eq(as_of)].copy()
    if day.empty:
        print(f"[asof] no NBA graded rows on {as_of}")
        return 1

    full = NbaBoxIndex(args.db)
    sliced = full.restrict_before(as_of)
    n_full = len(full.df)
    n_sliced = len(sliced.df)
    n_after = n_full - n_sliced
    print(f"[asof] {as_of}  graded={len(day)}  boxes_full={n_full}  boxes_before={n_sliced}  dropped_later={n_after}")

    lookups = walk_forward_lookups_by_date(nba)
    lookup = lookups.get(as_of, {})
    a = _score(day, full, lookup, as_of)
    b = _score(day, sliced, lookup, as_of)

    rows = []
    n_diff = 0
    max_abs = 0.0
    for col in COLS:
        sa = pd.to_numeric(a[col], errors="coerce")
        sb = pd.to_numeric(b[col], errors="coerce")
        both = sa.notna() & sb.notna()
        delta = (sa - sb).abs()
        nd = int((both & (delta > 1e-9)).sum())
        mx = float(delta.loc[both].max()) if both.any() else 0.0
        n_diff += nd
        max_abs = max(max_abs, mx)
        rows.append({"col": col, "n_diff": nd, "max_abs": round(mx, 8), "n_compared": int(both.sum())})
        print(f"  {col:16} n_diff={nd:5}  max_abs={mx:.8f}  compared={int(both.sum())}")

    gate_a = a["_gate"].astype(bool)
    gate_b = b["_gate"].astype(bool)
    gate_flip = int((gate_a != gate_b).sum())
    print(f"  {'_gate':16} flips={gate_flip}")

    leak = n_diff > 0 or gate_flip > 0
    verdict = "LOOKAHEAD" if leak else "CLEAN"
    print(f"[asof] {verdict}  later boxes {'changed' if leak else 'did not change'} {as_of} projections")

    dest = _REPO / "data" / "reports" / "ml_discover_gates" / "nba_proj_asof_isolation.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(
        json.dumps(
            {
                "date": as_of,
                "verdict": verdict,
                "graded": len(day),
                "boxes_full": n_full,
                "boxes_before": n_sliced,
                "dropped_later": n_after,
                "cols": rows,
                "gate_flips": gate_flip,
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"wrote {dest}")
    return 1 if leak else 0


if __name__ == "__main__":
    raise SystemExit(main())
