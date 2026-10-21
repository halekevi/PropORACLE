"""Re-run May/June backtest using cached matrix (skip cell rebuild)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from scripts.build_leg_correlation_matrix import (  # noqa: E402
    backtest,
    build_lookup,
    load_graded_props,
    load_tickets,
)

ROOT = Path(__file__).resolve().parent
cells = json.loads((ROOT / "data/cache/leg_correlation_matrix.json").read_text(encoding="utf-8"))
lookup = build_lookup(cells)
print("[cache] loaded matrix cells")

df = load_graded_props(str(ROOT / "ui_runner/templates"))
tickets = load_tickets(
    str(ROOT / "ui_runner/data/combined_slate_tickets_2026-0[56]*.json"),
    graded_df=df,
)
print(f"[tickets] {len(tickets)}")
backtest(tickets, lookup, str(ROOT / "data/reports/corr_backtest_may_june.md"))
