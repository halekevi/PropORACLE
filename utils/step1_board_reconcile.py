"""Shared step1 board reconcile + empty-slate status (fail-loud).

Every sport's step1 should call ``run_step1_reconcile`` before writing CSV so
missing required tabs exit 2, fantasy stays listed with ticketable=false, and
empty files print ``no_slate`` or ``fetch_failed`` instead of going silent.
"""
from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import pandas as pd

from utils.fantasy_prop_filter import drop_fantasy_props
from utils.pp_board_prop_map import categories_for_sport
from utils.pp_category_reconcile import (
    assert_board_coverage,
    print_reconciliation,
    reconcile_categories,
    unmapped_from_rows,
    write_unmapped_labels,
)

# Minimum rows of the sport's "anchor" market before treating the board as thick
# enough to require every required tab (FGA-class miss detection).
_THICK_ANCHOR: dict[str, tuple[str, int]] = {
    "NBA": ("points", 20),
    "NBAP": ("points", 20),
    "WNBA": ("points", 20),
    "NHL": ("shots_on_goal", 20),
    "NFL": ("pass_yds", 15),
    "NFLP": ("pass_yds", 15),
    "MLB": ("hits", 20),
    "SOCCER": ("shots", 20),
    "TENNIS": ("total_games", 10),
}


def classify_empty_step1(
    *,
    fetched_n: int,
    survived_n: int = 0,
    fetch_failed: bool = False,
) -> str:
    """Return ``fetch_failed`` or ``no_slate`` for an empty step1 write."""
    if fetch_failed:
        return "fetch_failed"
    if int(fetched_n or 0) <= 0 and int(survived_n or 0) <= 0:
        # Successful HTTP with zero projections → off-season / no games posted.
        return "no_slate"
    if int(survived_n or 0) <= 0:
        return "no_slate"
    return "no_slate"


def print_empty_reconcile(
    sport: str,
    *,
    status: str,
    fetched_n: int = 0,
    survived_n: int = 0,
) -> None:
    """Print per-category zeros so an empty file is never silent."""
    sport_u = str(sport or "").strip().upper()
    print(
        f"\n[category-reconcile {sport_u}] status={status} "
        f"on_board=0 fetched={int(fetched_n)} survived={int(survived_n)}"
    )
    print(
        "category | on_board | fetched | mapped | graded_cell | kept | dropped (reason)"
    )
    for cat in categories_for_sport(sport_u):
        mapped = "Y"
        cell = cat.hr_cell or "—"
        print(
            f"  {cat.key:22} | {0:5} | {0:5} | {mapped:1} | "
            f"{cell:16} | {0:5} | 0"
        )
    if not categories_for_sport(sport_u):
        print("  (no category map for sport — add to utils/pp_board_prop_map.py)")


def _anchor_min(sport: str) -> int:
    sport_u = str(sport or "").strip().upper()
    _, n = _THICK_ANCHOR.get(sport_u, ("points", 20))
    return int(n)


def run_step1_reconcile(
    *,
    sport: str,
    df: pd.DataFrame,
    out_dir: Path | None = None,
    fetch_failed: bool = False,
    fetched_n: int | None = None,
    mark_fantasy: bool = True,
) -> tuple[pd.DataFrame, list[Any]]:
    """
    Mark fantasy ticketable=false, print per-category table, exit-path via
    RuntimeError on coverage miss.

    Returns (df_with_ticketable, reconcile_rows). Raises RuntimeError on miss
    (caller should ``sys.exit(2)``).
    """
    sport_u = str(sport or "").strip().upper()
    board = df.copy() if df is not None else pd.DataFrame()
    n_fantasy = 0
    if mark_fantasy and board is not None and not board.empty:
        board, n_fantasy = drop_fantasy_props(board)
        if n_fantasy:
            print(
                f"  Flagged {n_fantasy} fantasy prop row(s) "
                f"ticketable=false (kept on board)"
            )

    fetched = int(fetched_n if fetched_n is not None else len(board))
    if board is None or board.empty:
        status = classify_empty_step1(
            fetched_n=fetched,
            survived_n=0,
            fetch_failed=fetch_failed,
        )
        print_empty_reconcile(
            sport_u, status=status, fetched_n=fetched, survived_n=0
        )
        return board if board is not None else pd.DataFrame(), []

    recon = reconcile_categories(
        sport=sport_u,
        board_df=board,
        fetched_df=board,
        kept_df=board,
        dropped={"fantasy_filter": int(n_fantasy or 0)},
    )
    # Fantasy kept: annotate drop_reason as ticketable=false when present.
    for r in recon:
        if r.category == "fantasy" and r.kept > 0:
            r.drop_reason = "ticketable=false"
            r.dropped = 0
    print_reconciliation(recon, sport=sport_u)

    unmapped = unmapped_from_rows(recon)
    if unmapped and out_dir is not None:
        lost = sum(n for _, n in unmapped)
        print(
            f"  [WARN] {sport_u} unmapped card labels: {len(unmapped)} "
            f"({lost} props). See unmapped_labels.csv"
        )
        um_path = Path(out_dir) / "unmapped_labels.csv"
        write_unmapped_labels(um_path, unmapped, sport=sport_u)
        print(f"  Unmapped → {um_path}")

    assert_board_coverage(recon, sport=sport_u, min_points=_anchor_min(sport_u))
    return board, recon
