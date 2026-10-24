"""Per-category board vs fetch reconciliation for PrizePicks step1.

Prints:
  category | on_board | fetched | mapped | graded_cell | kept | dropped (reason)

Fails loudly when a required category is on the board for the slate date
but zero rows were kept (coverage bug — Tatum FGA class of miss).
"""
from __future__ import annotations

import csv
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

import pandas as pd

from utils.pp_board_prop_map import (
    PropCategory,
    categories_for_sport,
    lookup_category,
    required_keys,
)
from utils.prop_norm import fold


@dataclass
class CategoryRow:
    category: str
    card_label: str
    on_board: int
    fetched: int
    mapped: bool
    graded_cell: str
    kept: int
    dropped: int
    drop_reason: str = ""


def _prop_series(df: pd.DataFrame | None) -> list[str]:
    if df is None or getattr(df, "empty", True):
        return []
    for col in ("prop_type", "Prop", "prop", "stat_type"):
        if col in df.columns:
            return [str(x).strip() for x in df[col].tolist() if str(x).strip()]
    return []


def _count_by_key(
    labels: Iterable[str], sport: str
) -> tuple[Counter[str], Counter[str], list[tuple[str, int]]]:
    """Returns (by_key, by_label, unmapped[(label, n)])."""
    by_label: Counter[str] = Counter(labels)
    by_key: Counter[str] = Counter()
    unmapped: list[tuple[str, int]] = []
    for label, n in by_label.items():
        cat = lookup_category(sport, label)
        if cat is None:
            unmapped.append((label, n))
            by_key[f"UNMAPPED:{label}"] += n
        else:
            by_key[cat.key] += n
    return by_key, by_label, unmapped


def write_unmapped_labels(
    path: Path,
    unmapped: Sequence[tuple[str, int]],
    *,
    sport: str,
) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["sport", "card_label", "n_props_lost"])
        for label, n in sorted(unmapped, key=lambda x: (-x[1], x[0])):
            w.writerow([sport, label, n])


def reconcile_categories(
    *,
    sport: str,
    board_df: pd.DataFrame | None,
    fetched_df: pd.DataFrame | None,
    kept_df: pd.DataFrame | None = None,
    dropped: Mapping[str, int] | None = None,
    graded_cells: set[str] | frozenset[str] | None = None,
) -> list[CategoryRow]:
    """
    board_df  — raw projections for the slate date (pre policy drops)
    fetched_df — same as board when the API dump is the board source
    kept_df   — rows written after fantasy/all-star drops (optional)
    dropped   — reason → count (e.g. fantasy_filter=93)
    """
    sport_u = str(sport or "").strip().upper()
    board_labels = _prop_series(board_df)
    fetched_labels = _prop_series(fetched_df if fetched_df is not None else board_df)
    kept_labels = _prop_series(kept_df if kept_df is not None else fetched_df)

    board_by_key, _, unmapped_board = _count_by_key(board_labels, sport_u)
    fetched_by_key, _, unmapped_fetched = _count_by_key(fetched_labels, sport_u)
    kept_by_key, _, _ = _count_by_key(kept_labels, sport_u)

    # Union of mapped categories + any unmapped labels seen
    cats = {c.key: c for c in categories_for_sport(sport_u)}
    keys = set(cats) | {k for k in board_by_key if not k.startswith("UNMAPPED:")}
    keys |= {k for k in fetched_by_key if not k.startswith("UNMAPPED:")}

    drop_map = {str(k): int(v) for k, v in (dropped or {}).items()}
    graded = graded_cells or set()

    rows: list[CategoryRow] = []
    for key in sorted(keys):
        cat = cats.get(key)
        label = cat.card_label if cat else key
        on_b = int(board_by_key.get(key, 0))
        fet = int(fetched_by_key.get(key, 0))
        kep = int(kept_by_key.get(key, 0))
        drop_n = max(0, fet - kep)
        reasons = []
        if drop_n and key in {"fantasy"} and drop_map.get("fantasy_filter"):
            reasons.append("fantasy_filter")
        if cat and cat.kind == "special":
            reasons.append("special_ou_excluded")
        if cat and cat.kind == "combo_player":
            reasons.append("combo_player_excluded")
        if drop_n and not reasons:
            reasons.append("policy_drop")
        cell = cat.hr_cell if cat else ""
        rows.append(
            CategoryRow(
                category=key,
                card_label=label,
                on_board=on_b,
                fetched=fet,
                mapped=cat is not None,
                graded_cell=cell if (not graded or cell in graded or not cell) else cell,
                kept=kep,
                dropped=drop_n,
                drop_reason=",".join(reasons),
            )
        )

    # Unmapped labels as their own rows
    seen_unmapped = {lab for lab, _ in unmapped_board} | {lab for lab, _ in unmapped_fetched}
    for label in sorted(seen_unmapped):
        on_b = sum(1 for x in board_labels if x == label)
        fet = sum(1 for x in fetched_labels if x == label)
        kep = sum(1 for x in kept_labels if x == label)
        rows.append(
            CategoryRow(
                category=f"UNMAPPED:{label}",
                card_label=label,
                on_board=on_b,
                fetched=fet,
                mapped=False,
                graded_cell="",
                kept=kep,
                dropped=max(0, fet - kep),
                drop_reason="unmapped_label",
            )
        )
    return rows


def print_reconciliation(rows: Sequence[CategoryRow], *, sport: str) -> None:
    print(
        f"\n[category-reconcile {sport}] "
        "category | on_board | fetched | mapped | graded_cell | kept | dropped (reason)"
    )
    for r in rows:
        mapped = "Y" if r.mapped else "N"
        cell = r.graded_cell or "—"
        # Fantasy stays kept with ticketable=false — still print the flag.
        show_reason = r.drop_reason and (r.dropped or r.drop_reason == "ticketable=false")
        reason = f" ({r.drop_reason})" if show_reason else ""
        print(
            f"  {r.category:22} | {r.on_board:5} | {r.fetched:5} | {mapped:1} | "
            f"{cell:16} | {r.kept:5} | {r.dropped}{reason}"
        )


_THICK_KEYS: dict[str, tuple[str, ...]] = {
    "NBA": ("points",),
    "NBAP": ("points",),
    "WNBA": ("points",),
    "NHL": ("shots_on_goal", "goals", "points"),
    "NFL": ("pass_yds", "rush_yds", "rec_yds"),
    "NFLP": ("pass_yds", "rush_yds", "rec_yds"),
    "MLB": ("hits", "total_bases", "pitcher_ks"),
    "SOCCER": ("shots", "shots_on_target"),
    "TENNIS": ("total_games", "games_won", "aces"),
}


def _thick_fetched(rows: Sequence[CategoryRow], sport: str) -> int:
    keys = _THICK_KEYS.get(str(sport or "").strip().upper(), ("points",))
    return sum(r.fetched for r in rows if r.category in keys)


def missing_required_fetched(
    rows: Sequence[CategoryRow],
    *,
    sport: str,
    min_points: int = 20,
) -> list[str]:
    """Required categories on a thick board with zero fetched rows."""
    if _thick_fetched(rows, sport) < int(min_points):
        return []
    need = required_keys(sport)
    miss = []
    for r in rows:
        if r.category in need and r.on_board > 0 and r.fetched == 0:
            miss.append(r.category)
        # Required key never appeared while the slate is thick (FGA-class miss).
        if r.category in need and r.on_board == 0 and r.fetched == 0:
            miss.append(r.category)
    out: list[str] = []
    seen: set[str] = set()
    for k in miss:
        if k not in seen:
            seen.add(k)
            out.append(k)
    return [k for k in out if k in need]


def assert_board_coverage(
    rows: Sequence[CategoryRow],
    *,
    sport: str,
    min_points: int = 20,
) -> None:
    miss = missing_required_fetched(rows, sport=sport, min_points=min_points)
    if miss:
        raise RuntimeError(
            f"{sport} step1 coverage miss — on board / required but fetched=0: "
            + ", ".join(miss)
            + ". Partner dump must include every board tab (FG Attempted, etc.)."
        )


def unmapped_from_rows(rows: Sequence[CategoryRow]) -> list[tuple[str, int]]:
    return [
        (r.card_label, r.on_board or r.fetched)
        for r in rows
        if not r.mapped and (r.on_board or r.fetched)
    ]
