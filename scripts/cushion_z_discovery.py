#!/usr/bin/env python3
"""Hit rate by play-side cushion, after L5=5, for three books.

Goblin OVER, Standard OVER, and Standard UNDER are separate cells.
Unders use (line − L10 average) / L10 std. Overs and Goblins use
(L10 average − line) / L10 std. Tercile edges are fit on the early
dates and scored on the later dates. Nothing here is wired into a gate.

  py -3.14 scripts/cushion_z_discovery.py
"""
from __future__ import annotations

import csv
import json
import sys
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

import prop_hit_tiers as T  # noqa: E402
from utils.comparison_tracks import wilson_band  # noqa: E402
from utils.cushion_z import (  # noqa: E402
    assign_tercile,
    clear_count,
    cushion_z,
    l5_clears,
    median_gap,
    row_excluded,
    tercile_edges,
)
from utils.fantasy_prop_filter import is_fantasy_prop_label  # noqa: E402
from utils.gate_shuffle import shuffle_cutoff  # noqa: E402
from cushion_log_window import iter_history, last10, load_series  # noqa: E402

GRADED = MAIN / "ui_runner" / "templates"
OUTS = MAIN / "outputs"
DEST = _REPO / "data" / "reports" / "cushion_z_discovery.json"
BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER")
FEATURES = ("cushion_z", "median_gap", "clear_count")
SPORTS = {
    "Tennis": "tennis",
    "MLB": "mlb",
    "WNBA": "wnba",
    "Soccer": "soccer",
    "NHL": "nhl",
    "NFL": "nfl",
    "CFB": "cfb",
    "Golf": "golf",
    "NBA": "nba",
    "CBB": "cbb",
    "WCBB": "wcbb",
}
GOBLIN_BE = (0.845, 0.769)
STANDARD_BE = (0.577, 0.550)
THIN_N = 30
R7_N = 150
DRAWS = 200


def fnum(v):
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    if x != x:
        return None
    return x


def norm_player(s: object) -> str:
    import re

    x = re.sub(r"[^a-z0-9 ]+", " ", str(s or "").lower())
    return re.sub(r"\s+", " ", x).strip()


def book_of(pick: object, side: str) -> str | None:
    label = T.book_label(str(pick or ""), side)
    return label if label in BOOKS else None


def find_step8(date: str, folder: str) -> Path | None:
    names = (
        f"step8_{folder}_direction.csv",
        f"step8_{folder}_direction_clean.csv",
        f"step8_{folder}_direction_clean_{date}.csv",
        f"step8_{folder}_direction_clean_{date}.parquet",
        f"step8_{folder}_direction.parquet",
        f"step8_{folder}_direction_clean.parquet",
        f"step8_{folder}_direction.xlsx",
        f"step8_{folder}_direction_clean.xlsx",
        f"step8_{folder}_direction_clean_{date}.xlsx",
    )
    for root in (OUTS, _REPO / "outputs"):
        folder_path = root / date / folder
        for name in names:
            p = folder_path / name
            if p.is_file():
                return p
        for name in names:
            p = root / date / name
            if p.is_file():
                return p
    return None


def _rows_of(path: Path) -> list[dict]:
    if path.suffix.lower() == ".csv":
        with path.open(encoding="utf-8", newline="") as fh:
            return list(csv.DictReader(fh))
    import pandas as pd

    if path.suffix.lower() == ".parquet":
        df = pd.read_parquet(path)
    else:
        df = pd.read_excel(path)
    return df.to_dict("records")


def _col(row: dict, *names: str):
    lower = {str(k).strip().lower(): k for k in row}
    for name in names:
        key = lower.get(name.lower())
        if key is not None:
            return row.get(key)
    return None


CURRENT_SEASON_SPORTS = {"NFL", "CFB", "CBB", "WCBB"}


def _games_from_row(row: dict, sport: str) -> list:
    """Current-season list when the sport must not blend last year. Else the face window."""
    current = [fnum(_col(row, f"current_stat_g{i}", f"Current G{i}")) for i in range(1, 11)]
    if sport in CURRENT_SEASON_SPORTS:
        return current
    face = [fnum(_col(row, f"stat_g{i}", f"Stat G{i}", f"G{i}", f"g{i}")) for i in range(1, 11)]
    if any(v is not None for v in current):
        return current
    return face


def load_games(path: Path, sport: str) -> dict[tuple, tuple]:
    idx: dict[tuple, tuple] = {}
    depth: dict[tuple, int] = {}
    for row in _rows_of(path):
        raw_prop = _col(row, "prop_norm", "prop_type", "prop", "Prop")
        if is_fantasy_prop_label(raw_prop):
            continue
        prop = T.canon_prop(sport, str(raw_prop or ""))
        if not prop:
            continue
        pick_raw = str(_col(row, "pick_type", "Pick Type", "pick type") or "")
        if "demon" in pick_raw.lower():
            continue
        if "goblin" in pick_raw.lower():
            pick = "Goblin"
        elif "standard" in pick_raw.lower() or pick_raw == "":
            pick = "Standard"
        else:
            continue
        line = fnum(_col(row, "line", "line_score", "Line"))
        if line is None:
            continue
        games = _games_from_row(row, sport)
        finite = sum(v is not None for v in games)
        if finite < 8:
            continue
        key = (norm_player(_col(row, "player", "player_name", "Player")), prop, pick, round(line, 1))
        if depth.get(key, -1) < finite:
            idx[key] = tuple(games)
            depth[key] = finite
    return idx


def features(games: tuple, line: float, side: str, margin: float) -> dict | None:
    if not l5_clears(games, line, side):
        return None
    z = cushion_z(games, line, side)
    med = median_gap(games, line, side)
    clears = clear_count(games, line, side, margin)
    if z is None or med is None or clears is None:
        return None
    return {"cushion_z": z, "median_gap": med, "clear_count": float(clears)}


def breakeven(book: str) -> tuple[float, float]:
    return GOBLIN_BE if book.startswith("Goblin") else STANDARD_BE


def cell_report(rows: list[dict], feature: str, book: str, sport: str) -> dict | None:
    train = [r for r in rows if r["split"] == "train"]
    hold = [r for r in rows if r["split"] == "holdout"]
    edges = tercile_edges([r[feature] for r in train])
    if edges is None:
        return None
    buckets = {name: [] for name in ("low", "mid", "high")}
    for r in hold:
        buckets[assign_tercile(r[feature], edges)].append(r)
    out_buckets = []
    for name in ("low", "mid", "high"):
        group = buckets[name]
        hits = sum(r["hit"] for r in group)
        n = len(group)
        band = wilson_band(hits, n) if n else None
        out_buckets.append(
            {
                "bucket": name,
                "hits": hits,
                "n": n,
                "rate": None if n == 0 else round(hits / n, 4),
                "wilson_lo": None if band is None else round(band[0], 4),
                "wilson_hi": None if band is None else round(band[1], 4),
                "thin": n < THIN_N,
            }
        )
    rates = [b["rate"] for b in out_buckets]
    rising = all(r is not None for r in rates) and rates[0] <= rates[1] <= rates[2]
    ceiling = all(r is not None and r >= 0.95 for r in rates)
    high = out_buckets[2]
    shuffle_p95 = None
    beats = None
    if len(hold) >= 15 and high["n"] > 0:
        labels = [r["hit"] for r in hold]
        groups = [r["date"] for r in hold]

        def score(shuffled: list[int]) -> float:
            picked = [
                shuffled[i]
                for i, r in enumerate(hold)
                if assign_tercile(r[feature], edges) == "high"
            ]
            if not picked:
                return 0.0
            return sum(picked) / len(picked)

        shuffle_p95 = round(shuffle_cutoff(groups, labels, score, draws=DRAWS, seed=0), 4)
        beats = high["rate"] > shuffle_p95
    be2, be3 = breakeven(book)
    notes = []
    if sport in {"NFL", "CFB"} and book == "Standard UNDER":
        notes.append("snap regrade was never applied; a zero-minute actual can cash an Under")
    if sport == "Tennis" and book == "Standard UNDER":
        notes.append("retirement and walkover readings were removed before the split")
    blocked = "discovery only; not wired"
    if sport in {"NFL", "CFB"} and book == "Standard UNDER":
        blocked = "snap regrade not applied"
    elif ceiling:
        blocked = "near 100% in every bucket; that is the line"
    elif high["thin"]:
        blocked = "top bucket n<30"
    elif not rising:
        blocked = "holdout hit rate does not rise low to high"
    elif beats is not True:
        blocked = "top bucket does not beat the shuffle cutoff"
    elif high["wilson_lo"] is None or high["wilson_lo"] <= be2:
        blocked = "Wilson low does not clear the 2-leg break-even"
    elif high["n"] < R7_N:
        blocked = "holdout top bucket is under the R7 n of 150"
    return {
        "train_n": len(train),
        "holdout_n": len(hold),
        "edges": [round(edges[0], 4), round(edges[1], 4)],
        "edges_tied": edges[0] == edges[1],
        "buckets": out_buckets,
        "rising": rising,
        "ceiling": ceiling,
        "shuffle_p95": shuffle_p95,
        "beats_shuffle": beats,
        "breakeven_2leg": be2,
        "breakeven_3leg": be3,
        "wired": False,
        "blocked_reason": blocked,
        "notes": notes,
    }


def load_corrections() -> dict[tuple, str]:
    """Latest grade fix. Includes corrected tennis attaches and the snap regrade."""
    path = _REPO / "data" / "reports" / "grade_corrections.jsonl"
    idx: dict[tuple, str] = {}
    if not path.is_file():
        return idx
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        new = str(rec.get("new") or "").strip().upper()
        if not new:
            continue
        prop = "".join(ch for ch in str(rec.get("prop") or "").lower() if ch.isalnum())
        line_v = fnum(rec.get("line"))
        idx[(
            str(rec.get("date") or "")[:10],
            norm_player(rec.get("player")),
            prop,
            str(rec.get("direction") or "").strip().upper(),
            None if line_v is None else round(line_v, 1),
        )] = new
    return idx


def _accept(g: dict, date: str, rows: list, dropped, boards: dict, series) -> None:
    sport = T.norm_sport(str(g.get("sport") or ""))
    if sport not in SPORTS:
        return
    result = str(g.get("result") or "").upper()
    side_key = str(g.get("direction") or g.get("over_under") or "").upper()
    prop_key = "".join(ch for ch in str(g.get("prop") or "").lower() if ch.isalnum())
    line_key = fnum(g.get("line"))
    fixed = corrections_for(g, date, prop_key, side_key, line_key)
    if fixed:
        result = fixed
    if result not in {"HIT", "MISS"}:
        if fixed:
            dropped["corrected_out"] += 1
        return
    player_name = str(g.get("player") or "")
    prop_name = str(g.get("prop") or "")
    if "+" in player_name or "combo" in prop_name.lower():
        dropped["combo"] += 1
        return
    why = row_excluded(g)
    if why:
        dropped[why] += 1
        return
    if is_fantasy_prop_label(g.get("prop")):
        dropped["fantasy"] += 1
        return
    side = side_key
    if side in {"O"}:
        side = "OVER"
    if side in {"U"}:
        side = "UNDER"
    book = book_of(g.get("pick_type"), side)
    if book is None:
        dropped["other_book"] += 1
        return
    prop = T.canon_prop(sport, prop_name)
    if not prop:
        dropped["prop"] += 1
        return
    line = line_key
    if line is None:
        return
    cache_key = (date, sport)
    if cache_key not in boards:
        path = find_step8(date, SPORTS[sport])
        boards[cache_key] = load_games(path, sport) if path else {}
    games = boards[cache_key].get(
        (norm_player(player_name), prop, book.split()[0], round(line, 1))
    )
    if not games:
        games = last10(series, sport, norm_player(player_name), prop, date)
    if not games:
        dropped["no_games"] += 1
        return
    margin = float(T.cover_need(sport, prop))
    feat = features(games, line, side, margin)
    if feat is None:
        dropped["not_l5_eq5_or_no_spread"] += 1
        return
    rows.append(
        {
            "date": date,
            "sport": sport,
            "prop": prop,
            "book": book,
            "hit": 1 if result == "HIT" else 0,
            **feat,
        }
    )


def corrections_for(g, date, prop_key, side_key, line_key):
    return _CORRECTIONS.get((
        date,
        norm_player(g.get("player")),
        prop_key,
        side_key,
        None if line_key is None else round(line_key, 1),
    ))


_CORRECTIONS: dict = {}


def main() -> None:
    global _CORRECTIONS
    _CORRECTIONS = load_corrections()
    print("loading game logs (includes the previous season)", flush=True)
    series = load_series()
    print("log series", len(series), flush=True)
    dates = sorted(p.stem.replace("graded_props_", "")[:10] for p in GRADED.glob("graded_props_20*.json"))
    rows: list[dict] = []
    dropped = defaultdict(int)
    boards: dict[tuple, dict] = {}
    for date in dates:
        raw = json.loads((GRADED / f"graded_props_{date}.json").read_text(encoding="utf-8"))
        props = raw.get("props") or raw.get("rows") or []
        for g in props:
            _accept(g, date, rows, dropped, boards, series)
        print(f"{date} rows {len(rows)}", flush=True)

    print("archive dates before the templates board", flush=True)
    n_before = len(rows)
    for g in iter_history(set(dates)):
        _accept(g, g["date"], rows, dropped, boards, series)
    print(f"archive added {len(rows) - n_before}", flush=True)

    by_sport: dict[str, dict] = {}
    sport_dates: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        sport_dates[r["sport"]].append(r["date"])
    for sport, seen in sport_dates.items():
        ordered = sorted(set(seen))
        cut = ordered[int(len(ordered) * 0.7)] if len(ordered) >= 4 else (ordered[-1] if ordered else "")
        by_sport[sport] = {"cut": cut, "dates": ordered}
    for r in rows:
        cut = by_sport[r["sport"]]["cut"]
        r["split"] = "holdout" if r["date"] >= cut else "train"

    grouped: dict[tuple, list] = defaultdict(list)
    for r in rows:
        grouped[(r["sport"], r["prop"], r["book"])].append(r)

    cells = []
    for (sport, prop, book), group in sorted(grouped.items()):
        for feature in FEATURES:
            report = cell_report(group, feature, book, sport)
            if report is None:
                continue
            cells.append({"sport": sport, "prop": prop, "book": book, "feature": feature, **report})

    rising_usable = [
        c
        for c in cells
        if c["feature"] == "cushion_z" and c["rising"] and not c["buckets"][2]["thin"] and not c["ceiling"]
    ]
    sport_summary = {}
    for sport, info in sorted(by_sport.items()):
        cut = info["cut"]
        ordered = info["dates"]
        legs = [r for r in rows if r["sport"] == sport]
        sport_summary[sport] = {
            "cut": cut,
            "eligible": len(legs),
            "holdout_legs": sum(1 for r in legs if r["split"] == "holdout"),
            "train_dates": [d for d in ordered if d < cut],
            "holdout_dates": [d for d in ordered if d >= cut],
        }
    payload = {
        "wired": False,
        "source": {
            "graded": str(GRADED),
            "archive": "data/cache/*_props_history.db for dates the templates board does not cover",
            "game_logs": "MLB stats cache (2025+2026), WNBA ESPN cache (2025+2026), historical_actuals player_game_logs (prior season through this season), Sackmann tennis matches",
            "step8": str(OUTS),
            "books": list(BOOKS),
            "over_and_goblin": "(L10 average - line) / L10 sample std",
            "standard_under": "(line - L10 average) / L10 sample std",
            "also": "median gap on the same side, and count of last-10 games clearing the line by the prop cover unit",
            "filter": "directional L5=5, at least 8 of the last 10 games present, DNP and tennis retirement/walkover out",
            "holdout": "per sport, dates on or after that sport's 70% date. The window uses the previous season's games.",
            "shuffle": "200 draws, labels shuffled inside the date, score is the high-tercile hit rate",
            "thin": THIN_N,
            "eligible_legs": len(rows),
            "dropped": dict(dropped),
            "by_sport": sport_summary,
        },
        "summary": {
            "cells": len(cells),
            "cushion_z_rising_and_n_at_least_30": len(rising_usable),
            "note": "A rising holdout is a description. blocked_reason says why it is not a gate.",
        },
        "cells": cells,
    }
    DEST.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("wrote", DEST, "cells", len(cells), "eligible", len(rows))
    for sport, info in sport_summary.items():
        print(
            f"  {sport} cut {info['cut']} holdout {info['holdout_legs']} "
            f"of {info['eligible']}  {info['holdout_dates'][0] if info['holdout_dates'] else ''} .. "
            f"{info['holdout_dates'][-1] if info['holdout_dates'] else ''}"
        )
    print("cushion_z rising with top n>=30", len(rising_usable))
    for c in rising_usable:
        high = c["buckets"][2]
        print(
            f"  {c['sport']} {c['prop']} {c['book']} {high['hits']}/{high['n']} {high['rate']} {c['blocked_reason']}"
        )


if __name__ == "__main__":
    main()
