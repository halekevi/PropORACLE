#!/usr/bin/env python3
"""Hit-rate lift from pre-match Sackmann tennis factors.

Joins graded tennis HIT/MISS rows to each player's last-5 Sackmann matches
*before* the slate date, then buckets continuous factors (high vs low vs median)
and categorical flags (lefty opp, surface, rest). Reports which factors move
hit rate for Goblin/Standard × prop markets we actually grade.

  py -3.14 scripts/tennis_factor_hit_rate_lift.py
  py -3.14 scripts/tennis_factor_hit_rate_lift.py --min-n 25
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any

_REPO = Path(__file__).resolve().parents[1]
_TENNIS = _REPO / "Sports" / "Tennis" / "scripts"
MAIN = _REPO.parent / "PropORACLE_main_cp"
for p in (_REPO, _TENNIS, _REPO / "scripts"):
    if str(p) not in sys.path:
        sys.path.insert(0, str(p))

from tennis_shared import (  # noqa: E402
    build_sackmann_player_index,
    ensure_sackmann_matches,
    norm_key,
    norm_tennis_prop,
    parse_sackmann_tourney_date,
)

OUT = _REPO / "data" / "reports" / "tennis_factor_hit_rate_lift.json"
GRADED_DIRS = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]

# Pre-match L5 means we can derive from the expanded Sackmann index.
NUMERIC_FACTORS = (
    "l5_aces",
    "l5_df",
    "l5_first_in_pct",
    "l5_first_won_pct",
    "l5_second_won_pct",
    "l5_bp_saved_pct",
    "l5_bp_won",
    "l5_games_won",
    "l5_match_games",
    "l5_minutes",
    "days_rest",
    "rank_gap",  # opp_rank - own_rank (positive = higher-ranked opp numerically worse)
)
CATEGORICAL_FACTORS = (
    "opp_lefty",
    "recent_surface",  # Hard / Clay / Grass from last match
    "rest_bucket",  # 0-1 / 2-3 / 4-7 / 8+
    "best_of",  # 3 / 5 from last match
)

AVAILABILITY = [
    {"family": "Service", "stat": "Aces", "source": "Sackmann + TA", "status": "tracked", "graded": "backfillable"},
    {"family": "Service", "stat": "Double faults", "source": "Sackmann + TA", "status": "tracked", "graded": "backfillable"},
    {"family": "Service", "stat": "1st serve % (serves in)", "source": "Sackmann + TA", "status": "tracked_now", "graded": "no"},
    {"family": "Service", "stat": "1st serve points won %", "source": "Sackmann + TA", "status": "tracked_now", "graded": "no"},
    {"family": "Service", "stat": "2nd serve points won %", "source": "Sackmann + TA", "status": "tracked_now", "graded": "no"},
    {"family": "Service", "stat": "Service games won %", "source": "partial (TA games)", "status": "partial", "graded": "no"},
    {"family": "Service", "stat": "Break points saved %", "source": "Sackmann + TA", "status": "tracked_now", "graded": "no"},
    {"family": "Service", "stat": "Service points won % total", "source": "derivable", "status": "not_yet", "graded": "no"},
    {"family": "Service", "stat": "Avg / max serve speed", "source": "none", "status": "unavailable", "graded": "no"},
    {"family": "Service", "stat": "Unreturned serves %", "source": "none", "status": "unavailable", "graded": "no"},
    {"family": "Return", "stat": "Return / 1st / 2nd return pts %", "source": "none in flat files", "status": "unavailable", "graded": "no"},
    {"family": "Return", "stat": "Break points converted", "source": "Sackmann + TA", "status": "tracked", "graded": "voids"},
    {"family": "Return", "stat": "Return games won %", "source": "none", "status": "unavailable", "graded": "no"},
    {"family": "Rally", "stat": "Winners / UFE / FE / rally length / net", "source": "none", "status": "unavailable", "graded": "no"},
    {"family": "Structure", "stat": "Total games / games won / sets / TB", "source": "score parse", "status": "tracked", "graded": "yes (games)"},
    {"family": "Structure", "stat": "Bagels / breadsticks / deciding-set", "source": "score parseable", "status": "not_yet", "graded": "no"},
    {"family": "Clutch", "stat": "BP faced/saved/converted counts", "source": "Sackmann + TA", "status": "tracked_now", "graded": "partial"},
    {"family": "Clutch", "stat": "Deuce / Ad / rebreak / hold %", "source": "point-by-point", "status": "unavailable", "graded": "no"},
    {"family": "Situational", "stat": "1st-set W/L, 3-set, day/night, venue", "source": "partial", "status": "not_yet", "graded": "no"},
    {"family": "Situational", "stat": "vs lefty", "source": "Sackmann hand + TA ohand", "status": "tracked_now", "graded": "n/a factor"},
    {"family": "Situational", "stat": "Surface win % / by surface", "source": "Sackmann surface", "status": "tracked_now", "graded": "n/a factor"},
    {"family": "Physical", "stat": "Match minutes", "source": "Sackmann", "status": "tracked_now", "graded": "n/a factor"},
    {"family": "Physical", "stat": "Days rest / B2B", "source": "derived from dates", "status": "tracked_now", "graded": "n/a factor"},
    {"family": "Physical", "stat": "Distance run", "source": "none", "status": "unavailable", "graded": "no"},
    {"family": "H2H", "stat": "H2H overall / surface / recent", "source": "row h2h_* sometimes", "status": "partial_row", "graded": "n/a"},
    {"family": "Ranking", "stat": "ATP/WTA rank + gap", "source": "Sackmann + TA", "status": "tracked_now", "graded": "D via rank"},
    {"family": "Ranking", "stat": "ELO / seeding trend", "source": "none", "status": "unavailable", "graded": "no"},
    {"family": "Tournament", "stat": "Level / best-of / surface", "source": "Sackmann", "status": "tracked_now", "graded": "n/a factor"},
    {"family": "Tournament", "stat": "Altitude / draw difficulty", "source": "none", "status": "unavailable", "graded": "no"},
]


def _finite(v: object) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:
        return None
    return f


def _mean(vals: list[float]) -> float | None:
    if not vals:
        return None
    return sum(vals) / len(vals)


def _book(row: dict) -> str:
    pt = str(row.get("pick_type") or row.get("book") or "").strip().lower()
    direction = str(row.get("direction") or row.get("over_under") or "").strip().upper()
    if "demon" in pt:
        return f"Demon {direction}" if direction in ("OVER", "UNDER") else "Demon"
    if "goblin" in pt:
        side = direction if direction in ("OVER", "UNDER") else "OVER"
        return f"Goblin {side}"
    if direction in ("OVER", "UNDER"):
        return f"Standard {direction}"
    return "Unknown"


def _graded_paths() -> list[Path]:
    seen: set[str] = set()
    out: list[Path] = []
    for d in GRADED_DIRS:
        if not d.exists():
            continue
        for p in sorted(d.glob("graded_props_20*.json")):
            key = p.name
            if key in seen:
                continue
            seen.add(key)
            out.append(p)
    return out


def _iter_props(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("props", "legs", "rows", "graded", "items"):
        v = data.get(key)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
    # some files are {date: [...]} or sport buckets
    for v in data.values():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            sample = v[0]
            if "player" in sample or "prop" in sample or "sport" in sample:
                return v
    return []


def _slate_date(path: Path, rows: list[dict]) -> date | None:
    m = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
    if m:
        return date.fromisoformat(m.group(1))
    for r in rows[:5]:
        raw = str(r.get("graded_at") or r.get("date") or "")[:10]
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
            return date.fromisoformat(raw)
    return None


def _prior_rows(index_rows: list[dict], slate: date) -> list[dict]:
    """Matches strictly before slate (tourney_date is week-start; keep conservative)."""
    out: list[dict] = []
    for rec in index_rows:
        dt = parse_sackmann_tourney_date(rec.get("date"))
        if dt is None:
            continue
        if dt < slate:
            out.append(rec)
    return out


def _l5_mean(rows: list[dict], key: str, n: int = 5) -> float | None:
    vals: list[float] = []
    for rec in rows[:n]:
        v = _finite(rec.get(key))
        if v is not None:
            vals.append(v)
    return _mean(vals)


def _factors_for(rows: list[dict], slate: date) -> dict[str, Any]:
    if not rows:
        return {}
    last = rows[0]
    last_dt = parse_sackmann_tourney_date(last.get("date"))
    days_rest = (slate - last_dt).days if last_dt else None
    own_rank = _finite(last.get("rank"))
    opp_rank = _finite(last.get("opp_rank"))
    rank_gap = None
    if own_rank is not None and opp_rank is not None:
        rank_gap = opp_rank - own_rank
    opp_hand = str(last.get("opp_hand") or "").strip().upper()
    surface = str(last.get("surface") or "").strip().title() or None
    if surface and surface.lower() in {"nan", "none", "null", "nat"}:
        surface = None
    best_of = last.get("best_of")
    if isinstance(best_of, float) and best_of != best_of:
        best_of = None
    try:
        if best_of is not None and best_of != "":
            best_of = int(float(best_of))
    except (TypeError, ValueError):
        best_of = None
    if days_rest is None:
        rest_bucket = None
    elif days_rest <= 1:
        rest_bucket = "0-1"
    elif days_rest <= 3:
        rest_bucket = "2-3"
    elif days_rest <= 7:
        rest_bucket = "4-7"
    else:
        rest_bucket = "8+"
    return {
        "l5_aces": _l5_mean(rows, "aces"),
        "l5_df": _l5_mean(rows, "double_faults"),
        "l5_first_in_pct": _l5_mean(rows, "first_in_pct"),
        "l5_first_won_pct": _l5_mean(rows, "first_won_pct"),
        "l5_second_won_pct": _l5_mean(rows, "second_won_pct"),
        "l5_bp_saved_pct": _l5_mean(rows, "bp_saved_pct"),
        "l5_bp_won": _l5_mean(rows, "break_points_won"),
        "l5_games_won": _l5_mean(rows, "games_won"),
        "l5_match_games": _l5_mean(rows, "match_total_games"),
        "l5_minutes": _l5_mean(rows, "minutes"),
        "days_rest": float(days_rest) if days_rest is not None else None,
        "rank_gap": rank_gap,
        "opp_lefty": "Y" if opp_hand == "L" else ("N" if opp_hand in {"R", "U"} else None),
        "recent_surface": surface,
        "rest_bucket": rest_bucket,
        "best_of": str(best_of) if best_of is not None else None,
    }


def _median(vals: list[float]) -> float | None:
    if not vals:
        return None
    s = sorted(vals)
    mid = len(s) // 2
    if len(s) % 2:
        return s[mid]
    return 0.5 * (s[mid - 1] + s[mid])


def _pct(hits: int, n: int) -> float | None:
    if n <= 0:
        return None
    return round(100.0 * hits / n, 1)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--min-n", type=int, default=30, help="Min samples per bucket to report")
    ap.add_argument("--out", type=Path, default=OUT)
    args = ap.parse_args()

    print("Loading Sackmann…")
    matches = ensure_sackmann_matches()
    index = build_sackmann_player_index(matches)
    print(f"  players={len(index)} matches={len(matches)}")

    # Collect enriched legs
    legs: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    paths = _graded_paths()
    print(f"Scanning {len(paths)} graded_props files…")
    for path in paths:
        rows = _iter_props(path)
        slate = _slate_date(path, rows)
        if slate is None:
            continue
        for row in rows:
            if str(row.get("sport") or "").strip().lower() != "tennis":
                continue
            result = str(row.get("result") or "").strip().upper()
            if result not in ("HIT", "MISS"):
                continue
            player = str(row.get("player") or "").strip()
            prop = str(row.get("prop") or row.get("prop_type") or "").strip()
            if not player or not prop:
                continue
            pnorm = norm_tennis_prop(prop)
            book = _book(row)
            if "Demon" in book:
                continue
            line = _finite(row.get("line"))
            pk = norm_key(player)
            # de-dupe across main_cp + open copies
            dkey = f"{slate}|{pk}|{pnorm}|{book}|{line}|{result}"
            if dkey in seen_keys:
                continue
            seen_keys.add(dkey)
            prior = _prior_rows(index.get(pk) or [], slate)
            factors = _factors_for(prior, slate)
            if not factors:
                continue
            legs.append(
                {
                    "slate": slate.isoformat(),
                    "player": player,
                    "prop": prop,
                    "prop_norm": pnorm,
                    "book": book,
                    "line": line,
                    "hit": 1 if result == "HIT" else 0,
                    **factors,
                }
            )

    print(f"  enriched legs={len(legs)}")

    # Baseline by book × prop
    base: dict[tuple[str, str], dict[str, int]] = defaultdict(lambda: {"n": 0, "hits": 0})
    for leg in legs:
        key = (leg["book"], leg["prop_norm"])
        base[key]["n"] += 1
        base[key]["hits"] += int(leg["hit"])

    baselines = []
    for (book, prop), st in sorted(base.items(), key=lambda x: (-x[1]["n"], x[0])):
        baselines.append(
            {
                "book": book,
                "prop": prop,
                "n": st["n"],
                "hit_pct": _pct(st["hits"], st["n"]),
            }
        )

    # Numeric: high vs low vs median within book×prop
    numeric_rows: list[dict[str, Any]] = []
    for book, prop in {(l["book"], l["prop_norm"]) for l in legs}:
        subset = [l for l in legs if l["book"] == book and l["prop_norm"] == prop]
        base_n = len(subset)
        base_hits = sum(int(l["hit"]) for l in subset)
        base_pct = _pct(base_hits, base_n)
        for factor in NUMERIC_FACTORS:
            vals = [_finite(l.get(factor)) for l in subset]
            known = [(l, v) for l, v in zip(subset, vals) if v is not None]
            if len(known) < args.min_n * 2:
                continue
            med = _median([v for _, v in known])
            if med is None:
                continue
            high = [(l, v) for l, v in known if v >= med]
            low = [(l, v) for l, v in known if v < med]
            for label, bucket in (("high", high), ("low", low)):
                n = len(bucket)
                if n < args.min_n:
                    continue
                hits = sum(int(l["hit"]) for l, _ in bucket)
                hp = _pct(hits, n)
                lift = None if hp is None or base_pct is None else round(hp - base_pct, 1)
                numeric_rows.append(
                    {
                        "book": book,
                        "prop": prop,
                        "factor": factor,
                        "bucket": label,
                        "threshold": "median",
                        "median": round(med, 3),
                        "n": n,
                        "hit_pct": hp,
                        "base_n": base_n,
                        "base_hit_pct": base_pct,
                        "lift_pp": lift,
                        "coverage_n": len(known),
                    }
                )

    # Categorical
    cat_rows: list[dict[str, Any]] = []
    for book, prop in {(l["book"], l["prop_norm"]) for l in legs}:
        subset = [l for l in legs if l["book"] == book and l["prop_norm"] == prop]
        base_n = len(subset)
        base_hits = sum(int(l["hit"]) for l in subset)
        base_pct = _pct(base_hits, base_n)
        for factor in CATEGORICAL_FACTORS:
            buckets: dict[str, list[dict]] = defaultdict(list)
            for l in subset:
                raw = l.get(factor)
                if raw is None or raw == "":
                    continue
                buckets[str(raw)].append(l)
            for label, bucket in buckets.items():
                n = len(bucket)
                if n < args.min_n:
                    continue
                hits = sum(int(l["hit"]) for l in bucket)
                hp = _pct(hits, n)
                lift = None if hp is None or base_pct is None else round(hp - base_pct, 1)
                cat_rows.append(
                    {
                        "book": book,
                        "prop": prop,
                        "factor": factor,
                        "bucket": label,
                        "n": n,
                        "hit_pct": hp,
                        "base_n": base_n,
                        "base_hit_pct": base_pct,
                        "lift_pp": lift,
                    }
                )

    # Rank lifts: best absolute lift among high/low for each factor×book×prop
    lifts = [r for r in numeric_rows + cat_rows if r.get("lift_pp") is not None]
    lifts_sorted = sorted(lifts, key=lambda r: (-abs(float(r["lift_pp"])), -int(r["n"])))

    # Keep interesting: |lift| >= 3pp and n>=min
    notable = [r for r in lifts_sorted if abs(float(r["lift_pp"])) >= 3.0][:80]

    report = {
        "generated": date.today().isoformat(),
        "min_n": args.min_n,
        "legs": len(legs),
        "players_indexed": len(index),
        "availability": AVAILABILITY,
        "baselines": baselines,
        "notable_lifts": notable,
        "numeric_lifts": sorted(numeric_rows, key=lambda r: (-abs(float(r.get("lift_pp") or 0)), -r["n"])),
        "categorical_lifts": sorted(cat_rows, key=lambda r: (-abs(float(r.get("lift_pp") or 0)), -r["n"])),
        "notes": [
            "Factors are pre-match L5 means from Sackmann matches with tourney_date < slate.",
            "High/low splits are vs the median within that book×prop (not a fixed threshold).",
            "Demon books excluded. Fantasy already blocked at intake.",
            "Serve-speed, rally, winners/UFE, distance, ELO, altitude are unavailable in Sackmann/TA flat feeds.",
        ],
    }
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"Wrote {args.out}")
    print(f"Baselines: {len(baselines)}  notable |lift|>=3pp: {len(notable)}")
    for r in notable[:15]:
        print(
            f"  {r['book']:18} {r['prop']:16} {r['factor']:20} {r['bucket']:6} "
            f"n={r['n']:4} hit={r['hit_pct']}% base={r['base_hit_pct']}% lift={r['lift_pp']:+}pp"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
