"""Re-grade catalog-suspect cells with strict DNP / leakage / pick twins.

Targets from the master hit-rate catalog critique:
  (1) NBA1Q Goblin OVER Points/Rebounds/Assists + Standard OVER Assists
      — confirm 1Q legs not graded on full-game boxes
  (2) NHL Standard UNDER Points/Assists at L5=5+L10>=8 — scratch/DNP voids
  (3) Explain NFL Std UNDER identical gate counts, NHL FOW 98.4%, Soccer
      Passes Attempted 0%

Writes data/reports/audit_catalog_suspect_cells.{json,md}
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from analyze_graded_history import (  # noqa: E402
    _norm_dir,
    _norm_pick,
    _norm_sport,
    _parse_hit,
)
from analyze_l5_gate_rates import _directional_l5  # noqa: E402
from all_ranked_prop_hit_rates import l10_dir, l5_dir  # noqa: E402
from utils.comparison_tracks import wilson_band  # noqa: E402
from utils.dnp_zero_void import is_dnp_zero_actual  # noqa: E402
from utils.pp_vegas_gap import is_grading_suspect  # noqa: E402

CATALOG = ROOT / "data" / "reports" / "all_ranked_prop_hit_rates.json"
OUT_JSON = ROOT / "data" / "reports" / "audit_catalog_suspect_cells.json"
OUT_MD = ROOT / "data" / "reports" / "audit_catalog_suspect_cells.md"
DB = ROOT / "data" / "cache" / "proporacle_ref.db"

GRADED_DIRS = [
    ROOT.parent / "PropORACLE_main_cp" / "ui_runner" / "templates",
    ROOT / "ui_runner" / "templates",
]

# Catalog targets: (sport, book, prop_norm substrings)
TARGETS = [
    ("NBA1Q", "Goblin OVER", "points", "l5eq5"),
    ("NBA1Q", "Goblin OVER", "rebounds", "l5eq5"),
    ("NBA1Q", "Goblin OVER", "assists", "l5eq5"),
    ("NBA1Q", "Standard OVER", "assists", "l5eq5"),
    ("NHL", "Standard UNDER", "points", "l5eq5_l10ge8"),
    ("NHL", "Standard UNDER", "assists", "l5eq5_l10ge8"),
]

EXPLAIN = [
    ("NFL", "Standard UNDER", "receiving yards"),
    ("NFL", "Standard UNDER", "receptions"),
    ("NFL", "Standard UNDER", "rushing yards"),
    ("NFL", "Standard UNDER", "sacks"),
    ("NFL", "Standard UNDER", "longest reception"),
    ("NHL", "Standard OVER", "faceoffs won"),
    ("Soccer", "Standard OVER", "passes attempted"),
    ("Soccer", "Standard OVER", "clearances"),
]


def _ci(hits: int, n: int) -> dict[str, Any]:
    hr = (hits / n) if n else None
    band = wilson_band(hits, n) if n else None
    return {
        "hits": hits,
        "n": n,
        "hit_rate": round(hr, 4) if hr is not None else None,
        "wilson_low": round(band[0], 4) if band else None,
        "wilson_high": round(band[1], 4) if band else None,
    }


def _prop_key(s: object) -> str:
    return str(s or "").strip().lower().replace("_", " ")


def _book(row: dict) -> str | None:
    pt = _norm_pick(row.get("pick_type"))
    side = _norm_dir(row.get("direction") or row.get("over_under"))
    if pt == "goblin" and side == "OVER":
        return "Goblin OVER"
    if pt == "standard" and side == "OVER":
        return "Standard OVER"
    if pt == "standard" and side == "UNDER":
        return "Standard UNDER"
    return None


def _num(v: object) -> float | None:
    try:
        if v is None or str(v).strip() in {"", "—", "-", "nan", "None"}:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def _actual(r: dict) -> float | None:
    for k in ("actual", "actual_value", "stat_value"):
        v = _num(r.get(k))
        if v is not None:
            return v
    return None


def _norm_player(s: object) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def _catalog_row(rows: list[dict], sport: str, book: str, prop: str) -> dict | None:
    prop_l = prop.lower()
    for r in rows:
        if r.get("sport") != sport or r.get("book") != book:
            continue
        pd = str(r.get("prop_display") or r.get("prop") or "").lower().replace("_", " ")
        if pd == prop_l or prop_l in pd:
            return r
    return None


def _gate_counts(cat: dict, gate: str) -> dict[str, Any]:
    if gate == "l5eq5":
        return _ci(int(cat.get("l5eq5_hits") or 0), int(cat.get("l5eq5_n") or 0))
    if gate == "l5eq5_l10ge8":
        return _ci(
            int(cat.get("l5eq5_l10ge8_hits") or 0),
            int(cat.get("l5eq5_l10ge8_n") or 0),
        )
    return _ci(int(cat.get("hits") or 0), int(cat.get("n") or 0))


class NbaFgIndex:
    """Full-game NBA boxes for contamination check vs 1Q actuals."""

    def __init__(self) -> None:
        self.by_player: dict[str, list[tuple[str, dict[str, float]]]] = defaultdict(list)
        if not DB.is_file():
            return
        con = sqlite3.connect(str(DB))
        try:
            cols = {r[1] for r in con.execute("PRAGMA table_info(nba)")}
            need = ["game_date", "player", "pts", "reb", "ast", "minutes"]
            use = [c for c in need if c in cols]
            if len(use) < 3:
                return
            q = f"SELECT {', '.join(use)} FROM nba"
            for row in con.execute(q):
                gd = str(row[0] or "")[:10]
                if len(gd) != 10:
                    continue
                pn = _norm_player(row[1])
                vals: dict[str, float] = {}
                for i, c in enumerate(use[2:], start=2):
                    try:
                        vals[c] = float(row[i])
                    except (TypeError, ValueError):
                        continue
                self.by_player[pn].append((gd, vals))
        except sqlite3.Error:
            return
        finally:
            con.close()
        for pn in self.by_player:
            self.by_player[pn].sort(key=lambda x: x[0])

    def fg_on(self, player: str, game_date: str) -> dict[str, float] | None:
        pn = _norm_player(player)
        for gd, vals in self.by_player.get(pn, []):
            if gd == game_date:
                return vals
        return None


class NhlIndex:
    def __init__(self) -> None:
        self.by_player: dict[str, list[tuple[str, dict[str, float]]]] = defaultdict(list)
        if not DB.is_file():
            return
        con = sqlite3.connect(str(DB))
        try:
            cols = {r[1] for r in con.execute("PRAGMA table_info(nhl)")}
            need = ["game_date", "player", "points", "assists", "toi", "faceoffs_won"]
            use = [c for c in need if c in cols]
            if "game_date" not in use or "player" not in use:
                return
            q = f"SELECT {', '.join(use)} FROM nhl"
            for row in con.execute(q):
                gd = str(row[0] or "")[:10]
                if len(gd) != 10:
                    continue
                pn = _norm_player(row[1])
                vals: dict[str, float] = {}
                for i, c in enumerate(use[2:], start=2):
                    try:
                        vals[c] = float(row[i])
                    except (TypeError, ValueError):
                        continue
                self.by_player[pn].append((gd, vals))
        except sqlite3.Error:
            return
        finally:
            con.close()
        for pn in self.by_player:
            self.by_player[pn].sort(key=lambda x: x[0])

    def games(self, player: str) -> list[tuple[str, dict[str, float]]]:
        return self.by_player.get(_norm_player(player), [])


def _leakage_l5(
    games: list[tuple[str, dict[str, float]]],
    *,
    cutoff: str,
    col: str,
    line: float,
    direction: str,
) -> int | None:
    prior = [(gd, vals) for gd, vals in games if gd < cutoff and col in vals]
    if len(prior) < 5:
        return None
    window = prior[-5:]
    hits = 0
    for _, vals in window:
        v = vals[col]
        if direction == "OVER" and v > line:
            hits += 1
        elif direction == "UNDER" and v < line:
            hits += 1
    return hits


def _iter_graded() -> list[tuple[str, list[dict]]]:
    seen: set[str] = set()
    out: list[tuple[str, list[dict]]] = []
    for d in GRADED_DIRS:
        if not d.is_dir():
            continue
        for path in sorted(d.glob("graded_props_*.json")):
            if ".bak_" in path.name:
                continue
            date = path.stem.replace("graded_props_", "")[:10]
            if date in seen:
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            chunk = raw.get("props", raw.get("rows", []))
            if not isinstance(chunk, list):
                continue
            seen.add(date)
            out.append((date, [r for r in chunk if isinstance(r, dict)]))
    return out


def _twin_ok(r: dict, goblin_lines: dict[tuple[str, str], set[float]]) -> tuple[bool, str]:
    """Verify pick_type vs Goblin/Standard line twins on the same slate file."""
    pt = _norm_pick(r.get("pick_type"))
    side = _norm_dir(r.get("direction") or r.get("over_under"))
    line = _num(r.get("line"))
    player = str(r.get("player") or "").strip().lower()
    prop = _prop_key(r.get("prop"))
    glines = goblin_lines.get((player, prop)) or set()
    if pt == "goblin":
        if not glines:
            return True, "goblin_no_twin_check"
        # Goblin should be the easier OVER line (lower than Standard when both exist)
        return True, "goblin"
    if pt != "standard":
        return False, "not_std_goblin"
    if line is None:
        return False, "no_line"
    if not glines:
        return True, "std_no_goblin_twin"
    # Standard OVER line below Goblin twin = mislabel / wrong face
    if side == "OVER" and line < min(glines) - 1e-9:
        return False, "std_over_below_goblin"
    if side == "OVER" and any(abs(line - g) < 1e-9 for g in glines):
        return False, "std_line_eq_goblin"
    return True, "std_twin_ok"


def regrade_targets(catalog_rows: list[dict]) -> dict[str, Any]:
    nba_fg = NbaFgIndex()
    nhl = NhlIndex()
    results: dict[str, Any] = {}

    # Accumulators per target key
    acc: dict[str, dict[str, Any]] = {}
    for sport, book, prop, gate in TARGETS:
        key = f"{sport}|{book}|{prop}|{gate}"
        cat = _catalog_row(catalog_rows, sport, book, prop)
        acc[key] = {
            "sport": sport,
            "book": book,
            "prop": prop,
            "gate": gate,
            "catalog_before": _gate_counts(cat, gate) if cat else None,
            "catalog_ungated": _ci(int(cat["hits"]), int(cat["n"])) if cat else None,
            "raw_n": 0,
            "after": Counter(),
            "drop_reasons": Counter(),
            "nba1q_fg_match": 0,
            "nba1q_fg_mismatch": 0,
            "nba1q_fg_unchecked": 0,
            "nba1q_suspect_flag": 0,
            "nba1q_actual_gt_fg": 0,
            "samples_dropped": [],
            "samples_kept_miss": [],
        }

    for file_date, chunk in _iter_graded():
        goblin_lines: dict[tuple[str, str], set[float]] = defaultdict(set)
        for r in chunk:
            if _norm_pick(r.get("pick_type")) != "goblin":
                continue
            line = _num(r.get("line"))
            if line is None:
                continue
            goblin_lines[
                (str(r.get("player") or "").strip().lower(), _prop_key(r.get("prop")))
            ].add(line)

        for r in chunk:
            sport = _norm_sport(r.get("sport"))
            book = _book(r)
            if not book:
                continue
            prop = _prop_key(r.get("prop"))
            # match targets
            matched = None
            for sp, bk, pr, gate in TARGETS:
                if sport == sp and book == bk and (prop == pr or pr in prop):
                    matched = (sp, bk, pr, gate)
                    break
            if not matched:
                continue
            sp, bk, pr, gate = matched
            key = f"{sp}|{bk}|{pr}|{gate}"
            a = acc[key]
            a["raw_n"] += 1

            result = str(r.get("result") or "").strip().upper()
            if result in {"VOID", "PUSH", "DNP", "CANCELLED", "CANCELED"}:
                a["drop_reasons"]["already_void"] += 1
                continue

            # Gate filter (catalog definition)
            side = _norm_dir(r.get("direction") or r.get("over_under"))
            l5 = l5_dir(r, side) if side in {"OVER", "UNDER"} else None
            if l5 is None:
                l5 = _directional_l5(r)
            l10 = l10_dir(r, side) if side in {"OVER", "UNDER"} else None
            if gate == "l5eq5":
                if l5 is None or int(l5) != 5:
                    a["drop_reasons"]["l5_not_5"] += 1
                    continue
            elif gate == "l5eq5_l10ge8":
                if l5 is None or int(l5) != 5:
                    a["drop_reasons"]["l5_not_5"] += 1
                    continue
                if l10 is None or float(l10) < 8:
                    a["drop_reasons"]["l10_lt_8"] += 1
                    continue

            hit = _parse_hit(result)
            if hit is None:
                a["drop_reasons"]["undecided"] += 1
                continue

            actual_f = _actual(r)
            if is_dnp_zero_actual(actual_f, r):
                a["drop_reasons"]["strict_dnp_void"] += 1
                if len(a["samples_dropped"]) < 15:
                    a["samples_dropped"].append(
                        {
                            "reason": "strict_dnp_void",
                            "player": r.get("player"),
                            "date": file_date,
                            "actual": actual_f,
                            "line": r.get("line"),
                            "hit_was": hit,
                        }
                    )
                continue

            # Unknown PT + actual 0 on UNDER — count as soft flag, do NOT void
            # (strict rule). Still track inflation.
            if (
                side == "UNDER"
                and actual_f == 0.0
                and not is_dnp_zero_actual(actual_f, r)
            ):
                a["drop_reasons"]["under_zero_unknown_pt_kept"] += 1

            twin_ok, twin_reason = _twin_ok(r, goblin_lines)
            if not twin_ok:
                a["drop_reasons"][f"pick_twin:{twin_reason}"] += 1
                continue

            if is_grading_suspect(r):
                a["nba1q_suspect_flag"] += 1
                a["drop_reasons"]["grading_suspect_flag"] += 1
                continue

            game_date = str(r.get("game_date") or r.get("Game Date") or "")[:10]
            if len(game_date) != 10:
                game_date = file_date

            # NBA1Q: full-game contamination check
            if sport == "NBA1Q":
                fg = nba_fg.fg_on(str(r.get("player") or ""), game_date)
                col = {"points": "pts", "rebounds": "reb", "assists": "ast"}.get(pr)
                if fg is None or col is None or col not in fg or actual_f is None:
                    a["nba1q_fg_unchecked"] += 1
                else:
                    fg_v = fg[col]
                    if abs(fg_v - actual_f) < 1e-9:
                        a["nba1q_fg_match"] += 1
                        a["drop_reasons"]["nba1q_actual_equals_full_game"] += 1
                        if len(a["samples_dropped"]) < 20:
                            a["samples_dropped"].append(
                                {
                                    "reason": "nba1q_actual_equals_full_game",
                                    "player": r.get("player"),
                                    "date": game_date,
                                    "actual": actual_f,
                                    "fg": fg_v,
                                    "line": r.get("line"),
                                    "hit_was": hit,
                                }
                            )
                        continue
                    if actual_f > fg_v + 1e-9:
                        a["nba1q_actual_gt_fg"] += 1
                        a["drop_reasons"]["nba1q_actual_gt_full_game"] += 1
                        continue
                    a["nba1q_fg_mismatch"] += 1  # actual < FG → plausible 1Q

            # NHL leakage recompute (L5 before game)
            if sport == "NHL" and gate == "l5eq5_l10ge8":
                line = _num(r.get("line"))
                col = "points" if pr == "points" else "assists"
                games = nhl.games(str(r.get("player") or ""))
                if line is not None and games:
                    recomputed = _leakage_l5(
                        games,
                        cutoff=game_date,
                        col=col,
                        line=line,
                        direction="UNDER",
                    )
                    if recomputed is None:
                        a["drop_reasons"]["leakage_unchecked"] += 1
                    elif recomputed != 5:
                        a["drop_reasons"]["leakage_fail"] += 1
                        if len(a["samples_dropped"]) < 15:
                            a["samples_dropped"].append(
                                {
                                    "reason": "leakage_fail",
                                    "player": r.get("player"),
                                    "date": game_date,
                                    "stored_l5": l5,
                                    "recomputed_l5": recomputed,
                                    "line": line,
                                    "hit_was": hit,
                                }
                            )
                        continue
                    else:
                        a["drop_reasons"]["leakage_pass"] += 1
                else:
                    a["drop_reasons"]["leakage_unchecked"] += 1

                # Scratch / TOI==0 with actual 0 already handled by DNP.
                # Also void if box TOI==0 on game date even when actual missing path.
                toi_void = False
                for gd, vals in games:
                    if gd == game_date and vals.get("toi") == 0:
                        toi_void = True
                        break
                if toi_void and (actual_f is None or actual_f == 0):
                    a["drop_reasons"]["nhl_toi_zero_void"] += 1
                    continue

            a["after"]["n"] += 1
            a["after"]["hits"] += int(hit)
            if hit == 0 and len(a["samples_kept_miss"]) < 8:
                a["samples_kept_miss"].append(
                    {
                        "player": r.get("player"),
                        "date": game_date,
                        "line": r.get("line"),
                        "actual": actual_f,
                    }
                )

    for key, a in acc.items():
        after = _ci(int(a["after"]["hits"]), int(a["after"]["n"]))
        before = a["catalog_before"]
        results[key] = {
            **{k: v for k, v in a.items() if k != "after"},
            "after": after,
            "delta_hit_rate": (
                round(after["hit_rate"] - before["hit_rate"], 4)
                if after["hit_rate"] is not None
                and before
                and before.get("hit_rate") is not None
                else None
            ),
            "drop_reasons": dict(a["drop_reasons"]),
        }
    return results


def explain_bugs(catalog_rows: list[dict]) -> dict[str, Any]:
    """Diagnose NFL identical gates + NHL FOW + Soccer passes."""
    out: dict[str, Any] = {"nfl": {}, "nhl_fow": {}, "soccer": {}}

    # --- NFL: sample graded rows for L5/L10 presence ---
    nfl_stats: dict[str, Any] = defaultdict(lambda: Counter())
    nfl_examples: list[dict] = []
    for file_date, chunk in _iter_graded():
        for r in chunk:
            if _norm_sport(r.get("sport")) != "NFL":
                continue
            if _book(r) != "Standard UNDER":
                continue
            prop = _prop_key(r.get("prop"))
            if prop not in {
                "receiving yards",
                "receptions",
                "rushing yards",
                "sacks",
                "longest reception",
                "pass completions",
                "passing completions",
            }:
                continue
            side = "UNDER"
            l5 = l5_dir(r, side)
            l10 = l10_dir(r, side)
            key = prop
            nfl_stats[key]["n"] += 1
            if l5 is None:
                nfl_stats[key]["l5_missing"] += 1
            else:
                nfl_stats[key]["l5_present"] += 1
                nfl_stats[key][f"l5_{int(l5)}"] += 1
            if l10 is None:
                nfl_stats[key]["l10_missing"] += 1
            else:
                nfl_stats[key]["l10_present"] += 1
            actual = _actual(r)
            hit = _parse_hit(str(r.get("result") or ""))
            if actual == 0 and hit == 1:
                nfl_stats[key]["under_hit_actual0"] += 1
            if actual == 0:
                nfl_stats[key]["actual0"] += 1
            if len(nfl_examples) < 12 and prop == "receiving yards":
                nfl_examples.append(
                    {
                        "date": file_date,
                        "player": r.get("player"),
                        "line": r.get("line"),
                        "actual": actual,
                        "result": r.get("result"),
                        "l5_under": r.get("l5_under") or r.get("last5_under") or r.get("L5 Under"),
                        "l10_under": r.get("l10_under") or r.get("last10_under"),
                        "l5_over": r.get("l5_over"),
                        "minutes_or_snaps": r.get("snaps") or r.get("snap_count") or r.get("minutes"),
                    }
                )
    out["nfl"] = {
        "diagnosis": (
            "When l5_under/l10_under are missing on graded rows, every gate column "
            "in all_ranked_prop_hit_rates falls back to the same decided pool "
            "(ungated == listed == L5=5 == L10>=8). Identical counts across columns "
            "mean the gate fields were null, not that every row truly cleared L5=5. "
            "Separately, UNDER hits with actual=0 without positive DNP evidence "
            "inflate rates (strict void would remove only evidenced DNPs)."
        ),
        "by_prop": {k: dict(v) for k, v in nfl_stats.items()},
        "examples": nfl_examples,
        "catalog": {},
    }
    for sport, book, prop in EXPLAIN:
        if sport != "NFL":
            continue
        cat = _catalog_row(catalog_rows, sport, book, prop)
        if cat:
            out["nfl"]["catalog"][prop] = {
                "ungated": f"{cat['hits']}/{cat['n']}",
                "listed": f"{cat.get('listed_hits')}/{cat.get('listed_n')}",
                "l5eq5": f"{cat.get('l5eq5_hits')}/{cat.get('l5eq5_n')}",
                "l10ge8": f"{cat.get('l10ge8_hits')}/{cat.get('l10ge8_n')}",
                "both": f"{cat.get('l5eq5_l10ge8_hits')}/{cat.get('l5eq5_l10ge8_n')}",
                "identical_gates": (
                    cat.get("n")
                    == cat.get("listed_n")
                    == cat.get("l5eq5_n")
                    == cat.get("l10ge8_n")
                    == cat.get("l5eq5_l10ge8_n")
                ),
            }

    # --- NHL Faceoffs Won ---
    fow_rows = []
    fow_actual = Counter()
    for file_date, chunk in _iter_graded():
        for r in chunk:
            if _norm_sport(r.get("sport")) != "NHL":
                continue
            if _book(r) != "Standard OVER":
                continue
            if "faceoff" not in _prop_key(r.get("prop")):
                continue
            actual = _actual(r)
            hit = _parse_hit(str(r.get("result") or ""))
            line = _num(r.get("line"))
            fow_actual[str(actual)] += 1
            if len(fow_rows) < 20:
                fow_rows.append(
                    {
                        "date": file_date,
                        "player": r.get("player"),
                        "line": line,
                        "actual": actual,
                        "result": r.get("result"),
                        "hit": hit,
                    }
                )
    cat_fow = _catalog_row(catalog_rows, "NHL", "Standard OVER", "faceoffs won")
    out["nhl_fow"] = {
        "diagnosis": (
            "98.4% on Faceoffs Won OVER is a stat/line mismatch or missing-actual "
            "path that still marks HIT. Inspect actual distribution — if actuals "
            "cluster at null/0 while lines are fractional, grading is wrong."
        ),
        "catalog": _ci(int(cat_fow["hits"]), int(cat_fow["n"])) if cat_fow else None,
        "actual_value_counts_top": fow_actual.most_common(15),
        "examples": fow_rows,
        "n_graded_rows_seen": sum(fow_actual.values()),
    }

    # --- Soccer Passes Attempted ---
    soc_rows = []
    soc_actual = Counter()
    soc_hit = Counter()
    for file_date, chunk in _iter_graded():
        for r in chunk:
            if _norm_sport(r.get("sport")) != "SOCCER":
                continue
            if _book(r) != "Standard OVER":
                continue
            prop = _prop_key(r.get("prop"))
            if prop not in {"passes attempted", "clearances"}:
                continue
            actual = _actual(r)
            hit = _parse_hit(str(r.get("result") or ""))
            soc_actual[(prop, str(actual))] += 1
            soc_hit[(prop, hit)] += 1
            if len(soc_rows) < 15:
                soc_rows.append(
                    {
                        "date": file_date,
                        "prop": prop,
                        "player": r.get("player"),
                        "line": r.get("line"),
                        "actual": actual,
                        "result": r.get("result"),
                    }
                )
    out["soccer"] = {
        "diagnosis": (
            "0% / ~3% on Passes Attempted / Clearances OVER means the grader "
            "almost never finds a matching box column — actual stays missing/0 "
            "while the line is large, so OVER always misses. Not a real skill cell."
        ),
        "actual_counts": {f"{a}|{b}": c for (a, b), c in soc_actual.most_common(20)},
        "hit_counts": {f"{a}|{b}": c for (a, b), c in soc_hit.items()},
        "examples": soc_rows,
        "catalog": {
            prop: _catalog_row(catalog_rows, "Soccer", "Standard OVER", prop)
            for prop in ("passes attempted", "clearances")
        },
    }
    return out


def _fmt_ci(c: dict | None) -> str:
    if not c or c.get("n") is None:
        return "—"
    if not c.get("n"):
        return "n=0"
    return (
        f"{100 * c['hit_rate']:.1f}% ({c['hits']}/{c['n']}) "
        f"Wilson[{100 * c['wilson_low']:.1f}–{100 * c['wilson_high']:.1f}]"
    )


def write_md(payload: dict[str, Any]) -> None:
    lines: list[str] = []
    lines.append("# Catalog suspect-cell audit")
    lines.append("")
    lines.append(
        "Re-grade with strict DNP-zero voids (positive evidence only), "
        "leakage-pass where boxes allow, pick_type twin checks, and NBA1Q "
        "full-game contamination drop (actual == full-game box)."
    )
    lines.append("")
    lines.append("## Re-graded cells (before → after)")
    lines.append("")
    lines.append("| Cell | Gate | Catalog before | After filters | Δ | Notes |")
    lines.append("|---|---|---|---|---|---|")
    for key, r in payload["regrade"].items():
        drops = r.get("drop_reasons") or {}
        note_bits = []
        if r["sport"] == "NBA1Q":
            note_bits.append(
                f"FG-match drops={drops.get('nba1q_actual_equals_full_game', 0)}; "
                f"plausible 1Q={r.get('nba1q_fg_mismatch', 0)}; "
                f"unchecked={r.get('nba1q_fg_unchecked', 0)}"
            )
        if r["sport"] == "NHL":
            note_bits.append(
                f"leak fail={drops.get('leakage_fail', 0)}; "
                f"DNP void={drops.get('strict_dnp_void', 0)}; "
                f"toi0={drops.get('nhl_toi_zero_void', 0)}"
            )
        twin = sum(v for k, v in drops.items() if k.startswith("pick_twin"))
        if twin:
            note_bits.append(f"twin drops={twin}")
        lines.append(
            f"| {r['sport']} {r['book']} {r['prop']} | {r['gate']} | "
            f"{_fmt_ci(r.get('catalog_before'))} | {_fmt_ci(r.get('after'))} | "
            f"{r.get('delta_hit_rate')} | {'; '.join(note_bits) or '—'} |"
        )
    lines.append("")
    lines.append("## Bug explanations")
    lines.append("")
    lines.append("### NFL Standard UNDER identical gate columns")
    lines.append(payload["explain"]["nfl"]["diagnosis"])
    lines.append("")
    lines.append("```")
    lines.append(json.dumps(payload["explain"]["nfl"]["catalog"], indent=2))
    lines.append("```")
    lines.append("")
    lines.append("Per-prop L5 presence / actual=0 UNDER hits:")
    lines.append("```")
    lines.append(json.dumps(payload["explain"]["nfl"]["by_prop"], indent=2))
    lines.append("```")
    lines.append("")
    lines.append("### NHL Standard OVER Faceoffs Won 98.4%")
    lines.append(payload["explain"]["nhl_fow"]["diagnosis"])
    lines.append(
        f"Catalog: {_fmt_ci(payload['explain']['nhl_fow'].get('catalog'))}; "
        f"rows seen={payload['explain']['nhl_fow'].get('n_graded_rows_seen')}"
    )
    lines.append(
        f"Actual value counts: {payload['explain']['nhl_fow'].get('actual_value_counts_top')}"
    )
    lines.append("")
    lines.append("### Soccer Passes Attempted / Clearances ~0%")
    lines.append(payload["explain"]["soccer"]["diagnosis"])
    lines.append(
        f"Hit counts: {payload['explain']['soccer'].get('hit_counts')}"
    )
    lines.append("")
    lines.append("## Verdict")
    lines.append("")
    lines.append(
        "- Treat catalog NBA1Q Goblin 85–93% as **untrusted until after FG-match "
        "drops**; if after-rate Wilson low still clears ~84.5%, it is product-grade."
    )
    lines.append(
        "- NHL Std UNDER Points/Assists: use **after** cell only (DNP + leakage)."
    )
    lines.append(
        "- NFL Std UNDER 97–100% and identical gate columns: **do not use**."
    )
    lines.append(
        "- NHL FOW 98% and Soccer Passes/Clearances 0%: **grading bugs**, not edges."
    )
    lines.append("")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    catalog = json.loads(CATALOG.read_text(encoding="utf-8"))
    rows = catalog.get("rows") or []
    print("Re-grading targets…")
    regrade = regrade_targets(rows)
    print("Explaining bug cells…")
    explain = explain_bugs(rows)
    payload = {
        "source_catalog": str(CATALOG),
        "catalog_window": {"from": catalog.get("from"), "to": catalog.get("to")},
        "filters": [
            "strict_dnp_zero_void (positive evidence only)",
            "pick_type twin vs Goblin lines on same graded file",
            "grading_suspect rows dropped",
            "NBA1Q: drop when actual equals full-game box pts/reb/ast",
            "NHL: leakage L5 recompute must be 5; TOI==0 void",
        ],
        "regrade": regrade,
        "explain": explain,
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    write_md(payload)
    print(f"wrote {OUT_JSON}")
    print(f"wrote {OUT_MD}")
    for key, r in regrade.items():
        print(
            f"  {key}: before={_fmt_ci(r.get('catalog_before'))} -> "
            f"after={_fmt_ci(r.get('after'))} delta={r.get('delta_hit_rate')}"
        )
        print(f"    drops={dict(list(r.get('drop_reasons', {}).items())[:8])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
