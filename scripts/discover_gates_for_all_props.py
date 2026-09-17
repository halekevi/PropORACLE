#!/usr/bin/env python3
"""Systematic gate discovery over graded MLB props with rich board features.

Joins dated step8 boards (L5/L10/cover/season cushion) plus slash BA/K% and
team pitch/bat tiers so the scan is not limited to coarse training columns
(edge / ml_prob / line_score).

Hard rules:
  - Never treat outcome-derived fields as candidate features
    (actual_value, margin, hit, result, and aliases).
  - Chronological OOS split (last 30% of dates held out).
  - Per-prop zero-actual / implausible-value sanity flags.

  py -3.14 scripts/discover_gates_for_all_props.py
  py -3.14 scripts/discover_gates_for_all_props.py --sport MLB --min-n 25
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

import all_ranked_prop_hit_rates as C  # noqa: E402
import mlb_counting_gate_grid as G  # noqa: E402
import prop_hit_tiers as T  # noqa: E402
from utils.defense_tiers import normalize_def_tier_label  # noqa: E402
from utils.mlb_keep_gates import (  # noqa: E402
    BA_FLOOR,
    K_RATE_FLOOR,
    PROD,
    STINGY,
    STRONG_BAT,
    player_slash,
)
from utils.mlb_prop_matchup import batting_strength_label  # noqa: E402

OUT_DIR = _REPO / "data" / "reports"
OUT_JSON = OUT_DIR / "discover_gates_mlb_ranked.json"
OUT_CSV = OUT_DIR / "discover_gates_mlb_ranked.csv"
OUT_EXPORT = OUT_DIR / "discover_gates_mlb_export.csv"
OUT_SANITY = OUT_DIR / "discover_gates_mlb_sanity.json"

# Outcome / label leakage — never candidate features.
LEAKAGE_COLS = frozenset(
    {
        "actual_value",
        "actual",
        "actuals",
        "margin",
        "hit",
        "result",
        "won",
        "is_hit",
        "grade",
        "graded_result",
        "outcome",
        "y",
        "label",
        "target",
        "push",
        "void_reason",
        # post-game derived that encode the same signal
        "actual_source",
        "actual_source_conflict",
    }
)

BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER")
TICKET_HR = 0.70
TICKET_N = 40
SHOW_N = 15
OOS_FRAC = 0.30

STEP8_COLS = {
    "player",
    "player_name",
    "prop_type",
    "prop_norm",
    "pick_type",
    "line",
    "bet_direction",
    "final_bet_direction",
    "direction",
    "team",
    "opp_team",
    "last5_over",
    "last5_under",
    "l10_over",
    "l10_under",
    "line_hits_over_5",
    "line_hits_under_5",
    "line_hits_over_10",
    "line_hits_under_10",
    "stat_last5_avg",
    "stat_last10_avg",
    "stat_season_avg",
    "avg_vs_line",
    "avg_L5_vs_line",
    "avg_L10_vs_line",
    "avg_season_vs_line",
    "def_tier",
    "DEF_TIER",
    "player_hr_historical",
    "hit_rate",
    "line_hit_rate",
    "mlb_player_id",
}


def _num(v: object) -> float | None:
    return C.num(v)


def _norm_player(s: object) -> str:
    return G._norm_player(s)


def _pick(raw: object) -> str:
    return G._pick(raw)


def _side(raw: object) -> str | None:
    return G._side(raw)


def _abbr(raw: object) -> str:
    s = str(raw or "").strip().upper()
    alias = {"ARI": "AZ", "CHW": "CWS", "OAK": "ATH", "WSN": "WSH", "WAS": "WSH"}
    return alias.get(s, s)


def load_def() -> dict[str, dict[str, str]]:
    path = _REPO / "Sports" / "MLB" / "mlb_defense_summary.csv"
    if not path.is_file():
        return {}
    d = pd.read_csv(path)
    d["TEAM_ABBREVIATION"] = d["TEAM_ABBREVIATION"].astype(str).str.strip().str.upper()
    out: dict[str, dict[str, str]] = {}
    for r in d.to_dict("records"):
        rec = {
            "pitch": normalize_def_tier_label(r.get("DEF_TIER") or r.get("def_tier")) or "",
            "hits": normalize_def_tier_label(r.get("OFF_HITS_TIER")) or "",
            "runs": normalize_def_tier_label(r.get("OFF_RUNS_TIER")) or "",
            "so": normalize_def_tier_label(r.get("OFF_SO_TIER")) or "",
        }
        ab = str(r["TEAM_ABBREVIATION"]).upper()
        out[ab] = rec
        for a, b in (("ARI", "AZ"), ("CHW", "CWS"), ("OAK", "ATH"), ("WSN", "WSH"), ("WAS", "WSH")):
            if ab == b:
                out[a] = rec
            if ab == a:
                out[b] = rec
    return out


def find_step8(date: str) -> Path | None:
    return G.find_step8_csv(date)


def load_step8_index(path: Path) -> dict[tuple, dict]:
    try:
        df = pd.read_csv(path, usecols=lambda c: c in STEP8_COLS, low_memory=False)
    except Exception:
        df = pd.read_csv(path, low_memory=False)
    out: dict[tuple, dict] = {}
    for row in df.to_dict("records"):
        prop = T.canon_prop("MLB", str(row.get("prop_type") or row.get("prop_norm") or ""))
        if not prop:
            continue
        side = _side(row.get("bet_direction") or row.get("final_bet_direction") or row.get("direction"))
        if side is None:
            continue
        line = _num(row.get("line"))
        key = (
            _norm_player(row.get("player") or row.get("player_name")),
            prop,
            _pick(row.get("pick_type")),
            line,
            side,
        )
        cover = _num(row.get("avg_L5_vs_line"))
        if cover is None:
            cover = _num(row.get("avg_vs_line"))
        season_cush = _num(row.get("avg_season_vs_line"))
        rec = {
            "l5_over": _num(row.get("last5_over") if row.get("last5_over") not in (None, "") else row.get("line_hits_over_5")),
            "l5_under": _num(row.get("last5_under") if row.get("last5_under") not in (None, "") else row.get("line_hits_under_5")),
            "l10_over": _num(row.get("l10_over") if row.get("l10_over") not in (None, "") else row.get("line_hits_over_10")),
            "l10_under": _num(row.get("l10_under") if row.get("l10_under") not in (None, "") else row.get("line_hits_under_10")),
            "def_tier": row.get("def_tier") or row.get("DEF_TIER") or "",
            "l5_avg": _num(row.get("stat_last5_avg")),
            "l10_avg": _num(row.get("stat_last10_avg")),
            "season_avg": _num(row.get("stat_season_avg")),
            "cover_signed": cover,
            "season_cushion": season_cush,
            "season_hr": _num(row.get("player_hr_historical") or row.get("hit_rate") or row.get("line_hit_rate")),
            "team": row.get("team") or "",
            "opp_team": row.get("opp_team") or "",
            "mlb_player_id": str(row.get("mlb_player_id") or "").strip(),
        }
        prev = out.get(key)
        if prev is None:
            out[key] = rec
        else:
            G._fill(prev, rec, tuple(rec.keys()))
    return out


def graded_by_date() -> dict[str, Path]:
    by: dict[str, Path] = {}
    for d in (
        MAIN / "ui_runner" / "templates",
        _REPO / "ui_runner" / "templates",
    ):
        if not d.is_dir():
            continue
        for p in sorted(d.glob("graded_props_20*.json")):
            if ".bak_" in p.name:
                continue
            date = p.stem.replace("graded_props_", "")[:10]
            if date not in by:
                by[date] = p
    return by


def iter_mlb_rows(defs: dict[str, dict[str, str]]) -> list[dict]:
    by_date = graded_by_date()
    rows: list[dict] = []
    for date in sorted(by_date):
        path = by_date[date]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        merged: dict[tuple, dict] = {}
        for r in raw.get("props") or []:
            if not isinstance(r, dict):
                continue
            if T.norm_sport(str(r.get("sport") or "")) != "MLB":
                continue
            res = str(r.get("result") or "").strip().upper()
            if res not in ("HIT", "MISS"):
                continue
            prop = T.canon_prop("MLB", str(r.get("prop") or r.get("prop_type") or ""))
            if not prop:
                continue
            side = _side(r.get("direction") or r.get("over_under"))
            if side is None:
                continue
            book = C.pick_book(str(r.get("pick_type") or ""), side)
            if book not in BOOKS:
                continue
            # Demon / other already excluded by pick_book → Standard|Goblin only
            line = _num(r.get("line"))
            key = (
                _norm_player(r.get("player")),
                prop,
                _pick(r.get("pick_type")),
                line,
                side,
            )
            hit = 1 if res == "HIT" else 0
            actual = _num(r.get("actual_value"))
            if key not in merged:
                merged[key] = {
                    "date": date,
                    "player": r.get("player"),
                    "team": r.get("team") or "",
                    "opp_team": r.get("opp_team") or "",
                    "prop": prop,
                    "book": book,
                    "side": side,
                    "line": line,
                    "hit": hit,
                    "actual_value": actual,  # sanity only — never a feature
                    "l5_over": r.get("l5_over") or r.get("last5_over"),
                    "l5_under": r.get("l5_under") or r.get("last5_under"),
                    "l10_over": r.get("l10_over") or r.get("last10_over"),
                    "l10_under": r.get("l10_under") or r.get("last10_under"),
                    "def_tier": r.get("def_tier") or r.get("def") or "",
                    "cover": r.get("cover") or r.get("dist_l5"),
                    "player_hr_historical": r.get("player_hr_historical"),
                    "l5_avg": r.get("l5_avg") or r.get("stat_last5_avg"),
                    "mlb_player_id": str(r.get("mlb_player_id") or r.get("player_id") or "").strip(),
                    "edge": _num(r.get("edge")),
                    "ml_prob": _num(r.get("ml_prob")),
                    "line_score": _num(r.get("line_score") or r.get("line")),
                }
            else:
                G.merge_grade(merged[key], r)
                if merged[key].get("actual_value") is None and actual is not None:
                    merged[key]["actual_value"] = actual
                merged[key]["hit"] = hit

        step_path = find_step8(date)
        step_idx = load_step8_index(step_path) if step_path else {}
        for key, rec in merged.items():
            s8 = step_idx.get(key)
            if s8:
                for fld in (
                    "l5_over",
                    "l5_under",
                    "l10_over",
                    "l10_under",
                    "l5_avg",
                    "l10_avg",
                    "season_avg",
                    "season_cushion",
                ):
                    if rec.get(fld) in (None, "") and s8.get(fld) not in (None, ""):
                        rec[fld] = s8.get(fld)
                if not str(rec.get("def_tier") or "").strip():
                    rec["def_tier"] = s8.get("def_tier") or ""
                if rec.get("cover") in (None, "") and s8.get("cover_signed") is not None:
                    rec["cover"] = s8["cover_signed"]
                if rec.get("player_hr_historical") in (None, "") and s8.get("season_hr") is not None:
                    rec["player_hr_historical"] = s8["season_hr"]
                if not str(rec.get("team") or "").strip() and s8.get("team"):
                    rec["team"] = s8["team"]
                if not str(rec.get("opp_team") or "").strip() and s8.get("opp_team"):
                    rec["opp_team"] = s8["opp_team"]
                if not rec.get("mlb_player_id") and s8.get("mlb_player_id"):
                    rec["mlb_player_id"] = s8["mlb_player_id"]

            team = _abbr(rec.get("team"))
            opp = _abbr(rec.get("opp_team"))
            own = defs.get(team) or {}
            opp_d = defs.get(opp) or {}
            rec["own_pitch_tier"] = own.get("pitch") or ""
            rec["opp_pitch_tier"] = opp_d.get("pitch") or ""
            rec["own_bats_tier"] = batting_strength_label(own.get("hits") or "") or ""
            rec["opp_bats_tier"] = batting_strength_label(opp_d.get("hits") or "") or ""
            # keep raw hits-tier labels for leaky/stingy own-bat checks too
            rec["own_hits_tier_raw"] = own.get("hits") or ""
            rec["opp_hits_tier_raw"] = opp_d.get("hits") or ""

            slash = player_slash(
                {
                    "player": rec.get("player"),
                    "mlb_player_id": rec.get("mlb_player_id"),
                    "ba": None,
                    "k_rate": None,
                }
            )
            rec["ba"] = slash.get("avg")
            rec["k_rate"] = slash.get("k_rate")
            rec["ab"] = slash.get("ab")
            rows.append(rec)
    return rows


def directional_cover(rec: dict) -> float | None:
    side = rec["side"]
    cover = _num(rec.get("cover"))
    line = rec.get("line")
    if cover is None:
        avg = _num(rec.get("l5_avg"))
        if avg is not None and line is not None:
            cover = (avg - line) if side == "OVER" else (line - avg)
    elif side == "UNDER" and cover is not None:
        # step8 avg_L5_vs_line is signed avg-line; flip for UNDER cushion
        cover = -cover
    return cover


def build_atoms(rec: dict) -> dict[str, bool]:
    """Boolean candidate atoms. No leakage cols."""
    side = rec["side"]
    l5 = C.l5_dir(rec, side)
    l10 = C.l10_dir(rec, side)
    cover = directional_cover(rec)
    season_cush = _num(rec.get("season_cushion"))
    if season_cush is not None and side == "UNDER":
        season_cush = -season_cush
    if season_cush is None:
        savg = _num(rec.get("season_avg"))
        line = rec.get("line")
        if savg is not None and line is not None:
            season_cush = (savg - line) if side == "OVER" else (line - savg)
    season_hr = _num(rec.get("player_hr_historical"))
    if season_hr is not None and season_hr > 1.5:
        season_hr = season_hr / 100.0

    ba = _num(rec.get("ba"))
    kr = _num(rec.get("k_rate"))
    ab = _num(rec.get("ab"))
    ab_ok = ab is None or ab >= 80

    own_p = normalize_def_tier_label(rec.get("own_pitch_tier")) or ""
    opp_p = normalize_def_tier_label(rec.get("opp_pitch_tier")) or ""
    own_b = str(rec.get("own_bats_tier") or "")
    opp_b = str(rec.get("opp_bats_tier") or "")
    own_hits_raw = normalize_def_tier_label(rec.get("own_hits_tier_raw")) or ""
    opp_hits_raw = normalize_def_tier_label(rec.get("opp_hits_tier_raw")) or ""

    line = rec.get("line")
    line_05 = line is not None and abs(line - 0.5) < 1e-9
    line_15 = line is not None and abs(line - 1.5) < 1e-9
    line_25p = line is not None and line >= 2.5

    atoms = {
        "L5>=4": l5 is not None and l5 >= 4,
        "L5=5": l5 is not None and l5 == 5,
        "L10>=8": l10 is not None and l10 >= 8,
        "L10>=9": l10 is not None and l10 >= 9,
        "L10=10": l10 is not None and l10 == 10,
        "own_pitch_leaky": own_p in PROD,
        "own_pitch_stingy": own_p in STINGY,
        "opp_pitch_leaky": opp_p in PROD,
        "opp_pitch_stingy": opp_p in STINGY,
        "opp_bats_Strong": opp_b in STRONG_BAT or opp_hits_raw in PROD,
        "own_bats_Strong": own_b in STRONG_BAT or own_hits_raw in PROD,
        "BA>=.275": ab_ok and ba is not None and ba >= BA_FLOOR,
        "BA>=.300": ab_ok and ba is not None and ba >= 0.300,
        "K%>=28": ab_ok and kr is not None and kr >= K_RATE_FLOOR,
        "K%>=22": ab_ok and kr is not None and kr >= 0.22,
        "cover>=0.5": cover is not None and cover >= 0.5,
        "cover>=1": cover is not None and cover >= 1.0,
        "cover>=1.5": cover is not None and cover >= 1.5,
        "cover>=2": cover is not None and cover >= 2.0,
        "season_cush>=1": season_cush is not None and season_cush >= 1.0,
        "season_cush>=2": season_cush is not None and season_cush >= 2.0,
        "season_HR>70%": season_hr is not None and season_hr > 0.70,
        "line_0.5": line_05,
        "line_1.5": line_15,
        "line_2.5+": line_25p,
    }
    return atoms


# Curated combo families (avoid full 2^n explosion).
RECENCY = ("L5>=4", "L5=5", "L10>=8", "L10>=9", "L10=10")
TIERS = (
    "own_pitch_leaky",
    "own_pitch_stingy",
    "opp_pitch_leaky",
    "opp_pitch_stingy",
    "opp_bats_Strong",
    "own_bats_Strong",
)
HITTER = ("BA>=.275", "BA>=.300", "K%>=28", "K%>=22")
CUSHION = ("cover>=0.5", "cover>=1", "cover>=1.5", "cover>=2", "season_cush>=1", "season_cush>=2", "season_HR>70%")
LINES = ("line_0.5", "line_1.5", "line_2.5+")


def gate_catalog() -> list[tuple[str, tuple[str, ...]]]:
    """Named gates as conjunctions of atom names."""
    gates: list[tuple[str, tuple[str, ...]]] = [("all", ())]
    atoms_all = RECENCY + TIERS + HITTER + CUSHION + LINES
    for a in atoms_all:
        gates.append((a, (a,)))

    # Recency × tier / hitter / cushion (the keep-gate shapes)
    for r in RECENCY:
        for t in TIERS + HITTER + CUSHION:
            gates.append((f"{r} + {t}", (r, t)))

    # Known keep-style triples
    triples = [
        ("L10>=8 + own_pitch_leaky", ("L10>=8", "own_pitch_leaky")),
        ("L10>=8 + own_pitch_stingy", ("L10>=8", "own_pitch_stingy")),
        ("L10>=8 + opp_bats_Strong", ("L10>=8", "opp_bats_Strong")),
        ("L5=5 + BA>=.275 + opp_pitch_leaky", ("L5=5", "BA>=.275", "opp_pitch_leaky")),
        ("L10>=8 + K%>=28 + opp_pitch_stingy", ("L10>=8", "K%>=28", "opp_pitch_stingy")),
        ("L5=5 + L10>=8 + own_pitch_leaky", ("L5=5", "L10>=8", "own_pitch_leaky")),
        ("L5=5 + L10>=8 + own_pitch_stingy", ("L5=5", "L10>=8", "own_pitch_stingy")),
        ("L5>=4 + L10>=8 + own_pitch_leaky", ("L5>=4", "L10>=8", "own_pitch_leaky")),
        ("L5=5 + L10>=8", ("L5=5", "L10>=8")),
        ("L5>=4 + L10>=8", ("L5>=4", "L10>=8")),
        ("L10>=8 + cover>=1", ("L10>=8", "cover>=1")),
        ("L5=5 + cover>=1", ("L5=5", "cover>=1")),
        ("L10>=8 + season_cush>=1", ("L10>=8", "season_cush>=1")),
        ("BA>=.275 + opp_pitch_leaky", ("BA>=.275", "opp_pitch_leaky")),
        ("K%>=28 + opp_pitch_stingy", ("K%>=28", "opp_pitch_stingy")),
        ("L5=5 + line_0.5", ("L5=5", "line_0.5")),
        ("L10>=8 + line_0.5", ("L10>=8", "line_0.5")),
        ("L5=5 + L10>=8 + line_0.5", ("L5=5", "L10>=8", "line_0.5")),
    ]
    seen = {g[0] for g in gates}
    for name, atoms in triples:
        if name not in seen:
            gates.append((name, atoms))
            seen.add(name)

    # Extra: recency × tier × cushion (selected)
    for r in ("L10>=8", "L5=5"):
        for t in TIERS:
            for c in ("cover>=1", "season_cush>=1"):
                name = f"{r} + {t} + {c}"
                if name not in seen:
                    gates.append((name, (r, t, c)))
                    seen.add(name)
        for h in HITTER:
            for t in ("opp_pitch_leaky", "opp_pitch_stingy"):
                name = f"{r} + {h} + {t}"
                if name not in seen:
                    gates.append((name, (r, h, t)))
                    seen.add(name)
    return gates


def passes(atoms: dict[str, bool], needed: tuple[str, ...]) -> bool:
    return all(atoms.get(a, False) for a in needed)


def rate(h: int, n: int) -> float | None:
    if n <= 0:
        return None
    return round(h / n, 4)


def wilson_low(h: int, n: int, z: float = 1.96) -> float | None:
    if n <= 0:
        return None
    p = h / n
    z2 = z * z
    denom = 1 + z2 / n
    center = p + z2 / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z2 / (4 * n)) / n)
    return round((center - spread) / denom, 4)


def sanity_report(rows: list[dict]) -> dict:
    """Zero-actual / implausible-value flags per book×prop (label audit only)."""
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        buckets[(r["book"], r["prop"])].append(r)

    out_cells = []
    flags = []
    for (book, prop), pool in sorted(buckets.items()):
        actuals = [r["actual_value"] for r in pool if r.get("actual_value") is not None]
        n = len(pool)
        n_act = len(actuals)
        zeros = sum(1 for a in actuals if a == 0)
        zero_hit = 0
        zero_n = 0
        for r in pool:
            a = r.get("actual_value")
            if a is None:
                continue
            if a == 0:
                zero_n += 1
                if r["hit"] == 1:
                    zero_hit += 1
        mean_a = (sum(actuals) / n_act) if n_act else None
        lines = [_num(r.get("line")) for r in pool if _num(r.get("line")) is not None]
        mean_line = (sum(lines) / len(lines)) if lines else None
        # implausible: OVER lines where mean actual << mean line (e.g. passes-style)
        ratio = None
        if mean_a is not None and mean_line is not None and mean_line > 0:
            ratio = round(mean_a / mean_line, 3)
        zero_rate = (zeros / n_act) if n_act else None
        cell = {
            "book": book,
            "prop": prop,
            "n": n,
            "n_with_actual": n_act,
            "zero_n": zeros,
            "zero_rate": None if zero_rate is None else round(zero_rate, 4),
            "zero_as_hit": zero_hit,
            "zero_hit_rate": rate(zero_hit, zero_n),
            "mean_actual": None if mean_a is None else round(mean_a, 3),
            "mean_line": None if mean_line is None else round(mean_line, 3),
            "actual_over_line": ratio,
            "hr_all": rate(sum(r["hit"] for r in pool), n),
        }
        reasons = []
        if zero_rate is not None and zero_rate >= 0.20 and "UNDER" in book:
            reasons.append(f"zero_actual_under_inflation zero_rate={zero_rate:.1%}")
        if zero_rate is not None and zero_rate >= 0.35:
            reasons.append(f"high_zero_actuals zero_rate={zero_rate:.1%}")
        if (
            ratio is not None
            and "OVER" in book
            and mean_line is not None
            and mean_line >= 3
            and ratio < 0.35
        ):
            reasons.append(f"actual_<<_line ratio={ratio}")
        if reasons:
            cell["flags"] = reasons
            flags.append(cell)
        out_cells.append(cell)

    flags.sort(key=lambda c: (-(c.get("zero_rate") or 0), -c["n"]))
    return {"cells": out_cells, "flagged": flags}


def oos_split_dates(dates: list[str]) -> tuple[set[str], set[str]]:
    dates = sorted(dates)
    if len(dates) < 5:
        return set(dates), set(dates)
    cut = max(1, int(round(len(dates) * (1.0 - OOS_FRAC))))
    return set(dates[:cut]), set(dates[cut:])


def eval_pool(
    pool: list[dict],
    gates: list[tuple[str, tuple[str, ...]]],
    train_dates: set[str],
    test_dates: set[str],
    min_n: int,
) -> list[dict]:
    # Precompute atoms once
    for r in pool:
        r["_atoms"] = build_atoms(r)

    base_n = len(pool)
    base_h = sum(r["hit"] for r in pool)
    base_hr = rate(base_h, base_n)

    results = []
    for name, needed in gates:
        n = h = 0
        n_tr = h_tr = 0
        n_te = h_te = 0
        for r in pool:
            if not passes(r["_atoms"], needed):
                continue
            n += 1
            h += r["hit"]
            if r["date"] in train_dates:
                n_tr += 1
                h_tr += r["hit"]
            if r["date"] in test_dates:
                n_te += 1
                h_te += r["hit"]
        if n < min_n:
            continue
        hr = rate(h, n)
        lift = None if base_hr is None or hr is None else round(hr - base_hr, 4)
        results.append(
            {
                "gate": name,
                "atoms": list(needed) if needed else ["all"],
                "n": n,
                "hits": h,
                "hr": hr,
                "wilson_low": wilson_low(h, n),
                "lift_vs_base": lift,
                "base_hr": base_hr,
                "base_n": base_n,
                "train_n": n_tr,
                "train_hr": rate(h_tr, n_tr),
                "oos_n": n_te,
                "oos_hr": rate(h_te, n_te),
                "ticket_clear": bool(
                    hr is not None and hr >= TICKET_HR and n >= TICKET_N
                ),
                "oos_ticket_clear": bool(
                    rate(h_te, n_te) is not None
                    and rate(h_te, n_te) >= TICKET_HR
                    and n_te >= max(15, TICKET_N // 3)
                ),
            }
        )
    results.sort(
        key=lambda g: (
            -int(g["ticket_clear"]),
            -(g["hr"] or 0),
            -(g["oos_hr"] or 0),
            -g["n"],
        )
    )
    return results


def write_export(rows: list[dict], path: Path) -> None:
    """Enriched feature export for external re-runs (no leakage as X)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "date",
        "player",
        "team",
        "opp_team",
        "prop",
        "book",
        "side",
        "line",
        "hit",  # label only
        "actual_value",  # sanity only
        "l5",
        "l10",
        "ba",
        "k_rate",
        "ab",
        "own_pitch_tier",
        "opp_pitch_tier",
        "own_bats_tier",
        "opp_bats_tier",
        "cover_dir",
        "season_cushion_dir",
        "season_avg",
        "l5_avg",
        "player_hr_historical",
        "edge",
        "ml_prob",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            side = r["side"]
            season_cush = _num(r.get("season_cushion"))
            if season_cush is None:
                savg = _num(r.get("season_avg"))
                line = r.get("line")
                if savg is not None and line is not None:
                    season_cush = (savg - line) if side == "OVER" else (line - savg)
            elif side == "UNDER":
                season_cush = -season_cush
            w.writerow(
                {
                    "date": r["date"],
                    "player": r.get("player"),
                    "team": r.get("team"),
                    "opp_team": r.get("opp_team"),
                    "prop": r["prop"],
                    "book": r["book"],
                    "side": side,
                    "line": r.get("line"),
                    "hit": r["hit"],
                    "actual_value": r.get("actual_value"),
                    "l5": C.l5_dir(r, side),
                    "l10": C.l10_dir(r, side),
                    "ba": r.get("ba"),
                    "k_rate": r.get("k_rate"),
                    "ab": r.get("ab"),
                    "own_pitch_tier": r.get("own_pitch_tier"),
                    "opp_pitch_tier": r.get("opp_pitch_tier"),
                    "own_bats_tier": r.get("own_bats_tier"),
                    "opp_bats_tier": r.get("opp_bats_tier"),
                    "cover_dir": directional_cover(r),
                    "season_cushion_dir": season_cush,
                    "season_avg": r.get("season_avg"),
                    "l5_avg": r.get("l5_avg"),
                    "player_hr_historical": r.get("player_hr_historical"),
                    "edge": r.get("edge"),
                    "ml_prob": r.get("ml_prob"),
                }
            )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sport", default="MLB", help="Only MLB implemented in this pass")
    ap.add_argument("--min-n", type=int, default=SHOW_N)
    ap.add_argument("--top", type=int, default=80, help="Ranked rows to print")
    args = ap.parse_args()
    if str(args.sport).upper() != "MLB":
        raise SystemExit("This runner currently supports --sport MLB only")

    print("loading defense + graded MLB + step8 join...")
    defs = load_def()
    rows = iter_mlb_rows(defs)
    dates = sorted({r["date"] for r in rows})
    train_dates, test_dates = oos_split_dates(dates)
    print(
        f"rows={len(rows):,} dates={len(dates)} "
        f"({dates[0] if dates else '?'} -> {dates[-1] if dates else '?'}) "
        f"train_dates={len(train_dates)} oos_dates={len(test_dates)} "
        f"def_teams={len(defs)}"
    )

    # Coverage of rich features
    cov = {
        "l5": sum(1 for r in rows if C.l5_dir(r, r["side"]) is not None),
        "l10": sum(1 for r in rows if C.l10_dir(r, r["side"]) is not None),
        "ba": sum(1 for r in rows if r.get("ba") is not None),
        "k_rate": sum(1 for r in rows if r.get("k_rate") is not None),
        "own_pitch": sum(1 for r in rows if r.get("own_pitch_tier")),
        "opp_pitch": sum(1 for r in rows if r.get("opp_pitch_tier")),
        "cover": sum(1 for r in rows if directional_cover(r) is not None),
        "actual": sum(1 for r in rows if r.get("actual_value") is not None),
    }
    print("coverage:", {k: f"{v}/{len(rows)}" for k, v in cov.items()})

    write_export(rows, OUT_EXPORT)
    print(f"export -> {OUT_EXPORT}")

    sanity = sanity_report(rows)
    OUT_SANITY.write_text(json.dumps(sanity, indent=2), encoding="utf-8")
    print(f"sanity flagged props: {len(sanity['flagged'])} -> {OUT_SANITY}")
    for f in sanity["flagged"][:12]:
        print(
            f"  FLAG {f['book']} {f['prop']}: "
            f"zero_rate={f.get('zero_rate')} mean_a={f.get('mean_actual')} "
            f"mean_line={f.get('mean_line')} ratio={f.get('actual_over_line')} "
            f"{f.get('flags')}"
        )

    gates = gate_catalog()
    # Deduplicate identical atom sets keeping first name
    dedup: dict[tuple[str, ...], str] = {}
    unique_gates: list[tuple[str, tuple[str, ...]]] = []
    for name, atoms in gates:
        key = tuple(sorted(atoms))
        if key in dedup:
            continue
        dedup[key] = name
        unique_gates.append((name, atoms))
    print(f"gates scanned per cell: {len(unique_gates)}")

    # Leakage guard: atom names must not intersect banned outcome cols
    atom_set = set(RECENCY + TIERS + HITTER + CUSHION + LINES)
    overlap = atom_set & LEAKAGE_COLS
    if overlap:
        raise SystemExit(f"leakage atoms slipped in: {overlap}")

    by_cell: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        by_cell[(r["book"], r["prop"])].append(r)

    ranked: list[dict] = []
    report_cells = []
    for (book, prop), pool in sorted(by_cell.items(), key=lambda x: -len(x[1])):
        if len(pool) < args.min_n:
            continue
        cell_gates = eval_pool(pool, unique_gates, train_dates, test_dates, args.min_n)
        report_cells.append(
            {
                "book": book,
                "prop": prop,
                "n": len(pool),
                "base_hr": rate(sum(r["hit"] for r in pool), len(pool)),
                "top": cell_gates[:25],
            }
        )
        for g in cell_gates:
            ranked.append({"book": book, "prop": prop, **g})

    ranked.sort(
        key=lambda g: (
            -int(g.get("ticket_clear") and g.get("gate") != "all"),
            -int(bool(g.get("oos_ticket_clear"))),
            -(g.get("wilson_low") or 0),
            -(g.get("hr") or 0),
            -g.get("n", 0),
        )
    )

    headline = [g for g in ranked if g.get("gate") != "all"]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "sport": "MLB",
        "n_rows": len(rows),
        "dates": dates,
        "train_dates": sorted(train_dates),
        "oos_dates": sorted(test_dates),
        "ticket_bar": {"hr": TICKET_HR, "n": TICKET_N},
        "leakage_excluded": sorted(LEAKAGE_COLS),
        "feature_atoms": list(RECENCY + TIERS + HITTER + CUSHION + LINES),
        "coverage": cov,
        "sanity_flagged_n": len(sanity["flagged"]),
        "sanity_flagged": sanity["flagged"][:30],
        "n_gates": len(unique_gates),
        "ranked": headline[:500],
        "cells": report_cells,
        "exports": {
            "ranked_json": str(OUT_JSON),
            "ranked_csv": str(OUT_CSV),
            "row_export": str(OUT_EXPORT),
            "sanity_json": str(OUT_SANITY),
        },
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        fields = [
            "book",
            "prop",
            "gate",
            "hr",
            "n",
            "hits",
            "wilson_low",
            "lift_vs_base",
            "base_hr",
            "train_hr",
            "train_n",
            "oos_hr",
            "oos_n",
            "ticket_clear",
            "oos_ticket_clear",
        ]
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for g in headline:
            w.writerow(g)

    print(f"\nranked -> {OUT_CSV}")
    print(f"json   -> {OUT_JSON}")
    print(f"\n=== TOP {args.top} (ticket clears / high Wilson) ===")
    print(f"{'HR':>6} {'n':>5} {'OOS':>10} {'lift':>6}  book / prop / gate")
    for g in headline[: args.top]:
        oos = (
            f"{100 * g['oos_hr']:.0f}%/{g['oos_n']}"
            if g.get("oos_hr") is not None
            else f"-/{g.get('oos_n', 0)}"
        )
        lift = g.get("lift_vs_base")
        lift_s = f"{100 * lift:+.1f}" if lift is not None else "  -"
        flag = " *" if g.get("ticket_clear") else ""
        print(
            f"{100 * (g['hr'] or 0):5.1f}% {g['n']:5d} {oos:>10} {lift_s:>6}  "
            f"{g['book']} | {g['prop']} | {g['gate']}{flag}"
        )


if __name__ == "__main__":
    main()
