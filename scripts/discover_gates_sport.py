#!/usr/bin/env python3
"""Systematic gate discovery for any pipeline sport (CFB / Soccer / Tennis / WNBA / …).

Same methodology as discover_gates_nfl.py / MLB discover_gates_for_all_props:
  - Graded HIT/MISS + dated step8 L5/L10/D/cover join
  - Outcome leakage banned
  - Chronological OOS (last 30% of dates; last date only when ≤4 slates)
  - Unique player-game dedupe (ladder boards)
  - Zero-actual / implausible-value sanity
  - Per-prop split-balance: late-arriving props skewed into the global
    holdout clear oos_ticket_clear (WNBA FGA failure mode)

  py -3.14 scripts/discover_gates_sport.py --sport CFB
  py -3.14 scripts/discover_gates_sport.py --sport Soccer --min-n 25 --top 60
  py -3.14 scripts/discover_gates_sport.py --sport Tennis
  py -3.14 scripts/discover_gates_sport.py --sport WNBA
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from collections import defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

import all_ranked_prop_hit_rates as C  # noqa: E402
import prop_hit_tiers as T  # noqa: E402
from utils.defense_tiers import normalize_def_tier_label  # noqa: E402
from utils.discover_gates_split_balance import (  # noqa: E402
    flagged_split_imbalance,
    split_balance_by_cell,
    split_balance_for_cell,
)

try:
    from utils.nfl_unsupported_props import is_football_period_split_prop
except Exception:  # pragma: no cover

    def is_football_period_split_prop(_p) -> bool:
        return False

try:
    from utils.stat_attach_invalid import is_analysis_invalid_prop
except Exception:  # pragma: no cover

    def is_analysis_invalid_prop(_sport, _p) -> bool:
        return False


OUT_DIR = _REPO / "data" / "reports"

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
        "actual_source",
        "actual_source_conflict",
    }
)

BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER")
PROD = frozenset({"Weak", "Below Avg"})
STINGY = frozenset({"Elite", "Above Avg"})

RECENCY = ("L5>=4", "L5=5", "L10>=8", "L10>=9", "L10=10")
TIERS = ("D_ok", "D_weak", "D_elite")
CUSHION = ("cover>=1", "cover>=2", "cover>=8", "season_HR>70%")
LINES = ("line_0.5", "line_1.5", "line_ge10", "line_ge20", "line_ge40")
ROLE = ("minutes_high", "role_starter", "std_gap>=4")

# sport_key -> (folder, step8 stems, accept_sports, ticket_n, note)
SPORT_CFG = {
    "CFB": {
        "folder": "cfb",
        "accept": frozenset({"CFB"}),
        "ticket_n": 25,
        "exclude_period": True,
        "note": "Early CFB window may be thin; do not auto-wire.",
    },
    "CFB1H": {
        "folder": "cfb1h",
        "accept": frozenset({"CFB1H"}),
        "ticket_n": 20,
        "exclude_period": False,
        "note": "First-half board; separate from full-game CFB.",
    },
    "Soccer": {
        "folder": "soccer",
        "accept": frozenset({"Soccer"}),
        "ticket_n": 40,
        "exclude_period": False,
        "note": "Keep Shots/SOT/Saves already gated; look for unseen props.",
    },
    "Tennis": {
        "folder": "tennis",
        "accept": frozenset({"Tennis"}),
        "ticket_n": 40,
        "exclude_period": False,
        "note": "Serve UNDER Aces/DF already faded post-backfill; sanity zero rates.",
    },
    "WNBA": {
        "folder": "wnba",
        "accept": frozenset({"WNBA"}),
        "ticket_n": 40,
        "exclude_period": False,
        "note": "Full-game WNBA only (not 1H/1Q).",
    },
    "NHL": {
        "folder": "nhl",
        "accept": frozenset({"NHL"}),
        "ticket_n": 40,
        "exclude_period": False,
        "note": "Off-season for live card; still useful for gate catalog.",
    },
    "NBA": {
        "folder": "nba",
        "accept": frozenset({"NBA"}),
        "ticket_n": 40,
        "exclude_period": False,
        "note": "Full-game NBA. Live G70 is held: generic L5=5+L10>=8+D is -EV at scraped Goblin quotes.",
    },
    "NBA1H": {
        "folder": "nba1h",
        "accept": frozenset({"NBA1H"}),
        "ticket_n": 30,
        "exclude_period": False,
        "note": "First-half NBA. Mixer/G70 held with full-game NBA.",
    },
    "WNBA1H": {
        "folder": "wnba1h",
        "accept": frozenset({"WNBA1H"}),
        "ticket_n": 30,
        "exclude_period": False,
        "note": "First-half WNBA. List + tickets still L5=5.",
    },
    "MLB": {
        "folder": "mlb",
        "accept": frozenset({"MLB"}),
        "ticket_n": 40,
        "exclude_period": False,
        "note": "MLB keep props 1-10 replace the generic stack. Standard stays off tickets.",
    },
    "NFL": {
        "folder": "nfl",
        "accept": frozenset({"NFL"}),
        "ticket_n": 30,
        "exclude_period": True,
        "note": "Regular-season NFL. Period splits excluded. Goblin still add-rule + default stack.",
    },
    "Golf": {
        "folder": "golf",
        "accept": frozenset({"GOLF", "PGA", "Golf"}),
        "ticket_n": 25,
        "exclude_period": False,
        "note": "No opponent D. Keep is L5=5 + L10>=8.",
    },
    "CBB": {
        "folder": "cbb",
        "accept": frozenset({"CBB"}),
        "ticket_n": 30,
        "exclude_period": False,
        "note": "Out of season for live card. Graded archive only.",
    },
}

TICKET_HR = 0.70
# Break-evens are the pick type's own payout, not the Goblin 3-leg bar.
# Goblin: 2-leg at 1.40x, 3-leg at 2.20x. Standard: 2-leg near 3x, 3-leg near 6x.
GOBLIN_BREAK_EVEN = (0.845, 0.769)
STANDARD_BREAK_EVEN = (0.577, 0.550)
SHOW_N = 15


def _num(v):
    return C.num(v)


def _norm_player(s: object) -> str:
    x = re.sub(r"[^a-z0-9 ]+", " ", str(s or "").lower())
    return re.sub(r"\s+", " ", x).strip()


def _side(raw: object) -> str | None:
    s = str(raw or "").strip().upper()
    if s in ("O", "OVER"):
        return "OVER"
    if s in ("U", "UNDER"):
        return "UNDER"
    return None


def _pick_book(pick: object, side: str) -> str | None:
    return C.pick_book(str(pick or ""), side)


def _prop_fold(s: object) -> str:
    t = re.sub(r"[^a-z0-9]+", "", str(s or "").lower())
    if t.startswith("total"):
        t = t[5:]
    return t


def _line_key(raw: object) -> float | None:
    n = _num(raw)
    if n is None:
        return None
    return round(float(n), 2)


_CORRECTIONS: dict[tuple, tuple[str, str]] | None = None


def grade_corrections() -> dict[tuple, tuple[str, str]]:
    """Board-opponent grade fixes. Apply once; do not append the log again."""
    global _CORRECTIONS
    if _CORRECTIONS is not None:
        return _CORRECTIONS
    idx: dict[tuple, tuple[str, str]] = {}
    path = _REPO / "data" / "reports" / "grade_corrections.jsonl"
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            key = (
                str(rec.get("date") or "")[:10],
                _norm_player(rec.get("player")),
                _prop_fold(rec.get("prop")),
                str(rec.get("direction") or "").strip().upper(),
                _line_key(rec.get("line")),
            )
            old = str(rec.get("old") or "").strip().upper()
            new = str(rec.get("new") or "").strip().upper()
            if old and new:
                idx[key] = (old, new)
    _CORRECTIONS = idx
    return idx


def soccer_notinbox_keys() -> set[tuple[str, str, str]]:
    """Player-date-prop keys whose stored actual was not in the matched box."""
    path = _REPO / "data" / "reports" / "soccer_unresolved_multi_event.csv"
    keys: set[tuple[str, str, str]] = set()
    if not path.is_file():
        return keys
    with path.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("bucket") != "player_not_in_box":
                continue
            prop = str(row.get("prop") or "")
            if prop not in {"shots", "sog", "saves"}:
                continue
            player = re.sub(r"[^a-z0-9]+", "", str(row.get("player_raw") or "").lower())
            keys.add((str(row.get("day") or "")[:10], player, prop))
    return keys


_SOCCER_NOT_IN_BOX: set[tuple[str, str, str]] | None = None


def graded_candidates(date: str) -> list[Path]:
    """Every graded file for a date. The main copy can omit a sport the checkout has."""
    found: list[Path] = []
    name = f"graded_props_{date}.json"
    for base in (MAIN, _REPO):
        path = base / "ui_runner" / "templates" / name
        if path.is_file() and path not in found:
            found.append(path)
    return found


def _file_has_sport(raw: dict, accept: frozenset[str]) -> bool:
    for row in raw.get("props") or []:
        if not isinstance(row, dict):
            continue
        if T.norm_sport(str(row.get("sport") or "")) in accept:
            return True
    return False


def _file_has_usable_props(raw: dict, accept: frozenset[str], sport_key: str) -> bool:
    """A sport tag with a blank prop must not hide the other copy of the file."""
    canon_sport = "CFB" if sport_key in {"CFB", "CFB1H"} else sport_key
    for row in raw.get("props") or []:
        if not isinstance(row, dict):
            continue
        sport = T.norm_sport(str(row.get("sport") or ""))
        if sport not in accept:
            continue
        if T.canon_prop(canon_sport, str(row.get("prop") or row.get("prop_type") or "")):
            return True
    return False


_JSON_CACHE: dict[str, dict] = {}


def _read_json(path: Path) -> dict | None:
    key = str(path)
    hit = _JSON_CACHE.get(key)
    if hit is not None:
        return hit
    try:
        loaded = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if isinstance(loaded, dict):
        _JSON_CACHE[key] = loaded
        return loaded
    return None


def graded_by_date() -> dict[str, Path]:
    by: dict[str, Path] = {}
    for d in (MAIN / "ui_runner" / "templates", _REPO / "ui_runner" / "templates"):
        if not d.is_dir():
            continue
        for p in sorted(d.glob("graded_props_20*.json")):
            if ".bak_" in p.name:
                continue
            date = p.stem.replace("graded_props_", "")[:10]
            if date not in by:
                by[date] = p
    return by


def find_step8(sport_key: str, date: str) -> Path | None:
    folder = SPORT_CFG[sport_key]["folder"]
    names = (
        f"step8_{folder}_direction_clean_{date}.xlsx",
        f"step8_{folder}_direction_clean.xlsx",
        f"step8_{folder}_direction_clean_{date}.csv",
        f"step8_{folder}_direction.csv",
        f"step8_{folder}_direction_clean.parquet",
    )
    for root in (MAIN, _REPO):
        d = root / "outputs" / date / folder
        for name in names:
            p = d / name
            if p.exists():
                return p
        # dated root mirrors
        for name in names:
            p = root / "outputs" / date / name
            if p.exists():
                return p
    return None


def load_step8_index(path: Path, sport_key: str) -> dict[tuple, dict]:
    try:
        if path.suffix.lower() in (".xlsx", ".xls"):
            df = __import__("pandas").read_excel(path)
        elif path.suffix.lower() == ".parquet":
            df = __import__("pandas").read_parquet(path)
        else:
            df = __import__("pandas").read_csv(path, low_memory=False)
    except Exception:
        return {}
    colmap = {str(c).strip().lower(): c for c in df.columns}

    def col(*names):
        for n in names:
            if n.lower() in colmap:
                return colmap[n.lower()]
        return None

    c_player = col("player", "Player")
    c_prop = col("prop", "Prop", "prop_type", "prop_norm")
    c_pick = col("pick type", "Pick Type", "pick_type")
    c_line = col("line", "Line")
    c_dir = col("direction", "Direction", "bet_direction", "final_bet_direction")
    c_l5o = col("l5 over", "L5 Over", "last5_over", "l5_over")
    c_l5u = col("l5 under", "L5 Under", "last5_under", "l5_under")
    c_l10o = col("l10 over", "L10 Over", "l10_over")
    c_l10u = col("l10 under", "L10 Under", "l10_under")
    c_def = col("def tier", "Def Tier", "def_tier", "opp_def_tier")
    c_cover = col("abs edge", "Abs Edge", "cover", "dist_l5", "avg_L5_vs_line")

    out: dict[tuple, dict] = {}
    for row in df.to_dict("records"):
        prop = T.canon_prop(sport_key, str(row.get(c_prop) or ""))
        if not prop:
            continue
        if SPORT_CFG[sport_key].get("exclude_period") and is_football_period_split_prop(prop):
            continue
        side = _side(row.get(c_dir) if c_dir else None)
        if side is None:
            continue
        pick = (
            "Goblin"
            if "goblin" in str(row.get(c_pick) or "").lower()
            else (
                "Demon"
                if "demon" in str(row.get(c_pick) or "").lower()
                else "Standard"
            )
        )
        line = _num(row.get(c_line) if c_line else None)
        key = (_norm_player(row.get(c_player) if c_player else None), prop, pick, line, side)
        rec = {
            "l5_over": _num(row.get(c_l5o) if c_l5o else None),
            "l5_under": _num(row.get(c_l5u) if c_l5u else None),
            "l10_over": _num(row.get(c_l10o) if c_l10o else None),
            "l10_under": _num(row.get(c_l10u) if c_l10u else None),
            "def_tier": row.get(c_def) if c_def else "",
            "cover": _num(row.get(c_cover) if c_cover else None),
        }
        prev = out.get(key)
        if prev is None:
            out[key] = rec
        else:
            for k, v in rec.items():
                if prev.get(k) in (None, "") and v not in (None, ""):
                    prev[k] = v
    return out


def unique_game_dedupe(rows: list[dict]) -> list[dict]:
    groups: dict[tuple, list[dict]] = defaultdict(list)
    for r in rows:
        key = (r["date"], r["player_norm"], r["prop"], r["side"], r["book"])
        groups[key].append(r)
    out = []
    for _key, pool in groups.items():
        lines = [_num(r.get("line")) for r in pool]
        lines_ok = [x for x in lines if x is not None]
        if not lines_ok:
            out.append(pool[0])
            continue
        med = sorted(lines_ok)[len(lines_ok) // 2]
        best = min(
            pool,
            key=lambda r: (
                abs((_num(r.get("line")) or med) - med),
                abs((_num(r.get("actual_value")) or 0) - (_num(r.get("line")) or 0)),
            ),
        )
        out.append(best)
    return out


def iter_sport_rows(sport_key: str) -> tuple[list[dict], dict]:
    cfg = SPORT_CFG[sport_key]
    accept = cfg["accept"]
    by_date = graded_by_date()
    rows: list[dict] = []
    skipped_period = 0
    skipped_other = 0
    corrections_applied = 0
    for date in sorted(by_date):
        candidates = graded_candidates(date) or [by_date[date]]
        raw = None
        for path in candidates:
            loaded = _read_json(path)
            if loaded is None:
                continue
            raw = loaded
            if _file_has_usable_props(loaded, accept, sport_key):
                break
        if raw is None:
            continue
        step_path = find_step8(sport_key, date)
        step_idx = load_step8_index(step_path, sport_key) if step_path else {}
        for r in raw.get("props") or []:
            if not isinstance(r, dict):
                continue
            sport = T.norm_sport(str(r.get("sport") or ""))
            if sport not in accept:
                skipped_other += 1
                continue
            corr = grade_corrections().get(
                (
                    date[:10],
                    _norm_player(r.get("player")),
                    _prop_fold(r.get("prop") or r.get("prop_type")),
                    _side(r.get("direction") or r.get("over_under")) or "",
                    _line_key(r.get("line")),
                )
            )
            res = str(r.get("result") or "").strip().upper()
            if corr and res == corr[0]:
                res = corr[1]
                corrections_applied += 1
            if res not in ("HIT", "MISS"):
                continue
            prop = T.canon_prop(sport_key, str(r.get("prop") or r.get("prop_type") or ""))
            if not prop:
                continue
            if sport_key == "NHL":
                from utils.nhl_excluded_props import nhl_cell_excluded_props

                if prop in nhl_cell_excluded_props():
                    continue
            if str(r.get("minutes_status") or "") == "minutes_unknown":
                continue
            reading = str(r.get("retirement_reading") or "")
            if reading in {"readings_differ", "walkover_chart_silent"}:
                continue
            if sport_key == "Soccer":
                global _SOCCER_NOT_IN_BOX
                if _SOCCER_NOT_IN_BOX is None:
                    _SOCCER_NOT_IN_BOX = soccer_notinbox_keys()
                player_key = re.sub(r"[^a-z0-9]+", "", str(r.get("player") or "").lower())
                game_date = str(r.get("game_date") or r.get("date") or date)[:10]
                if (game_date, player_key, prop) in _SOCCER_NOT_IN_BOX:
                    continue
            if cfg.get("exclude_period") and (
                is_football_period_split_prop(prop)
                or is_football_period_split_prop(r.get("prop"))
                or is_analysis_invalid_prop(sport_key, prop)
                or is_analysis_invalid_prop(sport_key, r.get("prop"))
            ):
                skipped_period += 1
                continue
            side = _side(r.get("direction") or r.get("over_under"))
            if side is None:
                continue
            book = _pick_book(r.get("pick_type"), side)
            if book not in BOOKS:
                continue
            line = _num(r.get("line"))
            player_norm = _norm_player(r.get("player"))
            pick = (
                "Goblin"
                if "goblin" in str(r.get("pick_type") or "").lower()
                else (
                    "Demon"
                    if "demon" in str(r.get("pick_type") or "").lower()
                    else "Standard"
                )
            )
            key = (player_norm, prop, pick, line, side)
            s8 = step_idx.get(key) or {}
            rec = {
                "date": date,
                "player": r.get("player"),
                "player_norm": player_norm,
                "team": r.get("team") or "",
                "opp_team": r.get("opp_team") or "",
                "prop": prop,
                "book": book,
                "side": side,
                "line": line,
                "hit": 1 if res == "HIT" else 0,
                "actual_value": _num(r.get("actual_value")),
                "l5_over": r.get("l5_over")
                if r.get("l5_over") not in (None, "")
                else s8.get("l5_over"),
                "l5_under": r.get("l5_under")
                if r.get("l5_under") not in (None, "")
                else s8.get("l5_under"),
                "l10_over": r.get("l10_over")
                if r.get("l10_over") not in (None, "")
                else s8.get("l10_over"),
                "l10_under": r.get("l10_under")
                if r.get("l10_under") not in (None, "")
                else s8.get("l10_under"),
                "def_tier": r.get("def_tier")
                or r.get("def")
                or s8.get("def_tier")
                or "",
                "cover": r.get("cover") or r.get("dist_l5") or s8.get("cover"),
                "player_hr_historical": r.get("player_hr_historical"),
                "minutes_tier": r.get("minutes_tier") or "",
                "role_tier": r.get("role_tier") or "",
                "standard_line": _num(r.get("standard_line") or s8.get("standard_line")),
                # Raw label so Standard-book readers can drop blank/dash defaults
                # (book_label maps anything non-Goblin/Demon to Standard).
                "pick_type_raw": str(r.get("pick_type") or "").strip(),
            }
            if rec["l5_over"] in (None, ""):
                rec["l5_over"] = r.get("last5_over")
            if rec["l5_under"] in (None, ""):
                rec["l5_under"] = r.get("last5_under")
            rows.append(rec)

    meta = {
        "skipped_period": skipped_period,
        "skipped_other_sport_rows": skipped_other,
        "raw_n": len(rows),
        "corrections_applied": corrections_applied,
    }
    deduped = unique_game_dedupe(rows)
    meta["unique_n"] = len(deduped)
    meta["dedupe_drop"] = len(rows) - len(deduped)
    return deduped, meta


def directional_cover(rec: dict) -> float | None:
    cover = _num(rec.get("cover"))
    if cover is None:
        return None
    return abs(cover)


def build_atoms(rec: dict) -> dict[str, bool]:
    side = rec["side"]
    l5 = C.l5_dir(rec, side)
    l10 = C.l10_dir(rec, side)
    cover = directional_cover(rec)
    d_raw = normalize_def_tier_label(rec.get("def_tier")) or ""
    season_hr = _num(rec.get("player_hr_historical"))
    if season_hr is not None and season_hr > 1.5:
        season_hr = season_hr / 100.0
    line = rec.get("line")
    d_ok = (side == "OVER" and d_raw in PROD) or (side == "UNDER" and d_raw in STINGY)
    return {
        "L5>=4": l5 is not None and l5 >= 4,
        "L5=5": l5 is not None and l5 == 5,
        "L10>=8": l10 is not None and l10 >= 8,
        "L10>=9": l10 is not None and l10 >= 9,
        "L10=10": l10 is not None and l10 == 10,
        "D_ok": d_ok,
        "D_weak": d_raw in PROD,
        "D_elite": d_raw in STINGY,
        "cover>=1": cover is not None and cover >= 1.0,
        "cover>=2": cover is not None and cover >= 2.0,
        "cover>=8": cover is not None and cover >= 8.0,
        "season_HR>70%": season_hr is not None and season_hr > 0.70,
        "line_0.5": line is not None and abs(line - 0.5) < 1e-9,
        "line_1.5": line is not None and abs(line - 1.5) < 1e-9,
        "line_ge10": line is not None and line >= 10,
        "line_ge20": line is not None and line >= 20,
        "line_ge40": line is not None and line >= 40,
        "minutes_high": str(rec.get("minutes_tier") or "").strip().upper() == "HIGH",
        "role_starter": str(rec.get("role_tier") or "").strip().upper() in {"STARTER", "PRIMARY", "HIGH_VOL"},
        "std_gap>=4": (
            _num(rec.get("standard_line")) is not None
            and line is not None
            and _num(rec.get("standard_line")) - line >= 4
        ),
    }


def gate_catalog() -> list[tuple[str, tuple[str, ...]]]:
    gates: list[tuple[str, tuple[str, ...]]] = [("all", ())]
    atoms = RECENCY + TIERS + CUSHION + LINES + ROLE
    for a in atoms:
        gates.append((a, (a,)))
    for r in RECENCY:
        for t in TIERS + CUSHION:
            gates.append((f"{r} + {t}", (r, t)))
    extras = [
        ("L5=5 + L10>=8", ("L5=5", "L10>=8")),
        ("L5>=4 + L10>=8", ("L5>=4", "L10>=8")),
        ("L5=5 + L10>=8 + D_ok", ("L5=5", "L10>=8", "D_ok")),
        ("L5>=4 + L10>=8 + D_ok", ("L5>=4", "L10>=8", "D_ok")),
        ("L10>=8 + D_ok", ("L10>=8", "D_ok")),
        ("L5=5 + D_ok", ("L5=5", "D_ok")),
        ("L5>=4 + D_ok", ("L5>=4", "D_ok")),
        ("L5=5 + cover>=1", ("L5=5", "cover>=1")),
        ("L5=5 + cover>=2", ("L5=5", "cover>=2")),
        ("L10>=8 + cover>=1", ("L10>=8", "cover>=1")),
        ("L5=5 + line_0.5", ("L5=5", "line_0.5")),
        ("L5=5 + L10>=8 + line_0.5", ("L5=5", "L10>=8", "line_0.5")),
    ]
    seen = {g[0] for g in gates}
    for name, atoms_t in extras:
        if name not in seen:
            gates.append((name, atoms_t))
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


def oos_split_dates(dates: list[str]) -> tuple[set[str], set[str]]:
    dates = sorted(dates)
    if len(dates) <= 1:
        return set(dates), set(dates)
    if len(dates) <= 4:
        return set(dates[:-1]), {dates[-1]}
    cut = max(1, int(round(len(dates) * 0.70)))
    return set(dates[:cut]), set(dates[cut:])


def gate_uses_cover_or_cush(gate: str) -> bool:
    g = (gate or "").lower()
    return "cover" in g or "season_hr" in g or "season_cush" in g or "cush" in g


def sanity_report(rows: list[dict]) -> dict:
    buckets: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        buckets[(r["book"], r["prop"])].append(r)
    flagged = []
    cells = []
    for (book, prop), pool in sorted(buckets.items()):
        actuals = [r["actual_value"] for r in pool if r.get("actual_value") is not None]
        n_act = len(actuals)
        zeros = sum(1 for a in actuals if a == 0)
        zero_rate = (zeros / n_act) if n_act else None
        mean_a = (sum(actuals) / n_act) if n_act else None
        lines = [_num(r.get("line")) for r in pool if _num(r.get("line")) is not None]
        mean_line = (sum(lines) / len(lines)) if lines else None
        ratio = None
        if mean_a is not None and mean_line is not None and mean_line > 0:
            ratio = round(mean_a / mean_line, 3)
        cell = {
            "book": book,
            "prop": prop,
            "n": len(pool),
            "n_with_actual": n_act,
            "zero_n": zeros,
            "zero_rate": None if zero_rate is None else round(zero_rate, 4),
            "mean_actual": None if mean_a is None else round(mean_a, 3),
            "mean_line": None if mean_line is None else round(mean_line, 3),
            "actual_over_line": ratio,
            "hr_all": rate(sum(r["hit"] for r in pool), len(pool)),
        }
        reasons = []
        if zero_rate is not None and zero_rate >= 0.35 and "UNDER" in book:
            reasons.append(f"zero_actual_under_inflation zero_rate={zero_rate:.1%}")
        if ratio is not None and ratio < 0.35 and "OVER" in book and mean_line and mean_line >= 5:
            reasons.append(f"actual_<<_line ratio={ratio}")
        cell["flags"] = reasons
        cells.append(cell)
        if reasons:
            flagged.append(cell)
    return {"cells": cells, "flagged": flagged}


def eval_pool(pool, unique_gates, train_dates, test_dates, min_n, ticket_n):
    base_h = sum(r["hit"] for r in pool)
    base_n = len(pool)
    base_hr = rate(base_h, base_n)
    out = []
    for name, needed in unique_gates:
        h = n = th = tn = oh = on = 0
        for r in pool:
            atoms = build_atoms(r)
            if not passes(atoms, needed):
                continue
            n += 1
            h += r["hit"]
            if r["date"] in train_dates:
                tn += 1
                th += r["hit"]
            if r["date"] in test_dates:
                on += 1
                oh += r["hit"]
        if n < min_n:
            continue
        hr = rate(h, n)
        wl = wilson_low(h, n)
        thr = rate(th, tn)
        ohr = rate(oh, on)
        lift = None if base_hr is None or hr is None else round(hr - base_hr, 4)
        ticket_clear = bool(hr is not None and hr >= TICKET_HR and n >= ticket_n)
        oos_ticket_clear = bool(
            ohr is not None and ohr >= TICKET_HR and on >= max(8, ticket_n // 5)
        )
        out.append(
            {
                "gate": name,
                "hr": hr,
                "n": n,
                "hits": h,
                "wilson_low": wl,
                "lift_vs_base": lift,
                "base_hr": base_hr,
                "train_hr": thr,
                "train_n": tn,
                "oos_hr": ohr,
                "oos_n": on,
                "ticket_clear": ticket_clear,
                "oos_ticket_clear": oos_ticket_clear,
                "uses_cover_or_cush": gate_uses_cover_or_cush(name),
            }
        )
    out.sort(
        key=lambda g: (
            -int(bool(g.get("oos_ticket_clear"))),
            -int(bool(g.get("ticket_clear"))),
            -(g.get("wilson_low") or 0),
            -(g.get("hr") or 0),
            -g.get("n", 0),
        )
    )
    return out


def write_export(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fields = sorted({k for r in rows for k in r.keys()})
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(r)


def run_sport(sport_key: str, min_n: int, top: int) -> Path:
    cfg = SPORT_CFG[sport_key]
    ticket_n = int(cfg["ticket_n"])
    tag = sport_key.lower()
    out_json = OUT_DIR / f"discover_gates_{tag}_ranked.json"
    out_csv = OUT_DIR / f"discover_gates_{tag}_ranked.csv"
    out_export = OUT_DIR / f"discover_gates_{tag}_export.csv"
    out_sanity = OUT_DIR / f"discover_gates_{tag}_sanity.json"
    out_note = OUT_DIR / f"discover_gates_{tag}_run.md"

    print(f"\n======== {sport_key} ========")
    print(f"loading graded {sport_key} (unique-game dedupe)...")
    rows, meta = iter_sport_rows(sport_key)
    dates = sorted({r["date"] for r in rows})
    train_dates, test_dates = oos_split_dates(dates)
    print(
        f"unique_rows={len(rows):,} raw={meta['raw_n']:,} dedupe_drop={meta['dedupe_drop']} "
        f"skipped_period={meta['skipped_period']} "
        f"dates={len(dates)} ({dates[0] if dates else '?'} -> {dates[-1] if dates else '?'}) "
        f"oos_n_dates={len(test_dates)}"
    )
    if not rows:
        out_note.write_text(f"# {sport_key}\n\nNo graded rows found.\n", encoding="utf-8")
        print("NO ROWS")
        return out_note

    cov = {
        "l5": sum(1 for r in rows if C.l5_dir(r, r["side"]) is not None),
        "l10": sum(1 for r in rows if C.l10_dir(r, r["side"]) is not None),
        "def": sum(1 for r in rows if str(r.get("def_tier") or "").strip()),
        "cover": sum(1 for r in rows if directional_cover(r) is not None),
        "actual": sum(1 for r in rows if r.get("actual_value") is not None),
    }
    print("coverage:", {k: f"{v}/{len(rows)}" for k, v in cov.items()})

    write_export(rows, out_export)
    sanity = sanity_report(rows)
    split_bal = split_balance_by_cell(rows, train_dates, test_dates)
    split_flags = flagged_split_imbalance(split_bal)
    sanity["split_imbalance"] = split_flags
    out_sanity.write_text(json.dumps(sanity, indent=2), encoding="utf-8")
    print(f"sanity flagged: {len(sanity['flagged'])}")
    print(f"split-imbalance cells: {len(split_flags)}")
    for f in split_flags[:12]:
        print(
            f"  SPLIT {f['book']} {f['prop']}: "
            f"train={f['train_n']}/{f['n']} oos={f['oos_n']}/{f['n']} "
            f"oos_share={f.get('oos_share')} | {f['flags'][0] if f.get('flags') else ''}"
        )

    gates = gate_catalog()
    dedup: dict[tuple[str, ...], str] = {}
    unique_gates: list[tuple[str, tuple[str, ...]]] = []
    for name, atoms in gates:
        key = tuple(sorted(atoms))
        if key in dedup:
            continue
        dedup[key] = name
        unique_gates.append((name, atoms))

    by_cell: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        by_cell[(r["book"], r["prop"])].append(r)

    ranked: list[dict] = []
    report_cells = []
    for (book, prop), pool in sorted(by_cell.items(), key=lambda x: -len(x[1])):
        if len(pool) < min_n:
            continue
        bal = split_bal.get((book, prop)) or split_balance_for_cell(
            pool, train_dates, test_dates
        )
        cell_gates = eval_pool(
            pool, unique_gates, train_dates, test_dates, min_n, ticket_n
        )
        for g in cell_gates:
            g["oos_split_reliable"] = bool(bal.get("oos_split_reliable", True))
            g["oos_split_oos_share"] = bal.get("oos_share")
            g["oos_split_train_n_cell"] = bal.get("train_n")
            g["oos_split_flags"] = list(bal.get("flags") or [])
            # Preserve raw clear; clean lists require reliable split.
            g["oos_ticket_clear_raw"] = bool(g.get("oos_ticket_clear"))
            if not g["oos_split_reliable"]:
                g["oos_ticket_clear"] = False
        report_cells.append(
            {
                "book": book,
                "prop": prop,
                "n": len(pool),
                "base_hr": rate(sum(r["hit"] for r in pool), len(pool)),
                "split_balance": bal,
                "top": cell_gates[:20],
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
    combos = len(unique_gates) * len(by_cell)
    for g in headline:
        book = str(g.get("book") or "")
        be2, be3 = STANDARD_BREAK_EVEN if "standard" in book.lower() else GOBLIN_BREAK_EVEN
        g["break_even_2leg"] = be2
        g["break_even_3leg"] = be3
        reasons: list[str] = []
        oos_n = int(g.get("oos_n") or 0)
        cell_n = int(g.get("n") or 0)
        if not g.get("oos_split_reliable", True) or (cell_n and oos_n / cell_n >= 0.70):
            reasons.append("split_balance")
        if int(g.get("oos_n") or 0) == 0:
            reasons.append("holdout_empty")
        if int(g.get("n") or 0) < 30:
            reasons.append("n_under_30")
        if combos >= 500 and int(g.get("n") or 0) < 150:
            reasons.append("search_chance")
        low = g.get("wilson_low")
        if low is not None and float(low) < be3:
            reasons.append("wilson_under_3leg_breakeven")
        g["unreliable"] = bool(reasons)
        g["unreliable_reasons"] = reasons
    clean = [
        g
        for g in headline
        if g.get("oos_ticket_clear")
        and not g.get("uses_cover_or_cush")
        and g.get("oos_split_reliable", True)
    ]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "sport": sport_key,
        "n_rows_unique": len(rows),
        "meta": meta,
        "dates": dates,
        "train_dates": sorted(train_dates),
        "oos_dates": sorted(test_dates),
        "ticket_bar": {"hr": TICKET_HR, "n": ticket_n},
        "leakage_excluded": sorted(LEAKAGE_COLS),
        "feature_atoms": list(RECENCY + TIERS + CUSHION + LINES + ROLE),
        "combinations_searched": combos,
        "break_even": {
            "goblin_2leg_1_4x": GOBLIN_BREAK_EVEN[0],
            "goblin_3leg_2_2x": GOBLIN_BREAK_EVEN[1],
            "standard_2leg": STANDARD_BREAK_EVEN[0],
            "standard_3leg": STANDARD_BREAK_EVEN[1],
        },
        "r7": "ticket_clear does not wire a gate",
        "coverage": cov,
        "sanity_flagged_n": len(sanity["flagged"]),
        "sanity_flagged": sanity["flagged"][:30],
        "split_imbalance_n": len(split_flags),
        "split_imbalance": split_flags[:40],
        "n_gates": len(unique_gates),
        "ranked": headline[:400],
        "clean_oos_no_cover_cush": clean[:80],
        "cells": report_cells,
        "caveat": cfg["note"],
        "exports": {
            "ranked_json": str(out_json),
            "ranked_csv": str(out_csv),
            "row_export": str(out_export),
            "sanity_json": str(out_sanity),
        },
    }
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    with out_csv.open("w", encoding="utf-8", newline="") as f:
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
            "oos_ticket_clear_raw",
            "oos_split_reliable",
            "oos_split_oos_share",
            "uses_cover_or_cush",
        ]
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for g in headline:
            w.writerow(g)

    # clean summary table
    lines = [
        f"# {sport_key} discover_gates run",
        "",
        f"Window **{dates[0]} → {dates[-1]}** · unique n={len(rows)} · OOS dates={len(test_dates)}",
        "",
        cfg["note"],
        "",
        "## Clean OOS clears (no cover / season_HR atoms)",
        "",
        "Requires `oos_ticket_clear` **and** `oos_split_reliable` (prop history not",
        "skewed into the global holdout — WNBA FGA failure mode).",
        "",
        "| HR | n | OOS | base | 2-leg BE | 3-leg BE | flag | gate | cell |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for g in clean[:25]:
        flag = ",".join(g.get("unreliable_reasons") or []) or "ok"
        lines.append(
            f"| {100*(g['hr'] or 0):.1f}% | {g['n']} | "
            f"{100*(g['oos_hr'] or 0):.0f}%/{g['oos_n']} | "
            f"{100*(g['base_hr'] or 0):.1f}% | "
            f"{g['break_even_2leg']:.3f} | {g['break_even_3leg']:.3f} | {flag} | "
            f"`{g['gate']}` | {g['book']} {g['prop']} |"
        )
    if not clean:
        lines.append("| — | — | — | — | none cleared | — |")
    if split_flags:
        lines.extend(
            [
                "",
                "## Split-imbalance flags (do not trust global OOS on these cells)",
                "",
            ]
        )
        for f in split_flags[:20]:
            lines.append(
                f"- **{f['book']} {f['prop']}**: train {f['train_n']}/{f['n']} · "
                f"oos {f['oos_n']}/{f['n']} · {'; '.join(f.get('flags') or [])}"
            )
    lines.extend(
        [
            "",
            f"Exports: `{out_export.name}`, `{out_csv.name}`, `{out_sanity.name}`",
            "",
            "```text",
            f"py -3.14 scripts/discover_gates_sport.py --sport {sport_key}",
            "```",
            "",
        ]
    )
    out_note.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(f"clean OOS (no cover/cush): {len(clean)}")
    print(f"ranked -> {out_csv}")
    print(f"\n=== TOP {top} (any) ===")
    for g in headline[:top]:
        oos = (
            f"{100 * g['oos_hr']:.0f}%/{g['oos_n']}"
            if g.get("oos_hr") is not None
            else f"-/{g.get('oos_n', 0)}"
        )
        flag = " *" if g.get("ticket_clear") else ""
        cush = " [cush]" if g.get("uses_cover_or_cush") else ""
        print(
            f"{100 * (g['hr'] or 0):5.1f}% {g['n']:5d} {oos:>10}  "
            f"{g['book']} | {g['prop']} | {g['gate']}{flag}{cush}"
        )
    print(f"\n=== CLEAN OOS TOP {min(15, len(clean))} ===")
    for g in clean[:15]:
        print(
            f"{100 * (g['hr'] or 0):5.1f}% n={g['n']} oos={100*(g['oos_hr'] or 0):.0f}%/{g['oos_n']}  "
            f"{g['book']} | {g['prop']} | {g['gate']}"
        )
    return out_note


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--sport",
        required=True,
        choices=sorted(SPORT_CFG.keys()) + ["ALL_ACTIVE"],
        help="Sport key, or ALL_ACTIVE = CFB+Soccer+Tennis+WNBA",
    )
    ap.add_argument("--min-n", type=int, default=SHOW_N)
    ap.add_argument("--top", type=int, default=40)
    args = ap.parse_args()

    sports = (
        ["CFB", "Soccer", "Tennis", "WNBA"]
        if args.sport == "ALL_ACTIVE"
        else [args.sport]
    )
    notes = []
    for sp in sports:
        notes.append(run_sport(sp, args.min_n, args.top))

    # cross-sport rollup
    rollup = OUT_DIR / "discover_gates_all_active_summary.md"
    parts = ["# Discover gates — active sports rollup", ""]
    for note in notes:
        if note.exists():
            parts.append(note.read_text(encoding="utf-8"))
            parts.append("")
    rollup.write_text("\n".join(parts), encoding="utf-8")
    print(f"\nrollup -> {rollup}")


if __name__ == "__main__":
    main()
