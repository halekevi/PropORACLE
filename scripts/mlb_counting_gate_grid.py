"""Gate grid for MLB Goblin/Standard TB, Hits, and earned runs.

Joins graded HIT/MISS to dated step8 boards so L5 / D / cover exist on
dates where grades only stored L10. Prints every combo with n>=15 and
flags ticket-grade lift (HR>=70% and n>=40).

  py -3.14 scripts/mlb_counting_gate_grid.py
"""
from __future__ import annotations

import json
import re
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
MAIN = _REPO.parent / "PropORACLE_main_cp"
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import all_ranked_prop_hit_rates as C  # noqa: E402
import prop_hit_tiers as T  # noqa: E402
from utils.defense_tiers import d_aligned, normalize_def_tier_label  # noqa: E402

PROPS = frozenset({"total_bases", "hits", "earned_runs"})
BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER")
OUT = _REPO / "data" / "reports" / "mlb_counting_gate_grid.json"
TICKET_HR = 0.70
TICKET_N = 40
SHOW_N = 15
STEP8_USECOLS = [
    "player",
    "player_name",
    "prop_type",
    "prop_norm",
    "pick_type",
    "line",
    "bet_direction",
    "final_bet_direction",
    "direction",
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
    "def_tier",
    "DEF_TIER",
    "player_hr_historical",
    "hit_rate",
    "line_hit_rate",
]


def _num(v) -> float | None:
    return C.num(v)


def _fill(dst: dict, src: dict, keys: tuple[str, ...]) -> None:
    for k in keys:
        if dst.get(k) is None or dst.get(k) == "":
            v = src.get(k)
            if v is not None and v != "":
                dst[k] = v


def _norm_player(s: object) -> str:
    x = re.sub(r"[^a-z0-9 ]+", " ", str(s or "").lower())
    return re.sub(r"\s+", " ", x).strip()


def _pick(raw: object) -> str:
    s = str(raw or "").strip().lower()
    if "goblin" in s:
        return "Goblin"
    if "demon" in s:
        return "Demon"
    return "Standard"


def _side(raw: object) -> str | None:
    s = str(raw or "").strip().upper()
    if s in ("O", "OVER"):
        return "OVER"
    if s in ("U", "UNDER"):
        return "UNDER"
    return None


def _line_bucket(line: float | None) -> str:
    if line is None:
        return "unk"
    if abs(line - 0.5) < 1e-9:
        return "0.5"
    if abs(line - 1.5) < 1e-9:
        return "1.5"
    if line < 1.5:
        return "<1.5"
    return "2.5+"


def graded_by_date() -> dict[str, list[Path]]:
    by: dict[str, list[Path]] = defaultdict(list)
    for d in (
        _REPO / "ui_runner" / "templates",
        MAIN / "ui_runner" / "templates",
    ):
        if not d.is_dir():
            continue
        for p in sorted(d.glob("graded_props_20*.json")):
            if ".bak_" in p.name:
                continue
            date = p.stem.replace("graded_props_", "")[:10]
            by[date].append(p)
    return dict(by)


def find_step8_csv(date: str) -> Path | None:
    for root in (MAIN, _REPO):
        d = root / "outputs" / date / "mlb"
        for name in (
            f"step8_mlb_direction.csv",
            f"step8_mlb_direction_{date}.csv",
        ):
            p = d / name
            if p.exists():
                return p
    return None


def load_step8_index(path: Path) -> dict[tuple, dict]:
    try:
        df = pd.read_csv(path, usecols=lambda c: c in STEP8_USECOLS, low_memory=False)
    except Exception:
        df = pd.read_csv(path, low_memory=False)
    out: dict[tuple, dict] = {}
    for row in df.to_dict("records"):
        prop = T.canon_prop("MLB", str(row.get("prop_type") or row.get("prop_norm") or ""))
        if prop not in PROPS:
            continue
        side = _side(row.get("bet_direction") or row.get("final_bet_direction") or row.get("direction"))
        if side is None:
            continue
        line = _num(row.get("line"))
        key = (_norm_player(row.get("player") or row.get("player_name")), prop, _pick(row.get("pick_type")), line, side)
        rec = {
            "l5_over": _num(row.get("last5_over") if row.get("last5_over") not in (None, "") else row.get("line_hits_over_5")),
            "l5_under": _num(row.get("last5_under") if row.get("last5_under") not in (None, "") else row.get("line_hits_under_5")),
            "l10_over": _num(row.get("l10_over") if row.get("l10_over") not in (None, "") else row.get("line_hits_over_10")),
            "l10_under": _num(row.get("l10_under") if row.get("l10_under") not in (None, "") else row.get("line_hits_under_10")),
            "def_tier": row.get("def_tier") or row.get("DEF_TIER") or "",
            "l5_avg": _num(row.get("stat_last5_avg")),
            "cover_signed": _num(row.get("avg_L5_vs_line"))
            if _num(row.get("avg_L5_vs_line")) is not None
            else _num(row.get("avg_vs_line")),
            "season_hr": _num(row.get("player_hr_historical") or row.get("hit_rate") or row.get("line_hit_rate")),
        }
        prev = out.get(key)
        if prev is None:
            out[key] = rec
        else:
            _fill(prev, rec, tuple(rec.keys()))
    return out


def merge_grade(dst: dict, r: dict) -> None:
    _fill(
        dst,
        r,
        (
            "l5_over",
            "l5_under",
            "last5_over",
            "last5_under",
            "l10_over",
            "l10_under",
            "def_tier",
            "def",
            "cover",
            "dist_l5",
            "hit_rate_l5",
            "player_hr_historical",
            "l5_avg",
            "stat_last5_avg",
        ),
    )


def iter_rows() -> list[dict]:
    by_date = graded_by_date()
    rows: list[dict] = []
    for date in sorted(by_date):
        merged: dict[tuple, dict] = {}
        for path in by_date[date]:
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            for r in raw.get("props") or []:
                if not isinstance(r, dict):
                    continue
                if T.norm_sport(str(r.get("sport") or "")) != "MLB":
                    continue
                res = str(r.get("result") or "").strip().upper()
                if res not in ("HIT", "MISS"):
                    continue
                prop = T.canon_prop("MLB", str(r.get("prop") or r.get("prop_type") or ""))
                if prop not in PROPS:
                    continue
                side = _side(r.get("direction") or r.get("over_under"))
                if side is None:
                    continue
                book = C.pick_book(str(r.get("pick_type") or ""), side)
                if book not in BOOKS:
                    continue
                line = _num(r.get("line"))
                key = (
                    _norm_player(r.get("player")),
                    prop,
                    _pick(r.get("pick_type")),
                    line,
                    side,
                )
                hit = 1 if res == "HIT" else 0
                if key not in merged:
                    merged[key] = {
                        "date": date,
                        "player": r.get("player"),
                        "prop": prop,
                        "book": book,
                        "side": side,
                        "line": line,
                        "hit": hit,
                        "l5_over": r.get("l5_over") or r.get("last5_over"),
                        "l5_under": r.get("l5_under") or r.get("last5_under"),
                        "l10_over": r.get("l10_over") or r.get("last10_over"),
                        "l10_under": r.get("l10_under") or r.get("last10_under"),
                        "def_tier": r.get("def_tier") or r.get("def") or "",
                        "def": r.get("def") or "",
                        "cover": r.get("cover") or r.get("dist_l5"),
                        "dist_l5": r.get("dist_l5"),
                        "hit_rate_l5": r.get("hit_rate_l5"),
                        "player_hr_historical": r.get("player_hr_historical"),
                        "l5_avg": r.get("l5_avg") or r.get("stat_last5_avg"),
                        "stat_last5_avg": r.get("stat_last5_avg"),
                    }
                else:
                    merge_grade(merged[key], r)
                    if merged[key]["hit"] != hit:
                        merged[key]["hit"] = hit

        step_path = find_step8_csv(date)
        step_idx = load_step8_index(step_path) if step_path else {}
        for key, rec in merged.items():
            s8 = step_idx.get(key)
            if s8:
                if rec.get("l5_over") in (None, ""):
                    rec["l5_over"] = s8.get("l5_over")
                if rec.get("l5_under") in (None, ""):
                    rec["l5_under"] = s8.get("l5_under")
                if rec.get("l10_over") in (None, ""):
                    rec["l10_over"] = s8.get("l10_over")
                if rec.get("l10_under") in (None, ""):
                    rec["l10_under"] = s8.get("l10_under")
                if not str(rec.get("def_tier") or rec.get("def") or "").strip():
                    rec["def_tier"] = s8.get("def_tier") or ""
                if rec.get("cover") in (None, "") and s8.get("cover_signed") is not None:
                    rec["cover"] = s8["cover_signed"]
                if rec.get("l5_avg") in (None, ""):
                    rec["l5_avg"] = s8.get("l5_avg")
                if rec.get("player_hr_historical") in (None, ""):
                    rec["player_hr_historical"] = s8.get("season_hr")
            rows.append(rec)
    return rows


def features(rec: dict) -> dict:
    side = rec["side"]
    prop = rec["prop"]
    l5 = C.l5_dir(rec, side)
    if l5 is None:
        hr5 = _num(rec.get("hit_rate_l5"))
        if hr5 is not None:
            l5 = round(hr5 * 5)
    l10 = C.l10_dir(rec, side)
    d_raw = rec.get("def_tier") or rec.get("def") or ""
    d_ok = d_aligned("MLB", side, d_raw, prop)
    d_label = normalize_def_tier_label(d_raw)
    line = rec.get("line")
    cover = _num(rec.get("cover"))
    if cover is None:
        avg = _num(rec.get("l5_avg") or rec.get("stat_last5_avg"))
        if avg is not None and line is not None:
            cover = (avg - line) if side == "OVER" else (line - avg)
    elif side == "UNDER" and cover is not None:
        # grades sometimes store avg-line; ticket cover is directional
        pass
    season = _num(rec.get("player_hr_historical"))
    if season is not None and season > 1.5:
        season = season / 100.0
    return {
        "l5": l5,
        "l10": l10,
        "d_ok": bool(d_ok),
        "d_known": bool(d_label),
        "line_b": _line_bucket(line),
        "cover": cover,
        "season": season,
        "l5_ge4": l5 is not None and l5 >= 4,
        "l5_eq5": l5 is not None and l5 == 5,
        "l10_ge8": l10 is not None and l10 >= 8,
        "l10_ge9": l10 is not None and l10 >= 9,
        "l10_eq10": l10 is not None and l10 == 10,
        "cover_ge05": cover is not None and cover >= 0.5,
        "cover_ge1": cover is not None and cover >= 1.0,
        "cover_ge15": cover is not None and cover >= 1.5,
        "szn70": season is not None and season > 0.70,
    }


GATES: list[tuple[str, callable]] = [
    ("all", lambda f: True),
    ("line 0.5", lambda f: f["line_b"] == "0.5"),
    ("line 1.5", lambda f: f["line_b"] == "1.5"),
    ("line 2.5+", lambda f: f["line_b"] == "2.5+"),
    ("L5>=4", lambda f: f["l5_ge4"]),
    ("L5=5", lambda f: f["l5_eq5"]),
    ("L10>=8", lambda f: f["l10_ge8"]),
    ("L10>=9", lambda f: f["l10_ge9"]),
    ("L10=10", lambda f: f["l10_eq10"]),
    ("D", lambda f: f["d_ok"]),
    ("cover>=0.5", lambda f: f["cover_ge05"]),
    ("cover>=1", lambda f: f["cover_ge1"]),
    ("cover>=1.5", lambda f: f["cover_ge15"]),
    ("season HR>70%", lambda f: f["szn70"]),
    ("L5=5 + L10>=8", lambda f: f["l5_eq5"] and f["l10_ge8"]),
    ("L5>=4 + L10>=8", lambda f: f["l5_ge4"] and f["l10_ge8"]),
    ("L5=5 + D", lambda f: f["l5_eq5"] and f["d_ok"]),
    ("L10>=8 + D", lambda f: f["l10_ge8"] and f["d_ok"]),
    ("L5=5 + L10>=8 + D", lambda f: f["l5_eq5"] and f["l10_ge8"] and f["d_ok"]),
    ("L5>=4 + L10>=8 + D", lambda f: f["l5_ge4"] and f["l10_ge8"] and f["d_ok"]),
    ("L5=5 + L10>=8 + D + cover>=1", lambda f: f["l5_eq5"] and f["l10_ge8"] and f["d_ok"] and f["cover_ge1"]),
    ("L10>=8 + cover>=1", lambda f: f["l10_ge8"] and f["cover_ge1"]),
    ("L10>=8 + D + cover>=1", lambda f: f["l10_ge8"] and f["d_ok"] and f["cover_ge1"]),
    ("L10=10 + D", lambda f: f["l10_eq10"] and f["d_ok"]),
    ("L5=5 + L10>=8 + D + season>70%", lambda f: f["l5_eq5"] and f["l10_ge8"] and f["d_ok"] and f["szn70"]),
    ("L10>=8 + line 0.5", lambda f: f["l10_ge8"] and f["line_b"] == "0.5"),
    ("L10>=8 + line 1.5", lambda f: f["l10_ge8"] and f["line_b"] == "1.5"),
    ("L5=5 + line 0.5", lambda f: f["l5_eq5"] and f["line_b"] == "0.5"),
    ("D + line 0.5", lambda f: f["d_ok"] and f["line_b"] == "0.5"),
    ("D + cover>=1 + line 0.5", lambda f: f["d_ok"] and f["cover_ge1"] and f["line_b"] == "0.5"),
]


def rate(h: int, n: int) -> float | None:
    if n <= 0:
        return None
    return round(h / n, 4)


def main() -> None:
    rows = iter_rows()
    cells: dict[tuple[str, str], list[dict]] = defaultdict(list)
    coverage = defaultdict(lambda: {"n": 0, "l5": 0, "l10": 0, "d": 0, "cover": 0})
    for rec in rows:
        f = features(rec)
        rec["_f"] = f
        key = (rec["book"], rec["prop"])
        cells[key].append(rec)
        cov = coverage[key]
        cov["n"] += 1
        if f["l5"] is not None:
            cov["l5"] += 1
        if f["l10"] is not None:
            cov["l10"] += 1
        if f["d_known"]:
            cov["d"] += 1
        if f["cover"] is not None:
            cov["cover"] += 1

    report = {
        "n_rows": len(rows),
        "dates": sorted({r["date"] for r in rows}),
        "ticket_bar": {"hr": TICKET_HR, "n": TICKET_N},
        "coverage": {f"{b}|{p}": v for (b, p), v in coverage.items()},
        "cells": [],
        "clears_ticket": [],
        "best_short_of_ticket": [],
    }

    print(f"rows={len(rows)} dates={len(report['dates'])}")
    for (book, prop) in sorted(cells):
        pool = cells[(book, prop)]
        print(f"\n=== {book}  {prop}  n={len(pool)} ===")
        cov = coverage[(book, prop)]
        print(
            f"  fields  L5 {cov['l5']}/{cov['n']}  L10 {cov['l10']}/{cov['n']}  "
            f"D {cov['d']}/{cov['n']}  cover {cov['cover']}/{cov['n']}"
        )
        cell_gates = []
        for name, fn in GATES:
            n = h = 0
            for rec in pool:
                if not fn(rec["_f"]):
                    continue
                n += 1
                h += rec["hit"]
            hr = rate(h, n)
            recg = {"book": book, "prop": prop, "gate": name, "hits": h, "n": n, "hr": hr}
            cell_gates.append(recg)
            if n >= SHOW_N:
                flag = ""
                if hr is not None and hr >= TICKET_HR and n >= TICKET_N:
                    flag = "  TICKET"
                    report["clears_ticket"].append(recg)
                print(f"  {100.0 * hr:5.1f}%  {h:4d}/{n:<5d}  {name}{flag}")
        report["cells"].append({"book": book, "prop": prop, "gates": cell_gates})

    # Best n>=40 under 70, and best n>=15 at/above 70 (thin)
    candidates = []
    for cell in report["cells"]:
        for g in cell["gates"]:
            if g["n"] >= SHOW_N and g["hr"] is not None:
                candidates.append(g)
    under = [g for g in candidates if g["n"] >= TICKET_N and g["hr"] < TICKET_HR]
    under.sort(key=lambda g: (-g["hr"], -g["n"]))
    report["best_short_of_ticket"] = under[:12]
    thin_ok = [g for g in candidates if g["hr"] >= TICKET_HR and g["n"] < TICKET_N]
    thin_ok.sort(key=lambda g: (-g["n"], -g["hr"]))
    report["thin_70"] = thin_ok[:12]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nclears_ticket n={len(report['clears_ticket'])} -> {OUT}")
    for g in report["clears_ticket"]:
        print(f"  {g['hr']:.1%} {g['hits']}/{g['n']}  {g['book']} {g['prop']}  {g['gate']}")


if __name__ == "__main__":
    main()
