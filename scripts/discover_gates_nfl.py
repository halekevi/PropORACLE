#!/usr/bin/env python3
"""Systematic gate discovery over graded NFL props (early-season).

Mirrors MLB discover_gates methodology with NFL constraints:
  - Unique player-game dedup (ladder boards inflate raw n)
  - Period / first-N props excluded (fabricated L5 closed this session)
  - Outcome leakage banned (actual_value, margin, hit, result, …)
  - Chronological OOS (last date held out when few slate dates)
  - Zero-actual / implausible-value sanity

Reality: Week 1–2 graded window is thin vs MLB. Ticket-bar clears are
candidates for a Week-2+ ledger, not auto-wires.

  py -3.14 scripts/discover_gates_nfl.py
  py -3.14 scripts/discover_gates_nfl.py --min-n 12 --top 60
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
from discover_gates_sport import _file_has_usable_props, graded_candidates  # noqa: E402
from utils.nfl_unsupported_props import is_football_period_split_prop  # noqa: E402

OUT_DIR = _REPO / "data" / "reports"
OUT_JSON = OUT_DIR / "discover_gates_nfl_ranked.json"
OUT_CSV = OUT_DIR / "discover_gates_nfl_ranked.csv"
OUT_EXPORT = OUT_DIR / "discover_gates_nfl_export.csv"
OUT_SANITY = OUT_DIR / "discover_gates_nfl_sanity.json"
OUT_NOTE = OUT_DIR / "discover_gates_nfl_run.md"

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
TICKET_HR = 0.70
# Early season: show thin candidates; ticket_clear still needs n>=40 when possible
SHOW_N = 12
TICKET_N = 25  # softer than MLB 40 given Week-1 volume
PROD = frozenset({"Weak", "Below Avg"})
STINGY = frozenset({"Elite", "Above Avg"})

RECENCY = ("L5>=4", "L5=5", "L10>=8", "L10>=9", "L10=10")
TIERS = ("D_ok", "D_weak", "D_elite")
CUSHION = ("cover>=1", "cover>=8", "cover>=15", "season_HR>70%")
LINES = ("line_0.5", "line_1.5", "line_ge10", "line_ge20", "line_ge40")


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


def find_step8(date: str) -> Path | None:
    for root in (MAIN, _REPO):
        d = root / "outputs" / date / "nfl"
        for name in (
            f"step8_nfl_direction_clean_{date}.xlsx",
            "step8_nfl_direction_clean.xlsx",
            f"step8_nfl_direction_{date}.csv",
            "step8_nfl_direction.csv",
        ):
            p = d / name
            if p.exists():
                return p
    return None


def load_step8_index(path: Path) -> dict[tuple, dict]:
    try:
        if path.suffix.lower() in (".xlsx", ".xls"):
            df = __import__("pandas").read_excel(path)
        else:
            df = __import__("pandas").read_csv(path, low_memory=False)
    except Exception:
        return {}
    # normalize column names
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
    c_dir = col("direction", "Direction", "bet_direction")
    c_l5o = col("l5 over", "L5 Over", "last5_over", "l5_over")
    c_l5u = col("l5 under", "L5 Under", "last5_under", "l5_under")
    c_l10o = col("l10 over", "L10 Over", "l10_over")
    c_l10u = col("l10 under", "L10 Under", "l10_under")
    c_def = col("def tier", "Def Tier", "def_tier")
    c_cover = col("abs edge", "Abs Edge", "cover", "dist_l5", "avg_L5_vs_line")

    out: dict[tuple, dict] = {}
    for row in df.to_dict("records"):
        prop = T.canon_prop("NFL", str(row.get(c_prop) or ""))
        if not prop or is_football_period_split_prop(prop):
            continue
        side = _side(row.get(c_dir) if c_dir else None)
        if side is None:
            continue
        pick = "Goblin" if "goblin" in str(row.get(c_pick) or "").lower() else (
            "Demon" if "demon" in str(row.get(c_pick) or "").lower() else "Standard"
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
    """Keep one row per (date, player, prop, side, book); prefer median-line."""
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


def iter_nfl_rows() -> tuple[list[dict], dict]:
    by_date = graded_by_date()
    rows: list[dict] = []
    skipped_period = 0
    skipped_nflp = 0
    for date in sorted(by_date):
        raw = None
        for path in graded_candidates(date) or [by_date[date]]:
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            raw = loaded
            if _file_has_usable_props(loaded, frozenset({"NFL"}), "NFL"):
                break
        if raw is None:
            continue
        step_path = find_step8(date)
        step_idx = load_step8_index(step_path) if step_path else {}
        for r in raw.get("props") or []:
            if not isinstance(r, dict):
                continue
            sport = T.norm_sport(str(r.get("sport") or ""))
            if sport == "NFLP":
                skipped_nflp += 1
                continue
            if sport != "NFL":
                continue
            res = str(r.get("result") or "").strip().upper()
            if res not in ("HIT", "MISS"):
                continue
            if str(r.get("minutes_status") or "") == "minutes_unknown":
                continue
            prop = T.canon_prop("NFL", str(r.get("prop") or r.get("prop_type") or ""))
            if not prop:
                continue
            if is_football_period_split_prop(prop) or is_football_period_split_prop(
                r.get("prop")
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
                "l5_over": r.get("l5_over") if r.get("l5_over") not in (None, "") else s8.get("l5_over"),
                "l5_under": r.get("l5_under") if r.get("l5_under") not in (None, "") else s8.get("l5_under"),
                "l10_over": r.get("l10_over") if r.get("l10_over") not in (None, "") else s8.get("l10_over"),
                "l10_under": r.get("l10_under") if r.get("l10_under") not in (None, "") else s8.get("l10_under"),
                "def_tier": r.get("def_tier") or r.get("def") or s8.get("def_tier") or "",
                "cover": r.get("cover") or r.get("dist_l5") or s8.get("cover"),
                "player_hr_historical": r.get("player_hr_historical"),
                "edge": _num(r.get("edge")),
                "ml_prob": _num(r.get("ml_prob")),
            }
            # last5 aliases
            if rec["l5_over"] in (None, ""):
                rec["l5_over"] = r.get("last5_over")
            if rec["l5_under"] in (None, ""):
                rec["l5_under"] = r.get("last5_under")
            rows.append(rec)

    meta = {
        "skipped_period": skipped_period,
        "skipped_nflp": skipped_nflp,
        "raw_n": len(rows),
    }
    deduped = unique_game_dedupe(rows)
    meta["unique_n"] = len(deduped)
    meta["dedupe_drop"] = len(rows) - len(deduped)
    return deduped, meta


def directional_cover(rec: dict) -> float | None:
    cover = _num(rec.get("cover"))
    if cover is not None:
        return abs(cover)  # Abs Edge on NFL boards
    # synthesize from nothing else available
    return None


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
        "cover>=8": cover is not None and cover >= 8.0,
        "cover>=15": cover is not None and cover >= 15.0,
        "season_HR>70%": season_hr is not None and season_hr > 0.70,
        "line_0.5": line is not None and abs(line - 0.5) < 1e-9,
        "line_1.5": line is not None and abs(line - 1.5) < 1e-9,
        "line_ge10": line is not None and line >= 10,
        "line_ge20": line is not None and line >= 20,
        "line_ge40": line is not None and line >= 40,
    }


def gate_catalog() -> list[tuple[str, tuple[str, ...]]]:
    gates: list[tuple[str, tuple[str, ...]]] = [("all", ())]
    atoms = RECENCY + TIERS + CUSHION + LINES
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
        ("L5=5 + cover>=8", ("L5=5", "cover>=8")),
        ("L10>=8 + cover>=8", ("L10>=8", "cover>=8")),
        ("L5=5 + D_ok + cover>=8", ("L5=5", "D_ok", "cover>=8")),
        ("L5=5 + line_0.5", ("L5=5", "line_0.5")),
        ("L5>=4 + line_ge20", ("L5>=4", "line_ge20")),
        ("D_ok + line_ge20", ("D_ok", "line_ge20")),
        ("L5=5 + D_ok + line_ge20", ("L5=5", "D_ok", "line_ge20")),
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
        # hold out the last slate date only
        return set(dates[:-1]), {dates[-1]}
    cut = max(1, int(round(len(dates) * 0.70)))
    return set(dates[:cut]), set(dates[cut:])


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
            "period_prop": bool(is_football_period_split_prop(prop)),
        }
        reasons = []
        if cell["period_prop"]:
            reasons.append("period_split_prop_should_be_excluded")
        if zero_rate is not None and zero_rate >= 0.35 and "UNDER" in book:
            reasons.append(f"zero_actual_under_inflation zero_rate={zero_rate:.1%}")
        if (
            ratio is not None
            and "OVER" in book
            and mean_line is not None
            and mean_line >= 5
            and ratio < 0.35
        ):
            reasons.append(f"actual_<<_line ratio={ratio}")
        if reasons:
            cell["flags"] = reasons
            flagged.append(cell)
        cells.append(cell)
    flagged.sort(key=lambda c: (-(c.get("zero_rate") or 0), -c["n"]))
    return {"cells": cells, "flagged": flagged}


def eval_pool(pool, gates, train_dates, test_dates, min_n):
    for r in pool:
        r["_atoms"] = build_atoms(r)
    base_n = len(pool)
    base_h = sum(r["hit"] for r in pool)
    base_hr = rate(base_h, base_n)
    results = []
    for name, needed in gates:
        n = h = n_tr = h_tr = n_te = h_te = 0
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
        results.append(
            {
                "gate": name,
                "atoms": list(needed) if needed else ["all"],
                "n": n,
                "hits": h,
                "hr": hr,
                "wilson_low": wilson_low(h, n),
                "lift_vs_base": None if base_hr is None or hr is None else round(hr - base_hr, 4),
                "base_hr": base_hr,
                "base_n": base_n,
                "train_n": n_tr,
                "train_hr": rate(h_tr, n_tr),
                "oos_n": n_te,
                "oos_hr": rate(h_te, n_te),
                "ticket_clear": bool(hr is not None and hr >= TICKET_HR and n >= TICKET_N),
                "oos_ticket_clear": bool(
                    rate(h_te, n_te) is not None
                    and rate(h_te, n_te) >= TICKET_HR
                    and n_te >= max(8, TICKET_N // 3)
                ),
            }
        )
    results.sort(key=lambda g: (-int(g["ticket_clear"]), -(g["hr"] or 0), -g["n"]))
    return results


def write_export(rows: list[dict], path: Path) -> None:
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
        "hit",
        "actual_value",
        "l5",
        "l10",
        "def_tier",
        "cover_abs",
        "player_hr_historical",
        "edge",
        "ml_prob",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            side = r["side"]
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
                    "def_tier": r.get("def_tier"),
                    "cover_abs": directional_cover(r),
                    "player_hr_historical": r.get("player_hr_historical"),
                    "edge": r.get("edge"),
                    "ml_prob": r.get("ml_prob"),
                }
            )


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--min-n", type=int, default=SHOW_N)
    ap.add_argument("--top", type=int, default=50)
    args = ap.parse_args()

    print("loading graded NFL (unique-game dedupe, period props excluded)...")
    rows, meta = iter_nfl_rows()
    dates = sorted({r["date"] for r in rows})
    train_dates, test_dates = oos_split_dates(dates)
    print(
        f"unique_rows={len(rows):,} raw={meta['raw_n']:,} dedupe_drop={meta['dedupe_drop']} "
        f"skipped_period={meta['skipped_period']} skipped_nflp={meta['skipped_nflp']} "
        f"dates={len(dates)} ({dates[0] if dates else '?'} -> {dates[-1] if dates else '?'}) "
        f"train={sorted(train_dates)} oos={sorted(test_dates)}"
    )
    if len(dates) < 8:
        print(
            "NOTE: early-season window — OOS is thin; treat ticket_clear as "
            "Week-2+ candidates, not auto-wires."
        )

    cov = {
        "l5": sum(1 for r in rows if C.l5_dir(r, r["side"]) is not None),
        "l10": sum(1 for r in rows if C.l10_dir(r, r["side"]) is not None),
        "def": sum(1 for r in rows if str(r.get("def_tier") or "").strip()),
        "cover": sum(1 for r in rows if directional_cover(r) is not None),
        "actual": sum(1 for r in rows if r.get("actual_value") is not None),
    }
    print("coverage:", {k: f"{v}/{len(rows)}" for k, v in cov.items()})

    write_export(rows, OUT_EXPORT)
    print(f"export -> {OUT_EXPORT}")

    sanity = sanity_report(rows)
    split_bal = split_balance_by_cell(rows, train_dates, test_dates)
    split_flags = flagged_split_imbalance(split_bal)
    sanity["split_imbalance"] = split_flags
    OUT_SANITY.write_text(json.dumps(sanity, indent=2), encoding="utf-8")
    print(f"sanity flagged: {len(sanity['flagged'])} -> {OUT_SANITY}")
    print(f"split-imbalance cells: {len(split_flags)}")
    for f in sanity["flagged"][:15]:
        print(
            f"  FLAG {f['book']} {f['prop']}: zero={f.get('zero_rate')} "
            f"ratio={f.get('actual_over_line')} {f.get('flags')}"
        )
    for f in split_flags[:12]:
        print(
            f"  SPLIT {f['book']} {f['prop']}: "
            f"train={f['train_n']}/{f['n']} oos={f['oos_n']}/{f['n']} "
            f"| {f['flags'][0] if f.get('flags') else ''}"
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
    atom_set = set(RECENCY + TIERS + CUSHION + LINES)
    if atom_set & LEAKAGE_COLS:
        raise SystemExit("leakage atoms slipped in")
    print(f"gates scanned per cell: {len(unique_gates)}")

    by_cell: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for r in rows:
        by_cell[(r["book"], r["prop"])].append(r)

    ranked: list[dict] = []
    report_cells = []
    for (book, prop), pool in sorted(by_cell.items(), key=lambda x: -len(x[1])):
        if len(pool) < args.min_n:
            continue
        bal = split_bal.get((book, prop)) or split_balance_for_cell(
            pool, train_dates, test_dates
        )
        cell_gates = eval_pool(pool, unique_gates, train_dates, test_dates, args.min_n)
        for g in cell_gates:
            g["oos_split_reliable"] = bool(bal.get("oos_split_reliable", True))
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
    # Same stamp as discover_gates_sport: pick-type break-even and the
    # gate-level split flag when the holdout is 70% or more of the cell.
    GOBLIN_BE = (0.845, 0.769)
    STANDARD_BE = (0.577, 0.550)
    for g in headline:
        book = str(g.get("book") or "")
        be2, be3 = STANDARD_BE if "standard" in book.lower() else GOBLIN_BE
        g["break_even_2leg"] = be2
        g["break_even_3leg"] = be3
        reasons: list[str] = []
        oos_n = int(g.get("oos_n") or 0)
        cell_n = int(g.get("n") or 0)
        if not g.get("oos_split_reliable", True) or (cell_n and oos_n / cell_n >= 0.70):
            reasons.append("split_balance")
        if oos_n == 0:
            reasons.append("holdout_empty")
        if cell_n < 30:
            reasons.append("n_under_30")
        low = g.get("wilson_low")
        if low is not None and float(low) < be3:
            reasons.append("wilson_under_3leg_breakeven")
        g["unreliable"] = bool(reasons)
        g["unreliable_reasons"] = reasons

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "sport": "NFL",
        "n_rows_unique": len(rows),
        "meta": meta,
        "dates": dates,
        "train_dates": sorted(train_dates),
        "oos_dates": sorted(test_dates),
        "ticket_bar": {"hr": TICKET_HR, "n": TICKET_N},
        "leakage_excluded": sorted(LEAKAGE_COLS),
        "feature_atoms": list(RECENCY + TIERS + CUSHION + LINES),
        "coverage": cov,
        "sanity_flagged_n": len(sanity["flagged"]),
        "sanity_flagged": sanity["flagged"][:30],
        "split_imbalance_n": len(split_flags),
        "split_imbalance": split_flags[:40],
        "n_gates": len(unique_gates),
        "ranked": headline[:400],
        "cells": report_cells,
        "caveat": (
            "Early-season graded window only. Do not wire from pooled clears "
            "without OOS hold and unique-game n. Period/split props excluded. "
            "oos_ticket_clear requires oos_split_reliable (prop not skewed into "
            "the global holdout)."
        ),
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
            "oos_ticket_clear_raw",
            "oos_split_reliable",
        ]
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for g in headline:
            w.writerow(g)

    note = f"""# NFL discover_gates run

Unique-game graded NFL (period/split excluded). Window {dates[0] if dates else '?'} → {dates[-1] if dates else '?'}.

- unique n={len(rows)} (raw {meta['raw_n']}, dropped {meta['dedupe_drop']} ladder dupes)
- skipped period props: {meta['skipped_period']}
- OOS dates: {', '.join(sorted(test_dates)) or 'n/a'}
- Ticket bar used: HR≥{TICKET_HR:.0%} and n≥{TICKET_N} (softer than MLB 40 due to early season)
- Split-imbalance cells (late-arriving props vs global split): {len(split_flags)}

**Do not auto-wire.** Re-check any clear on OOS + unique n after more weeks.
`oos_ticket_clear` is cleared when `oos_split_reliable` is false.

Exports: `discover_gates_nfl_export.csv`, `discover_gates_nfl_ranked.csv`, `discover_gates_nfl_sanity.json`.
"""
    OUT_NOTE.write_text(note, encoding="utf-8")

    print(f"\nranked -> {OUT_CSV}")
    print(f"json   -> {OUT_JSON}")
    print(f"\n=== TOP {args.top} ===")
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
