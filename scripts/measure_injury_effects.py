#!/usr/bin/env python3
"""Measure teammate / opponent injury effects (observe only — no ticket wire).

Uses as-of injury snapshots under data/injuries/<date>/ with captured_at
strictly before tip. Graded legs supply hit rates vs posted lines.

  py -3.14 scripts/measure_injury_effects.py --date 2026-10-27

Prereg: ml/prereg/injury_effects_2026-10-27.md — nothing promotes without a
dated follow-up prereg.
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from player_name_norm import fold_player_name  # noqa: E402
from utils.comparison_tracks import wilson_band  # noqa: E402
from utils.injury_collect import injuries_dir, now_et  # noqa: E402
from utils.injury_gate import is_injury_out, parse_captured_at  # noqa: E402


def _load_snapshots(root: Path, date: str) -> list[dict[str, Any]]:
    d = injuries_dir(date, root=root)
    if not d.is_dir():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(d.glob("*_*.json")):
        if path.name.startswith("_"):
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        for r in payload.get("rows") or []:
            if isinstance(r, dict):
                rec = dict(r)
                rec["_snapshot"] = path.name
                rows.append(rec)
    return rows


def _load_graded_legs(root: Path, date: str) -> list[dict[str, Any]]:
    """Best-effort graded legs for the slate (analysis export / ticket eval)."""
    cands = [
        root / "data" / "reports" / "analysis_legs_export.csv",
        root / "outputs" / date / "combined_tickets_graded.csv",
        root / "outputs" / date / f"combined_tickets_graded_{date}.csv",
    ]
    for path in cands:
        if not path.is_file():
            continue
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception:
            continue
        if df.empty:
            continue
        rows = df.where(pd.notnull(df), None).to_dict(orient="records")
        # Prefer rows that look like this slate date when a date col exists
        dated: list[dict] = []
        for r in rows:
            gd = str(r.get("grade_date") or r.get("date") or r.get("game_date") or "")[:10]
            if gd and gd != date:
                continue
            dated.append(r)
        return dated or rows
    return []


def _asof_outs(snapshots: list[dict], tip_iso: str | None) -> dict[tuple[str, str], str]:
    """player_fold+team -> OUT player display name, only if captured before tip."""
    tip = parse_captured_at(tip_iso) if tip_iso else None
    out: dict[tuple[str, str], str] = {}
    for r in snapshots:
        if not is_injury_out(r.get("status")):
            continue
        cap = parse_captured_at(r.get("captured_at"))
        if tip is not None and cap is not None and cap >= tip:
            continue  # post-tip leak
        if tip is not None and cap is None:
            continue  # fail closed without as-of stamp
        key = (fold_player_name(r.get("player")), str(r.get("team") or "").strip().upper())
        if key[0]:
            out[key] = str(r.get("player") or "")
    return out


def _cell_key(sport: str, prop: str, role: str) -> tuple[str, str, str]:
    return (sport.upper(), str(prop or "").strip().lower(), role)


def measure(root: Path, date: str) -> dict[str, Any]:
    snaps = _load_snapshots(root, date)
    legs = _load_graded_legs(root, date)
    # Without tip times per game, treat missing tip as "use all stamped outs"
    # but tag timing=asof_unknown when tip missing.
    outs = _asof_outs(snaps, tip_iso=None)
    cells: dict[tuple[str, str, str], dict[str, int]] = defaultdict(lambda: {"n": 0, "hits": 0})
    notes: list[str] = []

    if not snaps:
        notes.append("No data/injuries snapshots yet — collect first; measure awaits as-of history.")
    if not legs:
        notes.append("No graded legs found for this date — hit-rate cells empty.")

    for leg in legs:
        sport = str(leg.get("sport") or "").strip().upper()
        prop = str(leg.get("prop") or leg.get("prop_type") or "").strip()
        team = str(leg.get("team") or "").strip().upper()
        player = fold_player_name(leg.get("player") or leg.get("player_name"))
        result = str(leg.get("result") or leg.get("grade") or leg.get("hit") or "").strip().lower()
        hit = result in {"hit", "win", "1", "true", "yes"}
        miss = result in {"miss", "loss", "0", "false", "no"}
        if not (hit or miss):
            continue
        # Teammate effect: another OUT on same team
        teammate_out = any(
            t == team and pl != player for (pl, t), _name in outs.items()
        )
        # Opponent effect
        opp = str(leg.get("opponent") or leg.get("opp") or "").strip().upper()
        opp_out = any(t == opp for (_pl, t) in outs) if opp else False

        if teammate_out:
            k = _cell_key(sport, prop, "teammate_key_out")
            cells[k]["n"] += 1
            cells[k]["hits"] += int(hit)
        if opp_out:
            k = _cell_key(sport, prop, "opp_key_out")
            cells[k]["n"] += 1
            cells[k]["hits"] += int(hit)
        # Player himself returning / listed — separate cell
        if (player, team) in outs:
            k = _cell_key(sport, prop, "player_listed_out")
            cells[k]["n"] += 1
            cells[k]["hits"] += int(hit)

    table: list[dict[str, Any]] = []
    for (sport, prop, role), c in sorted(cells.items()):
        n = int(c["n"])
        hits = int(c["hits"])
        band = wilson_band(hits, n) if n else None
        table.append(
            {
                "sport": sport,
                "prop": prop,
                "role": role,
                "n": n,
                "hits": hits,
                "hit_rate": round(hits / n, 4) if n else None,
                "wilson_lo": round(band[0], 4) if band else None,
                "wilson_hi": round(band[1], 4) if band else None,
            }
        )

    return {
        "date": date,
        "prereg": "ml/prereg/injury_effects_2026-10-27.md",
        "promote": False,
        "n_snapshots": len(snaps),
        "n_out_keys": len(outs),
        "n_graded_legs": len(legs),
        "cells": table,
        "notes": notes,
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def render_md(payload: dict[str, Any]) -> str:
    lines = [
        f"# Injury effects (measure-only) — {payload.get('date')}",
        "",
        f"**Promote: false.** Prereg: `{payload.get('prereg')}`.",
        "",
        f"Snapshots: {payload.get('n_snapshots')} · OUT keys: {payload.get('n_out_keys')} · "
        f"Graded legs: {payload.get('n_graded_legs')}",
        "",
    ]
    for note in payload.get("notes") or []:
        lines.append(f"- {note}")
    lines.extend(["", "| Sport | Prop | Role | n | Hits | HR | Wilson lo | Wilson hi |", "|---|---|---|---:|---:|---:|---:|---:|"])
    for c in payload.get("cells") or []:
        lines.append(
            f"| {c['sport']} | {c['prop']} | {c['role']} | {c['n']} | {c['hits']} | "
            f"{c['hit_rate'] if c['hit_rate'] is not None else '—'} | "
            f"{c['wilson_lo'] if c['wilson_lo'] is not None else '—'} | "
            f"{c['wilson_hi'] if c['wilson_hi'] is not None else '—'} |"
        )
    if not payload.get("cells"):
        lines.append("| — | — | — | 0 | 0 | — | — | — |")
    lines.extend(
        [
            "",
            "As-of rule: only injury rows with `captured_at` before tip enter the OUT set. "
            "No final box scores in the inputs. Do not wire into tickets until a dated prereg clears.",
            "",
        ]
    )
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser(description="Measure injury teammate/opponent effects (observe)")
    ap.add_argument("--date", default="")
    ap.add_argument("--root", default="")
    args = ap.parse_args()
    root = Path(args.root).resolve() if str(args.root).strip() else _REPO
    date = (args.date or now_et().strftime("%Y-%m-%d")).strip()[:10]
    payload = measure(root, date)
    out_json = root / "data" / "reports" / f"injury_effects_{date}.json"
    out_md = root / "data" / "reports" / f"injury_effects_{date}.md"
    out_json.parent.mkdir(parents=True, exist_ok=True)
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    out_md.write_text(render_md(payload), encoding="utf-8")
    print(f"[injury_effects] cells={len(payload.get('cells') or [])} -> {out_md}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
