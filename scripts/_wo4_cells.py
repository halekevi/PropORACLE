"""Work order 4: DNP before/after cells, and fetch_gap by sport."""
from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
import sys

sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import prop_hit_tiers as T  # noqa: E402

OUT = ROOT / "data" / "reports" / "wo4_dnp_fetch.json"
LOG = ROOT / "data" / "reports" / "grade_corrections.jsonl"

GOBLIN_BE3 = 0.769
STANDARD_BE2 = 0.577
CLOSED = {
    "soccer_saves_l5ge4",
    "tennis_gw_goblin",
    "tennis_gw_discount",
}


def num(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:
        return None
    return out


def wilson_low(hits: int, n: int) -> float | None:
    if n <= 0:
        return None
    z = 1.96
    p = hits / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (centre - margin) / denom


def dates() -> list[str]:
    found = set()
    for base in (MAIN, ROOT):
        folder = base / "ui_runner" / "templates"
        if not folder.is_dir():
            continue
        for path in folder.glob("graded_props_20*.json"):
            if ".bak_" in path.name:
                continue
            found.add(path.stem.replace("graded_props_", "")[:10])
    return sorted(found)


def rows_for(day: str) -> list[dict]:
    blobs = []
    name = f"graded_props_{day}.json"
    for base in (MAIN, ROOT):
        path = base / "ui_runner" / "templates" / name
        if path.is_file():
            try:
                blobs.append(json.loads(path.read_text(encoding="utf-8")))
            except Exception:
                continue
    best: dict[str, tuple[int, list]] = {}
    for blob in blobs:
        grouped: dict[str, list] = defaultdict(list)
        for row in blob.get("props") or []:
            if isinstance(row, dict):
                sport = T.norm_sport(str(row.get("sport") or "")) or "UNKNOWN"
                grouped[sport].append(row)
        for sport, rows in grouped.items():
            if len(rows) > best.get(sport, (0, []))[0]:
                best[sport] = (len(rows), rows)
    out = []
    for _n, rows in best.values():
        out.extend(rows)
    return out


def dnp_old() -> dict[tuple, str]:
    old = {}
    if not LOG.is_file():
        return old
    for line in LOG.read_text(encoding="utf-8").splitlines():
        if "dnp_zero_minutes" not in line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if rec.get("reason") != "dnp_zero_minutes":
            continue
        key = (
            str(rec.get("date") or "")[:10],
            str(rec.get("player") or ""),
            str(rec.get("prop") or ""),
            str(rec.get("line") or ""),
            str(rec.get("direction") or "").upper(),
        )
        old[key] = str(rec.get("old") or "").upper()
    return old


def l5(row, side: str):
    key = "l5_under" if side == "UNDER" else "l5_over"
    v = num(row.get(key))
    if v is None:
        v = num(row.get("last5_under" if side == "UNDER" else "last5_over"))
    return v


def l10(row, side: str):
    key = "l10_under" if side == "UNDER" else "l10_over"
    return num(row.get(key))


def cell_names(sport: str, prop: str, pick: str, side: str, row) -> list[str]:
    names = []
    goblin = "goblin" in pick
    standard = "standard" in pick and not goblin
    if sport == "Soccer" and prop == "shots" and side == "OVER":
        if l5(row, side) == 5 and (l10(row, side) or -1) >= 8:
            names.append("soccer_shots_l5eq5_l10ge8")
    if sport == "Soccer" and prop == "sog" and side == "OVER":
        if (l5(row, side) or -1) >= 4:
            names.append("soccer_sot_l5ge4")
    if sport == "Soccer" and prop == "saves" and side == "OVER":
        if (l5(row, side) or -1) >= 4:
            names.append("soccer_saves_l5ge4")
    if sport == "Soccer" and prop == "fouls" and goblin and side == "OVER":
        line = num(row.get("line"))
        if line is not None and abs(line - 0.5) < 1e-9:
            names.append("soccer_fouls_goblin_line05")
    if sport in {"NHL", "WNBA", "MLB"} and (goblin or standard) and side in {"OVER", "UNDER"}:
        if l5(row, side) == 5 and (l10(row, side) or -1) >= 8:
            book = "goblin" if goblin else "standard"
            names.append(f"{sport.lower()}_{prop}_{book}_{side.lower()}_l5eq5_l10ge8")
    return names


def fetch_gap(row) -> bool:
    res = str(row.get("result") or "").upper()
    reason = str(row.get("void_reason") or row.get("dnp_reason") or "").lower()
    reading = str(row.get("retirement_reading") or "")
    if reading in {"walkover_chart_silent", "readings_differ"}:
        return False
    if "walkover" in reason or "retir" in reason or "dnp" in reason or "postpon" in reason:
        return False
    if res not in {"VOID", "NO_ACTUAL", "UNRESOLVED"}:
        return False
    if any(tok in reason for tok in ("no_data", "fetch", "no_match", "no_actual_match", "no_projection", "incomplete")):
        return True
    return res in {"VOID", "NO_ACTUAL"} and reason.strip() in {"", "nan", "none"}


def keep_shape(row) -> bool:
    side = str(row.get("direction") or row.get("over_under") or "").upper()
    pick = str(row.get("pick_type") or "").lower()
    if "demon" in pick:
        return False
    if side not in {"OVER", "UNDER"}:
        return False
    return l5(row, side) == 5 and (l10(row, side) or -1) >= 8


def pack(hits: int, n: int) -> dict:
    hr = (hits / n) if n else None
    low = wilson_low(hits, n)
    return {
        "hits": hits,
        "n": n,
        "hr": None if hr is None else round(hr, 4),
        "wilson_low": None if low is None else round(low, 4),
    }


def main() -> None:
    restored = dnp_old()
    cells: dict[str, dict[str, int]] = defaultdict(lambda: {"bh": 0, "bn": 0, "ah": 0, "an": 0, "unknown": 0})
    gap_sport = Counter()
    gap_keep = Counter()
    for day in dates():
        for row in rows_for(day):
            sport = T.norm_sport(str(row.get("sport") or "")) or "UNKNOWN"
            if fetch_gap(row):
                gap_sport[sport] += 1
                if keep_shape(row):
                    gap_keep[sport] += 1
            prop = T.canon_prop(sport, str(row.get("prop") or ""))
            side = str(row.get("direction") or row.get("over_under") or "").upper()
            pick = str(row.get("pick_type") or "").lower()
            names = cell_names(sport, prop, pick, side, row)
            if not names:
                continue
            result = str(row.get("result") or "").upper()
            status = str(row.get("minutes_status") or "")
            key = (day, str(row.get("player") or ""), str(row.get("prop") or ""), str(row.get("line") or ""), side)
            before_hit = None
            if result in {"HIT", "MISS"}:
                before_hit = 1 if result == "HIT" else 0
            elif result == "NO_ACTUAL" and restored.get(key) in {"HIT", "MISS"}:
                before_hit = 1 if restored[key] == "HIT" else 0
            if before_hit is None:
                continue
            unknown = status == "minutes_unknown" or (result in {"HIT", "MISS"} and status == "minutes_unknown")
            played = result in {"HIT", "MISS"} and status not in {"dnp", "minutes_unknown"}
            for name in names:
                bucket = cells[name]
                bucket["bh"] += before_hit
                bucket["bn"] += 1
                if unknown and result in {"HIT", "MISS"}:
                    bucket["unknown"] += 1
                if played:
                    bucket["ah"] += before_hit
                    bucket["an"] += 1
    report_cells = {}
    repriced = []
    for name, bucket in sorted(cells.items()):
        before = pack(bucket["bh"], bucket["bn"])
        after = pack(bucket["ah"], bucket["an"])
        shift = None
        if before["hr"] is not None and after["hr"] is not None:
            shift = round(after["hr"] - before["hr"], 4)
        be = STANDARD_BE2 if "standard" in name else GOBLIN_BE3
        cross = False
        if before["hr"] is not None and after["wilson_low"] is not None:
            cross = (before["hr"] - be) * (after["wilson_low"] - be) < 0
        reprice = (
            name not in CLOSED
            and bucket["an"] >= 30
            and shift is not None
            and (abs(shift) > 0.03 or cross)
        )
        if reprice:
            repriced.append(name)
        report_cells[name] = {
            "before": before,
            "after": after,
            "shift": shift,
            "minutes_unknown_removed": bucket["unknown"],
            "break_even": be,
            "reprice": reprice,
        }
    gap_total = sum(gap_sport.values())
    keep_total = sum(gap_keep.values())
    payload = {
        "cells": report_cells,
        "repriced": repriced,
        "fetch_gap": gap_total,
        "fetch_gap_by_sport": dict(gap_sport),
        "fetch_gap_keep_shape": keep_total,
        "fetch_gap_keep_shape_by_sport": dict(gap_keep),
        "fetch_gap_keep_shape_rate": (keep_total / gap_total) if gap_total else None,
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({k: payload[k] for k in payload if k != "cells"}, indent=2))
    for name, rec in report_cells.items():
        if rec["before"]["n"] == 0:
            continue
        print(
            f"{name} before {rec['before']['hits']}/{rec['before']['n']} "
            f"after {rec['after']['hits']}/{rec['after']['n']} "
            f"shift {rec['shift']} unknown {rec['minutes_unknown_removed']} reprice {rec['reprice']}"
        )


if __name__ == "__main__":
    main()
