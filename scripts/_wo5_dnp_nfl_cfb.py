"""NFL and CFB DNP regrade by ESPN MIN, plus the soccer shots on-row cell."""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import prop_hit_tiers as T  # noqa: E402
from _wo3_dnp import fold, load_minutes, lookup_minutes, num  # noqa: E402
from discover_gates_sport import graded_by_date, graded_candidates  # noqa: E402
from utils.cfb_keep_gates import REC_YDS, RUSH_YDS  # noqa: E402
from utils.dnp_zero_minutes import minutes_status  # noqa: E402
from utils.nfl_keep_gates import NFL_STD_UNDER_KEEP  # noqa: E402

OUT = ROOT / "data" / "reports" / "wo5_dnp_nfl_cfb.json"
LOG = ROOT / "data" / "reports" / "grade_corrections.jsonl"
ET = ZoneInfo("America/New_York")
GOBLIN_BE = 0.769
STANDARD_BE = 0.577


def wilson_low(hits: int, n: int) -> float | None:
    if n <= 0:
        return None
    z = 1.96
    p = hits / n
    denom = 1 + z * z / n
    centre = p + z * z / (2 * n)
    margin = z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)
    return (centre - margin) / denom


def pack(hits: int, n: int) -> dict:
    hr = (hits / n) if n else None
    low = wilson_low(hits, n)
    return {
        "hits": hits,
        "n": n,
        "hr": None if hr is None else round(hr, 4),
        "wilson_low": None if low is None else round(low, 4),
    }


def cell_key(sport: str, prop: str, pick: str, side: str) -> str | None:
    goblin = "goblin" in pick
    standard = "standard" in pick and not goblin
    if sport == "NFL" and standard and side == "UNDER" and prop in NFL_STD_UNDER_KEEP:
        return f"nfl_std_under_{prop}"
    if sport in {"CFB", "CFB1H"} and standard and side == "UNDER" and prop in {RUSH_YDS, REC_YDS}:
        return f"cfb_std_under_{prop}"
    if goblin and sport in {"NFL", "CFB", "CFB1H"} and side in {"OVER", "UNDER"}:
        family = "cfb" if sport.startswith("CFB") else "nfl"
        return f"{family}_goblin_{prop}_{side.lower()}"
    return None


def main() -> None:
    indexes = load_minutes()
    seen = set()
    if LOG.is_file():
        for line in LOG.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            seen.add(
                (
                    str(rec.get("date") or "")[:10],
                    fold(rec.get("player")),
                    str(rec.get("prop") or ""),
                    str(rec.get("line") or ""),
                    str(rec.get("reason") or ""),
                )
            )
    cells: dict[str, dict[str, int]] = defaultdict(
        lambda: {"bh": 0, "bn": 0, "ah": 0, "an": 0, "unknown": 0, "dnp": 0}
    )
    shots = {"rows": 0, "unknown": 0, "dnp": 0, "played": 0, "no_status": 0, "hit": 0}
    corrections = []
    files_changed = 0
    counted = set()
    for day in sorted(graded_by_date()):
      for path in graded_candidates(day):
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        changed = False
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            sport = T.norm_sport(str(row.get("sport") or "")) or ""
            prop = T.canon_prop(sport, str(row.get("prop") or ""))
            side = str(row.get("direction") or row.get("over_under") or "").upper()
            pick = str(row.get("pick_type") or "").lower()
            result = str(row.get("result") or "").upper()
            row_key = (
                day,
                fold(row.get("player")),
                prop,
                str(row.get("line") or ""),
                side,
                sport,
                pick,
            )
            if row_key in counted:
                continue
            counted.add(row_key)
            if sport == "Soccer" and prop == "shots" and side == "OVER":
                l5 = num(row.get("l5_over"))
                l10 = num(row.get("l10_over"))
                if l5 == 5 and (l10 or -1) >= 8 and result in {"HIT", "MISS", "NO_ACTUAL"}:
                    shots["rows"] += 1
                    looked = lookup_minutes(indexes, sport, str(row.get("player") or ""), day, prop)
                    status0 = minutes_status(looked) if row.get("minutes_status") in {None, ""} else str(row.get("minutes_status") or "")
                    if not row.get("minutes_status"):
                        status0 = minutes_status(looked)
                    if status0 == "minutes_unknown":
                        shots["unknown"] += 1
                    elif status0 == "dnp":
                        shots["dnp"] += 1
                    elif status0 == "played":
                        shots["played"] += 1
                    else:
                        shots["no_status"] += 1
                    if result == "HIT":
                        shots["hit"] += 1
            if sport not in {"NFL", "CFB", "CFB1H"}:
                continue
            name = cell_key(sport, prop, pick, side)
            actual = num(row.get("actual_value"))
            before_result = result
            status = str(row.get("minutes_status") or "")
            if actual == 0 and result in {"HIT", "MISS", "PUSH"}:
                minutes = lookup_minutes(indexes, sport, str(row.get("player") or ""), day, prop)
                status = minutes_status(minutes)
                if row.get("minutes_status") != status:
                    row["minutes_status"] = status
                    changed = True
                if status == "dnp" and result in {"HIT", "MISS"}:
                    key = (
                        day,
                        fold(row.get("player")),
                        str(row.get("prop") or ""),
                        str(row.get("line") or ""),
                        "dnp_zero_minutes",
                    )
                    if key not in seen:
                        corrections.append(
                            {
                                "date": day,
                                "player": row.get("player"),
                                "prop": row.get("prop"),
                                "direction": side,
                                "line": row.get("line"),
                                "sport": sport,
                                "old": result,
                                "new": "NO_ACTUAL",
                                "reason": "dnp_zero_minutes",
                                "version": 1,
                                "logged_at": datetime.now(ET).isoformat(timespec="seconds"),
                            }
                        )
                        seen.add(key)
                    row["result"] = "NO_ACTUAL"
                    row["void_reason"] = "dnp"
                    row["dnp_reason"] = "dnp_zero_minutes"
                    changed = True
                    result = "NO_ACTUAL"
            if name is None or before_result not in {"HIT", "MISS"}:
                continue
            bucket = cells[name]
            bucket["bh"] += 1 if before_result == "HIT" else 0
            bucket["bn"] += 1
            if status == "minutes_unknown":
                bucket["unknown"] += 1
            elif status == "dnp":
                bucket["dnp"] += 1
            else:
                bucket["ah"] += 1 if before_result == "HIT" else 0
                bucket["an"] += 1
        if changed:
            path.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
            files_changed += 1
    if corrections:
        with LOG.open("a", encoding="utf-8") as fh:
            for rec in corrections:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
    report_cells = {}
    for name, bucket in sorted(cells.items()):
        before = pack(bucket["bh"], bucket["bn"])
        after = pack(bucket["ah"], bucket["an"])
        shift = None
        if before["hr"] is not None and after["hr"] is not None:
            shift = round(after["hr"] - before["hr"], 4)
        be = GOBLIN_BE if "goblin" in name else STANDARD_BE
        cross = False
        if before["hr"] is not None and after["wilson_low"] is not None and before["wilson_low"] is not None:
            cross = (before["wilson_low"] - be) * (after["wilson_low"] - be) < 0
        # No stored numeric price on these NFL/CFB gates. Do not invent one.
        reprice = False
        report_cells[name] = {
            "before": before,
            "after": after,
            "shift": shift,
            "minutes_unknown_removed": bucket["unknown"],
            "dnp_removed": bucket["dnp"],
            "break_even": be,
            "reprice": reprice,
            "n_under_30": bucket["an"] < 30,
            "live_priced_cell": False,
        }
    payload = {
        "files_changed": files_changed,
        "corrections": len(corrections),
        "soccer_shots_l5eq5_l10ge8": shots,
        "cells": report_cells,
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({"files_changed": files_changed, "corrections": len(corrections), "shots": shots}, indent=2))
    for name, rec in report_cells.items():
        if rec["before"]["n"] == 0:
            continue
        print(
            f"{name} before {rec['before']['hits']}/{rec['before']['n']} "
            f"({rec['before']['hr']}) after {rec['after']['hits']}/{rec['after']['n']} "
            f"({rec['after']['hr']}) shift {rec['shift']} unknown {rec['minutes_unknown_removed']} "
            f"dnp {rec['dnp_removed']}"
        )


if __name__ == "__main__":
    main()
