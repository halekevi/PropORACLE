"""W9: Games Won / Total Games after unresolved retirements and walkovers are out."""
from __future__ import annotations

import json
import math
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import prop_hit_tiers as T  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402

OUT = ROOT / "data" / "reports" / "tennis_cell_rebuild.json"
LOG = ROOT / "data" / "reports" / "grade_corrections.jsonl"
ET = ZoneInfo("America/New_York")
PROPS = {"games_won", "match_total_games"}


def wilson(h, n):
    if n <= 0:
        return None
    z = 1.96
    p = h / n
    z2 = z * z
    denom = 1 + z2 / n
    center = p + z2 / (2 * n)
    spread = z * math.sqrt((p * (1 - p) + z2 / (4 * n)) / n)
    return round((center - spread) / denom, 4)


def files(root: Path):
    out = {}
    folder = root / "ui_runner" / "templates"
    if not folder.is_dir():
        return out
    for path in sorted(folder.glob("graded_props_20*.json")):
        if ".bak_" in path.name:
            continue
        out[path.stem.replace("graded_props_", "")[:10]] = path
    return out


def main() -> None:
    current = files(MAIN) or files(ROOT)
    other_root = files(ROOT) if current and next(iter(current.values())).is_relative_to(MAIN) else {}
    # Prefer the copy that is not the one we grade from when both exist.
    primary = files(MAIN) if (MAIN / "ui_runner" / "templates").is_dir() else files(ROOT)
    secondary = files(ROOT)
    cells = {
        prop: {"hits": 0, "n": 0, "unresolved_in_hitmiss": 0, "walkover_in_hitmiss": 0}
        for prop in PROPS
    }
    logged = 0
    walkover_were_graded = 0
    seen = set()
    if LOG.is_file():
        for line in LOG.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            seen.add((rec.get("date"), rec.get("player"), str(rec.get("line")), rec.get("reason")))
    new_logs = []
    for day, path in primary.items():
        raw = json.loads(path.read_text(encoding="utf-8"))
        old_raw = None
        other = secondary.get(day)
        if other and other.resolve() != path.resolve():
            try:
                old_raw = json.loads(other.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                old_raw = None
        old_index = {}
        if old_raw:
            for row in old_raw.get("props") or []:
                if not isinstance(row, dict):
                    continue
                if T.norm_sport(str(row.get("sport") or "")) != "Tennis":
                    continue
                prop = canon_prop("Tennis", str(row.get("prop") or ""))
                if prop not in PROPS:
                    continue
                try:
                    line = round(float(row.get("line")), 2)
                except (TypeError, ValueError):
                    continue
                old_index[(str(row.get("player") or "").casefold(), prop, line, str(row.get("direction") or "").upper())] = str(row.get("result") or "").upper()
        for row in raw.get("props") or []:
            if not isinstance(row, dict) or T.norm_sport(str(row.get("sport") or "")) != "Tennis":
                continue
            prop = canon_prop("Tennis", str(row.get("prop") or ""))
            if prop not in PROPS:
                continue
            res = str(row.get("result") or "").upper()
            reading = str(row.get("retirement_reading") or "")
            if res in {"HIT", "MISS"}:
                cells[prop]["n"] += 1
                cells[prop]["hits"] += int(res == "HIT")
                if reading == "readings_differ":
                    cells[prop]["unresolved_in_hitmiss"] += 1
                if reading == "walkover_chart_silent":
                    cells[prop]["walkover_in_hitmiss"] += 1
            if reading == "walkover_chart_silent" and old_index:
                try:
                    line = round(float(row.get("line")), 2)
                except (TypeError, ValueError):
                    continue
                old = old_index.get((str(row.get("player") or "").casefold(), prop, line, str(row.get("direction") or "").upper()))
                if old in {"HIT", "MISS"}:
                    walkover_were_graded += 1
                    key = (day, row.get("player"), str(row.get("line")), "walkover_chart_silent")
                    if key not in seen and res != old:
                        new_logs.append(
                            {
                                "date": day,
                                "player": row.get("player"),
                                "prop": row.get("prop"),
                                "direction": row.get("direction"),
                                "line": row.get("line"),
                                "sport": "Tennis",
                                "old": old,
                                "new": res or "UNRESOLVED",
                                "reason": "walkover_chart_silent",
                                "version": 1,
                                "logged_at": datetime.now(ET).isoformat(timespec="seconds"),
                            }
                        )
                        seen.add(key)
    if new_logs:
        with LOG.open("a", encoding="utf-8") as fh:
            for rec in new_logs:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")
        logged = len(new_logs)
    for prop, cell in cells.items():
        cell["hr"] = round(cell["hits"] / cell["n"], 4) if cell["n"] else None
        cell["wilson_low"] = wilson(cell["hits"], cell["n"])
    payload = {
        "cells_excluding_unresolved_and_walkover": cells,
        "walkover_rows_previously_hit_or_miss": walkover_were_graded,
        "walkover_corrections_logged": logged,
        "priced_games_won_cap": 0.62,
        "priced_total_games": 0.702,
        "same_copy": not any(
            secondary.get(d) and primary.get(d) and secondary[d].resolve() != primary[d].resolve()
            for d in primary
        ),
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
