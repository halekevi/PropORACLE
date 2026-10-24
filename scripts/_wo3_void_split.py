"""Leg-level void split. Only a missing actual is fetch_gap."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
OUT = ROOT / "data" / "reports" / "void_reason_split.json"


def files():
    by = {}
    for folder in (MAIN / "ui_runner" / "templates", ROOT / "ui_runner" / "templates"):
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_20*.json")):
            if ".bak_" in path.name:
                continue
            by.setdefault(path.stem.replace("graded_props_", "")[:10], path)
    return by


def bucket(row) -> str | None:
    res = str(row.get("result") or "").upper()
    reason = str(row.get("void_reason") or row.get("dnp_reason") or "").lower()
    reading = str(row.get("retirement_reading") or "")
    if reading == "walkover_chart_silent" or "walkover" in reason:
        return "walkover"
    if reading == "readings_differ" or "retir" in reason:
        return "retirement"
    if (
        reason == "dnp"
        or "dnp" in reason
        or row.get("dnp_reason") == "dnp_zero_minutes"
    ):
        return "dnp"
    if "postpon" in reason or "cancel" in reason:
        return "postponed"
    if res not in {"VOID", "NO_ACTUAL", "UNRESOLVED"} and not reading:
        return None
    if any(
        tok in reason
        for tok in ("no_data", "fetch", "no_match", "no_actual_match", "no_projection", "incomplete")
    ):
        return "fetch_gap"
    if res in {"VOID", "NO_ACTUAL", "UNRESOLVED"} and reason.strip() in {"", "nan", "none"}:
        if res == "UNRESOLVED":
            return None
        return "fetch_gap"
    return None


def main() -> None:
    counts = Counter()
    other = Counter()
    for path in files().values():
        raw = json.loads(path.read_text(encoding="utf-8"))
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            name = bucket(row)
            if name:
                counts[name] += 1
            else:
                res = str(row.get("result") or "").upper()
                if res in {"VOID", "NO_ACTUAL", "UNRESOLVED"}:
                    other[str(row.get("void_reason") or res)[:80]] += 1
    payload = {
        "dnp": counts["dnp"],
        "postponed": counts["postponed"],
        "retirement": counts["retirement"],
        "walkover": counts["walkover"],
        "fetch_gap": counts["fetch_gap"],
        "other_unsettled_top": other.most_common(8),
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))


if __name__ == "__main__":
    main()
