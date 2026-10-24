"""How many graded soccer shots rows still match L5=5+L10>=8."""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import prop_hit_tiers as T  # noqa: E402
from discover_gates_sport import graded_by_date, graded_candidates  # noqa: E402


def num(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:
        return None
    return out


def main() -> None:
    seen = set()
    status = Counter()
    result = Counter()
    blank_l5 = 0
    matched = 0
    for day in sorted(graded_by_date()):
        raw = None
        for path in graded_candidates(day):
            try:
                loaded = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue
            if any(
                T.norm_sport(str(r.get("sport") or "")) == "Soccer"
                for r in (loaded.get("props") or [])
                if isinstance(r, dict)
            ):
                raw = loaded
                break
        if raw is None:
            continue
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if T.norm_sport(str(row.get("sport") or "")) != "Soccer":
                continue
            prop = T.canon_prop("Soccer", str(row.get("prop") or ""))
            if prop != "shots":
                continue
            side = str(row.get("direction") or "").upper()
            if side != "OVER":
                continue
            res = str(row.get("result") or "").upper()
            if res not in {"HIT", "MISS"}:
                continue
            key = (day, str(row.get("player") or ""), str(row.get("line") or ""), res)
            if key in seen:
                continue
            seen.add(key)
            l5 = num(row.get("l5_over"))
            l10 = num(row.get("l10_over"))
            if l5 is None:
                blank_l5 += 1
                continue
            if l5 == 5 and (l10 or -1) >= 8:
                matched += 1
                status[str(row.get("minutes_status") or "blank")] += 1
                result[res] += 1
    print("hitmiss shots over", len(seen), "blank_l5", blank_l5, "matched", matched)
    print("status", dict(status))
    print("result", dict(result))


if __name__ == "__main__":
    main()
