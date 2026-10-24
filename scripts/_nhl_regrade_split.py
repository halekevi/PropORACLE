"""Split NHL regrade zero share into matched vs still-old rows."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import prop_hit_tiers as T  # noqa: E402

MAIN = ROOT.parent / "PropORACLE_main_cp"


def num(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out


def main() -> None:
    by = {}
    for folder in (MAIN / "ui_runner" / "templates", ROOT / "ui_runner" / "templates"):
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_20*.json")):
            if ".bak_" in path.name:
                continue
            date = path.stem.replace("graded_props_", "")[:10]
            by.setdefault(date, path)
    out = {
        p: {"matched_n": 0, "matched_zero": 0, "old_n": 0, "old_zero": 0}
        for p in ("sog", "hits")
    }
    for path in by.values():
        raw = json.loads(path.read_text(encoding="utf-8"))
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if T.norm_sport(str(row.get("sport") or "")) != "NHL":
                continue
            prop = T.canon_prop("NHL", str(row.get("prop") or ""))
            if prop not in out:
                continue
            if str(row.get("result") or "").upper() not in {"HIT", "MISS", "PUSH"}:
                continue
            actual = num(row.get("actual_value"))
            zero = actual == 0
            if row.get("actual_source") == "nhl_boxscore_ref":
                out[prop]["matched_n"] += 1
                out[prop]["matched_zero"] += int(zero)
            else:
                out[prop]["old_n"] += 1
                out[prop]["old_zero"] += int(zero)
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
