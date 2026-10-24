"""How many graded soccer rows each cell loses when the 264 not-in-box keys drop."""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import prop_hit_tiers as T  # noqa: E402

MAIN = ROOT.parent / "PropORACLE_main_cp"
UNRESOLVED = ROOT / "data" / "reports" / "soccer_unresolved_multi_event.csv"
OUT = ROOT / "data" / "reports" / "soccer_notinbox_cell_loss.json"
PROPS = ("shots", "sog", "saves")


def fold(name: object) -> str:
    return "".join(ch for ch in str(name or "").lower() if ch.isalnum())


def main() -> None:
    keys: set[tuple[str, str, str]] = set()
    with UNRESOLVED.open(encoding="utf-8", newline="") as fh:
        for row in csv.DictReader(fh):
            if row.get("bucket") != "player_not_in_box":
                continue
            prop = row.get("prop") or ""
            if prop not in PROPS:
                continue
            keys.add((row.get("day") or "", fold(row.get("player_raw")), prop))
    sports: dict[str, int] = {}
    decided = {p: 0 for p in PROPS}
    lost = {p: 0 for p in PROPS}
    files = {}
    for folder in (MAIN / "ui_runner" / "templates", ROOT / "ui_runner" / "templates"):
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_20*.json")):
            if ".bak_" in path.name:
                continue
            date = path.stem.replace("graded_props_", "")[:10]
            files.setdefault(date, path)
    for date, path in files.items():
        raw = json.loads(path.read_text(encoding="utf-8"))
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            sport = T.norm_sport(str(row.get("sport") or ""))
            if sport.lower() != "soccer":
                sports[sport] = sports.get(sport, 0) + 1
                continue
            prop = T.canon_prop(sport, str(row.get("prop") or ""))
            if prop not in PROPS:
                continue
            if str(row.get("result") or "").upper() not in {"HIT", "MISS"}:
                continue
            decided[prop] += 1
            game_date = str(row.get("game_date") or row.get("date") or date)[:10]
            if (game_date, fold(row.get("player")), prop) in keys:
                lost[prop] += 1
    # The counter above increments sports for every non-soccer row. Don't keep it.
    report = {
        "not_in_box_keys": len(keys),
        "decided": decided,
        "lost_from_cell": lost,
        "remaining": {p: decided[p] - lost[p] for p in PROPS},
    }
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
