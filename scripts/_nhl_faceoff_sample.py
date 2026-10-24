"""Show a few faceoff grades next to the box column."""
from __future__ import annotations

import json
import sqlite3
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import prop_hit_tiers as T  # noqa: E402

REF = ROOT / "data" / "cache" / "proporacle_ref.db"


def fold(name: object) -> str:
    text = unicodedata.normalize("NFKD", str(name or ""))
    return "".join(ch for ch in text.lower() if ch.isalnum())


def main() -> None:
    conn = sqlite3.connect(REF)
    box = {
        (fold(p), str(d)[:10]): (s, f, toi)
        for d, p, s, f, toi in conn.execute(
            "SELECT game_date, player, shots_on_goal, faceoffs_won, toi FROM nhl"
        )
    }
    conn.close()
    shown = 0
    for folder in (MAIN / "ui_runner" / "templates",):
        for path in sorted(folder.glob("graded_props_20*.json")):
            raw = json.loads(path.read_text(encoding="utf-8"))
            day = path.stem.replace("graded_props_", "")[:10]
            for row in raw.get("props") or []:
                if T.norm_sport(str(row.get("sport") or "")) != "NHL":
                    continue
                if T.canon_prop("NHL", str(row.get("prop") or "")) != "faceoffs_won":
                    continue
                found = box.get((fold(row.get("player")), day))
                if shown < 8:
                    print(
                        day,
                        row.get("player"),
                        "line", row.get("line"),
                        "actual", row.get("actual_value"),
                        "result", row.get("result"),
                        "box", found,
                        "prop", row.get("prop"),
                    )
                    shown += 1
            if shown >= 8:
                return


if __name__ == "__main__":
    main()
