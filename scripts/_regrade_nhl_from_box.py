"""Regrade NHL shots and hits from the boxscore table.

The ESPN label SOG is shootout goals. Graded history that used it is replaced
with shots_on_goal / hits from data/cache/proporacle_ref.db when the player
and date match. Discovery reads the updated graded file.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from grading.leg_grade_utils import slate_grade_row  # noqa: E402
import prop_hit_tiers as T  # noqa: E402

MAIN = ROOT.parent / "PropORACLE_main_cp"
REF = ROOT / "data" / "cache" / "proporacle_ref.db"
OUT = ROOT / "data" / "reports" / "nhl_boxscore_regrade.json"

STAT = {
    "sog": "shots_on_goal",
    "hits": "hits",
}


def fold(name: object) -> str:
    text = unicodedata.normalize("NFKD", str(name or ""))
    return "".join(ch for ch in text.lower() if ch.isalnum())


def last_token(name: object) -> str:
    parts = [p for p in str(name or "").replace(".", " ").split() if p]
    return fold(parts[-1]) if parts else ""


def graded_files() -> dict[str, Path]:
    by: dict[str, Path] = {}
    for folder in (
        MAIN / "ui_runner" / "templates",
        ROOT / "ui_runner" / "templates",
    ):
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_20*.json")):
            if ".bak_" in path.name:
                continue
            date = path.stem.replace("graded_props_", "")[:10]
            if date not in by:
                by[date] = path
    return by


def load_box() -> tuple[dict, dict]:
    exact: dict[tuple[str, str], dict] = {}
    by_last: dict[tuple[str, str], list[dict]] = {}
    conn = sqlite3.connect(REF)
    rows = conn.execute(
        """
        SELECT game_date, player, shots_on_goal, hits
        FROM nhl
        WHERE shots_on_goal IS NOT NULL
        """
    ).fetchall()
    conn.close()
    for game_date, player, sog, hits in rows:
        rec = {
            "player": player,
            "shots_on_goal": float(sog) if sog is not None else None,
            "hits": float(hits) if hits is not None else None,
        }
        key = (fold(player), str(game_date)[:10])
        exact[key] = rec
        by_last.setdefault((last_token(player), str(game_date)[:10]), []).append(rec)
    return exact, by_last


def num(value: object) -> float | None:
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:
        return None
    return out


def main() -> None:
    exact, by_last = load_box()
    files = graded_files()
    before = {k: {"n": 0, "zero": 0} for k in STAT}
    after = {k: {"n": 0, "zero": 0, "matched": 0, "flipped": 0, "unmatched": 0} for k in STAT}
    files_written = 0

    for date, path in files.items():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        changed = False
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            sport = T.norm_sport(str(row.get("sport") or ""))
            if sport != "NHL":
                continue
            prop = T.canon_prop("NHL", str(row.get("prop") or row.get("prop_type") or ""))
            if prop not in STAT:
                continue
            result = str(row.get("result") or "").strip().upper()
            if result not in {"HIT", "MISS", "PUSH"}:
                continue
            before[prop]["n"] += 1
            old = num(row.get("actual_value"))
            if old == 0:
                before[prop]["zero"] += 1
            game_date = str(row.get("game_date") or row.get("date") or date)[:10]
            player = row.get("player")
            box = exact.get((fold(player), game_date))
            if box is None:
                cands = by_last.get((last_token(player), game_date), [])
                if len(cands) == 1:
                    box = cands[0]
            if box is None:
                after[prop]["unmatched"] += 1
                continue
            new = box.get(STAT[prop])
            if new is None:
                after[prop]["unmatched"] += 1
                continue
            after[prop]["matched"] += 1
            after[prop]["n"] += 1
            if new == 0:
                after[prop]["zero"] += 1
            graded, _reason, margin = slate_grade_row(new, row)
            if graded != result:
                after[prop]["flipped"] += 1
            if old != new or graded != result:
                row["actual_value"] = new
                row["result"] = graded
                try:
                    margin_f = float(margin)
                except (TypeError, ValueError):
                    margin_f = None
                if margin_f is not None and margin_f == margin_f:
                    row["margin"] = margin_f
                row["actual_source"] = "nhl_boxscore_ref"
                changed = True
        if changed:
            path.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
            files_written += 1

    report = {
        "box_rows_indexed": len(exact),
        "graded_files": len(files),
        "files_written": files_written,
        "before": before,
        "after_matched": after,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
