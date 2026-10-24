"""Join the shots 0.5 cell and faceoff rows to the boxscore table."""
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
from discover_gates_sport import unique_game_dedupe  # noqa: E402

REF = ROOT / "data" / "cache" / "proporacle_ref.db"


def fold(name: object) -> str:
    text = unicodedata.normalize("NFKD", str(name or ""))
    return "".join(ch for ch in text.lower() if ch.isalnum())


def num(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:
        return None
    return out


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


def main() -> None:
    conn = sqlite3.connect(REF)
    box = {}
    for game_date, player, sog, fow in conn.execute(
        "SELECT game_date, player, shots_on_goal, faceoffs_won FROM nhl"
    ):
        box[(fold(player), str(game_date)[:10])] = (num(sog), num(fow))
    conn.close()
    sog_rows = []
    face = []
    for date, path in files().items():
        raw = json.loads(path.read_text(encoding="utf-8"))
        for row in raw.get("props") or []:
            if not isinstance(row, dict) or T.norm_sport(str(row.get("sport") or "")) != "NHL":
                continue
            if str(row.get("result") or "").upper() not in {"HIT", "MISS"}:
                continue
            prop = T.canon_prop("NHL", str(row.get("prop") or ""))
            side = str(row.get("direction") or "").upper()
            pick = str(row.get("pick_type") or "").lower()
            rec = {
                "date": date,
                "player_norm": " ".join(str(row.get("player") or "").lower().split()),
                "fold": fold(row.get("player")),
                "prop": prop,
                "side": side,
                "book": "Goblin OVER" if "goblin" in pick and side == "OVER" else pick,
                "line": num(row.get("line")),
                "actual_value": num(row.get("actual_value")),
                "hit": 1 if str(row.get("result")).upper() == "HIT" else 0,
                "source": str(row.get("actual_source") or ""),
            }
            if prop == "sog" and rec["book"] == "Goblin OVER":
                sog_rows.append(rec)
            if prop == "faceoffs_won":
                face.append(rec)
    cell = [
        r
        for r in unique_game_dedupe(sog_rows)
        if r["line"] is not None and abs(r["line"] - 0.5) < 1e-9
    ]
    matched = equal = old_zero = box_zero = missing = 0
    for r in cell:
        found = box.get((r["fold"], r["date"]))
        if found is None or found[0] is None:
            missing += 1
            if r["actual_value"] == 0:
                old_zero += 1
            continue
        matched += 1
        if r["actual_value"] == found[0]:
            equal += 1
        if found[0] == 0:
            box_zero += 1
        if r["actual_value"] == 0 and found[0] not in (None, 0):
            old_zero += 1
    facts = [r["actual_value"] for r in face if r["actual_value"] is not None]
    lines = [r["line"] for r in face if r["line"] is not None]
    facts.sort()
    lines.sort()
    def med(vals):
        if not vals:
            return None
        return vals[len(vals) // 2]
    face_match = face_equal = face_missing = 0
    for r in face:
        found = box.get((r["fold"], r["date"]))
        if found is None or found[1] is None:
            face_missing += 1
            continue
        face_match += 1
        if r["actual_value"] == found[1]:
            face_equal += 1
    overs = [r for r in face if r["side"] == "OVER"]
    print(json.dumps({
        "cell_n": len(cell),
        "box_matched": matched,
        "actual_equals_box": equal,
        "missing_box": missing,
        "stored_zero_box_positive": old_zero,
        "box_zero_among_matched": box_zero,
        "tagged_box_source": sum(1 for r in cell if r["source"] == "nhl_boxscore_ref"),
        "face_n": len(face),
        "face_median_line": med(lines),
        "face_median_actual": med(facts),
        "face_over_hits": f"{sum(r['hit'] for r in overs)}/{len(overs)}",
        "face_box_matched": face_match,
        "face_actual_equals_box": face_equal,
        "face_missing_box": face_missing,
    }, indent=2))


if __name__ == "__main__":
    main()
