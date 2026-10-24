"""Nightly: pp_result versus our grade. A Power win with a graded MISS is a FAIL."""
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
from utils.postseason_slate import counts_toward_gated_book  # noqa: E402

DB = ROOT / "data" / "db" / "MyTicketPerformance.db"
MANIFEST = ROOT / "data" / "reports" / "gated_goblin_slip_manifest_20261007.json"
OUT = ROOT / "data" / "reports" / "settlement_roi.json"

LEG_COLS = (
    "espn_event_id",
    "stored_start_time",
    "standard_line_source",
    "fill_status",
    "gate_branch",
    "board_opponent",
)


def fold(name: object) -> str:
    text = unicodedata.normalize("NFKD", str(name or ""))
    return "".join(ch for ch in text.lower() if ch.isalnum())


def ensure(conn: sqlite3.Connection) -> None:
    cols = {row[1] for row in conn.execute("PRAGMA table_info(entry_legs)")}
    for name in LEG_COLS:
        if name not in cols:
            conn.execute(f"ALTER TABLE entry_legs ADD COLUMN {name} TEXT")


def _text(value) -> str:
    text = str(value or "").strip()
    if text.lower() in {"", "none", "null", "nan"}:
        return ""
    return text


def from_attrs(attrs: dict) -> dict[str, str]:
    if not isinstance(attrs, dict):
        return {}
    return {
        "board_opponent": _text(attrs.get("opponent_name") or attrs.get("board_opponent")),
        "espn_event_id": _text(attrs.get("espn_event_id") or attrs.get("event_id")),
        "stored_start_time": _text(attrs.get("start_time") or attrs.get("game_start")),
        "standard_line_source": _text(attrs.get("standard_line_source")),
        "fill_status": _text(attrs.get("season_l5_flag") or attrs.get("fill_status")),
        "gate_branch": _text(attrs.get("gate_recipe") or attrs.get("gate_branch")),
    }


def win_word(value: str) -> str:
    text = value.strip().lower()
    if text in {"hit", "won", "win", "correct", "winner", "more", "over_hit"}:
        return "HIT"
    if text in {"miss", "lost", "loss", "incorrect", "loss", "under_hit"}:
        return "MISS"
    return ""


def graded_index() -> dict[tuple, str]:
    out = {}
    for folder in (MAIN / "ui_runner" / "templates", ROOT / "ui_runner" / "templates"):
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_20*.json")):
            if ".bak_" in path.name:
                continue
            day = path.stem.replace("graded_props_", "")[:10]
            if (day, "", "") in out:
                continue
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            for row in raw.get("props") or []:
                if not isinstance(row, dict):
                    continue
                res = str(row.get("result") or "").upper()
                if res not in {"HIT", "MISS"}:
                    continue
                key = (
                    day,
                    fold(row.get("player")),
                    T.canon_prop(str(row.get("sport") or ""), str(row.get("prop") or "")),
                )
                out.setdefault(key, res)
    return out


def main() -> int:
    report = {
        "scraped_payout_slips": 0,
        "overlap_with_127": 0,
        "stamped_needed_for_roi_30": None,
        "legs_compared": 0,
        "conflicts": 0,
        "power_win_with_miss": 0,
        "fields_filled": {name: 0 for name in LEG_COLS},
        "fail": False,
    }
    if not DB.is_file():
        report["fail"] = True
        OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
        print(json.dumps(report, indent=2))
        return 1
    manifest_keys = set()
    if MANIFEST.is_file():
        payload = json.loads(MANIFEST.read_text(encoding="utf-8"))
        for slip in payload.get("slips") or []:
            if not slip.get("resolved"):
                continue
            if not counts_toward_gated_book(slip):
                continue
            players = tuple(sorted(fold(p) for p in (slip.get("players") or [])))
            manifest_keys.add((str(slip.get("date") or "")[:10], players))
    grades = graded_index()
    with sqlite3.connect(DB) as conn:
        ensure(conn)
        payout_rows = conn.execute(
            """
            SELECT entry_id, created_at, raw_json
            FROM user_entries
            WHERE scraped_n_correct_x IS NOT NULL
            """
        ).fetchall()
        report["scraped_payout_slips"] = len(payout_rows)
        for entry_id, created_at, raw in payout_rows:
            players = [
                fold(r[0])
                for r in conn.execute(
                    "SELECT player_name FROM entry_legs WHERE entry_id = ?",
                    (entry_id,),
                )
            ]
            day = str(created_at or "")[:10]
            if (day, tuple(sorted(players))) in manifest_keys:
                report["overlap_with_127"] += 1
            if raw:
                try:
                    item = json.loads(raw)
                except json.JSONDecodeError:
                    item = {}
                attrs = item.get("attributes", {}) if isinstance(item, dict) else {}
                filled = from_attrs(attrs)
                for leg_id, in conn.execute(
                    "SELECT id FROM entry_legs WHERE entry_id = ?", (entry_id,)
                ):
                    sets = []
                    vals = []
                    for col, val in filled.items():
                        if not val:
                            continue
                        sets.append(f"{col} = COALESCE(NULLIF({col}, ''), ?)")
                        vals.append(val)
                    if sets:
                        conn.execute(
                            f"UPDATE entry_legs SET {', '.join(sets)} WHERE id = ?",
                            (*vals, leg_id),
                        )
        for name in LEG_COLS:
            report["fields_filled"][name] = conn.execute(
                f"SELECT COUNT(*) FROM entry_legs WHERE {name} IS NOT NULL AND {name} != ''"
            ).fetchone()[0]
        conflicts = []
        for entry_id, player, stat, league, pp_result, created_at in conn.execute(
            """
            SELECT l.entry_id, l.player_name, l.stat_type, l.league, l.pp_result, e.created_at
            FROM entry_legs l
            JOIN user_entries e ON e.entry_id = l.entry_id
            WHERE l.pp_result IS NOT NULL AND l.pp_result != ''
            """
        ):
            pp = win_word(str(pp_result))
            if not pp:
                continue
            day = str(created_at or "")[:10]
            ours = grades.get((day, fold(player), T.canon_prop(str(league or ""), str(stat or ""))))
            if not ours:
                continue
            report["legs_compared"] += 1
            if pp != ours:
                report["conflicts"] += 1
                if pp == "HIT" and ours == "MISS":
                    report["power_win_with_miss"] += 1
                conflicts.append(
                    {
                        "entry_id": entry_id,
                        "player": player,
                        "stat": stat,
                        "pp_result": pp_result,
                        "our_grade": ours,
                    }
                )
        conn.commit()
    overlap = report["overlap_with_127"]
    if overlap < 30:
        report["stamped_needed_for_roi_30"] = 30 - overlap
    report["conflict_rows"] = conflicts[:20]
    report["fail"] = report["power_win_with_miss"] > 0 or report["conflicts"] > 0
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps({k: report[k] for k in report if k != "conflict_rows"}, indent=2))
    return 1 if report["fail"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
