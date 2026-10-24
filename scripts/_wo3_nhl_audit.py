"""W7: NHL actuals vs box, faceoff regrade, Thomas leg, zero shares."""
from __future__ import annotations

import json
import sqlite3
import statistics
import sys
import unicodedata
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import prop_hit_tiers as T  # noqa: E402

REF = ROOT / "data" / "cache" / "proporacle_ref.db"
DB = ROOT / "data" / "db" / "MyTicketPerformance.db"
OUT = ROOT / "data" / "reports" / "nhl_prop_agreement.json"
LOG = ROOT / "data" / "reports" / "grade_corrections.jsonl"
ET = ZoneInfo("America/New_York")

PROP_COL = {
    "sog": "shots_on_goal",
    "hits": "hits",
    "blocked_shots": "blocked_shots",
    "assists": "assists",
    "points": "points",
    "saves": "saves",
    "toi": "toi",
    "pp_points": "pp_points",
    "goals": "goals",
    "faceoffs_won": "faceoffs_won",
    "goals_allowed": "goals_allowed",
    "pim": "pim",
    "plus_minus": "plus_minus",
}
BOX_COLS = (
    "goals",
    "assists",
    "points",
    "shots_on_goal",
    "hits",
    "blocked_shots",
    "pim",
    "plus_minus",
    "pp_points",
    "faceoffs_won",
    "toi",
    "saves",
    "goals_allowed",
)


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


def close(a, b) -> bool:
    if a is None or b is None:
        return False
    return abs(a - b) < 0.051


def files() -> dict[str, Path]:
    by: dict[str, Path] = {}
    for folder in (MAIN / "ui_runner" / "templates", ROOT / "ui_runner" / "templates"):
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_20*.json")):
            if ".bak_" in path.name:
                continue
            by.setdefault(path.stem.replace("graded_props_", "")[:10], path)
    return by


def grade(actual, line, direction: str) -> str | None:
    if actual is None or line is None:
        return None
    side = direction.upper()
    if abs(actual - line) < 1e-9:
        return "PUSH"
    if side == "OVER":
        return "HIT" if actual > line else "MISS"
    if side == "UNDER":
        return "HIT" if actual < line else "MISS"
    return None


def existing_keys() -> set[tuple]:
    keys = set()
    if not LOG.is_file():
        return keys
    for line in LOG.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        keys.add(
            (
                str(rec.get("date") or "")[:10],
                fold(rec.get("player")),
                str(rec.get("prop") or ""),
                str(rec.get("direction") or "").upper(),
                str(rec.get("line") or ""),
                str(rec.get("old") or ""),
                str(rec.get("new") or ""),
                str(rec.get("reason") or ""),
            )
        )
    return keys


def main() -> None:
    conn = sqlite3.connect(REF)
    box: dict[tuple[str, str], dict] = {}
    q = "SELECT game_date, player, " + ", ".join(BOX_COLS) + " FROM nhl"
    for row in conn.execute(q):
        game_date, player, *vals = row
        box[(fold(player), str(game_date)[:10])] = {
            col: num(val) for col, val in zip(BOX_COLS, vals)
        }
    conn.close()

    stats: dict[str, dict] = {}
    face_stored: list[float] = []
    face_col_hits = {col: 0 for col in BOX_COLS}
    face_matched = 0
    corrections: list[dict] = []
    seen = existing_keys()
    files_changed = 0
    face_regraded = 0
    thomas = []

    for date, path in files().items():
        raw = json.loads(path.read_text(encoding="utf-8"))
        changed = False
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if T.norm_sport(str(row.get("sport") or "")) != "NHL":
                continue
            prop = T.canon_prop("NHL", str(row.get("prop") or ""))
            if not prop:
                continue
            actual = num(row.get("actual_value"))
            bucket = stats.setdefault(
                prop,
                {
                    "rows": 0,
                    "with_actual": 0,
                    "zeros_before": 0,
                    "zeros_after": 0,
                    "box_matched": 0,
                    "agree": 0,
                    "col_hits": {c: 0 for c in BOX_COLS},
                },
            )
            bucket["rows"] += 1
            if actual == 0:
                bucket["zeros_before"] += 1
            if actual is not None:
                bucket["with_actual"] += 1
            found = box.get((fold(row.get("player")), date))
            assigned = PROP_COL.get(prop)
            if found and assigned and found.get(assigned) is not None and actual is not None:
                bucket["box_matched"] += 1
                if close(actual, found[assigned]):
                    bucket["agree"] += 1
                for col, val in found.items():
                    if close(actual, val):
                        bucket["col_hits"][col] += 1
            if prop == "faceoffs_won" and actual is not None:
                face_stored.append(actual)
                if found:
                    face_matched += 1
                    for col, val in found.items():
                        if close(actual, val):
                            face_col_hits[col] += 1
                if found and found.get("faceoffs_won") is not None:
                    new_actual = found["faceoffs_won"]
                    direction = str(row.get("direction") or row.get("over_under") or "").upper()
                    old_res = str(row.get("result") or "").upper()
                    new_res = grade(new_actual, num(row.get("line")), direction)
                    if not close(actual, new_actual) or (new_res and new_res != old_res):
                        row["actual_value"] = new_actual
                        row["actual_source"] = "nhl_boxscore_ref"
                        if new_res:
                            row["result"] = new_res
                            row["hit"] = 1 if new_res == "HIT" else (0 if new_res == "MISS" else None)
                        if old_res in {"HIT", "MISS"} and new_res and new_res != old_res:
                            rec = {
                                "date": date,
                                "player": row.get("player"),
                                "prop": row.get("prop"),
                                "direction": direction,
                                "line": row.get("line"),
                                "sport": "NHL",
                                "old": old_res,
                                "new": new_res,
                                "reason": "nhl_faceoff_was_win_pct",
                                "version": 1,
                                "note": f"stored {actual} vs box faceoffs_won {new_actual}",
                                "logged_at": datetime.now(ET).isoformat(timespec="seconds"),
                            }
                            key = (
                                date,
                                fold(row.get("player")),
                                str(row.get("prop") or ""),
                                direction,
                                str(row.get("line") or ""),
                                old_res,
                                new_res,
                                "nhl_faceoff_was_win_pct",
                            )
                            if key not in seen:
                                corrections.append(rec)
                                seen.add(key)
                        changed = True
                        face_regraded += 1
                        actual = new_actual
            if actual == 0:
                bucket["zeros_after"] += 1
            player = str(row.get("player") or "")
            if "robert thomas" in player.lower() and prop == "faceoffs_won":
                line = num(row.get("line"))
                if line is not None and abs(line - 10.5) < 1e-9:
                    thomas.append(
                        {
                            "date": date,
                            "result": row.get("result"),
                            "actual": row.get("actual_value"),
                            "direction": row.get("direction"),
                        }
                    )
        if changed:
            path.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
            files_changed += 1

    if corrections:
        with LOG.open("a", encoding="utf-8") as fh:
            for rec in corrections:
                fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    account = []
    if DB.is_file():
        conn = sqlite3.connect(DB)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(entry_legs)")}
        pp = "pp_result" if "pp_result" in cols else "NULL"
        account = [
            dict(zip(("entry_id", "player", "stat", "line", "pp_result"), row))
            for row in conn.execute(
                f"""
                SELECT entry_id, player_name, stat_type, line, {pp}
                FROM entry_legs
                WHERE lower(player_name) LIKE '%thomas%'
                  AND lower(stat_type) LIKE '%faceoff%'
                """
            )
        ]
        conn.close()

    excluded = []
    props_out = {}
    for prop, bucket in sorted(stats.items()):
        matched = bucket["box_matched"]
        agree = bucket["agree"]
        rate = (agree / matched) if matched else None
        best_col = max(bucket["col_hits"], key=lambda c: bucket["col_hits"][c])
        best_n = bucket["col_hits"][best_col]
        item = {
            "rows": bucket["rows"],
            "with_actual": bucket["with_actual"],
            "zeros_before": bucket["zeros_before"],
            "zeros_after": bucket["zeros_after"],
            "zero_share_before": (
                round(bucket["zeros_before"] / bucket["with_actual"], 4)
                if bucket["with_actual"]
                else None
            ),
            "zero_share_after": (
                round(bucket["zeros_after"] / bucket["with_actual"], 4)
                if bucket["with_actual"]
                else None
            ),
            "box_matched": matched,
            "agree": agree,
            "agree_rate": None if rate is None else round(rate, 4),
            "assigned_column": PROP_COL.get(prop),
            "best_column": best_col,
            "best_column_n": best_n,
        }
        props_out[prop] = item
        if prop == "faceoffs_won" or (rate is not None and rate < 0.98 and matched >= 30):
            excluded.append(prop)
        elif rate is not None and rate < 0.98 and matched < 30:
            item["note"] = "under 98% but n<30, reported, not a gate change"

    face_col_ranked = sorted(face_col_hits.items(), key=lambda kv: -kv[1])
    payload = {
        "box_rows": len(box),
        "files_changed": files_changed,
        "faceoffs_regraded": face_regraded,
        "corrections_logged": len(corrections),
        "faceoffs_stored_n": len(face_stored),
        "faceoffs_stored_median": statistics.median(face_stored) if face_stored else None,
        "faceoffs_matched_for_column_hunt": face_matched,
        "faceoffs_column_hits": face_col_ranked[:6],
        "faceoffs_column": (
            "ESPN label FO is faceoff win percentage. The writer aliased FO onto "
            "faceoffs_won. No box column matches that stored value; box faceoffs_won is the count."
        ),
        "excluded_props": sorted(set(excluded)),
        "props": props_out,
        "robert_thomas_10_5_graded": thomas,
        "robert_thomas_account": account,
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps({k: payload[k] for k in payload if k != "props"}, indent=2))
    for prop, item in props_out.items():
        print(
            f"{prop} agree {item['agree']}/{item['box_matched']} "
            f"({item['agree_rate']}) zeros {item['zeros_before']}->{item['zeros_after']} "
            f"best {item['best_column']} {item['best_column_n']}"
        )


if __name__ == "__main__":
    main()
