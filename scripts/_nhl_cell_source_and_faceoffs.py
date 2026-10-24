"""Grade source for the Goblin shots 0.5 cell, and the faceoff line distribution."""
from __future__ import annotations

import json
import sqlite3
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import prop_hit_tiers as T  # noqa: E402
from capture_entries import DB_PATH  # noqa: E402
from discover_gates_sport import unique_game_dedupe  # noqa: E402
from utils.nhl_sog_shadow import log_live_legs  # noqa: E402

OUT = ROOT / "data" / "reports" / "nhl_cell_source_and_faceoffs.json"


def _num(value):
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    if out != out:
        return None
    return out


def _files():
    by = {}
    for folder in (MAIN / "ui_runner" / "templates", ROOT / "ui_runner" / "templates"):
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_20*.json")):
            if ".bak_" in path.name:
                continue
            date = path.stem.replace("graded_props_", "")[:10]
            by.setdefault(date, path)
    return by


def _rows():
    sog = []
    face = []
    for date, path in _files().items():
        raw = json.loads(path.read_text(encoding="utf-8"))
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if T.norm_sport(str(row.get("sport") or "")) != "NHL":
                continue
            if str(row.get("result") or "").upper() not in {"HIT", "MISS"}:
                continue
            prop = T.canon_prop("NHL", str(row.get("prop") or ""))
            side = str(row.get("direction") or "").upper()
            pick = str(row.get("pick_type") or "").lower()
            book = (
                "Goblin OVER"
                if "goblin" in pick and side == "OVER"
                else "other"
            )
            rec = {
                "date": date,
                "player_norm": " ".join(str(row.get("player") or "").lower().split()),
                "prop": prop,
                "side": side,
                "book": book,
                "line": _num(row.get("line")),
                "actual_value": _num(row.get("actual_value")),
                "actual_source": str(row.get("actual_source") or ""),
                "hit": 1 if str(row.get("result")).upper() == "HIT" else 0,
            }
            if prop == "sog" and book == "Goblin OVER":
                sog.append(rec)
            if prop == "faceoffs_won":
                face.append({**rec, "pick": pick, "result": str(row.get("result")).upper()})
    return sog, face


def _payouts() -> dict:
    out = {"db": str(DB_PATH), "faceoff_legs": 0, "with_n_correct": 0, "multipliers": []}
    if not Path(DB_PATH).is_file():
        out["db"] = "missing"
        return out
    conn = sqlite3.connect(DB_PATH)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(entry_legs)")}
    if "stat_type" not in cols:
        conn.close()
        return out
    legs = conn.execute(
        """
        SELECT entry_id, stat_type, line, player_name
        FROM entry_legs
        WHERE lower(COALESCE(stat_type, '')) LIKE '%faceoff%'
        """
    ).fetchall()
    out["faceoff_legs"] = len(legs)
    entry_ids = sorted({str(r[0]) for r in legs})
    ucols = {r[1] for r in conn.execute("PRAGMA table_info(user_entries)")}
    if entry_ids and "scraped_n_correct_x" in ucols:
        q = ",".join("?" for _ in entry_ids)
        rows = conn.execute(
            f"SELECT entry_id, scraped_n_correct_x FROM user_entries WHERE entry_id IN ({q})",
            entry_ids,
        ).fetchall()
        mults = [float(r[1]) for r in rows if r[1] is not None]
        out["with_n_correct"] = len(mults)
        out["multipliers"] = mults
    conn.close()
    ladder = ROOT / "ui_runner" / "data" / "payout_ladder_live_cdp.json"
    out["ladder_faceoff_rows"] = 0
    if ladder.is_file():
        text = ladder.read_text(encoding="utf-8").lower()
        out["ladder_faceoff_rows"] = text.count("faceoff")
    return out


def _live_shadow() -> int:
    wrote = 0
    root = MAIN / "outputs" / "2026-10-08"
    if not root.is_dir():
        return 0
    import pandas as pd

    frames = []
    for path in list(root.rglob("*nhl*")):
        if path.suffix.lower() not in {".csv", ".xlsx"}:
            continue
        if "step1" not in path.name.lower() and "step8" not in path.name.lower():
            continue
        try:
            df = pd.read_csv(path) if path.suffix.lower() == ".csv" else pd.read_excel(path)
        except Exception:
            continue
        frames.append(df)
    rows = []
    for df in frames:
        cols = {str(c).lower(): c for c in df.columns}
        for _, rec in df.iterrows():
            rows.append(
                {
                    "sport": "NHL",
                    "player": rec.get(cols.get("player") or cols.get("name") or "player"),
                    "prop": rec.get(cols.get("prop") or cols.get("stat type") or "prop"),
                    "pick_type": rec.get(cols.get("pick type") or cols.get("pick_type") or "pick"),
                    "direction": rec.get(cols.get("direction") or cols.get("over_under") or "dir"),
                    "line": rec.get(cols.get("line")),
                }
            )
    wrote += log_live_legs(rows, slate_date="2026-10-08")
    return wrote


def main() -> None:
    sog, face = _rows()
    deduped = unique_game_dedupe(sog)
    cell = [r for r in deduped if r["line"] is not None and abs(r["line"] - 0.5) < 1e-9]
    source = Counter(r["actual_source"] or "(blank)" for r in cell)
    zeros = sum(1 for r in cell if r["actual_value"] == 0)
    hits = sum(r["hit"] for r in cell)
    lines = Counter()
    for r in face:
        if r["line"] is None:
            lines["blank"] += 1
        else:
            lines[f"{r['line']:.1f}"] += 1
    goblin_lines = Counter()
    for r in face:
        if "goblin" not in r["pick"] or r["line"] is None:
            continue
        goblin_lines[f"{r['line']:.1f}"] += 1
    report = {
        "sog_line05": {
            "n": len(cell),
            "hits": hits,
            "zeros": zeros,
            "actual_source": dict(source),
            "boxscore_share": (
                source.get("nhl_boxscore_ref", 0) / len(cell) if cell else None
            ),
        },
        "faceoffs": {
            "n": len(face),
            "line_counts": dict(lines.most_common()),
            "goblin_line_counts": dict(goblin_lines.most_common()),
            "goblin_n": sum(goblin_lines.values()),
        },
        "scraped_payout": _payouts(),
        "shadow_legs_logged": _live_shadow(),
    }
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
