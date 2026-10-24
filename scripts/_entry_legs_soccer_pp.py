"""PrizePicks settlement vs our actual for placed Saves / Shots / SOT legs."""
from __future__ import annotations

import json
import sqlite3
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from grade_entry_legs import find_myticket_db  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402

KEEP = {"saves", "shots", "sog"}


def _norm(name: object) -> str:
    raw = unicodedata.normalize("NFKD", str(name or ""))
    raw = "".join(ch for ch in raw if not unicodedata.combining(ch))
    return " ".join(raw.casefold().split())


def _pp_result(obj) -> str:
    if not isinstance(obj, dict):
        return ""
    for key in ("result", "grade", "status", "outcome", "settlement"):
        val = obj.get(key)
        if isinstance(val, str) and val.strip():
            return val.strip().upper()
    return ""


def main() -> None:
    db = find_myticket_db()
    print("db", db, flush=True)
    if db is None:
        return
    conn = sqlite3.connect(str(db))
    tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")]
    print("tables", tables, flush=True)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(entry_legs)")]
    print("entry_legs cols", cols, flush=True)
    n = conn.execute("SELECT COUNT(*) FROM entry_legs").fetchone()[0]
    print("entry_legs", n, flush=True)
    rows = conn.execute(
        "SELECT entry_id, player_name, stat_type, line, description, league FROM entry_legs"
    ).fetchall()
    hit_rows = []
    for entry_id, player, stat, line, desc, league in rows:
        prop = canon_prop("SOCCER", str(stat or ""))
        blob = f"{stat or ''} {desc or ''}"
        if prop not in KEEP and not any(
            w in blob.casefold() for w in ("save", "shot", "target")
        ):
            continue
        # keep soccer-ish
        league_s = str(league or "")
        if league_s and "soc" not in league_s.casefold() and "fifa" not in league_s.casefold():
            if prop not in KEEP:
                continue
        hit_rows.append((entry_id, player, stat, line, desc, league, prop))
    print("candidate legs", len(hit_rows), flush=True)
    # PP result lives on the entry raw_json when the capture stored it.
    raw_cols = [r[1] for r in conn.execute("PRAGMA table_info(user_entries)")]
    print("user_entries cols", raw_cols, flush=True)
    if not hit_rows:
        # show stat_type sample
        sample = conn.execute(
            "SELECT stat_type, league, COUNT(*) c FROM entry_legs GROUP BY 1, 2 ORDER BY c DESC LIMIT 25"
        ).fetchall()
        print("top stat types", sample, flush=True)
        return
    ids = sorted({r[0] for r in hit_rows})
    raws = {}
    if "raw_json" in raw_cols:
        q = "SELECT entry_id, status, raw_json FROM user_entries WHERE entry_id IN ({})".format(
            ",".join("?" * len(ids))
        )
        for eid, status, raw in conn.execute(q, ids):
            raws[eid] = (status, raw)
    agree = mismatch = no_pp = no_ours = 0
    # our grade: actuals files, any day, player+prop
    ours: dict[tuple[str, str], list[float]] = {}
    main = ROOT.parent / "PropORACLE_main_cp" / "outputs"
    import pandas as pd

    if main.is_dir():
        for path in main.glob("2026-*/actuals_soccer_2026-*.csv"):
            try:
                df = pd.read_csv(path, dtype=str)
            except Exception:
                continue
            for _, r in df.iterrows():
                prop = canon_prop("SOCCER", str(r.get("prop_type") or ""))
                if prop not in KEEP:
                    continue
                try:
                    val = float(r.get("actual"))
                except (TypeError, ValueError):
                    continue
                ours.setdefault((_norm(r.get("player")), prop), []).append(val)
    shown = 0
    for entry_id, player, stat, line, desc, league, prop in hit_rows:
        status, raw = raws.get(entry_id, ("", None))
        pp = ""
        if raw:
            try:
                payload = json.loads(raw)
            except Exception:
                payload = None
            pp = _pp_result(payload if isinstance(payload, dict) else {})
            if not pp and isinstance(payload, dict):
                # selections / projections list
                for key in ("selections", "predictions", "legs", "projections"):
                    block = payload.get(key)
                    if isinstance(block, list):
                        for item in block:
                            if not isinstance(item, dict):
                                continue
                            name = _norm(item.get("player_name") or item.get("name") or "")
                            if name and name == _norm(player):
                                pp = _pp_result(item) or pp
        if not pp:
            text = f"{desc or ''} {status or ''}".upper()
            if "WON" in text or "HIT" in text:
                pp = "HIT"
            elif "LOST" in text or "MISS" in text:
                pp = "MISS"
        if not pp:
            no_pp += 1
            if shown < 8:
                print(" no pp", player, stat, line, status, str(desc)[:80], flush=True)
                shown += 1
            continue
        vals = ours.get((_norm(player), prop if prop in KEEP else canon_prop("SOCCER", stat or "")), [])
        if not vals or line is None:
            no_ours += 1
            continue
        try:
            line_f = float(line)
        except (TypeError, ValueError):
            no_ours += 1
            continue
        # direction from description
        direction = "OVER"
        blob = f"{desc or ''}".upper()
        if "UNDER" in blob or "LESS" in blob:
            direction = "UNDER"
        # if several actuals, count a match when any day agrees with PP
        def _ours(v: float) -> str:
            if direction == "OVER":
                if v > line_f:
                    return "HIT"
                if v < line_f:
                    return "MISS"
            else:
                if v < line_f:
                    return "HIT"
                if v > line_f:
                    return "MISS"
            return "PUSH"

        grades = {_ours(v) for v in vals}
        pp_n = "HIT" if pp in {"HIT", "WON", "WIN", "CORRECT", "MORE"} else (
            "MISS" if pp in {"MISS", "LOST", "LOSS", "INCORRECT", "LESS"} else pp
        )
        if pp_n in grades:
            agree += 1
        else:
            mismatch += 1
            if shown < 20:
                print(
                    f" mismatch {player} {stat} line={line} pp={pp_n} ours={sorted(grades)} days={len(vals)}",
                    flush=True,
                )
                shown += 1
    print(
        f"pp compared agree {agree} mismatch {mismatch} no_pp_result {no_pp} no_our_actual {no_ours}",
        flush=True,
    )


if __name__ == "__main__":
    main()
