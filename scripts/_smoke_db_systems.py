"""One-shot SQLite/Parquet health check for a slate date.

Usage:
  py -3.14 scripts/_smoke_db_systems.py --phase pre
  py -3.14 scripts/_smoke_db_systems.py --phase post --date 2026-08-28
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

CORE_DBS = [
    ROOT / "data/cache/proporacle_ref.db",
    ROOT / "data/line_history.db",
    ROOT / "data/cache/mlb_props_history.db",
    ROOT / "data/cache/wnba_props_history.db",
    ROOT / "data/cache/soccer_props_history.db",
    ROOT / "data/cache/tennis_props_history.db",
    ROOT / "data/cache/nfl_props_history.db",
    ROOT / "data/cache/cfb_props_history.db",
    ROOT / "Sports/NBA/data/cache/proporacle_ref.db",
]

KEYED_TABLES = (
    "wnba",
    "mlb_gamelog",
    "espn_boxscore_rows",
    "nba",
    "soccer",
    "nhl",
    "cfb",
    "nfl",
)

PARQUET_GLOBS = (
    "**/step8_*_direction_clean.parquet",
    "**/step7*.parquet",
    "**/step6*.parquet",
    "**/combined_slate_tickets*.parquet",
)


def _tables(con: sqlite3.Connection) -> list[str]:
    return [
        r[0]
        for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY 1"
        )
    ]


def inspect_dbs() -> dict:
    out: dict = {"ts": datetime.now().isoformat(), "dbs": []}
    for p in CORE_DBS:
        rec: dict = {
            "path": str(p),
            "exists": p.exists(),
            "mb": round(p.stat().st_size / 1e6, 1) if p.exists() else 0,
        }
        if not p.exists():
            rec["error"] = "missing"
            out["dbs"].append(rec)
            continue
        con = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        try:
            rec["integrity"] = con.execute("PRAGMA quick_check").fetchone()[0]
            rec["tables"] = {}
            for t in _tables(con):
                try:
                    rec["tables"][t] = con.execute(
                        f'SELECT COUNT(*) FROM "{t}"'
                    ).fetchone()[0]
                except Exception as exc:
                    rec["tables"][t] = f"ERR {exc}"
            if p.name == "proporacle_ref.db" and p.parent.name == "cache":
                schema: dict = {}
                for t in KEYED_TABLES:
                    if t not in rec["tables"]:
                        continue
                    schema[t] = [
                        r[1] for r in con.execute(f"PRAGMA table_info({t})")
                    ][:12]
                    if t == "espn_boxscore_rows":
                        rec["espn_boxscore_by_sport"] = dict(
                            con.execute(
                                "SELECT sport, COUNT(*) FROM espn_boxscore_rows GROUP BY 1"
                            ).fetchall()
                        )
                    if t == "wnba":
                        rec["wnba_max_date"] = con.execute(
                            "SELECT MAX(game_date) FROM wnba"
                        ).fetchone()[0]
                    if t == "mlb_gamelog":
                        rec["mlb_max_date"] = con.execute(
                            "SELECT MAX(game_date) FROM mlb_gamelog"
                        ).fetchone()[0]
                rec["schema_cols"] = schema
        finally:
            con.close()
        out["dbs"].append(rec)
    return out


def inspect_parquet(slate_date: str) -> dict:
    out_dir = ROOT / "outputs" / slate_date
    found = []
    missing_xlsx_sidecars = []
    if out_dir.is_dir():
        for pq in sorted(out_dir.rglob("*.parquet")):
            found.append(
                {
                    "path": str(pq.relative_to(ROOT)),
                    "mb": round(pq.stat().st_size / 1e6, 3),
                    "mtime": datetime.fromtimestamp(pq.stat().st_mtime).isoformat(
                        timespec="seconds"
                    ),
                }
            )
        for xlsx in sorted(out_dir.rglob("step8_*_direction_clean.xlsx")):
            sidecar = xlsx.with_suffix(".parquet")
            if not sidecar.exists():
                missing_xlsx_sidecars.append(str(xlsx.relative_to(ROOT)))
        for xlsx in sorted(out_dir.rglob("combined_slate_tickets*.xlsx")):
            sidecar = xlsx.with_suffix(".parquet")
            if not sidecar.exists() and "to_grade" not in xlsx.name:
                missing_xlsx_sidecars.append(str(xlsx.relative_to(ROOT)))
    return {
        "out_dir": str(out_dir),
        "exists": out_dir.is_dir(),
        "parquet_count": len(found),
        "parquet": found,
        "missing_step8_or_combined_sidecars": missing_xlsx_sidecars,
    }


def print_summary(payload: dict, phase: str) -> None:
    print(f"=== DB smoke {phase} ===")
    for d in payload.get("dbs", []):
        integ = d.get("integrity", "?")
        ntab = len(d.get("tables") or {})
        print(f"{integ:8} {d['mb']:8}MB  tables={ntab:3}  {d['path']}")
        if d.get("espn_boxscore_by_sport"):
            print("         espn_boxscore_rows", d["espn_boxscore_by_sport"])
        if d.get("wnba_max_date"):
            print(
                "         wnba max",
                d["wnba_max_date"],
                "rows",
                (d.get("tables") or {}).get("wnba"),
            )
        if d.get("mlb_max_date"):
            print(
                "         mlb_gamelog max",
                d["mlb_max_date"],
                "rows",
                (d.get("tables") or {}).get("mlb_gamelog"),
            )
    pq = payload.get("parquet")
    if pq:
        print(
            f"parquet files: {pq['parquet_count']}  missing sidecars: {len(pq['missing_step8_or_combined_sidecars'])}"
        )
        for row in pq["parquet"][:40]:
            print(f"  {row['mb']:7}MB  {row['path']}")
        if pq["missing_step8_or_combined_sidecars"]:
            print("MISSING parquet next to:")
            for p in pq["missing_step8_or_combined_sidecars"]:
                print("  ", p)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--phase", choices=("pre", "post"), default="pre")
    ap.add_argument("--date", default="")
    args = ap.parse_args()
    payload = inspect_dbs()
    if args.date:
        payload["parquet"] = inspect_parquet(args.date)
    print_summary(payload, args.phase)
    dest = ROOT / "logs" / f"db_smoke_{args.phase}_{args.date or 'nodate'}.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"wrote {dest}")
    bads = [d for d in payload["dbs"] if d.get("integrity") not in (None, "ok") and d.get("exists")]
    missing = [d for d in payload["dbs"] if not d.get("exists")]
    if bads:
        print("FAIL integrity", [d["path"] for d in bads])
        return 2
    if missing:
        print("WARN missing", [d["path"] for d in missing])
    if args.phase == "post" and args.date:
        pq = payload.get("parquet") or {}
        if not pq.get("parquet_count"):
            print("FAIL no parquet sidecars under outputs/" + args.date)
            return 3
        if pq.get("missing_step8_or_combined_sidecars"):
            print("WARN missing some expected sidecars")
            return 4
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
