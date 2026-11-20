#!/usr/bin/env python3
"""Rewrite proporacle_ref.soccer.league display names → ESPN slugs.

Writes a before/after tally CSV under backups/, then UPDATEs in place.
Safe to re-run (already-slug rows are unchanged).
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
import sys

sys.path.insert(0, str(REPO))
from utils.soccer_league_normalize import is_espn_slug, normalize_soccer_league  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--db",
        default=str(REPO / "data" / "cache" / "proporacle_ref.db"),
        help="Path to proporacle_ref.db",
    )
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    db = Path(args.db)
    if not db.is_file():
        raise SystemExit(f"missing db: {db}")

    con = sqlite3.connect(str(db))
    before = pd.read_sql(
        "SELECT league, COUNT(*) AS n FROM soccer GROUP BY league ORDER BY n DESC",
        con,
    )
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = REPO / "backups"
    backup_dir.mkdir(parents=True, exist_ok=True)
    before_path = backup_dir / f"soccer_league_before_{stamp}.csv"
    before.to_csv(before_path, index=False, encoding="utf-8-sig")
    print(f"wrote {before_path} ({len(before)} distinct leagues)")

    updates: list[tuple[str, str, int]] = []
    for r in before.itertuples(index=False):
        raw = str(r.league or "")
        norm = normalize_soccer_league(raw)
        if not norm or norm == raw:
            continue
        if raw and norm != raw:
            updates.append((norm, raw, int(r.n)))

    print(f"mappings to apply: {len(updates)}")
    for norm, raw, n in sorted(updates, key=lambda t: -t[2])[:25]:
        print(f"  {raw!r} → {norm!r}  ({n:,})")
    if len(updates) > 25:
        print(f"  … {len(updates) - 25} more")

    if args.dry_run:
        print("dry-run: no UPDATE")
        con.close()
        return

    cur = con.cursor()
    changed = 0
    for norm, raw, _n in updates:
        cur.execute("UPDATE soccer SET league = ? WHERE league = ?", (norm, raw))
        changed += cur.rowcount
    con.commit()

    after = pd.read_sql(
        "SELECT league, COUNT(*) AS n FROM soccer GROUP BY league ORDER BY n DESC",
        con,
    )
    after_path = backup_dir / f"soccer_league_after_{stamp}.csv"
    after.to_csv(after_path, index=False, encoding="utf-8-sig")
    name_left = after[~after["league"].map(is_espn_slug) & after["league"].astype(str).str.len().gt(0)]
    print(f"UPDATE rows touched={changed:,}")
    print(f"wrote {after_path} ({len(after)} distinct leagues)")
    print(f"non-slug leagues remaining: {len(name_left)} ({int(name_left['n'].sum()) if len(name_left) else 0} rows)")
    if len(name_left):
        print(name_left.head(20).to_string(index=False))
    # Spot-check Claude's four
    for slug in ("usa.1", "esp.1", "ita.1", "mex.1"):
        n = int(after.loc[after["league"] == slug, "n"].sum()) if slug in set(after["league"]) else 0
        print(f"  {slug}: {n:,}")
    con.close()


if __name__ == "__main__":
    main()
