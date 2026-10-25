"""Confirm per-game proj_edge freeze: tipped games keep frozen_at and values.

  py -3.14 scripts/check_nba_proj_edge_lock.py --date 2026-10-25 --before path.jsonl
  py -3.14 scripts/check_nba_proj_edge_lock.py --date 2026-10-25 --before path.jsonl --cutoff 2026-10-25T20:25:00+00:00

A game that tipped before --cutoff must have frozen_at earlier than its tip
and the same projection fields as --before. Open games may update.
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from utils.nba_proj_edge_snapshot import FROZEN_COLS, snapshot_path


def _parse(raw: object) -> datetime | None:
    s = str(raw or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _load(path: Path) -> dict[str, dict]:
    out: dict[str, dict] = {}
    if not path.is_file():
        return out
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rec = json.loads(line)
        key = str(rec.get("leg_key") or "")
        if key:
            out[key] = rec
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default=datetime.now(timezone.utc).date().isoformat())
    ap.add_argument("--before", required=True)
    ap.add_argument("--after", default="")
    ap.add_argument("--cutoff", default="")
    args = ap.parse_args()
    before = _load(Path(args.before))
    after_p = Path(args.after) if args.after else snapshot_path(str(args.date)[:10])
    after = _load(after_p)
    cutoff = _parse(args.cutoff) or datetime.now(timezone.utc)
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)

    print(f"before={len(before)} after={len(after)} cutoff={cutoff.isoformat()}")
    print(f"after_path={after_p}")

    tipped = 0
    ok = 0
    bad: list[str] = []
    open_n = 0
    missing_tip = 0
    for key, rec in after.items():
        tip = _parse(rec.get("tip_utc"))
        if tip is None:
            missing_tip += 1
            continue
        if tip >= cutoff:
            open_n += 1
            continue
        tipped += 1
        prev = before.get(key)
        frozen = _parse(rec.get("frozen_at"))
        if frozen is None or frozen >= tip:
            bad.append(f"FROZEN_AT_NOT_BEFORE_TIP {key} frozen_at={rec.get('frozen_at')} tip={rec.get('tip_utc')}")
            continue
        if prev is None:
            bad.append(f"MISSING_IN_BEFORE {key}")
            continue
        if str(prev.get("frozen_at") or "") != str(rec.get("frozen_at") or ""):
            bad.append(f"FROZEN_AT_CHANGED {key} {prev.get('frozen_at')} -> {rec.get('frozen_at')}")
            continue
        drift = False
        for col in FROZEN_COLS:
            if prev.get(col) != rec.get(col):
                bad.append(f"VALUE_CHANGED {key} {col} {prev.get(col)} -> {rec.get(col)}")
                drift = True
                break
        if not drift:
            ok += 1

    print(f"tipped_before_cutoff={tipped} unchanged_ok={ok} open_after_cutoff={open_n} missing_tip_utc={missing_tip}")
    if not tipped:
        print("NO_TIPPED_GAMES — slate had no tip before cutoff; lock set is empty.")
    for line in bad[:40]:
        print(line)
    if bad:
        print(f"FAIL n={len(bad)}")
        return 1
    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
