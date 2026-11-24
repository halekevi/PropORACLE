"""Move boards between active / inactive in config/active_sports.json from ESPN's calendar.

A league goes inactive the day after its championship and returns the day before its
first preseason game (utils.season_calendar). Year-round sports are left as they are.
A league whose status is unknown (ESPN error) is never moved.

    py -3.14 scripts/sync_active_sports.py            # apply
    py -3.14 scripts/sync_active_sports.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from utils.season_calendar import BOARD_LEAGUE, calendar  # noqa: E402

CONFIG = REPO / "config" / "active_sports.json"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--refresh", action="store_true", help="Ignore today's cached ESPN calendar")
    args = ap.parse_args()

    cal = calendar(refresh=args.refresh).get("leagues") or {}
    cfg = json.loads(CONFIG.read_text(encoding="utf-8"))
    active = list(cfg.get("active") or [])
    inactive = list(cfg.get("inactive") or [])

    moves: list[str] = []
    for src, dst, want in ((active, inactive, False), (inactive, active, True)):
        for row in list(src):
            key = str(row.get("key") or "").strip().upper()
            st = cal.get(BOARD_LEAGUE.get(key, ""), {})
            if st.get("active") is want:
                src.remove(row)
                dst.append(row)
                moves.append(f"{key} -> {'active' if want else 'inactive'} ({st.get('reason', '')})")

    for lg, st in sorted(cal.items()):
        extra = " ".join(f"{k}={st[k]}" for k in ("off_from", "resume_on") if st.get(k))
        print(f"  {lg:5} active={st.get('active')}  {st.get('reason', '')}  {extra}".rstrip())

    if not moves:
        print("[sync_active_sports] no changes")
        return 0
    for m in moves:
        print(f"[sync_active_sports] {m}")
    if args.dry_run:
        return 0
    cfg["active"], cfg["inactive"] = active, inactive
    CONFIG.write_text(json.dumps(cfg, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    # The live site reads this file from origin/main.
    import subprocess

    rel = str(CONFIG.relative_to(REPO))
    msg = "chore: active sports from ESPN calendar — " + "; ".join(moves)
    for cmd in (["git", "add", rel], ["git", "commit", "-m", msg, "--", rel], ["git", "push"]):
        res = subprocess.run(cmd, cwd=REPO, capture_output=True, text=True)
        if res.returncode != 0:
            print(f"[sync_active_sports] WARN {' '.join(cmd[:2])} failed: {(res.stderr or res.stdout).strip()[:200]}")
            break
    try:
        from utils.pipeline_alerts import emit_pipeline_alert

        emit_pipeline_alert("active-sports", "; ".join(moves))
    except Exception:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
