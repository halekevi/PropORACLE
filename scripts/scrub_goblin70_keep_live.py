#!/usr/bin/env python3
"""Lightweight Goblin-70 keep revalidation against the current board.

Full G70 rebuilds are expensive and cadence-bound (~4–8h worst healthy gap).
This pass only re-checks already-published G70 legs against today's step8 board
via ``goblin_70_eligible`` / keep gates, and drops slips that now fail.

Off-board legs are left alone (no false-drop from incomplete published fields).
CORE/mixer hygiene stays on the dual-card merge path.

  py -3.14 scripts/scrub_goblin70_keep_live.py
  py -3.14 scripts/scrub_goblin70_keep_live.py --date 2026-09-15 --write --publish-live
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from build_goblin70_tickets import (  # noqa: E402
    _board_leg_key,
    _pick_board_row,
    index_board_legs,
    load_today_board,
    publish_live,
)
from utils.ticket_70_pool import (  # noqa: E402
    goblin_70_eligible,
    standard_ticket_eligible,
)
from utils.ui_live_json import (  # noqa: E402
    existing_path,
    sync_live_json_pairs,
    write_paths as live_json_write_paths,
)

ET = ZoneInfo("America/New_York")
LOGS = _REPO / "logs"


def _is_g70_family(ticket: dict, group_name: str = "") -> bool:
    track = str(ticket.get("ticket_track") or "").strip().lower()
    if track.startswith("goblin70"):
        return True
    policy = str(ticket.get("pool_policy") or ticket.get("core_recipe") or "").strip().lower()
    if policy.startswith("goblin70"):
        return True
    mode = str(ticket.get("mode") or "").strip().lower()
    if mode.startswith("goblin70"):
        return True
    gn = str(group_name or "").upper()
    return "GOBLIN-70" in gn or "YOLO" in gn


def _load_tickets(path: Path | None = None) -> tuple[Path, dict]:
    if path and path.is_file():
        return path, json.loads(path.read_text(encoding="utf-8"))
    candidates = [
        _REPO.parent / "PropORACLE_main_cp" / "ui_runner" / "templates" / "tickets_latest.json",
        _REPO / "ui_runner" / "runtime" / "tickets_latest.json",
        _REPO / "ui_runner" / "templates" / "tickets_latest.json",
    ]
    ep = existing_path("tickets_latest.json", _REPO)
    if ep is not None:
        candidates.insert(0, ep)
    for p in candidates:
        if p.is_file():
            return p, json.loads(p.read_text(encoding="utf-8"))
    raise FileNotFoundError("tickets_latest.json not found")


def _leg_fail_reason(leg: dict, board_row: dict | None) -> str | None:
    """Return failure reason when board-backed keep fails; None if keep or unmatched."""
    if board_row is None:
        return None  # off-board / unmatched — do not false-drop
    # Prefer the published ticket pick_type (YOLO may mix Standard onto Goblin slips).
    pt = str(leg.get("pick_type") or board_row.get("pick_type") or "").strip().title()
    side = str(
        leg.get("direction")
        or leg.get("side")
        or board_row.get("side")
        or board_row.get("direction")
        or "OVER"
    ).strip().upper()
    # Evaluate eligibility on the board row (full L5/L10/D fields), not the thin web leg.
    if pt == "Goblin" and "UNDER" not in side:
        if goblin_70_eligible(board_row):
            return None
        return "goblin_70_keep_fail"
    if pt == "Standard":
        if standard_ticket_eligible(board_row):
            return None
        return "standard_gate_fail"
    return None


def scrub_payload(payload: dict, board: list[dict]) -> tuple[dict, dict]:
    idx = index_board_legs(board)
    report: dict = {
        "g70_tickets_in": 0,
        "g70_tickets_dropped": 0,
        "g70_legs_checked": 0,
        "g70_legs_unmatched": 0,
        "g70_legs_failed": 0,
        "dropped": [],
        "failed_legs": [],
    }
    out = dict(payload)
    new_groups: list[dict] = []
    for g in payload.get("groups") or []:
        if not isinstance(g, dict):
            continue
        gname = str(g.get("group_name") or "")
        kept: list[dict] = []
        for t in g.get("tickets") or []:
            if not isinstance(t, dict):
                continue
            if not _is_g70_family(t, gname):
                kept.append(t)
                continue
            report["g70_tickets_in"] += 1
            legs = [leg for leg in (t.get("legs") or []) if isinstance(leg, dict)]
            fail_reasons: list[dict] = []
            for leg in legs:
                k = _board_leg_key(
                    leg.get("player"),
                    leg.get("sport"),
                    leg.get("prop_type") or leg.get("prop"),
                    leg.get("pick_type"),
                    leg.get("direction") or leg.get("side"),
                )
                try:
                    old_line = float(leg.get("line"))
                except (TypeError, ValueError):
                    old_line = None
                row = _pick_board_row(idx.get(k) or [], old_line)
                report["g70_legs_checked"] += 1
                if row is None:
                    report["g70_legs_unmatched"] += 1
                    continue
                reason = _leg_fail_reason(leg, row)
                if reason:
                    report["g70_legs_failed"] += 1
                    fail_reasons.append(
                        {
                            "player": leg.get("player"),
                            "prop": leg.get("prop_type") or leg.get("prop"),
                            "line": leg.get("line"),
                            "reason": reason,
                            "board_l5": row.get("l5_over") or row.get("l5"),
                            "board_def": row.get("def") or row.get("def_tier"),
                        }
                    )
            if fail_reasons:
                report["g70_tickets_dropped"] += 1
                report["dropped"].append(
                    {
                        "group": gname,
                        "ticket_id": t.get("ticket_id") or t.get("id"),
                        "n_fail_legs": len(fail_reasons),
                        "legs": fail_reasons,
                    }
                )
                report["failed_legs"].extend(fail_reasons)
                continue
            kept.append(t)
        if not kept:
            continue
        ng = dict(g)
        ng["tickets"] = kept
        new_groups.append(ng)
    out["groups"] = new_groups
    out["g70_keep_scrubbed_at"] = datetime.now(timezone.utc).isoformat()
    return out, report


def _write_tickets(payload: dict) -> list[Path]:
    text = json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False)
    written: list[Path] = []
    paths = list(live_json_write_paths("tickets_latest.json", _REPO, include_data_snapshot=True))
    main_cp = _REPO.parent / "PropORACLE_main_cp"
    if main_cp.is_dir() and main_cp.resolve() != _REPO.resolve():
        paths.extend(
            live_json_write_paths("tickets_latest.json", main_cp, include_data_snapshot=True)
        )
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        written.append(path)
        print("web", path)
    try:
        sync_live_json_pairs(_REPO)
        if main_cp.is_dir() and main_cp.resolve() != _REPO.resolve():
            sync_live_json_pairs(main_cp)
    except Exception as exc:
        print(f"WARN: live JSON sync skipped ({exc})")
    return written


def write_scrub_stamp(report: dict, *, date: str) -> Path:
    LOGS.mkdir(parents=True, exist_ok=True)
    stamp = {
        "ok": True,
        "kind": "g70_keep_scrub",
        "slate_date": date,
        "scrubbed_at_utc": datetime.now(timezone.utc).isoformat(),
        "scrubbed_at_et": datetime.now(ET).isoformat(),
        "g70_tickets_in": report.get("g70_tickets_in"),
        "g70_tickets_dropped": report.get("g70_tickets_dropped"),
        "g70_legs_failed": report.get("g70_legs_failed"),
        "g70_legs_unmatched": report.get("g70_legs_unmatched"),
        "dropped_groups": [d.get("group") for d in (report.get("dropped") or [])],
    }
    path = LOGS / "LAST_G70_KEEP_SCRUB.json"
    path.write_text(json.dumps(stamp, indent=2), encoding="utf-8")
    # Mirror into sibling worktrees (same pattern as daily health).
    for peer in _REPO.parent.glob("PropORACLE*"):
        if not peer.is_dir() or peer.resolve() == _REPO.resolve():
            continue
        peer_logs = peer / "logs"
        try:
            peer_logs.mkdir(parents=True, exist_ok=True)
            (peer_logs / "LAST_G70_KEEP_SCRUB.json").write_text(
                json.dumps(stamp, indent=2), encoding="utf-8"
            )
        except OSError:
            pass
    return path


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--date", default="", help="Slate date (default: tickets_latest.date)")
    ap.add_argument("--tickets", type=Path, default=None)
    ap.add_argument("--write", action="store_true", help="Rewrite tickets_latest when drops occur")
    ap.add_argument(
        "--write-always",
        action="store_true",
        help="Rewrite tickets_latest even when nothing dropped (stamp scrubbed_at)",
    )
    ap.add_argument(
        "--publish-live",
        action="store_true",
        help="After write with drops, Publish-LiveSite.ps1 so Railway picks up scrub",
    )
    ap.add_argument("--dry-run", action="store_true", help="Report only (default if no --write)")
    args = ap.parse_args()

    src, payload = _load_tickets(args.tickets)
    date = (args.date or str(payload.get("date") or "")).strip()[:10]
    if not date:
        date = datetime.now(ET).strftime("%Y-%m-%d")
    board = load_today_board(date)
    print(f"scrub source={src} date={date} board_rows={len(board)}")
    if not board:
        print("WARN: empty board — cannot board-back keep checks; no drops")
        report = {
            "g70_tickets_in": 0,
            "g70_tickets_dropped": 0,
            "g70_legs_checked": 0,
            "g70_legs_unmatched": 0,
            "g70_legs_failed": 0,
            "dropped": [],
            "warn": "empty_board",
        }
        write_scrub_stamp(report, date=date)
        return 0

    scrubbed, report = scrub_payload(payload, board)
    print(
        f"g70_in={report['g70_tickets_in']} dropped={report['g70_tickets_dropped']} "
        f"legs_failed={report['g70_legs_failed']} unmatched={report['g70_legs_unmatched']}"
    )
    for d in report["dropped"][:20]:
        print(f"  DROP {d['group']} id={d.get('ticket_id')} fails={d['n_fail_legs']}")
        for leg in d.get("legs") or []:
            print(
                f"    {leg.get('player')} {leg.get('prop')} {leg.get('line')} "
                f"({leg.get('reason')})"
            )

    LOGS.mkdir(parents=True, exist_ok=True)
    (LOGS / f"g70_keep_scrub_{date}.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    write_scrub_stamp(report, date=date)

    do_write = bool(args.write or args.write_always) and not args.dry_run
    if do_write and (report["g70_tickets_dropped"] or args.write_always):
        _write_tickets(scrubbed)
        if args.publish_live and report["g70_tickets_dropped"]:
            rc = publish_live(date)
            print(f"publish-live exit {rc}")
            return int(rc)
    elif report["g70_tickets_dropped"] and not do_write:
        print("NOTE: drops found but --write not set; live card unchanged")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
