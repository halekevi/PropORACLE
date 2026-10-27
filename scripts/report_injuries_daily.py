#!/usr/bin/env python3
"""One-page daily injury report + optional ntfy on starter status changes.

  py -3.14 scripts/report_injuries_daily.py --date 2026-10-27
  py -3.14 scripts/report_injuries_daily.py --date 2026-10-27 --ntfy --window 430PM
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd
import requests

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from player_name_norm import fold_player_name  # noqa: E402
from utils.injury_collect import injuries_dir, load_latest_rows, now_et  # noqa: E402
from utils.injury_gate import is_injury_out, is_injury_soft_flag, normalize_inj_status  # noqa: E402


def push_ntfy(body: str, *, title: str = "injury") -> bool:
    topic = str(os.environ.get("PROPORACLE_NTFY_TOPIC") or "").strip()
    if not topic:
        return False
    url = str(os.environ.get("PROPORACLE_NTFY_URL") or f"https://ntfy.sh/{topic}").strip()
    try:
        r = requests.post(
            url,
            data=body.encode("utf-8"),
            headers={"Title": title, "Tags": "injury,warning"},
            timeout=12,
        )
        return r.status_code < 300
    except Exception:
        return False


def _load_board_attach(root: Path, date: str) -> list[dict]:
    path = injuries_dir(date, root=root) / f"board_injury_{date}.csv"
    if not path.is_file():
        return []
    try:
        df = pd.read_csv(path)
    except Exception:
        return []
    return df.where(pd.notnull(df), None).to_dict(orient="records")


def _prev_status_map(root: Path, date: str) -> dict[tuple[str, str, str], str]:
    """Load prior collect summary's implied statuses from previous HHMM snapshots."""
    d = injuries_dir(date, root=root)
    if not d.is_dir():
        return {}
    snaps = sorted(d.glob("*_*.json"), key=lambda p: p.stat().st_mtime)
    # Use second-newest collect if present, else empty
    if len(snaps) < 2:
        return {}
    # Build from all but the newest sport files in the last collect batch is hard;
    # instead compare injuries_latest vs a saved state file.
    state = d / "status_state_prev.json"
    if not state.is_file():
        return {}
    try:
        data = json.loads(state.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[tuple[str, str, str], str] = {}
    for r in data.get("rows") or []:
        key = (
            str(r.get("sport") or "").upper(),
            fold_player_name(r.get("player")),
            str(r.get("team") or "").upper(),
        )
        if key[1]:
            out[key] = normalize_inj_status(r.get("status"))
    return out


def _save_status_state(root: Path, date: str, rows: list[dict]) -> None:
    d = injuries_dir(date, root=root)
    d.mkdir(parents=True, exist_ok=True)
    cur = d / "status_state.json"
    prev = d / "status_state_prev.json"
    if cur.is_file():
        try:
            prev.write_text(cur.read_text(encoding="utf-8"), encoding="utf-8")
        except Exception:
            pass
    payload = {
        "date": date,
        "rows": [
            {
                "sport": r.get("sport"),
                "player": r.get("player"),
                "team": r.get("team"),
                "status": normalize_inj_status(r.get("status")),
            }
            for r in rows
        ],
    }
    cur.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def build_report(root: Path, date: str) -> tuple[str, dict[str, Any]]:
    latest = load_latest_rows(root=root)
    board = _load_board_attach(root, date)
    by_sport: dict[str, list[dict]] = defaultdict(list)
    for r in latest:
        by_sport[str(r.get("sport") or "?").upper()].append(r)

    # Top-2 usage proxy on board: highest avg_l5 / season avg per team
    team_top: dict[tuple[str, str], list[tuple[float, str]]] = defaultdict(list)
    for r in board:
        sport = str(r.get("sport") or "").upper()
        team = str(r.get("team") or "").upper()
        player = str(r.get("player") or r.get("player_name") or "").strip()
        avg = None
        for k in ("avg_l5", "stat_last5_avg", "l5_avg", "season_avg", "avg"):
            try:
                avg = float(r.get(k))
                break
            except (TypeError, ValueError):
                continue
        if sport and team and player and avg is not None:
            team_top[(sport, team)].append((avg, player))
    top2_out: dict[str, list[str]] = defaultdict(list)
    for (sport, team), ranked in team_top.items():
        ranked = sorted(ranked, key=lambda x: x[0], reverse=True)
        names = [p for _a, p in ranked[:2]]
        out_names = []
        for r in latest:
            if str(r.get("sport") or "").upper() != sport:
                continue
            if str(r.get("team") or "").upper() != team:
                continue
            if not is_injury_out(r.get("status")):
                continue
            if fold_player_name(r.get("player")) in {fold_player_name(n) for n in names}:
                out_names.append(str(r.get("player")))
        if out_names:
            top2_out[sport].append(f"{team}: {', '.join(sorted(set(out_names)))}")

    lines = [
        f"# Daily injury report — {date}",
        "",
        f"Source: `data/injuries/injuries_latest.csv` ({len(latest)} rows). "
        "OUT hard-drops from tickets; Q/GTD/Doubtful flag only.",
        "",
    ]
    affected = [r for r in board if r.get("inj_status") or r.get("team_out_list") or r.get("opp_out_list")]
    for sport in sorted(by_sport.keys()):
        rows = by_sport[sport]
        outs = [r for r in rows if is_injury_out(r.get("status"))]
        soft = [r for r in rows if is_injury_soft_flag(r.get("status"))]
        lines.append(f"## {sport}")
        lines.append("")
        out_lbl = ", ".join(f"{r.get('player')} ({r.get('team')})" for r in outs[:30]) or "—"
        soft_lbl = ", ".join(
            f"{r.get('player')} ({r.get('team')}) [{normalize_inj_status(r.get('status'))}]"
            for r in soft[:30]
        ) or "—"
        lines.append(f"- OUT ({len(outs)}): {out_lbl}")
        lines.append(f"- Questionable/GTD/Doubtful ({len(soft)}): {soft_lbl}")
        if top2_out.get(sport):
            lines.append("- Top-2 usage OUT: " + "; ".join(top2_out[sport]))

        # Backup / minutes spots: board props for players still available when a
        # teammate (esp. top-2) is OUT — flag only, not ticketed.
        spots = [
            r
            for r in board
            if str(r.get("sport") or "").upper() == sport
            and int(r.get("minutes_opp") or 0) == 1
            and not is_injury_out(r.get("inj_status"))
        ]
        # Dedupe player+prop
        seen_spot: set[tuple[str, str]] = set()
        spot_lines: list[str] = []
        for r in spots:
            key = (
                fold_player_name(r.get("player") or r.get("player_name")),
                str(r.get("prop") or r.get("prop_type") or "").strip().lower(),
            )
            if key in seen_spot:
                continue
            seen_spot.add(key)
            spot_lines.append(
                f"  - {r.get('player')} {r.get('prop') or r.get('prop_type') or ''} "
                f"({r.get('team')}) ← OUT {r.get('minutes_opp_from') or r.get('team_out_list')}"
            )
        if spot_lines:
            lines.append(
                f"- Backup / minutes spots ({len(spot_lines)}, flag only — more minutes/stats potential):"
            )
            lines.extend(spot_lines[:25])
        elif top2_out.get(sport):
            lines.append(
                "- Backup / minutes spots: (no board props matched yet — attach after step8)"
            )

        board_hits = [
            r
            for r in board
            if str(r.get("sport") or "").upper() == sport
            and (
                is_injury_out(r.get("inj_status"))
                or r.get("team_out_list")
                or r.get("opp_out_list")
            )
        ]
        if board_hits:
            lines.append(f"- Board props touched: {len(board_hits)}")
            for r in board_hits[:15]:
                lines.append(
                    f"  - {r.get('player')} {r.get('prop') or r.get('prop_type') or ''} "
                    f"inj={r.get('inj_status') or '—'} "
                    f"team_out={r.get('team_out_list') or '—'} "
                    f"opp_out={r.get('opp_out_list') or '—'}"
                )
        lines.append("")

    payload = {
        "date": date,
        "n_latest": len(latest),
        "n_board_affected": len(affected),
        "top2_out": dict(top2_out),
        "n_minutes_opp": sum(1 for r in board if int(r.get("minutes_opp") or 0) == 1),
        "sports": {s: len(v) for s, v in by_sport.items()},
    }
    return "\n".join(lines), payload


def detect_starter_changes(root: Path, date: str, latest: list[dict]) -> list[str]:
    prev = _prev_status_map(root, date)
    alerts: list[str] = []
    for r in latest:
        key = (
            str(r.get("sport") or "").upper(),
            fold_player_name(r.get("player")),
            str(r.get("team") or "").upper(),
        )
        cur = normalize_inj_status(r.get("status"))
        old = prev.get(key, "")
        if old == cur:
            continue
        # Alert when moving into/out of OUT or softening from OUT
        if cur == "OUT" or old == "OUT":
            alerts.append(
                f"{key[0]} {r.get('player')} ({r.get('team')}): {old or '—'} → {cur or '—'}"
            )
    return alerts


def main() -> int:
    ap = argparse.ArgumentParser(description="Daily injury report + ntfy")
    ap.add_argument("--date", default="")
    ap.add_argument("--root", default="")
    ap.add_argument("--ntfy", action="store_true", help="Push starter status-change alerts")
    ap.add_argument("--window", default="", help="Run label e.g. 430PM (ntfy only after 4:30)")
    args = ap.parse_args()
    root = Path(args.root).resolve() if str(args.root).strip() else _REPO
    date = (args.date or now_et().strftime("%Y-%m-%d")).strip()[:10]

    md, payload = build_report(root, date)
    out = root / "data" / "reports" / f"injury_daily_{date}.md"
    out_json = root / "data" / "reports" / f"injury_daily_{date}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")
    out_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"[injury_report] -> {out}")

    latest = load_latest_rows(root=root)
    alerts = detect_starter_changes(root, date, latest)
    _save_status_state(root, date, latest)

    window = str(args.window or "").upper().replace(":", "")
    after_430 = window in {"430PM", "1630", "4:30PM", "T60", "T-60"} or args.ntfy
    if args.ntfy and after_430 and alerts:
        body = f"Injury status changes ({date}):\n" + "\n".join(alerts[:20])
        ok = push_ntfy(body, title=f"injury {date}")
        print(f"[injury_report] ntfy alerts={len(alerts)} sent={ok}")
    elif alerts:
        print(f"[injury_report] status changes (no ntfy): {len(alerts)}")
        for a in alerts[:10]:
            print(f"  - {a}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
