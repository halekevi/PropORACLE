#!/usr/bin/env python3
"""
Ladder fill — probe PrizePicks Power payouts for every S/G/D mix (2–6 legs) × gap bucket.

Read-only: probe slips are built on the board and read, never submitted.

Rules:
  * Every Goblin/Demon probe leg has a real Standard sibling on the same fresh board
    (same player + prop, exactly one Standard row). The gap is |alt line − sibling line|
    and every leg is tagged standard_line_source="sibling". Never an estimated Standard.
  * Gap buckets 1.0 / 1.5 / 2.0+; a probe slip keeps all its Goblin/Demon legs in one bucket.
  * Fewest-samples-first: cells with the fewest ladder-fill scrapes run first until each
    (mix, bucket) has --target scrapes. All-Standard mixes use the fixed Standard table.
  * Results go to ui_runner/data/payout_ladder_live_cdp.json from the Power "N correct
    pays" read (power_min_x). The 1st-place figure is never stored as the payout.
  * CDP down or PrizePicks logged out → skip and record why in logs/LAST_LADDER_FILL.json.

  py -3.14 scripts/ladder_fill_capture.py --dry-run
  py -3.14 scripts/ladder_fill_capture.py --max-slips 25
"""
from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import urllib.request
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
SCRIPTS = ROOT / "scripts"
for _p in (ROOT, SCRIPTS):
    if str(_p) not in sys.path:
        sys.path.insert(0, str(_p))

ET = ZoneInfo("America/New_York")
CACHE_DIR = ROOT / "data" / "cache" / "ladder_fill"
REPORTS_DIR = ROOT / "data" / "reports"
STATUS_PATH = ROOT / "logs" / "LAST_LADDER_FILL.json"
LADDER_LIVE_PATH = ROOT / "ui_runner" / "data" / "payout_ladder_live_cdp.json"
PROBE_PREFIX = "LF-"

BUCKETS: tuple[str, ...] = ("1.0", "1.5", "2.0+")
MIN_LEGS, MAX_LEGS = 2, 6
# Board fetchers that write a step1 CSV with player/prop_type/line/pick_type/start_time/team.
STEP1_FETCHERS: dict[str, tuple[Path, list[str]]] = {
    "NBA": (ROOT / "Sports" / "NBA" / "scripts" / "step1_fetch_prizepicks_api.py", ["--no-preseason"]),
}


def gap_bucket(gap: float | None) -> str | None:
    if gap is None:
        return None
    try:
        g = float(gap)
    except (TypeError, ValueError):
        return None
    if 0.75 <= g < 1.25:
        return "1.0"
    if 1.25 <= g < 1.75:
        return "1.5"
    if g >= 1.75:
        return "2.0+"
    return None


def all_cells() -> list[tuple[int, int, int, str]]:
    """(n_std, n_gob, n_dem, bucket) for every 2–6 leg mix with at least one Goblin/Demon."""
    out: list[tuple[int, int, int, str]] = []
    for n in range(MIN_LEGS, MAX_LEGS + 1):
        for d in range(n + 1):
            for g in range(n - d + 1):
                if g == 0 and d == 0:
                    continue
                for b in BUCKETS:
                    out.append((n - g - d, g, d, b))
    return out


def comp_label(s: int, g: int, d: int) -> str:
    return f"{s}S+{g}G+{d}D"


# ── Board ────────────────────────────────────────────────────────────────────


def fetch_board(sport: str, date_str: str) -> Path | None:
    spec = STEP1_FETCHERS.get(sport)
    if spec is None:
        print(f"[ladder-fill] no board fetcher for {sport}")
        return None
    script, extra = spec
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    out = CACHE_DIR / f"step1_{sport.lower()}_{date_str}.csv"
    cmd = [sys.executable, "-X", "utf8", str(script), "--date", date_str, "--min_rows", "20",
           "--min_teams", "2", "--output", str(out), *extra]
    print(f"[ladder-fill] fetch {sport} board -> {out}")
    r = subprocess.run(cmd, cwd=str(script.parent.parent), capture_output=True, text=True,
                       encoding="utf-8", errors="replace")
    if r.returncode != 0 or not out.is_file():
        print(f"[ladder-fill] {sport} fetch failed (exit {r.returncode}): {(r.stdout or '')[-400:]}")
        return None
    return out


def build_pool(board: pd.DataFrame, sport: str, *, now: datetime, min_lead_min: int) -> list[dict]:
    """Probe-eligible legs: pregame, single player, Goblin/Demon only with a board sibling."""
    df = board.copy()
    df = df[~df["player"].astype(str).str.contains(r"\+| & ", regex=True)]
    df["pt"] = df["pick_type"].astype(str).str.strip().str.lower()
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df = df[df["line"].notna()]
    starts = pd.to_datetime(df["start_time"], errors="coerce", utc=True)
    df = df[starts > pd.Timestamp(now + timedelta(minutes=min_lead_min))]

    std = df[df["pt"] == "standard"].groupby(["player", "prop_type"])["line"].agg(["count", "first"])
    sib = std[std["count"] == 1]["first"]

    pool: list[dict] = []
    for r in df.itertuples(index=False):
        rd = r._asdict()
        pt = rd["pt"]
        leg: dict[str, Any] = {
            "player": str(rd["player"]).strip(),
            "prop_type": str(rd["prop_type"]).strip(),
            "line": float(rd["line"]),
            "direction": "OVER",
            "sport": sport,
            "team": str(rd.get("team") or ""),
            "opp": str(rd.get("opp_team") or ""),
            "game_id": str(rd.get("pp_game_id") or ""),
            "start_time": str(rd.get("start_time") or ""),
            "event_start_time": str(rd.get("start_time") or ""),
        }
        if pt == "standard":
            if (rd["player"], rd["prop_type"]) not in sib.index:
                continue
            leg.update(pick_type="Standard", standard_line=leg["line"], line_distance=0.0,
                       standard_line_source="self", bucket=None)
        elif pt in ("goblin", "demon"):
            key = (rd["player"], rd["prop_type"])
            if key not in sib.index:
                continue
            std_line = float(sib.loc[key])
            gap = round(abs(leg["line"] - std_line), 2)
            b = gap_bucket(gap)
            if b is None:
                continue
            # Goblin sits below its Standard, Demon above — anything else is a mislabel.
            if (pt == "goblin" and leg["line"] >= std_line) or (pt == "demon" and leg["line"] <= std_line):
                continue
            leg.update(pick_type=pt.capitalize(), standard_line=std_line, line_distance=gap,
                       delta=gap, standard_line_source="sibling", bucket=b)
        else:
            continue
        pool.append(leg)
    return pool


# ── Coverage + plan ──────────────────────────────────────────────────────────


def _row_cell(row: dict) -> tuple[str, str] | None:
    comp = str(row.get("leg_composition") or "").strip()
    deltas: list[float] = []
    for k in ("goblin_deltas", "demon_deltas"):
        v = row.get(k)
        items = v if isinstance(v, list) else [x for x in str(v or "").replace(",", "+").split("+") if x.strip()]
        for x in items:
            try:
                deltas.append(float(x))
            except (TypeError, ValueError):
                return None
    try:
        n_gd = sum(int(p[:-1] or 0) for p in comp.split("+") if p[-1:] in ("G", "D"))
    except ValueError:
        return None
    buckets = {gap_bucket(x) for x in deltas}
    if not comp or len(deltas) != n_gd or len(buckets) != 1 or None in buckets:
        return None
    return comp, buckets.pop()


def ladder_fill_coverage(path: Path = LADDER_LIVE_PATH) -> Counter:
    """Count ladder-fill (sibling-verified) scrapes per (composition, bucket)."""
    counts: Counter = Counter()
    if not path.is_file():
        return counts
    try:
        rows = json.loads(path.read_text(encoding="utf-8")).get("rows") or []
    except (OSError, json.JSONDecodeError):
        return counts
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not str(row.get("ticket_id") or "").startswith(PROBE_PREFIX):
            continue
        if str(row.get("source") or "") != "live_cdp":
            continue
        cell = _row_cell(row)
        if cell:
            counts[cell] += 1
    return counts


def _pick(cands: list[dict], n: int, used_players: set[str], avoid: set[str]) -> list[dict] | None:
    out: list[dict] = []
    for pref in (True, False):
        for c in cands:
            if len(out) == n:
                break
            p = c["player"]
            if p in used_players or (pref and p in avoid):
                continue
            out.append(c)
            used_players.add(p)
        if len(out) == n:
            return out
    return None


def build_slip(pool: list[dict], s: int, g: int, d: int, bucket: str, *,
               rng: random.Random, avoid: set[str]) -> list[dict] | None:
    gob = [c for c in pool if c["pick_type"] == "Goblin" and c["bucket"] == bucket]
    dem = [c for c in pool if c["pick_type"] == "Demon" and c["bucket"] == bucket]
    std = [c for c in pool if c["pick_type"] == "Standard"]
    if len(gob) < g or len(dem) < d or len(std) < s:
        return None
    center = {"1.0": 1.0, "1.5": 1.5, "2.0+": 2.0}[bucket]
    for _ in range(6):
        for lst in (gob, dem, std):
            rng.shuffle(lst)
        # Stay near the bucket's own gap (2.0+ would otherwise mix 2-point and 12-point Demons).
        for lst in (gob, dem):
            lst.sort(key=lambda c: round(abs(float(c["line_distance"]) - center) * 2) / 2)
        used: set[str] = set()
        legs: list[dict] = []
        for cands, n in ((dem, d), (gob, g), (std, s)):
            if n == 0:
                continue
            got = _pick(cands, n, used, avoid)
            if got is None:
                legs = []
                break
            legs.extend(got)
        if len(legs) == s + g + d and len({c["team"] for c in legs}) >= 2:
            return legs
    return None


def plan_probes(pool: list[dict], coverage: Counter, *, target: int, max_slips: int,
                seed: str) -> tuple[list[dict], dict[str, Any]]:
    rng = random.Random(seed)
    cells = all_cells()
    planned: Counter = Counter()
    infeasible: set[tuple] = set()
    slips: list[dict] = []
    avoid: set[str] = set()
    while len(slips) < max_slips:
        open_cells = [
            c for c in cells
            if c not in infeasible and coverage[(comp_label(*c[:3]), c[3])] + planned[c] < target
        ]
        if not open_cells:
            break
        open_cells.sort(key=lambda c: (
            coverage[(comp_label(*c[:3]), c[3])] + planned[c],
            sum(c[:3]),
            c[2] > 0,
            BUCKETS.index(c[3]),
        ))
        progressed = False
        for cell in open_cells:
            s, g, d, b = cell
            legs = build_slip(pool, s, g, d, b, rng=rng, avoid=avoid)
            if legs is None:
                infeasible.add(cell)
                continue
            avoid.update(c["player"] for c in legs)
            planned[cell] += 1
            slips.append({"cell": cell, "legs": legs})
            progressed = True
            break
        if not progressed:
            break
    total = len(cells)
    done = sum(1 for c in cells if coverage[(comp_label(*c[:3]), c[3])] >= target)
    meta = {
        "cells_total": total,
        "cells_at_target": done,
        "cells_infeasible_today": sorted(f"{comp_label(*c[:3])}@{c[3]}" for c in infeasible),
        "planned_cells": [f"{comp_label(*s['cell'][:3])}@{s['cell'][3]}" for s in slips],
    }
    return slips, meta


def write_probe_tickets(slips: list[dict], date_str: str, path: Path) -> list[dict]:
    tickets = []
    for i, sl in enumerate(slips, 1):
        s, g, d, b = sl["cell"]
        legs = [{k: v for k, v in leg.items() if k != "bucket"} for leg in sl["legs"]]
        tickets.append({
            "ticket_id": f"{PROBE_PREFIX}{date_str}-{i:02d}",
            "ladder_cell": f"{comp_label(s, g, d)}@{b}",
            "probe": True,
            "legs": legs,
        })
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "date": date_str,
        "kind": "ladder_fill_probe",
        "note": "Probe slips for payout reads only — never submitted.",
        "groups": [{"group_name": "LADDER FILL", "tickets": tickets}],
    }, indent=2, ensure_ascii=False), encoding="utf-8")
    return tickets


# ── Capture ──────────────────────────────────────────────────────────────────


def cdp_up(cdp_url: str) -> bool:
    try:
        with urllib.request.urlopen(cdp_url.rstrip("/") + "/json/version", timeout=5) as r:
            return r.status == 200
    except Exception:
        return False


def prizepicks_logged_in(cdp_url: str) -> bool | None:
    """False when the board shows a logged-out header; None when it cannot tell."""
    import collect_payout_data as cpd

    try:
        p, browser, _ctx, page = cpd.connect_existing_browser(cdp_url)
    except Exception as e:
        print(f"[ladder-fill] CDP attach failed: {e}")
        return False
    try:
        cpd.navigate_board_for_sport(page, "NBA", settle_ms=3000)
        text = str(page.evaluate("() => (document.body && document.body.innerText) || ''") or "")
        low = text.lower()
        if ("log in" in low or "login" in low) and "sign up" in low and "entries" not in low:
            return False
        return True
    except Exception as e:
        print(f"[ladder-fill] login check inconclusive: {e}")
        return None
    finally:
        for fn in (browser.close, p.stop):
            try:
                fn()
            except Exception:
                pass


def tag_probe_rows(path: Path = LADDER_LIVE_PATH) -> int:
    if not path.is_file():
        return 0
    data = json.loads(path.read_text(encoding="utf-8"))
    n = 0
    for row in data.get("rows") or []:
        if isinstance(row, dict) and str(row.get("ticket_id") or "").startswith(PROBE_PREFIX):
            row["probe"] = "ladder_fill"
            row["standard_line_source"] = "sibling"
            if "ladder_fill" not in str(row.get("notes") or ""):
                row["notes"] = f"ladder_fill probe (never submitted); {row.get('notes') or ''}".strip("; ")
            n += 1
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    return n


def write_status(payload: dict) -> None:
    STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = {"at": datetime.now(timezone.utc).isoformat(), **payload}
    STATUS_PATH.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[ladder-fill] status -> {STATUS_PATH}: {payload.get('status')}")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--date", default="", help="ET slate date (default: today ET)")
    ap.add_argument("--sports", default="NBA", help="Comma list of boards to probe (default NBA)")
    ap.add_argument("--board", default="", help="Use this step1 CSV instead of fetching (testing)")
    ap.add_argument("--max-slips", type=int, default=25)
    ap.add_argument("--target", type=int, default=5, help="Scrapes wanted per (mix, bucket)")
    ap.add_argument("--min-lead-min", type=int, default=60, help="Skip legs starting sooner than this")
    ap.add_argument("--cdp-url", default="http://127.0.0.1:9222")
    ap.add_argument("--max-runtime-sec", type=float, default=2700.0)
    ap.add_argument("--dry-run", action="store_true", help="Plan and write probe JSON; no CDP")
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    date_str = (args.date or now.astimezone(ET).strftime("%Y-%m-%d"))[:10]
    max_slips = max(1, min(int(args.max_slips), 25))

    if not args.dry_run and not cdp_up(args.cdp_url):
        write_status({"status": "skipped", "reason": f"CDP not up on {args.cdp_url}", "date": date_str})
        return 0

    pool: list[dict] = []
    for sport in [s.strip().upper() for s in args.sports.split(",") if s.strip()]:
        path = Path(args.board) if args.board else fetch_board(sport, date_str)
        if path is None or not path.is_file():
            continue
        pool += build_pool(pd.read_csv(path), sport, now=now, min_lead_min=int(args.min_lead_min))
    by_type = Counter((c["pick_type"], c.get("bucket")) for c in pool)
    print(f"[ladder-fill] pool={len(pool)} {dict(by_type)}")
    if not pool:
        write_status({"status": "skipped", "reason": "no pregame board legs with Standard siblings",
                      "date": date_str})
        return 0

    coverage = ladder_fill_coverage()
    slips, meta = plan_probes(pool, coverage, target=int(args.target), max_slips=max_slips,
                              seed=f"{date_str}-{now.hour}")
    probe_path = CACHE_DIR / f"probe_tickets_{date_str}.json"
    tickets = write_probe_tickets(slips, date_str, probe_path)
    print(f"[ladder-fill] planned {len(tickets)} probe slips -> {probe_path}")
    for t in tickets:
        print(f"  {t['ticket_id']} {t['ladder_cell']}: " + " | ".join(
            f"{l['player']} {l['prop_type']} {l['line']:g} {l['pick_type'][0]}"
            + (f" (std {l['standard_line']:g})" if l['pick_type'] != 'Standard' else "")
            for l in t["legs"]))

    base = {"date": date_str, "pool": len(pool), "planned": len(tickets), **meta}
    if args.dry_run:
        write_status({"status": "dry_run", **base})
        return 0
    if not tickets:
        write_status({"status": "nothing_to_do", **base})
        return 0

    logged_in = prizepicks_logged_in(args.cdp_url)
    if logged_in is False:
        write_status({"status": "skipped", "reason": "PrizePicks not logged in on CDP Chrome", **base})
        return 0

    import collect_payout_data as cpd

    cpd._sacks_under_observe_slip = lambda _d: None
    out_path = REPORTS_DIR / f"payout_ladder_fill_{date_str}.json"
    cpd._acquire_payout_capture_lock()
    try:
        rc = cpd.capture_tickets_from_board(
            tickets_path=probe_path,
            output_path=out_path,
            fields=list(cpd.DEFAULT_CAPTURE_FIELDS),
            cdp_url=args.cdp_url,
            entry_amount=1.0,
            max_cases=len(tickets),
            delay_sec=1.0,
            write_back=False,
            date_override=date_str,
            strict_lines=True,
            gentle=True,
            max_runtime_sec=float(args.max_runtime_sec),
        )
    finally:
        cpd._release_payout_capture_lock()

    captured = []
    if out_path.is_file():
        captured = [s for s in (json.loads(out_path.read_text(encoding="utf-8")).get("slips") or [])
                    if isinstance(s, dict) and str(s.get("ticket_id") or "").startswith(PROBE_PREFIX)]
    ok = [s for s in captured if str(s.get("status") or "") == "ok"]
    if ok:
        cpd.sync_captures_to_payout_ladder_live(ok, date_str=date_str, tickets_path=probe_path)
        tag_probe_rows()
    after = ladder_fill_coverage()
    errors = Counter(str(s.get("error") or "")[:40] for s in captured if s.get("status") != "ok")
    write_status({
        "status": "ok" if ok else "no_rows",
        "capture_exit": rc,
        "captured": len(captured),
        "ok": len(ok),
        "errors": dict(errors),
        "cells_at_target_after": sum(1 for c in all_cells()
                                     if after[(comp_label(*c[:3]), c[3])] >= int(args.target)),
        **base,
    })
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
