"""Gated ticket construction efficiency — the instrument for \"are we efficient?\"

Reads keep-gated Goblin-70 archives (L5=5 + sport keep gates), grades slips
against actuals, and reports Goblin-only / Standard-only / Mixed separately
(plus YOLO side tracks). This is *not* the ungated book catalog.

Usage:
  py -3.14 scripts/report_gated_construction_efficiency.py
  py -3.14 scripts/report_gated_construction_efficiency.py --from 2026-09-01 --to 2026-09-27
  py -3.14 scripts/report_gated_construction_efficiency.py --append-history
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(REPO), str(REPO / "scripts")]

import build_ticket_eval as te  # noqa: E402
from utils.gated_construction_tracks import (  # noqa: E402
    GATED_CONSTRUCTION_TRACKS,
    GATED_GOBLIN,
    GATED_MIXED,
    GATED_STANDARD,
    TRACK_LABELS,
    efficiency_track,
)
from utils.proporacle_data_root import persistent_data_dir  # noqa: E402

REPORTS = REPO / "data" / "reports"
OUT_JSON = REPORTS / "gated_construction_efficiency.json"
OUT_LATEST = REPORTS / "gated_construction_efficiency_latest.json"
G70_GLOB = "goblin70_tickets_*.json"

TRACK_ORDER = (
    GATED_GOBLIN,
    GATED_STANDARD,
    GATED_MIXED,
    "standard_yolo",
    "goblin70_yolo",
)

# Construction-relevant infrastructure landings inside the archive window.
# actuals_merge is measurement-only (does not change how tickets were built).
INFRA_FIXES: dict[str, dict[str, str]] = {
    "saves_own_d": {
        "date": "2026-09-15",
        "note": "Saves keep gate own-D + opp-off",
    },
    "season_boundary": {
        "date": "2026-09-20",
        "note": "NFL weeks 1-3 PRIOR_SEASON L5 fill",
    },
    "exposure_anchor": {
        "date": "2026-09-24",
        "note": "Exposure trim + cross-ticket anchor cap",
    },
    "nfl_wave1_goblin": {
        "date": "2026-09-27",
        "note": "NFL wave-1 Goblin tickets enabled",
    },
    "actuals_merge_grade": {
        "date": "2026-09-28",
        "note": "NFL/CFB actuals CSV merge (measurement only)",
    },
}
# First archive day on/after the exposure+anchor cluster.
POST_FIX_START = "2026-09-25"


def _parse_date(s: str) -> date:
    return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()


def _daterange(start: date, end: date) -> list[str]:
    out: list[str] = []
    cur = start
    while cur <= end:
        out.append(cur.isoformat())
        cur += timedelta(days=1)
    return out


def _archive_date(path: Path) -> str | None:
    name = path.stem  # goblin70_tickets_2026-09-27 or ..._l5eq5
    parts = name.split("_")
    for p in parts:
        if len(p) == 10 and p[4] == "-" and p[7] == "-":
            try:
                _parse_date(p)
                return p
            except ValueError:
                pass
    return None


def discover_g70_archives(*, start: date | None, end: date | None) -> list[tuple[str, Path]]:
    """Prefer dated archives; skip ``*_latest`` and prefer non-suffix over ``_l5eq5``."""
    by_date: dict[str, list[Path]] = defaultdict(list)
    for path in sorted(REPORTS.glob(G70_GLOB)):
        if path.name.endswith("_latest.json"):
            continue
        ds = _archive_date(path)
        if not ds:
            continue
        d = _parse_date(ds)
        if start and d < start:
            continue
        if end and d > end:
            continue
        by_date[ds].append(path)

    chosen: list[tuple[str, Path]] = []
    for ds, paths in sorted(by_date.items()):
        # Prefer the plain dated file over diagnostic suffixes.
        plain = [p for p in paths if p.stem == f"goblin70_tickets_{ds}"]
        pick = plain[0] if plain else paths[0]
        chosen.append((ds, pick))
    return chosen


def payload_from_g70(path: Path, date_s: str) -> dict[str, Any]:
    raw = json.loads(path.read_text(encoding="utf-8"))
    tickets = list(raw.get("tickets") or [])
    if not tickets and isinstance(raw.get("groups"), list):
        return {
            "date": str(raw.get("date") or date_s)[:10],
            "groups": raw["groups"],
            "source": str(path.name),
        }

    by_group: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for t in tickets:
        if not isinstance(t, dict):
            continue
        # Normalize side → direction for ticket_eval matching.
        legs = []
        for leg in t.get("legs") or []:
            if not isinstance(leg, dict):
                continue
            L = dict(leg)
            if not L.get("direction") and L.get("side"):
                L["direction"] = L["side"]
            if not L.get("prop_type") and L.get("prop"):
                L["prop_type"] = L["prop"]
            legs.append(L)
        tt = dict(t)
        tt["legs"] = legs
        # Archives store ``id`` (e.g. P3-1). Prefer that as ticket_id for dedupe.
        if not tt.get("ticket_id") and tt.get("id"):
            tt["ticket_id"] = f"{date_s}|{tt.get('id')}"
        # Money outcome reads power_payout / flex_payout / payout JSON.
        # G70 archives store N-correct multiples on n_correct instead.
        nc = tt.get("n_correct") or {}
        n_legs = int(tt.get("n_legs") or len(legs) or 0)
        all_hit = None
        if isinstance(nc, dict) and n_legs:
            all_hit = nc.get(str(n_legs), nc.get(n_legs))
        try:
            all_hit_f = float(all_hit) if all_hit is not None else None
        except (TypeError, ValueError):
            all_hit_f = None
        product = str(tt.get("product") or tt.get("play") or "").strip().lower()
        if all_hit_f is not None:
            if "flex" in product:
                if not tt.get("flex_payout"):
                    tt["flex_payout"] = all_hit_f
            elif not tt.get("power_payout"):
                tt["power_payout"] = all_hit_f
            if not isinstance(tt.get("payout"), dict):
                tt["payout"] = {
                    "payout": all_hit_f,
                    "n_correct": nc,
                    "product": tt.get("product") or tt.get("play") or "Power",
                }
        if tt.get("ev_n_correct") is not None and tt.get("predicted_ev") is None:
            try:
                tt["predicted_ev"] = float(tt["ev_n_correct"]) - 1.0
            except (TypeError, ValueError):
                pass
        gname = str(tt.get("web_group") or tt.get("name") or "Goblin-70")
        by_group[gname].append(tt)

    groups = [{"group_name": gn, "tickets": ts} for gn, ts in by_group.items()]
    return {
        "date": str(raw.get("date") or date_s)[:10],
        "groups": groups,
        "source": str(path.name),
        "pool": raw.get("pool"),
    }


def _empty_bucket() -> dict[str, Any]:
    return {
        "slips": 0,
        "decided": 0,
        "wins": 0,
        "losses": 0,
        "voids": 0,
        "ungraded_days": 0,
        "win_rate": None,
        "net_per_10": None,
        "roi_pct": None,
        "avg_ev_predicted": None,
        "avg_legs": None,
        "by_date": [],
    }


def _rollup(rows: list[dict[str, Any]]) -> dict[str, Any]:
    b = _empty_bucket()
    if not rows:
        return b
    slips = sum(int(r.get("n_slips") or r.get("n_tickets") or 0) for r in rows)
    wins = sum(int(r.get("wins") or 0) for r in rows)
    losses = sum(int(r.get("losses") or 0) for r in rows)
    voids = sum(int(r.get("voids") or 0) for r in rows)
    decided = wins + losses
    # Reconstruct net from per-day net_per_10 * decided when present.
    net_total = 0.0
    net_n = 0
    ev_w = 0.0
    ev_n = 0.0
    legs_w = 0.0
    legs_n = 0.0
    for r in rows:
        d = int(r.get("wins") or 0) + int(r.get("losses") or 0)
        if r.get("net_per_10") is not None and d:
            net_total += float(r["net_per_10"]) * d
            net_n += d
        if r.get("avg_ev_predicted") is not None and d:
            ev_w += float(r["avg_ev_predicted"]) * d
            ev_n += d
        n_slips = int(r.get("n_slips") or r.get("n_tickets") or 0)
        if r.get("avg_legs") is not None and n_slips:
            legs_w += float(r["avg_legs"]) * n_slips
            legs_n += n_slips
    b.update(
        {
            "slips": slips,
            "decided": decided,
            "wins": wins,
            "losses": losses,
            "voids": voids,
            "win_rate": round(wins / decided, 4) if decided else None,
            "net_per_10": round(net_total / net_n, 2) if net_n else None,
            "roi_pct": round(100.0 * net_total / (10.0 * net_n), 2) if net_n else None,
            "avg_ev_predicted": round(ev_w / ev_n, 4) if ev_n else None,
            "avg_legs": round(legs_w / legs_n, 2) if legs_n else None,
            "by_date": [
                {
                    "date": r.get("date"),
                    "slips": r.get("n_slips") or r.get("n_tickets"),
                    "wins": r.get("wins"),
                    "losses": r.get("losses"),
                    "voids": r.get("voids"),
                    "win_rate": r.get("win_rate"),
                    "net_per_10": r.get("net_per_10"),
                    "roi_pct": r.get("roi_pct"),
                    "avg_ev_predicted": r.get("avg_ev_predicted"),
                }
                for r in sorted(rows, key=lambda x: str(x.get("date") or ""))
            ],
        }
    )
    return b


def _rollup_by_date_window(
    by_date: list[dict[str, Any]],
    *,
    start: str | None = None,
    end: str | None = None,
) -> dict[str, Any]:
    """Roll a track's by_date rows. ``end`` is exclusive."""
    wins = losses = voids = slips = 0
    net_total = 0.0
    net_n = 0
    dates: list[str] = []
    for d in by_date:
        ds = str(d.get("date") or "")
        if start and ds < start:
            continue
        if end and ds >= end:
            continue
        w = int(d.get("wins") or 0)
        l = int(d.get("losses") or 0)
        v = int(d.get("voids") or 0)
        wins += w
        losses += l
        voids += v
        slips += int(d.get("slips") or 0)
        decided = w + l
        if d.get("net_per_10") is not None and decided:
            net_total += float(d["net_per_10"]) * decided
            net_n += decided
        dates.append(ds)
    decided = wins + losses
    return {
        "slips": slips,
        "decided": decided,
        "wins": wins,
        "losses": losses,
        "voids": voids,
        "win_rate": round(wins / decided, 4) if decided else None,
        "roi_pct": round(100.0 * net_total / (10.0 * net_n), 2) if net_n else None,
        "dates": dates,
    }


def _era_split(tracks_out: dict[str, Any]) -> dict[str, Any]:
    """Before vs after POST_FIX_START, plus Saves / season-boundary cuts."""
    before: dict[str, Any] = {}
    after: dict[str, Any] = {}
    for track, roll in tracks_out.items():
        by_date = roll.get("by_date") or []
        before[track] = _rollup_by_date_window(by_date, end=POST_FIX_START)
        after[track] = _rollup_by_date_window(by_date, start=POST_FIX_START)
    saves = "2026-09-15"
    season = "2026-09-20"
    alt_cuts = {
        "saves_own_d": {
            "cut": saves,
            "before": {
                t: _rollup_by_date_window(tracks_out[t].get("by_date") or [], end=saves)
                for t in TRACK_ORDER
                if t in tracks_out
            },
            "after": {
                t: _rollup_by_date_window(tracks_out[t].get("by_date") or [], start=saves)
                for t in TRACK_ORDER
                if t in tracks_out
            },
        },
        "season_boundary": {
            "cut": season,
            "before": {
                t: _rollup_by_date_window(tracks_out[t].get("by_date") or [], end=season)
                for t in TRACK_ORDER
                if t in tracks_out
            },
            "after": {
                t: _rollup_by_date_window(tracks_out[t].get("by_date") or [], start=season)
                for t in TRACK_ORDER
                if t in tracks_out
            },
        },
    }
    return {
        "post_fix_start": POST_FIX_START,
        "post_fix_note": (
            "First archive day on/after exposure+anchor (2026-09-24). "
            "Saves own-D 2026-09-15; season-boundary 2026-09-20. "
            "actuals merge is measurement-only."
        ),
        "fixes": INFRA_FIXES,
        "before": before,
        "after": after,
        "alt_cuts": alt_cuts,
    }


def grade_day(
    date_s: str,
    path: Path,
    *,
    append_history: bool,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    payload = payload_from_g70(path, date_s)
    n_tickets = sum(len(g.get("tickets") or []) for g in payload.get("groups") or [])
    meta = {
        "date": date_s,
        "source": path.name,
        "n_tickets_in_archive": n_tickets,
        "by_family_in_archive": {},
        "graded_merge_dates": [],
    }
    fam_counts: dict[str, int] = defaultdict(int)
    for g in payload.get("groups") or []:
        gname = str(g.get("group_name") or "")
        for t in g.get("tickets") or []:
            tr = efficiency_track(t, gname) or "unclassified"
            fam_counts[tr] += 1
    meta["by_family_in_archive"] = dict(fam_counts)

    if n_tickets == 0:
        return [], meta

    tennis_match_date = te._resolve_tennis_match_date_from_payload(date_s, payload)
    # Empty step8 candidates: construction efficiency grades from actuals_*.csv
    # (and graded overlays). Avoids loading multi-MB step8 workbooks per day.
    sport_candidates: dict[str, list[Path]] = {}
    graded_merge_dates, _ = te.resolve_ticket_eval_graded_merge_dates(date_s, payload, [])
    # Always include slate date so actuals_{sport}_{date}.csv are merged.
    if date_s not in graded_merge_dates:
        graded_merge_dates = [date_s] + list(graded_merge_dates)
    meta["graded_merge_dates"] = list(graded_merge_dates)
    _ = tennis_match_date  # reserved for tennis match-day actuals overlay

    # Grade all efficiency tracks (gated + YOLO) in one pass.
    def _classify(t: dict[str, Any], gname: str) -> str | None:
        return efficiency_track(t, gname)

    recs = te._build_efficiency_grade_history_records(
        payload,
        date_s,
        sport_candidates,
        graded_merge_dates,
        tracks=frozenset(TRACK_ORDER),
        classify=_classify,
        off_card=True,
    )
    if append_history:
        for rec in recs:
            te._append_grade_history(rec)
    return recs, meta


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--from", dest="date_from", default=None, help="YYYY-MM-DD inclusive")
    ap.add_argument("--to", dest="date_to", default=None, help="YYYY-MM-DD inclusive")
    ap.add_argument(
        "--append-history",
        action="store_true",
        help="Write per-day track rows into data/grade_history.json",
    )
    ap.add_argument(
        "--out",
        default=str(OUT_JSON),
        help="Report JSON path",
    )
    args = ap.parse_args(argv)

    start = _parse_date(args.date_from) if args.date_from else None
    end = _parse_date(args.date_to) if args.date_to else None
    archives = discover_g70_archives(start=start, end=end)
    if not archives:
        print("No goblin70_tickets_*.json archives in range.")
        return 1

    all_recs: list[dict[str, Any]] = []
    day_meta: list[dict[str, Any]] = []
    for date_s, path in archives:
        print(f"\n=== {date_s} ({path.name}) ===")
        recs, meta = grade_day(date_s, path, append_history=bool(args.append_history))
        day_meta.append(meta)
        all_recs.extend(recs)
        if not recs:
            print("  (no decided gated/YOLO slips - graded/actuals missing or all pending)")

    by_track: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for r in all_recs:
        by_track[str(r.get("track"))].append(r)

    tracks_out: dict[str, Any] = {}
    for track in TRACK_ORDER:
        roll = _rollup(by_track.get(track, []))
        roll["label"] = TRACK_LABELS.get(track, track)
        roll["off_card"] = True
        tracks_out[track] = roll

    gated_rows = []
    for t in GATED_CONSTRUCTION_TRACKS:
        gated_rows.extend(by_track.get(t, []))
    gated_total = _rollup(gated_rows)
    gated_total["label"] = "All gated construction (Goblin+Standard+Mixed)"
    gated_total["off_card"] = True

    eras = _era_split(tracks_out)

    report = {
        "built_at": datetime.now().isoformat(timespec="seconds"),
        "instrument": "gated_construction_efficiency",
        "note": (
            "Keep-gated ticket construction efficiency from Goblin-70 archives. "
            "Tracks Goblin-only, Standard-only, and Mixed separately (plus YOLO "
            "side tracks). Not the ungated book catalog — only slips that "
            "cleared L5=5 + sport keep gates. Off Income card win-rate. "
            "The full-window blend spans infra fixes; use eras.before/after "
            f"(cut {POST_FIX_START}) before reading Mixed/Standard."
        ),
        "window": {
            "from": archives[0][0],
            "to": archives[-1][0],
            "n_archive_days": len(archives),
        },
        "tracks": tracks_out,
        "gated_total": gated_total,
        "eras": eras,
        "days": day_meta,
        "grade_history_appended": bool(args.append_history),
        "grade_history_path": str(persistent_data_dir(REPO) / "grade_history.json"),
    }

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(report, indent=2, ensure_ascii=False) + "\n"
    out.write_text(text, encoding="utf-8")
    OUT_LATEST.write_text(text, encoding="utf-8")

    print("\n=== Gated construction efficiency ===")
    print(f"Window {report['window']['from']} -> {report['window']['to']}  ({len(archives)} archive days)")
    for track in TRACK_ORDER:
        b = tracks_out[track]
        wr = f"{100 * b['win_rate']:.1f}%" if b["win_rate"] is not None else "—"
        roi = f"{b['roi_pct']:+.1f}%" if b["roi_pct"] is not None else "—"
        print(
            f"  {track:18s}  slips={b['slips']:3d}  decided={b['decided']:3d}  "
            f"W/L={b['wins']}/{b['losses']}  wr={wr:>7s}  ROI@10={roi:>8s}"
        )
    gt = gated_total
    wr = f"{100 * gt['win_rate']:.1f}%" if gt["win_rate"] is not None else "—"
    roi = f"{gt['roi_pct']:+.1f}%" if gt["roi_pct"] is not None else "—"
    print(
        f"  {'GATED TOTAL':18s}  slips={gt['slips']:3d}  decided={gt['decided']:3d}  "
        f"W/L={gt['wins']}/{gt['losses']}  wr={wr:>7s}  ROI@10={roi:>8s}"
    )
    print(f"\n=== Eras (cut {POST_FIX_START} = post exposure/anchor) ===")
    for label, block in (("BEFORE", eras["before"]), ("AFTER", eras["after"])):
        print(f"  {label}")
        for track in TRACK_ORDER:
            b = block.get(track) or {}
            wr_e = f"{100 * b['win_rate']:.1f}%" if b.get("win_rate") is not None else "—"
            roi_e = f"{b['roi_pct']:+.1f}%" if b.get("roi_pct") is not None else "—"
            print(
                f"    {track:18s}  decided={b.get('decided', 0):3d}  "
                f"W/L={b.get('wins', 0)}/{b.get('losses', 0)}  "
                f"wr={wr_e:>7s}  ROI@10={roi_e:>8s}"
            )
    print(f"\nWrote {out}")
    print(f"Wrote {OUT_LATEST}")
    eras_path = REPORTS / "gated_construction_eras.json"
    eras_path.write_text(json.dumps(eras, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {eras_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
