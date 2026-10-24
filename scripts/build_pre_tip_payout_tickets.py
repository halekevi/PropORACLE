#!/usr/bin/env python3
"""Build random PrizePicks mix tickets from games that have not started.

Goblin N-correct payouts change after tip. This writes a filtered step1 CSV
and a clickable ticket list covering S/G compositions so
validate_payout_ladder.py / collect_payout_data.py can fill the payout table.
Never submits entries.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "scripts"))

import validate_payout_ladder as vpl  # noqa: E402

def _default_sources(date_str: str) -> list[Path]:
    """Newest on-disk step1 first so live lines beat the morning 1AM/8AM snapshot."""
    roots = [
        ROOT / "outputs" / date_str,
        ROOT.parent / "PropORACLE_main_cp" / "outputs" / date_str,
    ]
    paths: list[Path] = [
        ROOT / "Sports" / "MLB" / "step1_mlb_props.csv",
        ROOT / "Sports" / "WNBA" / "step1_wnba_props.csv",
    ]
    for root in roots:
        for sport in ("mlb", "wnba", "tennis", "soccer"):
            paths.append(root / sport / f"step1_{sport}_props.csv")
    existing = [p for p in paths if p.is_file()]
    existing.sort(key=lambda p: p.stat().st_mtime, reverse=True)
    return existing


def _parse_dt(raw: str) -> datetime | None:
    s = (raw or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def _player(row: dict) -> str:
    return str(row.get("player") or row.get("player_name") or "").strip()


def filter_pre_tip_rows(
    paths: list[Path],
    *,
    now: datetime,
    buffer_min: int,
) -> tuple[list[dict[str, str]], dict[str, object]]:
    cutoff = now + timedelta(minutes=max(0, buffer_min))
    kept: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    games: dict[str, datetime] = {}
    stats = {
        "source_rows": 0,
        "pre_tip": 0,
        "too_soon_or_live": 0,
        "no_start": 0,
        "by_pick": defaultdict(int),
        "games": [],
    }
    fieldnames: list[str] = []
    for path in paths:
        if not path.is_file():
            continue
        with path.open(encoding="utf-8-sig", newline="") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames:
                for col in reader.fieldnames:
                    if col not in fieldnames:
                        fieldnames.append(col)
            for row in reader:
                stats["source_rows"] += 1
                pid = str(row.get("projection_id") or row.get("pp_projection_id") or "").strip()
                if pid and pid in seen_ids:
                    continue
                if pid:
                    seen_ids.add(pid)
                st = _parse_dt(str(row.get("start_time") or row.get("game_time") or ""))
                if st is None:
                    stats["no_start"] += 1
                    continue
                if st <= cutoff:
                    stats["too_soon_or_live"] += 1
                    continue
                if "player" not in row or not str(row.get("player") or "").strip():
                    row = dict(row)
                    row["player"] = _player(row)
                if "line" not in row or not str(row.get("line") or "").strip():
                    row = dict(row)
                    row["line"] = str(row.get("line_score") or row.get("line") or "")
                sport = str(row.get("sport") or row.get("league") or "").strip()
                if not sport:
                    name = path.name.lower() + " " + str(path.parent).lower()
                    for token, label in (
                        ("tennis", "Tennis"),
                        ("wnba", "WNBA"),
                        ("soccer", "Soccer"),
                        ("mlb", "MLB"),
                        ("nfl", "NFL"),
                    ):
                        if token in name:
                            sport = label
                            break
                if sport:
                    row = dict(row)
                    row["sport"] = sport
                pick = str(row.get("pick_type") or "").strip() or "?"
                stats["by_pick"][pick] += 1
                stats["pre_tip"] += 1
                gid = str(row.get("pp_game_id") or row.get("game_id") or st.isoformat())
                games[gid] = st
                kept.append(row)
    if "player" not in fieldnames:
        fieldnames.insert(0, "player")
    if "line" not in fieldnames:
        fieldnames.append("line")
    stats["by_pick"] = dict(stats["by_pick"])
    stats["games"] = [
        {"game_id": gid, "start_time": st.isoformat()}
        for gid, st in sorted(games.items(), key=lambda kv: kv[1])
    ]
    stats["n_games"] = len(games)
    stats["fieldnames"] = fieldnames
    return kept, stats


def pick_mix_delta_recipes(
    recipes: list[dict],
    *,
    max_cases: int,
    all_combos: bool = False,
) -> list[dict]:
    """Round-robin S/G mixes × distinct Goblin-Δ signatures.

    all_combos=True keeps every unique named (composition, Δ-sig), including
    1S+1G through 0S+6G. max_cases<=0 means no cap.
    """
    clickable = {1.0, 1.5, 2.0}

    def _sig_key(sig: str) -> tuple:
        raw = str(sig or "").strip()
        if not raw:
            return (2, 99, 99, 99, "")
        parts = [float(x) for x in raw.split("+") if x.strip()]
        n_click = sum(1 for x in parts if x in clickable)
        n_uniq = len(set(parts))
        n_half = sum(1 for x in parts if abs(x - 0.5) < 1e-9)
        # all_combos: still scrape clickable mixed first, but do not drop the rest.
        return (0, -n_click, -n_uniq, n_half, raw)

    by_comp: dict[str, list[dict]] = defaultdict(list)
    for r in recipes:
        if int(r.get("n_demon") or 0) > 0:
            continue
        if int(r.get("n_goblin") or 0) == 0:
            continue
        if all_combos and not str(r.get("goblin_delta_sig") or "").strip():
            continue
        by_comp[str(r.get("composition") or "")].append(r)

    uniq: dict[str, list[dict]] = {}
    for comp, lst in by_comp.items():
        lst.sort(key=lambda r: _sig_key(str(r.get("goblin_delta_sig") or "")))
        seen: set[str] = set()
        keep: list[dict] = []
        for rec in lst:
            sig = str(rec.get("goblin_delta_sig") or "").strip() or "_none"
            if sig in seen:
                continue
            seen.add(sig)
            keep.append(rec)
        uniq[comp] = keep

    def _n_legs(comp: str) -> int:
        return sum(int(x[0]) for x in comp.split("+")[:2] if x and x[0].isdigit())

    comps = [f"{n_s}S+{n_g}G+0D" for n_s, n_g in vpl._DISCOVER_COMPOSITIONS if n_g > 0]
    comps = [c for c in comps if uniq.get(c)]
    comps.sort(key=lambda c: ({5: 0, 4: 1, 6: 2, 3: 3, 2: 4}.get(_n_legs(c), 9), c))

    cap = int(max_cases)
    if all_combos and cap <= 0:
        cap = 10**9
    elif cap <= 0:
        cap = 20

    picked: list[dict] = []
    idx = {c: 0 for c in comps}
    while len(picked) < cap:
        added = False
        for comp in comps:
            i = idx[comp]
            cand = uniq.get(comp) or []
            if i < len(cand):
                picked.append(cand[i])
                idx[comp] = i + 1
                added = True
                if len(picked) >= cap:
                    break
        if not added:
            break
    return picked


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--source",
        action="append",
        default=[],
        help="step1 CSV (repeatable). Default: any unstarted tennis/WNBA/soccer/MLB board.",
    )
    ap.add_argument(
        "--buffer-min",
        type=int,
        default=45,
        help="Drop games starting within this many minutes (default 45).",
    )
    ap.add_argument("--max-cases", type=int, default=20)
    ap.add_argument(
        "--all-combos",
        action="store_true",
        help="Every fillable S/G mix × Goblin-Δ signature (no sample cap unless --max-cases > 0).",
    )
    ap.add_argument(
        "--tickets-out",
        default="",
        help="Ticket JSON path. Default: ui_runner/data/payout_ladder_validation_tickets.json",
    )
    ap.add_argument("--date", default=datetime.now().astimezone().strftime("%Y-%m-%d"))
    ap.add_argument(
        "--sport",
        default="",
        help="Force one board (mlb/wnba/tennis/soccer). Default: fattest unstarted S+G pool.",
    )
    args = ap.parse_args()
    if args.all_combos and "--max-cases" not in sys.argv:
        args.max_cases = 0

    sources = (
        [Path(p) for p in args.source]
        if args.source
        else [p for p in _default_sources(str(args.date)) if p.is_file()]
    )
    if not sources:
        print("[pre-tip] no step1 CSV found")
        return 1

    now = datetime.now().astimezone()
    rows, stats = filter_pre_tip_rows(sources, now=now, buffer_min=int(args.buffer_min))
    print(
        f"[pre-tip] now={now.isoformat()} buffer={args.buffer_min}m "
        f"sources={len(sources)} kept={stats['pre_tip']} "
        f"dropped_live_or_soon={stats['too_soon_or_live']} games={stats['n_games']}"
    )
    print(f"[pre-tip] pick_type={stats['by_pick']}")
    if not args.all_combos:
        for g in stats["games"]:
            print(f"  game {g['game_id']}  {g['start_time']}")
    else:
        print(f"[pre-tip] all-combos: skipping {stats['n_games']} game lines")
    if stats["pre_tip"] < 20:
        print("[pre-tip] FATAL: not enough unstarted props")
        return 1

    # One board per scrape so CDP can actually click. Pick the unstarted
    # sport with the most Standard+Goblin cards (payout math is the same).
    by_sport: dict[str, list[dict[str, str]]] = defaultdict(list)
    for r in rows:
        sp = str(r.get("sport") or "unknown").strip() or "unknown"
        by_sport[sp].append(r)

    def _sg_n(items: list[dict[str, str]]) -> int:
        return sum(
            1
            for r in items
            if "standard" in str(r.get("pick_type") or "").lower()
            or "goblin" in str(r.get("pick_type") or "").lower()
        )

    want = str(args.sport or "").strip().lower()
    if want:
        match = next((s for s in by_sport if s.lower() == want or want in s.lower()), None)
        if match is None:
            print(f"[pre-tip] FATAL: --sport {want!r} not in {list(by_sport)}")
            return 1
        primary_sport = match
    else:
        primary_sport = max(by_sport.keys(), key=lambda s: _sg_n(by_sport[s]))
    rows = by_sport[primary_sport]
    print(
        f"[pre-tip] using {primary_sport} for clickable tickets "
        f"(S+G={_sg_n(rows)}; other sports={[ (s, _sg_n(v)) for s,v in by_sport.items() if s != primary_sport ]})"
    )

    out_dir = ROOT / "data" / "reports"
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"payout_pre_tip_step1_{args.date}.csv"
    fieldnames = list(stats["fieldnames"])
    extra = [k for row in rows[:1] for k in row if k not in fieldnames]
    with csv_path.open("w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=fieldnames + extra, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)
    print(f"[pre-tip] wrote {csv_path}")

    sports = [str(r.get("sport") or "").strip() for r in rows if str(r.get("sport") or "").strip()]
    primary_sport = max(set(sports), key=sports.count) if sports else "Tennis"
    standard, goblins, demons, meta = vpl.load_pools_from_step1_csv(
        csv_path, sport=primary_sport
    )
    sport_by_player = {
        vpl.cpd._norm(r.get("player")): str(r.get("sport") or primary_sport)
        for r in rows
        if r.get("player")
    }
    for pool in (standard, goblins, demons):
        for card in pool:
            sp = sport_by_player.get(vpl.cpd._norm(card.get("player"))) or primary_sport
            card["sport"] = sp
            card["league"] = sp
    standard, goblins, demons, tip = vpl.filter_pools_to_pre_tip(
        standard, goblins, demons, {}, now=now, drop_unknown=True
    )
    print(f"[pre-tip] pools after tip filter {tip} meta={meta.get('goblin_delta_bins')}")
    if len(standard) < 4 or len(goblins) < 4:
        print("[pre-tip] FATAL: need at least 4 Standard and 4 Goblin cards")
        return 1

    recipes = vpl.build_discovery_recipes_from_board(
        standard=standard,
        goblins=goblins,
        demons=demons,
        max_cases=0,
        exhaustive=True,
        prefer_missing_live=True,
    )
    picked = pick_mix_delta_recipes(
        recipes,
        max_cases=int(args.max_cases),
        all_combos=bool(args.all_combos),
    )
    ticket_cap = 0 if args.all_combos and int(args.max_cases) <= 0 else int(args.max_cases)

    payload, plan_rows = vpl.build_live_board_tickets_payload(
        picked,
        standard=standard,
        goblins=goblins,
        demons=demons,
        date_str=str(args.date),
        max_cases=ticket_cap,
        delta_tol=0.75,
        force_alt_cycle=False,
    )
    tickets_path = (
        Path(args.tickets_out)
        if str(args.tickets_out or "").strip()
        else ROOT / "ui_runner" / "data" / "payout_ladder_validation_tickets.json"
    )
    tickets_path.parent.mkdir(parents=True, exist_ok=True)
    tickets_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")

    planned = [p for p in plan_rows if p.get("status") == "planned"]
    preview = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "cutoff_buffer_min": int(args.buffer_min),
        "n_games": stats["n_games"],
        "games": stats["games"],
        "pools": {"standard": len(standard), "goblin": len(goblins), "demon": len(demons)},
        "delta_bins": meta.get("goblin_delta_bins"),
        "n_planned": len(planned),
        "n_unbuildable": sum(1 for p in plan_rows if p.get("status") == "unbuildable"),
        "tickets": [
            {
                "composition": p.get("composition"),
                "goblin_delta_sig": p.get("goblin_delta_sig"),
                "legs": [
                    f"{lg.get('player')} {lg.get('prop')} {lg.get('pick_type')} {lg.get('line')} Δ={lg.get('line_distance')}"
                    for lg in (p.get("legs") or [])
                ],
            }
            for p in planned
        ],
        "step1_csv": str(csv_path),
        "tickets_path": str(tickets_path),
        "next": (
            "Capture N-correct from unstarted games (does not place bets): "
            "py -3.14 scripts/collect_payout_data.py --tickets "
            "ui_runner/data/payout_ladder_validation_tickets.json "
            "--gentle --allow-line-fallback "
            f"--max-cases {args.max_cases} --date {args.date}"
        ),
    }
    preview_path = out_dir / f"payout_pre_tip_tickets_{args.date}.json"
    preview_path.write_text(json.dumps(preview, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"[pre-tip] tickets -> {tickets_path} planned={len(planned)}")
    print(f"[pre-tip] preview -> {preview_path}")
    by_comp_n: dict[str, int] = defaultdict(int)
    for t in preview["tickets"]:
        by_comp_n[str(t.get("composition") or "?")] += 1
    print(f"[pre-tip] by_composition={dict(sorted(by_comp_n.items()))}")
    show = preview["tickets"] if len(preview["tickets"]) <= 40 else preview["tickets"][:24]
    for t in show:
        print(f"  {t['composition']:<10} Δ={t['goblin_delta_sig'] or '—'}  {t['legs'][0] if t['legs'] else ''}")
    if len(preview["tickets"]) > 40:
        print(f"  … {len(preview['tickets']) - 24} more (see {preview_path})")
    return 0 if planned else 1


if __name__ == "__main__":
    raise SystemExit(main())
