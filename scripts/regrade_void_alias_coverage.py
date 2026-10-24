#!/usr/bin/env python3
"""Regrade void-win slips after prop aliases, and classify missing actuals.

Does not write grade history.
"""
from __future__ import annotations

import json
import sys
import unicodedata
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import build_ticket_eval as te  # noqa: E402
import check_gated_joint_calibration as cal  # noqa: E402
from report_gated_construction_efficiency import (  # noqa: E402
    discover_g70_archives,
    payload_from_g70,
)
from utils.gated_construction_tracks import GATED_GOBLIN, construction_track  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import norm_sport  # noqa: E402


def _name_key(raw: object) -> str:
    text = unicodedata.normalize("NFKD", str(raw or ""))
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return "".join(ch for ch in text.lower() if ch.isalnum())


def _slip_key(date_s: str, legs: list[dict], ticket: dict | None = None) -> tuple:
    """Dedupe key. Prefer ticket_id / id so duplicate board rows collapse."""
    tid = ""
    if isinstance(ticket, dict):
        tid = str(ticket.get("ticket_id") or ticket.get("id") or "").strip()
    if tid:
        return (date_s, "id", tid)
    parts = []
    for leg in legs:
        parts.append(
            (
                _name_key(leg.get("player")),
                _name_key(leg.get("prop") or leg.get("prop_type")),
                str(leg.get("line")),
            )
        )
    return (date_s, "legs", tuple(sorted(parts)))


def _grade() -> tuple[list[dict], list[dict]]:
    slips: list[dict] = []
    voids: list[dict] = []
    for date_s, path in discover_g70_archives(start=None, end=None):
        payload = payload_from_g70(path, date_s)
        graded_merge_dates, _ = te.resolve_ticket_eval_graded_merge_dates(date_s, payload, [])
        if date_s not in graded_merge_dates:
            graded_merge_dates = [date_s] + list(graded_merge_dates)
        indices = te._load_actuals_indices(
            {}, graded_merge_dates, slate_date=date_s, payload=payload
        )
        injury = te._injury_dnp_by_sport(graded_merge_dates)
        for group in payload.get("groups") or []:
            gname = str(group.get("group_name") or "")
            for ticket in group.get("tickets") or []:
                if not isinstance(ticket, dict):
                    continue
                if construction_track(ticket, gname) != GATED_GOBLIN:
                    continue
                legs = [leg for leg in (ticket.get("legs") or []) if isinstance(leg, dict)]
                grades: list[str] = []
                pending: list[dict] = []
                for leg in legs:
                    use = dict(leg)
                    if not use.get("direction") and use.get("side"):
                        use["direction"] = use["side"]
                    if not use.get("prop_type") and use.get("prop"):
                        use["prop_type"] = use["prop"]
                    matched = te._match_leg_to_row_multi(use, indices)
                    try:
                        line = float(use.get("line"))
                    except (TypeError, ValueError):
                        line = None
                    direction = str(use.get("direction") or use.get("side") or "").strip().upper()
                    actual = matched["actual"] if matched else None
                    grade_raw = matched["grade_raw"] if matched else ""
                    void_note = matched.get("void_note", "") if matched else ""
                    g_leg = te._leg_grade(actual, line, direction, grade_raw, void_note)
                    g_leg = te._resolve_void_pending_if_injury_dnp(
                        g_leg, use, actual, line, grade_raw, void_note, injury
                    )
                    grades.append(g_leg)
                    rec = {
                        "date": date_s,
                        "player": str(leg.get("player") or leg.get("name") or ""),
                        "sport": str(leg.get("sport") or ""),
                        "prop": str(leg.get("prop") or leg.get("prop_type") or ""),
                        "line": line,
                        "side": direction,
                        "league": str(leg.get("league") or leg.get("competition") or ""),
                        "grade": g_leg,
                        "grade_raw": grade_raw,
                        "void_note": void_note,
                        "actual": actual,
                    }
                    if g_leg == "VOID":
                        pending.append(rec)
                if not grades or any(g == "UNGRADED" for g in grades):
                    continue
                if all(g == "VOID" for g in grades):
                    continue
                outcome = te._ticket_eval_money_outcome(gname, grades, ticket)
                if outcome.get("pending"):
                    continue
                result = str(outcome.get("result") or "")
                if result == "VOID_LOSS" and not te._ticket_pays_money(gname, grades):
                    continue
                win = bool(te._ticket_pays_money(gname, grades))
                slip = {
                    "key": _slip_key(date_s, legs, ticket),
                    "date": date_s,
                    "win": win,
                    "voids": sum(1 for g in grades if g == "VOID"),
                    "misses": sum(1 for g in grades if g == "MISS"),
                    "hits": sum(1 for g in grades if g == "HIT"),
                    "ticket_id": str(ticket.get("ticket_id") or ticket.get("id") or ""),
                    "group": gname,
                    "leg_bits": [
                        (
                            str(leg.get("player") or ""),
                            str(leg.get("prop") or leg.get("prop_type") or ""),
                            grades[i] if i < len(grades) else "",
                        )
                        for i, leg in enumerate(legs)
                    ],
                    "payout_source": str(
                        (ticket.get("payout") or {}).get("payout_source")
                        or ticket.get("payout_source")
                        or ""
                    ),
                    "legs": legs,
                }
                slips.append(slip)
                for rec in pending:
                    rec["slip_win"] = win
                    rec["slip_key"] = slip["key"]
                    voids.append(rec)
        print(f"graded {date_s} aliases={te.PROP_ALIAS_INDEX}", flush=True)
    return slips, voids


def _players_in_actuals(date_s: str) -> dict[str, set[str]]:
    """sport-slug -> name keys present in that date's actuals files."""
    out: dict[str, set[str]] = defaultdict(set)
    seen: set[str] = set()
    for d in te._actuals_csv_dirs_for_date(date_s):
        if not d.is_dir():
            continue
        for path in sorted(d.glob(f"actuals_*_{date_s}.csv")):
            key = str(path.resolve())
            if key in seen:
                continue
            seen.add(key)
            slug = path.name.split("_")[1] if path.name.startswith("actuals_") else ""
            try:
                rows = te._load_actuals_csv_rows(path)
            except Exception:
                continue
            for row in rows:
                out[slug].add(_name_key(row.get("player") or row.get("Player")))
            out[slug].add(f"__file__{path.name}")
    return out


def _sport_slug(sport: str) -> str:
    s = norm_sport(sport).lower()
    if s in {"tennis"}:
        return "tennis"
    if s in {"soccer"}:
        return "soccer"
    if s in {"cfb"}:
        return "cfb"
    if s in {"nfl"}:
        return "nfl"
    if s in {"mlb"}:
        return "mlb"
    return s


def _classify_missing(voids: list[dict]) -> None:
    na = [v for v in voids if cal._void_reason(v) == "NO_ACTUAL"]
    cache: dict[str, dict[str, set[str]]] = {}

    def players(day: str) -> dict[str, set[str]]:
        if day not in cache:
            cache[day] = _players_in_actuals(day)
        return cache[day]

    buckets: dict[str, list[dict]] = defaultdict(list)
    for v in na:
        slug = _sport_slug(v["sport"])
        day = v["date"]
        here = players(day).get(slug, set())
        files = [x for x in here if x.startswith("__file__")]
        names = here - set(files)
        key = _name_key(v["player"])
        if not files:
            reason = "no_actuals_file"
        elif key in names:
            reason = "player_in_file"
        else:
            prev = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
            nxt = (date.fromisoformat(day) + timedelta(days=1)).isoformat()
            prev_names = players(prev).get(slug, set())
            next_names = players(nxt).get(slug, set())
            prev_names = {n for n in prev_names if not n.startswith("__file__")}
            next_names = {n for n in next_names if not n.startswith("__file__")}
            last = key[-6:] if len(key) >= 6 else key
            fuzzy = any(n.endswith(last) and abs(len(n) - len(key)) <= 3 for n in names)
            if key in next_names:
                reason = "on_next_day_file"
            elif key in prev_names:
                reason = "on_prev_day_file"
            elif fuzzy:
                reason = "near_name_in_file"
            else:
                reason = "player_absent"
        v["coverage"] = reason
        buckets[reason].append(v)

    print(f"\nNO_ACTUAL legs before alias: {len(na)}")
    for reason, chunk in sorted(buckets.items(), key=lambda kv: -len(kv[1])):
        print(f"  {reason}: {len(chunk)}")
    absent = buckets.get("player_absent", []) + buckets.get("no_actuals_file", [])
    for reason in ("on_next_day_file", "on_prev_day_file", "near_name_in_file"):
        chunk = buckets.get(reason) or []
        if not chunk:
            continue
        print(f"\n  {reason}:")
        for v in sorted(chunk, key=lambda r: (r["sport"], r["date"], r["player"])):
            print(f"    {v['date']} {v['sport']} {v['player']} {v['prop']}")
    print(f"\nAbsent from that date's fetch file: {len(absent)}")
    by_sport: dict[str, int] = defaultdict(int)
    for v in absent:
        by_sport[norm_sport(v["sport"]) or v["sport"]] += 1
    print("  by sport: " + ", ".join(f"{k}={n}" for k, n in sorted(by_sport.items())))
    print("  detail:")
    for v in sorted(absent, key=lambda r: (r["sport"], r["date"], r["player"])):
        print(
            f"    {v['coverage']} {v['date']} {v['sport']} {v['player']} {v['prop']}"
            + (f" league={v['league']}" if v["league"] else "")
        )


def _rule_legs(slips: list[dict]) -> None:
    rows = []
    for s in slips:
        for leg in s["legs"]:
            sport = norm_sport(str(leg.get("sport") or ""))
            prop = canon_prop(sport, str(leg.get("prop") or leg.get("prop_type") or ""))
            if sport == "SOCCER" and prop == "tackles":
                rows.append((s["date"], "Tackles", leg.get("player"), s["ticket_id"]))
            if sport == "MLB" and prop == "hitter_ks":
                rows.append((s["date"], "Hitter Ks", leg.get("player"), s["ticket_id"]))
    print(f"\nGated legs outside today's keep list: {len(rows)}")
    for date_s, kind, player, tid in sorted(rows):
        print(f"  {date_s} {kind} {player} {tid}")


def _archive_payout_count() -> None:
    n = live = 0
    tackles = []
    for _date_s, path in discover_g70_archives(start=None, end=None):
        raw = json.loads(path.read_text(encoding="utf-8"))
        tickets = list(raw.get("tickets") or [])
        if not tickets:
            for g in raw.get("groups") or []:
                if isinstance(g, dict):
                    tickets.extend(t for t in (g.get("tickets") or []) if isinstance(t, dict))
        for t in tickets:
            n += 1
            pay = t.get("payout") if isinstance(t.get("payout"), dict) else {}
            src = str(pay.get("payout_source") or t.get("payout_source") or "").lower()
            if src == "live_cdp":
                live += 1
            for leg in t.get("legs") or []:
                if not isinstance(leg, dict):
                    continue
                sport = norm_sport(str(leg.get("sport") or ""))
                prop = canon_prop(sport, str(leg.get("prop") or leg.get("prop_type") or ""))
                if sport == "SOCCER" and prop == "tackles":
                    tackles.append(
                        (
                            _date_s,
                            str(leg.get("player") or ""),
                            str(t.get("ticket_id") or ""),
                            str(leg.get("prop") or leg.get("prop_type") or ""),
                        )
                    )
    print(f"\nArchive tickets in goblin70 files: {n}  with scraped live_cdp: {live}")
    print(f"Soccer Tackles legs on any archive ticket: {len(tackles)}")
    for row in tackles:
        print(f"  {row[0]} {row[1]} {row[3]} {row[2]}")


def main() -> int:
    _archive_payout_count()
    te.PROP_ALIAS_INDEX = False
    old_slips, old_voids = _grade()
    te.PROP_ALIAS_INDEX = True
    new_slips, _new_voids = _grade()

    def _dedupe(rows: list[dict]) -> list[dict]:
        out: list[dict] = []
        seen: set = set()
        for s in rows:
            if s["key"] in seen:
                continue
            seen.add(s["key"])
            out.append(s)
        return out

    old_slips = _dedupe(old_slips)
    new_slips = _dedupe(new_slips)
    old_by = {s["key"]: s for s in old_slips}
    new_by = {s["key"]: s for s in new_slips}
    void_wins = [s for s in old_slips if s["win"] and s["voids"] > 0]
    print(f"\nOld void-win slips: {len(void_wins)} of {len(old_slips)}")
    flipped = stayed = still_void = missing = 0
    for s in void_wins:
        nxt = new_by.get(s["key"])
        if nxt is None:
            missing += 1
            continue
        if nxt["voids"] > 0:
            still_void += 1
        if s["win"] and not nxt["win"]:
            flipped += 1
        elif nxt["win"]:
            stayed += 1
    resolved = [s for s in new_slips if s["voids"] == 0]
    unresolved = [s for s in new_slips if s["voids"] > 0]
    resolved_wins = sum(1 for s in resolved if s["win"])
    old_wins = sum(1 for s in old_slips if s["win"])
    new_wins = sum(1 for s in new_slips if s["win"])
    same = [new_by[s["key"]] for s in old_slips if s["key"] in new_by]
    same_wins = sum(1 for s in same if s["win"])
    extra = [s for s in new_slips if s["key"] not in old_by]
    extra_wins = sum(1 for s in extra if s["win"])
    print(
        f"RESOLVED_BASIS {resolved_wins}/{len(resolved)} "
        f"unresolved={len(unresolved)} scored={len(new_slips)}"
    )
    print(f"Old recorded wins {old_wins}/{len(old_slips)}")
    print(f"Same slips after alias: {same_wins}/{len(same)}")
    print(f"Slips the alias newly graded: {extra_wins}/{len(extra)} wins")
    print(f"All slips after alias (voids pay): {new_wins}/{len(new_slips)}")
    print("  named void legs after alias:")
    for s in new_slips:
        for player, prop, grade in s.get("leg_bits") or []:
            if any(tok in player for tok in ("Gea", "Osaka", "Williams", "J'Koby")):
                print(f"    {s['date']} {player} {prop} {grade} slip_win={s['win']} group={s['group']}")
    print(
        f"  of {len(void_wins)} void-wins: flipped to loss {flipped}, "
        f"still a win {stayed}, still carrying a void {still_void}, unmatched {missing}"
    )
    print("  slips that flipped:")
    for s in void_wins:
        nxt = new_by.get(s["key"])
        if nxt is None or nxt["win"]:
            continue
        names = ", ".join(
            f"{leg.get('player')} {leg.get('prop') or leg.get('prop_type')}"
            for leg in s["legs"]
        )
        print(f"    {s['date']} {names} -> misses {nxt['misses']} voids {nxt['voids']}")
    _classify_missing(old_voids)
    _rule_legs(new_slips or old_slips)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
