#!/usr/bin/env python3
"""Compare gated-ticket joint hit rates to graded results, and quote ROI to scrape ROI.

The +9.8% Goblin ROI is the gated-construction rollup. This script grades the
same Goblin-70 archives and does not write grade_history.
"""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import build_ticket_eval as te  # noqa: E402
from build_goblin70_tickets import fold_name  # noqa: E402
from report_gated_construction_efficiency import (  # noqa: E402
    discover_g70_archives,
    payload_from_g70,
)
from utils.category_hit_rate import load_category_hr_index, lookup_category_hr  # noqa: E402
from utils.gated_construction_tracks import GATED_GOBLIN, construction_track  # noqa: E402
from utils.player_prob_shrinkage import shrink_prob_toward_baseline  # noqa: E402
from utils.ticket_70_pool import P_GOBLIN_L5EQ5  # noqa: E402
from utils.ticket_ev_build import GATE_SHRINK_K, _parent_hr, attach_leg_p  # noqa: E402
from utils.ticket_keep_registry import rule_for  # noqa: E402

REPORTS = ROOT / "data" / "reports"
BINS = (0.40, 0.50, 0.60, 0.70, 0.80)


def _f(raw: object) -> float | None:
    try:
        v = float(raw)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return v


def _bin_label(p: float) -> str:
    lo = 0.0
    for edge in BINS:
        if p < edge:
            return f"{lo:.2f}-{edge:.2f}"
        lo = edge
    return f"{BINS[-1]:.2f}+"


def _shrunk_p(leg: dict, index: dict) -> float | None:
    row = dict(leg)
    recipe = str(leg.get("gate_recipe") or "").strip()
    if not recipe:
        rule = rule_for(leg.get("sport"))
        mode = rule[0] if rule else "off"
        recipe = f"{leg.get('sport')}:{mode}"
    attach_leg_p(row, recipe=recipe, index=index)
    return _f(row.get("p"))


def _stored_p(leg: dict) -> float | None:
    return _f(leg.get("p"))


def _cell_record(leg: dict, index: dict, grade: str) -> dict:
    side = leg.get("side") or leg.get("direction")
    prop = leg.get("prop") or leg.get("prop_type")
    child = lookup_category_hr(
        index,
        sport=leg.get("sport"),
        prop=prop,
        pick_type=leg.get("pick_type"),
        direction=side,
    )
    parent = _parent_hr(
        index,
        sport=leg.get("sport"),
        pick_type=leg.get("pick_type"),
        direction=side,
    )
    raw = float(child["hr"]) if child else None
    cell_n = float(child["n"]) if child else None
    if child and parent:
        shrunk = shrink_prob_toward_baseline(
            float(child["hr"]), float(child["n"]), parent[0], GATE_SHRINK_K
        )
    elif child:
        shrunk = raw
    elif parent:
        shrunk = float(parent[0])
    else:
        shrunk = None
    stored = _stored_p(leg)
    source = str(leg.get("p_source") or "")
    floor = source == "l5eq5_floor"
    if not floor and stored is not None and abs(stored - P_GOBLIN_L5EQ5) < 0.002:
        if shrunk is None or shrunk < P_GOBLIN_L5EQ5 - 0.002:
            floor = True
    return {
        "sport": str(leg.get("sport") or "").upper(),
        "prop": str(prop or ""),
        "side": str(side or "").upper(),
        "pick": str(leg.get("pick_type") or ""),
        "raw": raw,
        "n": cell_n,
        "parent": float(parent[0]) if parent else None,
        "shrunk": shrunk,
        "stored": stored,
        "source": source,
        "floor": floor,
        "grade": grade,
        "grade_raw": str(leg.get("grade_raw") or ""),
        "void_note": str(leg.get("void_note") or ""),
    }


def _product(ps: list[float | None]) -> float | None:
    if not ps or any(p is None or p <= 0 for p in ps):
        return None
    out = 1.0
    for p in ps:
        out *= float(p)
    return out


def _void_reason(leg: dict) -> str:
    raw = str(leg.get("grade_raw") or "").strip().upper()
    note = str(leg.get("void_note") or "").strip().upper()
    blob = f"{raw} {note}"
    if "POSTPON" in blob:
        return "postponed"
    if any(tok in blob for tok in ("SCRATCH", "LINEUP", "DNP", "INACTIVE")):
        return "scratch_or_lineup"
    if raw in {"NO_ACTUAL", "MISSING_ACTUAL", "NO_DATA"} or note in {"NO_ACTUAL", "MISSING_ACTUAL", "NO_DATA"}:
        return "NO_ACTUAL"
    if raw in {"", "N/A", "NA"} and not note:
        return "NO_ACTUAL"
    if note:
        return note[:40]
    if raw:
        return raw
    return "unspecified"


def _binomial_check(rows: list[dict], p: float = 0.778) -> None:
    slips = [
        row for row in rows
        if row["n_legs"] == 3 and row["voids"] == 0
        and all(leg.get("grade") in {"HIT", "MISS"} for leg in (row.get("leg_info") or []))
        and len(row.get("leg_info") or []) == 3
    ]
    print(f"\n3-leg no-void slips vs binomial p={p:.1%}: n={len(slips)}")
    counts = [0, 0, 0, 0]
    for row in slips:
        k = sum(1 for leg in row["leg_info"] if leg["grade"] == "HIT")
        counts[k] += 1
    chi = 0.0
    print(f"  {'hits':>4} {'obs':>5} {'exp':>7}")
    n = len(slips)
    for k in range(4):
        exp = n * math.comb(3, k) * (p ** k) * ((1 - p) ** (3 - k))
        obs = counts[k]
        if exp > 0:
            chi += (obs - exp) ** 2 / exp
        print(f"  {k:4d} {obs:5d} {exp:7.1f}")
    print(f"  chi-square {chi:.2f} on 3 df (5% critical value is 7.81)")
    by_day: dict[str, list[dict]] = defaultdict(list)
    for row in slips:
        by_day[row["date"]].append(row)
    print(f"  {'date':<12} {'slips':>5} {'hits':>5} {'legs':>5} {'expected':>8}")
    day_chi = 0.0
    for date in sorted(by_day):
        chunk = by_day[date]
        hits = sum(1 for row in chunk for leg in row["leg_info"] if leg["grade"] == "HIT")
        legs_n = 3 * len(chunk)
        exp = legs_n * p
        var = legs_n * p * (1 - p)
        if var > 0:
            day_chi += (hits - exp) ** 2 / var
        print(f"  {date:<12} {len(chunk):5d} {hits:5d} {legs_n:5d} {exp:8.1f}")
    print(f"  day-level chi-square {day_chi:.2f} on {len(by_day)} days")


def _void_reasons(rows: list[dict]) -> None:
    print(f"\nVoid reasons on {len(rows)} Goblin slips with a void")
    slip_reason: dict[str, int] = defaultdict(int)
    leg_reason: dict[str, int] = defaultdict(int)
    for row in rows:
        reasons = [_void_reason(leg) for leg in row.get("leg_info") or [] if leg.get("grade") == "VOID"]
        for reason in reasons:
            leg_reason[reason] += 1
        slip_reason[reasons[0] if reasons else "unspecified"] += 1
    print("  slips")
    for reason, n in sorted(slip_reason.items(), key=lambda kv: -kv[1]):
        print(f"    {reason}: {n}")
    print("  void legs")
    for reason, n in sorted(leg_reason.items(), key=lambda kv: -kv[1]):
        print(f"    {reason}: {n}")


def _fit_gate_forward(forward_legs: list[dict]) -> None:
    """Gate-cell hit rate on graded props before Aug 26, scored on the later slips."""
    from utils.pp_vegas_gap import is_grading_suspect
    from utils.ticket_70_pool import canon_prop, norm_sport, ticket_gate_passes

    cut = "2026-08-26"
    templates = ROOT / "ui_runner" / "templates"
    gate: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])
    plain: dict[tuple, list[int]] = defaultdict(lambda: [0, 0])
    files = 0
    for path in sorted(templates.glob("graded_props_*.json")):
        date = path.stem.replace("graded_props_", "")[:10]
        if len(date) != 10 or date >= cut:
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        props = payload.get("props") if isinstance(payload, dict) else None
        if not isinstance(props, list):
            continue
        files += 1
        for raw in props:
            if not isinstance(raw, dict) or is_grading_suspect(raw):
                continue
            res = str(raw.get("result") or "").strip().upper()
            if res not in {"HIT", "MISS"}:
                continue
            row = dict(raw)
            if not row.get("side") and row.get("direction"):
                row["side"] = row["direction"]
            sport = norm_sport(str(row.get("sport") or ""))
            side = str(row.get("side") or "").strip().upper()
            kind = str(row.get("pick_type") or "").lower()
            if "goblin" in kind:
                pick = "Goblin"
            elif "standard" in kind or kind == "std":
                pick = "Standard"
            else:
                continue
            prop = canon_prop(sport, str(row.get("prop") or ""))
            if not sport or not prop or side not in {"OVER", "UNDER"}:
                continue
            key = (sport, prop, side, pick)
            hit = 1 if res == "HIT" else 0
            plain[key][0] += hit
            plain[key][1] += 1
            try:
                ok = ticket_gate_passes(row)
            except Exception:
                ok = False
            if ok:
                gate[key][0] += hit
                gate[key][1] += 1
    print(f"\nGate cells fit on graded props before {cut} ({files} files)")
    used = []
    missing = 0
    for leg in forward_legs:
        if leg.get("grade") not in {"HIT", "MISS"}:
            continue
        sport = norm_sport(leg.get("sport") or "")
        prop = canon_prop(sport, leg.get("prop") or "")
        side = str(leg.get("side") or "").upper()
        kind = str(leg.get("pick") or "")
        key = (sport, prop, side, kind)
        cell = gate.get(key)
        if not cell or cell[1] < 1:
            missing += 1
            continue
        used.append((key, cell[0] / cell[1], 1 if leg["grade"] == "HIT" else 0, plain.get(key, [0, 0])))
    if not used:
        print("  no forward legs matched a pre-period gate cell")
        return
    pred = sum(p for _, p, _, _ in used) / len(used)
    got = sum(h for _, _, h, _ in used) / len(used)
    print(
        f"  forward legs with a pre-Aug 26 gate cell: {len(used)}  "
        f"missing={missing}"
    )
    print(f"  mean pre-period gate rate on those legs: {pred:.1%}")
    print(f"  forward hit rate on those legs: {got:.1%}")
    by: dict[tuple, list] = defaultdict(list)
    for key, p, h, cat in used:
        by[key].append((p, h, cat))
    print(f"  {'cell':<42} {'pre n':>6} {'gate':>7} {'category':>9} {'fwd n':>6} {'fwd':>7}")
    for key, chunk in sorted(by.items(), key=lambda kv: -len(kv[1])):
        if len(chunk) < 5:
            continue
        g_hits, g_n = gate[key]
        c_hits, c_n = chunk[0][2]
        cat_s = f"{c_hits / c_n:.1%}" if c_n else "—"
        fwd = sum(h for _, h, _ in chunk) / len(chunk)
        label = f"{key[0]} {key[3]} {key[2]} {key[1]}"[:42]
        print(
            f"  {label:<42} {g_n:6d} {g_hits / g_n:7.1%} {cat_s:>9} "
            f"{len(chunk):6d} {fwd:7.1%}"
        )


def _sig(leg: dict) -> tuple[str, float] | None:
    name = fold_name(leg.get("player"))
    line = _f(leg.get("line"))
    if not name or line is None:
        return None
    return (name, round(line, 2))


def _load_scrapes() -> dict[tuple, list[float]]:
    """(slate date, frozenset of player+line) -> Power N-correct reads."""
    out: dict[tuple, list[float]] = defaultdict(list)
    paths = sorted(REPORTS.glob("payout_capture*.json"))
    paths += sorted(REPORTS.glob("payout_pre_tip_capture*.json"))
    paths += sorted(REPORTS.glob("payout_ladder_validation_capture_*.json"))
    paths += sorted(REPORTS.glob("payout_scrape_log_*.jsonl"))
    for path in paths:
        recs: list[dict] = []
        if path.suffix == ".jsonl":
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            for line in text.splitlines():
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(rec, dict):
                    recs.append(rec)
        else:
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            slips = payload.get("slips") if isinstance(payload, dict) else None
            if isinstance(slips, list):
                recs.extend(s for s in slips if isinstance(s, dict))
        for rec in recs:
            status = str(rec.get("status") or "ok").lower()
            if status not in {"ok", "partial", ""}:
                continue
            power = _f(rec.get("power_min_x") or rec.get("power_payout_x"))
            if power is None or power <= 0:
                continue
            raw = rec.get("legs") if isinstance(rec.get("legs"), list) else rec.get("lines")
            legs = [leg for leg in (raw or []) if isinstance(leg, dict)]
            sigs = [s for s in (_sig(leg) for leg in legs) if s]
            if len(sigs) < 2 or len(sigs) != len(legs):
                continue
            date = str(rec.get("date") or rec.get("captured_at") or "")[:10]
            tid = str(rec.get("ticket_id") or "")
            if len(tid) >= 10 and tid[4] == "-" and tid[7] == "-":
                date = tid[:10]
            if len(date) != 10:
                continue
            out[(date, frozenset(sigs))].append(power)
    return out


def _roi(nets: list[float]) -> float | None:
    if not nets:
        return None
    return 100.0 * sum(nets) / (10.0 * len(nets))


def _print_buckets(title: str, rows: list[dict], key: str) -> None:
    print(f"\n{title}")
    usable = [r for r in rows if r.get(key) is not None]
    if not usable:
        print("  no rows")
        return
    pred = sum(r[key] for r in usable) / len(usable)
    wins = sum(1 for r in usable if r["win"])
    print(
        f"  n={len(usable)}  mean predicted={pred:.1%}  "
        f"realized={wins / len(usable):.1%}  gap={wins / len(usable) - pred:+.1%}"
    )
    groups: dict[str, list[dict]] = defaultdict(list)
    for row in usable:
        groups[_bin_label(row[key])].append(row)
    print(f"  {'bucket':<12} {'n':>4} {'wins':>5} {'realized':>9} {'predicted':>10}")
    for label in sorted(groups):
        chunk = groups[label]
        w = sum(1 for r in chunk if r["win"])
        mean_p = sum(r[key] for r in chunk) / len(chunk)
        print(
            f"  {label:<12} {len(chunk):4d} {w:5d} {w / len(chunk):9.1%} {mean_p:10.1%}"
        )


def main() -> int:
    index = load_category_hr_index()
    scrapes = _load_scrapes()
    archives = discover_g70_archives(start=None, end=None)
    rows: list[dict] = []
    floor_legs = gate_legs = 0
    for date_s, path in archives:
        payload = payload_from_g70(path, date_s)
        graded_merge_dates, _ = te.resolve_ticket_eval_graded_merge_dates(date_s, payload, [])
        if date_s not in graded_merge_dates:
            graded_merge_dates = [date_s] + list(graded_merge_dates)
        indices = te._load_actuals_indices(
            {},
            graded_merge_dates,
            slate_date=date_s,
            payload=payload,
        )
        injury = te._injury_dnp_by_sport(graded_merge_dates)
        for group in payload.get("groups") or []:
            gname = str(group.get("group_name") or "")
            for ticket in group.get("tickets") or []:
                if not isinstance(ticket, dict):
                    continue
                track = construction_track(ticket, gname)
                if track is None:
                    continue
                legs = [leg for leg in (ticket.get("legs") or []) if isinstance(leg, dict)]
                grades: list[str] = []
                leg_uses: list[dict] = []
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
                    if matched and matched.get("line") is not None and line is None:
                        line = matched["line"]
                    g_leg = te._leg_grade(actual, line, direction, grade_raw, void_note)
                    g_leg = te._resolve_void_pending_if_injury_dnp(
                        g_leg, use, actual, line, grade_raw, void_note, injury
                    )
                    grades.append(g_leg)
                    use["grade_raw"] = grade_raw
                    use["void_note"] = void_note
                    leg_uses.append(use)
                    src = str(leg.get("p_source") or "")
                    if src == "l5eq5_floor":
                        floor_legs += 1
                    elif src == "gate_cell":
                        gate_legs += 1
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
                n_g = sum(1 for leg in legs if "goblin" in str(leg.get("pick_type") or "").lower())
                sigs = [s for s in (_sig(leg) for leg in legs) if s]
                scrape_hits = scrapes.get((date_s, frozenset(sigs)), []) if len(sigs) == len(legs) else []
                quote_x = _f(outcome.get("actual_payout")) if win else 0.0
                # The archive multiple is what the rollup paid winners. Losses pay 0.
                if not win:
                    quote_x = 0.0
                rows.append(
                    {
                        "date": date_s,
                        "track": track,
                        "n_legs": len(legs),
                        "n_g": n_g,
                        "voids": sum(1 for g in grades if g == "VOID"),
                        "win": win,
                        "result": result,
                        "shrunk": _product([_shrunk_p(leg, index) for leg in legs]),
                        "stored": _product([_stored_p(leg) for leg in legs]),
                        "quote_mult": _f(ticket.get("power_payout")) or _f(
                            (ticket.get("n_correct") or {}).get(str(len(legs)))
                            or (ticket.get("n_correct") or {}).get(len(legs))
                        ),
                        "paid_mult": _f(outcome.get("actual_payout")),
                        "net_10": _f(outcome.get("net_10")) or 0.0,
                        "scrape": scrape_hits[-1] if scrape_hits else None,
                        "scrape_n": len(scrape_hits),
                        "payout_source": str(
                            ticket.get("payout_source")
                            or (ticket.get("payout") or {}).get("payout_source")
                            or ""
                        ),
                        "captured_at": (
                            ticket.get("captured_at")
                            or (ticket.get("payout") or {}).get("captured_at")
                        ),
                        "leg_info": [_cell_record(use, index, g) for use, g in zip(leg_uses, grades)],
                    }
                )
        print(f"graded {date_s}", flush=True)

    print(f"\nleg p_source on graded tickets: floor={floor_legs} gate_cell={gate_legs}")
    gob = [r for r in rows if r["track"] == GATED_GOBLIN]
    clean = [r for r in rows if r["voids"] == 0]
    gob_clean = [r for r in gob if r["voids"] == 0]
    g2 = [r for r in gob_clean if r["n_legs"] == 2 and r["n_g"] == 2]
    print(f"gated decided={len(rows)}  goblin={len(gob)}  no-void={len(clean)}")
    gob_wins = sum(1 for r in gob if r["win"])
    print(
        f"goblin win rate {gob_wins}/{len(gob)} = "
        f"{(gob_wins / len(gob) if gob else 0):.1%}  "
        f"quote ROI={_roi([r['net_10'] for r in gob]):+.2f}%"
    )
    _print_buckets("All gated, no voids, shrunk joint (k=25, current index, no 0.763 floor)", clean, "shrunk")
    _print_buckets("All gated, no voids, builder stored joint (includes 0.763 floor)", clean, "stored")
    _print_buckets("Goblin-only, no voids, shrunk joint", gob_clean, "shrunk")
    _print_buckets("Goblin-only, no voids, builder stored joint", gob_clean, "stored")
    _print_buckets("2-leg Goblin, no voids, shrunk joint", g2, "shrunk")
    _print_buckets("2-leg Goblin, no voids, builder stored joint", g2, "stored")

    sources: dict[str, int] = defaultdict(int)
    for r in gob:
        sources[r["payout_source"] or "(blank)"] += 1
    print("\nGoblin payout_source")
    for key, n in sorted(sources.items(), key=lambda kv: -kv[1]):
        print(f"  {key}: {n}")

    from utils.scraped_quote import is_real_scraped_quote, log_real_quote_coverage

    # Fail-closed: external scrape hit is not enough — ticket must also carry
    # a scraped payout_source + non-null captured_at (same rule as promotion/P&L).
    log_real_quote_coverage(gob_clean, label="gated_goblin_novoid")
    matched = [
        r
        for r in gob
        if r["scrape"] is not None
        and r["voids"] == 0
        and is_real_scraped_quote(r.get("payout_source"), r.get("captured_at"))
    ]
    print(
        f"\nGoblin no-void slips with a real scraped quote "
        f"(source+captured_at): {len(matched)} / {len(gob_clean)}"
    )
    if matched:
        quote_nets = []
        scrape_nets = []
        for r in matched:
            q = r["quote_mult"]
            s = r["scrape"]
            if r["win"]:
                quote_nets.append(10.0 * (float(q) - 1.0) if q else r["net_10"])
                scrape_nets.append(10.0 * (float(s) - 1.0))
            else:
                quote_nets.append(-10.0)
                scrape_nets.append(-10.0)
        wins = sum(1 for r in matched if r["win"])
        print(f"  wins {wins}/{len(matched)}")
        print(f"  quote ROI on these slips {_roi(quote_nets):+.2f}%")
        print(f"  scrape ROI on these slips {_roi(scrape_nets):+.2f}%")
        gaps = []
        for r in matched:
            if r["win"] and r["quote_mult"] and r["scrape"]:
                gaps.append(float(r["quote_mult"]) - float(r["scrape"]))
        if gaps:
            print(
                f"  winner quote minus scrape: n={len(gaps)} "
                f"mean={sum(gaps) / len(gaps):+.2f}x"
            )
    legs = [leg for r in clean for leg in r.get("leg_info") or []]
    print(f"\nNo-void legs={len(legs)} slips={len(clean)}")
    sports: dict[str, int] = defaultdict(int)
    for leg in legs:
        sports[leg["sport"] or "?"] += 1
    print("  sports " + ", ".join(f"{k}={v}" for k, v in sorted(sports.items(), key=lambda kv: -kv[1])))
    have_raw = [leg for leg in legs if leg["raw"] is not None]
    missing = len(legs) - len(have_raw)
    floor_n = sum(1 for leg in legs if leg["floor"])
    print(f"  raw cell present={len(have_raw)} missing={missing} floor_fired={floor_n}")
    if have_raw:
        raws = sorted(leg["raw"] for leg in have_raw)
        ns = sorted(leg["n"] or 0 for leg in have_raw)
        print(
            f"  raw hr median={raws[len(raws)//2]:.1%} "
            f"mean={sum(raws)/len(raws):.1%} "
            f"cell n median={ns[len(ns)//2]:.0f}"
        )
        book = sum(1 for x in raws if x < 0.72)
        mid = sum(1 for x in raws if 0.72 <= x < 0.76)
        gate = sum(1 for x in raws if x >= 0.76)
        print(f"  raw <72%={book}  72-76%={mid}  >=76%={gate}")
    by_cell: dict[tuple, list] = defaultdict(list)
    for leg in legs:
        by_cell[(leg["sport"], leg["prop"], leg["side"], leg["pick"])].append(leg)
    print(f"  {'cell':<42} {'legs':>4} {'raw':>7} {'n':>7} {'floor':>5} {'hit':>6}")
    for key, chunk in sorted(by_cell.items(), key=lambda kv: -len(kv[1])):
        raws = [c["raw"] for c in chunk if c["raw"] is not None]
        decided = [c for c in chunk if c["grade"] in {"HIT", "MISS"}]
        hits = sum(1 for c in decided if c["grade"] == "HIT")
        raw_s = f"{sum(raws)/len(raws):.1%}" if raws else "—"
        n_s = f"{chunk[0]['n']:.0f}" if chunk[0]["n"] is not None else "—"
        hit_s = f"{hits/len(decided):.0%}" if decided else "—"
        label = f"{key[0]} {key[3]} {key[2]} {key[1]}"[:42]
        print(
            f"  {label:<42} {len(chunk):4d} {raw_s:>7} {n_s:>7} "
            f"{sum(1 for c in chunk if c['floor']):5d} {hit_s:>6}"
        )

    decided_legs = [leg for leg in legs if leg["grade"] in {"HIT", "MISS"}]
    hits = sum(1 for leg in decided_legs if leg["grade"] == "HIT")
    r = hits / len(decided_legs) if decided_legs else 0.0
    indep = []
    for row in clean:
        info = [leg for leg in row.get("leg_info") or [] if leg["grade"] in {"HIT", "MISS"}]
        if info:
            indep.append(r ** len(info))
    realized = sum(1 for row in clean if row["win"]) / len(clean) if clean else 0.0
    mean_indep = sum(indep) / len(indep) if indep else 0.0
    print(
        f"\nPer-leg hit rate on no-void slips: {hits}/{len(decided_legs)} = {r:.1%}"
    )
    print(f"  product of that rate across each slip, averaged: {mean_indep:.1%}")
    print(f"  realized joint: {realized:.1%}")
    three = [row for row in clean if row["n_legs"] == 3]
    if three:
        t_legs = [leg for row in three for leg in row.get("leg_info") or [] if leg["grade"] in {"HIT", "MISS"}]
        t_hits = sum(1 for leg in t_legs if leg["grade"] == "HIT")
        tr = t_hits / len(t_legs) if t_legs else 0.0
        t_real = sum(1 for row in three if row["win"]) / len(three)
        print(
            f"  3-leg only: per-leg {tr:.1%}  rate^3={tr**3:.1%}  "
            f"realized joint {t_real:.1%}  n={len(three)}"
        )

    void_rows = [row for row in gob if row["voids"] > 0]
    void_wins = [row for row in void_rows if row["win"]]
    same = 0
    scaled = 0
    print(f"\nGoblin slips with a void: {len(void_rows)}  wins among them: {len(void_wins)}")
    for row in void_wins:
        q = row.get("quote_mult")
        paid = row.get("paid_mult")
        if q and paid and abs(float(paid) - float(q)) < 0.05:
            same += 1
        elif q and paid and float(paid) < float(q) - 0.05:
            scaled += 1
    print(f"  wins paid at the archive n_correct: {same}")
    print(f"  wins paid at a lower multiple: {scaled}")
    if void_wins:
        show = void_wins[:8]
        print("  date legs voids quote paid")
        for row in show:
            print(
                f"  {row['date']} {row['n_legs']} {row['voids']} "
                f"{row.get('quote_mult')} {row.get('paid_mult')}"
            )
    _binomial_check(clean)
    _void_reasons(void_rows)
    _fit_gate_forward([leg for row in clean for leg in row.get("leg_info") or []])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
