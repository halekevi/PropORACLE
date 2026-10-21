#!/usr/bin/env python3
"""Grade 2026-09-23 G70 pool + submitted slips vs graded_props."""
from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path

G70 = Path(r"H:\PropORACLE_main_cp\data\reports\goblin70_tickets_2026-09-23.json")
GRADED = Path(r"H:\PropORACLE_main_cp\ui_runner\templates\graded_props_2026-09-23.json")


def norm_name(s: str) -> str:
    s = str(s or "").casefold()
    s = s.replace("é", "e").replace("ñ", "n").replace("ü", "u")
    s = re.sub(r"[^a-z0-9]+", "", s)
    return s


def norm_prop(s: str) -> str:
    s = re.sub(r"[^a-z0-9]+", "", str(s or "").casefold())
    aliases = {
        "ks": "pitcherstrikeouts",
        "strikeouts": "pitcherstrikeouts",
        "pitcherstrikeouts": "pitcherstrikeouts",
        "ha": "hitsallowed",
        "hitsallowed": "hitsallowed",
        "bb": "walksallowed",
        "walksallowed": "walksallowed",
        "walks": "walksallowed",
        "po": "pitchingouts",
        "pitchingouts": "pitchingouts",
        "rebounds": "rebounds",
        "reb": "rebounds",
        "ptsasts": "ptsasts",
        "pointsassists": "ptsasts",
        "rebsasts": "rebsasts",
        "totalgames": "totalgames",
        "points": "points",
        "earnedrunsallowed": "earnedruns",
        "earnedruns": "earnedruns",
        "era": "earnedruns",
        "totalbases": "totalbases",
        "tb": "totalbases",
    }
    return aliases.get(s, s)


def line_eq(a, b) -> bool:
    try:
        return abs(float(a) - float(b)) < 1e-9
    except (TypeError, ValueError):
        return False


def main() -> None:
    g70 = json.loads(G70.read_text(encoding="utf-8"))
    pool = g70.get("goblin_pool") or []
    graded = json.loads(GRADED.read_text(encoding="utf-8"))
    props = graded.get("props") or []

    idx = defaultdict(list)
    for p in props:
        if str(p.get("pick_type") or "").lower() != "goblin":
            continue
        if str(p.get("direction") or "").upper() != "OVER":
            continue
        idx[(norm_name(p.get("player")), norm_prop(p.get("prop")))].append(p)

    def match_row(player, prop, line):
        cands = idx.get((norm_name(player), norm_prop(prop))) or []
        exact = [c for c in cands if line_eq(c.get("line"), line)]
        return exact[0] if exact else (cands[0] if len(cands) == 1 else None), cands

    print("=== submitted slips ===")
    slips = [
        ("T1", [
            ("George Kirby", "Hits Allowed", 2.5),
            ("Chris Sale", "Pitcher Strikeouts", 4.5),
            ("Dominique Malonga", "Rebounds", 5.5),
        ]),
        ("T2", [
            ("Paige Bueckers", "Pts+Asts", 19.5),
            ("Merrill Kelly", "Walks Allowed", 0.5),
            ("Jeffrey Springs", "Walks Allowed", 0.5),
        ]),
    ]
    for tid, legs in slips:
        print(f"\n{tid}")
        hits = misses = voids = 0
        for player, prop, line in legs:
            row, cands = match_row(player, prop, line)
            if row is None:
                print(f"  {player} {prop} {line}: NO GRADE n_cands={len(cands)}")
                continue
            res = str(row.get("result") or "").upper()
            act = row.get("actual_value")
            gl = row.get("line")
            print(f"  {player} {row.get('prop')} {gl}: {res} actual={act}")
            if res == "HIT":
                hits += 1
            elif res in {"VOID", "PUSH"}:
                voids += 1
            else:
                misses += 1
        print(f"  legs HIT {hits} MISS {misses} VOID {voids}")

    print("\n=== G70 pool 9/23 vs graded goblin OVER ===")
    by_prop = defaultdict(lambda: [0, 0, 0, 0])  # hit miss void unmatched
    rows_out = []
    for r in pool:
        player, prop, line = r.get("player"), r.get("prop"), r.get("line")
        row, cands = match_row(player, prop, line)
        key = str(prop or "")
        if row is None:
            by_prop[key][3] += 1
            rows_out.append((player, prop, line, "NO_GRADE", None, r.get("l5"), r.get("sport")))
            continue
        res = str(row.get("result") or "").upper()
        act = row.get("actual_value")
        rows_out.append((player, prop, row.get("line"), res, act, r.get("l5"), r.get("sport")))
        if res == "HIT":
            by_prop[key][0] += 1
        elif res in {"VOID", "PUSH"}:
            by_prop[key][2] += 1
        else:
            by_prop[key][1] += 1

    h = m = v = u = 0
    for prop, (hh, mm, vv, uu) in sorted(by_prop.items()):
        n = hh + mm
        rate = (100.0 * hh / n) if n else None
        print(f"  {prop}: {hh}/{n} = {rate:.1f}%" if rate is not None else f"  {prop}: {hh} hit {mm} miss {vv} void {uu} unmatched")
        if rate is not None:
            print(f"     (void {vv} unmatched {uu})")
        h += hh
        m += mm
        v += vv
        u += uu
    n = h + m
    print(f"\nPOOL GRADED {h}/{n} = {100*h/n:.1f}%  void={v} unmatched={u}")

    print("\n=== WNBA Points OVER goblin on 9/23 graded board ===")
    wnba_pts = [
        p
        for p in props
        if str(p.get("sport") or "").upper() == "WNBA"
        and norm_prop(p.get("prop")) == "points"
        and str(p.get("pick_type") or "").lower() == "goblin"
        and str(p.get("direction") or "").upper() == "OVER"
    ]
    # G70-like: l5=5 if present
    def l5_of(p):
        try:
            return float(p.get("l5_over") or 0)
        except (TypeError, ValueError):
            return 0.0
    g70ish = [p for p in wnba_pts if l5_of(p) >= 5]
    for label, group in [("all goblin Points OVER", wnba_pts), ("l5_over>=5", g70ish)]:
        hh = sum(1 for p in group if str(p.get("result") or "").upper() == "HIT")
        mm = sum(1 for p in group if str(p.get("result") or "").upper() not in {"HIT", "VOID", "PUSH", ""})
        n = hh + mm
        print(f"  {label}: {hh}/{n} = {100*hh/n:.1f}%" if n else f"  {label}: n=0")

    print("\n=== pool legs ===")
    for row in rows_out:
        print(f"  {row[6]} {row[0]} {row[1]} {row[2]} L5={row[5]} {row[3]} actual={row[4]}")


if __name__ == "__main__":
    main()
