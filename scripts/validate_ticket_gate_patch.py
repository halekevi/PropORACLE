#!/usr/bin/env python3
"""Post-build gate for the weak-lane / premium YOLO ticket patch.

Run after a live Goblin-70 build with WNBA on the slate, then paste the
three Check lines before merging to main.

  py -3.14 scripts/validate_ticket_gate_patch.py
  py -3.14 scripts/validate_ticket_gate_patch.py --path ui_runner/runtime/tickets_latest.json
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
DEFAULTS = (
    REPO / "ui_runner" / "templates" / "tickets_latest.json",
    REPO / "ui_runner" / "runtime" / "tickets_latest.json",
)

PREMIUM = frozenset({"fga", "reb+ast", "threes", "threes_att", "fgattempted", "rebsasts", "3ptmade", "3ptattempted"})


def _norm_prop(text: object) -> str:
    s = str(text or "").strip().lower()
    aliases = {
        "pts+rebs+asts": "pra",
        "points+rebounds+assists": "pra",
        "fg attempted": "fga",
        "fga": "fga",
        "rebs+asts": "reb+ast",
        "reb+ast": "reb+ast",
        "3-pt made": "threes",
        "3pt made": "threes",
        "threes": "threes",
        "3-pt attempted": "threes_att",
        "3pt attempted": "threes_att",
        "threes_att": "threes_att",
    }
    if s in aliases:
        return aliases[s]
    compact = re.sub(r"[^a-z0-9]+", "", s)
    compact_map = {
        "ptsrebsasts": "pra",
        "pra": "pra",
        "fgattempted": "fga",
        "fga": "fga",
        "rebsasts": "reb+ast",
        "rebast": "reb+ast",
        "3ptmade": "threes",
        "threes": "threes",
        "3ptattempted": "threes_att",
        "threesatt": "threes_att",
    }
    return compact_map.get(compact, s)


def _prop(leg: dict) -> str:
    return _norm_prop(leg.get("prop") or leg.get("prop_type"))


def _tickets(group: dict) -> list[dict]:
    return list(group.get("tickets") or group.get("slips") or [])


def _group_label(group: dict) -> str:
    return str(
        group.get("group_name")
        or group.get("label")
        or group.get("web_group_name")
        or ""
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", type=Path, default=None)
    args = ap.parse_args()
    path = args.path
    if path is None:
        path = next((p for p in DEFAULTS if p.exists()), DEFAULTS[0])
    if not path.exists():
        print(f"MISSING {path}")
        return 2

    data = json.loads(path.read_text(encoding="utf-8"))
    groups = data.get("groups") or []
    date = data.get("date") or "?"

    all_legs: list[dict] = []
    yolo_legs: list[dict] = []
    g70_legs: list[dict] = []
    for g in groups:
        label = _group_label(g)
        is_yolo = "YOLO" in label.upper()
        is_g70 = "GOBLIN-70" in label.upper() or "GOBLIN70" in label.upper().replace("-", "")
        for t in _tickets(g):
            track = str(t.get("ticket_track") or "").lower()
            if "yolo" in track:
                is_yolo = True
            if track.startswith("goblin70"):
                is_g70 = True
            for leg in t.get("legs") or []:
                all_legs.append(leg)
                if is_yolo:
                    yolo_legs.append(leg)
                if is_g70 and not is_yolo:
                    g70_legs.append(leg)

    gob = [
        l
        for l in all_legs
        if "goblin" in str(l.get("pick_type") or "").lower()
    ]
    wnba_gob = [l for l in gob if str(l.get("sport") or "").upper() == "WNBA"]
    pra = [l for l in wnba_gob if _prop(l) == "pra"]
    premium_props = {"fga", "reb+ast", "threes", "threes_att"}
    premium_yolo = [
        l
        for l in yolo_legs
        if str(l.get("sport") or "").upper() == "WNBA" and _prop(l) in premium_props
    ]
    premium_any = [
        l for l in gob if str(l.get("sport") or "").upper() == "WNBA" and _prop(l) in premium_props
    ]
    hitter_ks = [
        l
        for l in gob
        if str(l.get("sport") or "").upper() == "MLB"
        and _norm_prop(l.get("prop") or l.get("prop_type"))
        in {"hitter_ks", "hitterstrikeouts", "hitterks"}
    ]

    print(f"slate={date} path={path}")
    print(f"Check 1 WNBA PRA legs: {len(pra)} (expect ~0)")
    print(
        f"Check 2 Premium legs in YOLO: {len(premium_yolo)} "
        f"(expect >0 if WNBA on slate; also on card={len(premium_any)}, "
        f"WNBA goblin legs={len(wnba_gob)})"
    )
    print(
        f"Check 3 Total Goblin legs on card: {len(gob)} "
        f"(G70 non-YOLO={len(g70_legs)}; compare to pre-patch baseline)"
    )
    print(f"Bonus MLB hitter_ks Goblin legs: {len(hitter_ks)} (expect 0)")

    # Pass / fail hints (WNBA-absent slate → Check 2 N/A)
    c1 = len(pra) == 0
    c2 = len(premium_yolo) > 0 if wnba_gob else None
    c3 = len(gob)  # informational
    print(
        "Verdict: "
        f"C1={'PASS' if c1 else 'FAIL'} "
        f"C2={'PASS' if c2 else ('N/A (no WNBA Goblin on card)' if c2 is None else 'FAIL')} "
        f"C3=n={c3}"
    )
    if not c1 or c2 is False:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
