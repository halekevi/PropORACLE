"""Listed hit rates with shadow prop cells held out (still tracked, not killed)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

OPEN = Path(r"H:\PropORACLE")
sys.path.insert(0, str(OPEN / "scripts"))
from _prop_category_gates import (  # noqa: E402
    B,
    BOOKS,
    GOBLIN_FLOOR,
    OPEN as _OPEN,
    build_merged,
    overall,
)

SHADOW_CELLS = (
    ("Tennis", "Goblin OVER", "aces", "tennis goblin aces (likely 0 actuals)"),
    ("Tennis", "Goblin OVER", "double_faults", "tennis goblin DF (likely 0 actuals)"),
    ("Tennis", "Standard UNDER", "games_won", "tennis std under games won"),
    ("Soccer", "Goblin OVER", "shots on target", "soccer goblin SOG"),
    ("Soccer", "Goblin OVER", "sog", "soccer goblin SOG"),
    ("Soccer", "Standard UNDER", "shots", "soccer std under shots"),
    ("Soccer", "Standard UNDER", "shots on target", "soccer std under SOG"),
    ("Soccer", "Standard UNDER", "sog", "soccer std under SOG"),
    ("MLB", "Goblin OVER", "singles", "mlb goblin singles"),
    ("MLB", "Standard UNDER", "total_bases", "mlb std under total bases"),
    ("WNBA", "Standard OVER", "points", "wnba std over raw points (not combos)"),
)

SHADOW_KEYS = {(s, b, p) for s, b, p, _ in SHADOW_CELLS}
SHADOW_REASON = {(s, b, p): why for s, b, p, why in SHADOW_CELLS}


def soccer_family(prop: str) -> str:
    p = (prop or "").lower()
    if "save" in p or "goalie" in p:
        return "saves"
    if "shot on" in p or p in {"sog", "shots on goal", "shots on target"}:
        return "sog"
    if "shot" in p:
        return "shots"
    return p


def shadow_reason(sport: str, book: str, prop: str) -> str | None:
    if (sport, book, prop) in SHADOW_REASON:
        return SHADOW_REASON[(sport, book, prop)]
    fam = soccer_family(prop)
    if sport == "Soccer" and book == "Goblin OVER" and fam == "sog":
        return SHADOW_REASON.get(("Soccer", "Goblin OVER", "sog")) or "soccer goblin SOG"
    if sport == "Soccer" and book == "Standard UNDER" and fam in ("shots", "sog"):
        return "soccer std under shots/SOG"
    return None


def goblin_floor_ok(row) -> bool:
    """Sport cover floor, except tennis games_won (floor hurts)."""
    if row["book"] != "Goblin OVER":
        return True
    if row["sport"] == "Tennis" and row["prop"] == "games_won":
        return True
    need = GOBLIN_FLOOR.get(row["sport"], 1.0)
    gap = row["min_gap"]
    return gap == gap and gap >= need


def fmt(rec: dict) -> str:
    if not rec or rec.get("n", 0) == 0:
        return "—"
    return f"{rec['rate']}%  {rec['hits']}/{rec['n']}"


def main():
    merged = build_merged()
    merged["shadow_why"] = [
        shadow_reason(s, b, p) for s, b, p in zip(merged["sport"], merged["book"], merged["prop"])
    ]
    merged["is_shadow"] = merged["shadow_why"].notna()
    listed = merged[merged["list_l5"] & merged["book"].isin(BOOKS)].copy()
    listed["floor_ok"] = listed.apply(goblin_floor_ok, axis=1)
    hot = listed[~listed["is_shadow"]]
    shadow = listed[listed["is_shadow"]]
    # Goblin floor only on Goblin OVER; Standard stays L5. Tennis games_won skips floor.
    hot_floor = listed[~listed["is_shadow"] & listed["floor_ok"]]

    out_cells = []
    for (sport, book, prop), g in shadow.groupby(["sport", "book", "prop"]):
        out_cells.append(
            {
                "sport": sport,
                "book": book,
                "prop": prop,
                "why": shadow_reason(sport, book, prop),
                "listed": overall(g),
            }
        )
    out_cells.sort(key=lambda r: (r["sport"], r["book"], r["prop"]))

    by_book = {}
    for book in BOOKS:
        b_all = listed[listed["book"] == book]
        b_hot = hot[hot["book"] == book]
        b_sh = shadow[shadow["book"] == book]
        b_fl = hot_floor[hot_floor["book"] == book]
        by_book[book] = {
            "listed": overall(b_all),
            "shadow": overall(b_sh),
            "minus_shadow": overall(b_hot),
            "minus_shadow_goblin_floor": overall(b_fl),
        }

    by_sport_book = {}
    for sport in B.SPORTS:
        by_sport_book[sport] = {}
        for book in BOOKS:
            b_all = listed[(listed["sport"] == sport) & (listed["book"] == book)]
            b_hot = hot[(hot["sport"] == sport) & (hot["book"] == book)]
            b_sh = shadow[(shadow["sport"] == sport) & (shadow["book"] == book)]
            by_sport_book[sport][book] = {
                "listed": overall(b_all),
                "shadow": overall(b_sh),
                "minus_shadow": overall(b_hot),
            }

    report = {
        "window": {
            "first": str(merged["date"].min()),
            "last": str(merged["date"].max()),
            "n_joined": int(len(merged)),
        },
        "note": "Shadow cells stay on the board for tracking; minus_shadow is the hot rate if they are held out of the live list only.",
        "combined_listed": {
            "listed": overall(listed),
            "shadow": overall(shadow),
            "minus_shadow": overall(hot),
            "minus_shadow_plus_goblin_floor": overall(hot_floor),
        },
        "by_book": by_book,
        "by_sport_book": by_sport_book,
        "shadow_cells": out_cells,
    }
    dest = _OPEN / "data" / "reports" / "hot_minus_shadow.json"
    dest.write_text(json.dumps(report, indent=2), encoding="utf-8")

    print("WINDOW", report["window"])
    print()
    print("COMBINED L5>=4 list")
    c = report["combined_listed"]
    print("  listed        ", fmt(c["listed"]))
    print("  shadow only   ", fmt(c["shadow"]))
    print("  minus shadow  ", fmt(c["minus_shadow"]))
    print("  minus + goblin floor (games_won exception)", fmt(c["minus_shadow_plus_goblin_floor"]))
    print()
    print("BY BOOK")
    for book in BOOKS:
        r = by_book[book]
        print(f"  {book}")
        print(f"    listed       {fmt(r['listed'])}")
        print(f"    shadow       {fmt(r['shadow'])}")
        print(f"    minus shadow {fmt(r['minus_shadow'])}")
        if book == "Goblin OVER":
            print(f"    minus + floor {fmt(r['minus_shadow_goblin_floor'])}")
    print()
    print("BY SPORT x BOOK  listed -> minus shadow  (shadow n)")
    for sport in B.SPORTS:
        print(f"  {sport}")
        for book in BOOKS:
            r = by_sport_book[sport][book]
            shn = r["shadow"]["n"]
            print(
                f"    {book:16} {fmt(r['listed']):22} -> {fmt(r['minus_shadow']):22}  "
                f"shadow {fmt(r['shadow']) if shn else 'n=0'}"
            )
    print()
    print("SHADOW CELLS (listed L5>=4)")
    for r in out_cells:
        print(f"  {r['sport']:7} {r['book']:16} {r['prop']:18} {fmt(r['listed'])}  {r['why']}")
    print("wrote", dest)


if __name__ == "__main__":
    main()
