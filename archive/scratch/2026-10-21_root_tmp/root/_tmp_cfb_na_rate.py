"""Yard-prop NO_ACTUAL rates and played-but-missing-stat voids."""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
from player_name_norm import fold_player_name  # noqa: E402

import pandas as pd

YARD = {"pass yards", "rush yards", "rec yards"}
PLAYED_PROPS = {
    "passing_yards",
    "rushing_yards",
    "receiving_yards",
    "receptions",
    "rushing_attempts",
    "pass_attempts",
    "completions",
}


def norm_prop(s: str) -> str:
    x = str(s or "").strip().lower()
    return {
        "pass yds": "pass yards",
        "passing yards": "pass yards",
        "rush yds": "rush yards",
        "rushing yards": "rush yards",
        "rec yds": "rec yards",
        "receiving yards": "rec yards",
        "reception yards": "rec yards",
    }.get(x, x)


def actuals_index(path: Path) -> dict[str, set[str]]:
    df = pd.read_csv(path)
    out: dict[str, set[str]] = {}
    for row in df.to_dict("records"):
        name = fold_player_name(row.get("player"))
        prop = str(row.get("prop_type") or "").strip().lower()
        if not name:
            continue
        out.setdefault(name, set()).add(prop)
    return out


def graded_json(date: str) -> Path:
    cands = []
    for root in (ROOT, MAIN):
        p = root / "ui_runner" / "templates" / f"graded_props_{date}.json"
        if p.is_file():
            cands.append(p)
    # Prefer the file with more CFB yard rows.
    best, best_n = None, -1
    for p in cands:
        n = 0
        raw = json.loads(p.read_text(encoding="utf-8"))
        for r in raw.get("props") or []:
            if str(r.get("sport") or "").upper() == "CFB" and norm_prop(r.get("prop") or r.get("prop_type")) in YARD:
                n += 1
        if n > best_n:
            best, best_n = p, n
    if best is None:
        raise SystemExit(f"no json {date}")
    return best


def side_of(r: dict) -> str:
    s = str(r.get("direction") or r.get("over_under") or r.get("side") or "").strip().upper()
    if s in {"O", "OVER"}:
        return "OVER"
    if s in {"U", "UNDER"}:
        return "UNDER"
    return s or "?"


def pick_of(r: dict) -> str:
    low = str(r.get("pick_type") or "").lower()
    if "goblin" in low:
        return "Goblin"
    if "demon" in low:
        return "Demon"
    return "Standard"


def load_rows(date: str) -> list[dict]:
    raw = json.loads(graded_json(date).read_text(encoding="utf-8"))
    rows = []
    for r in raw.get("props") or []:
        if str(r.get("sport") or "").upper() != "CFB":
            continue
        prop = norm_prop(r.get("prop") or r.get("prop_type"))
        if prop not in YARD:
            continue
        void = str(r.get("void_reason") or "")
        res = str(r.get("result") or "").upper()
        if "NO_ACTUAL" in void.upper() or res not in {"HIT", "MISS"}:
            res = "NO_ACTUAL"
        r = dict(r)
        r["_prop"] = prop
        r["_res"] = res
        r["_side"] = side_of(r)
        r["_pick"] = pick_of(r)
        rows.append(r)
    return rows


def classify(date: str, act_path: Path) -> None:
    rows = load_rows(date)
    idx = actuals_index(act_path)
    c = Counter(r["_res"] for r in rows)
    n = sum(c.values())
    na = c["NO_ACTUAL"]
    print(f"\n==== {date} yard props n={n} HIT={c['HIT']} MISS={c['MISS']} NO_ACTUAL={na} ({100*na/n:.1f}%) ====")
    print("  json", graded_json(date).name, "actuals", act_path.name)
    played_missing = []
    absent = []
    for r in rows:
        if r["_res"] != "NO_ACTUAL":
            continue
        props = idx.get(fold_player_name(r.get("player")), set())
        if props & PLAYED_PROPS:
            played_missing.append(r)
        else:
            absent.append(r)
    print(f"  NO_ACTUAL played-but-missing-stat {len(played_missing)}  player-absent {len(absent)}")
    lean = Counter((r["_pick"], r["_side"], r["_prop"]) for r in played_missing)
    print("  played-missing by pick/side/prop:")
    for k, v in sorted(lean.items(), key=lambda kv: -kv[1]):
        print(f"    {k[0]:9} {k[1]:5} {k[2]:12} {v}")
    over = sum(1 for r in played_missing if r["_side"] == "OVER")
    under = sum(1 for r in played_missing if r["_side"] == "UNDER")
    gob = sum(1 for r in played_missing if r["_pick"] == "Goblin" and r["_side"] == "OVER")
    print(f"  played-missing OVER {over} UNDER {under} Goblin OVER {gob}")
    # L5 on goblin overs if the grade row carries it
    l5_gob = []
    for r in played_missing:
        if not (r["_pick"] == "Goblin" and r["_side"] == "OVER"):
            continue
        l5 = r.get("l5_over")
        if l5 is None:
            l5 = r.get("last5_over")
        l5_gob.append(l5)
    if l5_gob:
        eq5 = sum(1 for x in l5_gob if x == 5 or x == 5.0)
        print(f"  Goblin OVER played-missing with L5=5: {eq5} of {len(l5_gob)}")


if __name__ == "__main__":
    classify("2026-09-05", MAIN / "outputs/2026-09-05/actuals_cfb_2026-09-05.csv")
    classify("2026-09-12", ROOT / "outputs/2026-09-12/actuals_cfb_2026-09-12.csv")
    classify("2026-09-26", ROOT / "outputs/2026-09-26/actuals_cfb_2026-09-26.csv")
    classify("2026-10-10", ROOT / "outputs/2026-10-10/actuals_cfb_2026-10-10.csv")
