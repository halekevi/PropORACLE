"""Monday 2026-09-21 WNBA listed keep + G70 from graded_props."""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from rank_best_props_today import _clears_list_gate  # noqa: E402
from utils.prop_norm import canon_prop, display_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402

GP = MAIN / "ui_runner" / "templates" / "graded_props_2026-09-21.json"


def num(v):
    try:
        if v in (None, "", "nan"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def tally(rows):
    h = sum(1 for x in rows if x == "HIT")
    m = sum(1 for x in rows if x == "MISS")
    d = h + m
    if not d:
        return f"— n={len(rows)}"
    return f"{100.0 * h / d:.1f}% ({h}/{d})"


def rec_from(p: dict) -> dict:
    side = str(p.get("direction") or p.get("over_under") or "").strip().upper()
    if side not in {"OVER", "UNDER"}:
        side = ""
    pt = str(p.get("pick_type") or "").strip()
    low = pt.lower()
    if "goblin" in low:
        pt = "Goblin"
    elif "demon" in low:
        pt = "Demon"
    elif "standard" in low:
        pt = "Standard"
    r = {
        "sport": "WNBA",
        "player": p.get("player"),
        "team": p.get("team"),
        "opp_team": p.get("opp_team"),
        "prop": p.get("prop"),
        "side": side,
        "line": num(p.get("line")),
        "pick_type": pt,
        "l5_over": num(p.get("l5_over")),
        "l5_under": num(p.get("l5_under")),
        "l10_over": num(p.get("l10_over")),
        "l10_under": num(p.get("l10_under")),
        "def": p.get("def_tier") or p.get("def"),
        "cover": abs(num(p.get("edge")) or 0),
        "dist_l5": num(p.get("edge")),
        "usage_tier": p.get("usage_tier"),
        "minutes_tier": p.get("minutes_tier"),
        "starter_tier": p.get("role_tier"),
        "prop_tier": p.get("tier"),
        "actual": num(p.get("actual_value")),
        "result": str(p.get("result") or "").strip().upper(),
    }
    return r


def main() -> None:
    data = json.loads(GP.read_text(encoding="utf-8"))
    props = data["props"] if isinstance(data, dict) and "props" in data else data
    listed = defaultdict(list)
    g70 = []
    std70 = []
    board = []
    misses_g70 = []
    hits_g70 = []
    listed_miss = []
    games = set()
    seen = set()
    sports = defaultdict(int)

    for p in props:
        sp = str(p.get("sport") or "").strip().upper()
        sports[sp] += 1
        if sp not in {"WNBA", "WNBA1H", "WNBA1Q"}:
            continue
        r = rec_from(p)
        r["sport"] = sp
        key = (r["player"], r["prop"], r["pick_type"], r["side"], r["line"], sp)
        if key in seen:
            continue
        seen.add(key)
        res = r["result"]
        if res not in {"HIT", "MISS", "PUSH"}:
            continue
        if r["team"] and r["opp_team"]:
            games.add(tuple(sorted([str(r["team"]), str(r["opp_team"])])))
        if res in {"HIT", "MISS"}:
            board.append(res)
        if not _clears_list_gate(r):
            continue
        book = f"{r['pick_type']} {r['side']}"
        prop = display_prop(canon_prop("WNBA", str(r["prop"] or "")))
        if res in {"HIT", "MISS"}:
            listed["ALL"].append(res)
            listed[book].append(res)
            listed[f"PROP|{prop}|{book}"].append(res)
            if res == "MISS":
                listed_miss.append(r)
        if goblin_70_eligible(r) and res in {"HIT", "MISS"}:
            g70.append(res)
            (hits_g70 if res == "HIT" else misses_g70).append(r)
        if standard_ticket_eligible(r) and res in {"HIT", "MISS"}:
            std70.append(res)

    print("sports in graded_props:", dict(sports))
    print("games:", sorted(games))
    print("raw WNBA board decided:", tally(board))
    print("listed keep L5>=4:", tally(listed["ALL"]))
    for book in ("Goblin OVER", "Standard OVER", "Standard UNDER"):
        print(f"  {book}:", tally(listed[book]))
    print("Goblin-70:", tally(g70))
    print("Standard ticket gate:", tally(std70))
    print("G70 hits:")
    for r in hits_g70:
        print(f"  HIT {r['player']} {r['prop']} O{r['line']} act={r['actual']}")
    print("G70 misses:")
    for r in misses_g70:
        print(f"  MISS {r['player']} {r['prop']} O{r['line']} act={r['actual']}")
    print("listed by prop (n>=8):")
    keys = [k for k in listed if k.startswith("PROP|") and len(listed[k]) >= 8]
    keys.sort(key=lambda k: (-(sum(x == "HIT" for x in listed[k]) / max(1, len(listed[k]))), -len(listed[k])))
    for k in keys:
        _, prop, book = k.split("|", 2)
        print(f"  {prop} {book}: {tally(listed[k])}")
    print("listed misses (Goblin OVER, sample):")
    gob_m = [r for r in listed_miss if r["pick_type"] == "Goblin"]
    gob_m.sort(key=lambda r: (str(r["player"]), str(r["prop"])))
    for r in gob_m[:25]:
        print(f"  {r['player']} {r['prop']} O{r['line']} act={r['actual']}")
    print(f"  ... {len(gob_m)} goblin listed misses total")


if __name__ == "__main__":
    main()
