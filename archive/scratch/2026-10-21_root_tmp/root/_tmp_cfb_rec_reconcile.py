"""One-off: Rec Goblin D-kept with same-game collapse vs any-later-date collapse."""
from datetime import date

import _tmp_cfb_l5_recut as R
import _tmp_cfb_live_stack as S

R.TAPE = [
    "2026-08-28",
    "2026-08-29",
    "2026-09-03",
    "2026-09-04",
    "2026-09-05",
    "2026-09-12",
    "2026-09-26",
]


def collapse(rows, max_days):
    from collections import defaultdict

    groups = defaultdict(list)
    for r in rows:
        groups[(r["player_norm"], r["prop"], r["book"], r["side"])].append(r)
    out = []
    for g in groups.values():
        g = sorted(g, key=lambda x: x["date"])
        kept = []
        for r in g:
            close = False
            if kept and abs((r.get("line") or 0) - (kept[-1].get("line") or 0)) <= 1.0:
                gap = (date.fromisoformat(r["date"]) - date.fromisoformat(kept[-1]["date"])).days
                close = gap <= max_days
            if close:
                kept[-1] = r
            else:
                kept.append(r)
        out.extend(kept)
    return out


# Rebuild pre-collapse rows by calling collect's loader via a local copy of the loop.
rows = []
for dte in R.TAPE:
    gpath = R.graded_for(dte)
    spath = R.find_step8(dte)
    if not gpath or not spath:
        print("missing", dte, bool(gpath), bool(spath))
        continue
    import json

    import prop_hit_tiers as T
    from prop_hit_tiers import cover_clears_floor

    idx = R.load_step8(spath)
    edges = S.load_signed_edge(spath)
    raw = json.loads(gpath.read_text(encoding="utf-8"))
    for r in raw.get("props") or []:
        if not isinstance(r, dict) or not R.is_cfb(r):
            continue
        res = str(r.get("result") or "").upper()
        if res not in {"HIT", "MISS"}:
            continue
        prop = T.canon_prop("CFB", str(r.get("prop") or r.get("prop_type") or ""))
        if not prop:
            continue
        side = R.side_of(r.get("direction") or r.get("over_under") or r.get("side"))
        if side is None:
            continue
        pick = R.pick_of(r.get("pick_type"))
        book = R.book_of(pick, side)
        if book not in R.BOOKS:
            continue
        line = R.num(r.get("line"))
        key = (R.norm_player(r.get("player")), prop, pick, line, side)
        s8 = idx.get(key) or {}
        rec = {
            "date": dte,
            "player": r.get("player"),
            "player_norm": key[0],
            "prop": prop,
            "book": book,
            "side": side,
            "line": line,
            "hit": 1 if res == "HIT" else 0,
            "l5_over": R.first_num(r.get("l5_over"), r.get("last5_over"), s8.get("l5_over")),
            "l5_under": R.first_num(r.get("l5_under"), r.get("last5_under"), s8.get("l5_under")),
            "def_tier": r.get("def_tier") or r.get("def") or s8.get("def_tier") or "",
            "cover": edges.get(key),
        }
        rec["l5"] = R.l5_dir(rec, side)
        rec["d"] = R.d_ok(side, rec["def_tier"])
        rec["cover_ok"] = cover_clears_floor("CFB", rec["cover"], side, prop)
        rows.append(rec)

unique = R.unique_dedupe(rows)


def report(label, pool):
    l5 = [r for r in pool if r["prop"] == "rec_yds" and r["book"] == "Goblin OVER" and r["l5"] == 5]
    kept = [r for r in l5 if r["d"]]
    dropped = [r for r in l5 if not r["d"]]
    def pack(xs):
        return f"{sum(r['hit'] for r in xs)}/{len(xs)}"
    print(label, "L5", pack(l5), "D-kept", pack(kept), "D-dropped", pack(dropped))


report("any later date", collapse(unique, 9999))
report("within 1 day", collapse(unique, 1))
report("no collapse", unique)
