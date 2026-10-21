"""WNBA Goblin-70 ledger 2026-09-01..22.

Graded_props only has WNBA from 9/18 (finals window). Reconstruct current
goblin_70_eligible on those days, split by prop, and score WNBA legs that
actually sat on goblin70 ticket JSONs.
"""
from __future__ import annotations

import json
import sys
import unicodedata
from collections import defaultdict
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from build_goblin70_tickets import attach_wnba_ticket_priors  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402

DATES = [f"2026-09-{d:02d}" for d in range(1, 23)]
OUT = ROOT / "_tmp_wnba_g70_ledger.json"


def pname(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.casefold().replace(".", " ").replace("'", "").replace("’", "")
    for tok in (" jr", " sr", " ii", " iii", " iv"):
        if t.endswith(tok):
            t = t[: -len(tok)]
    return " ".join(t.split())


def pflt(v):
    try:
        if v in (None, "", "nan"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def to_rec(r: dict) -> dict:
    side = str(r.get("direction") or r.get("side") or "").strip().upper()
    if side in {"O", "HIGHER"}:
        side = "OVER"
    rec = {
        "sport": "WNBA",
        "player": r.get("player"),
        "prop": r.get("prop") or r.get("prop_type"),
        "side": side,
        "line": pflt(r.get("line")),
        "pick_type": str(r.get("pick_type") or "").strip().title() or "Goblin",
        "l5_over": pflt(r.get("l5_over")),
        "l5_under": pflt(r.get("l5_under")),
        "l10_over": pflt(r.get("l10_over")),
        "l10_under": pflt(r.get("l10_under")),
        "cover": pflt(r.get("edge") if r.get("edge") not in (None, "") else r.get("cover")),
        "dist_l5": pflt(r.get("edge")),
        "def": r.get("def_tier") or r.get("def"),
        "usage_tier": r.get("usage_tier"),
        "minutes_tier": r.get("minutes_tier"),
        "prop_tier": r.get("tier") or r.get("prop_tier"),
        "game_date": str(r.get("game_date") or "")[:10],
    }
    return rec


def rate(h, n):
    if not n:
        return "—"
    return f"{100.0 * h / n:.1f}% ({h}/{n})"


def main() -> None:
    days = []
    by_prop = defaultdict(lambda: [0, 0])  # hit, miss
    by_prop_ex22 = defaultdict(lambda: [0, 0])
    tix_prop = defaultdict(lambda: [0, 0])
    tix_days = []

    for date in DATES:
        gp_path = MAIN / "ui_runner" / "templates" / f"graded_props_{date}.json"
        if not gp_path.is_file():
            days.append({"date": date, "wnba": False, "note": "no graded_props"})
            continue
        gp = json.loads(gp_path.read_text(encoding="utf-8"))
        wnba = [
            r
            for r in (gp.get("props") or [])
            if str(r.get("sport") or "").upper() == "WNBA"
        ]
        if not wnba:
            days.append({"date": date, "wnba": False, "note": "no WNBA in graded_props"})
            continue

        pool = []
        listed = []
        for r in wnba:
            rec = to_rec(r)
            attach_wnba_ticket_priors(rec, date)
            res = str(r.get("result") or "").strip().upper()
            rec["_res"] = res
            rec["_prop"] = canon_prop("WNBA", str(rec.get("prop") or ""))
            pt = rec["pick_type"]
            if pt == "Demon":
                continue
            l5 = rec["l5_over"] if rec["side"] == "OVER" else rec["l5_under"]
            if pt == "Goblin" and rec["side"] == "OVER" and l5 is not None and l5 >= 4:
                listed.append(rec)
            if goblin_70_eligible(rec):
                pool.append(rec)

        def score(rows):
            hit = sum(1 for x in rows if x["_res"] == "HIT")
            miss = sum(1 for x in rows if x["_res"] == "MISS")
            return hit, miss, hit + miss

        lh, lm, ln = score(listed)
        gh, gm, gn = score(pool)
        props = defaultdict(lambda: [0, 0])
        for x in pool:
            if x["_res"] == "HIT":
                props[x["_prop"]][0] += 1
                by_prop[x["_prop"]][0] += 1
                if date != "2026-09-22":
                    by_prop_ex22[x["_prop"]][0] += 1
            elif x["_res"] == "MISS":
                props[x["_prop"]][1] += 1
                by_prop[x["_prop"]][1] += 1
                if date != "2026-09-22":
                    by_prop_ex22[x["_prop"]][1] += 1
        day = {
            "date": date,
            "wnba": True,
            "listed": rate(lh, ln),
            "listed_n": ln,
            "g70": rate(gh, gn),
            "g70_n": gn,
            "g70_hit": gh,
            "g70_miss": gm,
            "props": {k: rate(v[0], v[0] + v[1]) for k, v in sorted(props.items())},
        }
        days.append(day)

        # Actual ticket WNBA legs
        for root in (MAIN, ROOT):
            tpath = root / "data" / "reports" / f"goblin70_tickets_{date}.json"
            if tpath.is_file():
                break
        else:
            tpath = None
        if tpath and tpath.is_file():
            payload = json.loads(tpath.read_text(encoding="utf-8"))
            index = {}
            for r in wnba:
                rec = to_rec(r)
                key = (
                    pname(rec["player"]),
                    canon_prop("WNBA", str(rec["prop"] or "")),
                    rec["line"],
                    rec["pick_type"],
                    rec["side"],
                )
                index[key] = str(r.get("result") or "").strip().upper()
            th = tm = 0
            for tix in payload.get("tickets") or []:
                if "yolo" in str(tix.get("name") or tix.get("id") or "").lower():
                    continue
                for lg in tix.get("legs") or []:
                    if str(lg.get("sport") or "").upper() != "WNBA":
                        continue
                    key = (
                        pname(lg.get("player")),
                        canon_prop("WNBA", str(lg.get("prop") or lg.get("prop_type") or "")),
                        pflt(lg.get("line")),
                        str(lg.get("pick_type") or "").strip().title(),
                        str(lg.get("side") or lg.get("direction") or "").strip().upper(),
                    )
                    res = index.get(key)
                    if res == "HIT":
                        th += 1
                        tix_prop[key[1]][0] += 1
                    elif res == "MISS":
                        tm += 1
                        tix_prop[key[1]][1] += 1
            tix_days.append(
                {
                    "date": date,
                    "file": str(tpath),
                    "legs": rate(th, th + tm),
                    "n": th + tm,
                }
            )

    tot_h = sum(d.get("g70_hit") or 0 for d in days)
    tot_m = sum(d.get("g70_miss") or 0 for d in days)
    ex = [d for d in days if d.get("wnba") and d["date"] != "2026-09-22"]
    ex_h = sum(d.get("g70_hit") or 0 for d in ex)
    ex_m = sum(d.get("g70_miss") or 0 for d in ex)

    payload = {
        "window": "2026-09-01..2026-09-22",
        "note": "WNBA only appears in graded_props from 9/18 (finals). Gate is current goblin_70_eligible.",
        "days": days,
        "g70_all_wnba_days": rate(tot_h, tot_h + tot_m),
        "g70_ex_922": rate(ex_h, ex_h + ex_m),
        "by_prop_all": {k: rate(v[0], v[0] + v[1]) for k, v in sorted(by_prop.items())},
        "by_prop_ex_922": {
            k: rate(v[0], v[0] + v[1]) for k, v in sorted(by_prop_ex22.items())
        },
        "ticket_wnba_legs": tix_days,
        "ticket_wnba_by_prop": {
            k: rate(v[0], v[0] + v[1]) for k, v in sorted(tix_prop.items())
        },
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("WNBA G70 reconstructed (current gates)")
    print(f"{'date':12} {'listed':20} {'G70':20} props")
    for d in days:
        if not d.get("wnba"):
            continue
        print(f"{d['date']:12} {d['listed']:20} {d['g70']:20} {d['props']}")
    print("ALL WNBA days", payload["g70_all_wnba_days"])
    print("ex 9/22     ", payload["g70_ex_922"])
    print("by prop all ", payload["by_prop_all"])
    print("by prop -22 ", payload["by_prop_ex_922"])
    print("ticket legs ", payload["ticket_wnba_legs"])
    print("ticket props", payload["ticket_wnba_by_prop"])
    print("wrote", OUT)


if __name__ == "__main__":
    main()
