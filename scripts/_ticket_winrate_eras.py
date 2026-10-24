"""Compare historical live-card ticket cash vs Goblin-70 published slips."""
from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
MAIN = REPO.parent / "PropORACLE_main_cp"
OUT = REPO / "data" / "reports" / "ticket_winrate_eras.json"

import sys

sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(REPO))
import all_ranked_prop_hit_rates as A  # noqa: E402
import prop_hit_tiers as T  # noqa: E402
from build_goblin70_tickets import fold_name  # noqa: E402


def num(v):
    try:
        if v is None or v == "":
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def load_hist() -> list[dict]:
    by_key: dict[tuple[str, str], dict] = {}
    for p in [
        REPO / "ui_runner" / "templates" / "grade_history.json",
        REPO / "mobile" / "www" / "data" / "grade_history.json",
        MAIN / "data" / "grade_history.json",
        MAIN / "ui_runner" / "templates" / "grade_history.json",
        MAIN / "mobile" / "www" / "data" / "grade_history.json",
    ]:
        if not p.is_file():
            continue
        data = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(data, list):
            continue
        for r in data:
            if not isinstance(r, dict):
                continue
            d = str(r.get("date") or "")[:10]
            tr = str(r.get("track") or "unknown")
            if d:
                by_key[(d, tr)] = r
    return list(by_key.values())


def era(d: str) -> str:
    if d < "2026-06-01":
        return "Apr-May"
    if d < "2026-07-01":
        return "Jun"
    if d < "2026-08-01":
        return "Jul"
    if d < "2026-08-26":
        return "Aug 1-25 pre-G70"
    return "Aug 26+ G70 live"


def pct(h, n):
    if not n:
        return None
    return round(100.0 * h / n, 1)


def summarize_hist(rows: list[dict]) -> dict:
    eras = defaultdict(lambda: defaultdict(lambda: {"n": 0, "wins": 0, "guar": 0, "void": 0, "days": set()}))
    monthly = defaultdict(lambda: {"n": 0, "wins": 0, "guar": 0})
    for r in rows:
        d = str(r.get("date") or "")[:10]
        tr = str(r.get("track") or "unknown")
        n = int(r.get("n_tickets") or 0)
        w = int(r.get("wins") or 0)
        g = int(r.get("guarantees") or 0)
        v = int(r.get("void_loss_ct") or 0)
        cell = eras[era(d)][tr]
        cell["n"] += n
        cell["wins"] += w
        cell["guar"] += g
        cell["void"] += v
        cell["days"].add(d)
        if tr in {"graded_main", "unknown", ""}:
            monthly[d[:7]]["n"] += n
            monthly[d[:7]]["wins"] += w
            monthly[d[:7]]["guar"] += g
    era_out = []
    for e, tracks in eras.items():
        for tr, c in tracks.items():
            era_out.append(
                {
                    "era": e,
                    "track": tr,
                    "days": len(c["days"]),
                    "n": c["n"],
                    "wins": c["wins"],
                    "guarantees": c["guar"],
                    "sweep_pct": pct(c["wins"], c["n"]),
                    "cash_pct": pct(c["wins"] + c["guar"], c["n"]),
                    "void_loss": c["void"],
                }
            )
    era_out.sort(key=lambda x: (x["era"], -x["n"]))
    month_out = []
    for m, c in sorted(monthly.items()):
        month_out.append(
            {
                "month": m,
                "n": c["n"],
                "wins": c["wins"],
                "guarantees": c["guar"],
                "sweep_pct": pct(c["wins"], c["n"]),
                "cash_pct": pct(c["wins"] + c["guar"], c["n"]),
            }
        )
    return {"by_era_track": era_out, "graded_main_monthly": month_out}


def load_grade_idx() -> dict[tuple, int]:
    idx: dict[tuple, int] = {}
    idx_noline: dict[tuple, list[int]] = defaultdict(list)
    for path in A.graded_paths():
        date = path.stem.replace("graded_props_", "")[:10]
        raw = json.loads(path.read_text(encoding="utf-8"))
        props = raw.get("props") or raw.get("rows") or []
        if not isinstance(props, list):
            continue
        for r in props:
            if not isinstance(r, dict):
                continue
            res = str(r.get("result") or "").strip().upper()
            if res not in {"HIT", "MISS"}:
                continue
            hit = 1 if res == "HIT" else 0
            sport = T.norm_sport(str(r.get("sport") or ""))
            prop = T.canon_prop(sport, str(r.get("prop") or r.get("prop_type") or ""))
            player = fold_name(r.get("player") or r.get("name"))
            line = num(r.get("line") or r.get("line_score"))
            side = str(r.get("direction") or r.get("over_under") or r.get("side") or "OVER").upper()
            if side in {"O", "OVER"}:
                side = "OVER"
            elif side in {"U", "UNDER", "LOWER"}:
                side = "UNDER"
            key = (date, player, sport, prop, None if line is None else round(line, 1), side)
            idx[key] = hit
            idx_noline[(date, player, sport, prop, side)].append(hit)
    return {"line": idx, "noline": idx_noline}


def lookup(idx, date, leg) -> int | None:
    sport = T.norm_sport(str(leg.get("sport") or ""))
    prop = T.canon_prop(sport, str(leg.get("prop") or leg.get("prop_type") or ""))
    player = fold_name(leg.get("player"))
    line = num(leg.get("line"))
    side = str(leg.get("side") or leg.get("direction") or "OVER").upper()
    if side in {"O", "OVER"}:
        side = "OVER"
    elif side in {"U", "UNDER", "LOWER"}:
        side = "UNDER"
    key = (date, player, sport, prop, None if line is None else round(line, 1), side)
    if key in idx["line"]:
        return idx["line"][key]
    alts = idx["noline"].get((date, player, sport, prop, side)) or []
    if len(set(alts)) == 1:
        return alts[0]
    return None


def ticket_files() -> list[Path]:
    by_date: dict[str, Path] = {}
    for root in (MAIN, REPO):
        d = root / "data" / "reports"
        if not d.is_dir():
            continue
        for p in sorted(d.glob("goblin70_tickets_2026-*.json")):
            if "_l5eq5" in p.name:
                continue
            date = p.stem.replace("goblin70_tickets_", "")[:10]
            if date not in by_date:
                by_date[date] = p
    return [by_date[k] for k in sorted(by_date)]


def sleeve(tid: str, n: int, product: str) -> str:
    t = (tid or "").upper()
    if t.startswith("Y") or "YOLO" in t:
        return "YOLO Power 4-6"
    if "POWER" in product.upper() and n <= 3:
        return "Power 2-3"
    if "FLEX" in product.upper() and n == 4:
        return "Flex 4"
    if n >= 5:
        return "Flex 5-6 other"
    return f"{product} {n}"


def grade_g70(idx) -> dict:
    buckets = defaultdict(lambda: {"tickets": 0, "graded": 0, "sweep": 0, "cash": 0, "legs_hit": 0, "legs": 0})
    daily = defaultdict(lambda: {"tickets": 0, "graded": 0, "sweep": 0, "cash": 0})
    files = []
    for path in ticket_files():
        raw = json.loads(path.read_text(encoding="utf-8"))
        date = str(raw.get("date") or "")[:10]
        rec = {"date": date, "path": str(path), "n": 0, "graded": 0, "sweep": 0, "cash": 0}
        for t in raw.get("tickets") or []:
            if not isinstance(t, dict):
                continue
            if t.get("exclude_from_winrate") or "YOLO" in str(t.get("web_group") or "").upper():
                continue
            if str(t.get("id") or "").upper().startswith("Y"):
                continue
            fam = str(t.get("family") or "")
            if fam not in {"goblin", "mix", "standard"}:
                continue
            legs = t.get("legs") or []
            n = len(legs)
            if n < 2:
                continue
            product = str(t.get("product") or t.get("play") or "Power")
            hits = []
            for leg in legs:
                h = lookup(idx, date, leg)
                hits.append(h)
            rec["n"] += 1
            sl = sleeve(str(t.get("id") or ""), n, product)
            b = buckets[sl]
            b["tickets"] += 1
            b["legs"] += n
            if any(h is None for h in hits):
                continue
            nh = sum(int(h) for h in hits)
            b["graded"] += 1
            b["legs_hit"] += nh
            rec["graded"] += 1
            daily[date]["graded"] += 1
            sweep = nh == n
            if "flex" in product.lower():
                cash = nh >= max(n - 1, 1)
            else:
                cash = sweep
            b["sweep"] += int(sweep)
            b["cash"] += int(cash)
            rec["sweep"] += int(sweep)
            rec["cash"] += int(cash)
            daily[date]["sweep"] += int(sweep)
            daily[date]["cash"] += int(cash)
            daily[date]["tickets"] += 1
        files.append(rec)
    out_b = []
    for k, c in sorted(buckets.items(), key=lambda kv: -kv[1]["graded"]):
        out_b.append(
            {
                "sleeve": k,
                **c,
                "sweep_pct": pct(c["sweep"], c["graded"]),
                "cash_pct": pct(c["cash"], c["graded"]),
                "leg_hr": pct(c["legs_hit"], c["legs"]) if c["graded"] else None,
            }
        )
    daily_out = []
    for d, c in sorted(daily.items()):
        daily_out.append(
            {
                "date": d,
                "graded": c["graded"],
                "sweep_pct": pct(c["sweep"], c["graded"]),
                "cash_pct": pct(c["cash"], c["graded"]),
                "sweep": c["sweep"],
                "cash": c["cash"],
            }
        )
    tot_g = sum(x["graded"] for x in out_b)
    tot_s = sum(x["sweep"] for x in out_b)
    tot_c = sum(x["cash"] for x in out_b)
    return {
        "files": files,
        "by_sleeve": out_b,
        "daily": daily_out,
        "all": {
            "graded": tot_g,
            "sweep": tot_s,
            "cash": tot_c,
            "sweep_pct": pct(tot_s, tot_g),
            "cash_pct": pct(tot_c, tot_g),
        },
    }


def main() -> None:
    hist = summarize_hist(load_hist())
    print("=== Live card grade_history (cash = wins + Flex guarantees) ===")
    for r in hist["by_era_track"]:
        print(
            f"  {r['era']:18} {r['track']:24} n={r['n']:4} d={r['days']:2} "
            f"sweep={r['sweep_pct']}% cash={r['cash_pct']}% void={r['void_loss']}"
        )
    print("=== graded_main monthly ===")
    for r in hist["graded_main_monthly"]:
        print(f"  {r['month']} n={r['n']:4} sweep={r['sweep_pct']}% cash={r['cash_pct']}%")
    print("loading grades…")
    idx = load_grade_idx()
    print(f"  index {len(idx['line'])}")
    g70 = grade_g70(idx)
    print("=== Goblin-70 published tickets vs graded_props ===")
    print(
        f"  ALL graded {g70['all']['graded']} sweep={g70['all']['sweep_pct']}% "
        f"cash={g70['all']['cash_pct']}%  {g70['all']['sweep']}/{g70['all']['graded']} sweep"
    )
    for r in g70["by_sleeve"]:
        print(
            f"  {r['sleeve']:18} tix={r['tickets']:3} graded={r['graded']:3} "
            f"sweep={r['sweep_pct']}% cash={r['cash_pct']}%  {r['sweep']}/{r['graded']}"
        )
    print("=== G70 daily ===")
    for r in g70["daily"]:
        print(f"  {r['date']} graded={r['graded']:3} sweep={r['sweep_pct']}% cash={r['cash_pct']}%")
    payload = {
        "note": (
            "Live-card grade_history cash = wins + Flex N-1 guarantees. "
            "Goblin-70 is published slips joined to graded_props HIT/MISS. "
            "Power cash = all legs; Flex cash = miss at most one."
        ),
        "grade_history": hist,
        "goblin70": g70,
        "benchmarks": {
            "ticket_leg_count_2026-05-05_06-09": {
                "main_2_4": {"n": 1824, "sweep_pct": 16.1},
                "long_5_6": {"n": 4178, "sweep_pct": 5.5},
                "all": {"n": 6002, "sweep_pct": 8.7},
            },
            "diamond_2x_2026-03-10_08-22": {
                "P3": {"n": 276, "sweep_pct": 42.0, "cash_pct": 42.0},
                "F4": {"n": 280, "sweep_pct": 26.8, "cash_pct": 70.7},
                "F5": {"n": 139, "sweep_pct": 17.3, "cash_pct": 59.0},
            },
            "goblin70_prop_gate": {
                "old_l5ge4_cover": {"n": 17879, "hr": 68.3},
                "l5eq5_l10ge8_cover": {"n": 2941, "hr": 74.1},
            },
        },
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("wrote", OUT)


if __name__ == "__main__":
    raise SystemExit(main())
