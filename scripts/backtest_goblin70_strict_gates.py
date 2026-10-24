#!/usr/bin/env python3
"""Backtest Goblin ticket gates on graded props and published Goblin-70 tickets.

Old live card: L5>=4 + cover floor + no shadow.
Candidates: L5=5 + L10>=8 + cover, plus S/A, plus S/A-or-WNBA-FGA.

  py -3.14 scripts/backtest_goblin70_strict_gates.py
"""
from __future__ import annotations

import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import all_ranked_prop_hit_rates as A  # noqa: E402
import prop_hit_tiers as T  # noqa: E402
from build_goblin70_tickets import fold_name  # noqa: E402
from utils.defense_tiers import normalize_def_tier_label  # noqa: E402
from utils.fantasy_prop_filter import is_fantasy_prop_label  # noqa: E402
from utils.slate_context_fill import is_hitter_strikeout_prop  # noqa: E402
from utils.ticket_70_pool import skip_combo_player, skip_era_half  # noqa: E402

OUT = _REPO / "data" / "reports" / "goblin70_strict_gate_backtest.json"
ACTIVE = T.ACTIVE
MAIN = _REPO.parent / "PropORACLE_main_cp"


def num(v):
    return A.num(v)


def l10_dir(row: dict, side: str):
    keys = (
        ("l10_over", "L10 Over", "last10_over")
        if side == "OVER"
        else ("l10_under", "L10 Under", "last10_under")
    )
    for k in keys:
        v = num(row.get(k))
        if v is not None:
            return v
    return num(row.get("l10"))


def cover_of(row: dict):
    for k in ("cover", "dist_l5", "edge"):
        v = num(row.get(k))
        if v is not None:
            return v
    avg = num(row.get("avg_l5") or row.get("last5_avg"))
    line = num(row.get("line") or row.get("line_score"))
    if avg is not None and line is not None:
        return avg - line
    return None


def d_ok_over(sport: str, prop: str, raw) -> bool:
    tier = normalize_def_tier_label(raw) or ""
    if sport == "MLB" and is_hitter_strikeout_prop(prop):
        return tier in ("Elite", "Above Avg")
    return tier in ("Weak", "Below Avg")


def sa_tier(sport: str, prop: str, cover, d_ok: bool) -> str:
    info = T.assign_tier(
        sport=sport,
        pick_type="Goblin",
        side="OVER",
        prop=prop,
        cover=cover,
        d_ok=d_ok,
    )
    return str(info.get("prop_tier") or "")


def passes(
    row: dict,
    *,
    l5_min: float,
    l10_min: float | None,
    require_sa: bool,
    keep_fga: bool,
) -> bool:
    sport = T.norm_sport(str(row.get("sport") or ""))
    if sport not in ACTIVE:
        return False
    if skip_combo_player(row) or skip_era_half(row):
        return False
    side = str(row.get("side") or row.get("direction") or "OVER").strip().upper()
    if side in ("O", "OVER"):
        side = "OVER"
    if side != "OVER":
        return False
    prop = T.canon_prop(sport, str(row.get("prop") or row.get("prop_type") or ""))
    if not prop or is_fantasy_prop_label(prop) or prop == "fantasy":
        return False
    if T.is_shadow(sport, "Goblin OVER", prop):
        return False
    l5 = A.l5_dir(row, side)
    if l5 is None:
        l5 = num(row.get("l5"))
    if l5 is None or l5 < l5_min:
        return False
    if l10_min is not None:
        l10 = l10_dir(row, side)
        if l10 is None or l10 < l10_min:
            return False
    cover = cover_of(row)
    if not T.cover_clears_floor(sport, cover, "OVER", prop):
        return False
    if require_sa:
        if keep_fga and sport == "WNBA" and prop == "fga":
            return True
        d_raw = row.get("def") or row.get("d") or row.get("def_tier")
        if sa_tier(sport, prop, cover, d_ok_over(sport, prop, d_raw)) not in ("S", "A"):
            return False
    return True


GATES = (
    ("old_l5ge4_cover", dict(l5_min=4, l10_min=None, require_sa=False, keep_fga=False)),
    ("l5eq5_l10ge8_cover", dict(l5_min=5, l10_min=8, require_sa=False, keep_fga=False)),
    ("l5eq5_l10ge8_sa", dict(l5_min=5, l10_min=8, require_sa=True, keep_fga=False)),
    ("l5eq5_l10ge8_sa_or_fga", dict(l5_min=5, l10_min=8, require_sa=True, keep_fga=True)),
)


def empty():
    return {"n": 0, "hits": 0}


def add(c, hit: int):
    c["n"] += 1
    c["hits"] += hit


def hr(c):
    if not c["n"]:
        return None
    return round(c["hits"] / c["n"], 4)


def fmt(c):
    if not c["n"]:
        return "--"
    return f"{100 * c['hits'] / c['n']:.1f}%  {c['hits']}/{c['n']}"


def load_grades() -> tuple[dict[str, list[dict]], dict[tuple, dict]]:
    by_date: dict[str, list[dict]] = defaultdict(list)
    idx: dict[tuple, dict] = {}
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
            if res not in ("HIT", "MISS"):
                continue
            sport = T.norm_sport(str(r.get("sport") or ""))
            side = str(r.get("direction") or r.get("over_under") or "").strip().upper()
            if side in ("O", "OVER"):
                side = "OVER"
            else:
                continue
            if A.pick_book(str(r.get("pick_type") or ""), side) != "Goblin OVER":
                continue
            prop = T.canon_prop(sport, str(r.get("prop") or r.get("prop_type") or ""))
            if not prop:
                continue
            rec = dict(r)
            rec["sport"] = sport
            rec["prop"] = prop
            rec["side"] = "OVER"
            rec["pick_type"] = "Goblin"
            rec["_hit"] = 1 if res == "HIT" else 0
            by_date[date].append(rec)
            line = num(r.get("line") or r.get("line_score"))
            key = (
                date,
                fold_name(r.get("player")),
                sport,
                prop,
                None if line is None else round(line, 1),
            )
            prev = idx.get(key)
            if prev is None or (A.l5_dir(rec, "OVER") or 0) >= (A.l5_dir(prev, "OVER") or 0):
                idx[key] = rec
    return by_date, idx


def grade_backtest(by_date: dict[str, list[dict]]) -> dict:
    overall = {g: empty() for g, _ in GATES}
    by_sport = defaultdict(lambda: {g: empty() for g, _ in GATES})
    by_prop = defaultdict(lambda: {g: empty() for g, _ in GATES})
    dropped_from_old = Counter()
    dropped_from_l5l10 = Counter()
    for rows in by_date.values():
        for r in rows:
            sport = r["sport"]
            if sport not in ACTIVE:
                continue
            prop = r["prop"]
            hit = r["_hit"]
            flags = {name: passes(r, **kw) for name, kw in GATES}
            for name, ok in flags.items():
                if ok:
                    add(overall[name], hit)
                    add(by_sport[sport][name], hit)
                    add(by_prop[f"{sport}|{prop}"][name], hit)
            if flags["old_l5ge4_cover"] and not flags["l5eq5_l10ge8_cover"]:
                dropped_from_old[f"{sport}|{prop}"] += 1
            if flags["l5eq5_l10ge8_cover"] and not flags["l5eq5_l10ge8_sa"]:
                dropped_from_l5l10[f"{sport}|{prop}"] += 1
    prop_rows = []
    for key, cells in by_prop.items():
        old = cells["old_l5ge4_cover"]
        if old["n"] < 20:
            continue
        sport, prop = key.split("|", 1)
        row = {"sport": sport, "prop": prop}
        for name, cell in cells.items():
            row[name] = {"n": cell["n"], "hits": cell["hits"], "hr": hr(cell)}
        row["removed_l5l10_n"] = old["n"] - cells["l5eq5_l10ge8_cover"]["n"]
        row["removed_sa_n"] = old["n"] - cells["l5eq5_l10ge8_sa"]["n"]
        prop_rows.append(row)
    prop_rows.sort(key=lambda r: -r["removed_l5l10_n"])
    return {
        "overall": {k: {**v, "hr": hr(v)} for k, v in overall.items()},
        "by_sport": {
            s: {k: {**v, "hr": hr(v)} for k, v in cells.items()}
            for s, cells in sorted(by_sport.items())
        },
        "by_prop": prop_rows,
        "dropped_old_not_l5l10": dropped_from_old.most_common(40),
        "dropped_l5l10_not_sa": dropped_from_l5l10.most_common(40),
    }


def attach_grade(leg: dict, date: str, idx: dict) -> dict:
    sport = T.norm_sport(str(leg.get("sport") or ""))
    prop = T.canon_prop(sport, str(leg.get("prop") or leg.get("prop_type") or ""))
    line = num(leg.get("line"))
    key = (
        date,
        fold_name(leg.get("player")),
        sport,
        prop,
        None if line is None else round(line, 1),
    )
    g = idx.get(key)
    merged = dict(leg)
    merged["sport"] = sport
    merged["prop"] = prop or str(leg.get("prop") or "")
    merged["side"] = "OVER"
    merged["pick_type"] = merged.get("pick_type") or "Goblin"
    if g:
        merged["_hit"] = g["_hit"]
        merged["result"] = "HIT" if g["_hit"] else "MISS"
        if A.l5_dir(merged, "OVER") is None and num(merged.get("l5")) is None:
            merged["l5_over"] = g.get("l5_over")
            merged["l5"] = g.get("l5_over")
        if l10_dir(merged, "OVER") is None:
            merged["l10_over"] = g.get("l10_over")
        if cover_of(merged) is None:
            merged["cover"] = cover_of(g)
        if not (merged.get("def") or merged.get("d") or merged.get("def_tier")):
            merged["def"] = g.get("def_tier") or g.get("def")
    return merged


def ticket_report_files() -> list[Path]:
    by_date: dict[str, Path] = {}
    for root in (MAIN, _REPO):
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


def ticket_backtest(idx: dict) -> dict:
    by_gate = {
        g: {
            "tickets": 0,
            "fully_graded": 0,
            "sweep_hits": 0,
            "flex_cash_hits": 0,
            "kept": 0,
            "kept_graded": 0,
            "kept_sweep": 0,
            "kept_flex_cash": 0,
        }
        for g, _ in GATES
    }
    pool = {g: empty() for g, _ in GATES}
    dropped_legs = Counter()
    dropped_pool = Counter()
    files = []
    n_tickets = 0
    n_pool = 0
    n_pool_graded = 0
    for path in ticket_report_files():
        raw = json.loads(path.read_text(encoding="utf-8"))
        date = str(raw.get("date") or "")[:10]
        if not date:
            continue
        files.append({"date": date, "path": str(path), "tickets": 0, "pool": 0})
        for rec in raw.get("goblin_pool") or []:
            if not isinstance(rec, dict):
                continue
            n_pool += 1
            files[-1]["pool"] += 1
            merged = attach_grade(rec, date, idx)
            for name, kw in GATES:
                if passes(merged, **kw):
                    if "_hit" in merged:
                        add(pool[name], merged["_hit"])
            if "_hit" in merged:
                n_pool_graded += 1
                if passes(merged, **GATES[0][1]) and not passes(merged, **GATES[1][1]):
                    sport = merged.get("sport") or ""
                    prop = merged.get("prop") or ""
                    dropped_pool[f"{sport}|{prop}"] += 1
        for t in raw.get("tickets") or []:
            if not isinstance(t, dict) or str(t.get("family") or "") != "goblin":
                continue
            legs_raw = t.get("legs") or []
            if len(legs_raw) < 2:
                continue
            n_tickets += 1
            files[-1]["tickets"] += 1
            merged = [attach_grade(leg, date, idx) for leg in legs_raw]
            n = len(merged)
            graded = all("_hit" in m for m in merged)
            hits = sum(m.get("_hit") or 0 for m in merged) if graded else None
            product = str(t.get("product") or t.get("play") or "").lower()
            sweep = hits == n if graded else None
            flex_cash = None
            if graded:
                need = n if "power" in product else max(n - 1, 1)
                flex_cash = hits >= need
            for name, kw in GATES:
                cell = by_gate[name]
                cell["tickets"] += 1
                if graded:
                    cell["fully_graded"] += 1
                    cell["sweep_hits"] += int(bool(sweep))
                    cell["flex_cash_hits"] += int(bool(flex_cash))
                keep = all(passes(m, **kw) for m in merged)
                if keep:
                    cell["kept"] += 1
                    if graded:
                        cell["kept_graded"] += 1
                        cell["kept_sweep"] += int(bool(sweep))
                        cell["kept_flex_cash"] += int(bool(flex_cash))
                elif name != "old_l5ge4_cover":
                    for m in merged:
                        if not passes(m, **kw):
                            dropped_legs[f"{name}|{m.get('sport')}|{m.get('prop')}"] += 1
    out_gates = {}
    for name, cell in by_gate.items():
        out_gates[name] = {
            **cell,
            "all_sweep_hr": round(cell["sweep_hits"] / cell["fully_graded"], 4)
            if cell["fully_graded"]
            else None,
            "all_cash_hr": round(cell["flex_cash_hits"] / cell["fully_graded"], 4)
            if cell["fully_graded"]
            else None,
            "kept_share": round(cell["kept"] / cell["tickets"], 4) if cell["tickets"] else None,
            "kept_sweep_hr": round(cell["kept_sweep"] / cell["kept_graded"], 4)
            if cell["kept_graded"]
            else None,
            "kept_cash_hr": round(cell["kept_flex_cash"] / cell["kept_graded"], 4)
            if cell["kept_graded"]
            else None,
        }
    drop_rows = []
    for k, n in dropped_legs.most_common(50):
        parts = k.split("|", 2)
        if len(parts) == 3:
            drop_rows.append({"gate": parts[0], "sport": parts[1], "prop": parts[2], "n": n})
    return {
        "files": files,
        "tickets_seen": n_tickets,
        "pool_seen": n_pool,
        "pool_graded": n_pool_graded,
        "pool_by_gate": {k: {**v, "hr": hr(v)} for k, v in pool.items()},
        "tickets_by_gate": out_gates,
        "dropped_ticket_legs": drop_rows,
        "dropped_pool_old_not_l5l10": dropped_pool.most_common(30),
    }


def main() -> None:
    print("loading grades…")
    by_date, idx = load_grades()
    print(f"  dates {len(by_date)} goblin-over index {len(idx)}")
    print("scoring grades…")
    grades = grade_backtest(by_date)
    print("\n===== GRADED Goblin OVER (active sports, cover floor, no shadow) =====")
    for name, cell in grades["overall"].items():
        print(f"  {name:28} {fmt({'n': cell['n'], 'hits': cell['hits']})}")
    print("\nby sport")
    for sport, cells in grades["by_sport"].items():
        print(f"  {sport}")
        for name, cell in cells.items():
            print(f"    {name:26} {fmt({'n': cell['n'], 'hits': cell['hits']})}")
    print("\nprops old n>=20, most removed by L5=5+L10>=8")
    for r in grades["by_prop"][:20]:
        o = r["old_l5ge4_cover"]
        m = r["l5eq5_l10ge8_cover"]
        sa = r["l5eq5_l10ge8_sa"]
        print(
            f"  {r['sport']:7} {r['prop'][:22]:22} "
            f"old {fmt({'n': o['n'], 'hits': o['hits']})}  "
            f"L5+L10 {fmt({'n': m['n'], 'hits': m['hits']})}  "
            f"S/A {fmt({'n': sa['n'], 'hits': sa['hits']})}  "
            f"drop {r['removed_l5l10_n']}"
        )
    print("\nscoring published Goblin-70 tickets + pools…")
    tickets = ticket_backtest(idx)
    print(f"  files {len(tickets['files'])} tickets {tickets['tickets_seen']} pool {tickets['pool_seen']}")
    print("  published pool (joined to grades)")
    for name, cell in tickets["pool_by_gate"].items():
        print(f"    {name:26} {fmt({'n': cell['n'], 'hits': cell['hits']})}")
    print("  tickets")
    for name, cell in tickets["tickets_by_gate"].items():
        print(
            f"    {name:26} keep {cell['kept']}/{cell['tickets']}  "
            f"all-sweep {cell['all_sweep_hr']}  "
            f"kept-sweep {cell['kept_sweep_hr']} (n={cell['kept_graded']})  "
            f"kept-cash {cell['kept_cash_hr']}"
        )
    payload = {
        "note": (
            "Active-sport Goblin OVER, cover floor, no shadow. "
            "old = prior ticket gate (L5>=4+cover). "
            "l5eq5_l10 = L5=5+L10>=8+cover (current code). "
            "sa = that plus S/A (promotions included). "
            "sa_or_fga keeps WNBA FGA (B, historically the top WNBA cell). "
            "Ticket sweep = every leg HIT. Cash = Power all-hit or Flex n-1."
        ),
        "grades": grades,
        "tickets": tickets,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print("\nwrote", OUT)


if __name__ == "__main__":
    main()
