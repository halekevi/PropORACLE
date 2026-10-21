"""Weak-market volume on Goblin-70 published tickets + pools (correct prop labels)."""
from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
bt = json.loads((REPO / "data/reports/goblin70_strict_gate_backtest.json").read_text(encoding="utf-8"))

WEAK_CANON = {
    "pra",  # Pts+Rebs+Asts
    "hitterks",
    "gameswon",  # Total Games Won (not Total Games)
}
EXTRA_CANON = {"totalbases"}


def canon_prop(p: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(p or "").strip().lower())


def classify(sport: object, prop: object) -> str | None:
    sp = str(sport or "").strip().upper()
    if sp == "TENNIS":
        sp = "TENNIS"
    cp = canon_prop(prop)
    if sp == "WNBA" and cp in {"pra", "ptsrebsasts", "pointsreboundsassists", "ptsrebast"}:
        return "pra"
    if sp == "MLB" and cp in {"hitterks", "hitterstrikeouts", "batterks", "batterstrikeouts"}:
        return "hitter_ks"
    if sp in {"TENNIS", "ATP", "WTA"} and cp in {"gameswon", "gameswon", "totalgameswon", "matchgameswon"}:
        return "games_won"
    if sp == "MLB" and cp in {"totalbases"}:
        return "total_bases"
    return None


def resolve(path_str: str) -> Path | None:
    p = Path(path_str)
    if p.exists():
        return p
    for root in (REPO, MAIN):
        alt = root / "data/reports" / p.name
        if alt.exists():
            return alt
    return None


day_rows = []
tot = Counter()
pool_tot = Counter()
ticket_prop_legs = Counter()
all_ticket_props = Counter()

for fmeta in bt["tickets"]["files"]:
    path = resolve(fmeta["path"])
    date = fmeta["date"]
    if not path:
        print("MISSING", fmeta["path"])
        continue
    raw = json.loads(path.read_text(encoding="utf-8"))
    pool = raw.get("goblin_pool") or []
    tickets = raw.get("tickets") or []

    pool_counts = Counter()
    for leg in pool:
        if not isinstance(leg, dict):
            continue
        tag = classify(leg.get("sport"), leg.get("prop") or leg.get("prop_type"))
        if tag:
            pool_counts[tag] += 1
            pool_tot[tag] += 1
        # also sport|prop for top
        all_ticket_props  # noop keep linter calm

    # pool prop distribution for WNBA
    wnba_pool = Counter()
    for leg in pool:
        if str(leg.get("sport") or "").upper() == "WNBA":
            wnba_pool[str(leg.get("prop"))] += 1

    tix_n = 0
    tix_any = 0
    tix_tag = Counter()
    tix_wnba = 0
    tix_wnba_pra = 0
    legs_n = 0
    for t in tickets:
        legs = t.get("legs") or []
        if len(legs) < 2:
            continue
        tix_n += 1
        tags = set()
        sports = set()
        for leg in legs:
            sport = str(leg.get("sport") or "")
            sports.add(sport.upper())
            legs_n += 1
            tag = classify(sport, leg.get("prop"))
            if tag:
                tags.add(tag)
                ticket_prop_legs[tag] += 1
            all_ticket_props[(sport.upper(), str(leg.get("prop")))] += 1
        if tags & {"pra", "hitter_ks", "games_won"}:
            tix_any += 1
            tot["tickets_any_weak"] += 1
        for tag in tags:
            tix_tag[tag] += 1
            tot[f"tix_{tag}"] += 1
        if "WNBA" in sports:
            tix_wnba += 1
            tot["wnba_tix"] += 1
            if "pra" in tags:
                tix_wnba_pra += 1
                tot["wnba_pra_tix"] += 1
        tot["tickets"] += 1
        tot["legs"] += legs_n and 0  # fix below
    tot["legs"] += legs_n

    day_rows.append(
        {
            "date": date,
            "path": str(path),
            "pool_n": len(pool),
            "pool_pra": pool_counts["pra"],
            "pool_hks": pool_counts["hitter_ks"],
            "pool_gw": pool_counts["games_won"],
            "pool_tb": pool_counts["total_bases"],
            "tix": tix_n,
            "anyW": tix_any,
            "pra": tix_tag["pra"],
            "hks": tix_tag["hks"] if False else tix_tag["hitter_ks"],
            "gw": tix_tag["games_won"],
            "tb": tix_tag["total_bases"],
            "wnba": tix_wnba,
            "wPRA": tix_wnba_pra,
            "wnba_pool_top": wnba_pool.most_common(8),
            "wnba_pool_n": sum(wnba_pool.values()),
        }
    )

print("=== Per-day Goblin-70 published (resolved paths) ===")
print(f"{'date':12} {'pool':>5} {'praP':>4} {'hksP':>4} {'gwP':>3} {'tix':>4} {'anyW':>4} {'pra':>3} {'hks':>3} {'gw':>3} {'wnba':>4} {'wPRA':>4}")
for r in day_rows:
    print(
        f"{r['date']:12} {r['pool_n']:5d} {r['pool_pra']:4d} {r['pool_hks']:4d} {r['pool_gw']:3d} "
        f"{r['tix']:4d} {r['anyW']:4d} {r['pra']:3d} {r['hks']:3d} {r['gw']:3d} {r['wnba']:4d} {r['wPRA']:4d}"
    )

print()
n_days = len(day_rows) or 1
print("Averages / day:")
print(f"  tickets/day: {tot['tickets']/n_days:.1f}")
print(f"  tickets with any bundle-weak/day: {tot['tickets_any_weak']/n_days:.1f} ({tot['tickets_any_weak']/max(tot['tickets'],1):.1%} of tickets)")
print(f"  PRA tickets/day: {tot.get('tix_pra',0)/n_days:.1f}")
print(f"  hitter_ks tickets/day: {tot.get('tix_hitter_ks',0)/n_days:.1f}")
print(f"  games_won tickets/day: {tot.get('tix_games_won',0)/n_days:.1f}")
print(f"  WNBA tickets/day: {tot['wnba_tix']/n_days:.1f}; of which with PRA: {tot['wnba_pra_tix']/n_days:.1f} ({tot['wnba_pra_tix']/max(tot['wnba_tix'],1):.1%})")

print()
print("Pool share (goblin_pool across days):")
pool_n = sum(r["pool_n"] for r in day_rows)
for tag in ("pra", "hitter_ks", "games_won", "total_bases"):
    n = pool_tot[tag]
    print(f"  {tag}: {n}/{pool_n} = {n/max(pool_n,1):.1%} of published goblin_pool legs")

print()
print("Ticket leg share:")
for tag in ("pra", "hitter_ks", "games_won", "total_bases"):
    n = ticket_prop_legs[tag]
    print(f"  {tag}: {n}/{tot['legs']} ticket-legs = {n/max(tot['legs'],1):.1%}")

print()
print("WNBA pool composition (summed days with pool):")
# re-sum
wnba_all = Counter()
for r in day_rows:
    for prop, n in r["wnba_pool_top"]:
        wnba_all[prop] += n
    # incomplete - only top8 stored. Re-scan.
wnba_all = Counter()
wnba_n = 0
for r in day_rows:
    raw = json.loads(Path(r["path"]).read_text(encoding="utf-8"))
    for leg in raw.get("goblin_pool") or []:
        if str(leg.get("sport") or "").upper() == "WNBA":
            wnba_all[str(leg.get("prop"))] += 1
            wnba_n += 1
print(f"  WNBA pool legs total={wnba_n}")
for prop, n in wnba_all.most_common():
    marker = "  <<<" if classify("WNBA", prop) == "pra" else ""
    print(f"  {prop:20s} {n:4d} ({n/max(wnba_n,1):.1%}){marker}")

print()
print("Grade-universe share (current gate):")
ov = bt["grades"]["overall"]["l5eq5_l10ge8_sa_or_fga"]["n"]
for r in bt["grades"]["by_prop"]:
    if (r["sport"], r["prop"]) in {("WNBA", "pra"), ("MLB", "hitter_ks"), ("Tennis", "games_won"), ("MLB", "total_bases")}:
        c = r["l5eq5_l10ge8_sa_or_fga"]
        print(f"  {r['sport']}/{r['prop']}: n={c['n']} ({c['n']/ov:.1%}) hr={c['hr']:.1%}")

print()
print("Ticket prop freq (all sports) top 25:")
for k, n in all_ticket_props.most_common(25):
    print(f"  {n:3d} {k}")
