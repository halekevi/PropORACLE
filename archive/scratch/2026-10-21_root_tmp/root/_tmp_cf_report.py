"""Counterfactuals on goblin70 current-gate population from by_prop aggregates."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(r"H:\PropORACLE\data\reports")
d = json.loads((ROOT / "goblin70_strict_gate_backtest.json").read_text(encoding="utf-8"))
GATE = "l5eq5_l10ge8_sa_or_fga"
BASELINE = 0.7452

overall = d["grades"]["overall"][GATE]
base_n = overall["n"]
base_hits = overall["hits"]
base_hr = overall["hr"]

print("SOURCE: data/reports/goblin70_strict_gate_backtest.json")
print("Gate:", GATE, f"n={base_n} hits={base_hits} hr={base_hr:.4f} (=74.5% book)")
print("Note: file has grades aggregates only (no raw graded leg rows). Counterfactuals = overall minus by_prop cells.")
print()

props = d["grades"]["by_prop"]


def cell(row, gate=GATE):
    c = row.get(gate) or {}
    return int(c.get("n") or 0), int(c.get("hits") or 0), float(c.get("hr") or 0.0)


def find(sport, prop):
    for r in props:
        if r.get("sport") == sport and r.get("prop") == prop:
            return r
    return None


print("=== Current-gate by_prop (all) ===")
for r in sorted(props, key=lambda x: (-cell(x)[0], x["sport"], x["prop"])):
    n, h, hr = cell(r)
    if n == 0:
        continue
    print(f"  {r['sport']:7s} {r['prop']:22s} n={n:4d} hits={h:4d} hr={hr:.3f}")

print()


def cf(name: str, remove: list[tuple[str, str]]):
    rem_n = rem_h = 0
    parts = []
    for sport, prop in remove:
        r = find(sport, prop)
        if not r:
            parts.append(f"{sport}/{prop}=MISSING")
            continue
        n, h, hr = cell(r)
        rem_n += n
        rem_h += h
        parts.append(f"{sport}/{prop} n={n} hr={hr:.3f}")
    new_n = base_n - rem_n
    new_h = base_hits - rem_h
    new_hr = (new_h / new_n) if new_n else float("nan")
    delta = new_hr - BASELINE
    print(f"### {name}")
    print(f"  remove: {'; '.join(parts)}")
    print(f"  legs removed: {rem_n}")
    print(f"  remaining n:  {new_n}")
    print(f"  remaining hits:{new_h}")
    print(f"  new HR:       {new_hr:.4f} ({100*new_hr:.2f}%)")
    print(f"  delta vs 74.5%: {delta:+.4f} ({100*delta:+.2f} pp)")
    print(f"  clears 0.755 implement bar: {new_hr >= 0.755}")
    print()
    return new_n, new_hr, rem_n


cf("1. PRA-off", [("WNBA", "pra")])
cf("2. PRA-off + MLB total_bases off", [("WNBA", "pra"), ("MLB", "total_bases")])
cf(
    "2b. + MLB hitter_ks off (extra)",
    [("WNBA", "pra"), ("MLB", "total_bases"), ("MLB", "hitter_ks")],
)

# Premium sleeve: keep-only subset
premium = [
    ("WNBA", "fga"),
    ("WNBA", "reb+ast"),
    ("WNBA", "threes"),
    ("Tennis", "match_total_games"),
]
print("### 3. Premium sleeve ONLY (WNBA fga + reb+ast + threes; Tennis match_total_games)")
pn = ph = 0
for sport, prop in premium:
    r = find(sport, prop)
    if not r:
        print(f"  MISSING {sport}/{prop}")
        continue
    n, h, hr = cell(r)
    pn += n
    ph += h
    print(f"  keep {sport}/{prop}: n={n} hits={h} hr={hr:.3f}")
phr = (ph / pn) if pn else float("nan")
print(f"  remaining n (sleeve): {pn}")
print(f"  new HR:               {phr:.4f} ({100*phr:.2f}%)")
print(f"  delta vs 74.5%:       {phr - BASELINE:+.4f} ({100*(phr-BASELINE):+.2f} pp)")
print(f"  clears 0.80 tier bar: {phr >= 0.80}")
print()

# WNBA-only premium
print("### 3b. WNBA premium only (fga + reb+ast + threes)")
wn = wh = 0
for prop in ("fga", "reb+ast", "threes"):
    r = find("WNBA", prop)
    n, h, hr = cell(r)
    wn += n
    wh += h
    print(f"  {prop}: n={n} hr={hr:.3f}")
whr = wh / wn
print(f"  n={wn} HR={whr:.4f} ({100*whr:.2f}%) delta={whr-BASELINE:+.4f} clears0.80={whr>=0.80}")
print()

# PRA better gate: Off already in live code; check S/A-only residual for PRA
print("### PRA better-gate check (from same file)")
pra = find("WNBA", "pra")
for gname in ("l5eq5_l10ge8_cover", "l5eq5_l10ge8_sa", "l5eq5_l10ge8_sa_or_fga"):
    n, h, hr = cell(pra, gname)
    print(f"  WNBA PRA under {gname}: n={n} hits={h} hr={hr:.3f}")
# If we keep only S/A PRA inside current pool: remove (sa_or_fga PRA - sa PRA)
n_all, h_all, _ = cell(pra, "l5eq5_l10ge8_sa_or_fga")
n_sa, h_sa, hr_sa = cell(pra, "l5eq5_l10ge8_sa")
# Under sa_or_fga, PRA that aren't S/A shouldn't exist unless FGA exception somehow - for PRA, sa and sa_or_fga should match
print(f"  Note: PRA sa vs sa_or_fga identical? n_sa={n_sa} n_all={n_all} (FGA exception doesn't keep B-tier PRA)")
print(f"  Requiring S/A on PRA does NOT shrink current-gate PRA (already S/A or promoted). Residual HR stays {hr_sa:.3f} at n={n_sa}.")
print()

# Soft underperformers to optionally gate
print("### Other current-gate cells <0.72 with n>=20")
for r in sorted(props, key=lambda x: cell(x)[2]):
    n, h, hr = cell(r)
    if n >= 20 and hr < 0.72:
        print(f"  {r['sport']:7s} {r['prop']:22s} n={n:4d} hr={hr:.3f}")

print()
print("=== Prior reports ===")
cs = (ROOT / "cross_section_lift.json").read_text(encoding="utf-8")
combo = (ROOT / "prop_gate_combo_search.json").read_text(encoding="utf-8")
for label, blob in (("cross_section_lift.json", cs), ("prop_gate_combo_search.json", combo)):
    hits = []
    for needle in ("pra_off", "pra-off", "exclude_pra", "PRA-off", "without pra", "no_pra"):
        if needle.lower() in blob.lower():
            hits.append(needle)
    print(f"  {label}: PRA-off counterfactual strings found = {hits or 'NONE'}")

# WNBA FGA from cross_section if present
csj = json.loads(cs)
print()
print("=== cross_section WNBA fga / pra Goblin OVER key cells ===")
for row in csj.get("prop_cells") or []:
    if row.get("book") != "Goblin OVER":
        continue
    if row.get("sport") != "WNBA":
        continue
    if row.get("prop") not in {"pra", "fga", "reb+ast", "threes"}:
        continue
    print(f"  {row['prop']} open n={row.get('open_n')} hr={row.get('open_hr')}")
    for k in ("L5=5", "L5=5 + D", "L5=5 + cover", "L5=5 + D + cover", "L5>=4 + D", "L5>=4 + D + cover"):
        c = row.get(k)
        if isinstance(c, dict) and c.get("n"):
            print(f"    {k:24s} n={c['n']:4d} hr={c['hr']}")
