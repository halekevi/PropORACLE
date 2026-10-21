import json
from collections import Counter
from pathlib import Path

d = json.loads(Path(r"H:\PropORACLE\data\reports\goblin70_tickets_2026-09-24.json").read_text(encoding="utf-8"))
print("date", d.get("date"), "built", d.get("built_at"), "root", d.get("step8_root"))
print("pool", d.get("pool"))
rows = d.get("goblin_pool") or []
std = d.get("standard_pool") or []
print("goblin_pool", len(rows), "standard", len(std))
c = Counter()
for r in rows:
    c[(r.get("sport"), r.get("prop"), r.get("side"))] += 1
print("\n--- by sport/prop ---")
for k, n in sorted(c.items(), key=lambda x: (x[0][0] or "", -x[1], x[0][1] or "")):
    print(f"{n:3}  {k[0]:8} {k[2]:5} {k[1]}")
print("\n--- faces ---")
for r in rows:
    print(
        f"{str(r.get('sport')):7} {str(r.get('player')):24} {str(r.get('prop')):22} "
        f"{r.get('line')} L5={r.get('l5')} L10={r.get('l10')} {r.get('badge') or ''} "
        f"{r.get('matchup')} d={r.get('d')}"
    )
print("\n--- std ---")
for r in std:
    print(r.get("sport"), r.get("player"), r.get("prop"), r.get("side"), r.get("line"), r.get("badge"))
print("\n--- packed tickets ---")
for t in d.get("tickets") or []:
    print(t.get("id"), t.get("n_legs"), t.get("product"), t.get("mix"), t.get("n_correct"), t.get("payout_note"))
    for L in t.get("legs") or []:
        print("   ", L.get("player"), L.get("prop"), L.get("line"), L.get("matchup"))
