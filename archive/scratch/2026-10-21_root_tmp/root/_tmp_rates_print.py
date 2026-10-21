import json
from pathlib import Path
from collections import defaultdict
r = json.loads(Path(r"H:\PropORACLE\data\reports\prop_hit_rates_live_books_latest.json").read_text(encoding="utf-8"))
print(f"window {r['window']} decided={r['decided_legs']:,} files={r['files']}")
print("\n=== ALL / sport rollups ===")
for x in r["rollups"]:
    print(f"  {x['sport']:8} {x['book']:16} {x['hit_pct']:5}% n={x['n']:6}  CI{x['ci95']}")

print("\n=== Best gated L5=5+L10>=8 (n>=25) ===")
for x in r["best_l5eq5_l10ge8_n25"][:30]:
    g = x.get("l5eq5_l10ge8_hit_pct")
    gn = x.get("l5eq5_l10ge8_n")
    print(f"  gated {g:5}% n={gn:4} | raw {x['hit_pct']:5}% n={x['n']:5} | {x['sport']:8} {x['book']:16} {x['prop']}")

print("\n=== Top ungated n>=40 ===")
for x in r["top_n40"][:20]:
    print(f"  {x['hit_pct']:5}% n={x['n']:5} {x['sport']:8} {x['book']:16} {x['prop']}")

print("\n=== Bottom ungated n>=40 ===")
for x in r["bottom_n40"][:20]:
    print(f"  {x['hit_pct']:5}% n={x['n']:5} {x['sport']:8} {x['book']:16} {x['prop']}")

# Sport book summary from all_cells weighted
print("\n=== Per-sport book summary (weighted from cells) ===")
agg = defaultdict(lambda: [0,0])
for x in r["all_cells"]:
    key = (x["sport"], x["book"])
    agg[key][0] += x["hits"]
    agg[key][1] += x["n"]
for (sp,bk), (h,n) in sorted(agg.items()):
    pct = round(100*h/n,1) if n else None
    print(f"  {sp:8} {bk:16} {pct:5}% n={n:6}")
