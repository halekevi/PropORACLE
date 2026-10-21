import json
from pathlib import Path
r=json.loads(Path(r"H:\PropORACLE\data\reports\tennis_all_props_rank_stack.json").read_text(encoding="utf-8"))
print(f"legs={r['legs']} cells={len(r['cells'])}")
print("BOOK ROLLUPS")
for x in r["book_rollups"]:
    print(f"  {x['label']:16} {x['hit_pct']:5}% n={x['n']}")
print("\nBASE by prop x book")
for c in r["cells"]:
    b=c["base"]; cov=c["rank_coverage"]
    print(f"  {c['book']:16} {c['prop']:22} {b['hit_pct']:5}% n={b['n']:5} both={cov['pct_both']}%")

# Notable lifts: |lift|>=5 and n>=40
print("\nNOTABLE STACKS (|lift|>=5, n>=40)")
notable=[]
for c in r["cells"]:
    base=c["base"]["hit_pct"]
    for s in c["stacks"]:
        if s["n"]>=40 and s["lift_pp"] is not None and abs(s["lift_pp"])>=5:
            notable.append((s["lift_pp"], c["book"], c["prop"], s["stack"], s["hit_pct"], s["n"], base))
notable.sort(key=lambda t: -abs(t[0]))
for lift, bk, prop, stack, hit, n, base in notable[:40]:
    print(f"  {lift:+5.1f}pp  {hit:5}% n={n:4}  {bk:16} {prop:18} {stack}  (base {base}%)")
