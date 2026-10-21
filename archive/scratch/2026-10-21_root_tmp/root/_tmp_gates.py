import json
from pathlib import Path
r=json.loads(Path(r"H:\PropORACLE\data\reports\tennis_all_props_rank_stack.json").read_text(encoding="utf-8"))
print(f"legs={r['legs']} cells={len(r['cells'])}")
# Compact gate table: each cell's ungated + gates with n>=15
for c in r["cells"]:
    print(f"\n{c['book']} · {c['prop']}")
    for g in c["gates"]:
        b = g.get("base") or {}
        n = b.get("n") or 0
        if n < 15 and g.get("gate") != "ungated":
            continue
        lift = b.get("lift_vs_ungated_pp")
        ls = f" ({lift:+}pp)" if lift is not None else ""
        print(f"  {g['gate']:46} {b.get('hit_pct'):5}% n={n:4}{ls}")
