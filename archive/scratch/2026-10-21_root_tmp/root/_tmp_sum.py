import json
from pathlib import Path
r=json.loads(Path(r"H:\PropORACLE\data\reports\tennis_keep_gates_rank_stack.json").read_text(encoding="utf-8"))
for g in r["gates"]:
    if "ungated" in g["gate"]:
        continue
    b=g["base"]
    cov=g["rank_coverage"]
    print(f"\n=== {g['gate']} ===")
    print(f"  base {b['hit_pct']}% n={b['n']} CI{b['ci95']} | both_ranks={cov['both_n']} ({cov['pct_both']}%)")
    print("  by player_tier:")
    for x in g["by_player_tier"]:
        if x["n"]>=10: print(f"    {x['bucket']:12} {x['hit_pct']:5}% n={x['n']:4} CI{x['ci95']}")
    print("  by opp_tier:")
    for x in g["by_opp_tier"]:
        if x["n"]>=10: print(f"    {x['bucket']:12} {x['hit_pct']:5}% n={x['n']:4} CI{x['ci95']}")
    print("  by rank_gap:")
    for x in g["by_rank_gap"]:
        if x["n"]>=10: print(f"    {x['bucket']:22} {x['hit_pct']:5}% n={x['n']:4}")
    print("  stacks n>=20:")
    for x in g["stacks"]:
        if x["n"]>=20:
            print(f"    {x['stack']:36} {x['hit_pct']:5}% n={x['n']:4} lift={x['lift_pp']:+}pp")
