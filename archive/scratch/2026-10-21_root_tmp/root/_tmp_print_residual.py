import pandas as pd

df = pd.read_csv(r"Sports/NFL/data/nfl_gated_residual_under70_week1_2.csv")
df = df.sort_values(["pick", "side", "hr", "n"], ascending=[True, True, True, False])
print("NOT CLEARING after soft gates — complete list")
print(f"n={len(df)}\n")
for pick in ("Standard", "Goblin"):
    for side in ("UNDER", "OVER"):
        sub = df[(df.pick == pick) & (df.side == side)]
        if sub.empty:
            continue
        print(f"## {pick} {side} ({len(sub)})")
        for _, r in sub.iterrows():
            print(
                f"  {r.prop:40s}  {int(r.hits):3d}/{int(r.n):<3d} = {r.hr:5.1f}%"
                f"   base {int(r.base_hits)}/{int(r.base_n)}={r.base_hr:.0f}%"
                f"   | {r.gate}"
            )
        print()
