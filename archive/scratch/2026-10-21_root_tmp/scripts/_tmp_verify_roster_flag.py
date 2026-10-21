import sys
from pathlib import Path

sys.path[:0] = [r"H:\PropORACLE", r"H:\PropORACLE\scripts"]
import rank_best_props_today as R
from utils.leg_flags import collect_leg_flags

df = R.load_nba(Path(r"H:\PropORACLE"), "2026-10-20")
print("rows", len(df), "roster_changed", int(df["roster_changed"].sum()))
sub = df[
    df["player"]
    .astype(str)
    .str.contains(
        "Tatum|Mitchell Robinson|Jaylen Brown|Paul George|Pritchard|Queta|Walsh|Hauser",
        case=False,
        na=False,
        regex=True,
    )
]
sub = sub[~sub["player"].astype(str).str.contains(r"\+", na=False)]
for p, g in sub.groupby("player"):
    r = g.iloc[0]
    print(
        f"{p:22} team={r.team:3} changed={int(r.roster_changed)} "
        f"note={str(r.roster_note)[:110]}"
    )
    print(f"  new_teammates={str(r.new_teammates)[:90]}")
    flags = collect_leg_flags(
        values=[1, 2, 3, 4, 5],
        line=10.5,
        side="UNDER",
        pick="Standard",
        sport="NBA",
        prop="x",
        row=dict(r),
    )
    print(f"  flags={flags}")

rows = R.recs(df)
print("\nSample formatted Celtics L5=5:")
n = 0
for r in rows:
    if r.get("team") != "BOS" or not r.get("roster_changed"):
        continue
    if "+" in str(r.get("player") or ""):
        continue
    side = r.get("side") or "UNDER"
    if side == "UNDER" and r.get("l5_under") != 5:
        continue
    if side == "OVER" and r.get("l5_over") != 5:
        continue
    print(R._fmt(r, side)[:220])
    n += 1
    if n >= 10:
        break
