import pandas as pd
from pathlib import Path

p = Path(r"H:\PropORACLE\data\reports\soccer_combo_gates_latest.xlsx")
df = pd.read_excel(p, "Combo HR Std+Goblin")
board = [
    "Passes Attempted",
    "Shots",
    "Shots On Target",
    "Goalie Saves",
    "Goals",
    "Clearances",
    "Attempted Dribbles",
    "Shots Assisted",
    "Tackles",
    "Crosses",
    "Goal + Assist",
    "Assists",
    "Fouls",
]


def cell(r, g):
    n = r.get(g + "_n")
    h = r.get(g + "_hits")
    hr = r.get(g + "_hr")
    if pd.isna(n) or int(n) == 0:
        return "—"
    return f"{int(h)}/{int(n)}={100 * float(hr):.1f}%"


print("BOARD PROP | pool | ungated | L5>=4 | L5=5 | L5=5+L10 | status")
matched = set()
for name in board:
    key = name.casefold()
    hit = None
    for prop in df["prop"]:
        pk = str(prop).strip().casefold()
        if pk == key or key in pk or pk in key:
            hit = prop
            break
    if hit is None:
        print(f"{name} | MISSING from workbook")
        continue
    matched.add(str(hit).casefold())
    r = df[df.prop == hit].iloc[0]
    print(
        f"{hit} | n={int(r.n_board)} | {cell(r,'all')} | {cell(r,'L5>=4')} | "
        f"{cell(r,'L5=5')} | {cell(r,'L5=5+L10')}"
    )

print()
print("Also in workbook, not on this board strip:")
for _, r in df.iterrows():
    if str(r.prop).casefold() not in matched:
        print(f"  {r.prop} | n={int(r.n_board)} | {cell(r,'all')}")
