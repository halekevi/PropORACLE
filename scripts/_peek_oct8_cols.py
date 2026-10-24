from pathlib import Path
import pandas as pd

day = Path(r"H:\PropORACLE_main_cp\outputs\2026-10-08")
frames = {
    "mlb": day / "mlb" / "step8_mlb_direction.csv",
    "soccer": day / "soccer" / "step8_soccer_direction.csv",
    "nfl": day / "nfl" / "step8_nfl_direction_clean.xlsx",
    "nhl": day / "nhl" / "step1_nhl_props.csv",
}
for name, path in frames.items():
    df = pd.read_excel(path, dtype=str) if path.suffix.lower().startswith(".xls") else pd.read_csv(path, dtype=str, nrows=3)
    cols = list(df.columns)
    print(name, "ncols", len(cols))
    interesting = [c for c in cols if any(k in c.lower() for k in ("pick", "prop", "team", "opp", "l5", "side", "dir", "goblin", "odds"))]
    print(" ", interesting[:25])
    if len(df):
        row = df.iloc[0]
        for c in interesting[:12]:
            print("   ", c, "=", row.get(c))
