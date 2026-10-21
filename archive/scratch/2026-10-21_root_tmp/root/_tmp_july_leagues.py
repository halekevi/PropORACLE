from pathlib import Path
import pandas as pd

ROOT = Path(r"H:/PropORACLE/outputs")
print("june dirs:", len(list(ROOT.glob("2026-06-*"))))
for d in sorted(ROOT.glob("2026-07-1*")) + sorted(ROOT.glob("2026-06-*"))[:5]:
    p = d / "soccer" / "step1_soccer_props.csv"
    if not p.exists():
        print(d.name, "no step1")
        continue
    if p.stat().st_size < 50:
        print(d.name, "empty step1 size", p.stat().st_size)
        continue
    try:
        df = pd.read_csv(p, low_memory=False)
    except Exception as e:
        print(d.name, "read fail", e)
        continue
    if "league" not in df.columns:
        print(d.name, "no league col", list(df.columns)[:8])
        continue
    vc = df["league"].astype(str).str.upper().value_counts()
    print(d.name, "rows", len(df), dict(vc.head(6)))
