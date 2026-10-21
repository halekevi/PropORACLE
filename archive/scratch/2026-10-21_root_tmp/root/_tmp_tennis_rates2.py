import json
from pathlib import Path

p = Path(r"H:\PropORACLE\data\reports\tennis_board_category_hit_rates.json")
d = json.loads(p.read_text(encoding="utf-8"))
print("generated", d.get("generated_at"))
print("window", d.get("method", {}).get("window"))
print("legs", d.get("method", {}).get("legs"), "decided", d.get("method", {}).get("decided"))
print()
for prop in d["props"]:
    name = prop.get("prop") or prop.get("name")
    print(f"### {name}")
    for k, v in prop.items():
        if k in ("prop", "name"):
            continue
        if isinstance(v, dict) and ("hit_pct" in v or "n" in v or "rate" in v):
            print(f"  {k}: {v}")
        elif isinstance(v, list):
            print(f"  {k}:")
            for item in v[:20]:
                if isinstance(item, dict):
                    slim = {kk: item[kk] for kk in item if kk in ("book","side","label","gate","n","hits","hit_pct","ci95","ci95_lo","ci95_hi","path","note")}
                    print("   ", slim if slim else item)
                else:
                    print("   ", item)
        elif not isinstance(v, (dict, list)):
            print(f"  {k}: {v}")
    print()
