import json
from pathlib import Path

r = json.loads(Path(r"H:\PropORACLE\data\reports\prop_hit_rates_live_books_latest.json").read_text(encoding="utf-8"))
print(f"window {r['window']} method={r['method']}")
print("\n=== Tennis book rollups ===")
for x in r["rollups"]:
    if x["sport"] in ("Tennis", "ALL"):
        print(f"  {x['sport']:8} {x['book']:16} {x['hit_pct']:5}% n={x['n']:6}  CI{x['ci95']}")

tennis = [x for x in r["all_cells"] if x["sport"] == "Tennis"]
tennis.sort(key=lambda x: (x["book"], -(x["hit_pct"] or 0), -x["n"]))
print("\n=== All Tennis cells (ungated + gated cols) ===")
print(f"{'book':16} {'prop':22} {'hit%':>6} {'n':>6} {'CI':>18} {'L5=5%':>7} {'L5n':>5} {'L10>=8%':>8} {'L10n':>5} {'L5+L10%':>8} {'gaten':>5}")
for x in tennis:
    ci = f"[{x.get('ci95_lo')}, {x.get('ci95_hi')}]"
    print(
        f"{x['book']:16} {x['prop']:22} {x['hit_pct']:6}% {x['n']:6} {ci:>18} "
        f"{str(x.get('l5eq5_hit_pct')):>7} {str(x.get('l5eq5_n')):>5} "
        f"{str(x.get('l10ge8_hit_pct')):>8} {str(x.get('l10ge8_n')):>5} "
        f"{str(x.get('l5eq5_l10ge8_hit_pct')):>8} {str(x.get('l5eq5_l10ge8_n')):>5}"
    )
