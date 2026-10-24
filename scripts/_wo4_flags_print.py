import json
from pathlib import Path

p = json.loads(Path("data/reports/wo4_discovery_flags.json").read_text(encoding="utf-8"))
for sport in ("Tennis", "Soccer", "NHL", "WNBA", "NFL", "CFB"):
    block = p.get(sport) or {}
    print("==", sport, "combos", block.get("combinations_searched"), "n", block.get("n_rows_unique"))
    for g in (block.get("top") or [])[:8]:
        print(
            f"  {g['book']} {g['prop']} {g['gate']} {g.get('hits')}/{g['n']} "
            f"hr={g['hr']} base={g['base_hr']} oos={g['oos_hr']}/{g['oos_n']} "
            f"be2={g['break_even_2leg']} be3={g['break_even_3leg']} {g['unreliable_reasons']}"
        )
