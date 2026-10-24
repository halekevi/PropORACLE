import json
from pathlib import Path

root = Path("data/reports")
tennis = json.loads((root / "discover_gates_tennis_ranked.json").read_text(encoding="utf-8"))
for g in tennis["ranked"]:
    if g.get("prop") == "games_won" and g.get("gate") == "L5=5 + D_elite":
        print("tennis split", g.get("oos_split_reliable"), g.get("oos_split_flags"), g.get("oos_n"), g.get("n"))
        break
soccer = json.loads((root / "discover_gates_soccer_ranked.json").read_text(encoding="utf-8"))
print("soccer dnp note", soccer.get("meta"))
for g in soccer["ranked"]:
    if g.get("prop") == "shots" and g.get("book") == "Goblin OVER":
        oos = g.get("oos_hr")
        if oos is not None and 0.64 <= float(oos) <= 0.72:
            print("shots", g.get("gate"), g.get("hits"), g.get("n"), oos, g.get("oos_n"), "base", g.get("base_hr"))
nfl = json.loads((root / "discover_gates_nfl_ranked.json").read_text(encoding="utf-8"))
print("nfl keys", [k for k in nfl if k != "ranked"][:20])
print("nfl n_gates", nfl.get("n_gates"), "cells", len(nfl.get("cells") or []))
