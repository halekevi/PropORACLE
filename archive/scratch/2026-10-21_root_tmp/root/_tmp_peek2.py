import json
from pathlib import Path
r=json.loads(Path(r"H:\PropORACLE\data\reports\tennis_keep_gates_rank_stack.json").read_text(encoding="utf-8"))
# We need slate dates on legs - not stored. Re-check: for ungated with both ranks, opp works via match window.
# Probe how many keep-eligible have match-window opp via a quick recount with date filter.
print("player coverage on GW keep:", r["gates"][0]["rank_coverage"])
print("by_player_tier GW keep:")
for x in r["gates"][0]["by_player_tier"]:
    if x["n"]: print(x)
