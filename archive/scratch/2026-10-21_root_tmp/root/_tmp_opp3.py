import json
from pathlib import Path
from collections import defaultdict

p = Path(r"H:\PropORACLE_main_cp\ui_runner\templates\graded_props_2026-08-28.json")
if not p.exists():
    p = next(Path(r"H:\PropORACLE_main_cp\ui_runner\templates").glob("graded_props_2026-08*.json"))
print("file", p)
data=json.loads(p.read_text(encoding="utf-8"))
rows=data if isinstance(data,list) else data.get("props") or data.get("rows") or []
ten=[r for r in rows if str(r.get("sport","")).lower()=="tennis"]
print("tennis", len(ten))
print("keys sample", sorted(ten[0].keys())[:40])
# game id pairing
by_game=defaultdict(set)
for r in ten:
    gid=r.get("pp_game_id") or r.get("game_id") or r.get("event_id")
    by_game[str(gid or "")].add(str(r.get("player") or ""))
multi={k:v for k,v in by_game.items() if k and len(v)>=2}
print("games with 2+ players", len(multi), "of", len(by_game))
# show 3
for i,(k,v) in enumerate(list(multi.items())[:3]):
    print(k, v)
# check description
for r in ten[:3]:
    print("desc", (r.get("pp_description") or r.get("description") or "")[:120], "game", r.get("pp_game_id"))
