import json
from pathlib import Path
p = Path(r"H:\PropORACLE_main_cp\ui_runner\templates\graded_props_2026-10-08.json")
raw = json.loads(p.read_text(encoding="utf-8"))
n = 0
for g in raw.get("props") or []:
    if "tennis" not in str(g.get("sport") or "").lower():
        continue
    if "game" not in str(g.get("prop") or "").lower():
        continue
    print({k: g.get(k) for k in ("player", "prop", "line", "result", "actual", "actual_value", "stat", "opp_team")})
    n += 1
    if n >= 4:
        break
print("shown", n)
