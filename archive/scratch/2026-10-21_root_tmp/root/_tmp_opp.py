import json, re
from pathlib import Path
from collections import Counter

MAIN = Path(r"H:\PropORACLE_main_cp")
REPO = Path(r"H:\PropORACLE")
keys = Counter()
samples = []
for d in [MAIN/"ui_runner"/"templates", REPO/"ui_runner"/"templates"]:
    if not d.exists(): continue
    for p in sorted(d.glob("graded_props_2026-08*.json"))[:3]:
        data = json.loads(p.read_text(encoding="utf-8"))
        rows = data if isinstance(data, list) else data.get("props") or data.get("rows") or []
        for r in rows:
            if str(r.get("sport","")).lower() != "tennis": continue
            for k in r.keys():
                if "opp" in k.lower() or "opponent" in k.lower() or "vs" in k.lower() or "rival" in k.lower():
                    keys[k] += 1
            if len(samples) < 5:
                samples.append({k:r.get(k) for k in ("player","prop","pick_type","opp","opp_team","opponent","opponent_name","vs","team") if k in r or True})
print("opp-like keys", keys)
for s in samples:
    print(s)
