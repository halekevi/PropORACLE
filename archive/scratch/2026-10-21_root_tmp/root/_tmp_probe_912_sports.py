import json
from collections import Counter
from pathlib import Path

p = Path(r"H:\PropORACLE_main_cp\ui_runner\templates\graded_props_2026-09-12.json")
raw = json.loads(p.read_text(encoding="utf-8"))
props = raw.get("props") or []
print("n", len(props), "keys", list(raw)[:8])
print("sport", Counter(str(r.get("sport") or "") for r in props[:]).most_common(20))
# also league
print("sample sports nonempty", [r.get("sport") for r in props if r.get("sport")][:10])
cfb = [r for r in props if "cfb" in str(r.get("sport") or "").lower() or "ncaaf" in str(r.get("sport") or "").lower() or str(r.get("league") or "") in ("15", "CFB")]
print("cfb-like", len(cfb))
if cfb:
    print(cfb[0].keys())
    print({k: cfb[0].get(k) for k in ("sport", "league", "player", "prop", "result", "l5_over", "last5_over")})
