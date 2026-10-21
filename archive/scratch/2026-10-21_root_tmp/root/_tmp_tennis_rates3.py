import json
from pathlib import Path

for name in ["tennis_keep_gates_restated.json", "tennis_serve_backfill_under_inflation.json", "tennis_next_cuts.json"]:
    p = Path(r"H:\PropORACLE\data\reports") / name
    d = json.loads(p.read_text(encoding="utf-8"))
    print(f"\n======== {name} ========")
    print(json.dumps(d, indent=2, ensure_ascii=True)[:8000])
