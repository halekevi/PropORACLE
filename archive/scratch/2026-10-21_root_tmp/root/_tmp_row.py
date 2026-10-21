import json
from pathlib import Path
p=Path(r"H:\PropORACLE_main_cp\ui_runner\templates\graded_props_2026-08-28.json")
rows=json.loads(p.read_text(encoding="utf-8"))
if isinstance(rows,dict):
    rows=rows.get("props") or rows.get("rows") or []
ten=[r for r in rows if str(r.get("sport","")).lower()=="tennis"][0]
for k in sorted(ten.keys()):
    v=ten[k]
    if v not in (None,"",[],{}):
        print(f"{k}={v!r}"[:160])
