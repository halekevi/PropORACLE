"""Coverage of on_ticket / ticket_id in May–June graded_props."""
from __future__ import annotations

import json
from pathlib import Path

templates = Path(r"H:\PropORACLE\ui_runner\templates")
print("date       | props | on_ticket | tids | tid_any | on_w_ml")
for p in sorted(templates.glob("graded_props_2026-0[56]*.json")):
    props = json.loads(p.read_text(encoding="utf-8")).get("props") or []
    on = [
        r
        for r in props
        if r.get("on_ticket") is True or str(r.get("on_ticket")).lower() == "true"
    ]
    tids = {r.get("ticket_id") for r in on if r.get("ticket_id")}
    with_tid = [r for r in props if r.get("ticket_id")]
    mlp = sum(1 for r in on if r.get("ml_prob") not in (None, ""))
    print(
        f"{p.stem[-10:]} | {len(props):5d} | {len(on):4d} | {len(tids):4d} | "
        f"{len(with_tid):4d} | {mlp:4d}"
    )
