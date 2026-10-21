import json
from pathlib import Path
from datetime import datetime
root = Path(r"H:/PropORACLE_main_cp")
for p in [
    root / "outputs/2026-09-26/best_props_2026-09-26.xlsx",
    root / "data/reports/goblin70_tickets_2026-09-26.json",
    root / "ui_runner/templates/tickets_latest.json",
]:
    print(f"{p.name}: {datetime.fromtimestamp(p.stat().st_mtime):%H:%M:%S} ({p.stat().st_size} bytes)")

web = json.loads((root / "ui_runner/templates/tickets_latest.json").read_text(encoding="utf-8"))
print("web generated", web.get("generated_at"), "mode", web.get("mode"), "groups", len(web.get("groups") or []))
for g in web.get("groups") or []:
    name = g.get("group_name")
    for t in g.get("tickets") or []:
        legs = t.get("legs") or []
        bits = []
        for L in legs:
            pl = L.get("player") or L.get("Player")
            pr = L.get("prop") or L.get("Prop")
            side = L.get("direction") or L.get("side") or L.get("Direction") or ""
            line = L.get("line") or L.get("Line")
            sport = L.get("sport") or ""
            bits.append(f"{sport} {pl} {pr} {side}{line}".strip())
        track = t.get("ticket_track") or t.get("mode")
        print(f"  [{track}] {name}: " + " | ".join(bits))
