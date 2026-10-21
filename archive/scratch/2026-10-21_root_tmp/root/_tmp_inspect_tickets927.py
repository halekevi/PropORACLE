import json
from pathlib import Path

d = json.loads(Path(r"H:/PropORACLE/data/reports/goblin70_tickets_2026-09-27.json").read_text(encoding="utf-8"))
print("keys", list(d)[:30])
print("n tickets", len(d.get("tickets") or []))
print("n groups", len(d.get("groups") or []))
for t in (d.get("tickets") or [])[:20]:
    print(" ", t.get("id") or t.get("ticket_id"), t.get("family"), len(t.get("legs") or []))

w = json.loads(Path(r"H:/PropORACLE/ui_runner/runtime/tickets_latest.json").read_text(encoding="utf-8"))
print("WEB mode", w.get("mode"), "date", w.get("date"), "groups", len(w.get("groups") or []))
for g in w.get("groups") or []:
    print("GROUP", g.get("group_name"), "n=", len(g.get("tickets") or []))
    for t in g.get("tickets") or []:
        legs = []
        for x in t.get("legs") or []:
            legs.append(
                f"{x.get('sport')} {x.get('player')} "
                f"{x.get('direction') or x.get('side')}{x.get('line')}"
            )
        print(" ", t.get("ticket_id") or t.get("ticket_no"), " | ".join(legs))
