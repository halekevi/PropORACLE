import json
from pathlib import Path
from utils.nfl_keep_gates import NFL_GOBLIN_TICKETS_ENABLED

w = json.loads(Path("ui_runner/runtime/tickets_latest.json").read_text(encoding="utf-8"))
print("live date", w.get("date"), "mode", w.get("mode"))
print("NFL_GOBLIN_TICKETS_ENABLED", NFL_GOBLIN_TICKETS_ENABLED)
nfl = []
for g in w.get("groups") or []:
    for t in g.get("tickets") or []:
        for leg in t.get("legs") or []:
            if str(leg.get("sport") or "").upper() == "NFL":
                nfl.append(
                    (
                        g.get("group_name"),
                        leg.get("player"),
                        leg.get("prop_type") or leg.get("prop"),
                        leg.get("direction"),
                        leg.get("line"),
                        leg.get("pick_type"),
                    )
                )
print("nfl legs on /tickets", len(nfl))
for x in nfl:
    print(" ", x)

g = json.loads(
    Path("data/reports/goblin70_tickets_2026-09-27.json").read_text(encoding="utf-8")
)
sp = g.get("standard_pool") or []
sy = g.get("standard_yolo_pool") or []
nfl_std = [r for r in sp if str(r.get("sport")).upper() == "NFL"]
nfl_y = [r for r in sy if str(r.get("sport")).upper() == "NFL"]
print("G70 standard_pool NFL", len(nfl_std))
for r in nfl_std[:20]:
    print(
        f"  {r.get('player')} {r.get('prop')} {r.get('side')}{r.get('line')} "
        f"L5={r.get('l5')} L10={r.get('l10')} D={r.get('d')}"
    )
print("G70 standard_yolo NFL", len(nfl_y))
for r in nfl_y[:12]:
    print(f"  {r.get('player')} {r.get('prop')} {r.get('side')}{r.get('line')}")
gob = [r for r in (g.get("goblin_pool") or []) if str(r.get("sport")).upper() == "NFL"]
print("G70 goblin_pool NFL", len(gob))

# tickets that include NFL
for t in g.get("tickets") or []:
    legs = t.get("legs") or []
    if any(str(x.get("sport") or "").upper() == "NFL" for x in legs):
        print(
            "ticket",
            t.get("id"),
            t.get("product"),
            [
                f"{x.get('sport')} {x.get('player')} {x.get('prop')} {x.get('side')}{x.get('line')}"
                for x in legs
                if str(x.get("sport") or "").upper() == "NFL"
            ],
        )
