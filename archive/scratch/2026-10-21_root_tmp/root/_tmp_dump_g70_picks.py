#!/usr/bin/env python3
import json
from pathlib import Path

p = Path(r"H:/PropORACLE_main_cp/ui_runner/templates/tickets_latest.json")
d = json.loads(p.read_text(encoding="utf-8"))
print(f"date={d.get('date')} generated={d.get('generated_at')}")
print(f"groups={len(d.get('groups') or [])}")
print()

for g in d.get("groups") or []:
    name = str(g.get("group_name") or "")
    print(f"## {name}")
    for ti, t in enumerate(g.get("tickets") or [], 1):
        mode = t.get("mode") or t.get("ticket_type") or t.get("bet_type") or ""
        nc = t.get("n_correct_pays") or t.get("n_correct") or t.get("sweep_payout_x") or ""
        print(f"  SLIP {ti} | {mode} | n_correct={nc}")
        for leg in t.get("legs") or []:
            sport = leg.get("sport") or ""
            player = leg.get("player") or leg.get("player_name") or ""
            prop = leg.get("prop_type") or leg.get("prop") or ""
            side = (
                leg.get("direction")
                or leg.get("bet_direction")
                or leg.get("side")
                or ""
            )
            line = leg.get("line")
            pick = leg.get("pick_type") or ""
            print(f"    {sport}\t{player}\t{prop}\t{side}\t{line}\t{pick}")
    print()

# Also dump G70 seed pool from report
rep = Path(r"H:/PropORACLE_main_cp/data/reports/goblin70_tickets_2026-09-19.json")
if rep.exists():
    r = json.loads(rep.read_text(encoding="utf-8"))
    print("=== REPORT KEYS", list(r.keys())[:20])
    for key in ("goblin_seeds", "seeds", "goblin", "standard", "tickets"):
        if key in r:
            val = r[key]
            print(f"=== {key} type={type(val).__name__} n={len(val) if hasattr(val, '__len__') else '?'}")
