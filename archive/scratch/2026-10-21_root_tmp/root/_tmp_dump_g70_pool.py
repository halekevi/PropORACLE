#!/usr/bin/env python3
import json
from pathlib import Path

r = json.loads(
    Path(r"H:/PropORACLE_main_cp/data/reports/goblin70_tickets_2026-09-19.json").read_text(
        encoding="utf-8"
    )
)
print("pool", r.get("pool"))
print()
print("GOBLIN POOL")
for x in r.get("goblin_pool") or []:
    print(
        "\t".join(
            str(v)
            for v in (
                x.get("sport"),
                x.get("player"),
                x.get("prop") or x.get("prop_type"),
                x.get("direction") or "OVER",
                x.get("line"),
                x.get("pick_type"),
                f"L5={x.get('l5') or x.get('l5_hits')}",
                f"badge={x.get('badge') or x.get('tier')}",
            )
        )
    )
print()
print("STANDARD POOL")
for x in r.get("standard_pool") or []:
    print(
        "\t".join(
            str(v)
            for v in (
                x.get("sport"),
                x.get("player"),
                x.get("prop") or x.get("prop_type"),
                x.get("direction"),
                x.get("line"),
                x.get("pick_type"),
            )
        )
    )
