import json
from pathlib import Path

d = json.loads(
    Path(r"H:\PropORACLE_main_cp\data\reports\goblin70_tickets_2026-09-23.json").read_text(
        encoding="utf-8"
    )
)
print("id name n product mix n_correct note")
for t in d.get("tickets") or []:
    print(t.get("id"), t.get("n_legs"), t.get("product"), t.get("mix"), t.get("n_correct"), t.get("payout_note"))
    for L in t.get("legs") or []:
        print(
            "  ",
            L.get("player"),
            L.get("prop"),
            L.get("line"),
            "|",
            L.get("matchup"),
            L.get("badge"),
        )
    print()
print("--- pool ---")
for r in d.get("goblin_pool") or []:
    print(
        f"{r.get('sport'):7} {str(r.get('player')):22} {str(r.get('prop')):22} "
        f"{r.get('line')} {str(r.get('badge') or ''):9} {r.get('matchup')} "
        f"l5={r.get('l5')} l10={r.get('l10')}"
    )
