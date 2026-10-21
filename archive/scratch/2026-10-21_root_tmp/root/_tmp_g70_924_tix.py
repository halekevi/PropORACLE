import json
from pathlib import Path

d = json.loads(Path(r"H:\PropORACLE\data\reports\goblin70_tickets_2026-09-24.json").read_text(encoding="utf-8"))
out = []
for t in d.get("tickets") or []:
    out.append({
        "id": t.get("id"),
        "n": t.get("n_legs"),
        "product": t.get("product"),
        "mix": t.get("mix"),
        "n_correct": t.get("n_correct"),
        "note": t.get("payout_note"),
        "legs": [
            {
                "player": L.get("player"),
                "prop": L.get("prop"),
                "line": L.get("line"),
                "sport": L.get("sport"),
                "matchup": L.get("matchup"),
                "badge": L.get("badge"),
            }
            for L in (t.get("legs") or [])
        ],
    })
Path(r"H:\PropORACLE\_tmp_g70_924_tickets.json").write_text(
    json.dumps(out, indent=2, ensure_ascii=True), encoding="utf-8"
)
print("wrote", len(out), "tickets")
