"""Why soccer and tennis Goblins fail the ticket gate on 2026-10-08."""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from build_goblin70_tickets import load_today_board  # noqa: E402
from utils.ticket_70_pool import directional_l5, directional_l10  # noqa: E402

rows = load_today_board("2026-10-08")
for sport in ("SOCCER", "TENNIS", "NFL", "GOLF"):
    gob = [r for r in rows if str(r.get("sport")) == sport and str(r.get("pick_type")) == "Goblin" and str(r.get("side")).upper() == "OVER"]
    l5s = [directional_l5(r) for r in gob]
    l5_5 = sum(1 for v in l5s if v is not None and v >= 5)
    missing = sum(1 for v in l5s if v is None)
    print(sport, "goblin", len(gob), "l5 missing", missing, "l5>=5", l5_5, "l10 sample", [directional_l10(r) for r in gob[:3]])
nfl_picks = Counter(str(r.get("pick_type")) for r in rows if str(r.get("sport")) == "NFL")
print("NFL pick types", dict(nfl_picks))
