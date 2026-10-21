import sys
from pathlib import Path
sys.path[:0] = [r"H:\PropORACLE", r"H:\PropORACLE\scripts"]
import pandas as pd
from utils.prop_norm import canon_prop
from utils.nfl_keep_gates import (
    NFL_GOBLIN_TICKETS_ENABLED,
    NFL_GOBLIN_OVER_KEEP_FLIP,
    nfl_goblin_ticket_eligible,
)
from utils.ticket_70_pool import goblin_70_eligible
from utils.competition_history import row_competition_ok

print("flag", NFL_GOBLIN_TICKETS_ENABLED)
path = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-27/nfl/step8_nfl_direction_clean.xlsx")
df = pd.read_excel(path)
print("rows", len(df))
# sample goblin wave1
n = 0
ok = 0
for i, r in df.iterrows():
    if "goblin" not in str(r.get("Pick Type") or "").lower():
        continue
    if str(r.get("Direction") or "").upper() != "OVER":
        continue
    prop = canon_prop("NFL", str(r.get("Prop") or ""))
    if prop not in NFL_GOBLIN_OVER_KEEP_FLIP:
        continue
    n += 1
    rec = {
        "sport": "NFL",
        "league": str(r.get("League") or "NFL"),
        "pick_type": "Goblin",
        "side": "OVER",
        "prop": prop,
        "player": r.get("Player"),
        "line": r.get("Line"),
        "l5_over": r.get("L5 Over"),
        "l10_over": r.get("L10 Over"),
        "def_tier": r.get("Def Tier"),
        "cover": r.get("Cover") or r.get("cover"),
        "l5_sample_n": r.get("l5_sample_n") or r.get("L5 Sample N") or r.get("games_used"),
        "l5_window_n": r.get("l5_window_n") or r.get("L5 Window N"),
        "season_l5_flag": r.get("season_l5_flag") or r.get("Season L5 Flag"),
        "date": "2026-09-27",
    }
    ge = goblin_70_eligible(rec)
    ne = nfl_goblin_ticket_eligible(rec)
    if ge:
        ok += 1
        if ok <= 12:
            print(
                "OK",
                rec["player"],
                prop,
                rec["line"],
                "L5",
                rec["l5_over"],
                "L10",
                rec["l10_over"],
                "D",
                rec["def_tier"],
                "comp",
                row_competition_ok(rec),
            )
    elif n <= 8 or (ne and n <= 40):
        if n <= 15:
            print(
                "FAIL",
                rec["player"],
                prop,
                rec["line"],
                "L5",
                rec["l5_over"],
                "L10",
                rec["l10_over"],
                "D",
                rec["def_tier"],
                "nfl_gob",
                ne,
                "comp",
                row_competition_ok(rec),
                "samp",
                rec["l5_sample_n"],
                "win",
                rec["l5_window_n"],
                "flag",
                rec["season_l5_flag"],
            )
print("wave1 goblin OVER rows", n, "g70_ok", ok)
