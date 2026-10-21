import json
from pathlib import Path
from collections import Counter

REPO = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")

def check(month):
    filled=0; empty=0; ranks=0
    for d in [MAIN/"ui_runner"/"templates", REPO/"ui_runner"/"templates"]:
        if not d.exists(): continue
        for p in d.glob(f"graded_props_{month}*.json"):
            data=json.loads(p.read_text(encoding="utf-8"))
            rows=data if isinstance(data,list) else data.get("props") or data.get("rows") or []
            for r in rows:
                if str(r.get("sport","")).lower()!="tennis": continue
                ot=str(r.get("opp_team") or "").strip()
                if ot: filled+=1
                else: empty+=1
                if r.get("opponent_rank") or r.get("opp_rank") or r.get("player_rank") or r.get("player_atp_rank"):
                    ranks+=1
    print(month, "opp_team filled", filled, "empty", empty, "row_rank_fields", ranks)

for m in ["2026-03","2026-05","2026-07","2026-08","2026-09"]:
    check(m)

# step8 sample columns
for p in [
    REPO/"Sports"/"Tennis"/"step8_tennis_direction.csv",
    MAIN/"outputs"/"2026-08-28"/"tennis"/"step8_tennis_direction_clean.xlsx",
]:
    print("exists", p, p.exists())
