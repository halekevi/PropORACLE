"""Find MLB Goblin OVER L5=5 keep-gated faces still live on PP board."""
from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
from playwright.sync_api import sync_playwright
import collect_payout_data as P
from utils.pp_manual_slip import react_search, _norm
from utils.mlb_keep_gates import mlb_goblin_over_keep

p = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-25/mlb/step8_mlb_direction_clean.xlsx")
df = pd.read_excel(p)

def g(r, *names):
    for n in names:
        if n in r.index: return r[n]
        for c in r.index:
            if str(c).lower().replace(" ","_") == n.lower().replace(" ","_"):
                return r[c]
    return None

rows = []
for _, r in df.iterrows():
    pt = str(g(r, "Pick Type") or "")
    if "goblin" not in pt.lower(): continue
    if str(g(r, "Direction") or "").upper() != "OVER": continue
    l5 = pd.to_numeric(g(r, "L5 Over"), errors="coerce")
    if pd.isna(l5) or float(l5) < 5: continue
    rec = {
        "sport": "MLB",
        "player": str(g(r, "Player") or ""),
        "prop": str(g(r, "Prop") or ""),
        "prop_type": str(g(r, "Prop") or ""),
        "line": float(g(r, "Line")),
        "direction": "OVER",
        "pick_type": "goblin",
        "l5_over": int(float(l5)),
        "l10_over": float(pd.to_numeric(g(r, "L10 Over"), errors="coerce") or 0),
        "def_tier": str(g(r, "Def Tier") or ""),
        "ba": g(r, "BA", "Avg", "Season BA"),
        "k_pct": g(r, "K%", "K Pct", "K_Pct"),
    }
    # map to keep-gate fields if needed
    try:
        ok = mlb_goblin_over_keep({
            **rec,
            "prop_norm": rec["prop"],
            "l5": rec["l5_over"],
            "l10": rec["l10_over"],
        })
    except TypeError:
        ok = True  # probe signature
    except Exception as e:
        ok = f"err:{e}"
    rows.append((ok, rec))

print("L5=5 goblin over rows", len(rows))
# show keep gate API
import inspect
print("keep sig", inspect.signature(mlb_goblin_over_keep))
print(inspect.getsource(mlb_goblin_over_keep)[:1500])
