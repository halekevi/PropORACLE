import pandas as pd
from pathlib import Path
import sys
_REPO = Path(r"H:/PropORACLE")
sys.path[:0] = [str(_REPO)]
from utils.cfb_keep_gates import cfb_list_eligible, cfb_pass_yards_goblin_skip_d

p = Path(r"H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx")
df = pd.read_excel(p)
print("rows", len(df))
print("cols sample", [c for c in df.columns if any(x in str(c).lower() for x in ("l5","l10","player","prop","pick","dir","line","def","flag","sample","team","opp"))][:30])

def col(*names):
    for n in names:
        if n in df.columns: return n
    low = {str(c).lower().replace(" ","_"): c for c in df.columns}
    for n in names:
        k = n.lower().replace(" ","_")
        if k in low: return low[k]
    return None

c_player=col("Player","player")
c_prop=col("Prop","prop","stat_type")
c_pt=col("Pick Type","pick_type")
c_dir=col("Direction","direction")
c_line=col("Line","line")
c_l5o=col("L5 Over","l5_over")
c_l5u=col("L5 Under","l5_under")
c_l10o=col("L10 Over","l10_over")
c_l10u=col("L10 Under","l10_under")
c_team=col("Team","team")
c_opp=col("Opp","opp")
c_def=col("Def Tier","def_tier")
c_flag=col("Season L5 Flag","season_l5_flag")
c_gn=col("L5 Sample N","l5_sample_n")
c_cov=col("Abs Edge","cover","dist_l5")

df["_l5o"]=pd.to_numeric(df[c_l5o], errors="coerce").fillna(0)
df["_l5u"]=pd.to_numeric(df[c_l5u], errors="coerce").fillna(0)
df["_l10o"]=pd.to_numeric(df[c_l10o], errors="coerce").fillna(0) if c_l10o else 0
df["_l10u"]=pd.to_numeric(df[c_l10u], errors="coerce").fillna(0) if c_l10u else 0
df["_dir"]=df[c_dir].astype(str).str.upper()
df["_dl5"]=df.apply(lambda r: r["_l5o"] if r["_dir"]=="OVER" else r["_l5u"], axis=1)
df["_dl10"]=df.apply(lambda r: r["_l10o"] if r["_dir"]=="OVER" else r["_l10u"], axis=1)
df["_gn"]=pd.to_numeric(df[c_gn], errors="coerce") if c_gn else None

print("dl5 dist", df["_dl5"].value_counts().sort_index().to_dict())
print("dl5>=4", int((df["_dl5"]>=4).sum()), "dl5=5", int((df["_dl5"]>=5).sum()))
print("l5n", df["_gn"].value_counts().sort_index().to_dict() if c_gn else None)
print("flags", df[c_flag].astype(str).value_counts().head(10).to_dict() if c_flag else None)

# L5=5 faces
eq5 = df[df["_dl5"]>=5].copy()
print("\n=== ALL directional L5=5:", len(eq5), "===")

# list eligibility
rows=[]
for _, r in eq5.iterrows():
    rec = {
        "sport": "CFB",
        "player": r[c_player],
        "prop": r[c_prop],
        "prop_type": r[c_prop],
        "pick_type": str(r[c_pt]),
        "direction": r["_dir"],
        "side": r["_dir"],
        "line": r[c_line],
        "l5_over": r["_l5o"],
        "l5_under": r["_l5u"],
        "l10_over": r["_l10o"],
        "l10_under": r["_l10u"],
        "def_tier": r[c_def] if c_def else "",
        "team": r[c_team] if c_team else "",
        "opp": r[c_opp] if c_opp else "",
    }
    # attach original cols
    for c in df.columns:
        rec[str(c)] = r[c]
    list_ok = True
    try:
        list_ok = cfb_list_eligible(rec)
    except Exception as e:
        list_ok = f"err:{e}"
    rows.append((list_ok, rec))

ok = [r for ok,r in rows if ok is True]
print("L5=5 and cfb_list_eligible:", len(ok))

def show(title, items, n=40):
    print(f"\n{title} (n={len(items)})")
    items = sorted(items, key=lambda x: (-float(x.get("l5_over") or 0 if str(x.get("direction")).upper()=="OVER" else x.get("l5_under") or 0), str(x.get("player"))))
    for r in items[:n]:
        d = str(r["direction"]).upper()
        l5 = r["l5_over"] if d=="OVER" else r["l5_under"]
        l10 = r["l10_over"] if d=="OVER" else r["l10_under"]
        print(f"  {str(r['pick_type'])[:8]:8} {d:5} {str(r['player'])[:22]:22} {str(r['prop'])[:20]:20} O{r['line']}  L5={int(l5)} L10={int(l10) if l10==l10 else '-'}  D={r.get('def_tier')}  {r.get('team')} vs {r.get('opp')}")

# group by pick type
gob = [r for r in ok if "goblin" in str(r["pick_type"]).lower() and str(r["direction"]).upper()=="OVER"]
std_o = [r for r in ok if "standard" in str(r["pick_type"]).lower() and str(r["direction"]).upper()=="OVER"]
std_u = [r for r in ok if "standard" in str(r["pick_type"]).lower() and str(r["direction"]).upper()=="UNDER"]
show("Goblin OVER L5=5 list-eligible", gob)
show("Standard OVER L5=5 list-eligible", std_o)
show("Standard UNDER L5=5 list-eligible", std_u)

# also show L5=5 that failed list gate
bad = [(okf,r) for okf,r in rows if okf is not True]
print(f"\nL5=5 but list-ineligible: {len(bad)}")
for okf,r in bad[:15]:
    print(f"  skip {r['pick_type']} {r['direction']} {r['player']} {r['prop']} {r['line']} reason={okf}")

# ticket-stack hint: L5=5 + L10>=8
ticketish = []
for r in ok:
    d = str(r["direction"]).upper()
    l10 = r["l10_over"] if d=="OVER" else r["l10_under"]
    if l10 is not None and float(l10) >= 8:
        ticketish.append(r)
show("L5=5 + L10>=8 (ticket-ish)", ticketish, n=30)
