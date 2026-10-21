from pathlib import Path
text = Path(r"H:\PropORACLE\best_plays_2026-09-18_am.txt").read_text(encoding="utf-8", errors="replace")
print(text[:300])
print("---LEN", len(text))
cur = mode = None
sec = {}
for ln in text.splitlines():
    if ln.startswith("===== "):
        cur = ln.replace("=====", "").strip().split()[0]
        mode = None
        sec.setdefault(cur, {"hdr": ln.strip(), "SO": [], "SU": [], "GO": []})
        continue
    if ln.startswith("Best props"):
        print(ln)
        continue
    if ln.startswith("Standard OVER"):
        mode = "SO"; continue
    if ln.startswith("Standard UNDER"):
        mode = "SU"; continue
    if ln.startswith("Goblin OVER"):
        mode = "GO"; continue
    if ln.startswith("Goblin ") or ln.startswith("[") or ln.startswith("Excel") or ln.startswith("====="):
        mode = None; continue
    if not cur or not mode: continue
    s = ln.strip()
    if s.startswith(("S/","A/","B/","C/","D/","W/")) or "(none" in s or s.startswith("…"):
        sec[cur][mode].append(s)

def pick_goblins(rows, n=12):
    # prefer Diamond/Platinum/Gold then Silver; skip miss-only junk later
    order = {"Diamond":0,"Platinum":1,"Gold":2,"Silver":3,"Bronze":4,"—":5,"-":5}
    def key(r):
        badge = "Bronze"
        for b in ("Diamond","Platinum","Gold","Silver","Bronze"):
            if f"/{b}" in r or f"/{b} " in r: badge=b; break
        # also S/Diamond style
        for b in ("Diamond","Platinum","Gold","Silver","Bronze"):
            if b in r.split()[0]: badge=b; break
        return (order.get(badge,9), r)
    # parse badge from "A/Platinum"
    def badge(r):
        part = r.split(None,1)[0]
        if "/" in part: return part.split("/",1)[1].replace("—","-")
        return "z"
    pri = {"Diamond":0,"Platinum":1,"Gold":2,"Silver":3,"Bronze":4,"-":5,"—":5}
    rows2 = sorted(rows, key=lambda r: (pri.get(badge(r),9), 0 if "miss" not in r else 1))
    return rows2[:n]

for sport in ["WNBA","MLB","SOCCER","TENNIS","CFB"]:
    s = sec.get(sport)
    if not s:
        print(f"\n## {sport}\n  (no section)"); continue
    print(f"\n## {sport}")
    print(s["hdr"][:100])
    print(f"Std OVER ({len(s['SO'])})")
    for r in s["SO"][:10]: print(" ", r[:130])
    if len(s["SO"])>10: print(f"  … +{len(s['SO'])-10}")
    print(f"Std UNDER ({len(s['SU'])})")
    for r in s["SU"][:10]: print(" ", r[:130])
    if len(s["SU"])>10: print(f"  … +{len(s['SU'])-10}")
    print(f"Goblin OVER ({len(s['GO'])}) — top")
    for r in pick_goblins(s["GO"], 15): print(" ", r[:130])
