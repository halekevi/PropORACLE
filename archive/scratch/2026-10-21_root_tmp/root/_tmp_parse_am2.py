from pathlib import Path
import re
text = Path(r"H:\PropORACLE\best_plays_2026-09-18_am.txt").read_text(encoding="utf-8", errors="replace")
# Print full Platinum/Gold goblin + all Standard lines for core sports
cur=mode=None
for ln in text.splitlines():
    if ln.startswith("===== "):
        cur = ln.replace("=====","").strip().split()[0]
        print("\n"+ln.strip()[:90])
        mode=None
        continue
    if ln.startswith("Standard OVER"): mode="S"; print(ln); continue
    if ln.startswith("Standard UNDER"): mode="S"; print(ln); continue
    if ln.startswith("Goblin OVER"): mode="G"; print(ln); continue
    if ln.startswith("Goblin ") or ln.startswith("["): mode=None; continue
    if not cur or cur not in ("WNBA","MLB","SOCCER","TENNIS","CFB"): continue
    s=ln.rstrip()
    if not s.startswith(("S/","A/","B/","C/","D/","W/")) and "(none" not in s and not s.startswith("…"):
        continue
    if mode=="S":
        print(s)
    elif mode=="G":
        if any(x in s for x in ("/Platinum","/Gold","/Diamond")) or "(none" in s:
            print(s)
