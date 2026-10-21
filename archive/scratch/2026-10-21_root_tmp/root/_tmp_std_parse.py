from pathlib import Path
text = Path(r"H:\PropORACLE\best_plays_tomorrow_standards_raw.txt").read_text(encoding="utf-8", errors="replace")
cur_sport = None
mode = None
sections = {}
for ln in text.splitlines():
    if ln.startswith("===== "):
        cur_sport = ln.replace("=====", "").strip().split()[0]
        mode = None
        continue
    if ln.startswith("Standard OVER"):
        mode = "OVER"
        sections.setdefault(cur_sport, {"OVER": [], "UNDER": []})
        continue
    if ln.startswith("Standard UNDER"):
        mode = "UNDER"
        sections.setdefault(cur_sport, {"OVER": [], "UNDER": []})
        continue
    if ln.startswith("Goblin") or ln.startswith("[table") or ln.startswith("Excel") or ln.startswith("["):
        mode = None
        continue
    if mode and cur_sport and (ln.strip().startswith(("S/", "A/", "B/", "C/", "D/", "W/")) or "(none" in ln):
        sections.setdefault(cur_sport, {"OVER": [], "UNDER": []})[mode].append(ln.strip())

for sport in ["WNBA", "MLB", "SOCCER", "TENNIS", "CFB"]:
    s = sections.get(sport, {"OVER": [], "UNDER": []})
    print(f"\n## {sport}")
    for dir_ in ("OVER", "UNDER"):
        rows = s.get(dir_, [])
        print(f"Std {dir_} n={len(rows)}")
        for r in rows:
            print(r)
