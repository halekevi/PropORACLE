import json
from pathlib import Path
r=json.loads(Path(r"H:\PropORACLE\data\reports\tennis_all_props_rank_stack.json").read_text(encoding="utf-8"))
for c in r["cells"]:
    if c["book"]!="Goblin OVER" or c["prop"]!="games_won":
        continue
    for g in c["gates"]:
        if g.get("gate") not in (
            "KEEP union (gap≥4|L10≥8+(lefty|2nd-won))",
            "L10≥8 + 2nd-won≥45.7",
            "L10≥8 + (lefty|2nd-won≥45.7)",
            "ungated",
        ):
            continue
        b=g.get("base") or {}
        print(f"\n{g['gate']}: {b.get('hit_pct')}% n={b.get('n')} hits={b.get('hits')}")
        for s in g.get("rank_stacks") or []:
            if "50+" in s.get("stack",""):
                print(f"  {s['stack']}: {s['hit_pct']}% n={s['n']} hits={s['hits']} lift={s['lift_pp']}")
        # compute without 50+
        for s in g.get("rank_stacks") or []:
            if s.get("stack")=="player better 50+":
                h,n=b["hits"],b["n"]
                sh,sn=s["hits"],s["n"]
                rem_h, rem_n = h-sh, n-sn
                pct = round(100*rem_h/rem_n,1) if rem_n else None
                print(f"  WITHOUT player-better-50+: {pct}% n={rem_n} hits={rem_h} (removed {sn})")
