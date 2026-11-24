#!/usr/bin/env python3
"""Pull player height/weight from ESPN team rosters for NBA and CBB (men's D1).
Usage: py -3 scripts/claude_heights.py            (both)   |  py -3 scripts/claude_heights.py nba
Writes data/rosters/nba_heights.csv and data/rosters/cbb_heights.csv"""
import sys, time, requests, pandas as pd, os
S=requests.Session(); S.headers["User-Agent"]="Mozilla/5.0"
def get(u,p=None):
    for i in range(3):
        try:
            r=S.get(u,params=p,timeout=30)
            if r.ok: return r.json()
        except Exception: pass
        time.sleep(2)
    return {}
def run(sport,league,out,params):
    base=f"https://site.api.espn.com/apis/site/v2/sports/{sport}/{league}"
    tj=get(base+"/teams",params)
    teams=[t["team"] for tt in tj.get("sports",[{}])[0].get("leagues",[{}])[0].get("teams",[]) for t in [tt]]
    rows=[]
    for i,t in enumerate(teams):
        j=get(f"{base}/teams/{t['id']}/roster")
        for a in j.get("athletes",[]):
            items=a.get("items",[a]) if isinstance(a,dict) and "items" in a else [a]
            for p in items:
                rows.append(dict(athlete_id=str(p.get("id")),player=p.get("fullName") or p.get("displayName"),team=t.get("abbreviation"),team_name=t.get("displayName"),position=(p.get("position") or {}).get("abbreviation"),height_in=p.get("height"),height=p.get("displayHeight"),weight_lb=p.get("weight"),jersey=p.get("jersey")))
        if i%20==0: print(sport,i,"/",len(teams),flush=True)
    df=pd.DataFrame(rows).drop_duplicates("athlete_id"); os.makedirs("data/rosters",exist_ok=True); df.to_csv(out,index=False)
    print(out,len(df),"players;",df.height_in.notna().sum(),"with height")
w=sys.argv[1:] or ["nba","cbb"]
if "nba" in w: run("basketball","nba","data/rosters/nba_heights.csv",{})
if "cbb" in w: run("basketball","mens-college-basketball","data/rosters/cbb_heights.csv",{"limit":"500","groups":"50"})
