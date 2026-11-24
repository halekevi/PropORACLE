#!/usr/bin/env python3
"""Rebuild CFB per-player game logs from ESPN box scores, INCLUDING zero-stat games.
ESPN lists only players who recorded a stat, so a game where a TE had no catches has no row.
We add zero rows: (a) players in the box score who lack a stat get 0 for it; (b) a player who is absent
from the box score entirely in a team game AFTER his first appearance gets an inferred-zero row
(inferred_zero=True; this can include games he did not dress for - ESPN does not list inactives).
Usage:  py -3 scripts/claude_cfb_logs.py --start 2026-08-29 --end 2026-11-13
        py -3 scripts/claude_cfb_logs.py --start 2025-08-30 --end 2026-01-20   (last season, same output file merges)
Writes data/game_logs/cfb_player_games_v2.csv (safe to re-run; raw JSON cached in data/cache/cfb_espn/)."""
import argparse, json, os, sys, time, requests, pandas as pd
from datetime import date, timedelta
B="https://site.api.espn.com/apis/site/v2/sports/football/college-football"
S=requests.Session(); S.headers["User-Agent"]="Mozilla/5.0"
CACHE="data/cache/cfb_espn"; os.makedirs(CACHE,exist_ok=True)
def get(u,p=None):
    for i in range(3):
        try:
            r=S.get(u,params=p,timeout=40)
            if r.ok: return r.json()
        except Exception: pass
        time.sleep(2)
    return {}
def num(x):
    try: return float(str(x).replace(',',''))
    except Exception: return None
def parse(ev,gd):
    j=get(B+"/summary",{"event":ev}) if not os.path.exists(f"{CACHE}/{ev}.json") else json.load(open(f"{CACHE}/{ev}.json"))
    if not j: return [],[]
    json.dump(j,open(f"{CACHE}/{ev}.json","w"))
    teams={}
    comp=(j.get("header",{}).get("competitions") or [{}])[0]
    for c in comp.get("competitors",[]): teams[str(c["team"]["id"])]=c["team"].get("abbreviation")
    ids=list(teams)
    rows={}
    for t in j.get("boxscore",{}).get("players",[]):
        tid=str(t["team"]["id"]); ab=t["team"].get("abbreviation") or teams.get(tid)
        opp=[teams[i] for i in ids if i!=tid]; opp=opp[0] if opp else None
        for cat in t.get("statistics",[]):
            nm=cat.get("name"); lab=cat.get("labels",[])
            for a in cat.get("athletes",[]):
                at=a["athlete"]; pid=str(at["id"]); st=dict(zip(lab,a.get("stats",[])))
                r=rows.setdefault((pid,tid),dict(game_date=gd,event_id=ev,athlete_id=pid,player=at.get("displayName"),team=ab,opp=opp,pass_yds=None,rush_yds=None,rec_yds=None,rec=None,rush_att=None,pass_att=None,pass_td=None,rush_td=None,rec_td=None,kick_pts=None,inferred_zero=False))
                if nm=="passing":
                    r["pass_yds"]=num(st.get("YDS")); r["pass_td"]=num(st.get("TD"))
                    ca=str(st.get("C/ATT","")); r["pass_att"]=num(ca.split("/")[1]) if "/" in ca else None
                elif nm=="rushing": r["rush_yds"]=num(st.get("YDS")); r["rush_td"]=num(st.get("TD")); r["rush_att"]=num(st.get("CAR"))
                elif nm=="receiving": r["rec"]=num(st.get("REC")); r["rec_yds"]=num(st.get("YDS")); r["rec_td"]=num(st.get("TD"))
                elif nm=="kicking": r["kick_pts"]=num(st.get("PTS"))
    stat=["pass_yds","rush_yds","rec_yds","rec","rush_att","pass_att","pass_td","rush_td","rec_td","kick_pts"]
    out=[]
    for r in rows.values():
        for k in stat:
            if r[k] is None: r[k]=0.0
        out.append(r)
    return out,[(ev,gd,teams[i],[teams[x] for x in ids if x!=i][0] if len(ids)>1 else None) for i in ids]
ap=argparse.ArgumentParser(); ap.add_argument("--start"); ap.add_argument("--end"); a=ap.parse_args()
d=date.fromisoformat(a.start); e=date.fromisoformat(a.end); P=[]; TG=[]
while d<=e:
    j=get(B+"/scoreboard",{"dates":d.strftime("%Y%m%d"),"groups":"80","limit":"400"})
    n=0
    for ev in j.get("events",[]):
        if ev.get("status",{}).get("type",{}).get("completed"):
            r,t=parse(ev["id"],d.isoformat()); P+=r; TG+=t; n+=1
    print(d,n,"games",flush=True); d+=timedelta(1)
new=pd.DataFrame(P); tg=pd.DataFrame(TG,columns=["event_id","game_date","team","opp"])
f="data/game_logs/cfb_player_games_v2.csv"
if os.path.exists(f):
    old=pd.read_csv(f,dtype={"event_id":str,"athlete_id":str}); new=pd.concat([old[old.inferred_zero==False],new]).drop_duplicates(["event_id","athlete_id"],keep="last")
    og=pd.concat([old[["event_id","game_date","team","opp"]].drop_duplicates(),tg]); tg=og.drop_duplicates(["event_id","team"])
new=new.drop_duplicates(["event_id","athlete_id"])
# infer zero games: team game AFTER the player's first appearance where he has no row
real=new[new.inferred_zero==False]; first=real.groupby(["athlete_id","team"]).game_date.min().reset_index().rename(columns={"game_date":"first"})
have=set(zip(real.event_id,real.athlete_id)); info=real.drop_duplicates(["athlete_id","team"])[["athlete_id","team","player"]]
add=[]
m=first.merge(tg,on="team")
m=m[m.game_date>m["first"]]
for r in m.itertuples():
    if (r.event_id,r.athlete_id) in have: continue
    add.append(dict(game_date=r.game_date,event_id=r.event_id,athlete_id=r.athlete_id,player=None,team=r.team,opp=r.opp,pass_yds=0.0,rush_yds=0.0,rec_yds=0.0,rec=0.0,rush_att=0.0,pass_att=0.0,pass_td=0.0,rush_td=0.0,rec_td=0.0,kick_pts=0.0,inferred_zero=True))
Z=pd.DataFrame(add)
if len(Z):
    nm_=real.drop_duplicates("athlete_id").set_index("athlete_id").player; Z["player"]=Z.athlete_id.map(nm_)
out=pd.concat([real,Z],ignore_index=True).sort_values(["player","game_date"])
out.to_csv(f,index=False); print("rows",len(out),"real",len(real),"inferred zero",len(Z))
