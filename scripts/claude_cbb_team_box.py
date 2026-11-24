#!/usr/bin/env python3
"""Last season's CBB team box scores (FGA, FTA, OREB, TOV, PTS) so we can compute pace for the CBB backtest.
Usage: py -3 scripts/claude_cbb_team_box.py --start 2025-12-16 --end 2026-04-06
Writes data/cache/cbb_team_box.csv (merges; safe to re-run; raw JSON cached in data/cache/cbb_box_espn/)."""
import argparse,json,os,time,requests,pandas as pd
from datetime import date,timedelta
B="https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball"
S=requests.Session(); S.headers["User-Agent"]="Mozilla/5.0"; C="data/cache/cbb_box_espn"; os.makedirs(C,exist_ok=True)
def get(u,p=None):
    for i in range(3):
        try:
            r=S.get(u,params=p,timeout=40)
            if r.ok: return r.json()
        except Exception: pass
        time.sleep(2)
    return {}
def att(v):
    s=str(v)
    try: return float(s.split('-')[1]) if '-' in s else float(s)
    except Exception: return None
def num(v):
    try: return float(str(v).replace(',',''))
    except Exception: return None
ap=argparse.ArgumentParser(); ap.add_argument('--start',required=True); ap.add_argument('--end',required=True); a=ap.parse_args()
d0=date.fromisoformat(a.start); d1=date.fromisoformat(a.end); rows=[]
d=d0
while d<=d1:
    sb=get(B+"/scoreboard",{"dates":d.strftime('%Y%m%d'),"groups":50,"limit":400})
    for e in sb.get('events',[]):
        if not e.get('status',{}).get('type',{}).get('completed'): continue
        ev=e['id']; fp=f"{C}/{ev}.json"
        j=json.load(open(fp)) if os.path.exists(fp) else get(B+"/summary",{"event":ev})
        if not j: continue
        json.dump(j,open(fp,'w'))
        tm=j.get('boxscore',{}).get('teams',[])
        if len(tm)!=2: continue
        info=[]
        for t in tm:
            st={s.get('name'):s.get('displayValue') for s in t.get('statistics',[])}
            fga=att(st.get('fieldGoalsMade-fieldGoalsAttempted',st.get('fieldGoalsAttempted')))
            fta=att(st.get('freeThrowsMade-freeThrowsAttempted',st.get('freeThrowsAttempted')))
            oreb=num(st.get('offensiveRebounds')); tov=num(st.get('totalTurnovers',st.get('turnovers')))
            info.append((t['team'].get('abbreviation'),fga,fta,oreb,tov))
        pts={c['team'].get('abbreviation'):num(c.get('score')) for c in (j.get('header',{}).get('competitions') or [{}])[0].get('competitors',[])}
        for i,(ab,fga,fta,oreb,tov) in enumerate(info):
            rows.append(dict(game_date=d.isoformat(),event_id=ev,team=ab,opp=info[1-i][0],fga=fga,fta=fta,oreb=oreb,tov=tov,pts=pts.get(ab)))
    print(d,len(rows),flush=True); d+=timedelta(days=1)
out='data/cache/cbb_team_box.csv'; n=pd.DataFrame(rows)
if os.path.exists(out): n=pd.concat([pd.read_csv(out),n]).drop_duplicates(['event_id','team'])
n.to_csv(out,index=False); print('wrote',out,len(n),'team-games; missing fga',int(n.fga.isna().sum()))
