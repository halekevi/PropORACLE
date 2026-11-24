"""Adds spread/total/blowout tags to NBA lists. Usage: python3 scripts/claude_spread_tags.py 2026-11-04
Input: outputs/D/nba/game_odds_D.csv (event_id,home,away,spread_home,total,spread_open_home; ESPN/DraftKings pickcenter, home spread, negative = home favored).
Adds: team_spread (negative = this team favored), spread_open, game_total, blowout_flag, spread_note. Flags only, never drops.
blowout_flag: 20+ / 15+ / 12+ by |spread|. 'NO SPREAD' when the line is not posted yet (re-run before building tickets)."""
import sys,pandas as pd,numpy as np
D=sys.argv[1]
A={'NY':'NYK','GS':'GSW','SA':'SAS','NO':'NOP','UTAH':'UTA','WSH':'WAS','PHO':'PHX','BKN':'BKN'}
n=lambda t:A.get(str(t).upper(),str(t).upper())
o=pd.read_csv(f'outputs/{D}/nba/game_odds_{D}.csv')
rows=[]
for _,r in o.iterrows():
    sp=pd.to_numeric(r.spread_home,errors='coerce'); op=pd.to_numeric(str(r.spread_open_home).replace('+',''),errors='coerce'); tot=pd.to_numeric(r.total,errors='coerce')
    rows.append((n(r.home),sp,op,tot)); rows.append((n(r.away),-sp,-op,tot))
t=pd.DataFrame(rows,columns=['tk','team_spread','spread_open','game_total']).drop_duplicates('tk')
for f in ['ranked_5of5','ranked_4of5','goblin_5of5','goblin_4of5','demon_5of5','demon_4of5']:
    p=f'outputs/{D}/ranked/nba_{f}.csv'
    try: d=pd.read_csv(p)
    except FileNotFoundError: continue
    d=d.drop(columns=['team_spread','spread_open','game_total','blowout_flag','spread_note'],errors='ignore')
    d['tk']=d.team.map(n); d=d.merge(t,on='tk',how='left').drop(columns='tk')
    a=d.team_spread.abs()
    d['blowout_flag']=np.select([d.team_spread.isna(),a>=20,a>=15,a>=12],['NO SPREAD','BLOWOUT 20+','BLOWOUT 15+','BLOWOUT 12+'],'')
    d['spread_note']=np.where(d.team_spread.isna(),'',np.where(d.team_spread<0,'favorite','underdog'))
    d.to_csv(p,index=False); print(f,len(d),'no spread',int(d.team_spread.isna().sum()),'flagged',int((d.blowout_flag.str.startswith('BLOWOUT')).sum()))
