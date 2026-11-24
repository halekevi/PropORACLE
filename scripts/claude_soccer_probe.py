#!/usr/bin/env python3
"""Probe: which per-player stats does ESPN give for soccer games we already have? (read-only, ~6 requests)
Run:  py -3 scripts/claude_soccer_probe.py"""
import sqlite3,requests,pandas as pd,collections
c=sqlite3.connect('data/cache/proporacle_ref.db')
ev=pd.read_sql("select league,event_id,max(game_date) d from soccer where game_date>='2026-10-01' group by league,event_id order by d desc",c)
picks=[]
intl=[l for l in ev.league.unique() if any(k in l for k in ('fifa','uefa','friendly','nations'))][:2]
for lg in intl+['eng.1','ita.1','esp.1','usa.1']:
    x=ev[ev.league==lg].head(1)
    if len(x): picks.append((lg,str(x.event_id.iloc[0]),x.d.iloc[0]))
S=requests.Session(); S.headers['User-Agent']='Mozilla/5.0'
for lg,eid,d in picks[:6]:
    j=S.get(f"https://site.api.espn.com/apis/site/v2/sports/soccer/{lg}/summary",params={'event':eid},timeout=40).json()
    names=collections.Counter(); n=0
    for r in j.get('rosters',[]):
        for p in r.get('roster',[]):
            n+=1
            for s in p.get('stats',[]): names[s.get('name')]+=1
    bs=[(t.get('team',{}).get('abbreviation'),len(t.get('statistics',[]))) for t in j.get('boxscore',{}).get('teams',[])]
    print(f"\n{lg} event {eid} ({d}): {n} rostered players; per-player stat names:")
    print(', '.join(f"{k}({v})" for k,v in sorted(names.items())) or '(none)')
    print('team-level stat blocks:',bs)
