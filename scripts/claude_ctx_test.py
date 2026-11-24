import sqlite3,pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",c)
n['k']=n.player.str.lower().str.strip(); n['gd']=pd.to_datetime(n.game_date)
S=['fg3m','fg3a','fta','ftm','fga','reb','ast']
for s in S+['minutes']: n[s]=pd.to_numeric(n[s],errors='coerce')
n=n[n.minutes>0].sort_values(['k','gd'])
n['b2b']=(n.groupby('k').gd.diff().dt.days<=1).astype(float)
n['road']=(n.team==n.away_team).astype(float); n['opponent']=np.where(n.team==n.home_team,n.away_team,n.home_team)
# opponent-allowed (soft/tough) per stat: team allowed per game, rolling 15 prior, relative to league
tg=n.groupby(['opponent','gd'])[S].sum().reset_index().sort_values('gd')
for s in S:
    tg[s+'_r']=tg.groupby('opponent')[s].transform(lambda v:v.shift(1).rolling(15,min_periods=8).mean())
    lg=tg[s].mean(); tg[s+'_rel']=tg[s+'_r']/lg
tgk=tg.set_index(['opponent','gd'])
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[d.hit.notna()&d.col.isin(S)].copy(); d['hit']=d.hit.astype(float)
m=n[['k','gd','opponent','b2b','road']].drop_duplicates(['k','gd'])
d=d.merge(m,on=['k','gd'],how='inner')
d['rel']=[tgk[r.col+'_rel'].get((r.opponent,r.gd),np.nan) if (r.opponent,r.gd) in tgk.index else np.nan for r in d.itertuples()]
d=d.dropna(subset=['rel']); print('rows',len(d))
H=d.sdate>=pd.Timestamp('2026-05-09')
def line(a,nm):
    h=a[H[a.index]]; return f'{nm:14s} {a.hit.mean()*100:5.1f}% n={len(a):4d} (hold {h.hit.mean()*100 if len(h)>=20 else float("nan"):5.1f}% n={len(h):4d})'
grp={'3PM':['fg3m'],'3PA':['fg3a'],'FTA':['fta'],'FTM':['ftm'],'FGA':['fga'],'REB':['reb'],'AST':['ast']}
for pt,gate in [('Goblin',lambda x:(x.h10>=.8)&(x.h20>=.7)),('Standard',lambda x:(x.h10>=.7)&(x.h20>=.6))]:
  for gname,cols in grp.items():
    x=d[(d.pt==pt)&d.col.isin(cols)]; x=x[gate(x)]
    for side,sn in [(1,'Over'),(-1,'Under')]:
        y=x[x.side==side]
        if len(y)<120: continue
        print(f'\n[{pt} {gname} {sn}] base {line(y,"all")}')
        # flip meaning for unders: "good for pick" soft defense helps over, tough helps under
        print('  opp soft(>1.05):',line(y[y.rel>1.05],'soft'),'| tough(<0.95):',line(y[y.rel<0.95],'tough'))
        print('  b2b:',line(y[y.b2b==1],'b2b'),'| rested:',line(y[y.b2b==0],'rested'))
        print('  road:',line(y[y.road==1],'road'),'| home:',line(y[y.road==0],'home'))
