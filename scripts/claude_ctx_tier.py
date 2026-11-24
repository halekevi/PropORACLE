import sqlite3,pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",c)
n['k']=n.player.str.lower().str.strip(); n['gd']=pd.to_datetime(n.game_date)
S=['pts','reb','ast','stl','blk','tov','fgm','fga','fg3m','fg3a','fg2m','fg2a','ftm','fta','oreb','dreb','pf','pra','pr','pa','ra','bs','fantasy_score']
for s in S+['minutes']: n[s]=pd.to_numeric(n[s],errors='coerce')
n=n[n.minutes>0]; n['opponent']=np.where(n.team==n.home_team,n.away_team,n.home_team)
tg=n.groupby(['opponent','gd'])[S].sum().reset_index().sort_values('gd')
for s in S:
    tg[s+'_r']=tg.groupby('opponent')[s].transform(lambda v:v.shift(1).rolling(15,min_periods=8).mean()); tg[s+'_rel']=tg[s+'_r']/tg[s].mean()
tgk=tg.set_index(['opponent','gd'])
T={'A skill/role':['fg2a','fga','fg3a','pa','pra','ast','pr','pts','fg2m','fantasy_score','reb','ra'],'B good':['fgm','fta','ftm','dreb'],'C mixed':['oreb','fg3m','tov'],'D luck':['blk','bs','pf','stl']}
inv={c:t for t,cs in T.items() for c in cs}
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[d.hit.notna()&d.col.isin(S)].copy(); d['hit']=d.hit.astype(float); d['tier']=d.col.map(inv)
m=n[['k','gd','opponent']].drop_duplicates(['k','gd']); d=d.merge(m,on=['k','gd'],how='inner')
rel=[]
for r in d.itertuples():
    key=(r.opponent,r.gd); rel.append(tgk[r.col+'_rel'].get(key,np.nan) if key in tgk.index else np.nan)
d['rel']=rel; d=d.dropna(subset=['rel'])
H=d.sdate>=pd.Timestamp('2026-05-09')
def f(a):
    h=a[H[a.index]]; return f'{a.hit.mean()*100:5.1f}% n={len(a):4d} (hold {h.hit.mean()*100 if len(h)>=20 else float("nan"):5.1f}% n={len(h):3d})'
G={'Goblin Over':lambda x:x[(x.pt=='Goblin')&(x.side==1)&(x.h10>=.8)&(x.h20>=.7)],
   'Standard Under':lambda x:x[(x.pt=='Standard')&(x.side==-1)&(x.h10>=.7)&(x.h20>=.6)],
   'Standard Over':lambda x:x[(x.pt=='Standard')&(x.side==1)&(x.h10>=.7)&(x.h20>=.6)]}
for g,fn in G.items():
    print('==',g); x=fn(d)
    for t in T:
        y=x[x.tier==t]
        if len(y)<60: continue
        print(f' {t:13s} soft(>1.05) {f(y[y.rel>1.05])} | mid {f(y[(y.rel>=.95)&(y.rel<=1.05)])} | tough(<.95) {f(y[y.rel<.95])}')
