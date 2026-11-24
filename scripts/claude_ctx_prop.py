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

g=d[((d.pt=='Goblin')&(d.side==1)&(d.h10>=.8)&(d.h20>=.7))|((d.pt=='Standard')&(d.h10>=.7)&(d.h20>=.6))]
print('prop | side | soft hit% (n) | tough hit% (n) | soft minus tough')
rows=[]
for c in S:
    for side,nm in [(1,'Over'),(-1,'Under')]:
        y=g[(g.col==c)&(g.side==side)]; a=y[y.rel>1.05]; b=y[y.rel<.95]
        if len(a)>=25 and len(b)>=25: rows.append((c,nm,a.hit.mean()*100,len(a),b.hit.mean()*100,len(b),a.hit.mean()*100-b.hit.mean()*100))
for r in sorted(rows,key=lambda r:(r[1],-abs(r[6]))): print(f'{r[0]:14s} {r[1]:5s} soft {r[2]:5.1f}% ({r[3]:4d}) tough {r[4]:5.1f}% ({r[5]:4d})  diff {r[6]:+5.1f}')
