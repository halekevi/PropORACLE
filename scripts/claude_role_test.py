import sqlite3,numpy as np,pandas as pd,warnings; warnings.filterwarnings('ignore')
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",c); n['gd']=pd.to_datetime(n.game_date); n['player']=n.player.str.lower().str.strip()
n['minutes']=pd.to_numeric(n.minutes,errors='coerce'); n=n[n.minutes>0].drop_duplicates(['event_id','player']).sort_values('gd')
d=pd.read_pickle('/tmp/posdef_ds2.pkl'); d=d[d.hit.notna()].copy(); d['hit']=d.hit.astype(float); d['sdate']=pd.to_datetime(d.slate_date)
# team (ESPN abbr) for each prop row via player's next game
a=d[['k','sdate']].reset_index().sort_values('sdate'); b=n[['player','gd','team']].rename(columns={'player':'k'}).sort_values('gd')
m=pd.merge_asof(a,b,left_on='sdate',right_on='gd',by='k',direction='forward',tolerance=pd.Timedelta(days=1)).set_index('index')
d['tm']=m.team; d=d[d.tm.notna()]
cols=sorted(d.col.unique())
for col in cols: n[col]=pd.to_numeric(n[col],errors='coerce')
# per player-stat rolling prior-10 mean, indexed by (player, gd)
res={}
for col in cols:
    r=n[['player','team','gd',col]].copy()
    r['av10']=r.groupby('player')[col].transform(lambda s:s.shift(1).rolling(10,min_periods=5).mean())
    r['np']=r.groupby('player').cumcount()
    res[col]=r
share=[];rank=[];idx=[]
for col,g in d.groupby('col'):
    r=res[col].dropna(subset=['av10'])
    # team-date table: players with av10 on that date
    t=r.groupby(['team','gd'])
    T={}
    for (tm,gd),x in t:
        tot=x.av10.sum(); T[(tm,gd)]=(x.set_index('player').av10,tot)
    for i,row in g.iterrows():
        v=T.get((row.tm,row.sdate)) or T.get((row.tm,row.sdate+pd.Timedelta(days=1)))
        if v is None or row.k not in v[0].index: share.append(np.nan);rank.append(np.nan);idx.append(i);continue
        s,tot=v; share.append(s[row.k]/tot if tot>0 else np.nan); rank.append(float((s>s[row.k]).sum()+1)); idx.append(i)
d['share']=pd.Series(share,index=idx); d['trank']=pd.Series(rank,index=idx)
d['clean']=(d.n>=20)&(d.mn10>=20)&(d.mnfl5>=12)&(d.mnsd<=6)&(d.mntrend>-4)&(d.dsl<=7)&(d.line>=1.5)
d.to_pickle('/tmp/role_ds.pkl')
d['role']=np.where(d.trank<=2,'primary (top 2 on team)',np.where(d.trank<=4,'secondary (3-4)','role (5+)'))
def rep(a): 
    h=a[a.sdate>=pd.Timestamp('2026-05-09')]; return f'all {a.hit.mean()*100:5.1f}% (n={len(a):5d}) | hold {h.hit.mean()*100 if len(h)>=20 else float("nan"):5.1f}% (n={len(h):4d})'
print('coverage',d.trank.notna().mean().round(3))
for pt in ['Goblin','Standard','Demon']:
    y=d[d.pt==pt]
    for side in ([1] if pt!='Standard' else [1,-1]):
        z=y[y.side==side]; tag='Over' if side==1 else 'Under'
        g=((z.h5>=1)&(z.h10>=.9)&(z.h20>=.8)) if pt=='Goblin' else ((z.h5>=.8)&(z.h10>=.8)&(z.h20>=.65)) if pt=='Standard' else ((z.h5>=.8)&(z.h10>=.7)&(z.zmin>=.25))
        print(f'\n== {pt} {tag}')
        for lab,mask in [('all props',z.index==z.index),('gated',g)]:
            print(f' [{lab}]')
            for ro,x in z[mask].groupby('role'): print(f'   {ro:26s} {rep(x)}')
