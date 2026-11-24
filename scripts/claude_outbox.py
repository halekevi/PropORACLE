import _r2
import sqlite3,numpy as np,pandas as pd,warnings,itertools; warnings.filterwarnings('ignore')
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",c); n['gd']=pd.to_datetime(n.game_date); n['player']=n.player.str.lower().str.strip()
n=n[pd.to_numeric(n.minutes,errors='coerce')>0].sort_values('gd')
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[d.hit.notna()].copy(); d['hit']=d.hit.astype(float)
P={k:g for k,g in n.groupby('player')}
last=[];lastz=[]; 
for r in d.itertuples():
    g=P.get(r.k)
    if g is None or r.col not in g.columns: last.append(np.nan);lastz.append(np.nan);continue
    dates=g.gd.values; i=np.searchsorted(dates,np.datetime64(r.sdate),side='left')
    if i<8: last.append(np.nan);lastz.append(np.nan);continue
    s=pd.to_numeric(g[r.col],errors='coerce').values[:i]; w=s[-20:]
    sd=max(np.nanstd(w),0.3); last.append(s[-1]); lastz.append(r.side*(s[-1]-np.nanmean(w))/sd)
d['lastz']=lastz   # side-adjusted: + means last game was favorable to this pick
d['mon']=d.sdate.dt.to_period('M')
H=d.sdate>=pd.Timestamp('2026-05-09')
def rep(a,name):
    h=a[H[a.index]]; print(f'  {name:48s} all {a.hit.mean()*100:5.1f}% (n={len(a):5d}) | hold {h.hit.mean()*100 if len(h)>=15 else float("nan"):5.1f}% (n={len(h):4d})')
print('=== A. GOBLIN extreme gates (breakeven 84.5% at 5/6 legs; 87% at 3-leg 1.5x)')
G=d[d.pt=='Goblin']
for nm,m in [('L5 5/5, L10>=90, L20>=80 (current gate)',(G.h5>=1)&(G.h10>=.9)&(G.h20>=.8)),
 ('+ L20 = 100%',(G.h5>=1)&(G.h10>=1)&(G.h20>=1)),
 ('L10=100, L20>=95, gap>=1 SD',(G.h10>=1)&(G.h20>=.95)&(G.zmin>=1)),
 ('L10=100, L20>=95, gap>=1.5 SD',(G.h10>=1)&(G.h20>=.95)&(G.zmin>=1.5)),
 ('L10=100, L20=100, gap>=1.5 SD',(G.h10>=1)&(G.h20>=1)&(G.zmin>=1.5)),
 ('L10=100, L20=100, gap>=2 SD',(G.h10>=1)&(G.h20>=1)&(G.zmin>=2)),
 ('L20=100, season>=95%, gap>=1 SD',(G.h20>=1)&(G.hs>=.95)&(G.zmin>=1)),
 ('L20=100, season>=95%, gap>=1 SD, clean',(G.h20>=1)&(G.hs>=.95)&(G.zmin>=1)&G.clean)]: rep(G[m],nm)
print('\n=== B. Last-game shock (side-adjusted z of his last game vs his L20; + = last game helped the pick), gated sets')
for pt,gate in [('Goblin',lambda x:(x.h5>=1)&(x.h10>=.9)&(x.h20>=.8)),('Standard',lambda x:(x.h5>=.8)&(x.h10>=.8)&(x.h20>=.65)&(x.zmin>=.5))]:
    y=d[d.pt==pt]
    for side in ([1] if pt=='Goblin' else [-1,1]):
        z=y[(y.side==side)&gate(y)&y.lastz.notna()]; tag=f'{pt} {"Over" if side==1 else "Under"}'
        print(' ',tag)
        for lo,hi in [(-9,-1),(-1,0),(0,1),(1,9)]: rep(z[(z.lastz>lo)&(z.lastz<=hi)],f'last game z in ({lo},{hi}]')
print('\n=== C. Season phase (Standard Under gated)')
z=d[(d.pt=='Standard')&(d.side==-1)&(d.h5>=.8)&(d.h10>=.8)&(d.h20>=.65)&(d.zmin>=.5)]
for mo,x in z.groupby('mon'): rep(x,str(mo))
print('\n=== D. Line size (Standard Under, L5 5/5 + L10>=70)')
z=d[(d.pt=='Standard')&(d.side==-1)&(d.h5>=1)&(d.h10>=.7)]
for lo,hi in [(0,1),(1,3),(3,8),(8,15),(15,25),(25,99)]: rep(z[(z.line>lo)&(z.line<=hi)],f'line in ({lo},{hi}]')
