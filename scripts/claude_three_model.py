import sqlite3,pandas as pd,numpy as np,warnings; from scipy.stats import binom; warnings.filterwarnings('ignore')
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",c)
n['k']=n.player.str.lower().str.strip(); n['gd']=pd.to_datetime(n.game_date)
for k in ['minutes','fg3m','fg3a']: n[k]=pd.to_numeric(n[k],errors='coerce')
n=n[n.minutes>0].sort_values('gd'); P={k:g for k,g in n.groupby('k')}
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[d.hit.notna()&d.col.isin(['fg3m'])].copy(); d['hit']=d.hit.astype(float)
out=[]
for r in d.itertuples():
    g=P.get(r.k)
    if g is None: continue
    i=np.searchsorted(g.gd.values,np.datetime64(r.sdate),side='left')
    if i<12: continue
    a=g.fg3a.values[:i]; m=g.fg3m.values[:i]
    pct=(m[-60:].sum()+0.36*80)/(a[-60:].sum()+80)       # shrunk individual 3P%
    att=a[-20:]; 
    # P(makes >= line+) using empirical last-20 attempts and individual pct
    thr=int(np.floor(r.line))   # over means makes>=floor(line)+1
    po=np.mean([1-binom.cdf(thr,int(x),pct) for x in att])
    out.append((r.sdate,r.pt,r.side,r.line,r.hit,po if r.side==1 else 1-po,pct,att.mean(),r.h10,r.h20,r.zmin))
o=pd.DataFrame(out,columns=['sdate','pt','side','line','hit','pmodel','pct','att','h10','h20','zmin'])
o.to_pickle('/tmp/three_ds.pkl'); H=o.sdate>=pd.Timestamp('2026-05-09')
def rep(a,nm):
    h=a[H[a.index]]; print(f'  {nm:40s} all {a.hit.mean()*100:5.1f}% n={len(a):4d} | hold {h.hit.mean()*100 if len(h)>=15 else float("nan"):5.1f}% n={len(h):4d}')
for pt in ['Standard','Goblin','Demon']:
    x=o[o.pt==pt]
    if len(x)<50: continue
    print('===',pt,'3PM; calibration of model prob (pick side)')
    for lo,hi in [(0,.45),(.45,.55),(.55,.65),(.65,.75),(.75,1.01)]:
        y=x[(x.pmodel>=lo)&(x.pmodel<hi)]
        if len(y)>=15: rep(y,f'model P in [{lo:.2f},{hi:.2f})  avgP={y.pmodel.mean():.2f}')
    print('  -- high individual 3P% (>=39%) vs low (<=34%), by side')
    for side,nm in [(1,'Over'),(-1,'Under')]:
        y=x[x.side==side]
        rep(y[y.pct>=.39],f'{nm} pct>=39%'); rep(y[y.pct<=.34],f'{nm} pct<=34%')
    print('  -- model>=0.65 AND L10/L20 agree (>=.7/.6)')
    y=x[(x.pmodel>=.65)&(x.h10>=.7)&(x.h20>=.6)]; rep(y,'combined')
