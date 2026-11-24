import sqlite3,pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",c)
n['k']=n.player.str.lower().str.strip(); n['gd']=pd.to_datetime(n.game_date); n['minutes']=pd.to_numeric(n.minutes,errors='coerce')
n=n[n.minutes>0].sort_values('gd'); P={k:g for k,g in n.groupby('k')}
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[d.hit.notna()&d.col.isin(['stl','blk','tov','oreb'])].copy(); d['hit']=d.hit.astype(float)
mm=[];mn=[]
for r in d.itertuples():
    g=P.get(r.k); 
    if g is None: mm.append(np.nan);mn.append(np.nan);continue
    i=np.searchsorted(g.gd.values,np.datetime64(r.sdate),side='left'); w=g.minutes.values[max(0,i-10):i]
    mm.append(w.mean() if len(w)>=5 else np.nan); mn.append(w.min() if len(w)>=5 else np.nan)
d['m10']=mm; d['mmin']=mn; d=d.dropna(subset=['m10']); H=d.sdate>=pd.Timestamp('2026-05-09')
def f(a):
    h=a[H[a.index]]; return f'{a.hit.mean()*100:5.1f}% n={len(a):4d} (hold {h.hit.mean()*100 if len(h)>=15 else float("nan"):5.1f}% n={len(h):3d})'
for col in ['stl','blk','tov','oreb']:
  for pt in ['Standard','Goblin']:
    for side,nm in [(-1,'Under'),(1,'Over')]:
        x=d[(d.col==col)&(d.pt==pt)&(d.side==side)&(d.line>=0.5)]
        if len(x)<80: continue
        print(f'\n[{col} {pt} {nm}] all {f(x)}')
        for lo,hi,l in [(0,16,'<16 min'),(16,22,'16-22'),(22,28,'22-28'),(28,60,'28+')]:
            y=x[(x.m10>=lo)&(x.m10<hi)]
            print(f'   {l:8s} all {f(y)} | gated(L10>=70,L20>=60) {f(y[(y.h10>=.7)&(y.h20>=.6)])} | L5 5/5 {f(y[(y.h5>=1)&(y.h10>=.7)])}')
