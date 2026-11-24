import pandas as pd,numpy as np,glob,sys,warnings; warnings.filterwarnings('ignore')
sys.path.insert(0,'scripts'); from claude_gamelog import nm
g=pd.read_parquet('data/game_logs/cbb_player_games.parquet'); g['gd']=pd.to_datetime(g.game_date); g['k']=g.player.map(nm)
for c in ['pts','reb','ast','stl','blk','tov','fg3m','minutes']: g[c]=pd.to_numeric(g[c],errors='coerce')
g=g[(g.minutes>0)&~g.is_dnp.fillna(False).astype(bool)] if 'is_dnp' in g else g[g.minutes>0]
g['pra']=g.pts+g.reb+g.ast; g['pr']=g.pts+g.reb; g['pa']=g.pts+g.ast; g['ra']=g.reb+g.ast
fs=sorted(glob.glob('data/ledger/legs/slate_date=*/*.parquet')); L=pd.concat([pd.read_parquet(f) for f in fs]); L=L[(L.sport=='CBB')&L.hit.notna()&L.pick_type.notna()].copy()
M={'points':'pts','rebounds':'reb','assists':'ast','pra':'pra','pts+reb':'pr','pts+ast':'pa','reb+ast':'ra','threes':'fg3m','steals':'stl','blocks':'blk','turnovers':'tov'}
ser0={k:v.sort_values('gd') for k,v in g.groupby('k')}; ser=ser0
L=L.reset_index(drop=True); L['col']=L.prop_canon.map(M)
CAND=['pts','reb','ast','pra','pr','pa','ra','fg3m','stl','blk','tov']
gk={(k,d):v for (k,d),v in g.set_index(['k','gd'])[CAND].iterrows()} if False else None
G2=g.drop_duplicates(['k','gd']).set_index(['k','gd'])[CAND]
L['k']=L.player.map(nm); L['gd']=pd.to_datetime(L.slate_date); inf=0; skip=0
for i in L.index[L.col.isna()]:
    r=L.loc[i]; key=(r.k,r.gd)
    if key not in G2.index or pd.isna(r.actual_value): skip+=1; continue
    row=G2.loc[key]; c=[x for x in CAND if row[x]==float(r.actual_value)]
    if not c: skip+=1; continue
    if len(c)>1:
        sr=ser0.get(r.k)
        if sr is None: skip+=1; continue
        pr=sr[sr.gd<r.gd]
        if len(pr)<8: skip+=1; continue
        dist=sorted((abs(float(r.line)-pr[x].tail(20).mean()),x) for x in c)
        if len(dist)>1 and dist[0][0]*2>dist[1][0]: skip+=1; continue
        c=[dist[0][1]]
    L.at[i,'col']=c[0]; inf+=1
print('inferred stat for',inf,'legs; skipped',skip)
L=L[L.col.notna()]; L['k']=L.player.map(nm); L['gd']=pd.to_datetime(L.slate_date)
L['side']=np.where(L.direction.str.upper().str.startswith('O'),1,-1); L['pt']=L.pick_type.str.capitalize(); L=L.drop_duplicates(['k','gd','col','line','side','pt'])
rows=[]
for r in L.itertuples():
    s=ser.get(r.k)
    if s is None: rows.append((np.nan,)*5);continue
    v=s[s.gd<r.gd][r.col].values.astype(float)
    if len(v)<8: rows.append((np.nan,)*5);continue
    hit=(v>r.line) if r.side==1 else (v<r.line); sd=max(float(np.std(v[-20:])),0.3)
    gp=[r.side*(v[-w:].mean()-r.line) for w in (5,10,20)]
    rows.append((hit[-5:].mean(),hit[-10:].mean(),hit[-20:].mean(),min(gp)/sd,len(v)))
F=pd.DataFrame(rows,columns=['h5','h10','h20','zmin','n'],index=L.index); L=pd.concat([L,F],axis=1).dropna(subset=['h5']); L['hit']=L.hit.astype(float)
L.to_pickle('/tmp/cbb_ds.pkl'); print('rows',len(L),L.gd.min().date(),L.gd.max().date(),L.pt.value_counts().to_dict())
T=L.gd>=pd.Timestamp('2026-03-10')
def f(nm,a):
    t=a[a.gd>=pd.Timestamp('2026-03-10')]; p=a[a.gd<pd.Timestamp('2026-03-10')]
    print(f'  {nm:36s} {a.hit.mean()*100:5.1f}% n={len(a):5d} | early {p.hit.mean()*100 if len(p)>=20 else float("nan"):5.1f}% n={len(p):4d} | late(Mar10+) {t.hit.mean()*100 if len(t)>=20 else float("nan"):5.1f}% n={len(t):4d}')
for pt,side,nm,gates in [('Goblin',1,'Goblin Over',[('all',lambda x:x.h5>=0),('L5 5/5,L10>=90,L20>=80',lambda x:(x.h5>=1)&(x.h10>=.9)&(x.h20>=.8)),('+gap>=1SD',lambda x:(x.h5>=1)&(x.h10>=.9)&(x.h20>=.8)&(x.zmin>=1))]),
  ('Standard',-1,'Standard Under',[('all',lambda x:x.h5>=0),('L5/L10>=80,L20>=65,gap>=.5',lambda x:(x.h5>=.8)&(x.h10>=.8)&(x.h20>=.65)&(x.zmin>=.5)),('L10>=70,L20>=60,gap>=.5',lambda x:(x.h10>=.7)&(x.h20>=.6)&(x.zmin>=.5)),('L10>=70,L20>=60,gap>=1',lambda x:(x.h10>=.7)&(x.h20>=.6)&(x.zmin>=1))]),
  ('Standard',1,'Standard Over',[('all',lambda x:x.h5>=0),('L5/L10>=80,L20>=65,gap>=.25',lambda x:(x.h5>=.8)&(x.h10>=.8)&(x.h20>=.65)&(x.zmin>=.25)),('L10>=70,L20>=60,gap>=1',lambda x:(x.h10>=.7)&(x.h20>=.6)&(x.zmin>=1))]),
  ('Demon',1,'Demon Over',[('all',lambda x:x.h5>=0),('L10>=60,L20>=50',lambda x:(x.h10>=.6)&(x.h20>=.5))])]:
    x=L[(L.pt==pt)&(L.side==side)]; print('==',nm)
    for gn,fn in gates: f(gn,x[fn(x)])
