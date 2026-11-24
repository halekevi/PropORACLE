import pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
T={'A: most skill/role':['fg2a','fga','fg3a','pa','pra','ast','pr','pts','fg2m','fantasy_score','reb','ra'],
   'B: good':['fgm','fta','ftm','dreb'],'C: mixed':['oreb','fg3m','tov'],'D: mostly luck':['blk','bs','pf','stl']}
inv={c:t for t,cs in T.items() for c in cs}
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[d.hit.notna()].copy(); d['hit']=d.hit.astype(float); d['tier']=d.col.map(inv); d=d.dropna(subset=['tier'])
H=d.sdate>=pd.Timestamp('2026-05-09')
def rep(a,nm):
    h=a[H[a.index]]; print(f'  {nm:22s} {a.hit.mean()*100:5.1f}% n={len(a):5d} | hold {h.hit.mean()*100 if len(h)>=20 else float("nan"):5.1f}% n={len(h):4d}')
gates={'Goblin Over':(d.pt=='Goblin')&(d.side==1)&(d.h5>=1)&(d.h10>=.9)&(d.h20>=.8),
 'Standard Under':(d.pt=='Standard')&(d.side==-1)&(d.h5>=.8)&(d.h10>=.8)&(d.h20>=.65)&(d.zmin>=.5),
 'Standard Over':(d.pt=='Standard')&(d.side==1)&(d.h5>=.8)&(d.h10>=.8)&(d.h20>=.65)&(d.zmin>=.25),
 'Demon Over':(d.pt=='Demon')&(d.side==1)&(d.h10>=.6)&(d.h20>=.5)}
for g,m in gates.items():
    print('==',g,'(gated)')
    x=d[m]
    for t in T: rep(x[x.tier==t],t)
    rep(x[x.tier.str[0].isin(['A','B'])],'A+B together')
