import _r2
"""Tier gates per prop (Standard Over/Under, Goblin Over). Rules chosen on TRAIN (<May 9, 2026) only, then scored on TEST. Layers: hit-rate gate, SD gap, defense (skip elite), minutes (<36), role (rank>=3)."""
import pandas as pd,numpy as np,itertools,sys,warnings; warnings.filterwarnings('ignore'); sys.path.insert(0,'scripts')
from claude_defense_rel import table
T=table(); AB={'SAS':'SA','GSW':'GS','NOP':'NO','NYK':'NY','UTA':'UTAH','WAS':'WSH','PHO':'PHX','BRK':'BKN'}
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[d.hit.notna()&d.pt.isin(['Standard','Goblin'])&((d.pt=='Standard')|(d.side==1))].copy()
d['te']=d.sdate>=pd.Timestamp('2026-05-09'); d['S']=np.where(d.side==1,'Over','Under')
nba=[AB.get(o,o) for o in d.opp.dropna().unique()]
tierof={}
for c in d.col.unique():
    if c not in T.columns: continue
    r=T.loc[[t for t in nba if t in T.index],c].rank(method='first'); tierof[c]=(r<=len(r)/5)  # elite = tightest 20%
d['elite']=[bool(tierof.get(c,pd.Series(dtype=bool)).get(AB.get(o,o),False)) if c in tierof else False for o,c in zip(d.opp,d.col)]
H={'none':lambda x:x.h5>=0,'L5 5/5':lambda x:x.h5>=1,'L10 70%+':lambda x:x.h10>=.7,'L10 80%+':lambda x:x.h10>=.8,'L20 70%+':lambda x:x.h20>=.7,'Season 60%+':lambda x:(x.hs>=.6)&(x.n>=20)}
Z={'':lambda x:x.zmin>-99,'SDgap>=0.5':lambda x:x.zmin>=.5,'SDgap>=1':lambda x:x.zmin>=1}
F={'':lambda x:x.hit>=0,'no elite D':lambda x:~x.elite,'min<36':lambda x:x.mn10<36,'role rank>=3':lambda x:x.trank>=3}
def hr(x): return (x.hit.mean()*100,len(x)) if len(x) else (np.nan,0)
rows=[]
for (pt,col,S),x in d.groupby(['pt','col','S']):
    tr=x[~x.te]; te=x[x.te]
    if len(tr)<80 or len(te)<30: continue
    best=None
    for (hn,hf),(zn,zf) in itertools.product(H.items(),Z.items()):
        m=tr[hf(tr)&zf(tr)]; r,n=hr(m)
        if n>=30 and (best is None or r>best[0]): best=(r,n,hn,zn)
    if best is None: continue
    _,_,hn,zn=best; base=lambda y:y[H[hn](y)&Z[zn](y)]
    cur=[]
    def fin(y): return hr(base(y))
    g=base(x); ge=g[g.elite]; gn=g[~g.elite]; gl=g[g.mn10>=36]; gr=g[g.trank<=2]
    rt,nt=fin(tr); re,ne=fin(te); r0,n0=hr(te)
    tier='D'
    if nt>=30 and ne>=15:
        mn=min(rt,re); tier='S' if mn>=65 else 'A' if mn>=60 else 'B' if mn>=55 else 'C' if mn>=50 else 'D'
    rows.append(dict(pick=pt,prop=col,side=S,rule=' + '.join([hn]+([zn] if zn else [])+cur),train_pct=round(rt,1),train_n=nt,test_pct=round(re,1),test_n=ne,test_ungated_pct=round(r0,1),test_ungated_n=n0,lift_pts=round(re-r0,1),elite_D_pct=round(hr(ge)[0],1),elite_D_n=len(ge),other_D_pct=round(hr(gn)[0],1),mn36plus_pct=round(hr(gl)[0],1),mn36plus_n=len(gl),top2_role_pct=round(hr(gr)[0],1),top2_role_n=len(gr),tier=tier,thin='thin' if ne<30 else ''))
R=pd.DataFrame(rows).sort_values(['tier','test_pct'],ascending=[True,False]); R.to_csv('outputs/prop_tiers_v2_2026-11-11.csv',index=False)
print(R.groupby(['pick','side']).tier.value_counts().unstack(fill_value=0))
pd.set_option('display.width',250); print(R[R.tier.isin(['S','A','B'])][['pick','prop','side','rule','train_pct','train_n','test_pct','test_n','tier','elite_D_pct','elite_D_n','other_D_pct']].to_string(index=False)); g=R[R.elite_D_n>=15]; print('\nrows w/ 15+ elite-D picks:',len(g),' elite-D lower than other-D in',(g.elite_D_pct<g.other_D_pct).sum(),'; mean elite',round(g.elite_D_pct.mean(),1),'other',round(g.other_D_pct.mean(),1))
