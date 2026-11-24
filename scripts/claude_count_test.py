import pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[d.hit.notna()].copy(); d['hit']=d.hit.astype(float)
print(list(d.columns)[:40]); print(d.col.value_counts().head(20).to_dict())
S=d[d.pt=='Standard'].copy()
S['low']=S.line<=4.5
H=S.sdate>=pd.Timestamp('2026-05-09')
def rep(a,nm):
    h=a[H[a.index]]; print(f'  {nm:46s} all {a.hit.mean()*100:5.1f}% n={len(a):5d} | hold {h.hit.mean()*100 if len(h)>=15 else float("nan"):5.1f}% n={len(h):4d}')
for low in [True,False]:
    print('=== LOW-COUNT (line<=4.5)' if low else '=== HIGH-RANGE (line>4.5)')
    x=S[S.low==low]
    for side,nm in [(-1,'Under'),(1,'Over')]:
        y=x[x.side==side]; print(' ',nm); rep(y,'all Standard')
        rep(y[(y.h5>=.8)&(y.h10>=.8)&(y.h20>=.65)],'count gate (no gap)')
        rep(y[(y.h5>=.8)&(y.h10>=.8)&(y.h20>=.65)&(y.zmin>=.5)],'count gate + gap>=0.5SD')
        rep(y[(y.h10>=.9)&(y.h20>=.75)],'L10>=90, L20>=75')
        rep(y[(y.h5>=1)&(y.h10>=.8)&(y.h20>=.8)],'L5 5/5, L10>=80, L20>=80')
print('\n=== per stat, gated (count gate), Standard')
g=S[(S.h5>=.8)&(S.h10>=.8)&(S.h20>=.65)]
for c,x in g.groupby('col'):
    if len(x)>=40:
        for side in (-1,1):
            y=x[x.side==side]
            if len(y)>=25: rep(y,f'{c} {"U" if side<0 else "O"}')
