import numpy as np,pandas as pd,warnings; warnings.filterwarnings('ignore')
d=pd.read_pickle('/tmp/study_ds3.pkl')
d=d.sort_values('sdate')
# player-level prior hit rate across ALL props of the same pick type+side, using only earlier dates
agg=d.groupby(['k','pt','side','sdate']).hit.agg(['sum','count']).reset_index().sort_values('sdate')
agg['cs']=agg.groupby(['k','pt','side'])['sum'].cumsum()-agg['sum']
agg['cc']=agg.groupby(['k','pt','side'])['count'].cumsum()-agg['count']
d=d.merge(agg[['k','pt','side','sdate','cs','cc']],on=['k','pt','side','sdate'])
d['pl_hr']=d.cs/d.cc.replace(0,np.nan); d['pl_n']=d.cc
# player steadiness: avg cv percentile across his props that date
ps=d.groupby(['k','sdate','pt']).cvpct.mean().rename('pl_cvpct').reset_index()
d=d.merge(ps,on=['k','sdate','pt'])
d.to_pickle('/tmp/study_ds4.pkl')
def rep(a,name):
    h=a[a.ho]; return f'  {name:50s} all {a.hit.mean()*100:5.1f}% (n={len(a):5d}) | hold {h.hit.mean()*100 if len(h) else float("nan"):5.1f}% (n={len(h):4d})'
for pt in ['Goblin','Standard','Demon']:
    y=d[d.pt==pt]
    gate=((y.h5>=1)&(y.h10>=.9)&(y.h20>=.8)) if pt=='Goblin' else ((y.h5>=.8)&(y.h10>=.8)&(y.h20>=.65))
    for side in ([1] if pt!='Standard' else [1,-1]):
        z=y[y.side==side]; gz=gate[z.index]; tag='Over' if side==1 else 'Under'
        print(f'\n=== {pt} {tag}  base all {z.hit.mean()*100:.1f}% hold {z[z.ho].hit.mean()*100:.1f}%')
        ok=z[z.pl_n>=30]
        print(rep(ok,'players w/ 30+ prior graded props (baseline)'))
        qs=[0,.5,.75,.9]
        thr={q:ok.pl_hr.quantile(q) for q in qs}
        for lo,hi in [(0,.5),(.5,.75),(.75,.9),(.9,1)]:
            a=ok[(ok.pl_hr>=ok.pl_hr.quantile(lo))&(ok.pl_hr<=ok.pl_hr.quantile(hi))]
            print(rep(a,f'player hit-rate pctile {int(lo*100)}-{int(hi*100)} ({a.pl_hr.min():.2f}-{a.pl_hr.max():.2f})'))
        top=ok[ok.pl_hr>=ok.pl_hr.quantile(.75)]
        print(rep(top,'TOP-QUARTILE players, all their props'))
        print(rep(top[gz[top.index]],'  + consistency gate'))
        print(rep(top[gz[top.index]&(top.cvpct<=.5)],'  + gate + prop steadier than median'))
        print(rep(top[gz[top.index]&top.clean],'  + gate + noise filter'))
        st=ok[ok.pl_cvpct<=ok.pl_cvpct.quantile(.33)]
        print(rep(st,'STEADIEST-third players (avg stat steadiness)'))
        print(rep(st[gz[st.index]],'  + consistency gate'))
        both=ok[(ok.pl_hr>=ok.pl_hr.quantile(.75))&(ok.pl_cvpct<=ok.pl_cvpct.quantile(.5))]
        print(rep(both[gz[both.index]],'top-quartile & steadier players + gate'))
