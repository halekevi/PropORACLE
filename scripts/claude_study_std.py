import numpy as np,pandas as pd,warnings; warnings.filterwarnings('ignore')
d=pd.read_pickle('/tmp/study_ds4.pkl'); y=d[d.pt=='Standard'].copy()
def rep(a,name):
    h=a[a.ho]; return f'  {name:58s} all {a.hit.mean()*100:5.1f}% (n={len(a):5d}) | hold {h.hit.mean()*100 if len(h)>=5 else float("nan"):5.1f}% (n={len(h):4d})'
for side,tag in ((1,'OVER'),(-1,'UNDER')):
    z=y[y.side==side]
    print(f'\n=== STANDARD {tag}  base all {z.hit.mean()*100:.1f}% hold {z[z.ho].hit.mean()*100:.1f}%')
    st=z.cvpct<=.33   # player steady on THIS stat
    print(rep(z[st],'player steady on this stat (lowest-third swings)'))
    print(rep(z[z.cvpct<=.2],'  very steady (lowest 20%)'))
    for zm in (0.25,0.5,0.75,1.0):
        print(rep(z[st&(z.zmin>=zm)],f'steady + gap>={zm} SD (all 3 windows)'))
    print(rep(z[st&(z.h10>=.7)&(z.h20>=.6)],'steady + L10>=70 + L20>=60'))
    print(rep(z[st&(z.h5>=.8)&(z.h10>=.8)],'steady + L5,L10>=80'))
    print(rep(z[st&(z.h5>=.8)&(z.h10>=.8)&(z.zmin>=.25)],'steady + L5,L10>=80 + gap>=.25SD'))
    print(rep(z[st&(z.h5>=.8)&(z.h10>=.8)&(z.zmin>=.25)&z.clean],'  + noise filter'))
    print(rep(z[st&(z.h10>=.7)&(z.zmin>=.5)&z.clean],'steady + L10>=70 + gap>=.5SD + noise filter'))
    pr=(z.ph>=.65)&(z.ph_n>=6)
    print(rep(z[pr],'player prior hit rate on prop >=65% (n>=6)'))
    print(rep(z[pr&st],'  + steady'))
    print(rep(z[pr&st&(z.h10>=.7)],'  + steady + L10>=70'))
    print(rep(z[pr&(z.h10>=.7)&z.clean],'prior>=65 + L10>=70 + noise filter'))
    # by prop: best player-steady rule hit
    print('  by prop for [steady + L10>=70 + L20>=60]:')
    s=z[st&(z.h10>=.7)&(z.h20>=.6)]
    for p,g in s.groupby('prop_canon'):
        if len(g)>=40: print(f'     {p:12s} {g.hit.mean()*100:5.1f}% (n={len(g)}) hold {g[g.ho].hit.mean()*100 if len(g[g.ho])>=10 else float("nan"):5.1f}% (n={len(g[g.ho])})')
