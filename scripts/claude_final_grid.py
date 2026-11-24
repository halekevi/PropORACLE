import pandas as pd,numpy as np,itertools,warnings; warnings.filterwarnings('ignore')
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[(d.pt=='Standard')&d.hit.notna()].copy(); d['hit']=d.hit.astype(float)
d['gm']=d.apply(lambda r:'|'.join(sorted([str(r.team),str(r.opp)])),axis=1)
H=d.sdate>=pd.Timestamp('2026-05-09')
def wl(h,n,z=1.64):
    p=h/n; return (p+z*z/2/n-z*np.sqrt(p*(1-p)/n+z*z/4/n/n))/(1+z*z/n)
G=list(itertools.product([0,.6,.8,1],[0,.7,.8,.9],[0,.6,.65,.8],[0,.25,.5,.75,1],[0,1],[0,1]))  # h5,h10,h20,zmin,clean,opp
best={}
for side,nm in ((-1,'Under'),(1,'Over')):
    z=d[d.side==side]; pre=z[~H[z.index]]; ho=z[H[z.index]]
    res=[]
    for h5,h10,h20,zm,cl,op in G:
        f=lambda x:(x.h5>=h5)&(x.h10>=h10)&(x.h20>=h20)&(x.zmin>=zm)&((x.clean)|(cl==0))&((x.r>=1.07)|(op==0))
        a=pre[f(pre)]
        if len(a)<100 or a.sdate.nunique()<20: continue
        b=ho[f(ho)]
        res.append((wl(a.hit.sum(),len(a)),a.hit.mean(),len(a),a.sdate.nunique(),b.hit.mean() if len(b) else np.nan,len(b),(h5,h10,h20,zm,cl,op)))
    res.sort(reverse=True); best[nm]=res
    print(f'\n=== STANDARD {nm}: best gates chosen on pre-May-9 data (n>=100, 20+ days), checked on holdout')
    print(' base all:',round(z.hit.mean()*100,1),'| hold base',round(ho.hit.mean()*100,1))
    for r in res[:8]: print(f'  gate(h5,h10,h20,zSD,clean,opp)={r[6]} pre {r[1]*100:5.1f}% n={r[2]:4d} days={r[3]:2d} | hold {r[4]*100:5.1f}% n={r[5]:4d}')
# per-prop best legs (Under and Over), top by pre lower bound, show hold
print('\n=== best per prop (Standard), gate chosen on pre data')
for side,nm in ((-1,'Under'),(1,'Over')):
    z=d[d.side==side]; out=[]
    for prop,g in z.groupby('prop_canon'):
        pre=g[~H[g.index]]; ho=g[H[g.index]]; bst=None
        for h5,h10,h20,zm,cl,op in G:
            f=lambda x:(x.h5>=h5)&(x.h10>=h10)&(x.h20>=h20)&(x.zmin>=zm)&((x.clean)|(cl==0))&((x.r>=1.07)|(op==0))
            a=pre[f(pre)]
            if len(a)<50: continue
            s=wl(a.hit.sum(),len(a))
            if bst is None or s>bst[0]: b=ho[f(ho)]; bst=(s,a.hit.mean(),len(a),b.hit.mean() if len(b) else np.nan,len(b))
        if bst: out.append((prop,)+bst[1:])
    print(nm); [print(f'  {p:12s} pre {a*100:5.1f}% (n={n}) hold {h*100:5.1f}% (n={m})') for p,a,n,h,m in sorted(out,key=lambda t:-(t[3] if t[3]==t[3] else 0))]
import pickle; pickle.dump(best,open('/tmp/best_gates.pkl','wb'))
