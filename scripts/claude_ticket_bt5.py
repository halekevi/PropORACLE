import pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[(d.pt=='Standard')&(d.side==-1)&d.hit.notna()].copy(); d['hit']=d.hit.astype(float)
d['gm']=d.apply(lambda r:'|'.join(sorted([str(r.team),str(r.opp)])),axis=1)
import sys
GATE=sys.argv[1]
d=d[((d.h5>=1)&(d.h10>=.7)) if GATE=='L5' else ((d.h5>=.8)&(d.h10>=.8)&(d.h20>=.65)&(d.zmin>=.5))]
print('GATE',GATE,'legs',len(d),'hit',round(d.hit.mean()*100,1),'days',d.sdate.nunique(),'days>=3 players',(d.groupby('sdate').k.nunique()>=3).sum())
PAY={2:3.0,3:6.0,4:10.0,5:20.0,6:40.0}
def build(day,n):
    out=[];pl=set();gc={}
    for r in day.sort_values('zmin',ascending=False).itertuples():
        if r.k in pl or gc.get(r.gm,0)>=2: continue
        out.append(r);pl.add(r.k);gc[r.gm]=gc.get(r.gm,0)+1
        if len(out)==n:
            return out if len({x.team for x in out})>1 else None
    return None
def tickets(minpool,n,multi):
    rows=[]
    for dt,day in d.groupby('sdate'):
        # pool size = unique players available after constraints
        if build(day,minpool) is None and minpool>0: continue
        day=day.copy()
        while True:
            t=build(day,n)
            if t is None: break
            rows.append((dt,float(all(x.hit==1 for x in t)),np.mean([x.hit for x in t])))
            day=day.drop(index=[x.Index for x in t])
            if not multi: break
    return pd.DataFrame(rows,columns=['d','win','leg'])
rng=np.random.default_rng(0)
def summ(r,n):
    if len(r)==0: return 'none'
    g=r.groupby('d').win.agg(['sum','count']); a,b=g['sum'].values,g['count'].values
    bs=[(a[i].sum()/b[i].sum())*PAY[n]-1 for i in (rng.integers(0,len(a),len(a)) for _ in range(1000))]
    h=r[r.d>=pd.Timestamp('2026-05-09')]
    return f'{r.d.nunique():3d} days {len(r):4d} tix win {r.win.mean()*100:4.1f}% leg {r.leg.mean()*100:4.1f}% ROI {(r.win.mean()*PAY[n]-1)*100:+4.0f}% (95% CI {np.percentile(bs,2.5)*100:+4.0f}..{np.percentile(bs,97.5)*100:+4.0f}) | hold {len(h):3d} tix ROI {(h.win.mean()*PAY[n]-1)*100 if len(h) else float("nan"):+4.0f}%'
print('ONE ticket per day, only days with a pool of at least N+k legs (k = extra choice)')
for n in (2,3,4,5,6):
    for k in (0,2):
        r=tickets(n+k,n,False); print(f' {n}-leg, pool>={n+k}: {summ(r,n)}')
print('\nMULTIPLE disjoint tickets per day, days with pool >= N+2')
for n in (2,3,4):
    r=tickets(n+2,n,True); print(f' {n}-leg: {summ(r,n)}')
