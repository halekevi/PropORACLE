import pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[(d.pt=='Standard')&d.hit.notna()].copy(); d['hit']=d.hit.astype(float)
d['clean']=(d.n>=20)&(d.mn10>=20)&(d.mnfl5>=12)&(d.mnsd<=6)&(d.mntrend>-4)&(d.dsl<=7)&(d.line>=1.5)
d['sdate']=pd.to_datetime(d.slate_date); d['gm']=d.apply(lambda r:'|'.join(sorted([str(r.team),str(r.opp)])),axis=1)
PAY={2:3.0,3:6.0,4:10.0,5:20.0,6:40.0}
def pool(side,mode):
    z=d[d.side==side]
    m=(z.h5>=.8)&(z.h10>=.8)&(z.h20>=.65)&(z.zmin>=.5)
    if mode=='opp': m&=(z.r>=1.07)
    if mode=='clean': m&=z.clean
    if mode=='opp+clean': m&=(z.r>=1.07)&z.clean
    if mode=='primary': m&=(z.trank<=2)
    if mode=='role': m&=(z.trank>=3)
    return z[m]
def build(day,n):
    out=[];pl=set();gc={};tm=[]
    for r in day.sort_values('zmin',ascending=False).itertuples():
        if r.k in pl or gc.get(r.gm,0)>=2: continue
        out.append(r);pl.add(r.k);gc[r.gm]=gc.get(r.gm,0)+1;tm.append(r.team)
        if len(out)==n:
            if len(set(tm))>1: return out
            out.pop();pl.discard(r.k);gc[r.gm]-=1;tm.pop()
    return None
def run(p,n,label,multi=False):
    res=[]
    for dt,day in p.groupby('sdate'):
        day=day.copy()
        while True:
            t=build(day,n)
            if t is None: break
            res.append((dt,all(x.hit==1 for x in t),np.mean([x.hit for x in t])))
            used={x.Index for x in t}; day=day.drop(index=list(used))
            if not multi: break
    if not res: print(f'{label:34s} no tickets'); return
    r=pd.DataFrame(res,columns=['d','win','leg']); 
    def f(x): return f'{len(x):4d} tix win {x.win.mean()*100:4.1f}% leg {x.leg.mean()*100:4.1f}% ROI {(x.win.mean()*PAY[n]-1)*100:+5.0f}%'
    h=r[r.d>=pd.Timestamp('2026-05-09')]
    print(f'{label:34s} all: {f(r)} | hold: {f(h) if len(h) else "-"} | days {r.d.nunique()}')
p=pool(-1,'gate')
for n in (3,4,5,6): run(p,n,f'UNDER gate {n}-leg (disjoint)',multi=True)
for n in (3,4,5,6): run(p,n,f'UNDER gate {n}-leg (1/day)')
p=pool(-1,'opp')
for n in (4,5): run(p,n,f'UNDER gate+opp {n}-leg (disjoint)',multi=True)
