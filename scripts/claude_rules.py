import pandas as pd,numpy as np,json,math,warnings; warnings.filterwarnings('ignore')
t=pd.read_pickle('/tmp/ds3.pkl'); F=json.load(open('/tmp/F.json'))
t['gap_all']=(t[['a5','a10','a15','a20']].min(axis=1)>0).astype(float)
F=F+['gap_all']
def lbz(y,z=1.0):
    n=len(y); p=y.mean()
    return (p+z*z/(2*n)-z*math.sqrt(p*(1-p)/n+z*z/(4*n*n)))/(1+z*z/n) if n else 0
def mine(tr,Fu,minN,maxc=3,beam=6):
    # beam search over conjunctions of threshold conditions; objective Wilson LB on train
    cands=[]
    for f in Fu:
        col=tr[f]; u=col.dropna().unique()
        if len(u)<2: continue
        qs=sorted(set(np.round(col.quantile([.15,.3,.45,.6,.75,.9]).values,4))) if len(u)>4 else sorted(u)[:-1]
        for q in qs:
            for op in ('>=','<='): cands.append((f,op,q,(tr[f]>=q) if op=='>=' else (tr[f]<=q)))
    beams=[((),pd.Series(True,index=tr.index),lbz(tr.y))]; best=None
    for depth in range(maxc):
        new=[]
        for rules,mask,_ in beams:
            used={r[0] for r in rules}
            for f,op,q,cm in cands:
                if f in used: continue
                m=mask&cm.fillna(False); n=m.sum()
                if n<minN: continue
                new.append((rules+((f,op,q),),m,lbz(tr.y[m])))
        new.sort(key=lambda z:-z[2]); beams=new[:beam]
        if not beams: break
        if best is None or beams[0][2]>best[2]: best=beams[0]
        yield beams
def ap(df,rules):
    m=pd.Series(True,index=df.index)
    for f,op,q in rules: m&=((df[f]>=q) if op=='>=' else (df[f]<=q)).fillna(False)
    return m
groups={'STANDARD OVER':(t.pt=='Standard')&(t.side==1),'STANDARD UNDER':(t.pt=='Standard')&(t.side==-1),'DEMON':(t.pt=='Demon')}
out={}
for name,m in groups.items():
    x=t[m]; tr=x[x.split=='train']; va=x[x.split=='val']
    Fu=[f for f in F if x[f].notna().mean()>.6 and not (f.startswith('rung') and name!='DEMON')]
    print(f'\n=== {name}  train {len(tr)} ({tr.y.mean():.3f}) val {len(va)} ({va.y.mean():.3f})')
    minN=max(150,int(.015*len(tr))); final=[]
    for beams in mine(tr,Fu,minN):
        for rules,mk,s in beams[:3]:
            mv=ap(va,rules); final.append((rules,int(mk.sum()),tr.y[mk].mean(),int(mv.sum()),va.y[mv].mean() if mv.sum() else np.nan))
    final=[z for z in final if z[3]>=60]; final.sort(key=lambda z:-z[4])
    seen=set(); k=0
    for rules,ntr,htr,nva,hva in final:
        key=tuple(r[0] for r in rules)
        if key in seen: continue
        seen.add(key); k+=1
        print('  ',' & '.join(f'{f}{op}{q:g}' for f,op,q in rules),f'| train n={ntr} {htr*100:.1f}% -> VAL n={nva} {hva*100:.1f}%')
        if k>=6: break
    out[name]=[(r,) for r,*_ in final[:12]]
import pickle; pickle.dump({k:[z[0][0] if isinstance(z,tuple) else z for z in v] for k,v in out.items()},open('/tmp/rules.pkl','wb'))
