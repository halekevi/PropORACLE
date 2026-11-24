import pandas as pd,numpy as np,json,math,itertools,warnings; warnings.filterwarnings('ignore')
t=pd.read_pickle('/tmp/ds3.pkl'); F=json.load(open('/tmp/F.json'))
F=[f for f in F if f not in('prop_id','line')]
def lb(y,z=1.28):
    n=len(y)
    if n==0: return 0
    p=y.mean(); return (p+z*z/(2*n)-z*math.sqrt(p*(1-p)/n+z*z/(4*n*n)))/(1+z*z/n)
def cands(tr,Fu):
    out=[]
    for f in Fu:
        c=tr[f].dropna()
        if c.nunique()<2: continue
        qs=sorted(set(np.round(c.quantile([.2,.4,.6,.8]).values,4))) if c.nunique()>4 else sorted(c.unique())[:-1]
        for q in qs:
            for op in('>=','<='): out.append((f,op,q))
    return out
def m(df,r):
    f,op,q=r; return ((df[f]>=q) if op=='>=' else (df[f]<=q)).fillna(False)
G={'Std Over':lambda x:(x.pt=='Standard')&(x.side==1),'Std Under':lambda x:(x.pt=='Standard')&(x.side==-1),'Demon':lambda x:x.pt=='Demon'}
rows=[]
for gname,gf in G.items():
    gg=t[gf(t)]; Fu=[f for f in F if gg[f].notna().mean()>.6 and not(f.startswith('rung') and gname!='Demon')]
    for prop,x in gg.groupby('prop'):
        tr=x[x.split=='train']; va=x[x.split=='val']; ho=x[x.split=='hold']
        if len(tr)<300 or len(va)<100 or len(ho)<60: continue
        C=cands(tr,Fu); best=None
        sc=[]
        for r in C:
            mt=m(tr,r); n=mt.sum()
            if n<max(60,.08*len(tr)): continue
            sc.append((lb(tr.y[mt]),r))
        sc.sort(key=lambda z:-z[0]); top=[r for _,r in sc[:8]]
        combos=[(r,) for r in top]+[(a,b) for a,b in itertools.combinations(top,2) if a[0]!=b[0]]
        for rl in combos:
            mt=np.logical_and.reduce([m(tr,r) for r in rl]); mv=np.logical_and.reduce([m(va,r) for r in rl])
            if mt.sum()<50 or mv.sum()<30: continue
            ht,hv=tr.y[mt].mean(),va.y[mv].mean()
            if hv<=va.y.mean()+.03 or ht<=tr.y.mean()+.03: continue   # must beat baseline in BOTH periods
            s=min(ht,hv)
            if best is None or s>best[0]: best=(s,rl,mt.sum(),ht,mv.sum(),hv)
        row=dict(group=gname,prop=prop,n_hold=len(ho),base_hold=ho.y.mean())
        if best:
            rl=best[1]; mh=np.logical_and.reduce([m(ho,r) for r in rl])
            row.update(rule=' & '.join(f'{f}{op}{q:g}' for f,op,q in rl),tr_n=best[2],tr=best[3],va_n=best[4],va=best[5],ho_n=int(mh.sum()),ho=ho.y[mh].mean() if mh.sum() else np.nan)
        rows.append(row)
r=pd.DataFrame(rows); r.to_csv('outputs/per_prop_validated_rules.csv',index=False)
pd.set_option('display.width',250,'display.max_colwidth',70)
for g in G:
    print('\n==',g); z=r[r.group==g].copy()
    for c in('base_hold','tr','va','ho'): z[c]=(z[c]*100).round(1)
    print(z[['prop','base_hold','rule','tr','va','ho_n','ho']].to_string(index=False))
