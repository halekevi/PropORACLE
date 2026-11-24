import pandas as pd,numpy as np,math,warnings; warnings.filterwarnings('ignore')
t=pd.read_pickle('/tmp/ctx3.pkl')
t['mg']=t[['a5','a10','a15','a20']].min(axis=1)/t.line*100
for w in ('5','10','15','20','s'):
    c='a'+w if w!='s' else 'as'; t['r'+w]=t[c]/t.line*100          # side-adjusted gap, % of line
t['abs_spread']=t.spread.abs(); t['min_trend']=t.mc
t['pos_G']=(t.pos2=='G').astype(int); t['pos_F']=(t.pos2=='F').astype(int); t['pos_C']=(t.pos2=='C').astype(int)
t['lvl']=t.ptier.map({'bottom':0,'mid':1,'top':2}).astype(float)
num=['votes','h5','h10','h15','h20','hs','r5','r10','r15','r20','rs','mg','opp_rel','min_trend','min20','min5','total','abs_spread','rest','line','lvl','b2b','home','pos_G','pos_F','pos_C']
cut=t.fd.quantile(.6)
def lb(y):  # Wilson lower bound z=1
    n=len(y); 
    if n==0: return 0
    p=y.mean(); z=1.0
    return (p+z*z/(2*n)-z*math.sqrt(p*(1-p)/n+z*z/(4*n*n)))/(1+z*z/n)
def mine(tr,minN,maxc=3):
    rules=[];mask=pd.Series(True,index=tr.index);best=lb(tr.y)
    for _ in range(maxc):
        cand=None
        for f in num:
            col=tr[f]; 
            if col.nunique()<2: continue
            qs=sorted(set(col.quantile([.2,.35,.5,.65,.8]).round(4))) if col.nunique()>3 else [0.5] if col.nunique()==2 else sorted(col.unique())[:-1]
            for q in qs:
                for op in ('>=','<='):
                    m=mask&((col>=q) if op=='>=' else (col<=q)).fillna(False)
                    if m.sum()<minN: continue
                    s=lb(tr.y[m])
                    if s>best+0.002 and (cand is None or s>cand[0]): cand=(s,f,op,q,m)
        if cand is None: break
        best,f,op,q,mask=cand; rules.append((f,op,q))
    return rules,mask
def apply(df,rules):
    m=pd.Series(True,index=df.index)
    for f,op,q in rules: m&=((df[f]>=q) if op=='>=' else (df[f]<=q)).fillna(False)
    return m
rows=[]
for (p,s),x in t.groupby(['prop','side']):
    tr=x[x.fd<cut];te=x[x.fd>=cut]
    if len(tr)<250 or len(te)<100: continue
    minN=max(40,int(.06*len(tr)))
    rules,m=mine(tr,minN)
    if not rules: rows.append((p,'Over' if s==1 else 'Under',len(x),round(x.y.mean()*100,1),'no gate beats base','','','','')); continue
    mt=apply(te,rules)
    desc=' & '.join(f'{f}{op}{q:g}' for f,op,q in rules)
    rows.append((p,'Over' if s==1 else 'Under',len(x),round(x.y.mean()*100,1),desc,int(m.sum()),round(tr.y[m].mean()*100,1),int(mt.sum()),round(te.y[mt].mean()*100,1) if mt.sum() else np.nan))
r=pd.DataFrame(rows,columns=['prop','side','n_all','base%','best_gate(train)','n_tr','hit_tr%','n_test','hit_test%'])
pd.set_option('display.width',260,'display.max_colwidth',90,'display.max_rows',100)
r['test_base%']=[round(t[(t.prop==a)&(t.side==(1 if b=='Over' else -1))&(t.fd>=cut)].y.mean()*100,1) for a,b in zip(r.prop,r.side)]
print(r.to_string(index=False)); r.to_csv('outputs/nba_best_gate_per_prop.csv',index=False)
ok=r[r.n_test>=25]; print('\nweighted test hit of gated picks:',round((ok['hit_test%']*ok.n_test).sum()/ok.n_test.sum(),1),'n',ok.n_test.sum(),'| their base:',round((ok['test_base%']*ok.n_test).sum()/ok.n_test.sum(),1))
