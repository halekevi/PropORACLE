import pandas as pd,numpy as np,math,warnings,sys; warnings.filterwarnings('ignore')
d=pd.read_pickle('/tmp/nba_rt.pkl'); d['side']=d.direction.str.upper().str[0]; d['fd']=pd.to_datetime(d.file_date)
d=d[(d.fd<'2026-06-11')|True]
d['proj_gap']=np.where(d.side=='O',d.projection-d.line,d.line-d.projection); d['proj_gap_pct']=d.proj_gap/d.line.clip(lower=.5)*100
tm={'A':3,'B':2,'C':1,'D':0}; d['tier_n']=d.tier.map(tm)
d['conf_n']=d.confidence_tier.map({'HIGH':2,'MED':1,'LOW':0})
d['min_n']=d.minutes_tier.map({'HIGH':3,'3':3,'MEDIUM':2,'2':2,'LOW':1,'1':1,'0':0,'UNKNOWN':np.nan})
d['def_n']=d.def_tier.map({'Elite':0,'Above Avg':1,'Avg':2,'Below Avg':3,'Weak':3})
d['role_n']=d.role_tier.map({'PRIMARY':2,'SECONDARY':1,'SUPPORT':0})
d['usage_n']=d.usage_tier.map({'star':3,'high':2,'medium':1,'role':0})
d['pace_n']=d.pace_context.map({'high_pace':2,'medium_pace':1,'low_pace':0})
d['playmaker']=(d.usage_role_type=='playmaker').astype(float); d['rebounder']=(d.usage_role_type=='rebounder').astype(float); d['scorer']=(d.usage_role_type=='scorer').astype(float)
d['l5_same']=np.where(d.side=='O',d.l5_over,d.l5_under)
d['hr_pick']=np.where(d.side=='O',d.line_hit_rate,1-d.line_hit_rate)  # line_hit_rate assumed over-rate
num=['edge','rank_score','projection','proj_gap','proj_gap_pct','composite_hit_rate','hit_rate_l5','l5_same','confidence_score','usage_pct','pace_delta','opp_def_rating','opp_pace','team_pace','game_pace','reb_pct','ast_pct','line','tier_n','conf_n','min_n','def_n','role_n','usage_n','pace_n','playmaker','rebounder','scorer','opp_pts_allowed_vs_position','opp_reb_allowed_vs_position','opp_ast_allowed_vs_position','ml_prob','ml_edge','opp_hr_historical']
num=[c for c in num if c in d]
def lb(y,z=1.0):
    n=len(y)
    if n==0: return 0
    p=y.mean(); return (p+z*z/(2*n)-z*math.sqrt(p*(1-p)/n+z*z/(4*n*n)))/(1+z*z/n)
def mine(tr,minN,maxc=2):
    rules=[];mask=pd.Series(True,index=tr.index);best=lb(tr.result_binary)
    for _ in range(maxc):
        cand=None
        for f in num:
            col=tr[f]
            if col.notna().sum()<minN*2 or col.nunique()<2: continue
            qs=sorted(set(col.quantile([.2,.35,.5,.65,.8]).round(4))) if col.nunique()>4 else sorted(col.dropna().unique())[:-1]
            for q in qs:
                for op in ('>=','<='):
                    m=mask&((col>=q) if op=='>=' else (col<=q)).fillna(False)
                    if m.sum()<minN: continue
                    s=lb(tr.result_binary[m])
                    if s>best+0.003 and (cand is None or s>cand[0]): cand=(s,f,op,q,m)
        if cand is None: break
        best,f,op,q,mask=cand; rules.append((f,op,q))
    return rules,mask
def apply(df,rules):
    m=pd.Series(True,index=df.index)
    for f,op,q in rules: m&=((df[f]>=q) if op=='>=' else (df[f]<=q)).fillna(False)
    return m
out=[]
for pt in ['Standard','Goblin']:
    x0=d[d.pick_type==pt]; cut=x0.fd.quantile(.6)
    for (p,s),x in x0.groupby(['prop','side']):
        x=x[~x.prop.str.contains('Combo')]
        tr=x[x.fd<cut];te=x[x.fd>=cut]
        if len(tr)<300 or len(te)<150: continue
        base_te=te.result_binary.mean()
        rules,m=mine(tr,max(60,int(.06*len(tr))))
        if not rules: out.append((pt,p,s,len(x),round(x.result_binary.mean()*100,1),'(no gate beats base)',0,np.nan,0,np.nan,round(base_te*100,1))); continue
        mt=apply(te,rules)
        out.append((pt,p,s,len(x),round(x.result_binary.mean()*100,1),' & '.join(f'{f}{o}{q:g}' for f,o,q in rules),int(m.sum()),round(tr.result_binary[m].mean()*100,1),int(mt.sum()),round(te.result_binary[mt].mean()*100,1) if mt.sum() else np.nan,round(base_te*100,1)))
r=pd.DataFrame(out,columns=['pick_type','prop','side','n','base%','gate(train)','n_tr','hit_tr%','n_te','hit_te%','te_base%'])
r['lift']=(r['hit_te%']-r['te_base%']).round(1)
pd.set_option('display.width',270,'display.max_colwidth',80,'display.max_rows',200)
r.to_csv('outputs/nba_best_gate_per_prop_v2.csv',index=False)
for pt in ['Standard','Goblin']:
    z=r[r.pick_type==pt]; print('\n=====',pt); print(z.drop(columns='pick_type').to_string(index=False))
    k=z[z.n_te>=30]; print('  gated test hit (n-weighted):',round((k['hit_te%']*k.n_te).sum()/k.n_te.sum(),1),'vs their base',round((k['te_base%']*k.n_te).sum()/k.n_te.sum(),1),'n',k.n_te.sum())
