import _r2,pandas as pd,numpy as np
d=pd.read_csv('../PropORACLE_main_cp/data/training/graded_export_20260516.csv',low_memory=False)
d=d[d.result_binary.isin([0,1])&d.ml_prob.notna()].copy(); d['pt']=d.pick_type.astype(str).str.lower(); d=d[d.pt.isin(['standard','goblin','demon'])]
d['dir']=d.direction.astype(str).str.upper(); d['game']=d.apply(lambda r:'|'.join(sorted([str(r.team),str(r.opp_team)])),axis=1)
print('rows',len(d),'ml_prob range',d.ml_prob.min(),d.ml_prob.max())
# calibration
d['pb']=pd.cut(d.ml_prob,[0,.4,.5,.6,.7,.8,1.01]); print(d.groupby(['pt','pb'],observed=True).result_binary.agg(['count','mean']).round(2).to_string())
def build(day,k,sport=None):
    x=day if sport is None else day[day.sport==sport]
    x=x.sort_values('ml_prob',ascending=False); legs=[];pl=set();gc={}
    for r in x.itertuples():
        if r.player in pl or gc.get((r.sport,r.game),0)>=2: continue
        legs.append(r); pl.add(r.player); gc[(r.sport,r.game)]=gc.get((r.sport,r.game),0)+1
        if len(legs)==k: break
    if len(legs)<k: return None
    if len({l.team for l in legs})<2: return None
    return legs
out=[]
for pt in ['standard','goblin','demon']:
    X=d[d.pt==pt]
    for k in [2,3,4,5,6]:
        for scope in ['ALL sports']+sorted(X.sport.unique()):
            wins=n=0; legp=[]; flex=0
            for dt,day in X.groupby('file_date'):
                L=build(day,k,None if scope=='ALL sports' else scope)
                if L is None: continue
                n+=1; h=sum(l.result_binary for l in L); wins+=(h==k); flex+=(h>=k-1); legp.append(np.mean([l.ml_prob for l in L]))
            if n>=15: out.append(dict(type=pt,legs=k,scope=scope,days=n,all_hit_pct=100*wins/n,miss_by_one_or_less_pct=100*flex/n,avg_ml_prob=np.mean(legp)*100))
R=pd.DataFrame(out); R.to_csv('outputs/ticket_cross_bt.csv',index=False)
pd.set_option('display.width',200); print(R.round(2).to_string(index=False))
