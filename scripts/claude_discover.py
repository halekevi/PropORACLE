import pandas as pd,numpy as np,warnings,math; warnings.filterwarnings('ignore')
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import roc_auc_score
from sklearn.inspection import permutation_importance
t=pd.read_pickle('/tmp/ds2.pkl')
L=t.line.clip(lower=.5)
t['mg']=t[['a5','a10','a15','a20']].min(axis=1)/L; 
for w in ('5','10','15','20','s'): t['r'+w]=(t['a'+w] if w!='s' else t['as'])/L
t['z_line']=(t.line-t.avg20)*t.side/t.sd20              # how many SD the line sits beyond the player's L20 mean (+ = harder for the pick)
t['cv']=t.sd20/t.avg20.clip(lower=.5)
t['line_vs_max']=(t.line-t.max20)/L; t['line_vs_p90']=(t.line-t.p90)/L
t['line_vs_med']=(t.line-t.med20)/L
t['rung']=np.where(t.pt=='Demon',(t.line-t.std_line)/t.sd20,np.nan)       # Demon spacing above Standard in SDs
t['rung_pct']=np.where(t.pt=='Demon',(t.line-t.std_line)/L,np.nan)
t['opp_rel']=(t.opp_al10/t.groupby('prop').opp_al10.transform('mean')-1)
t['opp_relS']=(t.opp_alS/t.groupby('prop').opp_alS.transform('mean')-1)
t['opp_sign']=t.opp_rel*t.side
t['mc']=t.mn5/t.mn20-1; t['mc_sign']=t.mc*t.side
t['abs_spread']=t.spread.abs(); t['new_st']=((t.st3>=.66)&(t.stprev<=.35)).astype(float); t['new_bn']=((t.st3<=.34)&(t.stprev>=.65)).astype(float)
t['pG']=(t.pos=='G').astype(int); t['pC']=(t.pos=='C').astype(int)
t['lvl']=t.groupby('prop').avg20.transform(lambda x:pd.qcut(x,5,labels=False,duplicates='drop')).astype(float)
t['votes']=sum((t[f'h{w}']>=.6).astype(int) for w in (5,10,15,20))+(t.hs>=.6).astype(int)+sum((t[f'a{w}']>0).astype(int) for w in (5,10,15,20))+(t['as']>0).astype(int)
pk={p:i for i,p in enumerate(sorted(t.prop.unique()))}; t['prop_id']=t.prop.map(pk)
t['team_star_out']=pd.to_numeric(t.team_star_out,errors='coerce')
F=['h5','h10','h15','h20','hs','r5','r10','r15','r20','rs','mg','votes','sd20','cv','z_line','line_vs_max','line_vs_p90','line_vs_med','line_pctile','n_ge_line20','n_gt_line10','last','last2','mn5','mn20','mn_sd','mn_floor','mc','mc_sign','st10','new_st','new_bn','rest','b2b','home','total','abs_spread','opp_rel','opp_relS','opp_sign','opp_pts_al10','opp_pts10','pG','pC','lvl','line','prop_id','rung','rung_pct']
t['last']=t['last'].astype(float)
t.to_pickle('/tmp/ds3.pkl')
import json; json.dump(F,open('/tmp/F.json','w'))
groups={'STANDARD OVER':(t.pt=='Standard')&(t.side==1),'STANDARD UNDER':(t.pt=='Standard')&(t.side==-1),'DEMON (Over)':(t.pt=='Demon')}
def lbz(y,z=1.0):
    n=len(y); p=y.mean()
    return (p+z*z/(2*n)-z*math.sqrt(p*(1-p)/n+z*z/(4*n*n)))/(1+z*z/n) if n else 0
for name,m in groups.items():
    x=t[m]; tr=x[x.split=='train']; va=x[x.split=='val']
    Fu=[f for f in F if x[f].notna().mean()>.5 and not (f.startswith('rung') and name!='DEMON (Over)')]
    gb=HistGradientBoostingClassifier(max_depth=3,learning_rate=.04,max_iter=200,min_samples_leaf=200,l2_regularization=8).fit(tr[Fu],tr.y)
    p=gb.predict_proba(va[Fu])[:,1]; va=va.assign(p=p)
    print(f'\n=== {name}: train {len(tr)} val {len(va)} | val base {va.y.mean():.3f} | AUC {roc_auc_score(va.y,p):.3f}')
    for fr in (.3,.2,.1,.05): k=va[va.p>=va.p.quantile(1-fr)]; print(f'   top {int(fr*100):2d}%: n={len(k)} hit={k.y.mean()*100:.1f}%')
    pi=permutation_importance(gb,va[Fu],va.y,scoring='roc_auc',n_repeats=3,random_state=0)
    print('   leans on:',', '.join(f'{a}({b:.3f})' for a,b in pd.Series(pi.importances_mean,Fu).sort_values(ascending=False).head(8).items()))
