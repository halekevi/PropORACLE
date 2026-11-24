import pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
t=pd.read_pickle('/tmp/ctx4.pkl')
t['votes']=sum((t[f'h{w}']>=.6).astype(int) for w in (5,10,15,20))+(t.hs>=.6).astype(int)+sum((t[f'a{w}']>0).astype(int) for w in (5,10,15,20))+(t['as']>0).astype(int)
L=t.line.clip(lower=.5)
for w in ('5','10','15','20','s'): t['r'+w]=(t['a'+w] if w!='s' else t['as'])/L
t['mg']=t[['r5','r10','r15','r20']].min(axis=1)
t['opp_rel']=t.opp_allow/t.groupby('prop').opp_allow.transform('mean')-1
t['mc']=t.min5/t.min20-1; t['abs_spread']=t.spread.abs()
t['new_st']=((t.st3>=.66)&(t.stprev<=.35)).astype(float); t['new_bn']=((t.st3<=.34)&(t.stprev>=.65)).astype(float)
t['pG']=(t.pos=='G').astype(int);t['pC']=(t.pos=='C').astype(int)
t['lvl']=t.groupby('prop').avg20.transform(lambda x:pd.qcut(x,3,labels=False,duplicates='drop')).astype(float)
fam={'Points':0,'Pts+Asts':1,'Pts+Rebs':1,'Pts+Rebs+Asts':1,'Rebs+Asts':1,'Rebounds':2,'Assists':3}
t['fam']=t.prop.map(fam).fillna(4)
t['opp_sign']=t.opp_rel*t.side   # +ve = defense favors the pick (Over: soft D, Under: tough D)
t['mc_sign']=t.mc*t.side
F=['h5','h10','h15','h20','hs','r5','r10','r15','r20','rs','mg','votes','opp_sign','mc_sign','min20','min5','total','abs_spread','rest','b2b','home','pG','pC','lvl','side','line','new_st','new_bn','fam']
t=t.sort_values('fd'); dates=np.sort(t.fd.unique())
# 3 expanding walk-forward folds
res=[]
cuts=[.5,.65,.8,1.0]
allp=[]
for i in range(3):
    a=t.fd.quantile(cuts[i]); b=t.fd.quantile(cuts[i+1])
    tr=t[t.fd<a]; te=t[(t.fd>=a)&(t.fd<=b)] if i<2 else t[t.fd>=a]
    m=HistGradientBoostingClassifier(max_depth=3,learning_rate=.05,max_iter=150,min_samples_leaf=150,l2_regularization=5.0).fit(tr[F],tr.y)
    lr=LogisticRegression(max_iter=1000,C=.3).fit(tr[F].fillna(0).clip(-5,5),tr.y)
    te=te.assign(p_gb=m.predict_proba(te[F])[:,1],p_lr=lr.predict_proba(te[F].fillna(0).clip(-5,5))[:,1]); allp.append(te)
    print(f'fold{i+1} train {len(tr)} test {len(te)} AUC gb {roc_auc_score(te.y,te.p_gb):.3f} lr {roc_auc_score(te.y,te.p_lr):.3f} base {te.y.mean():.3f}')
T=pd.concat(allp); print('\nOOS legs',len(T),'base',round(T.y.mean(),3))
def top(col,fr):
    k=T[T[col]>=T[col].quantile(1-fr)]; return len(k),round(k.y.mean()*100,1)
print('\nOOS hit rate of top X% by model score (walk-forward):')
for fr in (.5,.2,.1,.05,.02): print(f'  top {int(fr*100):2d}%  GB n,hit={top("p_gb",fr)}   LR n,hit={top("p_lr",fr)}')
# hand gate stack on same OOS legs
U=T.side==-1
hand=(T.votes>=8)&(T.mg>=.10)&(T.h10>=.8)&(T.h20>=.75)&np.where(U,T.opp_rel<=-.03,T.opp_rel>=0)
print('\nhand stacked gate on same OOS legs:',hand.sum(),round(T[hand].y.mean()*100,1))
hand2=(T.votes>=8)&(T.mg>=.10)&(T.h10>=.8)&(T.h20>=.75); print('hand gate w/o opponent:',hand2.sum(),round(T[hand2].y.mean()*100,1))
# model + hand gate combined
for fr in (.2,.1):
    k=T[(T.p_gb>=T.p_gb.quantile(1-fr))&hand2]; print(f'GB top {int(fr*100)}% AND hand gate (no opp):',len(k),round(k.y.mean()*100,1))
# by side & family for top 10%
k=T[T.p_gb>=T.p_gb.quantile(.9)]
print('\ntop 10% by side:',k.groupby('side').y.agg(['size','mean']).round(3).to_dict('index'))
print('top 10% by family:',k.groupby('fam').y.agg(['size','mean']).round(3).to_dict('index'))
# feature importance via permutation on last fold
from sklearn.inspection import permutation_importance
tr=t[t.fd<t.fd.quantile(.8)];te=t[t.fd>=t.fd.quantile(.8)]
m=HistGradientBoostingClassifier(max_depth=3,learning_rate=.05,max_iter=150,min_samples_leaf=150,l2_regularization=5.0).fit(tr[F],tr.y)
pi=permutation_importance(m,te[F],te.y,scoring='roc_auc',n_repeats=5,random_state=0)
print('\npermutation importance (AUC drop):'); print(pd.Series(pi.importances_mean,F).sort_values(ascending=False).round(4).head(10).to_string())
T.to_pickle('/tmp/oos.pkl')
