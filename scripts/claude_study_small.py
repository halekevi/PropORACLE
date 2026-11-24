import numpy as np, pandas as pd, warnings
from sklearn.ensemble import HistGradientBoostingClassifier as HGB
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import make_pipeline
from sklearn.impute import SimpleImputer
from sklearn.metrics import roc_auc_score
warnings.filterwarnings('ignore')
D=pd.read_pickle('/tmp/study_ds.pkl'); D['gdate']=pd.to_datetime(D.slate_date)
D['tr']=D.gdate<pd.Timestamp('2026-04-14'); D['va']=(D.gdate>=pd.Timestamp('2026-04-14'))&(D.gdate<pd.Timestamp('2026-05-09')); D['ho']=D.gdate>=pd.Timestamp('2026-05-09')
SETS={'A cons+gaps':['h5','h10','h20','hs','zmin','g5s','g10s','g20s','gewm','worst10','cv'],
'B +minutes sd':['h5','h10','h20','hs','zmin','g5s','g10s','g20s','gewm','worst10','cv','msd10','mp10'],
'C +opp/pos':['h5','h10','h20','hs','zmin','g5s','g10s','g20s','gewm','worst10','cv','opp_al','r'],
'D +rest/home':['h5','h10','h20','hs','zmin','g5s','g10s','g20s','gewm','worst10','cv','rest','home','out_min']}
for pt in ['Goblin','Standard','Demon']:
    df=D[D.pt==pt].copy()
    print(f'\n=== {pt} base hold {df[df.ho].hit.mean():.3f}')
    for nm,cols in SETS.items():
        cols=cols+['side']
        res=[]
        for mname in ['gbm','logit']:
            if mname=='gbm':
                m=HGB(max_depth=3,learning_rate=0.04,max_iter=250,l2_regularization=10,min_samples_leaf=150,random_state=0)
            else:
                m=make_pipeline(SimpleImputer(strategy='median'),StandardScaler(),LogisticRegression(C=0.3,max_iter=500))
            m.fit(df[df.tr][cols],df[df.tr].hit)
            p=m.predict_proba(df[cols])[:,1]
            va=df.va.values; ho=df.ho.values
            out=[f'{mname} AUC {roc_auc_score(df.hit[ho],p[ho]):.3f}']
            for q in (0.9,0.95):
                thr=np.quantile(p[va],q); sel=ho&(p>=thr)
                out.append(f'top{int((1-q)*100)}% {df.hit[sel].mean():.3f}(n={int(sel.sum())})')
            res.append(' '.join(out))
        print(f' {nm:14s} | '+' | '.join(res))
