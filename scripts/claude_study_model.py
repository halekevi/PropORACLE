import numpy as np, pandas as pd, warnings, json
from sklearn.ensemble import HistGradientBoostingClassifier as HGB
from sklearn.metrics import roc_auc_score, log_loss
warnings.filterwarnings('ignore')
D=pd.read_pickle('/tmp/study_ds.pkl')
D['gdate']=pd.to_datetime(D.slate_date)
D['pid']=D.prop_canon.astype('category').cat.codes
D['tr']=D.gdate<pd.Timestamp('2026-04-14'); D['va']=(D.gdate>=pd.Timestamp('2026-04-14'))&(D.gdate<pd.Timestamp('2026-05-09')); D['ho']=D.gdate>=pd.Timestamp('2026-05-09')
FAM={
 'consistency':['h5','h10','h20','hs','zmin','cv','mg'] if 'mg' in D else ['h5','h10','h20','hs','zmin','cv'],
 'gaps_sd':['g5s','g10s','g20s','gewm','gproj','gavs','eb_gap','pnorm','last','worst10','best10'],
 'level':['av3','av5','av10','av20','avs','ewm','sd10','sd20','proj','eb_line','line','line_over_avg','frac_eq','ng'],
 'minutes':['mp10','msd10','mp3','mtrend','mmin5','start10','mn10','mnsd','mnfl5','mntrend','rate_pm'],
 'rest':['rest','b2b','g7','team_rest','opp_rest','dsl'],
 'pace':['own_pace','opp_pace','pace_f','opp_al_poss'],
 'opp_def':['opp_al'],
 'pos_def':['r'],
 'teammates_out':['out_cnt','out_min'],
 'home':['home'],
}
base=['side','pid']
for f in FAM.values():
    for c in f:
        if c not in D: D[c]=np.nan
def feats(drop=None,only=None):
    cols=list(base)
    for k,v in FAM.items():
        if only is not None and k not in only: continue
        if drop is not None and k in drop: continue
        cols+=v
    return cols
def fit(df,cols):
    tr=df[df.tr]; va=df[df.va]
    m=HGB(max_depth=4,learning_rate=0.05,max_iter=400,l2_regularization=5.0,min_samples_leaf=80,early_stopping=True,validation_fraction=0.15,n_iter_no_change=30,categorical_features=[cols.index('pid')],random_state=0)
    m.fit(tr[cols],tr.hit); return m
def topk(df,p,thr_p):
    return df[p>=thr_p]
res={}
out=[]
for pt in ['Goblin','Standard','Demon']:
    df=D[D.pt==pt].copy()
    cols=feats(); m=fit(df,cols)
    df['p']=m.predict_proba(df[cols])[:,1]
    va=df[df.va]; ho=df[df.ho]
    base_rate=ho.hit.mean()
    print(f'\n=== {pt} === n_train={df.tr.sum()} val={len(va)} hold={len(ho)} base hold hit {base_rate:.3f}')
    print(f' AUC val {roc_auc_score(va.hit,va.p):.3f} hold {roc_auc_score(ho.hit,ho.p):.3f} | logloss hold {log_loss(ho.hit,ho.p):.4f} vs const {log_loss(ho.hit,np.full(len(ho),df[df.tr].hit.mean())):.4f}')
    # calibration by decile of predicted p (holdout)
    ho['dec']=pd.qcut(ho.p.rank(method='first'),10,labels=False)
    cal=ho.groupby('dec').agg(pred=('p','mean'),hit=('hit','mean'),n=('hit','size'))
    print(' calibration (hold) decile pred/hit:',' '.join(f'{a:.2f}/{b:.2f}' for a,b in zip(cal.pred,cal.hit)))
    # thresholds set on VALIDATION quantiles, applied to holdout
    for q in (0.80,0.90,0.95,0.98):
        thr=va.p.quantile(q); s=ho[ho.p>=thr]
        print(f' top{int((1-q)*100)}% (thr {thr:.3f}): hold hit {s.hit.mean():.3f} n={len(s)}')
    # family ablation on holdout: top10% hit & AUC
    thr_all=va.p.quantile(0.90)
    print(' family ablation (drop one family) -> hold AUC / top10% hit (thr from val of that model):')
    fa={}
    for k in FAM:
        c2=feats(drop=[k]); m2=fit(df,c2); p2=m2.predict_proba(df[c2])[:,1]
        v2=df.va.values; h2=df.ho.values
        thr2=np.quantile(p2[v2],0.90); sel=h2&(p2>=thr2)
        fa[k]=(roc_auc_score(df.hit[h2],p2[h2]),df.hit[sel].mean(),int(sel.sum()))
        print(f'   -{k:14s} AUC {fa[k][0]:.3f}  top10 {fa[k][1]:.3f} (n={fa[k][2]})')
    # only-family
    print(' single family only -> hold AUC / top10%:')
    for k in FAM:
        c2=feats(only=[k]); m2=fit(df,c2); p2=m2.predict_proba(df[c2])[:,1]
        v2=df.va.values; h2=df.ho.values
        thr2=np.quantile(p2[v2],0.90); sel=h2&(p2>=thr2)
        print(f'   {k:14s} AUC {roc_auc_score(df.hit[h2],p2[h2]):.3f}  top10 {df.hit[sel].mean():.3f} (n={int(sel.sum())})')
    # per-prop top slice (val thr 0.85 quantile within prop)
    print(' per prop, top 15% by model (thr from val within prop): hold hit / n / base')
    for prop,g in df.groupby('prop_canon'):
        gv=g[g.va]; gh=g[g.ho]
        if len(gv)<80 or len(gh)<60: continue
        thr=gv.p.quantile(0.85); s=gh[gh.p>=thr]
        if len(s)<15: continue
        print(f'   {prop:14s} {s.hit.mean():.3f} n={len(s)} base {gh.hit.mean():.3f}')
    df[['p']].to_pickle(f'/tmp/study_p_{pt}.pkl')
