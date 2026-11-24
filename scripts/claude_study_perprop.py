import numpy as np,pandas as pd,warnings; warnings.filterwarnings('ignore')
from sklearn.metrics import roc_auc_score
d=pd.read_pickle('/tmp/study_ds2.pkl')
d['opp_adj']=(d.opp_al-1)*d.side   # >0: opponent allows more in the pick's direction
d['pace_adj']=d.pace_f*d.side      # Over likes pace, Under dislikes
d['out_adj']=d.out_min*d.side
d['mp_adj']=d.mp10*d.side
d['home_adj']=d.home*d.side
d['b2b_adj']=d.b2b*d.side
d['msd_neg']=-d.msd10
d['cons']=(d.h5+d.h10+d.h20)/3
F={'consist':'cons','gap':'g10s','minSwing':'msd_neg','mins':'mp_adj','oppAllow':'opp_adj','posDef':'r','pace':'pace_adj','mateOut':'out_adj','home':'home_adj','b2b':'b2b_adj','margin':'absm'}
d['r']=(d.r-1)*d.side if False else d.r
rows=[]
for (pt,prop),g in d.groupby(['pt','prop_canon']):
    if len(g)<600 or g.hit.nunique()<2: continue
    row={'pt':pt,'prop':prop,'n':len(g)}
    for k,c in F.items():
        x=g[[c,'hit']].dropna()
        row[k]=roc_auc_score(x.hit,x[c])-0.5 if len(x)>400 and x[c].nunique()>1 else np.nan
    rows.append(row)
R=pd.DataFrame(rows)
pd.set_option('display.width',250)
for pt in ['Goblin','Standard','Demon']:
    x=R[R.pt==pt].drop(columns='pt').set_index('prop')
    print('\n',pt,'(AUC-0.5 ×100; + helps, - hurts)'); print((x.drop(columns='n')*100).round(1).assign(n=x.n).to_string())
