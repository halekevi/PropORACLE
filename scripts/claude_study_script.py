import sqlite3,numpy as np,pandas as pd,warnings; warnings.filterwarnings('ignore')
from sklearn.ensemble import HistGradientBoostingClassifier as HGB
from sklearn.metrics import roc_auc_score
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql('select game_date,event_id,home_team,away_team,player,team,minutes,pts from nba',c)
n=n[pd.to_numeric(n.minutes,errors='coerce')>0].drop_duplicates(['event_id','player']); n['gd']=pd.to_datetime(n.game_date); n['player']=n.player.str.lower().str.strip()
T=n.groupby(['event_id','team']).agg(gd=('gd','first'),pts=('pts','sum'),home=('home_team','first')).reset_index()
T['home']=(T.team==T.home).astype(int)
O=T[['event_id','team','pts']].rename(columns={'team':'ot','pts':'opts'})
T=T.merge(O,on='event_id'); T=T[T.team!=T.ot].drop_duplicates(['event_id','team']).sort_values(['team','gd'])
T['net']=T.pts-T.opts
T['net10']=T.groupby('team').net.transform(lambda x:x.shift(1).rolling(10,min_periods=4).mean())
K=T.set_index(['event_id','team'])
ev=n[['event_id','player','gd','team']].copy(); ev['ot']=np.nan
oth=T[['event_id','team','ot']].rename(columns={'team':'team2'})
d=pd.read_pickle('/tmp/study_ds.pkl'); d['sd']=pd.to_datetime(d.slate_date)
a=d[['k','sd']].reset_index().sort_values('sd'); b=ev.sort_values('gd').rename(columns={'player':'k'})
m=pd.merge_asof(a,b,left_on='sd',right_on='gd',by='k',direction='forward',tolerance=pd.Timedelta(days=1)).set_index('index')
ot=T.set_index(['event_id','team']).ot.to_dict()
def em(r):
    if pd.isna(r.event_id): return np.nan
    o=ot.get((r.event_id,r.team)); 
    try:
        return K.loc[(r.event_id,r.team)].net10-K.loc[(r.event_id,o)].net10+(2.5 if K.loc[(r.event_id,r.team)].home==1 else -2.5)
    except Exception: return np.nan
m['exp_margin']=m.apply(em,axis=1)
d['exp_margin']=m.exp_margin; d['absm']=d.exp_margin.abs()
d['gdate']=d.sd
d['tr']=d.gdate<pd.Timestamp('2026-04-14'); d['va']=(d.gdate>=pd.Timestamp('2026-04-14'))&(d.gdate<pd.Timestamp('2026-05-09')); d['ho']=d.gdate>=pd.Timestamp('2026-05-09')
d.to_pickle('/tmp/study_ds2.pkl')
print('exp_margin coverage',d.exp_margin.notna().mean().round(3), d.absm.describe()[['25%','50%','75%']].round(1).to_dict())
d=d[d.exp_margin.notna()]
d['bk']=pd.cut(d.absm,[-1,4,8,12,99],labels=['<4','4-8','8-12','12+'])
for pt in ['Goblin','Standard','Demon']:
    y=d[d.pt==pt]; print(f'\n== {pt} base {y.hit.mean():.3f}')
    for side,nm in ((1,'Over'),(-1,'Under')):
        z=y[y.side==side]; print(f' {nm}: '+' | '.join(f'|margin|{k}: {v.hit.mean():.3f} (n={len(v)})' for k,v in z.groupby('bk')))
    # gated: consistency gate then margin
    gt=y[(y.h5>=.8)&(y.h10>=.8)&(y.h20>=.65)]
    print(' gated (L5,L10>=80,L20>=65): '+' | '.join(f'{k}: {v.hit.mean():.3f} (n={len(v)})' for k,v in gt.groupby('bk')))
    cols=['h5','h10','h20','hs','zmin','g5s','g10s','g20s','gewm','worst10','cv','side']
    r=[]
    for extra in ([],['exp_margin','absm']):
        cc=cols+extra
        mm=HGB(max_depth=3,learning_rate=0.04,max_iter=250,l2_regularization=10,min_samples_leaf=150,random_state=0).fit(y[y.tr][cc],y[y.tr].hit)
        p=mm.predict_proba(y[cc])[:,1]; ho=y.ho.values; va=y.va.values
        thr=np.quantile(p[va],0.9); s=ho&(p>=thr)
        r.append(f'{"+margin" if extra else "base"} AUC {roc_auc_score(y.hit[ho],p[ho]):.3f} top10 {y.hit[s].mean():.3f}(n={int(s.sum())})')
    print(' model:',' | '.join(r))
