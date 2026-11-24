"""Backtest NBA Standard legs (Feb-May 2026 graded export): L5/L10/L15/L20/season hit counts, avg gaps, SD20, go-line vs real outcomes. Leak-free (games strictly before file_date)."""
import pandas as pd,numpy as np,re,unicodedata,warnings; warnings.filterwarnings('ignore')
from sklearn.metrics import roc_auc_score
from sklearn.linear_model import LogisticRegression
_u=lambda s:''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c))
nm=lambda s:re.sub(r'\b(jr|sr|ii|iii|iv)\b','',re.sub(r'[^a-z ]','',_u(s).lower().replace('.','').replace("'",''))).strip()
g=pd.read_parquet('data/game_logs/nba_player_games.parquet')
g=g[g.phase.astype(str).str.upper()!='PRESEASON']; g=g[~g.is_dnp.astype(bool)&(g.minutes>0)]
g['game_date']=pd.to_datetime(g.game_date); g['k']=g.player.map(nm); g=g.sort_values(['k','game_date'])
mp={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','3-PT Attempted':'fg3a','Free Throws Made':'ftm','Free Throws Attempted':'fta','Personal Fouls':'pf','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score','Defensive Rebounds':'dreb','Offensive Rebounds':'oreb'}
d=pd.read_csv('../PropORACLE_main_cp/data/training/graded_export_20260516.csv',low_memory=False,usecols=['file_date','sport','player','prop','line','pick_type','direction','result_binary'])
d=d[(d.sport=='NBA')&(d.pick_type=='Standard')&d.prop.isin(mp)&d.result_binary.isin([0,1])].copy()
d['k']=d.player.map(nm); d['fd']=pd.to_datetime(d.file_date)
by={k:(v.game_date.values,{c:v[c].astype(float).values for c in set(mp.values())}) for k,v in g.groupby('k')}
rows=[]
for r in d.itertuples():
    t=by.get(r.k)
    if t is None: continue
    dts,cols=t; n=np.searchsorted(dts,np.datetime64(r.fd),side='left')
    if n<20: continue
    s=cols[mp[r.prop]][:n]; sg=1 if r.direction=='OVER' else -1; L=r.line
    hit=(s>L) if sg==1 else (s<L)
    f=dict(y=r.result_binary,side=sg,date=r.fd,prop=r.prop,line=L)
    for w in (5,10,15,20): f[f'h{w}']=hit[-w:].mean(); f[f'a{w}']=sg*(s[-w:].mean()-L)
    f['hs']=hit.mean(); f['as']=sg*(s.mean()-L); f['sd20']=max(s[-20:].std(),.5); f['sdS']=max(s.std(),.5); f['nS']=n
    f['hpass']=int(hit[-5:].sum()>=4 and hit[-10:].sum()>=8 and hit[-15:].sum()>=11 and hit[-20:].sum()>=15)
    cand=np.arange(np.floor(s[-20:].min())-.5,s[-20:].max()+1.5,.5)
    good=[c for c in cand if ((s[-5:]>c).sum()>=4 and (s[-10:]>c).sum()>=8 and (s[-15:]>c).sum()>=11 and (s[-20:]>c).sum()>=15)] if sg==1 else [c for c in cand if ((s[-5:]<c).sum()>=4 and (s[-10:]<c).sum()>=8 and (s[-15:]<c).sum()>=11 and (s[-20:]<c).sum()>=15)]
    f['room']=((max(good)-L) if sg==1 else (L-min(good))) if good else np.nan
    rows.append(f)
t=pd.DataFrame(rows); print('legs',len(t),'base',round(t.y.mean(),3),'over',round(t[t.side==1].y.mean(),3),'under',round(t[t.side==-1].y.mean(),3))
for w in (5,10,15,20): t[f'z{w}']=t[f'a{w}']/t.sd20
t['zs']=t['as']/t.sdS; t['zmin']=t[['z10','z20']].min(axis=1)
t.to_pickle('/tmp/bt.pkl')
def tab(col,bins,labels=None,pct=False):
    c=pd.cut(t[col],bins,include_lowest=True,labels=labels); x=t.groupby(c,observed=True).y.agg(['size','mean']); print(f'\n[{col}]'); print(x.round(3).to_string())
for c in ['h5','h10','h15','h20','hs']: tab(c,[-.01,.4,.5,.6,.7,.8,.9,1.0])
tab('z5',[-9,-.5,0,.5,1,9]);tab('z10',[-9,-.5,0,.5,1,9]);tab('z15',[-9,-.5,0,.5,1,9]);tab('z20',[-9,-.5,0,.5,1,9]);tab('zs',[-9,-.5,0,.5,1,9]);tab('zmin',[-9,0,.5,1,9])
tab('sd20',[0,1,2,4,8,100])
print('\n[hpass 4/5,8/10,11/15,15/20]'); print(t.groupby('hpass').y.agg(['size','mean']).round(3).to_string())
print('\n[room>=0 vs go-line pass]'); print(t.assign(rg=t.room.ge(0)).groupby('rg').y.agg(['size','mean']).round(3).to_string())
print('\n[hpass & zmin>=.5]');print(t[(t.hpass==1)].assign(zz=lambda x:x.zmin>=.5).groupby('zz').y.agg(['size','mean']).round(3).to_string())
print('\n[hpass by side]');print(t[t.hpass==1].groupby('side').y.agg(['size','mean']).round(3).to_string())
feats=['h5','h10','h15','h20','hs','z5','z10','z15','z20','zs','zmin','a10','sd20']
print('\nAUC single (side-aware; 0.5=no signal)')
for f in feats: print(f, round(roc_auc_score(t.y,t[f].fillna(0)),3))
tr=t[t.date<t.date.quantile(.7)];te=t[t.date>=t.date.quantile(.7)]
def auc(cols):
    m=LogisticRegression(max_iter=500).fit(tr[cols].fillna(0).clip(-5,5),tr.y); return round(roc_auc_score(te.y,m.predict_proba(te[cols].fillna(0).clip(-5,5))[:,1]),3)
print('\nout-of-sample AUC (train first 70% dates, test last 30%)')
for name,cols in [('L10 only',['h10']),('L5+L10',['h5','h10']),('L5,L10,L15,L20',['h5','h10','h15','h20']),('+season hit',['h5','h10','h15','h20','hs']),('hits+gaps(z)',['h5','h10','h15','h20','hs','z5','z10','z15','z20','zs']),('all+SD',['h5','h10','h15','h20','hs','z5','z10','z15','z20','zs','sd20'])]: print(name,auc(cols))
