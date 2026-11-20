"""NBA gate study. Discovery = 2025-26_late (Feb19-May3); holdout TEST = 2026-27 (Oct-Nov). Gates are chosen on discovery only.
Goblin/Demon/Standard separate; Standard Over+Under together; Points&Pts-combos vs Other. Outputs data/reports/gate_study_nba_<date>.xlsx"""
import pandas as pd,numpy as np,itertools,sys
F='data/training/master_graded_2026-11-18.parquet'
d=pd.read_parquet(F); d=d[(d.void==0)&(d.push==0)&d.hit.notna()&d.l10_n.notna()].copy()
d['sgn']=np.where(d.side=='OVER',1,-1)
d['l5r']=d.l5_hits/d.l5_n; d['l10r']=d.l10_hits/d.l10_n; d['l20r']=d.l20_hits/d.l20_n
d['szr']=np.where(d.season_n>=10,d.season_hit_rate,np.nan)
d['sgap']=d.gap*d.sgn; d['z']=d.sgap/d.gap_sd.clip(lower=0.25)
d['grp']=np.where(d.prop.str.contains('Points|Pts'),'Points & Pts combos','Other props')
d['rung_top']=d.line==d.groupby(['game_date','player','prop','pick_type','side']).line.transform('max')
tr=d[d.season_tag=='2025-26_late']; te=d[d.season_tag=='2026-27']
def wil(h,n,z=1.96):
    if n==0: return (np.nan,np.nan)
    p=h/n; c=(p+z*z/(2*n))/(1+z*z/n); m=z*np.sqrt(p*(1-p)/n+z*z/(4*n*n))/(1+z*z/n); return round((c-m)*100,2),round((c+m)*100,2)
GR={'l5>=4':lambda x:x.l5_hits>=4,'l5=5':lambda x:x.l5_hits>=5}
grid=[]
for l5 in (0,3,4,5):
  for l10 in (0,.6,.7,.8,.9):
    for l20 in (0,.6,.7,.8,.9):
      for z in (-9,0,.5,1,1.5):
        for sz in (0,.6,.7,.8):
          grid.append((l5,l10,l20,z,sz))
def apply(x,g):
    l5,l10,l20,z,sz=g; m=(x.l5_hits>=l5)&(x.l10r>=l10)&(x.l20r>=l20)&(x.z>=z)
    if sz: m&=(x.szr>=sz)
    return x[m]
res=[];base={}
for (pk,gp),t in tr.groupby(['pick_type','grp']):
    tt=te[(te.pick_type==pk)&(te.grp==gp)]
    if pk=='Demon' : continue
    base[(pk,gp)]=(len(t),t.hit.mean(),len(tt),tt.hit.mean())
    for g in grid:
        a=apply(t,g); n=len(a)
        if n<300: continue
        b=apply(tt,g); res.append(dict(pick=pk,group=gp,l5_min=g[0],l10_min=g[1],l20_min=g[2],z_min=g[3],season_min=g[4],train_n=n,train_hit=round(a.hit.mean()*100,2),test_n=len(b),test_hit=round(b.hit.mean()*100,2) if len(b) else np.nan,test_hits=int(b.hit.sum()) if len(b) else 0))
R=pd.DataFrame(res)
for r in R.itertuples(): pass
R[['ci_lo','ci_hi']]=[wil(h,n) for h,n in zip(R.test_hits,R.test_n)]
print('BASELINES (train n, train hit, test n, test hit)')
for k,v in base.items(): print(k,v[0],round(v[1]*100,2),v[2],round(v[3]*100,2))
out={}
for (pk,gp),g in R.groupby(['pick','group']):
    top=g[g.test_n>=100].sort_values('train_hit',ascending=False).head(8); out[f'{pk[:3]} {gp[:6]}']=top
    print('\n##',pk,gp,'(top 8 by TRAIN hit, shown with TEST)'); print(top[['l5_min','l10_min','l20_min','z_min','season_min','train_n','train_hit','test_n','test_hit','ci_lo','ci_hi']].to_string(index=False))
R.to_csv('data/reports/gate_study_nba_grid.csv',index=False)
