import _r2
import sqlite3,pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",c)
n['k']=n.player.str.lower().str.strip(); n['gd']=pd.to_datetime(n.game_date)
S=['pts','reb','ast','stl','blk','tov','fgm','fga','fg3m','fg3a','fg2m','fg2a','ftm','fta','oreb','dreb','pf','pra','pr','pa','ra','bs','fantasy_score']
for s in S+['minutes']: n[s]=pd.to_numeric(n[s],errors='coerce')
n=n[n.minutes>0].sort_values(['k','gd']); n['opponent']=np.where(n.team==n.home_team,n.away_team,n.home_team)
n['road']=(n.team==n.away_team).astype(float); n['b2b']=(n.groupby('k').gd.diff().dt.days<=1).astype(float)
n['m10']=n.groupby('k').minutes.transform(lambda v:v.shift(1).rolling(10,min_periods=5).mean())
tg=n.groupby(['opponent','gd'])[S].sum().reset_index().sort_values('gd')
for s in S:
    tg[s+'_r']=tg.groupby('opponent')[s].transform(lambda v:v.shift(1).rolling(15,min_periods=8).mean()); tg[s+'_rel']=tg[s+'_r']/tg[s].mean()
tgk=tg.set_index(['opponent','gd'])
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[d.hit.notna()&d.col.isin(S)].copy(); d['hit']=d.hit.astype(float)
m=n[['k','gd','opponent','road','b2b','m10']].drop_duplicates(['k','gd']); d=d.merge(m,on=['k','gd'],how='inner')
d['rel']=[tgk[r.col+'_rel'].get((r.opponent,r.gd),np.nan) if (r.opponent,r.gd) in tgk.index else np.nan for r in d.itertuples()]
d['hold']=d.sdate>=pd.Timestamp('2026-05-09')
gate={'Goblin':lambda x:(x.h5>=1)&(x.h10>=.9)&(x.h20>=.8),'Standard':lambda x:(x.h5>=.8)&(x.h10>=.8)&(x.h20>=.65),'Demon':lambda x:(x.h10>=.6)&(x.h20>=.5)}
def st(a):
    if len(a)<15: return None
    h=a[a.hold]; return (round(a.hit.mean()*100,1),len(a),round(h.hit.mean()*100,1) if len(h)>=15 else None,len(h))
rows=[]
for (col,pt,side),x in d.groupby(['col','pt','side']):
    if len(x)<80: continue
    g=x[gate[pt](x)]; r={'prop':col,'pick_type':pt,'side':'Over' if side==1 else 'Under','all_n':len(x),'all_hit':round(x.hit.mean()*100,1)}
    s=st(g); r.update({'gate_hit':s[0],'gate_n':s[1],'gate_hold':s[2],'gate_hold_n':s[3]} if s else {})
    base=g if s else x
    for nm,a in [('role<22min',base[base.m10<22]),('mid22-28',base[(base.m10>=22)&(base.m10<28)]),('starter28+',base[base.m10>=28]),
                 ('soft_def',base[base.rel>1.05]),('tough_def',base[base.rel<.95]),('home',base[base.road==0]),('road',base[base.road==1]),
                 ('b2b',base[base.b2b==1]),('line<=Q1',base[base.line<=x.line.quantile(.33)]),('line>=Q3',base[base.line>=x.line.quantile(.67)])]:
        t=st(a); r[nm]=f'{t[0]}% ({t[1]})' if t else ''
    rows.append(r)
o=pd.DataFrame(rows).sort_values(['pick_type','side','prop'])
import os; os.makedirs('outputs',exist_ok=True)
with pd.ExcelWriter('outputs/prop_deep_dive_2026-11-10.xlsx') as w:
    for pt in ['Goblin','Standard','Demon']:
        o[o.pick_type==pt].drop(columns='pick_type').to_excel(w,sheet_name=pt,index=False)
print(o[(o.pick_type=='Standard')][['prop','side','all_hit','gate_hit','gate_n','gate_hold','role<22min','mid22-28','starter28+','soft_def','tough_def']].to_string(index=False))
