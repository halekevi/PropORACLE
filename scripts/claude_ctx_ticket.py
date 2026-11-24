import sqlite3,pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",c)
n['k']=n.player.str.lower().str.strip(); n['gd']=pd.to_datetime(n.game_date)
S=['pts','reb','ast','stl','blk','tov','fgm','fga','fg3m','fg3a','fg2m','fg2a','ftm','fta','oreb','dreb','pf','pra','pr','pa','ra','bs','fantasy_score']
for s in S+['minutes']: n[s]=pd.to_numeric(n[s],errors='coerce')
n=n[n.minutes>0]; n['opponent']=np.where(n.team==n.home_team,n.away_team,n.home_team)
tg=n.groupby(['opponent','gd'])[S].sum().reset_index().sort_values('gd')
for s in S:
    tg[s+'_r']=tg.groupby('opponent')[s].transform(lambda v:v.shift(1).rolling(15,min_periods=8).mean()); tg[s+'_rel']=tg[s+'_r']/tg[s].mean()
tgk=tg.set_index(['opponent','gd'])
T={'A skill/role':['fg2a','fga','fg3a','pa','pra','ast','pr','pts','fg2m','fantasy_score','reb','ra'],'B good':['fgm','fta','ftm','dreb'],'C mixed':['oreb','fg3m','tov'],'D luck':['blk','bs','pf','stl']}
inv={c:t for t,cs in T.items() for c in cs}
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[d.hit.notna()&d.col.isin(S)].copy(); d['hit']=d.hit.astype(float); d['tier']=d.col.map(inv)
m=n[['k','gd','opponent']].drop_duplicates(['k','gd']); d=d.merge(m,on=['k','gd'],how='inner')
rel=[]
for r in d.itertuples():
    key=(r.opponent,r.gd); rel.append(tgk[r.col+'_rel'].get(key,np.nan) if key in tgk.index else np.nan)
d['rel']=rel; d=d.dropna(subset=['rel'])

import itertools
d['day']=d.gd.dt.date; d['game']=[ '|'.join(sorted([str(a),str(b)])) for a,b in zip(d.team,d.opp_team)]
base=d[(d.pt=='Standard')&(d.side==-1)&(d.h5>=.8)&(d.h10>=.8)&(d.h20>=.65)&(d.zmin>=.5)]
def build(day,n=3):
    day=day.sort_values('zmin',ascending=False); T=[];used=set()
    while True:
        t=[];pl=set();gc={}
        for r in day.itertuples():
            if r.k in pl or r.Index in used or gc.get(r.game,0)>=2: continue
            t.append(r);pl.add(r.k);gc[r.game]=gc.get(r.game,0)+1
            if len(t)==n: break
        if len(t)<n or len({x.team for x in t})==1: break
        T.append(t); used|={x.Index for x in t}
    return T
rng=np.random.default_rng(1)
def run(nm,x,mincand=6):
    res=[]
    for day,g in x.groupby('day'):
        if len(g)<mincand: continue
        for t in build(g): res.append((day,all(l.hit==1 for l in t)))
    r=pd.DataFrame(res,columns=['day','win']); 
    if len(r)==0: print(nm,'no tickets'); return
    r['pnl']=np.where(r.win,5,-1); hold=pd.to_datetime(r.day)>=pd.Timestamp('2026-05-09')
    days=r.groupby('day').pnl.agg(['sum','count']); bs=[]
    for _ in range(2000):
        ix=rng.integers(0,len(days),len(days)); bs.append(days['sum'].values[ix].sum()/days['count'].values[ix].sum())
    h=r[hold.values]
    print(f'  {nm:26s} tickets {len(r):3d} win {r.win.mean()*100:4.1f}% ROI {r.pnl.mean()*100:+5.0f}% (95% CI {np.percentile(bs,2.5)*100:+4.0f}..{np.percentile(bs,97.5)*100:+4.0f}) | hold n={len(h)} win {h.win.mean()*100 if len(h) else float("nan"):4.1f}% ROI {h.pnl.mean()*100 if len(h) else float("nan"):+5.0f}%')
for mc in (3,6):
    print(f'-- days need >= {mc} candidates')
    run('gate only',base,mc); run('gate + tough (<1.00)',base[base.rel<1.0],mc); run('gate + not soft (<=1.05)',base[base.rel<=1.05],mc)
