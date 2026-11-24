"""Rebuild graded NBA prop set from ledger legs, add gate features + opponent POSITION defense (rolling prior-10), test effect inside gates."""
import glob,sqlite3,pandas as pd,numpy as np,sys,warnings,time;warnings.filterwarnings('ignore')
sys.path.insert(0,'scripts');from claude_gamelog import nm
t0=time.time()
L=pd.concat([pd.read_parquet(f) for f in sorted(glob.glob('data/ledger/legs/slate_date=*/*.parquet'))])
import os
if os.path.exists('data/ledger_claude/legs_supplement.parquet'): L=pd.concat([L,pd.read_parquet('data/ledger_claude/legs_supplement.parquet')])
L=L[(L.sport.astype(str).str.upper()=='NBA')&L.hit.notna()&L.pick_type.notna()]
L['pt']=L.pick_type.str.capitalize();L['side']=np.where(L.direction.str.upper().str.startswith('O'),1,-1);L['gd']=pd.to_datetime(L.slate_date)
L['k']=L.player.map(nm);L=L.drop_duplicates(['k','gd','prop','line','side','pt'])
print('legs',len(L),L.gd.min(),L.gd.max(),L.pt.value_counts().to_dict(),flush=True)
c=sqlite3.connect('data/cache/proporacle_ref.db')
S=['pts','reb','ast','stl','blk','tov','fgm','fga','fg3m','fg3a','fg2m','fg2a','ftm','fta','oreb','dreb','pf','pra','pr','pa','ra','bs','fantasy_score']
d=pd.read_sql(f"select game_date,event_id,team,home_team,away_team,player,position,minutes,{','.join(S)} from nba where team is not null and coalesce(season_window,'')!='preseason'",c)
d['gd']=pd.to_datetime(d.game_date);d=d[d.minutes.fillna(0)>0]
pos=d.position.fillna('').str.upper()
d['pg']=np.where(pos.isin(['C','F-C','C-F']),'C',np.where(pos.isin(['G','PG','SG','G-F']),'G',np.where(pos.isin(['F','SF','PF','F-G']),'F','?')))
d=d[d.pg!='?'];d['opp']=np.where(d.team==d.home_team,d.away_team,d.home_team);d['k']=d.player.map(nm)
tm=set(d.team)|set(d.opp);print('db teams',sorted(tm),'ledger opps',sorted(set(L.opp_team.dropna())-tm),flush=True)
MAP={'SAS':'SA','NOP':'NO','GSW':'GS','NYK':'NY','UTA':'UTAH','WAS':'WSH','PHO':'PHX','BRK':'BKN','CHO':'CHA'}
L['opp']=L.opp_team.replace(MAP)
# position-group defense table
g=d.groupby(['event_id','gd','opp','pg'])[S].sum().reset_index()
ev=g[['event_id','gd','opp']].drop_duplicates();full=ev.merge(pd.DataFrame({'pg':['G','F','C']}),how='cross')
g=full.merge(g,how='left',on=['event_id','gd','opp','pg']).fillna(0).sort_values(['opp','pg','gd'])
roll=g.groupby(['opp','pg'])[S].transform(lambda x:x.shift(1).rolling(10,min_periods=4).mean())
lgm=g.sort_values('gd').groupby('pg')[S].transform(lambda x:x.shift(1).expanding(min_periods=50).mean()).reindex(g.index)
R=roll/lgm;R[['opp','pg','gd']]=g[['opp','pg','gd']]
Rl=R.melt(id_vars=['opp','pg','gd'],value_vars=S,var_name='col',value_name='r')
# player position (mode)
pp=d.groupby('k').pg.agg(lambda x:x.value_counts().index[0]).rename('pg')
mp={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','3-PT Attempted':'fg3a','Free Throws Made':'ftm','Free Throws Attempted':'fta','Personal Fouls':'pf','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score','Defensive Rebounds':'dreb','Offensive Rebounds':'oreb'}
L['col']=L.prop.map(mp);L=L[L.col.notna()].join(pp,on='k');L=L[L.pg.notna()]
L=L.merge(Rl,on=['opp','pg','gd','col'],how='left');print('with r',L.r.notna().sum(),flush=True)
# player series features
ser={k:v.sort_values('gd') for k,v in d.groupby('k')}
rows=[]
for r in L.itertuples():
    s=ser.get(r.k)
    if s is None: rows.append((np.nan,)*9);continue
    v=s[s.gd<r.gd][r.col].values.astype(float)
    if len(v)<8: rows.append((np.nan,)*9);continue
    hit=(v>r.line) if r.side==1 else (v<r.line);sg=r.side
    def hw(w):return hit[-w:].mean()
    g5=sg*(v[-5:].mean()-r.line);g10=sg*(v[-10:].mean()-r.line);g20=sg*(v[-20:].mean()-r.line)
    sd=max(float(np.std(v[-20:])),0.3)
    rows.append((hw(5),hw(10),hw(20),hit.mean(),min(g5,g10,g20)/sd,float(np.std(v[-20:])/max(abs(v[-20:].mean()),.5)),len(v),g5,g10))
F=pd.DataFrame(rows,columns=['h5','h10','h20','hs','zmin','cv','n','g5','g10'],index=L.index);L=pd.concat([L,F],axis=1)
L=L[L.h5.notna()&L.r.notna()]
L['fav']=np.where(((L.side==1)&(L.r>=1.10))|((L.side==-1)&(L.r<=0.90)),'favorable',np.where(((L.side==1)&(L.r<=0.90))|((L.side==-1)&(L.r>=1.10)),'unfavorable','neutral'))
L['ho']=L.gd>='2026-05-09'
L.to_pickle('/tmp/posdef_ds.pkl');print('rows',len(L),round(time.time()-t0),'s',flush=True)
