"""Opponent position-defense effect on gated props (walk-forward: rolling prior-10-game allowed by position group)."""
import sqlite3,pandas as pd,numpy as np,sys,warnings;warnings.filterwarnings('ignore')
sys.path.insert(0,'scripts');from claude_gamelog import nm
c=sqlite3.connect('data/cache/proporacle_ref.db')
S=['pts','reb','ast','stl','blk','tov','fgm','fga','fg3m','fg3a','fg2m','fg2a','ftm','fta','oreb','dreb','pf','pra','pr','pa','ra','bs','fantasy_score']
d=pd.read_sql(f"select game_date,event_id,team,home_team,away_team,player,position,{','.join(S)} from nba where team is not null",c)
d['gd']=pd.to_datetime(d.game_date)
pos=d.position.fillna('').str.upper()
d['pg']=np.where(pos.isin(['C','F-C','C-F']),'C',np.where(pos.isin(['G','PG','SG','G-F']),'G',np.where(pos.isin(['F','SF','PF','F-G']),'F','?')))
d=d[d.pg!='?'];d['opp']=np.where(d.team==d.home_team,d.away_team,d.home_team);d['k']=d.player.map(nm)
g=d.groupby(['event_id','gd','opp','pg'])[S].sum().reset_index()
ev=g[['event_id','gd','opp']].drop_duplicates();full=ev.merge(pd.DataFrame({'pg':['G','F','C']}),how='cross')
g=full.merge(g,how='left',on=['event_id','gd','opp','pg']).fillna(0).sort_values(['opp','pg','gd'])
roll=g.groupby(['opp','pg'])[S].transform(lambda x:x.shift(1).rolling(10,min_periods=4).mean())
lgm=g.sort_values('gd').groupby('pg')[S].transform(lambda x:x.shift(1).expanding(min_periods=50).mean()).reindex(g.index)
R=(roll/lgm);R[['opp','pg','gd']]=g[['opp','pg','gd']]
pl=d[['k','gd','opp','pg']].drop_duplicates(['k','gd'])
X=pd.read_pickle('/tmp/ds4.pkl');X['gd']=pd.to_datetime(X.fd)
mp={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','3-PT Attempted':'fg3a','Free Throws Made':'ftm','Free Throws Attempted':'fta','Personal Fouls':'pf','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score','Defensive Rebounds':'dreb','Offensive Rebounds':'oreb'}
X=X.merge(pl,on=['k','gd'],how='left');X=X[X.opp.notna()]
X['col']=X.prop.map(mp);X=X[X.col.notna()]
Rl=R.melt(id_vars=['opp','pg','gd'],value_vars=S,var_name='col',value_name='r')
X=X.merge(Rl,on=['opp','pg','gd','col'],how='left');X=X[X.r.notna()]
X['fav']=np.where(((X.side==1)&(X.r>=1.10))|((X.side==-1)&(X.r<=0.90)),'favorable',np.where(((X.side==1)&(X.r<=0.90))|((X.side==-1)&(X.r>=1.10)),'unfavorable','neutral'))
X['ho']=X.fd>='2026-05-09'
X.to_pickle('/tmp/posdef_ds.pkl');print(len(X))
def rep(name,m):
    z=X[m];print(name,'base',round(z.y.mean(),3),len(z))
    for f in ('favorable','neutral','unfavorable'):
        a=z[z.fav==f];h=a[a.ho];print(f'   {f:12s} {a.y.mean():.3f} ({len(a)})  hold {h.y.mean():.3f} ({len(h)})')
rep('ALL Standard Over',(X.pt=='Standard')&(X.side==1))
rep('ALL Standard Under',(X.pt=='Standard')&(X.side==-1))
rep('ALL Goblin',X.pt=='Goblin');rep('ALL Demon',X.pt=='Demon')
rep('Std Over gate B (L5,L10>=80,L20>=65,gap>=10%)',(X.pt=='Standard')&(X.side==1)&(X.h5>=.8)&(X.h10>=.8)&(X.h20>=.65)&(X.mg>=.1))
rep('Std Over gate A (L5,L10>=80)',(X.pt=='Standard')&(X.side==1)&(X.h5>=.8)&(X.h10>=.8))
rep('Std Under gate A (L5,L10>=80)',(X.pt=='Standard')&(X.side==-1)&(X.h5>=.8)&(X.h10>=.8))
rep('Goblin gate D (L5 5/5,L10>=90,L20>=80)',(X.pt=='Goblin')&(X.h5>=1)&(X.h10>=.9)&(X.h20>=.8))
rep('Goblin gate E (D + season>=75)',(X.pt=='Goblin')&(X.h5>=1)&(X.h10>=.9)&(X.h20>=.8)&(X.hs>=.75))
rep('Demon (L5>=60,L10>=60,L20>=50)',(X.pt=='Demon')&(X.h5>=.6)&(X.h10>=.6)&(X.h20>=.5))
