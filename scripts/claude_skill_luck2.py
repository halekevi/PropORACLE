import sqlite3,pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",c)
n['player']=n.player.str.lower().str.strip(); n['gd']=pd.to_datetime(n.game_date)
for k in ['minutes']+['pts','reb','ast','stl','blk','tov','fga','fg3a','fg3m','fta','ftm','fgm','fg2a','fg2m','oreb','dreb','pf','pra','pr','pa','ra','bs','fantasy_score']: n[k]=pd.to_numeric(n[k],errors='coerce')
n=n[(n.minutes>=15)].sort_values(['player','gd'])
rows=[]
for s in ['pts','reb','ast','stl','blk','tov','fga','fg3a','fg3m','fta','ftm','fgm','fg2a','fg2m','oreb','dreb','pf','pra','pr','pa','ra','bs','fantasy_score']:
    g=n.groupby('player')[s]; m=g.transform('mean'); cnt=g.transform('count'); x=n[cnt>=20].copy()
    # predictability: prior-10 mean vs next game (corr), and share of variance explained by player identity
    x['prior']=x.groupby('player')[s].transform(lambda v:v.shift(1).rolling(10,min_periods=8).mean())
    y=x.dropna(subset=['prior'])
    r=np.corrcoef(y.prior,y[s])[0,1]
    pm=x.groupby('player')[s].transform('mean'); between=(pm.var())/x[s].var()
    # dispersion vs Poisson within player
    within_var=x.groupby('player')[s].var().mean(); within_mean=x.groupby('player')[s].mean().mean()
    rows.append((s,round(r,2),round(between,2),round(within_var/within_mean,2),round(within_mean,1)))
print(pd.DataFrame(rows,columns=['stat','corr(prior10→next)','player_identity_share','var/mean(1=pure Poisson luck)','avg']).sort_values('corr(prior10→next)',ascending=False).to_string(index=False))
