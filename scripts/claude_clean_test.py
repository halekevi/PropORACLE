import sqlite3,pandas as pd,numpy as np,sys,warnings;warnings.filterwarnings('ignore')
sys.path.insert(0,'scripts');from claude_gamelog import nm
X=pd.read_pickle('/tmp/posdef_ds.pkl')
c=sqlite3.connect('data/cache/proporacle_ref.db')
d=pd.read_sql("select game_date,player,minutes,starter from nba where team is not null and coalesce(season_window,'')!='preseason'",c);d['gd']=pd.to_datetime(d.game_date);d=d[d.minutes.fillna(0)>0];d['k']=d.player.map(nm)
ser={k:v.sort_values('gd') for k,v in d.groupby('k')}
rows=[]
for r in X.itertuples():
    s=ser[r.k];s=s[s.gd<r.gd];m=s.minutes.values.astype(float);dt=s.gd.values
    rows.append((m[-10:].mean(),m[-10:].std(),m[-5:].min(),m[-5:].mean()-m[-20:].mean(),(r.gd-pd.Timestamp(dt[-1])).days))
F=pd.DataFrame(rows,columns=['mn10','mnsd','mnfl5','mntrend','dsl'],index=X.index);X=pd.concat([X,F],axis=1);X.to_pickle('/tmp/posdef_ds2.pkl');print(len(X))
