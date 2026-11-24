import _r2
"""NBA log-only forward tracking T1-T4 (LOCKED 2026-11-11; T5 added 2026-11-11 = Standard Under on Assists for guards, no other gate) + display-only Goblin Over score. Usage: python3 scripts/claude_nba_track.py YYYY-MM-DD  (needs /tmp/full_nba.pkl from claude_full_lists.py and /tmp/role_ds.pkl)"""
import pandas as pd,numpy as np,sys,os,sqlite3,datetime,warnings; warnings.filterwarnings('ignore'); sys.path.insert(0,'scripts')
from sklearn.linear_model import LogisticRegression; from sklearn.preprocessing import StandardScaler
day=sys.argv[1]; d=pd.read_pickle('/tmp/full_nba.pkl')
MP={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','3-PT Attempted':'fg3a','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','Free Throws Made':'ftm','Free Throws Attempted':'fta','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Offensive Rebounds':'oreb','Defensive Rebounds':'dreb','Fantasy Score':'fantasy_score','Personal Fouls':'pf'}
d['col']=d.Prop.map(MP); d=d[d.col.notna()&~d.Player.astype(str).str.contains(r' \+ |/',regex=True)].copy()
for c in ['L5%','L10%','L20%','seas%','zSD','games','Line','cv20']: d[c]=pd.to_numeric(d[c],errors='coerce')
S=d[d.Type=='Standard'].copy(); dup=S.groupby(['Player','col']).Line.nunique(); dk=set(dup[dup>1].index); S=S[[ (p,c) not in dk for p,c in zip(S.Player,S.col)]&(S.Line>1.0)]
U=S[S.Side=='Under']; O=S[S.Side=='Over']; b=lambda x:(x['L10%']>=70)&(x['L20%']>=60)
from claude_gamelog import nm
_p=pd.read_sql("select player,position from nba where position in ('G','PG','SG','F','C','PF','SF')",sqlite3.connect('data/cache/proporacle_ref.db')); _p['k']=_p.player.map(nm); _pos=_p.groupby('k').position.agg(lambda x:x.mode().iloc[0]); isG=lambda x:x.Player.map(nm).map(_pos).isin(['G','PG','SG'])
G={'T1':U[b(U)&(U.zSD>=1.0)],'T2':U[U.col.isin(['fga','fg2a'])&(U['L10%']>=80)&(U['L20%']>=65)],'T3':U[b(U)&(U.zSD>=0.5)],'T4':O[b(O)&(O.zSD>=1.0)],'T5':U[(U.col=='ast')&isG(U)]}
rows=[dict(date=day,group=g,player=r.Player,prop=r.Prop,col=r.col,side=r.Side.upper(),line=r.Line,h5=r._8 if False else r.L5,h10=r._13 if False else r[13] if False else r.__getattribute__('_14') if False else None) for g,x in G.items() for r in x.itertuples()] if False else []
for g,x in G.items():
    for _,r in x.iterrows(): rows.append(dict(date=day,group=g,player=r.Player,team=r.Team,opp=r.Opp,prop=r.Prop,col=r.col,side=r.Side.upper(),line=r.Line,h5=r['L5'],h10=r['L10%'],h20=r['L20%'],zmin=round(r.zSD,2),result='',actual='',graded_at=''))
new=pd.DataFrame(rows); OUT='outputs/ticket_log/forward_tracking.csv'; old=pd.read_csv(OUT) if os.path.exists(OUT) else pd.DataFrame()
if len(old): old=old[old.date!=day]
df=pd.concat([old,new],ignore_index=True)
# grade older rows from DB box scores
c=sqlite3.connect('data/cache/proporacle_ref.db'); n=pd.read_sql("select player,game_date,"+",".join(sorted(set(MP.values())))+" from nba where coalesce(season_window,'')!='preseason'",c); n['k']=n.player.str.lower().str.strip()
for col in ['result','actual','graded_at']: df[col]=df[col].astype('object')
for i,r in df[df.result.isna()|(df.result=='')].iterrows():
    if r.date>=day: continue
    x=n[(n.k==str(r.player).lower().strip())&(n.game_date.astype(str)==str(r.date))]
    if len(x): a=float(x.iloc[0][r.col]); df.loc[i,'actual']=a; df.loc[i,'result']='PUSH' if a==r.line else ('HIT' if (a>r.line)==(r.side=='OVER') else 'MISS'); df.loc[i,'graded_at']=datetime.datetime.utcnow().isoformat(timespec='seconds')+'Z'
df.to_csv(OUT,index=False)
print('NBA tracking logged today:',new.groupby('group').size().to_dict() if len(new) else 'none'); g=df[df.result.isin(['HIT','MISS'])]; print('graded so far:',g.groupby('group').result.apply(lambda s:f'{(s=="HIT").mean():.0%} of {len(s)}').to_dict() if len(g) else 'none yet')
# display-only Goblin Over score
R=pd.read_pickle('/tmp/role_ds.pkl'); R=R[(R.pt=='Goblin')&(R.side==1)&R.hit.notna()&(R.sdate<pd.Timestamp(day))].copy(); F=['h5','h10','h20','hs','zmin','cv']
for f in F: R[f]=pd.to_numeric(R[f],errors='coerce')
R=R.dropna(subset=F); sc=StandardScaler().fit(R[F]); m=LogisticRegression(C=.3,max_iter=500).fit(sc.transform(R[F]),R.hit.astype(float))
Gb=d[(d.Type=='Goblin')&(d.Side=='Over')].dropna(subset=['L5%','L10%','L20%','seas%','zSD','cv20']).copy()
X=pd.DataFrame({'h5':Gb['L5%']/100,'h10':Gb['L10%']/100,'h20':Gb['L20%']/100,'hs':Gb['seas%']/100,'zmin':Gb.zSD,'cv':Gb.cv20}); Gb['goblin_score']=m.predict_proba(sc.transform(X))[:,1].round(3)
Gb[['Player','Team','Opp','Prop','Line','L5','L10%','L20%','seas%','zSD','goblin_score']].sort_values('goblin_score',ascending=False).to_csv(f'outputs/goblin_scores_{day}.csv',index=False); print('goblin score rows',len(Gb),'top:',Gb.goblin_score.max())
