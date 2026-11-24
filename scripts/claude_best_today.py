import _r2
import pandas as pd,numpy as np,sys; sys.path.insert(0,'scripts'); from claude_defense_rel import table
d=pd.read_pickle('/tmp/full_nba.pkl'); T=table()
AB={'SAS':'SA','GSW':'GS','NOP':'NO','NYK':'NY','UTA':'UTAH','WAS':'WSH','PHO':'PHX','BRK':'BKN'}
MP={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','3-PT Attempted':'fg3a','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','Free Throws Made':'ftm','Free Throws Attempted':'fta','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Offensive Rebounds':'oreb','Defensive Rebounds':'dreb','Fantasy Score':'fantasy_score','Personal Fouls':'pf'}
d['col']=d.Prop.map(MP); d=d[d.col.notna()&~d.Player.astype(str).str.contains(r' \+ |/',regex=True)].copy()
d['opp1']=d.Opp.astype(str).str.upper().map(lambda x:AB.get(x,x)); d['rel']=[T.loc[o,c] if o in T.index and c in T.columns else np.nan for o,c in zip(d.opp1,d.col)]
for c in ['L5%','L10%','L20%','seas%','zSD','games','Line']: d[c]=pd.to_numeric(d[c],errors='coerce')
print('scale check L5%:',d['L5%'].dropna().max(), '| opp unmatched:',sorted(set(d[d.rel.isna()].Opp.astype(str)))[:8])
sc=100 if d['L5%'].max()>1.5 else 1
S=d[d.Type=='Standard']; dup=S.groupby(['Player','col']).Line.nunique(); dk=set(dup[dup>1].index)
u=S[(S.Side=='Under')&(S['L5%']>=.8*sc)&(S['L10%']>=.8*sc)&(S['L20%']>=.65*sc)&(S.zSD>=.5)&(S.Line>1.0)&~S.col.isin(['stl','pf','bs'])&(S['seas%']>=.55*sc)&(S.games>=30)]
u=u[[ (p,c) not in dk for p,c in zip(u.Player,u.col)]].copy(); u['defense']=np.where(u.rel<1.0,'TOUGH',np.where(u.rel>1.05,'soft','mid'))
print('\nSTANDARD UNDER candidates (gate + long-run filter), sorted by gap in SD:',len(u),'| dup-Standard players excluded:',len(dk))
print(u.sort_values('zSD',ascending=False)[['Player','Team','Opp','Prop','Line','L5','L5%','L10%','L20%','seas%','zSD','rel','defense']].round(2).head(30).to_string(index=False))
G=d[(d.Type=='Goblin')&(d.Side=='Over')&(d['L5%']>=1.0*sc)&(d['L10%']>=.9*sc)&(d['L20%']>=.8*sc)&(d.zSD>=1.0)&(d.games>=20)].copy(); G['defense']=np.where(G.rel>1.05,'SOFT',np.where(G.rel<.95,'tough','mid'))
print('\nGOBLIN OVER (gate D + gap>=1 SD):',len(G)); print(G.sort_values('zSD',ascending=False)[['Player','Team','Opp','Prop','Line','L10%','L20%','zSD','rel','defense']].round(2).head(30).to_string(index=False))
