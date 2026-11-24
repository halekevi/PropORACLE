import pandas as pd,numpy as np,re,unicodedata,warnings; warnings.filterwarnings('ignore')
_u=lambda s:''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c))
nm=lambda s:re.sub(r'\b(jr|sr|ii|iii|iv)\b','',re.sub(r'[^a-z ]','',_u(s).lower().replace('.','').replace("'",''))).strip()
mp={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','3-PT Attempted':'fg3a','Free Throws Made':'ftm','Free Throws Attempted':'fta','Personal Fouls':'pf','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score'}
g=pd.read_parquet('data/game_logs/nba_player_games.parquet'); g=g[g.phase.astype(str).str.upper()!='PRESEASON']; g=g[~g.is_dnp.astype(bool)&(g.minutes>0)]
g['game_date']=pd.to_datetime(g.game_date); g['k']=g.player.map(nm)
cols=sorted(set(mp.values())); tg=g.groupby(['game_date','team'])[cols].sum().reset_index().sort_values('game_date')
for c in cols: tg[c+'_o10']=tg.groupby('team')[c].transform(lambda s:s.shift(1).rolling(10,min_periods=6).mean())   # team's own production, prior 10 games
tm=g[['k','game_date','team']].drop_duplicates(['k','game_date'])
t=pd.read_pickle('/tmp/ds3.pkl'); t=t.merge(tm.rename(columns={'game_date':'fd'}),on=['k','fd'],how='left')
t=t.merge(tg[['game_date','team']+[c+'_o10' for c in cols]].rename(columns={'game_date':'fd'}),on=['fd','team'],how='left')
t['own']=[r[mp[p]+'_o10'] if p in mp else np.nan for p,r in zip(t.prop,t[[c+'_o10' for c in cols]].to_dict('records'))]
# rank of the team's own production among all teams playing that date
t['tpct']=t.groupby(['fd','prop']).own.rank(pct=True,ascending=False)   # 0 = best producing team
t['orank']=t.groupby(['fd','prop']).opp_al10.rank(ascending=False,pct=True)  # 0 = weakest defense
b=lambda s:pd.cut(s,[-.01,.17,.5,.83,1.01],labels=['top5','mid-hi','mid-lo','bottom5'])
t['tb']=b(t.tpct); t['ob']=b(t.orank)
t.to_pickle('/tmp/ds4.pkl')
def cell(x): return f'{x.y.mean()*100:.1f}% ({len(x)})' if len(x)>=25 else '-'
S=t[t.pt=='Standard']
for nmx,x in [('STD OVER',S[S.side==1]),('STD UNDER',S[S.side==-1]),('DEMON',t[t.pt=='Demon'])]:
    print('\n=====',nmx,'  rows: own team rank (top5=best producing) / cols: opponent defense (top5 col = WEAKEST defense)')
    for s in ('train+val','hold'):
        y=x[x.split.isin(['train','val'])] if s=='train+val' else x[x.split=='hold']
        print('--',s)
        tab=pd.DataFrame({o:{tbk:cell(y[(y.tb==tbk)&(y.ob==o)]) for tbk in ['top5','mid-hi','mid-lo','bottom5']} for o in ['top5','mid-hi','mid-lo','bottom5']}); tab.index.name='own team'; print(tab.to_string())
    print('-- own team only, train+val / hold')
    for tbk in ['top5','mid-hi','mid-lo','bottom5']:
        print(tbk,cell(x[x.split.isin(['train','val'])&(x.tb==tbk)]),'|',cell(x[(x.split=='hold')&(x.tb==tbk)]))
