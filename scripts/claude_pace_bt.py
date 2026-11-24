import pandas as pd,numpy as np,sqlite3
c=sqlite3.connect('data/cache/proporacle_ref.db')
d=pd.read_sql("select game_date,event_id,team,minutes,fga,fta,oreb,tov from nba where coalesce(season_window,'')!='preseason'",c)
for k in ['minutes','fga','fta','oreb','tov']: d[k]=pd.to_numeric(d[k],errors='coerce')
g=d.groupby(['event_id','game_date','team']).agg(fga=('fga','sum'),fta=('fta','sum'),oreb=('oreb','sum'),tov=('tov','sum'),mn=('minutes','sum')).reset_index()
g=g[g.fga>20]; g['poss']=g.fga+0.44*g.fta-g.oreb+g.tov; g['gd']=pd.to_datetime(g.game_date)
g['gp']=g.groupby('event_id').poss.transform('mean'); g['pace']=g.gp/(g.mn/5).clip(lower=48)*48
g=g.sort_values('gd')
TP={t:v[['gd','pace']].reset_index(drop=True) for t,v in g.groupby('team')}
def tp(team,day,n=10):
    x=TP.get(team)
    if x is None: return np.nan
    x=x[x.gd<day].tail(n)
    return x.pace.mean() if len(x)>=5 else np.nan
G=pd.read_csv('data/training/graded_export_20260516.csv',low_memory=False)
G=G[(G.sport=='NBA')&G.result_binary.isin([0,1])&G.void_reason.isna()&G.actual_value.notna()&(G.actual_value!=G.line)].copy()
G['d']=pd.to_datetime(G.file_date); print(len(G),G.team.nunique(),sorted(set(G.team.dropna())-set(TP))[:10],sorted(set(G.opp_team.dropna())-set(TP))[:10])
AB={'NYK':'NY','GSW':'GS','NOP':'NO','SAS':'SA','UTA':'UTAH','WAS':'WSH','PHO':'PHX','BRK':'BKN'}
def m(t): return t if t in TP else AB.get(t,t)
G['gp']=[np.nanmean([tp(m(a),d0),tp(m(b),d0)]) if not (pd.isna(a) or pd.isna(b)) else np.nan for a,b,d0 in zip(G.team,G.opp_team,G.d)]
G=G.dropna(subset=['gp']); print(len(G))
G['over']=(G.actual_value>G.line).astype(int)
G['bin']=pd.qcut(G.gp,3,labels=['slow','mid','fast'])
print(G.groupby('pick_type').size().to_dict())
for pk in ['Standard','Demon','Goblin']:
    x=G[G.pick_type==pk]; print('\n',pk,len(x)); print(x.groupby('bin',observed=True).over.agg(['mean','size']).round(4))
    print(x[x.prop.isin(['Points','Pts+Rebs+Asts','Rebounds','Assists','3-PT Made'])].pivot_table(index='prop',columns='bin',values='over',aggfunc='mean',observed=True).round(4))
print('pace cutoffs',G.gp.quantile([.33,.67]).round(2).tolist())
G.to_csv('outputs/pace_bt.csv',index=False)
