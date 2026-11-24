import sqlite3,pandas as pd,numpy as np
c=sqlite3.connect('data/cache/proporacle_ref.db'); out={}
def build(t,mins,start,minteam):
    d=pd.read_sql(f"select game_date,event_id,team,home_team,away_team,minutes,fga,fta,oreb,tov,pts from {t} where game_date>='{start}' and coalesce({'season_window' if t=='nba' else repr('')},'')!='preseason'",c)
    for k in ['minutes','fga','fta','oreb','tov','pts']: d[k]=pd.to_numeric(d[k],errors='coerce')
    g=d.groupby(['event_id','game_date','team']).agg(fga=('fga','sum'),fta=('fta','sum'),oreb=('oreb','sum'),tov=('tov','sum'),pts=('pts','sum'),mn=('minutes','sum')).reset_index()
    g=g[g.fga>20]; g['poss']=g.fga+0.44*g.fta-g.oreb+g.tov; g['gm_min']=(g.mn/5).clip(lower=mins)
    # game possessions = average of the two teams in the event
    gp=g.groupby('event_id').poss.transform('mean'); g['game_poss']=gp; g['pace']=g.game_poss/g.gm_min*mins
    g=g.sort_values('game_date')
    T=g.groupby('team').agg(games=('pace','size'),poss_per_game=('game_poss','mean'),pace=('pace','mean'),pts_per_game=('pts','mean')).round(2)
    T['pace_L5']=g.groupby('team').pace.apply(lambda s:s.tail(5).mean()).round(2)
    T=T[T.games>=3].sort_values('pace',ascending=False); T['pace_rank']=range(1,len(T)+1); T['vs_league_avg']=(T.pace-g.pace.mean()).round(2)
    return T.reset_index(),g
for t,m,s in (('nba',48,'2026-10-20'),('cbb',40,'2026-11-01')):
    T,g=build(t,m,s,None); out[t.upper()+' team pace']=T
    print(t,len(T),'teams; league pace',round(g.pace.mean(),2)); print(T.head(5)[['team','games','pace','poss_per_game','pace_L5']].to_string(index=False)); print(T.tail(3)[['team','games','pace']].to_string(index=False))
with pd.ExcelWriter('outputs/pace_nba_cbb_2026-11-14.xlsx') as w:
    for k,v in out.items(): v.to_excel(w,sheet_name=k,index=False)
