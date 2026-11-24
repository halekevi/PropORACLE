"""Opponent-defense softness per stat. rel = opp's allowed-per-game (last 15 games, prior to date) / league avg. >1 soft, <1 tough.
Usage: from claude_defense_rel import table; t=table()  -> DataFrame index=opponent(ESPN abbr), columns=stat
(Map ledger abbrs to ESPN: SAS->SA, GSW->GS, NOP->NO, NYK->NY, UTA->UTAH, WAS->WSH.)"""
import sqlite3,pandas as pd,numpy as np
S=['pts','reb','ast','stl','blk','tov','fgm','fga','fg3m','fg3a','fg2m','fg2a','ftm','fta','oreb','dreb','pf','pra','pr','pa','ra','bs','fantasy_score']
def table(db='data/cache/proporacle_ref.db'):
    n=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",sqlite3.connect(db))
    for s in S+['minutes']: n[s]=pd.to_numeric(n[s],errors='coerce')
    n=n[n.minutes>0]; n['gd']=pd.to_datetime(n.game_date); n['opponent']=np.where(n.team==n.home_team,n.away_team,n.home_team)
    tg=n.groupby(['opponent','gd'])[S].sum().reset_index().sort_values('gd')
    out={}
    for s in S:
        r=tg.groupby('opponent')[s].apply(lambda v:v.tail(15).mean()); out[s]=r/tg[s].mean()
    return pd.DataFrame(out)
if __name__=='__main__': print(table().round(2).to_string())
