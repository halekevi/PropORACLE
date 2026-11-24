"""Game log = parquet + every data/game_logs/nba_patch_claude_*.csv (dedup by player/date). Used by nightly board."""
import pandas as pd,numpy as np,glob,re,unicodedata
_u=lambda s:''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c))
nm=lambda s:re.sub(r'\b(jr|sr|ii|iii|iv)\b','',re.sub(r'[^a-z ]','',_u(s).lower().replace('.','').replace("'",''))).strip()
def load_log():
    g=pd.read_parquet('data/game_logs/nba_player_games.parquet'); g=g[g.phase.astype(str).str.upper()!='PRESEASON']; g=g[~g.is_dnp.astype(bool)&(g.minutes>0)].copy()
    g['game_date']=pd.to_datetime(g.game_date)
    ps=[]
    for f in sorted(glob.glob('data/game_logs/nba_patch_claude_*.csv')):
        p=pd.read_csv(f); ps.append(p)
    p=pd.concat(ps,ignore_index=True)
    for c in ['oreb','dreb','pf','opponent','home_away','starter']:
        if c not in p: p[c]=np.nan
    if (open('data/game_logs/nba_patch_pf_rest.txt').read().strip() if False else None): pass
    p['game_date']=pd.to_datetime(p.game_date.astype(str),format='%Y%m%d'); p=p[p.minutes>0].copy()
    p['k']=p.player.map(nm); g['k']=g.player.map(nm)
    p=p.drop_duplicates(['k','game_date'],keep='last'); p=p[~p.set_index(['k','game_date']).index.isin(g.set_index(['k','game_date']).index)]
    p['fg2m']=p.fgm-p.fg3m; p['fg2a']=p.fga-p.fg3a; p['pra']=p.pts+p.reb+p.ast; p['pr']=p.pts+p.reb; p['pa']=p.pts+p.ast; p['ra']=p.reb+p.ast; p['bs']=p.blk+p.stl
    p['fantasy_score']=p.pts+1.2*p.reb+1.5*p.ast+3*p.stl+3*p.blk-p.tov
    p['starter_flag']=p.starter; p['is_dnp']=False
    keep=[c for c in p.columns if c in g.columns or c=='k']
    out=pd.concat([g,p[keep]],ignore_index=True).sort_values(['k','game_date'])
    return out,len(p)
