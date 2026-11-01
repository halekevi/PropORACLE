"""Rebuilds the NFL Matchup Edge JSON from the 2026 game log: per team x category, top 5 and bottom 5 players.
Usage: python3 scripts/claude_nfl/build_nfl_matchup_edge.py 2026-11-01   (writes Sports/NFL/data/nfl_matchup_edge.json)"""
import sys,json,pandas as pd,numpy as np
D=sys.argv[1] if len(sys.argv)>1 else '2026-11-01'
g=pd.read_parquet('data/game_logs/nfl_player_games.parquet')
g=g[(~g.is_dnp.astype(bool))&(g.game_date>='2026-09-01')&(g.game_date<D)&(g.phase.astype(str).str.upper()!='PRESEASON')].sort_values('game_date')
CATS=[('pass_yds','Pass yards',200.0,'PASS_YDS','pass_def_rank'),('rush_yds','Rush yards',60.0,'RUSH_YDS','rush_def_rank'),
('rec_yds','Receiving yards',50.0,'REC_YDS','pass_def_rank'),('receptions','Receptions',4.0,'REC','pass_def_rank'),
('pass_td','Pass TDs',1.5,'PASS_TD','pass_def_rank'),('rush_td','Rush TDs',0.5,'RUSH_TD','rush_def_rank'),
('rec_td','Receiving TDs',0.4,'REC_TD','pass_def_rank'),('kick_pts','Kicking points',7.0,'KICK_PTS','kick_pts_def_rank')]
g=g.copy(); g['tid']=g.team_id.astype(str)
_b=pd.read_excel(f'outputs/{D}/nfl/step8_nfl_direction_clean.xlsx',sheet_name='ALL')[['Player','Team']].dropna()
_n=lambda t:''.join(ch for ch in str(t).lower().replace('.','') if ch.isalpha() or ch==' ').replace(' sr','').replace(' jr','').strip()
_b['pn']=_b.Player.map(_n); g['pn']=g.player.map(_n)
_m=g.merge(_b[['pn','Team']].drop_duplicates('pn'),on='pn'); ab=_m.groupby('tid').Team.agg(lambda s:s.mode().iat[0])
print('team ids mapped',len(ab),'of',g.tid.nunique())
team=g.groupby('player_key').tid.last().map(ab)
dfr=pd.read_csv(f'outputs/{D}/nfl/defense_rankings.csv').set_index('team')
x=pd.read_excel(f'outputs/{D}/nfl/step8_nfl_direction_clean.xlsx',sheet_name='ALL')[['Team','Opp']].dropna().drop_duplicates('Team'); opp=dict(zip(x.Team,x.Opp))
def tier(r): return 'ELITE' if r<=8 else 'WEAK' if r>=25 else 'MID'
out={}; N=0
for cid,lab,thr,col,rk in CATS:
    a=g.groupby('player_key').agg(player=('player','last'),gp=(col,'size'),avg=(col,'mean'),tot=(col,'sum'))
    a['team']=team; a=a.dropna(subset=['team']); a=a[a.gp>=2]
    if cid.endswith('td') or cid=='kick_pts': a=a[a.tot>0]
    else: a=a[a.avg>={'pass_yds':20,'rush_yds':3,'rec_yds':8,'receptions':0.8}[cid]]
    a['league_rank']=a.avg.rank(ascending=False,method='min').astype(int)
    for t,b in a.groupby('team'):
        b=b.sort_values('avg',ascending=False); n=len(b)
        top=b.head(5); bot=b.iloc[5:].tail(5); rows=[]
        for i,(k,r) in enumerate(top.iterrows(),1): rows.append((r,i,None,'top'))
        for j,(k,r) in enumerate(bot.iloc[::-1].iterrows(),1): rows.append((r,None,j,'bottom'))
        o=opp.get(t,''); dr=dfr[rk].get(o) if o in dfr.index else None
        pl=[{'player':(r.player.title() if r.player==r.player.lower() else r.player),'player_norm':r.player.lower(),'pos':'','rank_on_team':rt,'bottom_rank_on_team':rb,'leader_slice':sl,'bottom3_on_team':bool(rb and rb<=3),
             'season_avg':round(float(r.avg),2),'games':int(r.gp),'league_rank':int(r.league_rank),'team_n':n,'game_score':round(float(r.avg),1),
             'edge':'NEUTRAL','notes':'No strong matchup edge either way.','overperform_vs_weak':False,'def_boost':None,'pp_line':None,'pp_edge':None} for r,rt,rb,sl in rows]
        out[f'{t}|{cid}']={'team_slate':t,'category':cid,'category_label':lab,'threshold':thr,
            'opponent':{'slate_abbr':o,'name':o,'def_rank':None if dr is None or pd.isna(dr) else int(dr),'def_tier':'' if dr is None or pd.isna(dr) else tier(dr)},'players':pl}; N+=len(pl)
old=json.load(open('Sports/NFL/data/nfl_matchup_edge.json'))
old.update({'generated_at':pd.Timestamp.utcnow().isoformat(),'slate_note':f'2026 season through {D} (game log). Top 5 and bottom 5 players per team and category by season average.','players_by_team_cat':out,'season':2026})

for p in ('Sports/NFL/data','mobile/www/data','ui_runner/templates'): json.dump(old,open(f'{p}/nfl_matchup_edge.json','w'))
print('blocks',len(out),'player rows',N,'blocks with bottom',sum(any(q['leader_slice']=='bottom' for q in v['players']) for v in out.values()),'opp filled',sum(1 for v in out.values() if v['opponent']['slate_abbr']))
