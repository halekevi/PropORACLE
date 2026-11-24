import _r2
"""Top-30 props per sport from the main_cp boards (Nov 4-9) using each sport's game log. Usage: python3 scripts/claude_top30.py
Rules: L10 = last 10 games played (non-DNP, non-preseason), NBA needs 10 games & >=8/10; other sports need >=7 games & >=80%. L5 4/5+ kept. Standard Over+Under; Goblin/Demon Over only.
Excluded props: longest*, saves, fantasy score, double/triple double, dunks, combos not in log. Ranked by L10 pct, L5, then average cushion. Max 2 rows per player. Flags only; stale log tagged."""
import pandas as pd,numpy as np,re,glob,os
R='../PropORACLE_main_cp/outputs/'
import unicodedata
_u=lambda s:''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c))
nm=lambda s:re.sub(r'\b(jr|sr|ii|iii|iv)\b','',re.sub(r'[^a-z ]','',_u(s).lower().replace('.','').replace("'",''))).strip()
def logs(f,need):
    g=pd.read_parquet('data/game_logs/'+f)
    if 'phase' in g: g=g[g.phase.astype(str).str.upper()!='PRESEASON']
    if 'is_dnp' in g: g=g[~g.is_dnp.astype(bool)]
    g['k']=g.player.map(nm); g['game_date']=pd.to_datetime(g.game_date); return g.sort_values(['k','game_date'])
def build(sport,board,g,mp,minn,gate,pcol,tcol,pkcol,boardfile_dates):
    last=g.game_date.max().date();     gt=g.team.astype(str)
    byname={}
    for k_,t_ in zip(g.k,gt): byname.setdefault(k_,set()).add(t_)
    from collections import Counter,defaultdict
    vote=defaultdict(Counter)
    for _,r_ in board.drop_duplicates(['_player','team']).iterrows():
        c_=byname.get(nm(r_['_player']))
        if c_ and len(c_)==1: vote[str(r_['team']).upper()][list(c_)[0]]+=1
    tmap={a_:c.most_common(1)[0][0] for a_,c in vote.items()}
    hist={}
    for (k_,t_),v in g.groupby(['k',gt]): hist.setdefault(k_,{})[t_]=v
    tn=lambda t:tmap.get(str(t).upper().strip(),'?')
    out=[]; skipped=0; amb=0
    for _,r in board.iterrows():
        e=mp.get(r[pcol]); 
        if e is None: continue
        cand=hist.get(nm(r['_player']))
        if not cand: continue
        bt=tn(r['team'])
        if bt in cand: h=cand[bt]
        elif len(cand)==1: h=list(cand.values())[0]
        else: skipped+=1; continue
        try: v=sum(h[c].astype(float).fillna(0).values for c in e)[::-1]
        except KeyError: continue
        if len(v)<minn: continue
        pt=str(r[pkcol]).lower(); sides=['OVER','UNDER'] if pt=='standard' else ['OVER']
        for s in sides:
            hit=(lambda x:(x>r.line) if s=='OVER' else (x<r.line)); a=hit(v[:10]); b=hit(v[:5]); n10=len(a)
            if a.mean()<gate or b.sum()<4: continue
            out.append(dict(sport=sport,board_date=r['_date'],player=r['_player'],team=r['team'],opp=r.get('opp_team',r.get('pp_opp_team','')),side=s,prop=r[pcol],line=r.line,pick_type=pt.title(),
              l10=f'{int(a.sum())}/{n10}',l5=f'{int(b.sum())}/5',l10_pct=round(a.mean()*100),l5_hits=int(b.sum()),avg10=round(v[:10].mean(),1),avg5=round(v[:5].mean(),1),gap5=round((v[:5].mean()-r.line) if s=='OVER' else (r.line-v[:5].mean()),1),cushion=round((v[:10].mean()-r.line) if s=='OVER' else (r.line-v[:10].mean()),1),last10=' '.join(f'{x:g}' for x in v[:10]),newest_game=str(last),tip=r.get('start_time','')))
    print(sport,'rows skipped (same name, no team match):',skipped)
    d=pd.DataFrame(out)
    if d.empty: return d
    d['cushion_pct']=(d.cushion/d.line.replace(0,np.nan)*100).round(0)
    d['gap5_pct']=(d.gap5/d.line.replace(0,np.nan)*100).round(0)
    d['min_gap_pct']=d[['gap5_pct','cushion_pct']].min(axis=1)
    _a=d.l10.str.split('/',expand=True).astype(int); d['miss']=(_a[1]-_a[0])+(5-d.l5_hits)
    d=d.sort_values(['miss','l5_hits','l10_pct','min_gap_pct'],ascending=[True,False,False,False])
    d=d.drop_duplicates(['player','prop','side','pick_type'])  # best row per player/prop/side
    d['pc']=d.groupby(['player','pick_type']).cumcount(); d=d[d.pc<2].drop(columns='pc')
    return d
def board_load(sport,pat,pc,pt,plyr,datefilter=None):
    fs=[];
    for f in sorted(glob.glob(R+pat)):
        try: d=pd.read_csv(f,low_memory=False)
        except Exception: continue
        if len(d)==0: continue
        d['_date']=f.split('outputs/')[1][:10]; d['_player']=d[plyr]; fs.append(d)
    return pd.concat(fs,ignore_index=True) if fs else pd.DataFrame()
res={}
# NFL
b=board_load('NFL','2026-11-0[5-9]/nfl/step1_pp_props_today.csv','prop_type','pick_type','player_name')
mp={'receiving_yards':['rec_yds'],'receptions':['rec'],'rushing_yards':['rush_yds'],'passing_yards':['pass_yds'],'pass_rush_yds':['pass_yds','rush_yds'],'rush_attempts':['rush_att'],'pass_completions':['pass_cmp'],'passing_tds':['pass_td'],'pass_attempts':['pass_att'],'rush_rec_yds':['rush_yds','rec_yds'],'int':['pass_int'],'kicking_points':['kick_pts'],'anytime_tds':['rush_td','rec_td']}
gn_=logs('nfl_player_games.parquet',0)
pt_=pd.read_csv('data/game_logs/nfl_patch_claude_20261101.csv')
pt_['game_date']=pd.to_datetime(pt_.game_date.astype(str),format='%Y%m%d')
last_=gn_.sort_values('game_date').groupby('k').team.last()
pt_['k']=pt_.player.map(nm); pt_['team']=pt_.k.map(last_)
for c_ in ['pass_yds','pass_att','pass_cmp','pass_td','pass_int','rush_att','rush_yds','rush_td','rec','rec_yds','rec_td','kick_pts']: pt_[c_]=pt_[c_].astype(float)
gn_=pd.concat([gn_,pt_.drop(columns=['team_abbr','evt'])],ignore_index=True).sort_values(['k','game_date'])
res['NFL']=build('NFL',b,gn_,mp,7,.8,'prop_type','','pick_type',None)
# CFB
b=board_load('CFB','2026-11-0[5-9]/cfb/step1_cfb.csv','stat_type','odds_type','player')
mp={'Receiving Yards':['rec_yds'],'Rush Yards':['rush_yds'],'Pass Yards':['pass_yds'],'Receptions':['rec'],'Pass TDs':['pass_td'],'Rec TDs':['rec_td'],'Rush TDs':['rush_td'],'Anytime TDs':['rush_td','rec_td'],'Pass+Rush Yds':['pass_yds','rush_yds'],'Pass Attempts':['pass_att'],'Rush Attempts':['rush_att'],'Rush+Rec Yds':['rush_yds','rec_yds'],'Kicking Points':['kick_pts']}
res['CFB']=build('CFB',b,logs('cfb_player_games.parquet',0),mp,7,.8,'stat_type','','odds_type',None)
# Soccer
b=board_load('SOC','2026-11-0[5-9]/soccer/step1_soccer_props.csv','prop_type','pick_type','player')
mp={'Shots':['shots'],'Shots On Target':['shots_on_target'],'Goals':['goals'],'Goal + Assist':['goals','assists'],'Assists':['assists'],'Tackles':['tackles'],'Fouls':['fouls']}
gs=logs('soccer_player_games.parquet',0); gs=gs[gs.minutes>0]
res['SOCCER']=build('SOCCER',b,gs,mp,7,.8,'prop_type','','pick_type',None)
# NBA
b=board_load('NBA','2026-11-05/nba/step1_pp_props_today.csv','prop_type','pick_type','player')
mp={'Points':['pts'],'Rebounds':['reb'],'Assists':['ast'],'3-PT Made':['fg3m'],'Steals':['stl'],'Blocked Shots':['blk'],'Turnovers':['tov'],'Pts+Rebs+Asts':['pra'],'Pts+Rebs':['pr'],'Pts+Asts':['pa'],'Rebs+Asts':['ra'],'Blks+Stls':['bs'],'FG Made':['fgm'],'FG Attempted':['fga'],'3-PT Attempted':['fg3a'],'Free Throws Made':['ftm'],'Free Throws Attempted':['fta'],'Personal Fouls':['pf'],'Two Pointers Made':['fg2m'],'Two Pointers Attempted':['fg2a']}
gn=logs('nba_player_games.parquet',0) if os.path.exists('data/game_logs/nba_player_games.parquet') else None
gn=gn[gn.minutes>0]
_ra=pd.concat([pd.read_csv('data/game_logs/nba_patch_claude_rest_a.csv'),pd.read_csv('data/game_logs/nba_patch_claude_rest_b.csv')],ignore_index=True)
_ra['pf']=[int(c) for c in open('data/game_logs/nba_patch_pf_rest.txt').read().strip()]
pt=pd.concat([pd.read_csv('data/game_logs/nba_patch_claude_20261031_20261102.csv'),pd.read_csv('data/game_logs/nba_patch_claude_board_20261031_20261102.csv'),_ra,pd.read_csv('data/game_logs/nba_patch_claude_20261104_phx_mia.csv')],ignore_index=True).fillna(0)
pt['game_date']=pd.to_datetime(pt.game_date.astype(str),format='%Y%m%d'); pt['k']=pt.player.map(nm); pt=pt.drop_duplicates(['k','game_date'],keep='last')
for a_,b_ in [('pra',['pts','reb','ast']),('pr',['pts','reb']),('pa',['pts','ast']),('ra',['reb','ast']),('bs',['blk','stl'])]: pt[a_]=pt[b_].sum(axis=1)
pt['fg3m']=pt.fgm*0+pt.fg3m
gn=pd.concat([gn,pt],ignore_index=True).sort_values(['k','game_date'])
gn['fg2m']=gn.fgm-gn.fg3m; gn['fg2a']=gn.fga-gn.fg3a
res['NBA']=build('NBA',b,gn,mp,10,.8,'prop_type','','pick_type',None)
res['NBA'].to_csv('outputs/2026-11-03_top30/nba_all_qualifying.csv',index=False)
for s_,d in res.items():
    for pt in ['Standard','Goblin','Demon']:
        t=d[d.pick_type==pt].head(30).copy() if not d.empty else d
        t.insert(0,'rank',range(1,len(t)+1)) if len(t) else None
        t.to_csv(f'outputs/2026-11-03_top30/{s_.lower()}_{pt.lower()}.csv',index=False)
        print(f'\n### {s_} {pt} ({len(t)} qualify, capped at 30)')
        if len(t):
            print('| # | Player | Team | Side | Prop | Line | L10 | L5 | Avg | Date |\n|---|---|---|---|---|---|---|---|---|---|')
            for _,r in t.iterrows(): print(f"| {r['rank']} | {r.player} | {r.team} | {r.side.title()} | {r.prop} | {r.line:g} | {r.cushion:+g} ({r.cushion_pct:.0f}%) | {r.l10} | {r.l5} | {r.board_date[5:]} |")
