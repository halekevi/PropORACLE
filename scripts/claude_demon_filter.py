"""Nightly Demon filter + Standard-Over tiebreaker. Usage: python scripts/claude_demon_filter.py 2026-11-06
 DEMON  A: votes>=7 (test 45%/41%/46% train/val/holdout; Demon base 36/37/29%)
        B: smallest side gap >=6.7% of line AND season hit rate >=.54 (53/44/48%)
        C: line below L20 mean (z<.12) AND line cleared in >=9 of last 20 AND home (42/43/42%)
 STANDARD OVER tiebreaker: line cleared in >=14 of last 20 (56/52/54% vs base 50/49/48%)
 Logs every flagged pick to outputs/forward_test/demon_std_forward.csv for later grading."""
import sys,os,re,unicodedata,pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
D=sys.argv[1] if len(sys.argv)>1 else '2026-11-06'; asof=pd.Timestamp(D)
_u=lambda s:''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c))
nm=lambda s:re.sub(r'\b(jr|sr|ii|iii|iv)\b','',re.sub(r'[^a-z ]','',_u(s).lower().replace('.','').replace("'",''))).strip()
mp={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','3-PT Attempted':'fg3a','Free Throws Made':'ftm','Free Throws Attempted':'fta','Personal Fouls':'pf','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score','Defensive Rebounds':'dreb','Offensive Rebounds':'oreb'}
g=pd.read_parquet('data/game_logs/nba_player_games.parquet'); g=g[g.phase.astype(str).str.upper()!='PRESEASON']; g=g[~g.is_dnp.astype(bool)&(g.minutes>0)]
g['game_date']=pd.to_datetime(g.game_date); g['k']=g.player.map(nm); g=g[g.game_date<asof].sort_values(['k','game_date'])
by={k:v for k,v in g.groupby('k')}
b=pd.read_excel(f'../PropORACLE_main_cp/outputs/{D}/nba/step7_ranked_props.xlsx',sheet_name='ALL')
b=b[b.prop_type.isin(mp)&b.pick_type.isin(['Standard','Demon'])&~b.is_combo_player.astype(bool)].copy() if 'is_combo_player' in b else b
b['k']=b.player.map(nm)
std=b[b.pick_type=='Standard'].drop_duplicates(['k','prop_type']).set_index(['k','prop_type']).line
rows=[]
for r in b.itertuples():
    v=by.get(r.k)
    if v is None or len(v)<20: continue
    s=v[mp[r.prop_type]].values.astype(float); L=r.line; n=len(s)
    f={}
    gaps=[];hr=[]
    for w in (5,10,15,20): gaps.append(s[-w:].mean()-L); hr.append((s[-w:]>L).mean())
    hs=(s>L).mean(); votes=sum(h>=.6 for h in hr)+(hs>=.6)+sum(x>0 for x in gaps)+((s.mean()-L)>0)
    l20=s[-20:]; sd=max(l20.std(),.5); z=(L-l20.mean())/sd; nge=int((l20>=L).sum()); mg=min(gaps)/max(L,.5)
    home=1 if str(r.team)==str(r.pp_home_team) else 0
    tag=''
    if r.pick_type=='Demon':
        A=votes>=7; B=(mg>=.0667)&(hs>=.5385); C=(z<.12)&(nge>=9)&(home==1)
        tag=''.join(x for x,c in zip('ABC',[A,B,C]) if c)
    else: tag='TB' if nge>=14 else ''
    if tag: rows.append(dict(date=D,player=r.player,team=r.team,opp=r.opp_team,prop=r.prop_type,type=r.pick_type,side='Over',line=L,votes=int(votes),gap_pct=round(mg*100,1),hit_season=round(hs,2),line_vs_L20_sd=round(z,2),cleared_L20=nge,home=home,rules=tag,rule_count=len(tag) if r.pick_type=='Demon' else 1))
o=pd.DataFrame(rows)
if o.empty: print('nothing flagged'); sys.exit()
o=o.sort_values(['type','rule_count','gap_pct'],ascending=[True,False,False])
o.to_csv(f'outputs/nba_demon_std_filter_{D}.csv',index=False)
os.makedirs('outputs/forward_test',exist_ok=True); fp='outputs/forward_test/demon_std_forward.csv'
old=pd.read_csv(fp) if os.path.exists(fp) else pd.DataFrame()
pd.concat([old[old.date!=D] if len(old) else old,o.assign(result='')]).to_csv(fp,index=False)
print(o.groupby(['type','rules']).size()); print(o[o.type=='Demon'].head(25).to_string(index=False))
