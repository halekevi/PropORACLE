import _r2
"""Full-stat prop lists by sport. Usage: python scripts/claude_full_lists.py 2026-11-07 -> outputs/props_full_<date>.xlsx
Per player/prop: line, hit counts+% for L5/L10/L15/L20/season, averages, gaps (side-adjusted avg-line), votes(0-10), games."""
import sys,re,unicodedata,glob,pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
D=sys.argv[1] if len(sys.argv)>1 else '2026-11-07'; asof=pd.Timestamp(D)
sys.path.insert(0,'scripts'); from claude_gamelog import load_log,nm
def stats(s,L,side):
    n=len(s); r={}; hit=(s>L) if side=='Over' else (s<L); sg=1 if side=='Over' else -1
    for w in (5,10,15,20):
        k=min(w,n); r[f'L{w}']=f'{int(hit[-w:].sum())}/{k}'; r[f'L{w}%']=round(hit[-w:].mean()*100); r[f'avg{w}']=round(s[-w:].mean(),1); r[f'gap{w}']=round(sg*(s[-w:].mean()-L),1)
    r['seas%']=round(hit.mean()*100); r['avgS']=round(s.mean(),1); r['gapS']=round(sg*(s.mean()-L),1); r['games']=n
    r['votes']=int(sum(r[f'L{w}%']>=60 for w in (5,10,15,20))+(r['seas%']>=60)+sum(r[f'gap{w}']>0 for w in (5,10,15,20))+(r['gapS']>0))
    r['zSD']=round(float(min(r['gap5'],r['gap10'],r['gap20'])/max(float(np.std(s[-20:])),0.3)),2)
    r['cv20']=round(float(np.std(s[-20:])/max(abs(np.mean(s[-20:])),.5)),2)
    r['worst10']=round(float((s[-10:].min()-L) if side=='Over' else (L-s[-10:].max())),1)
    r['min_gap%']=round(min(r[f'gap{w}'] for w in (5,10,15,20))/max(L,.5)*100,1); return r
def build(rows,series_fn,sides_fn):
    out=[]
    for r in rows:
        s=series_fn(r)
        if s is None or len(s)<5: continue
        for side in sides_fn(r):
            d=dict(Player=r['player'],Team=r['team'],Opp=r['opp'],Prop=r['prop'],Type=r['pt'],Side=side,Line=r['line']); d.update(stats(s,r['line'],side)); out.append(d)
    return pd.DataFrame(out)
# ---------- NBA
g,_=load_log(); g=g[g.game_date<asof]; by={k:v for k,v in g.groupby('k')}
mp={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','3-PT Attempted':'fg3a','Free Throws Made':'ftm','Free Throws Attempted':'fta','Personal Fouls':'pf','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score','Defensive Rebounds':'dreb','Offensive Rebounds':'oreb'}
import os
_s7=f'../PropORACLE_main_cp/outputs/{D}/nba/step7_ranked_props.xlsx'
b=pd.read_excel(_s7,sheet_name='ALL') if os.path.exists(_s7) else pd.read_csv(f'../PropORACLE_main_cp/outputs/{D}/nba/step6a_with_opp_stats.csv'); b=b[b.prop_type.isin(mp)&b.pick_type.isin(['Standard','Goblin','Demon'])&~b.is_combo_player.astype(bool)]
rows=[dict(player=r.player,team=r.team,opp=r.opp_team,prop=r.prop_type,pt=r.pick_type,line=r.line) for r in b.itertuples()]
nb=build(rows,lambda r:(by[nm(r['player'])][mp[r['prop']]].values.astype(float) if nm(r['player']) in by else None),lambda r:['Over','Under'] if r['pt']=='Standard' else ['Over'])
if not os.path.exists(f'../PropORACLE_main_cp/outputs/{D}/cfb/step8_cfb_direction_clean_{D}.xlsx'):
    nb.to_pickle('/tmp/full_nba.pkl')
    with pd.ExcelWriter(f'outputs/props_full_{D}.xlsx') as x:
        for t_ in ('Standard','Goblin','Demon'):
            z=nb[nb.Type==t_]
            if len(z): z.sort_values(['votes','zSD'],ascending=False).to_excel(x,f'NBA {t_}',index=False)
    print('NBA only, rows',len(nb)); sys.exit(0)
# ---------- CFB
c=pd.read_parquet('data/game_logs/cfb_player_games.parquet'); c['game_date']=pd.to_datetime(c.game_date)
p=pd.read_csv('data/game_logs/cfb_patch_claude_20261031.csv'); p['game_date']=pd.to_datetime(p.game_date.astype(str),format='%Y%m%d'); p=p.rename(columns={'rec':'rec','rec_yds':'rec_yds'})
c['k']=c.player.map(nm); p['k']=p.player.map(nm)
key=lambda d:set(zip(d.k,d.game_date)); have=key(c); p=p[[(k,d) not in have for k,d in zip(p.k,p.game_date)]]
cols=['pass_att','pass_yds','pass_td','rush_att','rush_yds','rush_td','rec','rec_yds','rec_td']
cc=pd.concat([c[['k','game_date']+[x for x in cols if x in c]],p[['k','game_date']+cols]],ignore_index=True).sort_values(['k','game_date']); cc=cc[cc.game_date<asof]
cc['pass_rush']=cc.pass_yds.fillna(0)+cc.rush_yds.fillna(0); cc['rush_rec']=cc.rush_yds.fillna(0)+cc.rec_yds.fillna(0)
cmp={'Rush Yards':'rush_yds','Receiving Yards':'rec_yds','Pass Yards':'pass_yds','Pass+Rush Yds':'pass_rush','Rush+Rec Yds':'rush_rec','Receptions':'rec','Pass TDs':'pass_td','Rush TDs':'rush_td','Rec TDs':'rec_td','Rush Attempts':'rush_att','Pass Attempts':'pass_att'}
cby={k:v for k,v in cc.groupby('k')}
a=pd.read_excel(f'../PropORACLE_main_cp/outputs/{D}/cfb/step8_cfb_direction_clean_{D}.xlsx',sheet_name='ALL'); a=a[a.Prop.isin(cmp)]
rows=[dict(player=r.Player,team=r.Team,opp=r.Opp,prop=r.Prop,pt=r._asdict()['_9'] if False else getattr(r,'_9'),line=r.Line) for r in a.itertuples()] if False else [dict(player=x['Player'],team=x['Team'],opp=x['Opp'],prop=x['Prop'],pt=x['Pick Type'],line=x['Line']) for _,x in a.iterrows()]
cf=build(rows,lambda r:(cby[nm(r['player'])][cmp[r['prop']]].values.astype(float) if nm(r['player']) in cby else None),lambda r:['Over','Under'] if r['pt']=='Standard' else ['Over'])
def order(d): return d.sort_values(['votes','zSD'],ascending=False)
with pd.ExcelWriter(f'outputs/props_full_{D}.xlsx') as x:
    for nmx,d in (('NBA',nb),('CFB',cf)):
        for t in ('Standard','Goblin','Demon'):
            z=d[d.Type==t]
            if len(z): order(z).to_excel(x,f'{nmx} {t}',index=False)
print('NBA rows',len(nb),'CFB rows',len(cf)); nb.to_pickle('/tmp/full_nba.pkl'); cf.to_pickle('/tmp/full_cfb.pkl')
