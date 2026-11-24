import _r2
import pandas as pd,numpy as np,sys,sqlite3,warnings; warnings.filterwarnings('ignore'); sys.path.insert(0,'scripts')
from claude_defense_rel import table; from claude_gamelog import nm
day=sys.argv[1]; T=table(); AB={'SAS':'SA','GSW':'GS','NOP':'NO','NYK':'NY','UTA':'UTAH','WAS':'WSH','PHO':'PHX','BRK':'BKN'}
MP={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','3-PT Attempted':'fg3a','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','Free Throws Made':'ftm','Free Throws Attempted':'fta','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Offensive Rebounds':'oreb','Defensive Rebounds':'dreb','Fantasy Score':'fantasy_score','Personal Fouls':'pf'}
d=pd.read_pickle('/tmp/full_nba.pkl'); d['col']=d.Prop.map(MP); d=d[d.col.notna()&~d.Player.astype(str).str.contains(r' \+ |/',regex=True)].copy()
for c in ['L5%','L10%','L20%','seas%','zSD','games','Line']: d[c]=pd.to_numeric(d[c],errors='coerce')
d['opp1']=d.Opp.astype(str).str.upper().map(lambda x:AB.get(x,x)); d['rel']=[T.loc[o,c] if o in T.index and c in T.columns else np.nan for o,c in zip(d.opp1,d.col)]
d['Defense']=np.where(d.rel>1.05,'soft',np.where(d.rel<1.0,'tough','mid')); d['Defense (rel)']=d.rel.round(2)
S=d[d.Type=='Standard'].copy(); dup=S.groupby(['Player','col']).Line.nunique(); dk=set(dup[dup>1].index); S=S[[(p,c) not in dk for p,c in zip(S.Player,S.col)]&(S.Line>1.0)]
cols=['Player','Team','Opp','Prop','Side','Line','L5','avg5','gap5','L10','avg10','gap10','L20','avg20','gap20','avgS','gapS','seas%','games','zSD','Defense','Defense (rel)']
ren={'avg5':'L5 avg','gap5':'L5 gap','avg10':'L10 avg','gap10':'L10 gap','avg20':'L20 avg','gap20':'L20 gap','avgS':'Season avg','gapS':'Season gap','zSD':'Gap in SD (min)'}
b=lambda x:(x['L10%']>=70)&(x['L20%']>=60)
sheets={'NBA Goblin Over':d[(d.Type=='Goblin')&(d.Side=='Over')&(d['L5%']>=100)&(d['L10%']>=90)&(d['L20%']>=80)&(d.zSD>=1.0)&(d.games>=20)].sort_values('zSD',ascending=False),
 'NBA Standard Under':S[(S.Side=='Under')&b(S)&(S.zSD>=.5)].sort_values('zSD',ascending=False),'NBA Standard Over':S[(S.Side=='Over')&b(S)&(S.zSD>=.5)].sort_values('zSD',ascending=False)}
sheets={k:v[cols].rename(columns=ren) for k,v in sheets.items()}
# CBB
g1=pd.read_parquet('data/game_logs/cbb_player_games.parquet'); g1['gd']=pd.to_datetime(g1.game_date); g2=pd.read_sql("select * from cbb",sqlite3.connect('data/cache/proporacle_ref.db')); g2['gd']=pd.to_datetime(g2.game_date)
C=['pts','reb','ast','stl','blk','tov','fg3m','minutes']; g=pd.concat([g1[['player','gd']+C],g2[['player','gd']+C]],ignore_index=True)
for c in C: g[c]=pd.to_numeric(g[c],errors='coerce')
g=g[g.minutes>0].drop_duplicates(['player','gd']); g['k']=g.player.map(nm); g['pra']=g.pts+g.reb+g.ast; g['pr']=g.pts+g.reb; g['pa']=g.pts+g.ast; g['ra']=g.reb+g.ast; g['bs']=g.blk+g.stl
CM={'points':'pts','rebounds':'reb','assists':'ast','pts+rebs+asts':'pra','pts+rebs':'pr','pts+asts':'pa','rebs+asts':'ra','3-pt made':'fg3m','steals':'stl','blocked shots':'blk','turnovers':'tov','blks+stls':'bs'}
ser={k:v.sort_values('gd') for k,v in g.groupby('k')}; s=pd.read_csv(f'../PropORACLE_main_cp/outputs/{day}/cbb/step1_cbb.csv'); s=s[s.duration.eq('Full')|s.duration.isna()]; s['stat']=s.stat_type.str.lower().map(CM); s=s[s.stat.notna()]
dup=s[s.odds_type=='standard'].groupby(['player','stat']).line.nunique(); dk2=set(dup[dup>1].index); rows=[]
for r in s.drop_duplicates(['player','stat','line','odds_type']).itertuples():
    if r.odds_type=='demon': continue
    if r.odds_type=='standard' and ((r.player,r.stat) in dk2 or r.line<=1.0): continue
    sr=ser.get(nm(r.player)); 
    if sr is None: continue
    h=sr[sr.gd<pd.Timestamp(day)]
    if len(h)<8 or (h.gd>=pd.Timestamp('2026-10-01')).sum()<2: continue
    v=h[r.stat].values.astype(float); sd=max(float(np.std(v[-20:])),.3)
    for side,sg in (('Over',1),('Under',-1)):
        if r.odds_type=='goblin' and side=='Under': continue
        hit=(v>r.line) if sg==1 else (v<r.line); a={w:v[-w:].mean() for w in (5,10,20)}; gp={w:sg*(a[w]-r.line) for w in a}
        rows.append({'Player':r.player,'Team':r.team,'Opp':r.pp_opp_team,'Prop':r.stat_type,'Type':r.odds_type.capitalize(),'Side':side,'Line':r.line,'L5':f'{int(hit[-5:].sum())}/{len(hit[-5:])}','L5 avg':round(a[5],1),'L5 gap':round(gp[5],1),'L10':f'{int(hit[-10:].sum())}/{len(hit[-10:])}','L10 avg':round(a[10],1),'L10 gap':round(gp[10],1),'L20':f'{int(hit[-20:].sum())}/{len(hit[-20:])}','L20 avg':round(a[20],1),'L20 gap':round(gp[20],1),'Gap in SD (min)':round(min(gp.values())/sd,2),'games':len(v),'_h5':hit[-5:].mean(),'_h10':hit[-10:].mean(),'_h20':hit[-20:].mean()})
C2=pd.DataFrame(rows)
gb=C2[(C2.Type=='Goblin')&(C2._h5>=1)&(C2._h10>=.9)&(C2._h20>=.8)].sort_values('Gap in SD (min)',ascending=False)
su=C2[(C2.Type=='Standard')&(C2.Side=='Under')&(C2._h10>=.7)&(C2._h20>=.6)&(C2['Gap in SD (min)']>=.5)].sort_values('Gap in SD (min)',ascending=False)
so=C2[(C2.Type=='Standard')&(C2.Side=='Over')&(C2._h10>=.7)&(C2._h20>=.6)&(C2['Gap in SD (min)']>=.5)].sort_values('Gap in SD (min)',ascending=False)
dr=['_h5','_h10','_h20','Type']; sheets.update({'CBB Goblin Over':gb.drop(columns=dr),'CBB Standard Under':su.drop(columns=dr),'CBB Standard Over':so.drop(columns=dr)})
note=pd.DataFrame({'Notes':['Gaps are side-adjusted: positive = the average is on the favorable side of the line for that pick; negative = against.','Gap in SD (min) = the smallest of the L5/L10/L20 gaps divided by the player\'s spread (SD of last 20); 0.5+ is the Standard gate, 1.0+ is a strong gap.','NBA Goblin: L5 5/5, L10 90%+, L20 80%+, gap 1 SD+. Standard sheets: L10 70%+, L20 60%+, gap 0.5 SD+, single-line Standards only, line above 1.0.','Standard Overs are watch-only (no edge found in testing). Defense = opponent allowed vs league for that stat (soft = easier, tough = harder); NBA only.','CBB history is mostly last season for early-season players; needs 8+ games and 2+ this season. Check injuries and lineups before building; this uses the early-morning board.']})
with pd.ExcelWriter(f'outputs/best_props_{day}.xlsx') as w:
    note.to_excel(w,sheet_name='Read me',index=False)
    for k,v in sheets.items(): v.to_excel(w,sheet_name=k,index=False)
    for nm_,ws in w.sheets.items(): ws.set_column(0,25,15); ws.set_column(0,0,34 if nm_=='Read me' else 22)
print({k:len(v) for k,v in sheets.items()})
