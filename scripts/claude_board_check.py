import _r2
"""Board freshness + line-move check + best-props lists. Usage: python3 scripts/claude_board_check.py YYYY-MM-DD
Archives the current NBA/CBB step1 boards (time-stamped), rebuilds the best-props lists from the CURRENT board, and marks every pick of the day's FIRST board as SAME / LINE MOVED / NOW DEMON/GOBLIN ONLY / GONE.
Writes outputs/best_props_<day>_<HHMM>.xlsx. Gaps are side-adjusted (positive = favorable to the pick)."""
import pandas as pd,numpy as np,sys,os,glob,shutil,sqlite3,datetime,warnings; warnings.filterwarnings('ignore'); sys.path.insert(0,'scripts')
from claude_gamelog import nm
from claude_defense_rel import table
day=sys.argv[1]; B=f'../PropORACLE_main_cp/outputs/{day}'; SN=f'outputs/board_snapshots/{day}'; os.makedirs(SN,exist_ok=True)
ET=datetime.timezone(datetime.timedelta(hours=-5)); now=datetime.datetime.now(ET)
def snap(src,tag):
    if not os.path.exists(src): return None
    d=pd.read_csv(src,encoding='utf-8-sig'); fa=pd.to_datetime(d.fetched_at,utc=True,errors='coerce').max().tz_convert('America/New_York'); dst=f'{SN}/{tag}_{fa:%H%M}.csv'
    if not os.path.exists(dst): shutil.copy(src,dst)
    return d,fa
nb=snap(f'{B}/nba/step1_pp_props_today.csv','nba'); cb=snap(f'{B}/cbb/step1_cbb.csv','cbb')
stamp={'NBA':nb[1] if nb else None,'CBB':cb[1] if cb else None}
print({k:(f'{v:%b %d %H:%M} ET, {(now-v).total_seconds()/3600:.1f}h old' if v is not None else 'no board') for k,v in stamp.items()})
MPN={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','3-PT Attempted':'fg3a','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','Free Throws Made':'ftm','Free Throws Attempted':'fta','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Offensive Rebounds':'oreb','Defensive Rebounds':'dreb','Fantasy Score':'fantasy_score','Personal Fouls':'pf'}
MPC={k.lower():v for k,v in MPN.items() if v in('pts','reb','ast','fg3m','stl','blk','tov','pra','pr','pa','ra','bs')}
DB=sqlite3.connect('data/cache/proporacle_ref.db')
def hist(table_,cols,extra=None):
    h=pd.read_sql(f"select player,game_date,minutes,{','.join(cols)} from {table_}"+(" where coalesce(season_window,'')!='preseason'" if table_=='nba' else ''),DB)
    if extra is not None: h=pd.concat([h,extra],ignore_index=True)
    for c in cols+['minutes']: h[c]=pd.to_numeric(h[c],errors='coerce')
    h=h[h.minutes>0].drop_duplicates(['player','game_date']); h['gd']=pd.to_datetime(h.game_date); h['k']=h.player.map(nm); return {k:v.sort_values('gd') for k,v in h.groupby('k')}
NC=sorted(set(MPN.values())); NS=hist('nba',NC) if nb else {}
g1=pd.read_parquet('data/game_logs/cbb_player_games.parquet')[['player','game_date','minutes','pts','reb','ast','stl','blk','tov','fg3m']]
CC=['pts','reb','ast','stl','blk','tov','fg3m']; CS=hist('cbb',CC,g1) if cb else {}
for s in CS.values():
    s['pra']=s.pts+s.reb+s.ast; s['pr']=s.pts+s.reb; s['pa']=s.pts+s.ast; s['ra']=s.reb+s.ast; s['bs']=s.blk+s.stl
def feat(v,line,sg):
    hit=(v>line) if sg==1 else (v<line); sd=max(float(np.std(v[-20:])),.3); a={w:v[-w:].mean() for w in (5,10,20)}; gp={w:sg*(a[w]-line) for w in a}
    return dict(h5=hit[-5:].mean(),h10=hit[-10:].mean(),h20=hit[-20:].mean(),hs=hit.mean(),L5=f'{int(hit[-5:].sum())}/{len(hit[-5:])}',L10=f'{int(hit[-10:].sum())}/{len(hit[-10:])}',L20=f'{int(hit[-20:].sum())}/{len(hit[-20:])}',
      a5=round(a[5],1),g5=round(gp[5],1),a10=round(a[10],1),g10=round(gp[10],1),a20=round(a[20],1),g20=round(gp[20],1),aS=round(v.mean(),1),gS=round(sg*(v.mean()-line),1),z=round(min(gp.values())/sd,2),games=len(v))
T=table(); AB={'SAS':'SA','GSW':'GS','NOP':'NO','NYK':'NY','UTA':'UTAH','WAS':'WSH','PHO':'PHX','BRK':'BKN'}
OPP={}
f6=f'{B}/nba/step6a_with_opp_stats.csv'
if os.path.exists(f6): a=pd.read_csv(f6,usecols=['player','opp_team']).dropna().drop_duplicates('player'); OPP=dict(zip(a.player,a.opp_team))
def picks(d,sport,asof):
    rows=[]; d=d.copy(); d=d[~d.player.astype(str).str.contains(r' \+ |/',regex=True)]
    if sport=='NBA': d['stat']=d.prop_type.map(MPN); d['ptype']=d.pick_type.str.lower(); d['pname']=d.prop_type; ser=NS; d['opp']=d.player.map(OPP)
    else:
        d=d[d.duration.eq('Full')|d.duration.isna()]; d['stat']=d.stat_type.str.lower().map(MPC); d['ptype']=d.odds_type.str.lower(); d['pname']=d.stat_type; ser=CS; d['opp']=d.pp_opp_team
    d=d[d.stat.notna()&d.ptype.isin(['goblin','standard'])]
    dup=d[d.ptype=='standard'].groupby(['player','stat']).line.nunique(); dk=set(dup[dup>1].index)
    for r in d.drop_duplicates(['player','stat','line','ptype']).itertuples():
        if r.ptype=='standard' and ((r.player,r.stat) in dk or r.line<=1.0): continue
        s=ser.get(nm(r.player)); 
        if s is None: continue
        h=s[s.gd<pd.Timestamp(day)]
        if len(h)<8 or (sport=='CBB' and (h.gd>=pd.Timestamp('2026-10-01')).sum()<2): continue
        v=h[r.stat].values.astype(float)
        for side,sg in (('Over',1),('Under',-1)):
            if r.ptype=='goblin' and side=='Under': continue
            f=feat(v,r.line,sg); rel=np.nan
            if sport=='NBA':
                o=AB.get(str(r.opp).upper(),str(r.opp).upper()); rel=T.loc[o,r.stat] if o in T.index and r.stat in T.columns else np.nan
            ok=False; grp=None
            if r.ptype=='goblin' and side=='Over' and f['h5']>=1 and f['h10']>=.9 and f['h20']>=.8 and f['z']>=1 and (sport=='CBB' or f['games']>=20): ok=True; grp='Goblin Over'
            if r.ptype=='standard' and f['h10']>=.7 and f['h20']>=.6 and f['z']>=.5: ok=True; grp=f'Standard {side}'
            if ok: rows.append({'sport':sport,'group':grp,'Player':r.player,'Team':r.team,'Opp':r.opp,'Prop':r.pname,'stat':r.stat,'Side':side,'ptype':r.ptype,'Line':r.line,'L5':f['L5'],'L5 avg':f['a5'],'L5 gap':f['g5'],'L10':f['L10'],'L10 avg':f['a10'],'L10 gap':f['g10'],'L20':f['L20'],'L20 avg':f['a20'],'L20 gap':f['g20'],'Season avg':f['aS'],'Season gap':f['gS'],'Season hit%':round(f['hs']*100),'Games':f['games'],'Gap in SD (min)':f['z'],'Defense':('' if np.isnan(rel) else 'soft' if rel>1.05 else 'tough' if rel<1.0 else 'mid'),'Board as of':f'{asof:%b %d %H:%M} ET'})
    return pd.DataFrame(rows)
cur={}
if nb: cur['NBA']=picks(nb[0],'NBA',nb[1])
if cb: cur['CBB']=picks(cb[0],'CBB',cb[1])
# first board of the day -> compare
def first_snap(tag):
    fs=sorted(glob.glob(f'{SN}/{tag}_*.csv')); return fs[0] if fs else None
def status(first,cur_board,sport):
    if first is None or not len(first): return {}
    out={}
    cur_board=cur_board.copy(); cur_board['pt']=(cur_board.pick_type if sport=='NBA' else cur_board.odds_type).str.lower(); pn='prop_type' if sport=='NBA' else 'stat_type'
    for r in first.itertuples():
        x=cur_board[(cur_board.player==r.Player)&(cur_board[pn]==r.Prop)]
        key=(r.Player,r.Prop,r.Side)
        if not len(x): out[key]=('GONE (player/prop off board)','')
        else:
            same=x[(x.pt==r.ptype)]
            if not len(same): out[key]=(f'NOW {"/".join(sorted(set(x.pt.str.upper())))} ONLY (no {r.ptype})',', '.join(f'{p} {l}' for p,l in zip(x.pt,x.line)))
            elif (same.line==r.Line).any(): out[key]=('SAME','')
            else: out[key]=('LINE MOVED',f'{r.Line} -> '+', '.join(str(l) for l in same.line))
    return out
res={}
for sport,tag,bd in [('NBA','nba',nb),('CBB','cbb',cb)]:
    fs=first_snap(tag)
    if not bd: continue
    if fs and os.path.basename(fs)!=os.path.basename(f'{SN}/{tag}_{bd[1]:%H%M}.csv'):
        fb=pd.read_csv(fs,encoding='utf-8-sig'); ffa=pd.to_datetime(fb.fetched_at,utc=True,errors='coerce').max().tz_convert('America/New_York'); fp=picks(fb,sport,ffa); res[sport]=(fp,status(fp,bd[0],sport),ffa)
rows_out={}
for sport,df in cur.items():
    df=df.copy(); st=res.get(sport,(None,{},None))[1]
    df['Status vs first board']=[st.get((p,pr,s),('NEW since first board' if res.get(sport) else 'first board',''))[0] for p,pr,s in zip(df.Player,df.Prop,df.Side)]
    for g,x in df.groupby('group'): rows_out[f'{sport} {g}']=x.sort_values('Gap in SD (min)',ascending=False).drop(columns=['sport','group','stat','ptype'])
mv=[]
for sport,(fp,st,ffa) in res.items():
    for r in fp.itertuples():
        s_,note=st.get((r.Player,r.Prop,r.Side),('',''))
        if s_ and s_!='SAME': mv.append({'Sport':sport,'Group':r.group,'Player':r.Player,'Prop':r.Prop,'Side':r.Side,'First-board line':r.Line,'First board':f'{ffa:%H:%M} ET','What happened':s_,'Now':note})
tab=int(not os.path.exists('x'))
HH=max([v for v in stamp.values() if v is not None],default=now); out=f'outputs/best_props_{day}_{HH:%H%M}.xlsx'
rd=pd.DataFrame({'Notes':[f'Board as of: '+'; '.join(f'{k} {v:%b %d %H:%M} ET ({(now-v).total_seconds()/3600:.1f}h old when built)' for k,v in stamp.items() if v is not None),
 'Status vs first board: compares each pick from the first board of the day with the CURRENT board (SAME / LINE MOVED / NOW DEMON-or-GOBLIN ONLY / GONE).',
 'Gaps are side-adjusted (positive = favorable to the pick). Gap in SD (min) = smallest of L5/L10/L20 gaps / player spread; 0.5+ passes Standard gate, 1.0+ strong.',
 'Goblin: L5 5/5, L10 90%+, L20 80%+, gap 1 SD+. Standard: L10 70%+, L20 60%+, gap 0.5 SD+, single-line Standards, line above 1.0. Standard Overs are watch-only.',
 'Lines move: re-check the line on PrizePicks right before building. CBB history is mostly last season for early-season players (8+ games, 2+ this season).']})
with pd.ExcelWriter(out) as w:
    rd.to_excel(w,sheet_name='Read me',index=False)
    pd.DataFrame(mv).to_excel(w,sheet_name='Moves since first board',index=False) if mv else pd.DataFrame({'Moves':['none yet (only one board snapshot so far)' if not res else 'no moved picks']}).to_excel(w,sheet_name='Moves since first board',index=False)
    for k,v in rows_out.items(): v.to_excel(w,sheet_name=k[:31],index=False)
    for n_,ws in w.sheets.items(): ws.set_column(0,30,15); ws.set_column(0,0,34 if n_ in('Read me',) else 22)
print(out,{k:len(v) for k,v in rows_out.items()},'moves:',len(mv))
