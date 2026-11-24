"""Early vs late fetch, graded from the LIVE line_history db (no manual snapshots).
python scripts/claude_fetch_ab_live.py <day>   (after the day's games are in proporacle_ref.db)
Per projection_id (one line rung): first line posted vs last line posted BEFORE game start, both graded vs the actual.
Also hit rate of every fetch snapshot. Appends to outputs/fetch_ab_live_log.csv (replaces that day) and prints pooled totals.
Goblin/Standard only (Demon skipped). Standard graded Over and Under; Goblin Over."""
import sys,sqlite3,unicodedata,pandas as pd,numpy as np
D=sys.argv[1]; LIVE='../PropORACLE_main_cp/data/line_history.db'
def nm(s): return ''.join(c for c in unicodedata.normalize('NFD',str(s)) if not unicodedata.combining(c)).lower().replace('.','').replace("'",'').replace(' jr','').replace(' iii','').replace(' ii','').strip()
M={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','3-PT Attempted':'fg3a','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','Free Throws Made':'ftm','Free Throws Attempted':'fta','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score','Personal Fouls':'pf','Offensive Rebounds':'oreb','Defensive Rebounds':'dreb'}
L=sqlite3.connect(LIVE)
H=pd.read_sql("select projection_id,player,prop_type,pick_type,line,fetched_at,start_time from line_history where sport='NBA' and game_date=? and pick_type in ('Standard','Goblin')",L,params=(D,))
H['ft']=pd.to_datetime(H.fetched_at,utc=True,errors='coerce'); H['st']=pd.to_datetime(H.start_time,utc=True,errors='coerce')
H=H[H.ft<H.st].dropna(subset=['line']).sort_values('ft')   # pre-game lines only
R=sqlite3.connect('data/cache/proporacle_ref.db'); have=[r[1] for r in R.execute('pragma table_info(nba)')]; use=[k for k in sorted(set(M.values())) if k in have]
A=pd.read_sql(f"select player,minutes,{','.join(use)} from nba where game_date='{D}'",R); A['k']=A.player.map(nm)
for k in use+['minutes']: A[k]=pd.to_numeric(A[k],errors='coerce')
A=A[A.minutes>0].groupby('k').first()
H['k']=H.player.map(nm); H['stat']=H.prop_type.map(M)
def act(k,s): return A.loc[k,s] if (k in A.index and s in A.columns) else np.nan
def hit(a,line,side):
    if pd.isna(a) or a==line: return np.nan
    return float((a>line) if side=='Over' else (a<line))
fa=H.groupby('projection_id').first(); la=H.groupby('projection_id').last()
P=fa[['player','prop_type','pick_type','k','stat','line','ft']].rename(columns={'line':'first_line','ft':'first_ft'}).join(la[['line','ft']].rename(columns={'line':'last_line','ft':'last_ft'}))
P['actual']=[act(k,s) for k,s in zip(P.k,P.stat)]
rows=[]
for r in P.itertuples():
    for side in (['Over','Under'] if r.pick_type=='Standard' else ['Over']):
        rows.append((r.Index,r.player,r.prop_type,r.pick_type,side,r.first_line,r.last_line,r.actual,hit(r.actual,r.first_line,side),hit(r.actual,r.last_line,side)))
G=pd.DataFrame(rows,columns=['pid','Player','Prop','Pick','Side','first_line','last_line','Actual','hit_first','hit_last'])
G=G[G.Actual.notna()]
G['Change']=np.where(G.first_line==G.last_line,'unchanged',np.where((G.last_line>G.first_line)==(G.Side=='Over'),'moved against','moved for'))
nfetch=H.groupby('projection_id').ft.nunique(); G['seen_once']=G.pid.map(nfetch)==1
out=[]
for (p,s),g in G.groupby(['Pick','Side']):
    for c,m in (('ALL',g.index==g.index),('unchanged',g.Change=='unchanged'),('moved for',g.Change=='moved for'),('moved against',g.Change=='moved against')):
        x=g[m].dropna(subset=['hit_first','hit_last'])
        if len(x): out.append(dict(day=D,Pick=p,Side=s,Group=c,n=len(x),first_hit=round(x.hit_first.mean()*100,2),last_hit=round(x.hit_last.mean()*100,2)))
S=pd.DataFrame(out)
# snapshot view: each fetch time, hit rate of the lines posted at that fetch
snaps=[]
for t,g in H.groupby('fetched_at'):
    g=g.copy(); g['actual']=[act(k,s) for k,s in zip(g.k,g.stat)]; g=g[g.actual.notna()]
    for side in ('Over','Under'):
        for pk in ('Goblin','Standard'):
            if pk=='Goblin' and side=='Under': continue
            x=g[g.pick_type==pk]; h=[hit(a,l,side) for a,l in zip(x.actual,x.line)]; h=[v for v in h if v==v]
            if h: snaps.append(dict(day=D,fetch=t[:16],Pick=pk,Side=side,n=len(h),hit=round(np.mean(h)*100,2)))
SN=pd.DataFrame(snaps)
f='outputs/fetch_ab_live_log.csv'
try: old=pd.read_csv(f); old=old[old.day!=D]; S2=pd.concat([old,S])
except FileNotFoundError: S2=S
S2.to_csv(f,index=False); SN.to_csv(f'outputs/fetch_ab_live_snapshots_{D}.csv',index=False)
print(S.to_string(index=False)); print(); print(SN.to_string(index=False))
print('\nPOOLED over logged days (n-weighted):')
S2['wf']=S2.first_hit*S2.n; S2['wl']=S2.last_hit*S2.n; g=S2.groupby(['Pick','Side','Group'])[['n','wf','wl']].sum(); g['first_hit']=(g.wf/g.n).round(2); g['last_hit']=(g.wl/g.n).round(2); print(g[['n','first_hit','last_hit']].to_string())
