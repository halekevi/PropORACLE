"""Season-to-date (current NBA season) hit rate per player per prop category, from every saved daily board vs actual box scores.
python scripts/claude_season_consistency.py   -> outputs/season_consistency_<lastday>.xlsx (+ graded rows csv)"""
import pandas as pd,numpy as np,sqlite3,glob,os,unicodedata
def nm(s): return ''.join(c for c in unicodedata.normalize('NFD',str(s)) if not unicodedata.combining(c)).lower().replace('.','').replace("'",'').replace(' jr','').replace(' iii','').replace(' ii','').strip()
M={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','3-PT Attempted':'fg3a','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','Free Throws Made':'ftm','Free Throws Attempted':'fta','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score','Personal Fouls':'pf','Offensive Rebounds':'oreb','Defensive Rebounds':'dreb'}
c=sqlite3.connect('data/cache/proporacle_ref.db'); have=[r[1] for r in c.execute('pragma table_info(nba)')]; use=[k for k in sorted(set(M.values())) if k in have]
A=pd.read_sql(f"select player,game_date,minutes,{','.join(use)} from nba where game_date>='2026-10-01'",c); A['k']=A.player.map(nm)
for k in use+['minutes']: A[k]=pd.to_numeric(A[k],errors='coerce')
A=A.groupby(['game_date','k']).first()
rows=[]
for f in sorted(glob.glob('../PropORACLE_main_cp/outputs/2026-1[01]-*/nba/step1_pp_props_today.csv')):
    D=f.split('outputs/')[1][:10]
    if (D,) not in [(x,) for x in A.index.get_level_values(0).unique()]: continue
    d=pd.read_csv(f,encoding='utf-8-sig'); d=d[d.pick_type.isin(['Standard','Goblin'])]
    d=d[pd.to_datetime(d.start_time,utc=True,errors='coerce').dt.tz_convert('America/New_York').dt.strftime('%Y-%m-%d')==D]
    d=d.drop_duplicates(['player','prop_type','pick_type','line']); d['k']=d.player.map(nm); d['day']=D; rows.append(d[['day','player','k','team','prop_type','pick_type','line']])
B=pd.concat(rows); out=[]
for r in B.itertuples():
    st=M.get(r.prop_type)
    if st is None or st not in A.columns or (r.day,r.k) not in A.index: continue
    h=A.loc[(r.day,r.k)]
    if pd.isna(h.minutes) or h.minutes<=0 or pd.isna(h[st]): continue
    a=h[st]
    for side in (['Over','Under'] if r.pick_type=='Standard' else ['Over']):
        if a==r.line: continue
        out.append((r.day,r.player,r.team,r.prop_type,r.pick_type,side,r.line,a,int((a>r.line) if side=='Over' else (a<r.line))))
G=pd.DataFrame(out,columns=['day','Player','Team','Prop','Pick','Side','Line','Actual','Hit'])
G.to_csv('outputs/season_graded_rows.csv',index=False)
S=G.groupby(['Pick','Prop','Side','Player']).agg(Team=('Team','last'),Games=('Hit','size'),Hits=('Hit','sum'),AvgLine=('Line','mean'),AvgActual=('Actual','mean')).reset_index()
S['Hit%']=(S.Hits/S.Games*100).round(2); S.round(2).to_csv('outputs/season_consistency.csv',index=False)
print(G.day.min(),G.day.max(),G.day.nunique(),'days',len(G),'graded rows'); print(G.groupby(['Pick','Side']).Hit.agg(['size','mean']).round(4))
