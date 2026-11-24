"""Does 'every game this season (or every L10 game) is over/under the line' hit? Walk-forward: only games BEFORE the prop day.
python scripts/claude_all_games_over.py  -> prints results; outputs/all_games_today_<day>.xlsx if day given as argv[1]"""
import pandas as pd,numpy as np,sqlite3,sys,unicodedata
def nm(s): return ''.join(c for c in unicodedata.normalize('NFD',str(s)) if not unicodedata.combining(c)).lower().replace('.','').replace("'",'').replace(' jr','').replace(' iii','').replace(' ii','').strip()
M={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','3-PT Attempted':'fg3a','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','Free Throws Made':'ftm','Free Throws Attempted':'fta','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score','Personal Fouls':'pf'}
c=sqlite3.connect('data/cache/proporacle_ref.db'); have=[r[1] for r in c.execute('pragma table_info(nba)')]; use=[k for k in sorted(set(M.values())) if k in have]
A=pd.read_sql(f"select player,game_date,minutes,{','.join(use)} from nba where game_date>='2026-10-01'",c); A['k']=A.player.map(nm)
for k in use+['minutes']: A[k]=pd.to_numeric(A[k],errors='coerce')
A=A[A.minutes>0].sort_values('game_date'); H={k:v for k,v in A.groupby('k')}
def flags(k,stat,line,day,side):
    v=H.get(k)
    if v is None: return None
    s=v[v.game_date<day][stat].dropna().values
    if len(s)<10: return None
    ok=(lambda x:(x>line).all()) if side=='Over' else (lambda x:(x<line).all())
    return len(s),bool(ok(s)),bool(ok(s[-10:]))
if len(sys.argv)>1:
    D=sys.argv[1]; B=pd.read_csv(f'../PropORACLE_main_cp/outputs/{D}/nba/step1_pp_props_today.csv',encoding='utf-8-sig'); B=B[B.pick_type.isin(['Standard','Goblin'])]
    rows=[]
    for r in B.itertuples():
        st=M.get(r.prop_type)
        if st is None: continue
        for side in (['Over','Under'] if r.pick_type=='Standard' else ['Over']):
            f=flags(nm(r.player),st,r.line,D,side)
            if f and (f[1] or f[2]): rows.append((r.pick_type,r.prop_type,side,r.player,r.team,r.line,f[0],'ALL season' if f[1] else 'L10 only'))
    T=pd.DataFrame(rows,columns=['Pick','Prop','Side','Player','Team','Line','Games','AllOver/Under']); T.to_excel(f'outputs/all_games_today_{D}.xlsx',index=False); print(T.groupby(['Pick','AllOver/Under']).size()); sys.exit()
G=pd.read_csv('outputs/season_graded_rows_1perday.csv'); out=[]
for r in G.itertuples():
    st=M.get(r.Prop)
    if st is None: continue
    f=flags(nm(r.Player),st,r.Line,r.day,r.Side)
    if f: out.append((r.Pick,r.Side,f[0],f[1],f[2],r.Hit))
R=pd.DataFrame(out,columns=['Pick','Side','n','AllSeason','AllL10','Hit'])
for p in ('Goblin','Standard'):
    g=R[R.Pick==p]; print(f'\n{p}: all rows n={len(g)} hit {g.Hit.mean()*100:.2f}%')
    for lab,m in (('Every game this season beyond line',g.AllSeason),('Every L10 game beyond line',g.AllL10),('L10 yes but season no',g.AllL10&~g.AllSeason),('Neither',~g.AllL10)):
        s=g[m]; print(f'  {lab:38s} n={len(s):5d} hit {s.Hit.mean()*100:.2f}%' if len(s) else f'  {lab}: none')
