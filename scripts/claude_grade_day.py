import pandas as pd,numpy as np,sqlite3,sys,unicodedata
D=sys.argv[1]; TAG=sys.argv[2] if len(sys.argv)>2 else ''
def nm(s): return ''.join(c for c in unicodedata.normalize('NFD',str(s)) if not unicodedata.combining(c)).lower().replace('.','').replace("'",'').replace(' jr','').replace(' iii','').replace(' ii','').strip()
M={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','3-PT Attempted':'fg3a','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','Free Throws Made':'ftm','Free Throws Attempted':'fta','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score','Personal Fouls':'pf'}
c=sqlite3.connect('data/cache/proporacle_ref.db'); out=[]
x=pd.ExcelFile(f'outputs/tiered_all_{D}{TAG}.xlsx')
gate=set()
for lg,tbl in (('NBA','nba'),('CBB','cbb')):
    cols=sorted(set(M.values())); have=[r[1] for r in c.execute(f'pragma table_info({tbl})')]; use=[k for k in cols if k in have]
    h=pd.read_sql(f"select player,minutes,{','.join(use)} from {tbl} where game_date='{D}'",c); h['k']=h.player.map(nm)
    for k in use+['minutes']: h[k]=pd.to_numeric(h[k],errors='coerce')
    h=h.groupby('k').first()
    for s in x.sheet_names:
        if not s.startswith(lg+' '): continue
        d=x.parse(s); isg='Gates' in s; t=s.replace(lg+' ','').replace('Gates ','')
        pt,sd=t.split(' ')[0],t.split(' ')[1]
        d=d.assign(league=lg,pick=pt,side=sd,gates=isg,sheet=s); out.append(d)
A=pd.concat(out,ignore_index=True)
A['k']=A.Player.map(nm); res=[]
H={'NBA':None,'CBB':None}
for lg,tbl in (('NBA','nba'),('CBB','cbb')):
    have=[r[1] for r in c.execute(f'pragma table_info({tbl})')]; use=[k for k in sorted(set(M.values())) if k in have]
    h=pd.read_sql(f"select player,minutes,{','.join(use)} from {tbl} where game_date='{D}'",c); h['k']=h.player.map(nm)
    for k in use+['minutes']: h[k]=pd.to_numeric(h[k],errors='coerce')
    H[lg]=h.groupby('k').first()
def grade(r):
    h=H[r.league]; st=M.get(r.Prop)
    if st is None or st not in h.columns or r.k not in h.index: return pd.Series([np.nan,'no result'])
    m=h.loc[r.k,'minutes']
    if pd.isna(m) or m<=0: return pd.Series([np.nan,'DNP void'])
    a=h.loc[r.k,st]
    if pd.isna(a): return pd.Series([np.nan,'no result'])
    if a==r.Line: return pd.Series([a,'push'])
    return pd.Series([a,'hit' if ((a>r.Line) if r.side=='Over' else (a<r.Line)) else 'miss'])
A[['Actual','Result']]=A.apply(grade,axis=1)
A.to_csv(f'outputs/graded_ours_{D}{TAG}.csv',index=False)
G=A[A.Result.isin(['hit','miss'])]
def summ(g,name):
    s=g.groupby(['league','pick','side']).Result.agg(n='size',hits=lambda v:(v=='hit').sum()); s['hit%']=(s.hits/s.n*100).round(2); print('\n',name); print(s.to_string())
print(A.Result.value_counts().to_dict())
summ(G[G.gates],'GATE PASS lists'); 
summ(G[G.Status.eq('PASS')&G.Tier.isin(['S','A','B'])&~G.gates],'TIER PASS S/A/B (verified)')
summ(G[~G.gates],'ALL props')
