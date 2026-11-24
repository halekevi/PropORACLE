import pandas as pd,numpy as np,re,unicodedata,warnings; warnings.filterwarnings('ignore')
_u=lambda s:''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c))
nm=lambda s:re.sub(r'\b(jr|sr|ii|iii|iv)\b','',re.sub(r'[^a-z ]','',_u(s).lower().replace('.','').replace("'",''))).strip()
g=pd.read_parquet('data/game_logs/nba_player_games.parquet')
g=g[g.phase.astype(str).str.upper()!='PRESEASON']; g=g[~g.is_dnp.astype(bool)&(g.minutes>0)]
g['game_date']=pd.to_datetime(g.game_date); g['k']=g.player.map(nm); g=g.sort_values(['k','game_date'])
mp={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','3-PT Attempted':'fg3a','Free Throws Made':'ftm','Free Throws Attempted':'fta','Personal Fouls':'pf','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a'}
cols=sorted(set(mp.values()))
tg=g.groupby(['game_date','team','opponent'])[cols].sum().reset_index()
al=tg.rename(columns={'team':'o','opponent':'team'}); al=al[['game_date','team']+cols].rename(columns={c:c+'_al' for c in cols})
tg=tg.merge(al,on=['game_date','team'],how='left').sort_values('game_date')
T={t:v.reset_index(drop=True) for t,v in tg.groupby('team')}
d=pd.read_csv('../PropORACLE_main_cp/data/training/graded_export_20260516.csv',low_memory=False,usecols=['file_date','sport','player','prop','line','pick_type','direction','result_binary'])
d=d[(d.sport=='NBA')&(d.pick_type=='Standard')&d.prop.isin(mp)&d.result_binary.isin([0,1])].copy()
d['k']=d.player.map(nm); d['fd']=pd.to_datetime(d.file_date)
by={k:v for k,v in g.groupby('k')}
rows=[]
for r in d.itertuples():
    v=by.get(r.k)
    if v is None: continue
    past=v[v.game_date<r.fd]
    if len(past)<20: continue
    today=v[v.game_date==r.fd]
    if len(today)==0: continue
    me=today.iloc[0]; c=mp[r.prop]; s=past[c].values.astype(float); sg=1 if r.direction=='OVER' else -1; L=r.line
    hit=(s>L) if sg==1 else (s<L)
    f=dict(y=r.result_binary,side=sg,prop=r.prop,line=L,fd=r.fd)
    for w in (5,10,15,20): f[f'h{w}']=hit[-w:].mean(); f[f'a{w}']=sg*(s[-w:].mean()-L)
    f['hs']=hit.mean(); f['as']=sg*(s.mean()-L)
    o=T.get(me.opponent)
    if o is not None:
        oo=o[o.game_date<r.fd].tail(10)
        if len(oo)>=6: f['opp_allow']=oo[c+'_al'].mean(); f['opp_allow_all']=T[me.opponent][T[me.opponent].game_date<r.fd][c+'_al'].mean()
    f['total']=me.game_total; f['spread']=me.spread; f['rest']=me.rest_days; f['b2b']=me.back_to_back; f['home']=1 if str(me.home_away).lower().startswith('h') else 0
    m=past.minutes.values; f['min5']=m[-5:].mean(); f['min20']=m[-20:].mean()
    rows.append(f)
t=pd.DataFrame(rows); t.to_pickle('/tmp/ctx.pkl'); print(len(t)); print(t[['opp_allow','total','spread','rest','b2b','home']].notna().mean().round(2).to_dict())
