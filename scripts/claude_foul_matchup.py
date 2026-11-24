import pandas as pd,numpy as np,re,unicodedata,warnings; warnings.filterwarnings('ignore')
_u=lambda s:''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c))
nm=lambda s:re.sub(r'\b(jr|sr|ii|iii|iv)\b','',re.sub(r'[^a-z ]','',_u(s).lower().replace('.','').replace("'",''))).strip()
g=pd.read_parquet('data/game_logs/nba_player_games.parquet')
g=g[g.phase.astype(str).str.upper()!='PRESEASON']; g=g[~g.is_dnp.astype(bool)&(g.minutes>0)]
g['game_date']=pd.to_datetime(g.game_date); g['k']=g.player.map(nm); g=g.sort_values(['k','game_date'])
print(g[['team','opponent']].head(3).to_string())
tg=g.groupby(['game_date','team','opponent']).agg(fta=('fta','sum'),pf=('pf','sum')).reset_index().sort_values('game_date')
# team drawn = own fta ; team committed = own pf ; allowed fta = opponent's fta in games vs them
allowed=tg.rename(columns={'team':'opp_t','opponent':'team'})[['game_date','team','fta']].rename(columns={'fta':'fta_allowed'})
tg=tg.merge(allowed,on=['game_date','team'],how='left')
T={t:v.reset_index(drop=True) for t,v in tg.groupby('team')}
# star foul drawer: opponent top player fta avg last 10 (per team, by date)
pl=g[['game_date','team','k','fta']]
d=pd.read_csv('../PropORACLE_main_cp/data/training/graded_export_20260516.csv',low_memory=False,usecols=['file_date','sport','player','prop','line','pick_type','direction','result_binary'])
d=d[(d.sport=='NBA')&(d.pick_type=='Standard')&d.prop.isin(['Free Throws Attempted','Personal Fouls','Free Throws Made'])&d.result_binary.isin([0,1])].copy()
d['k']=d.player.map(nm); d['fd']=pd.to_datetime(d.file_date)
gm=g.set_index(['k','game_date'])[['team','opponent']]
rows=[]
for r in d.itertuples():
    try: me=gm.loc[(r.k,r.fd)]
    except KeyError: continue
    if isinstance(me,pd.DataFrame): me=me.iloc[0]
    opp=me.opponent; my=me.team
    o=T.get(opp); m=T.get(my)
    if o is None or m is None: continue
    oo=o[o.game_date<r.fd].tail(10); mm=m[m.game_date<r.fd].tail(10)
    if len(oo)<6 or len(mm)<6: continue
    star=pl[(pl.team==opp)&(pl.game_date<r.fd)]
    star=star[star.game_date>=oo.game_date.min()].groupby('k').fta.mean().max()
    rows.append(dict(y=r.result_binary,side=1 if r.direction=='OVER' else -1,prop=r.prop,line=r.line,
        opp_drawn=oo.fta.mean(),opp_committed=oo.pf.mean(),opp_allowed=oo.fta_allowed.mean(),my_drawn=mm.fta.mean(),star_fta=star))
t=pd.DataFrame(rows); print(len(t)); t.to_pickle('/tmp/foul.pkl')
def terc(x): return pd.qcut(x,3,labels=['low','mid','high'])
for prop,feats in [('Free Throws Attempted',['opp_allowed','opp_committed','star_fta']),('Personal Fouls',['opp_drawn','star_fta','opp_allowed'])]:
    x=t[t.prop==prop]; print('\n==',prop,len(x),'base',round(x.y.mean(),3))
    for f in feats:
        x=x.assign(b=terc(x[f]))
        print(f'  by opponent {f} tercile (hit rate; Over / Under):')
        for b,v in x.groupby('b',observed=True):
            o=v[v.side==1];u=v[v.side==-1]
            print(f'    {b:4s} n={len(v):4d} Over {o.y.mean():.3f} (n={len(o)}) | Under {u.y.mean():.3f} (n={len(u)})  [{v[f].min():.1f}-{v[f].max():.1f}]')
