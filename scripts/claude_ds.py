import pandas as pd,numpy as np,re,unicodedata,warnings,time; warnings.filterwarnings('ignore')
t0=time.time()
_u=lambda s:''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c))
nm=lambda s:re.sub(r'\b(jr|sr|ii|iii|iv)\b','',re.sub(r'[^a-z ]','',_u(s).lower().replace('.','').replace("'",''))).strip()
g=pd.read_parquet('data/game_logs/nba_player_games.parquet')
g=g[g.phase.astype(str).str.upper()!='PRESEASON']; g=g[~g.is_dnp.astype(bool)&(g.minutes>0)]
g['game_date']=pd.to_datetime(g.game_date); g['k']=g.player.map(nm); g=g.sort_values(['k','game_date'])
mp={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','3-PT Attempted':'fg3a','Free Throws Made':'ftm','Free Throws Attempted':'fta','Personal Fouls':'pf','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score','Defensive Rebounds':'dreb','Offensive Rebounds':'oreb'}
cols=sorted(set(mp.values()))
tg=g.groupby(['game_date','team','opponent'])[cols].sum().reset_index()
al=tg.rename(columns={'team':'o','opponent':'team'}); al=al[['game_date','team']+cols].rename(columns={c:c+'_al' for c in cols})
tg=tg.merge(al,on=['game_date','team'],how='left').sort_values('game_date')
T={t:v.reset_index(drop=True) for t,v in tg.groupby('team')}
d=pd.read_pickle('/tmp/nba_rt.pkl')
d=d[d.prop.isin(mp)&d.result_binary.isin([0,1])&d.pick_type.isin(['Standard','Demon','Goblin'])].copy()
d['k']=d.player.map(nm); d['fd']=pd.to_datetime(d.file_date); d['side']=np.where(d.direction.str.upper().str[0]=='O',1,-1)
# rung lines for the same player-prop-date
piv=d.pivot_table(index=['file_date','k','prop'],columns='pick_type',values='line',aggfunc='first').reset_index().rename(columns={'Standard':'std_line','Goblin':'gob_line','Demon':'dem_line'})
d=d.merge(piv,on=['file_date','k','prop'],how='left')
by={k:v for k,v in g.groupby('k')}
rows=[]; cnt=0
for r in d.itertuples():
    v=by.get(r.k)
    if v is None: continue
    dts=v.game_date.values; n=np.searchsorted(dts,np.datetime64(r.fd),side='left')
    if n<20: continue
    c=mp[r.prop]; s=v[c].values[:n].astype(float); sg=r.side; L=r.line
    hit=(s>L) if sg==1 else (s<L)
    f=dict(y=r.result_binary,k=r.k,fd=r.fd,prop=r.prop,pt=r.pick_type,side=sg,line=L,std_line=r.std_line,gob_line=r.gob_line,dem_line=r.dem_line)
    for w in (5,10,15,20): f[f'h{w}']=hit[-w:].mean(); f[f'a{w}']=sg*(s[-w:].mean()-L)
    f['hs']=hit.mean(); f['as']=sg*(s.mean()-L); f['nS']=n
    l20=s[-20:]; f['sd20']=max(l20.std(),.5); f['avg20']=l20.mean(); f['med20']=np.median(l20); f['max20']=l20.max(); f['min20v']=l20.min(); f['p90']=np.percentile(l20,90); f['p10']=np.percentile(l20,10)
    f['line_pctile']=(l20<L).mean()          # share of last-20 games strictly below the line
    f['n_ge_line20']=(l20>=L).sum(); f['n_gt_line10']=(s[-10:]>L).sum()
    f['last']=(s[-1]>L) if sg==1 else (s[-1]<L); f['last2']=hit[-2:].mean()
    f['avg3']=s[-3:].mean(); f['avg5']=s[-5:].mean(); f['avg10']=s[-10:].mean()
    m=v.minutes.values[:n]; f['mn5']=m[-5:].mean(); f['mn20']=m[-20:].mean(); f['mn_sd']=m[-10:].std()
    f['mn_floor']=m[-10:].min()
    st=v.starter_flag.astype(float).values[:n]
    f['st3']=np.nanmean(st[-3:]) if np.isfinite(st[-3:]).any() else np.nan; f['stprev']=np.nanmean(st[-23:-3]) if np.isfinite(st[-23:-3]).any() else np.nan
    f['st10']=np.nanmean(st[-10:]) if np.isfinite(st[-10:]).any() else np.nan
    td=v[v.game_date==r.fd]
    if len(td):
        me=td.iloc[0]; f['rest']=me.rest_days; f['b2b']=me.back_to_back; f['home']=1 if str(me.home_away).lower().startswith('h') else 0
        f['total']=me.game_total; f['spread']=me.spread; f['pos']=str(me.position); opp=me.opponent
        o=T.get(opp)
        if o is not None:
            oo=o[o.game_date<r.fd]; oo10=oo.tail(10)
            if len(oo10)>=6: f['opp_al10']=oo10[c+'_al'].mean(); f['opp_alS']=oo[c+'_al'].mean()
            if len(oo10)>=6: f['opp_pts10']=oo10['pts'].mean(); f['opp_pts_al10']=oo10['pts_al'].mean()
    rows.append(f); cnt+=1
    if cnt%20000==0: print(cnt,round(time.time()-t0),flush=True)
t=pd.DataFrame(rows)
t=t.merge(d[['fd','k','prop','pick_type','team_star_out','key_facilitator_out']].rename(columns={'pick_type':'pt'}).drop_duplicates(['fd','k','prop','pt']),on=['fd','k','prop','pt'],how='left')
t.to_pickle('/tmp/ds.pkl'); print('done',t.shape,round(time.time()-t0),'s',flush=True)
