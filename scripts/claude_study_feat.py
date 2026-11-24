import sqlite3, numpy as np, pandas as pd, warnings, sys
warnings.filterwarnings('ignore')
DB='data/cache/proporacle_ref.db'
c=sqlite3.connect(DB)
n=pd.read_sql('select * from nba',c)
n['player']=n.player.str.lower().str.strip()
n['gd']=pd.to_datetime(n.game_date)
for col in ['minutes','pts','reb','ast','stl','blk','tov','fgm','fga','fg3m','fg3a','fg2m','fg2a','ftm','fta','oreb','dreb','pf','pra','pr','pa','ra','bs','fantasy_score','starter']:
    n[col]=pd.to_numeric(n[col],errors='coerce')
n=n[n.minutes>0].drop_duplicates(['event_id','player']).sort_values(['gd','event_id']).reset_index(drop=True)
n['home']=(n.team==n.home_team).astype(int)
n['opp_t']=np.where(n.home==1,n.away_team,n.home_team)
STATS=['pts','reb','ast','stl','blk','tov','fg3m','fg3a','fga','fgm','fg2a','fg2m','fta','ftm','dreb','oreb','pf','pra','pr','pa','ra','bs','fantasy_score']
# team-game table
T=n.groupby(['event_id','team']).agg(gd=('gd','first'),opp_t=('opp_t','first'),home=('home','first'),**{s:(s,'sum') for s in STATS}).reset_index()
T['poss']=T.fga+0.44*T.fta-T.oreb+T.tov
# allowed (opponent totals in same event)
O=T[['event_id','team']+STATS+['poss']].rename(columns={'team':'opp_t',**{s:'al_'+s for s in STATS},'poss':'al_poss'})
T=T.merge(O,on=['event_id','opp_t'],how='left').sort_values(['team','gd','event_id']).reset_index(drop=True)
g=T.groupby('team')
for s in STATS+['poss']:
    T['r_al_'+s]=g['al_'+s].transform(lambda x:x.shift(1).rolling(10,min_periods=3).mean())
    T['r_own_'+s]=g[s].transform(lambda x:x.shift(1).rolling(10,min_periods=3).mean())
T['rest']=g['gd'].diff().dt.days
T['g7']=g['gd'].transform(lambda x:(x-x.shift(3)).dt.days)
lg={s:T['al_'+s].expanding().mean().shift(1) for s in STATS+['poss']}
for s in STATS+['poss']: T['lg_'+s]=lg[s]
TK=T.set_index(['event_id','team'])
# player rolling minutes -> teammates out
n=n.sort_values(['player','gd']).reset_index(drop=True)
n['amin10']=n.groupby('player').minutes.transform(lambda x:x.rolling(10,min_periods=3).mean())
ev_players=n.groupby(['event_id','team']).player.apply(set).to_dict()
teams_ev=T[['team','event_id','gd']].sort_values(['team','gd','event_id'])
prev={}
last={}
for t,e in zip(teams_ev.team,teams_ev.event_id):
    prev[(t,e)]=last.get(t); last[t]=e
am=n.set_index(['event_id','team','player']).amin10.to_dict()
out_cnt={};out_min={}
for (t,e),pe in prev.items():
    if pe is None: out_cnt[(t,e)]=0;out_min[(t,e)]=0.;continue
    cur=ev_players.get((e,t),set()); cnt=0;mn=0.
    for p in ev_players.get((pe,t),set()):
        a=am.get((pe,t,p),np.nan)
        if a>=22 and p not in cur: cnt+=1; mn+=a
    out_cnt[(t,e)]=cnt;out_min[(t,e)]=mn
# per-player arrays
P={k:v for k,v in n.groupby('player')}
d=pd.read_pickle('/tmp/posdef_ds2.pkl'); d=d[d.hit.notna()].copy(); d['hit']=d.hit.astype(float)
d['sd']=pd.to_datetime(d.slate_date)
rows=[]
miss=0
for idx,r in d.iterrows():
    pdf=P.get(r.k)
    if pdf is None: miss+=1; continue
    col=r.col
    if col not in pdf.columns: miss+=1; continue
    dates=pdf.gd.values
    i=np.searchsorted(dates,np.datetime64(r.sd),side='left')  # games strictly before slate date
    if i<5 or i>=len(pdf): miss+=1; continue
    tgt=pdf.iloc[i]
    if (tgt.gd-r.sd).days>1: miss+=1; continue
    s=pdf[col].values[:i]; m=pdf.minutes.values[:i]; st=pdf.starter.values[:i]
    line=r.line; side=r.side
    f={'idx':idx}
    def av(a,k): return a[-k:].mean()
    for k in (3,5,10,20): f[f'av{k}']=av(s,k)
    f['avs']=s.mean(); f['ng']=i
    f['sd20']=max(s[-20:].std(),0.3); f['sd10']=max(s[-10:].std(),0.3)
    w=0.5**(np.arange(len(s))[::-1]/6); f['ewm']=(s*w).sum()/w.sum()
    # per-minute rate and projected
    mp10=m[-10:].mean(); rate=s[-20:].sum()/max(m[-20:].sum(),1)
    f['mp10']=mp10; f['msd10']=m[-10:].std(); f['mp3']=m[-3:].mean(); f['mtrend']=m[-5:].mean()-m[-20:].mean()
    f['mmin5']=m[-5:].min(); f['start10']=np.nanmean(st[-10:]); f['rate_pm']=rate; f['proj']=rate*mp10
    # EB shrink toward the line (market prior)
    kk=6.0; f['eb_line']=(i*0+min(i,20)*f['av20']+kk*line)/(min(i,20)+kk)
    f['eb_gap']=side*(f['eb_line']-line)/f['sd20']
    for k in (5,10,20): f[f'g{k}s']=side*(f[f'av{k}']-line)/f['sd20']
    f['gewm']=side*(f['ewm']-line)/f['sd20']; f['gproj']=side*(f['proj']-line)/f['sd20']
    f['gavs']=side*(f['avs']-line)/f['sd20']
    from math import erf,sqrt
    z=side*(f['ewm']-line)/f['sd20']; f['pnorm']=0.5*(1+erf(z/sqrt(2)))
    f['last']=side*(s[-1]-line)/f['sd20']
    f['worst10']=side*(s[-10:]*side).min()*side-line*side if False else (side*(s[-10:]-line)).min()
    f['best10']=(side*(s[-10:]-line)).max()
    f['frac_eq']=(s[-20:]==line).mean(); f['line_over_avg']=line/max(f['av20'],0.3)
    # schedule/context
    f['home']=tgt.home; f['rest']=(tgt.gd-pdf.gd.values[i-1]).days
    f['b2b']=int(f['rest']==1)
    key=(tgt.event_id,tgt.team)
    if key in TK.index:
        t=TK.loc[key]
        f['g7']=t.g7; f['team_rest']=t.rest
        f['own_pace']=t.r_own_poss; f['out_cnt']=out_cnt.get((tgt.team,tgt.event_id),0); f['out_min']=out_min.get((tgt.team,tgt.event_id),0.)
        ok=(tgt.event_id,tgt.opp_t)
        if ok in TK.index:
            o=TK.loc[ok]
            f['opp_pace']=o.r_own_poss; f['opp_rest']=o.rest
            f['opp_al']=o['r_al_'+col]/o['lg_'+col] if col in STATS and o['lg_'+col]>0 else np.nan
            f['opp_al_poss']=o.r_al_poss/o.lg_poss if o.lg_poss>0 else np.nan
            f['pace_f']=(f['own_pace']+f['opp_pace'])/2
    rows.append(f)
F=pd.DataFrame(rows).set_index('idx')
D=d.join(F,how='inner')
print('matched',len(D),'missed',miss,'of',len(d))
D.to_pickle('/tmp/study_ds.pkl')
