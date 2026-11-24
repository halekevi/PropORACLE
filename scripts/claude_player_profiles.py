import _r2
import sqlite3,numpy as np,pandas as pd,glob,warnings; warnings.filterwarnings('ignore')
from scipy import stats as st
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql('select * from nba',c)
n['gd']=pd.to_datetime(n.game_date); n['minutes']=pd.to_numeric(n.minutes,errors='coerce')
n=n[n.minutes>0].drop_duplicates(['event_id','player'])
S=['pts','reb','ast','fg3m','stl','blk','tov','pra','pr','pa','ra','fga','fta','ftm','pf','fantasy_score']
for s in S+['fgm','dreb','oreb']: n[s]=pd.to_numeric(n[s],errors='coerce')
n['home']=(n.team==n.home_team).astype(int); n['opp']=np.where(n.home==1,n.away_team,n.home_team)
# opponent allowed per game (team totals), rank tiers
T=n.groupby(['event_id','team']).agg(**{s:(s,'sum') for s in S}).reset_index()
O=n.groupby(['event_id','opp'])[S].sum().reset_index()   # what opponent 'opp' allowed? totals by players facing opp
A=O.groupby('opp')[S].mean()
tier={s:pd.qcut(A[s].rank(method='first'),3,labels=['tough','mid','soft']) for s in S}  # low allowed = tough
last=n.groupby('player').gd.max(); cnt=n.groupby('player').size()
act=[p for p in cnt.index if cnt[p]>=25 and last[p]>=pd.Timestamp('2026-10-20')]
rows_cv=[];rows_hr=[];rows_op=[];rows_tm=[]
for p in act:
    g=n[n.player==p].sort_values('gd'); mpg=g.minutes.tail(15).mean(); team=g.team.iloc[-1]
    for s in S:
        x=g[s].dropna()
        if len(x)<25 or x.mean()<0.5: continue
        cv=x.std()/x.mean(); l20=x.tail(20)
        rows_cv.append((p,team,round(mpg,1),s,len(x),round(x.mean(),2),round(x.std(),2),round(cv,2),round(l20.mean(),2),round(l20.std()/max(l20.mean(),.01),2)))
        h=g[g.home==1][s].dropna(); r=g[g.home==0][s].dropna()
        if len(h)>=10 and len(r)>=10:
            t,pv=st.ttest_ind(h,r,equal_var=False)
            rows_hr.append((p,team,s,round(h.mean(),2),len(h),round(r.mean(),2),len(r),round(h.mean()-r.mean(),2),round((h.mean()-r.mean())/max(x.std(),.3),2),round(t,2),round(pv,3)))
        g2=g.copy(); g2['tier']=g2.opp.map(lambda o:tier[s].get(o,np.nan))
        for tr,sub in g2.groupby('tier'):
            pass
        tg=g2[g2.tier=='tough'][s].dropna(); sf=g2[g2.tier=='soft'][s].dropna()
        if len(tg)>=8 and len(sf)>=8:
            t,pv=st.ttest_ind(sf,tg,equal_var=False)
            rows_op.append((p,team,s,round(tg.mean(),2),len(tg),round(sf.mean(),2),len(sf),round(sf.mean()-tg.mean(),2),round((sf.mean()-tg.mean())/max(x.std(),.3),2),round(t,2),round(pv,3)))
        for o,sub in g.groupby('opp'):
            y=sub[s].dropna()
            if len(y)>=3:
                z=(y.mean()-x.mean())/(x.std()/np.sqrt(len(y))) if x.std()>0 else 0
                rows_tm.append((p,team,s,o,len(y),round(y.mean(),2),round(x.mean(),2),round(y.mean()-x.mean(),2),round(z,2)))
cv=pd.DataFrame(rows_cv,columns=['player','team','mpg15','stat','n','avg','sd','cv','avg20','cv20'])
hr=pd.DataFrame(rows_hr,columns=['player','team','stat','home_avg','n_home','road_avg','n_road','diff','diff_sd','t','p'])
op=pd.DataFrame(rows_op,columns=['player','team','stat','tough_avg','n_tough','soft_avg','n_soft','soft_minus_tough','diff_sd','t','p'])
tm=pd.DataFrame(rows_tm,columns=['player','team','stat','opp','n','avg_vs','avg_all','diff','z'])
# ledger hit rates by player/prop/pick type
L=pd.concat([pd.read_parquet(f,columns=['sport','player','prop','line','direction','pick_type','hit']) for f in glob.glob('data/ledger/legs/slate_date=*/*')])
L=L[(L.sport=='NBA')&L.hit.notna()]; L['pt']=L.pick_type.str.title(); L['dir']=L.direction.str.upper().str[:1]
lh=L.groupby(['player','prop','pt','dir']).hit.agg(['mean','count']).reset_index(); lh=lh[lh['count']>=12].sort_values(['pt','mean'],ascending=[True,False])
cvs=cv.sort_values(['player','cv'])
out='outputs/player_profiles_2026-11-09.xlsx'
with pd.ExcelWriter(out) as w:
    cvs.to_excel(w,'Steadiness_by_player',index=False)
    hr.sort_values('t',key=abs,ascending=False).to_excel(w,'Home_vs_Road',index=False)
    op.sort_values('t',key=abs,ascending=False).to_excel(w,'Soft_vs_Tough_D',index=False)
    tm.sort_values('z',key=abs,ascending=False).to_excel(w,'Specific_opponents',index=False)
    lh.to_excel(w,'Ledger_hit_rates',index=False)
print(len(act),'players; wrote',out)
print('\nSteadiest stat (lowest CV, avg>=2 for count stats) for top-minute players:')
top=cv.drop_duplicates('player').sort_values('mpg15',ascending=False).head(25).player
for p in top:
    q=cv[(cv.player==p)&(cv.avg>=2)].sort_values('cv').head(3)
    print(f' {p:22s} '+' | '.join(f'{r.stat} avg {r.avg} cv {r.cv}' for r in q.itertuples()))
print('\nBiggest home/road splits (|t|>=2.5, avg>=2):'); 
x=hr[(hr.t.abs()>=2.5)&((hr.home_avg+hr.road_avg)/2>=2)].sort_values('t',key=abs,ascending=False).head(15); print(x[['player','stat','home_avg','road_avg','diff','t']].to_string(index=False))
print('\nBiggest soft-vs-tough defense effect (|t|>=2.5, avg>=2):')
x=op[(op.t.abs()>=2.5)&((op.soft_avg+op.tough_avg)/2>=2)].sort_values('t',key=abs,ascending=False).head(15); print(x[['player','stat','tough_avg','soft_avg','soft_minus_tough','t']].to_string(index=False))
print('\nsig home/road share',round((hr.p<.05).mean(),3),'expected 0.05 | soft/tough',round((op.p<.05).mean(),3))
