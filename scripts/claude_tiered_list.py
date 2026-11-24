import _r2
"""Tag the NBA consistency table with gate tiers. Usage: python scripts/claude_tiered_list.py 2026-11-06
 TIER 1: all 10 signals agree + gap>=20% of line (smallest of L5/L10/L15/L20 gaps) + opponent defense favors the pick by >=3%   (test 64.7%, n=119)
 TIER 2: signals>=8 + gap>=10% + L10>=80% + L20>=75% + opponent favors (>=0%) + no Over under 26 min + no Under after leaving starting 5 (test 62.7%, n=209)
 Everything else is untiered. Rows are Standard, one line per player/prop/side."""
import sys,pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
D=sys.argv[1] if len(sys.argv)>1 else '2026-11-06'
exec(open('scripts/claude_context_bt.py').read().split("d=pd.read_csv")[0])
asof=pd.Timestamp(D)
a=pd.read_csv(f'outputs/nba_consistency_all_{D}.csv')
n=lambda x:int(str(x).split('/')[0])
for w,k in [('L5',5),('L10',10),('L15',15),('L20',20)]: a['h'+w]=a[w].map(n)/k
a['votes']=sum((a['h'+w]>=.6).astype(int) for w in ['L5','L10','L15','L20'])+(a.hs>=.6).astype(int)+sum((a[x]>0).astype(int) for x in ['gap5','gap10','gap15','gap20'])+(a.aS>0).astype(int)
a['gap_pct']=a[['gap5','gap10','gap15','gap20']].min(axis=1)/a.line.clip(lower=.5)*100
s5=pd.read_csv(f'../PropORACLE_main_cp/outputs/{D}/nba/step5_with_hit_rates.csv',low_memory=False)
opp=s5[['player','opp_team']].dropna().drop_duplicates('player').set_index('player').opp_team
am={'WAS':'WSH','UTA':'UTAH','NOP':'NO','SAS':'SA','GSW':'GS','NYK':'NY'}
lg={c:np.mean([v[v.game_date<asof].tail(10)[c+'_al'].mean() for v in T.values() if len(v)>5]) for c in set(mp.values())}
def oppd(p,pr):
    o=opp.get(p); o=am.get(o,o)
    if o not in T or pr not in mp: return np.nan
    c=mp[pr]; return T[o][T[o].game_date<asof].tail(10)[c+'_al'].mean()/lg[c]-1
a['opp']=a.player.map(opp); a['opp_rel']=[oppd(p,r) for p,r in zip(a.player,a.prop)]
a['sgn']=np.where(a.side=='O',1,-1); a['opp_edge']=a.opp_rel*a.sgn*100   # +ve = defense favors the pick
gl=pd.read_parquet('data/game_logs/nba_player_games.parquet'); gl=gl[gl.phase.astype(str).str.upper()!='PRESEASON']; gl=gl[~gl.is_dnp.astype(bool)&(gl.minutes>0)]
gl['game_date']=pd.to_datetime(gl.game_date); gl['k']=gl.player.map(nm); gl=gl.sort_values(['k','game_date']); gl=gl[gl.game_date<asof]
by={k:v for k,v in gl.groupby('k')}
def info(p):
    v=by.get(nm(p)); 
    if v is None or len(v)<5: return (np.nan,np.nan,np.nan)
    m=v.minutes.values; st=v.starter_flag.astype(float).values
    s3=np.nanmean(st[-3:]) if np.isfinite(st[-3:]).any() else np.nan; sp=np.nanmean(st[-23:-3]) if np.isfinite(st[-23:-3]).any() else np.nan
    return (m[-20:].mean(), m[-5:].mean()/m[-20:].mean()-1, 1.0 if (s3<=.34 and sp>=.65) else 0.0)
x=a.player.map(info); a['min20']=x.map(lambda z:z[0]); a['min_trend']=x.map(lambda z:z[1]); a['demoted']=x.map(lambda z:z[2])
ok=(a.room>=0)&(a.nS>=7)&(~a.player.str.contains('/'))
t1=ok&(a.votes==10)&(a.gap_pct>=20)&(a.opp_edge>=3)
t2=ok&(a.votes>=8)&(a.gap_pct>=10)&(a.hL10>=.8)&(a.hL20>=.75)&(a.opp_edge>=0)&~((a.side=='O')&(a.min20<26))&~((a.side=='U')&(a.demoted==1))
a['tier']=np.where(t1,'T1',np.where(t2,'T2',''))
a['flags']=[', '.join(f for f,v in [('minutes up 15%+ vs Under',r.side=='U' and r.min_trend>.15),('minutes down 15%+ vs Over',r.side=='O' and r.min_trend<-.15)] if v) for r in a.itertuples()]
out=a[a.tier!=''].sort_values(['tier','gap_pct'],ascending=[True,False])
cols=['tier','player','opp','side','prop','line','L5','L10','L15','L20','avg5','avg20','gap_pct','opp_edge','min20','votes','flags']
out[cols].round(1).to_csv(f'outputs/nba_tiered_{D}.csv',index=False)
print(out[cols].round(1).to_string(index=False)); print('\nT1',(out.tier=='T1').sum(),'T2',(out.tier=='T2').sum())
