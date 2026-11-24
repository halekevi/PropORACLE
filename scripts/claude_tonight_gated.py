import _r2
import pandas as pd,numpy as np,re,warnings; warnings.filterwarnings('ignore')
pd.set_option('display.width',250,'display.max_rows',100,'display.max_columns',30)
base='../PropORACLE_main_cp/outputs/2026-11-06/nba/'
# ---------- GOBLIN OVERS (rules from v2 test) ----------
g=pd.read_excel(base+'step7_ranked_props.xlsx',sheet_name='GOB_DEM',low_memory=False) if False else pd.read_excel(base+'step7_ranked_props.xlsx',sheet_name='GOB_DEM')
g=g[g.pick_type=='Goblin'].copy()
print('Goblin rows',len(g), 'dirs',g.bet_direction.value_counts().to_dict() if 'bet_direction' in g else '')
g['prop']=g.prop_type; g['side']='O'
g['proj_gap']=g.projection-g.line; g['proj_gap_pct']=g.proj_gap/g.line.clip(lower=.5)*100
rules={'Rebounds':[('proj_gap_pct','>=',64.2857),('usage_pct','<=',0.253)],
 'Pts+Rebs+Asts':[('proj_gap_pct','>=',40.8421),('opp_ast_allowed_vs_position','>=',25)],
 'Points':[('proj_gap_pct','>=',41.5789),('ml_prob','>=',0.9065)],
 '3-PT Made':[('proj_gap_pct','>=',120),('game_pace','>=',99.16)],
 'Pts+Rebs':[('ml_edge','>=',0.411),('projection','<=',27.46)]}
res=[]
for p,rs in rules.items():
    x=g[g.prop==p]; m=pd.Series(True,index=x.index)
    for f,o,q in rs: m&=((x[f]>=q) if o=='>=' else (x[f]<=q)).fillna(False)
    y=x[m]; print(p,'rows',len(x),'pass',len(y)); res.append(y)
G=pd.concat(res)
G['gate']=G.prop.map(lambda p:' & '.join(f'{f}{o}{q:g}' for f,o,q in rules[p]))
cols=['player','team','opp_team','prop','line','projection','proj_gap_pct','ml_prob','ml_edge','usage_pct','game_pace','gate']
G=G.sort_values('proj_gap_pct',ascending=False)
print('\nGOBLIN OVERS passing the per-prop gate:',len(G)); print(G[[c for c in cols if c in G]].round(2).head(40).to_string(index=False))
G[[c for c in cols if c in G]].to_csv('outputs/nba_tonight_goblin_gated_2026-11-06.csv',index=False)
# ---------- STANDARDS: signals + gap + opponent gates ----------
exec(open('scripts/claude_context_bt.py').read().split("d=pd.read_csv")[0])
asof=pd.Timestamp('2026-11-06')
a=pd.read_csv('outputs/nba_consistency_all_2026-11-06.csv')
n=lambda x:int(str(x).split('/')[0])
for w,k in [('L5',5),('L10',10),('L15',15),('L20',20)]: a['h'+w]=a[w].map(n)/k
a['votes']=sum((a['h'+w]>=.6).astype(int) for w in ['L5','L10','L15','L20'])+(a.hs>=.6).astype(int)+sum((a[x]>0).astype(int) for x in ['gap5','gap10','gap15','gap20'])+(a.aS>0).astype(int)
a['mg']=a[['gap5','gap10','gap15','gap20']].min(axis=1)/a.line*100
s5=pd.read_csv(base+'step5_with_hit_rates.csv',low_memory=False)
opp=s5[['player','opp_team','is_back_to_back']].dropna(subset=['opp_team']).drop_duplicates('player').set_index('player')
am={'WAS':'WSH','UTA':'UTAH','NOP':'NO','SAS':'SA','GSW':'GS','NYK':'NY','PHX':'PHX'}
lg={}
for c in set(mp.values()): lg[c]=np.mean([v[v.game_date<asof].tail(10)[c+'_al'].mean() for v in T.values() if len(v)>5])
def oppdef(p,prop):
    o=opp.opp_team.get(p); 
    if o is None: return np.nan
    o=am.get(o,o); 
    if o not in T: return np.nan
    c=mp.get(prop); 
    if c is None: return np.nan
    return T[o][T[o].game_date<asof].tail(10)[c+'_al'].mean()/lg[c]-1
a['opp']=a.player.map(opp.opp_team); a['opp_rel']=[oppdef(p,pr) for p,pr in zip(a.player,a.prop)]
def mchg(p):
    m=g_log[g_log.k==nm(p)].minutes.values
    return (m[-5:].mean()/m[-20:].mean()-1,m[-20:].mean(),m[-5:].mean()) if len(m)>=20 else (np.nan,np.nan,np.nan)
g_log=globals()['g'] if False else pd.read_parquet('data/game_logs/nba_player_games.parquet')
g_log=g_log[g_log.phase.astype(str).str.upper()!='PRESEASON']; g_log=g_log[~g_log.is_dnp.astype(bool)&(g_log.minutes>0)]
g_log['game_date']=pd.to_datetime(g_log.game_date); g_log['k']=g_log.player.map(nm); g_log=g_log.sort_values(['k','game_date'])
mm=a.player.map(mchg); a['mc']=mm.map(lambda z:z[0]); a['min20']=mm.map(lambda z:z[1])
U=a.side=='U'
a['g_opp']=np.where(U,a.opp_rel<=-.03,a.opp_rel>=0)
a['f_min']=np.where(U,a.mc<=.05,a.mc>=-.05); a['f_lowmin_over']=(~U)&(a.min20<26)
a['b2b']=a.player.map(opp.is_back_to_back).fillna(0)
a['f_b2b_over']=(~U)&(a.b2b.astype(float)>0)
S=a[(a.votes>=8)&(a.room>=0)&(a.nS>=7)&(~a.player.str.contains('/'))&(a.mg>=10)&(a.hL10>=.8)&(a.hL20>=.75)&a.g_opp].copy()
S['flags']=[', '.join(f for f,v in [('minutes trend against',not r.f_min),('Over <26 min',r.f_lowmin_over),('Over on B2B',r.f_b2b_over)] if v) for r in S.itertuples()]
S=S.sort_values(['flags','mg'],ascending=[True,False])
print('\nSTANDARDS passing signals + gap + opponent gates:',len(S))
print(S[['player','opp','side','prop','line','L5','L10','L15','L20','avg5','avg20','mg','opp_rel','mc','min20','votes','flags']].round(2).to_string(index=False))
S.to_csv('outputs/nba_tonight_standard_gated_2026-11-06.csv',index=False)
