import _r2
"""Nightly ranked board with gate count. Usage: python scripts/claude_nightly_board.py 2026-11-06
Demon & Standard (Over/Under) rows -> 10 gates -> ngate. Demon ngate>=8 'BEST' (test ~45-53%), 6-7 'GOOD' (~41-47%).
Adds opponent defense rank + own-team production rank in that stat (1 = weakest defense / best-producing team) and logs to outputs/forward_test/board_forward.csv"""
import sys,os,re,unicodedata,pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
D=sys.argv[1] if len(sys.argv)>1 else '2026-11-06'; asof=pd.Timestamp(D)
_u=lambda s:''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c))
nm=lambda s:re.sub(r'\b(jr|sr|ii|iii|iv)\b','',re.sub(r'[^a-z ]','',_u(s).lower().replace('.','').replace("'",''))).strip()
mp={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','3-PT Attempted':'fg3a','Free Throws Made':'ftm','Free Throws Attempted':'fta','Personal Fouls':'pf','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Fantasy Score':'fantasy_score'}
sys.path.insert(0,'scripts'); from claude_gamelog import load_log
g,_np=load_log(); g=g[g.game_date<asof].sort_values(['k','game_date']); print('game log through',g.game_date.max().date(),'(+%d patch rows)'%_np)
cols=sorted(set(mp.values())); tg=g[g.opponent.notna()].groupby(['game_date','team','opponent'])[cols].sum().reset_index()
al=tg.rename(columns={'team':'o'}).rename(columns={'opponent':'team'})[['game_date','team']+cols].rename(columns={c:c+'_al' for c in cols})
tg=tg.merge(al,on=['game_date','team'],how='left').sort_values('game_date')
T={t:v.tail(10) for t,v in tg.groupby('team')}
am={'WAS':'WSH','UTA':'UTAH','NOP':'NO','SAS':'SA','GSW':'GS','NYK':'NY'}
by={k:v for k,v in g.groupby('k')}
b=pd.read_excel(f'../PropORACLE_main_cp/outputs/{D}/nba/step7_ranked_props.xlsx',sheet_name='ALL')
b=b[b.prop_type.isin(mp)&b.pick_type.isin(['Standard','Demon'])&~b.is_combo_player.astype(bool)].copy()
b['k']=b.player.map(nm)
def tm(x): 
    x=am.get(x,x); return x if x in T else None
rows=[]
for r in b.itertuples():
    v=by.get(r.k)
    if v is None or len(v)<20: continue
    c=mp[r.prop_type]; s=v[c].values.astype(float); L=r.line; l20=s[-20:]
    sides=['Over'] if r.pick_type=='Demon' else ['Over','Under']
    ot=tm(r.opp_team); mt=tm(r.team)
    oal=T[ot][c+'_al'].mean() if ot else np.nan; own=T[mt][c].mean() if mt else np.nan
    m=v.minutes.values; st=v.starter_flag.astype(float).values
    st3=np.nanmean(st[-3:]); stp=np.nanmean(st[-23:-3]); last=v.game_date.iloc[-1]
    for sd_ in sides:
        sg=1 if sd_=='Over' else -1; hit=(s>L) if sg==1 else (s<L)
        h={w:hit[-w:].mean() for w in(5,10,15,20)}; a={w:sg*(s[-w:].mean()-L) for w in(5,10,15,20)}
        hs=hit.mean(); votes=sum(x>=.6 for x in h.values())+(hs>=.6)+sum(x>0 for x in a.values())+(sg*(s.mean()-L)>0)
        mg=min(a.values())/max(L,.5); sd=max(l20.std(),.5); z=sg*(L-l20.mean())/sd
        rows.append(dict(player=r.player,team=r.team,opp=r.opp_team,prop=r.prop_type,type=r.pick_type,side=sd_,line=L,votes=int(votes),gap_pct=round(mg*100,1),h10=h[10],h20=h[20],
            _oal=oal,_own=own,z=z,min20=m[-20:].mean(),minfloor=m[-10:].min(),left5=int(stp>=.65 and st3<=.34),home=int(str(r.team)==str(r.pp_home_team)),b2b=int((asof-last).days<=1),sgn=sg,cleared20=int((l20>=L).sum())))
o=pd.DataFrame(rows)
# ranks among teams playing tonight, per prop
def trank(col,key,name):
    u=o.drop_duplicates(['prop',key])[['prop',key,col]].copy(); u[name]=u.groupby('prop')[col].rank(ascending=False,method='min'); u['n_'+name]=u.groupby('prop')[col].transform('count')
    return o.merge(u[['prop',key,name,'n_'+name]],on=['prop',key],how='left')
o=trank('_oal','opp','opp_rank'); o=trank('_own','team','team_rank')   # 1 = weakest defense / best-producing team, ranked among teams playing
opct=(o.opp_rank-1)/(o.n_opp_rank-1).clip(lower=1)
G=pd.DataFrame({'votes':o.votes>=8,'gap':o.gap_pct>=10,'h10':o.h10>=.7,'h20':o.h20>=.65,'opp':np.where(o.sgn==1,opct<=.4,opct>=.6),
    'min':(o.min20>=26)&(o.minfloor>=16),'role':o.left5==0,'home':o.home==1,'b2b':o.b2b==0,'line':o.z<=0}).fillna(False)
o['ngate']=G.sum(axis=1)
o['grade']=np.where(o.ngate>=8,'BEST',np.where(o.ngate>=6,'GOOD',''))
o['tag_TB']=((o.type=='Standard')&(o.side=='Over')&(o.cleared20>=14)).astype(int)
out=o.drop(columns=['_oal','_own','sgn','left5','b2b','z','n_opp_rank','n_team_rank']).sort_values(['type','side','ngate','gap_pct'],ascending=[True,True,False,False])
out.to_csv(f'outputs/nba_board_gates_{D}.csv',index=False)
os.makedirs('outputs/forward_test',exist_ok=True); fp='outputs/forward_test/board_forward.csv'
f=out[out.grade!=''].assign(date=D,result='')
old=pd.read_csv(fp) if os.path.exists(fp) else pd.DataFrame()
pd.concat([old[old.date!=D] if len(old) else old,f]).to_csv(fp,index=False)
print(out.groupby(['type','side','grade']).size().unstack(fill_value=0))
print(out[(out.type=='Demon')&(out.grade=='BEST')].head(25)[['player','team','opp','prop','line','ngate','votes','gap_pct','opp_rank','team_rank','home']].to_string(index=False))
