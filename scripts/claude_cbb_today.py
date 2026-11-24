import _r2
"""Tier today's CBB board with prop_tiers_cbb rules, then merge with the NBA tiered workbook into outputs/tiered_all_<day>.xlsx. Usage: python3 scripts/claude_cbb_today.py YYYY-MM-DD"""
import pandas as pd,numpy as np,sqlite3,sys,os,warnings; warnings.filterwarnings('ignore'); sys.path.insert(0,'scripts')
from claude_gamelog import nm
day=sys.argv[1]; D0=pd.Timestamp(day); B=f'../PropORACLE_main_cp/outputs/{day}/cbb/step1_cbb.csv'
MPC={'points':'pts','rebounds':'reb','assists':'ast','3-pt made':'fg3m','steals':'stl','blocked shots':'blk','turnovers':'tov','pts+rebs+asts':'pra','pts+rebs':'pr','pts+asts':'pa','rebs+asts':'ra','blks+stls':'bs'}
CC=['pts','reb','ast','stl','blk','tov','fg3m']
g1=pd.read_parquet('data/game_logs/cbb_player_games.parquet')[['player','game_date','minutes']+CC]
g2=pd.read_sql(f"select player,game_date,minutes,{','.join(CC)} from cbb",sqlite3.connect('data/cache/proporacle_ref.db'))
h=pd.concat([g1,g2],ignore_index=True)
for c in CC+['minutes']: h[c]=pd.to_numeric(h[c],errors='coerce')
h=h[h.minutes>0].drop_duplicates(['player','game_date']); h['gd']=pd.to_datetime(h.game_date); h=h[h.gd<D0]; h['k']=h.player.map(nm)
h['pra']=h.pts+h.reb+h.ast; h['pr']=h.pts+h.reb; h['pa']=h.pts+h.ast; h['ra']=h.reb+h.ast; h['bs']=h.blk+h.stl
G={k:v.sort_values('gd') for k,v in h.groupby('k')}
bd=pd.read_csv(B,encoding='utf-8-sig'); fa=pd.to_datetime(bd.fetched_at,utc=True,errors='coerce').max().tz_convert('America/New_York')
bd=bd[bd.duration.eq('Full')|bd.duration.isna()].copy(); bd['stat']=bd.stat_type.str.lower().map(MPC); bd['pt']=bd.odds_type.str.capitalize(); bd=bd[bd.stat.notna()&bd.pt.isin(['Goblin','Standard','Demon'])&~bd.player.astype(str).str.contains(r' \+ |/',regex=True)]
dup=bd[bd.pt=='Standard'].groupby(['player','stat']).line.nunique(); dk=set(dup[dup>1].index)
RA=pd.read_csv('outputs/prop_tiers_cbb_2026-11-11.csv'); PRES=set(zip(RA['pick'],RA['prop'],RA.side)); R=RA[RA.tier.isin(['S','A','B'])]
def feats(v,line,sg,sv):
    hit=(v>line) if sg==1 else (v<line); sd=max(float(np.std(v[-20:])),.3); a={w:v[-w:].mean() if len(v)>=w else np.nan for w in(5,10,15,20)}
    gz=[sg*(a[w]-line) for w in (5,10,20) if not np.isnan(a[w])]; f=dict(h5=hit[-5:].mean(),h10=hit[-10:].mean(),h20=hit[-20:].mean(),n=len(v),zmin=min(gz)/sd if gz else np.nan,thisseason=len(sv))
    for w,k in ((5,'G5'),(10,'G10'),(15,'G15'),(20,'G20')): f[k]=sg*(a[w]-line)/max(line,.5)
    f['GS']=np.nan; f['hs']=np.nan; return f
def ok(layer,f):
    L=layer.strip()
    if L=='L5 5/5': return f['h5']>=1
    if L=='L5 4/5+': return f['h5']>=.8
    if L=='L10 70%+': return f['h10']>=.7
    if L=='L10 80%+': return f['h10']>=.8
    if L=='L20 60%+': return f['h20']>=.6
    if L=='L20 70%+': return f['h20']>=.7
    if L.startswith('Season') and 'gap' not in L: return np.nan
    if L.startswith('SD gap>='): return f['zmin']>=float(L.split('>=')[1])
    if 'gap>=' in L:
        w=L.split()[0]; t=float(L.split('>=')[1].split('%')[0])/100; k={'L5':'G5','L10':'G10','L15':'G15','L20':'G20','Season':'GS'}[w]; v=f[k]; return np.nan if np.isnan(v) else v>=t
    return False
rows=[]
def Frow(r,side,reason,base=None):
    b=base or {'Pick type':r.pt,'Player':r.player,'Team':r.team,'Opp':r.pp_opp_team,'Prop':r.stat_type,'Side':side,'Line':r.line,'Board as of':f'{fa:%b %d %H:%M} ET'}
    return {**b,'Tier':'F','Rule':'','Rule test hit%':np.nan,'Rule test n':np.nan,'Status':'UNGATED','Reason':reason}
for r in bd.drop_duplicates(['player','stat','line','pt']).itertuples():
    sides=[('Over',1),('Under',-1)] if r.pt=='Standard' else [('Over',1)]
    why=None
    if r.pt=='Standard' and (r.player,r.stat) in dk: why='Standard with more than one line (ambiguous)'
    elif r.pt=='Standard' and r.line<=1.0: why='line 1.0 or lower (too coarse)'
    else:
        g=G.get(nm(r.player))
        if g is None or len(g)<8: why='fewer than 8 games of history'
        else:
            sv=g[g.gd>=pd.Timestamp('2026-10-01')]
            if len(sv)<2: why='fewer than 2 games this season'
    if why:
        for sd_,_ in sides: rows.append(Frow(r,sd_,why))
        continue
    v=g[r.stat].values.astype(float)
    for side,sg in (('Over',1),('Under',-1)):
        if r.pt in('Goblin','Demon') and side=='Under': continue
        f=feats(v,r.line,sg,sv); base={'Pick type':r.pt,'Player':r.player,'Team':r.team,'Opp':r.pp_opp_team,'Prop':r.stat_type,'Side':side,'Line':r.line,'L5':f"{int(round(f['h5']*5))}/5",'L10 hit%':round(f['h10']*100),'L20 hit%':round(f['h20']*100),'Games (all)':f['n'],'Games this season':f['thisseason'],'SD gap (min)':round(f['zmin'],2) if not np.isnan(f['zmin']) else np.nan,'Board as of':f'{fa:%b %d %H:%M} ET'}
        rule=R[(R['pick']==r.pt)&(R['prop']==r.stat)&(R.side==side)]
        if not len(rule):
            rows.append({**Frow(r,side,'group tier C/D (rule did not beat its baseline)' if (r.pt,r.stat,side) in PRES else 'group not tested (not enough data)',base)}); continue
        ru=rule.iloc[0]; layers=[] if ru.layers=='(no gate helped)' else ru.layers.split(' > '); res=[ok(l,f) for l in layers]
        if any((not (isinstance(x,float) and np.isnan(x))) and (not bool(x)) for x in res):
            rows.append(Frow(r,side,'failed its tier rule today',base)); continue
        pend=any(isinstance(x,float) and np.isnan(x) for x in res)
        rows.append({**base,'Tier':ru.tier,'Rule':ru.layers,'Rule test hit%':ru.test_pct,'Rule test n':ru.test_n,'Status':'needs season sample (10 games)' if pend else 'PASS','Reason':''})
P=pd.DataFrame(rows)
_G={k:v.sort_values('gd') for k,v in h.groupby('k')}
def _cm(pl):
    g=_G.get(nm(pl))
    if g is None or not len(g): return (np.nan,np.nan)
    m=g.minutes.values.astype(float); return (m[-5:].mean(),m[-5:].min())
_c={pl:_cm(pl) for pl in P.Player.unique()}
P['Exp min (L5 avg)']=P.Player.map(lambda x:_c[x][0]); P['Min L5 low']=P.Player.map(lambda x:_c[x][1]); P['Injury status']='no CBB injury feed'
S3=pd.read_csv(f'../PropORACLE_main_cp/outputs/{day}/cbb/step3b_with_def_rankings_cbb.csv',encoding='utf-8-sig').drop_duplicates('player')[['player','pp_opp_team','opp_def_rank','opp_def_ppg','opp_def_tier']]
S3.columns=['Player','_opp3','Opp def rank','Opp def PPG','Opp defense tier']; P=P.merge(S3,on='Player',how='left'); P['Opp']=P['Opp'].fillna(P['_opp3']); P=P.drop(columns='_opp3')
P['Defense flag']=np.where(P['Opp defense tier'].isna(),'MISSING (pipeline has no opponent rating)',np.where(P.Side=='Over','softer = better for Over','tougher = better for Under'))
dm=P['Opp defense tier'].map({'Elite':0,'Above Avg':1,'Below Avg':3,'Weak':4}); P['_d']=np.where(P.Side=='Over',dm,4-dm); P['_d']=P['_d'].fillna(-1)
P['o']=P.Tier.map({'S':0,'A':1,'B':2,'F':3}); P=P.sort_values(['Pick type','Side','o','Status','_d','SD gap (min)'],ascending=[True,True,True,True,False,False]).drop(columns=['o','_d'])
nbaf=f'outputs/tiered_picks_{day}.xlsx'; out=f'outputs/tiered_all_{day}.xlsx'
with pd.ExcelWriter(out) as w:
    pd.DataFrame({'Notes':[f'CBB board as of {fa:%b %d %H:%M} ET (NBA board time is on each NBA row). Four blocks per sport: Standard Over, Standard Under, Goblin Over, Demon Over; each tiered on its own, never ranked against each other.','Tier = how far a rule beat that block\'s own ungated hit rate in train and test (S >=12 pts, A >=8, B >=4, C >=0, D below, F untested). Rules chosen on train only.','CBB rules come from only 6 weeks of last season (Feb 24 - Apr 6, 2026, tournament-heavy; train < Mar 12). Treat CBB tiers as weak leads.','CBB defense comes from the pipeline (step3b: Elite / Above Avg / Below Avg / Weak, rank, PPG allowed) and is a ranking tiebreaker only (Overs: softer first; Unders: tougher first); rows with no opponent rating are flagged MISSING. No minutes or role columns for CBB. Season-based rules wait for 10 games this season.','Standard Overs are watch-only. Re-check lines on PrizePicks before building.']}).to_excel(w,sheet_name='Read me',index=False)
    for k,x in pd.read_excel(nbaf,sheet_name=None).items():
        if k!='Read me': x.to_excel(w,sheet_name=('NBA '+k)[:31],index=False); w.sheets[('NBA '+k)[:31]].set_column(0,25,16)
    for (pt,sd),x in P.groupby(['Pick type','Side']):
        l5=pd.to_numeric(x.L5.astype(str).str.split('/').str[0],errors='coerce'); l10=pd.to_numeric(x['L10 hit%'],errors='coerce'); l20=pd.to_numeric(x['L20 hit%'],errors='coerce')
        gs=pd.to_numeric(x['Games this season'],errors='coerce')
        ok=(l5>=(5 if pt=='Goblin' else 4))&(l10>=80)&(l20>=75); y=x[ok].copy()
        y['Tier shown']=np.where(y.Tier.isin(['S','A','B'])&y.Status.eq('PASS'),y.Tier,'F (unverified)')
        y['Gate check']=np.where(gs[ok]>=10,'L5/L10/L20 pass; season sample ok','L5/L10/L20 pass; season pending (<10 games)')
        y=y.drop(columns=['Pick type','Side']); n2=f'CBB Gates {pt} {sd}'[:31]
        if len(y): y.to_excel(w,sheet_name=n2,index=False); w.sheets[n2].set_column(0,30,16)
    for (pt,sd),x in P.groupby(['Pick type','Side']): n=f'CBB {pt} {sd}'[:31]; x.drop(columns=['Pick type','Side']).to_excel(w,sheet_name=n,index=False); w.sheets[n].set_column(0,25,16)
print(out,P.groupby(['Pick type','Side','Tier','Status']).size().to_dict())
