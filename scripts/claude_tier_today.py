import _r2
"""Apply the tiered per-prop rules (prop_tiers_cum2) to a day's NBA board. Usage: python3 scripts/claude_tier_today.py YYYY-MM-DD
Writes outputs/tiered_picks_<day>.xlsx and appends new picks to outputs/ticket_log/forward_tracking_tiers.csv (log-only; grades filled later by claude_tier_grade.py)."""
import pandas as pd,numpy as np,sqlite3,sys,os,datetime,warnings; warnings.filterwarnings('ignore'); sys.path.insert(0,'scripts')
from claude_gamelog import nm
from claude_defense_rel import table
day=sys.argv[1]; D0=pd.Timestamp(day); B=f'../PropORACLE_main_cp/outputs/{day}/nba/step1_pp_props_today.csv'
MPN={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','3-PT Attempted':'fg3a','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','Free Throws Made':'ftm','Free Throws Attempted':'fta','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a','Offensive Rebounds':'oreb','Defensive Rebounds':'dreb','Fantasy Score':'fantasy_score','Personal Fouls':'pf'}
RA=pd.read_csv('outputs/prop_tiers_cum3_2026-11-11.csv'); PRES=set(zip(RA['pick'],RA['prop'],RA.side)); R=RA.copy(); R=R[R.tier.isin(['S','A','B'])&(R.layers!='(no gate helped)')|((R.layers=='(no gate helped)')&R.tier.isin(['S','A','B']))]
bd=pd.read_csv(B,encoding='utf-8-sig'); fa=pd.to_datetime(bd.fetched_at,utc=True,errors='coerce').max().tz_convert('America/New_York')
bd['stat']=bd.prop_type.map(MPN); bd['pt']=bd.pick_type.str.capitalize(); bd=bd[bd.stat.notna()&bd.pt.isin(['Goblin','Standard','Demon'])&~bd.player.astype(str).str.contains(r' \+ |/',regex=True)]
dup=bd[bd.pt=='Standard'].groupby(['player','stat']).line.nunique(); dk=set(dup[dup>1].index)
f6=f'../PropORACLE_main_cp/outputs/{day}/nba/step6a_with_opp_stats.csv'; OPP=dict(pd.read_csv(f6,usecols=['player','opp_team']).dropna().drop_duplicates('player').values) if os.path.exists(f6) else {}
if not OPP:
    _g=bd.dropna(subset=['pp_game_id']).drop_duplicates(['pp_game_id','team'])[['pp_game_id','team']]; _tm=_g.groupby('pp_game_id').team.apply(list)
    OPP={}
    for _r in bd.drop_duplicates('player').itertuples():
        _l=_tm.get(_r.pp_game_id,[]); _o=[t for t in _l if t!=_r.team]
        if _o: OPP[_r.player]=_o[0]
cols=sorted(set(bd.stat)); db=sqlite3.connect('data/cache/proporacle_ref.db')
h=pd.read_sql(f"select player,game_date,minutes,{','.join(cols)} from nba where coalesce(season_window,'')!='preseason'",db)
for c in cols+['minutes']: h[c]=pd.to_numeric(h[c],errors='coerce')
h=h[h.minutes>0].drop_duplicates(['player','game_date']); h['gd']=pd.to_datetime(h.game_date); h=h[h.gd<D0]; h['k']=h.player.map(nm); G={k:v.sort_values('gd') for k,v in h.groupby('k')}
T=table(); AB={'SAS':'SA','GSW':'GS','NOP':'NO','NYK':'NY','UTA':'UTAH','WAS':'WSH','PHO':'PHX','BRK':'BKN'}
def dtier(opp,c):
    o=AB.get(str(opp).upper(),str(opp).upper()); 
    if o not in T.index or c not in T.columns: return ''
    nba=[t for t in T.index if t in set(AB.get(x,x) for x in OPP.values())]; r=T.loc[nba,c].rank(method='first'); 
    if o not in r.index: return ''
    return str(pd.cut(r,5,labels=['Elite','Above avg','Average','Below avg','Weak'])[o])
def feats(v,mn,line,sg):
    hit=(v>line) if sg==1 else (v<line); sd=max(float(np.std(v[-20:])),.3); a={w:v[-w:].mean() if len(v)>=w else np.nan for w in(5,10,15,20)}
    gz=[sg*(a[w]-line) for w in (5,10,20)]; sea=v[-0:] 
    f=dict(h5=hit[-5:].mean(),h10=hit[-10:].mean(),h20=hit[-20:].mean(),hs=hit.mean(),n=len(v),zmin=min(gz)/sd,mn10=mn[-10:].mean())
    for w,k in ((5,'G5'),(10,'G10'),(15,'G15'),(20,'G20')): f[k]=sg*(a[w]-line)/max(line,.5)
    f['GS']=np.nan   # season avg needs 10 games this season; not available yet early in season
    return f
def ok(layer,f):
    L=layer.strip()
    if L=='L5 5/5': return f['h5']>=1
    if L=='L5 4/5+': return f['h5']>=.8
    if L=='L10 70%+': return f['h10']>=.7
    if L=='L10 80%+': return f['h10']>=.8
    if L=='L20 60%+': return f['h20']>=.6
    if L=='L20 70%+': return f['h20']>=.7
    if L.startswith('Season') and 'gap' not in L: return np.nan   # needs season sample
    if L.startswith('SD gap>='): return f['zmin']>=float(L.split('>=')[1])
    if 'gap>=' in L:
        w=L.split()[0]; t=float(L.split('>=')[1].split('%')[0])/100; k={'L5':'G5','L10':'G10','L15':'G15','L20':'G20','Season':'GS'}[w]; v=f[k]; return np.nan if np.isnan(v) else v>=t
    return False
rows=[]
def Frow(r,side,reason,f=None):
    b={'Pick type':r.pt,'Tier':'F','Player':r.player,'Team':r.team,'Opp':OPP.get(r.player,''),'Prop':r.prop_type,'Side':side,'Line':r.line,'Rule':'','Rule test hit%':np.nan,'Rule test n':np.nan,'Status':'UNGATED','Reason':reason,'Board as of':f'{fa:%b %d %H:%M} ET'}
    if f: b.update({'L5':f"{int(round(f['h5']*5))}/5",'L10 hit%':round(f['h10']*100),'L20 hit%':round(f['h20']*100),'Season-to-date hit%':round(f['hs']*100),'Games':f['n'],'SD gap (min)':round(f['zmin'],2),'Avg min L10':round(f['mn10'],1),'Defense':dtier(OPP.get(r.player,''),r.stat),'Defense note':('softer = better for Over' if side=='Over' else 'tougher = better for Under')})
    return b
for r in bd.drop_duplicates(['player','stat','line','pt']).itertuples():
    sides=[('Over',1),('Under',-1)] if r.pt=='Standard' else [('Over',1)]
    if r.pt=='Standard' and (r.player,r.stat) in dk:
        for sd_,_ in sides: rows.append(Frow(r,sd_,'Standard with more than one line (ambiguous)'))
        continue
    if r.pt=='Standard' and r.line<=1.0:
        for sd_,_ in sides: rows.append(Frow(r,sd_,'line 1.0 or lower (too coarse)'))
        continue
    g=G.get(nm(r.player)); 
    if g is None or len(g)<8:
        for sd_,_ in sides: rows.append(Frow(r,sd_,'fewer than 8 games of history'))
        continue
    v=g[r.stat].values.astype(float); mn=g.minutes.values
    for side,sg in (('Over',1),('Under',-1)):
        if r.pt in('Goblin','Demon') and side=='Under': continue
        rule=R[(R['pick']==r.pt)&(R['prop']==r.stat)&(R.side==side)]
        if not len(rule):
            rows.append(Frow(r,side,'group tier C/D (rule did not beat its baseline)' if (r.pt,r.stat,side) in PRES else 'group not tested (not enough data)',feats(v,mn,r.line,sg))); continue
        ru=rule.iloc[0]; f=feats(v,mn,r.line,sg); layers=[] if ru.layers=='(no gate helped)' else ru.layers.split(' > ')
        res=[ok(l,f) for l in layers]; 
        if any((not (isinstance(x,float) and np.isnan(x))) and (not bool(x)) for x in res):
            rows.append(Frow(r,side,'failed its tier rule today',f)); continue
        pend=any(isinstance(x,float) and np.isnan(x) for x in res)
        rows.append({'Pick type':r.pt,'Tier':ru.tier,'Player':r.player,'Team':r.team,'Opp':OPP.get(r.player,''),'Prop':r.prop_type,'Side':side,'Line':r.line,'Rule':ru.layers,'Rule test hit%':ru.test_pct,'Rule test n':ru.test_n,'Status':'needs season sample (10 games)' if pend else 'PASS','Reason':'',
          'L5':f"{int(round(f['h5']*5))}/5",'L10 hit%':round(f['h10']*100),'L20 hit%':round(f['h20']*100),'Season-to-date hit%':round(f['hs']*100),'Games':f['n'],'SD gap (min)':round(f['zmin'],2),'Avg min L10':round(f['mn10'],1),'Defense':dtier(OPP.get(r.player,''),r.stat),'Defense note':('softer = better for Over' if side=='Over' else 'tougher = better for Under'),'Board as of':f'{fa:%b %d %H:%M} ET'})
P=pd.DataFrame(rows)
# --- expected minutes (from game logs) + injury status (data/injuries/injuries_latest.csv) ---
def _mins(pl):
    g=G.get(nm(pl))
    if g is None or not len(g): return (np.nan,np.nan,np.nan,np.nan)
    m=g.minutes.values.astype(float); return (m[-5:].mean(),m[-10:].mean(),m[-5:].min(),m[-5:].max())
_mm={pl:_mins(pl) for pl in P.Player.unique()}
P['Exp min (L5 avg)']=P.Player.map(lambda x:_mm[x][0]); P['Min L10 avg']=P.Player.map(lambda x:_mm[x][1]); P['Min L5 low']=P.Player.map(lambda x:_mm[x][2]); P['Min L5 high']=P.Player.map(lambda x:_mm[x][3])
try:
    _inj=pd.read_csv('../PropORACLE_main_cp/data/injuries/injuries_latest.csv'); _inj=_inj[_inj.sport=='NBA'].copy(); _inj['k']=_inj.player.map(nm)
    _inj=_inj.drop_duplicates('k',keep='first').set_index('k'); _ts=str(_inj.captured_at.max())[:16] if len(_inj) else '?'
    P['Injury status']=P.Player.map(lambda x:_inj.status.get(nm(x),'none listed')); P['Injury note']=P.Player.map(lambda x:str(_inj.injury.get(nm(x),''))[:140])
    P['Injury list as of (UTC)']=_ts
except Exception as e: print('injury attach skipped:',e)
if len(P):
    P['o']=P.Tier.map({'S':0,'A':1,'B':2,'F':3}); P['d']=P.Defense.map({'Elite':0,'Above avg':1,'Average':2,'Below avg':3,'Weak':4}); P['d']=np.where(P.Side=='Over',P.d,4-P.d)
    P=P.sort_values(['Pick type','Side','o','Status','d','SD gap (min)'],ascending=[True,True,True,True,False,False]).drop(columns=['o','d'])
out=f'outputs/tiered_picks_{day}.xlsx'
with pd.ExcelWriter(out) as w:
    pd.DataFrame({'Notes':[f'Board as of {fa:%b %d %H:%M} ET. Rules come from outputs/prop_tiers_cum2 (train before May 9, test after). Tier S/A/B; sorted by tier then defense (Overs: softer first; Unders: tougher first).','Goblin and Standard are separate blocks, never ranked against each other. Standard Overs are watch-only.','Season-based rules show "needs season sample" until players have 10 games this season. Role rank is not shown (needs team ranks).','LOG ONLY until about 100 forward picks per group.']}).to_excel(w,sheet_name='Read me',index=False)
    for (pt,sd),x in (P.groupby(['Pick type','Side']) if len(P) else []): x.drop(columns=['Pick type','Side']).to_excel(w,sheet_name=f'{pt} {sd}'[:31],index=False); w.sheets[f'{pt} {sd}'[:31]].set_column(0,25,16)
    # --- 2026-11-13: every prop that clears the minimum gates, tier F (unverified) when no tier rule backs it ---
    if len(P):
        for (pt,sd),x in P.groupby(['Pick type','Side']):
            l5=pd.to_numeric(x.L5.astype(str).str.split('/').str[0],errors='coerce'); l10=pd.to_numeric(x['L10 hit%'],errors='coerce'); l20=pd.to_numeric(x['L20 hit%'],errors='coerce')
            sea=pd.to_numeric(x['Season-to-date hit%'],errors='coerce') if 'Season-to-date hit%' in x.columns else pd.Series(np.nan,index=x.index)
            ok=(l5>=(5 if pt=='Goblin' else 4))&(l10>=80)&(l20>=75)&(~(sea<60))
            y=x[ok].copy(); y['Tier shown']=np.where(y.Tier.isin(['S','A','B'])&y.Status.eq('PASS'),y.Tier,'F (unverified)')
            y['Gate check']=np.where(sea[ok].isna(),'L5 5/5 (Goblin) or 4/5, L10 8+, L20 15+ pass; season pending','L5 5/5 (Goblin) or 4/5, L10 8+, L20 15+ and season>=60 pass')
            y=y.drop(columns=['Pick type','Side']); nm_=f'Gates {pt} {sd}'[:31]
            if len(y): y.to_excel(w,sheet_name=nm_,index=False); w.sheets[nm_].set_column(0,30,16)
os.makedirs('outputs/ticket_log',exist_ok=True); L='outputs/ticket_log/forward_tracking_tiers.csv'
Q=P[(P.Status=='PASS')&(P.Tier!='F')].assign(date=day,result='',actual='',graded_at='')[['date','Pick type','Tier','Player','Team','Opp','Prop','Side','Line','Rule','result','actual','graded_at']] if len(P) else pd.DataFrame()
if len(Q):
    if os.path.exists(L):
        old=pd.read_csv(L); k=['date','Player','Prop','Side','Line','Pick type']; Q=Q[~Q.set_index(k).index.isin(old.set_index(k).index)]; Q=pd.concat([old,Q],ignore_index=True)
    Q.to_csv(L,index=False)
print(out,len(P),P.groupby(['Pick type','Side','Tier','Status']).size().to_dict() if len(P) else '')
