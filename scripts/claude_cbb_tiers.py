import _r2
"""CBB: stacked-gate tiers per prop and pick type (same process as NBA), then apply to today's CBB board. Usage: python3 scripts/claude_cbb_tiers.py YYYY-MM-DD
Data: ledger legs Feb 24-Apr 6 2026 (/tmp/cbb_ds.pkl from claude_cbb_test.py). Train < Mar 12, test >= Mar 12. No defense/minutes/role for CBB."""
import pandas as pd,numpy as np,sqlite3,sys,os,itertools,warnings; warnings.filterwarnings('ignore'); sys.path.insert(0,'scripts')
from claude_gamelog import nm
day=sys.argv[1]; D0=pd.Timestamp(day)
CC=['pts','reb','ast','stl','blk','tov','fg3m']
g1=pd.read_parquet('data/game_logs/cbb_player_games.parquet')[['player','game_date','minutes']+CC]
g2=pd.read_sql(f"select player,game_date,minutes,{','.join(CC)} from cbb",sqlite3.connect('data/cache/proporacle_ref.db'))
h=pd.concat([g1,g2],ignore_index=True)
for c in CC+['minutes']: h[c]=pd.to_numeric(h[c],errors='coerce')
h=h[h.minutes>0].drop_duplicates(['player','game_date']); h['gd']=pd.to_datetime(h.game_date); h['k']=h.player.map(nm)
h['pra']=h.pts+h.reb+h.ast; h['pr']=h.pts+h.reb; h['pa']=h.pts+h.ast; h['ra']=h.reb+h.ast; h['bs']=h.blk+h.stl
G={k:v.sort_values('gd') for k,v in h.groupby('k')}
def avgs(v,dates,sd,idx=None):
    pass
d=pd.read_pickle('/tmp/cbb_ds.pkl').reset_index(drop=True); d['sdate']=pd.to_datetime(d.slate_date); d['te']=d.sdate>=pd.Timestamp('2026-03-12'); d['S']=np.where(d.side==1,'Over','Under')
for w in ('G5','G10','G15','G20','GS','hs'): d[w]=np.nan
for i,r in enumerate(d.itertuples()):
    g=G.get(r.k)
    if g is None or r.col not in g: continue
    gg=g[g.gd<r.sdate]; v=gg[r.col].values.astype(float); 
    if len(v)<5: continue
    ss=pd.Timestamp('2025-11-01') if r.sdate<pd.Timestamp('2026-08-01') else pd.Timestamp('2026-10-01'); sv=v[gg.gd.values>=np.datetime64(ss)]
    base=max(r.line,.5)
    for w,n in (('G5',5),('G10',10),('G15',15),('G20',20)):
        if len(v)>=n: d.at[i,w]=r.side*(v[-n:].mean()-r.line)/base
    if len(sv)>=10:
        d.at[i,'GS']=r.side*(sv.mean()-r.line)/base; d.at[i,'hs']=((sv>r.line) if r.side==1 else (sv<r.line)).mean()
H={'L5 5/5':lambda x:x.h5>=1,'L5 4/5+':lambda x:x.h5>=.8,'L10 70%+':lambda x:x.h10>=.7,'L10 80%+':lambda x:x.h10>=.8,'L20 60%+':lambda x:x.h20>=.6,'L20 70%+':lambda x:x.h20>=.7,'Season 55%+':lambda x:x.hs>=.55,'Season 65%+':lambda x:x.hs>=.65}
GP={}
for w,nmw in (('5','L5'),('10','L10'),('15','L15'),('20','L20'),('S','Season')):
    for t in (.05,.10,.20): GP[f'{nmw} gap>={int(t*100)}% of line']=(lambda w,t:(lambda x:x['G'+w]>=t))(w,t)
Z={'SD gap>=0.5':lambda x:x.zmin>=.5,'SD gap>=1':lambda x:x.zmin>=1}
hr=lambda x:(x.hit.mean()*100,len(x)) if len(x) else (np.nan,0)
def pick(tr,cands,cur_r):
    best=None
    for n_,f in cands.items():
        r,n=hr(tr[f(tr).fillna(False)]) if True else (0,0)
        if n>=30 and r>cur_r+1.0 and (best is None or r>best[0]): best=(r,n,n_)
    return best
rows=[]
for (pt,col,S),x in d.groupby(['pt','col','S']):
    tr=x[~x.te]; te=x[x.te]
    if len(tr)<60 or len(te)<30: continue
    cur=tr; r0=hr(tr)[0]; chain=[]; fs=[]
    for cands in (H,GP,Z):
        b=pick(cur,cands,hr(cur)[0])
        if b: chain.append(b[2]); fs.append(cands[b[2]]); cur=cur[cands[b[2]](cur).fillna(False)]
    g=te
    for f in fs: g=g[f(g).fillna(False)]
    rt,nt=hr(cur); re,ne=hr(g); r0t,n0=hr(te)
    m=min(rt-r0,re-r0t) if nt>=30 and ne>=15 else -99
    tier='D' if m==-99 else 'S' if m>=12 else 'A' if m>=8 else 'B' if m>=4 else 'C' if m>=0 else 'D'
    rows.append(dict(pick=pt,prop=col,side=S,layers=' > '.join(chain) if chain else '(no gate helped)',train_pct=round(rt,1),train_n=nt,train_base=round(r0,1),test_pct=round(re,1),test_n=ne,test_ungated_pct=round(r0t,1),lift_test=round(re-r0t,1),tier=tier,thin='thin' if ne<30 else ''))
R=pd.DataFrame(rows); allg={('Standard',c,s) for c in d.col.unique() for s in('Over','Under')}|{(p,c,'Over') for c in d.col.unique() for p in('Goblin','Demon')}
Fm=pd.DataFrame([dict(pick=p,prop=c,side=s,layers='untested (not enough data)',tier='F') for p,c,s in sorted(allg-set(zip(R['pick'],R['prop'],R.side)))])
Rall=pd.concat([R,Fm],ignore_index=True); Rall.to_csv('outputs/prop_tiers_cbb_2026-11-11.csv',index=False)
pd.set_option('display.width',250); print(Rall.groupby(['pick','side']).tier.value_counts().unstack(fill_value=0)); print(Rall[Rall.tier.isin(['S','A'])][['pick','prop','side','layers','train_pct','test_pct','test_n','test_ungated_pct','lift_test','tier']].to_string(index=False))
