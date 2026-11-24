"""Cumulative gate stacking per prop (same process as FGA): L1 best hit-rate gate -> L2 best gap (L5/L10/L15/L20/season, as % of line) -> L3 best SD gap. Chosen on TRAIN (<2026-05-09), scored on TEST. Defense/minutes/role = ranking info only."""
import pandas as pd,numpy as np,sqlite3,sys,warnings; warnings.filterwarnings('ignore'); sys.path.insert(0,'scripts')
from claude_gamelog import nm
from claude_defense_rel import table
T=table(); AB={'SAS':'SA','GSW':'GS','NOP':'NO','NYK':'NY','UTA':'UTAH','WAS':'WSH','PHO':'PHX','BRK':'BKN'}
d=pd.read_pickle('/tmp/role_ds.pkl'); d=d[d.hit.notna()&d.pt.isin(['Standard','Goblin','Demon'])&((d.pt=='Standard')|(d.side==1))].copy().reset_index(drop=True)
d['te']=d.sdate>=pd.Timestamp('2026-05-09'); d['S']=np.where(d.side==1,'Over','Under'); d['k2']=d.player.map(nm)
cols=sorted(d.col.unique()); db=sqlite3.connect('data/cache/proporacle_ref.db')
h=pd.read_sql(f"select player,game_date,minutes,{','.join(cols)} from nba where coalesce(season_window,'')!='preseason'",db)
for c in cols+['minutes']: h[c]=pd.to_numeric(h[c],errors='coerce')
h=h[h.minutes>0].drop_duplicates(['player','game_date']); h['gd']=pd.to_datetime(h.game_date); h['k2']=h.player.map(nm); h=h.sort_values(['k2','gd'])
for w in ('a5','a10','a15','a20','aS'): d[w]=np.nan
grp={k:v for k,v in h.groupby('k2')}
for (k,c),ix in d.groupby(['k2','col']).groups.items():
    g=grp.get(k)
    if g is None: continue
    v=g[c].values.astype(float); ok=~np.isnan(v); dates=g.gd.values[ok]; v=v[ok]; cs=np.concatenate([[0],np.cumsum(v)])
    sd_=d.loc[ix,'sdate'].values; idx=np.searchsorted(dates,sd_,side='left')
    for w,n in (('a5',5),('a10',10),('a15',15),('a20',20)):
        d.loc[ix,w]=np.where(idx>=n,(cs[idx]-cs[np.maximum(idx-n,0)])/n,np.nan)
    ss=np.where(sd_<np.datetime64('2026-08-01'),np.datetime64('2025-10-01'),np.datetime64('2026-10-01')); i0=np.searchsorted(dates,ss,side='left'); m=idx-i0
    d.loc[ix,'aS']=np.where(m>=10,(cs[idx]-cs[np.minimum(i0,idx)])/np.maximum(m,1),np.nan)
for w in ('a5','a10','a15','a20','aS'): d['G'+w[1:]]=d.side*(d[w]-d.line)/d.line.clip(lower=.5)   # gap as fraction of line
nba=[AB.get(o,o) for o in d.opp.dropna().unique()]; tierof={}
for c in cols:
    if c in T.columns: r=T.loc[[t for t in nba if t in T.index],c].rank(method='first'); tierof[c]=r<=len(r)/5
dt={}
for c in cols:
    if c in T.columns:
        r=T.loc[[t for t in nba if t in T.index],c].rank(method='first'); dt[c]=pd.cut(r,5,labels=['Elite','Above avg','Average','Below avg','Weak']).astype(str)
d['dtier']=[dt[c].get(AB.get(o,o),np.nan) if c in dt else np.nan for o,c in zip(d.opp,d.col)]
d['mbin']=pd.cut(d.mn10,[0,24,31,36,99],labels=['<24 min','24-31','31-36','36+'],right=False).astype(str)
d['rbin']=pd.cut(d.trank,[0,2,4,99],labels=['top 1-2','3-4','5+']).astype(str)
d['elite']=[bool(tierof[c].get(AB.get(o,o),False)) if c in tierof else False for o,c in zip(d.opp,d.col)]
H={'L5 5/5':lambda x:x.h5>=1,'L5 4/5+':lambda x:x.h5>=.8,'L10 70%+':lambda x:x.h10>=.7,'L10 80%+':lambda x:x.h10>=.8,'L20 60%+':lambda x:x.h20>=.6,'L20 70%+':lambda x:x.h20>=.7,'Season 55%+':lambda x:(x.hs>=.55)&(x.n>=20),'Season 65%+':lambda x:(x.hs>=.65)&(x.n>=20)}
GP={}
for w,nmw in (('5','L5'),('10','L10'),('15','L15'),('20','L20'),('S','Season')):
    for t in (.05,.10,.20): GP[f'{nmw} gap>={int(t*100)}% of line']=(lambda w,t:(lambda x:x['G'+w]>=t))(w,t)
Z={'SD gap>=0.5':lambda x:x.zmin>=.5,'SD gap>=1':lambda x:x.zmin>=1}
hr=lambda x:(x.hit.mean()*100,len(x)) if len(x) else (np.nan,0)
def pick(tr,cands,cur_r):
    best=None
    for n_,f in cands.items():
        m=tr[f(tr)]; r,n=hr(m)
        if n>=30 and r>cur_r+1.0 and (best is None or r>best[0]): best=(r,n,n_)
    return best
rows=[]
for (pt,col,S),x in d.groupby(['pt','col','S']):
    tr=x[~x.te]; te=x[x.te]
    if len(tr)<80 or len(te)<30: continue
    cur=tr; r0=hr(tr)[0]; chain=[]; fs=[]; steps=[round(r0,1)]
    for cands in (H,GP,Z):
        b=pick(cur,cands,hr(cur)[0])
        if b: chain.append(b[2]); fs.append(cands[b[2]]); cur=cur[cands[b[2]](cur)]; steps.append(round(b[0],1))
    g=te
    for f in fs: g=g[f(g)]
    gall=x
    for f in fs: gall=gall[f(gall)]
    rt,nt=hr(cur); re,ne=hr(g); r0t,n0t=hr(te); ge=gall[gall.elite]; gn=gall[~gall.elite]
    tier='D'
    if nt>=30 and ne>=15:
        mn=min(rt,re); tier='S' if mn>=65 else 'A' if mn>=60 else 'B' if mn>=55 else 'C' if mn>=50 else 'D'
    info={}
    for dim,nm_ in (('dtier','defense'),('mbin','minutes'),('rbin','role')):
        tb=gall[~gall.te].groupby(dim).hit.agg(['mean','size']); tb=tb[(tb['size']>=12)&(tb.index!='nan')]
        pooled=gall.groupby(dim).hit.agg(['mean','size'])
        info[nm_+'_all']=' | '.join(f"{k} {pooled.loc[k,'mean']*100:.0f}% ({int(pooled.loc[k,'size'])})" for k in pooled.index if k!='nan')
        if len(tb):
            bk=tb['mean'].idxmax(); tt=gall[gall.te&(gall[dim]==bk)]; info[nm_+'_best_in_train']=bk; info[nm_+'_best_test']=f"{hr(tt)[0]:.0f}% ({len(tt)})" if len(tt) else ''
    lo=gall[gall.h5<=.6]; hi=gall[gall.h5>=.8]
    info['L5_3of5_or_lower']=f"{hr(lo)[0]:.0f}% ({len(lo)})" if len(lo) else ''; info['L5_4of5_or_better']=f"{hr(hi)[0]:.0f}% ({len(hi)})" if len(hi) else ''; info['L5_low_share']=round(len(lo)/max(len(gall),1)*100)
    rows.append(dict(**info,train_base=round(r0,1),pick=pt,prop=col,side=S,layers=' > '.join(chain) if chain else '(no gate helped)',train_steps='>'.join(map(str,steps)),train_pct=round(rt,1),train_n=nt,test_pct=round(re,1),test_n=ne,test_ungated_pct=round(r0t,1),tier=tier,thin='thin' if ne<30 else '',elite_D_pct=round(hr(ge)[0],1),elite_D_n=len(ge),other_D_pct=round(hr(gn)[0],1)))
R=pd.DataFrame(rows); R['o']=R.tier.map({'S':0,'A':1,'B':2,'C':3,'D':4}); R=R.sort_values(['pick','side','o','test_pct'],ascending=[False,True,True,False]).drop(columns='o')
R.to_csv('outputs/prop_tiers_cum4_2026-11-11.csv',index=False); pd.set_option('display.width',300,'display.max_colwidth',90)
R['lift_train']=(R.train_pct-R.train_base).round(1); R['lift_test']=(R.test_pct-R.test_ungated_pct).round(1)
def lt(r):
    if r.train_n<30 or r.test_n<15: return 'D'
    m=min(r.lift_train,r.lift_test); return 'S' if m>=12 else 'A' if m>=8 else 'B' if m>=4 else 'C' if m>=0 else 'D'
R['tier_hitrate_old']=R.tier; R['tier']=R.apply(lt,axis=1)
R.to_csv('outputs/prop_tiers_cum4_2026-11-11.csv',index=False)
print('ok')
