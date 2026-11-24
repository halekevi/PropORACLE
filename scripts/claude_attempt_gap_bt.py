import pandas as pd,numpy as np,sqlite3,sys,unicodedata
sys.path.insert(0,'scripts')
def nm(s): return ''.join(c for c in unicodedata.normalize('NFD',str(s)) if not unicodedata.combining(c)).lower().replace('.','').replace(' jr','').replace(' iii','').replace(' ii','').strip()
M={'FG Attempted':'fga','FG Made':'fgm','Free Throws Attempted':'fta','Free Throws Made':'ftm','Two Pointers Attempted':'fg2a','Two Pointers Made':'fg2m','3-PT Attempted':'fg3a'}
g=pd.read_csv('data/training/graded_export_20260516.csv',low_memory=False)
g=g[(g.sport=='NBA')&g.prop.isin(M)&g.result_binary.isin([0,1])&g.void_reason.isna()].copy()
g['stat']=g.prop.map(M); g['d']=pd.to_datetime(g.file_date); g['k']=g.player.map(nm)
c=sqlite3.connect('data/cache/proporacle_ref.db')
h=pd.read_sql("select player,game_date,minutes,fga,fgm,fta,ftm,fg2a,fg2m,fg3a from nba where coalesce(season_window,'')!='preseason'",c)
h['minutes']=pd.to_numeric(h.minutes,errors='coerce'); h=h[h.minutes>0].drop_duplicates(['player','game_date']); h['gd']=pd.to_datetime(h.game_date); h['k']=h.player.map(nm)
G={k:v.sort_values('gd') for k,v in h.groupby('k')}
rows=[]
for r in g.itertuples():
    x=G.get(r.k)
    if x is None: continue
    x=x[x.gd<r.d]; v=pd.to_numeric(x[r.stat],errors='coerce').dropna().values
    if len(v)<5: continue
    l5=v[-5:]; med=np.median(l5); thr=max(3,0.5*med); out=int((abs(l5-med)>=thr).sum())
    gap=l5.mean()-r.line; side='Over' if gap>0 else 'Under'
    if gap==0: continue
    win=int(r.result_binary) if side==r.direction else None
    # outcome from actual value
    a=r.actual_value
    if pd.isna(a) or a==r.line: continue
    hit=int((a>r.line) if side=='Over' else (a<r.line))
    rows.append(dict(prop=r.prop,pick=r.pick_type,side=side,rel=abs(gap)/r.line,out=min(out,2),hit=hit))
D=pd.DataFrame(rows); D['gapbin']=pd.cut(D.rel,[0,.1,.2,.35,10],labels=['<10%','10-20%','20-35%','35%+'])
D['outs']=D.out.map({0:'0 outliers',1:'1 outlier',2:'2+ outliers'})
print(len(D))
for pk in ['Standard','Goblin','Demon']:
    x=D[D.pick==pk]
    if len(x)<30: print(pk,len(x)); continue
    print('\n',pk,len(x),'overall',round(x.hit.mean()*100,2))
    print((x.pivot_table(index='gapbin',columns='outs',values='hit',aggfunc=['mean','size'],observed=True)).round(4).to_string())
x=D[D.pick=='Standard']; print('\nStandard by side'); print(x.groupby(['side']).hit.agg(['mean','size']).round(4))
print('\nStandard, gap>=20%, <=1 outlier by prop'); y=x[(x.rel>=.2)&(x.out<=1)]; print(y.groupby('prop').hit.agg(['mean','size']).round(4)); print(y.groupby('side').hit.agg(['mean','size']).round(4))
D.to_csv('outputs/attempt_gap_bt.csv',index=False)
