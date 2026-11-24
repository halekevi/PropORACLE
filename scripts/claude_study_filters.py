import sqlite3,numpy as np,pandas as pd,warnings; warnings.filterwarnings('ignore')
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql('select * from nba',c); n['gd']=pd.to_datetime(n.game_date); n['player']=n.player.str.lower().str.strip()
n=n[pd.to_numeric(n.minutes,errors='coerce')>0].drop_duplicates(['event_id','player']).sort_values(['player','gd'])
d=pd.read_pickle('/tmp/study_ds2.pkl'); d['sdate']=pd.to_datetime(d.slate_date)
cols=sorted(d.col.unique())
frames=[]
for col in cols:
    x=pd.to_numeric(n[col],errors='coerce'); g=x.groupby(n.player)
    n['m']=g.transform(lambda s:s.shift(1).rolling(20,min_periods=12).mean())
    n['s']=g.transform(lambda s:s.shift(1).rolling(20,min_periods=12).std())
    f=n[['player','gd','m','s']].dropna().copy(); f['col']=col; f['cv20']=f.s/f.m.clip(lower=0.3)
    frames.append(f[['player','gd','col','cv20']])
C=pd.concat(frames)
# cv for the game ON that date uses shift(1) => row for game on slate date holds prior-20 cv
d=d.merge(C,left_on=['k','sdate','col'],right_on=['player','gd','col'],how='left',suffixes=('','_c'))
d=d[d.cv20.notna()].copy()
d['cvpct']=d.groupby(['pt','prop_canon']).cv20.rank(pct=True)
# players steadiest-stat rank within player/date among their stats
d['rk_in_player']=d.groupby(['k','sdate','pt']).cv20.rank(method='min')
# prior hit rate for same player/prop/pt/side (expanding, excludes current date)
d=d.sort_values('sdate'); key=['k','prop_canon','pt','side']
grp=d.groupby(key).hit
d['ph_n']=grp.cumcount()
d['ph']=grp.transform(lambda s:s.shift(1).expanding().mean())
d['clean']=(d.n>=20)&(d.mn10>=20)&(d.mnfl5>=12)&(d.mnsd<=6)&(d.mntrend>-4)&(d.dsl<=7)&(d.line>=1.5)
d.to_pickle('/tmp/study_ds3.pkl')
def rep(y,mask,name):
    a=y[mask]; h=a[a.ho]
    return f'{name:46s} all {a.hit.mean()*100:5.1f}% (n={len(a):5d}) | hold {h.hit.mean()*100 if len(h) else float("nan"):5.1f}% (n={len(h):4d})'
for pt in ['Goblin','Standard','Demon']:
    y=d[d.pt==pt]
    if pt=='Goblin': gate=(y.h5>=1)&(y.h10>=.9)&(y.h20>=.8)
    else: gate=(y.h5>=.8)&(y.h10>=.8)&(y.h20>=.65)
    print(f'\n=== {pt}  base all {y.hit.mean()*100:.1f}% hold {y[y.ho].hit.mean()*100:.1f}%')
    for side in ([1] if pt!='Standard' else [1,-1]):
        z=y[y.side==side]; gz=gate[z.index]
        tag='Over' if side==1 else 'Under'
        print(f' -- {tag}')
        print('  '+rep(z,z.index==z.index,'all props'))
        print('  '+rep(z,gz,'consistency gate'))
        print('  '+rep(z,gz&(z.cvpct<=.33),'gate + steadiest third for that prop'))
        print('  '+rep(z,gz&(z.cvpct<=.33)&(z.rk_in_player<=4),'  + among player\'s 4 steadiest stats'))
        print('  '+rep(z,gz&(z.ph>=.7)&(z.ph_n>=6),'gate + player prior hit rate>=70% (n>=6)'))
        print('  '+rep(z,gz&(z.cvpct<=.33)&(z.ph>=.7)&(z.ph_n>=6),'gate + steady third + player prior>=70%'))
        print('  '+rep(z,gz&(z.cvpct<=.33)&(z.ph>=.7)&(z.ph_n>=6)&z.clean,'  + noise filter'))
        print('  '+rep(z,gz&(z.cvpct<=.33)&z.clean,'gate + steady third + noise filter'))
