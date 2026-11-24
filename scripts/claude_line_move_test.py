import sqlite3,pandas as pd,glob,numpy as np
L=pd.concat([pd.read_parquet(f,columns=['sport','player','prop','line','direction','pick_type','slate_date','actual_value','hit']) for f in glob.glob('data/ledger/legs/slate_date=*/*')])
L=L[L.actual_value.notna()&(L.pick_type.str.lower()=='standard')]
L['slate_date']=L.slate_date.astype(str); L['dir']=L.direction.str.upper().str[:1]
c=sqlite3.connect('data/line_history.db')
h=pd.read_sql("select sport,player,prop_type as prop,line,fetched_at,game_date as slate_date from line_history where lower(pick_type)='standard' and game_date>='2026-05-01'",c)
h=h.sort_values('fetched_at'); g=h.groupby(['sport','player','prop','slate_date']).line
M=pd.DataFrame({'open':g.first(),'close':g.last(),'nsnap':g.size(),'nlines':g.nunique()}).reset_index()
x=L.merge(M,on=['sport','player','prop','slate_date'])
x=x[x.nsnap>=2].copy(); print('graded standard legs with >=2 snapshots:',len(x),x.sport.value_counts().to_dict())
x['moved']=(x.open!=x.close); print('moved share',x.moved.mean().round(3))
def hit(line): 
    return np.where(x.actual_value==line,np.nan,np.where(x['dir']=='O',x.actual_value>line,x.actual_value<line)).astype(float)
x['hit_open']=hit(x.open); x['hit_close']=hit(x.close)
x['move']=np.sign(x.close-x.open)  # +1 line went up
rng=np.random.default_rng(1)
def rep(s,dts):
    s=s.dropna()
    if len(s)<40: return f'n={len(s)}'
    d=pd.DataFrame({'h':s,'d':dts.loc[s.index]}).groupby('d').h.agg(['sum','count']); a=d['sum'].values; b=d['count'].values
    bs=[a[i].sum()/b[i].sum() for i in (rng.integers(0,len(a),len(a)) for _ in range(500))]
    return f'{100*s.mean():.1f}% n={len(s)} ({100*np.percentile(bs,2.5):.0f}-{100*np.percentile(bs,97.5):.0f})'
for sp in ['ALL','NBA','MLB','SOCCER','WNBA']:
    y=x if sp=='ALL' else x[x.sport==sp]
    if len(y)<100: continue
    print('\n##',sp,len(y))
    print(' all legs  graded at OPEN :',rep(y.hit_open,y.slate_date)); print(' all legs  graded at CLOSE:',rep(y.hit_close,y.slate_date))
    m=y[y.moved]
    print(' moved legs: at OPEN',rep(m.hit_open,m.slate_date),'| at CLOSE',rep(m.hit_close,m.slate_date))
    for side in ('O','U'):
        for mv,lab in ((1,'line rose'),(-1,'line fell')):
            z=m[(m['dir']==side)&(m.move==mv)]
            print(f'  {side} {lab}: open {rep(z.hit_open,z.slate_date)} | close {rep(z.hit_close,z.slate_date)}')
