import pandas as pd,numpy as np,itertools,warnings; warnings.filterwarnings('ignore')
d=pd.read_pickle('/tmp/posdef_ds2.pkl'); d=d[d.hit.notna()].copy(); d['hit']=d.hit.astype(float)
d['sdate']=pd.to_datetime(d.slate_date); d['gm']=d.apply(lambda r:'|'.join(sorted([str(r.team),str(r.opp)])),axis=1)
def pairs(df,name):
    p0=df.hit.mean(); rows=[]
    for dt,g in df.groupby('sdate'):
        g=g.drop_duplicates(['k','prop_canon']).reset_index(drop=True)
        if len(g)>150: g=g.sample(150,random_state=1)
        a=g.to_dict('records')
        for x,y in itertools.combinations(a,2):
            if x['k']==y['k']: continue
            rel='same game, same team' if (x['gm']==y['gm'] and x['team']==y['team']) else ('same game, opposite teams' if x['gm']==y['gm'] else 'different games')
            rows.append((rel,x['side']==y['side'],x['hit']*y['hit'],x['hit'],y['hit']))
    r=pd.DataFrame(rows,columns=['rel','sameside','both','a','b'])
    print(f'\n{name}: base leg {p0*100:.1f}%  (pairs {len(r)})')
    for rel,g in r.groupby('rel'):
        exp=(g.a*g.b).mean()  # independence expectation using pair-specific
        pa=g.a.mean();pb=g.b.mean()
        print(f'  {rel:28s} n={len(g):7d} both hit {g.both.mean()*100:5.1f}% vs independent {pa*pb*100:5.1f}%  lift {g.both.mean()/(pa*pb):.2f}')
S=d[d.pt=='Standard']
pairs(S[(S.side==-1)],'STANDARD UNDER (all)')
pairs(S[(S.side==-1)&(S.h10>=.7)&(S.h20>=.6)],'STANDARD UNDER (L10>=70, L20>=60)')
pairs(S[(S.side==1)],'STANDARD OVER (all)')
pairs(S[(S.side==1)&(S.h10>=.7)&(S.h20>=.6)],'STANDARD OVER (L10>=70, L20>=60)')
G=d[d.pt=='Goblin']
pairs(G[(G.h5>=1)&(G.h10>=.9)&(G.h20>=.8)],'GOBLIN gated')
