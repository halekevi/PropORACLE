"""python scripts/claude_fetch_compare.py <day>   (after the day's games are in the DB)
Grades the FIRST and LAST saved fetch of that day separately, appends to outputs/fetch_ab_log.csv (replacing that day),
and prints the pooled hit rate over all logged days + this day's same-line vs moved-line split."""
import sys,glob,subprocess,pandas as pd
D=sys.argv[1]
sn=sorted(glob.glob(f'outputs/tiered_all_{D}_snap_*.xlsx'),key=lambda f:(int(f.split('snap_')[1][:4])<1200,int(f.split('snap_')[1][:4]))); tags=[s.split(D)[1].replace('.xlsx','') for s in sn]
if len(tags)<2: sys.exit(f'need 2+ snapshots for {D}, have {tags}')
pair={'EARLY':tags[0],'LATE':tags[-1]}; G={}
for k,t in pair.items():
    subprocess.run([sys.executable,'scripts/claude_grade_day.py',D,t],check=True,stdout=subprocess.DEVNULL); G[k]=pd.read_csv(f'outputs/graded_ours_{D}{t}.csv')
rows=[]
for k,df in G.items():
    for scope,f in (('ALL',df[~df.gates]),('GATE PASS',df[df.gates])):
        g=f[f.Result.isin(['hit','miss'])]
        for (p,s),v in g.groupby(['pick','side']): rows.append(dict(day=D,fetch=k,scope=scope,pick=p,side=s,n=len(v),hits=int((v.Result=='hit').sum())))
R=pd.DataFrame(rows); L='outputs/fetch_ab_log.csv'
try: old=pd.read_csv(L); old=old[old.day!=D]; R=pd.concat([old,R])
except FileNotFoundError: pass
R.to_csv(L,index=False)
P=R.groupby(['scope','pick','side','fetch'])[['n','hits']].sum(); P['hit%']=(P.hits/P.n*100).round(2)
print('Days logged:',sorted(R.day.unique())); print(P.to_string())
k=['Player','Prop','pick','side']; I=G['EARLY'][~G['EARLY'].gates]; Lt=G['LATE'][~G['LATE'].gates]
M=I.merge(Lt,on=k,suffixes=('_e','_l'),how='outer',indicator=True)
M['cat']=M.apply(lambda r:'removed later' if r._merge=='left_only' else 'new later' if r._merge=='right_only' else 'same line' if r.Line_e==r.Line_l else 'line moved',axis=1)
for c,g in M.groupby('cat'):
    for sd in ('_e','_l'):
        r=g['Result'+sd].value_counts(); n=r.get('hit',0)+r.get('miss',0)
        if n: print(f'{c:14s} {"EARLY" if sd=="_e" else "LATE":6s} n={n} hit%={r.get("hit",0)/n*100:.2f}')
