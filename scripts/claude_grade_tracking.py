import _r2
import sys,re,unicodedata,datetime as dt,pandas as pd
D=sys.argv[1]; base='../PropORACLE_main_cp/outputs/%s/'%D
def nm(s):
    s=unicodedata.normalize('NFKD',str(s)).encode('ascii','ignore').decode().lower()
    s=re.sub(r"[.'’,]",'',s); s=re.sub(r'\b(jr|sr|ii|iii|iv)\b','',s); return re.sub(r'\s+',' ',s).strip()
def load(sp):
    a=pd.read_csv(base+f'actuals_{sp}_{D}.csv'); a=a[~a.player.astype(str).str.contains(r'\+')].copy()
    a['k']=a.player.map(nm); return a.drop_duplicates(['k','prop_type']).set_index(['k','prop_type'])['actual']
nba=load('nba'); cbb=load('cbb')
cmap={'pts':'Points','reb':'Rebounds','ast':'Assists','pr':'Pts+Rebs','pra':'Pts+Rebs+Asts','ra':'Rebs+Asts','stl':'Steals','blk':'Blocked Shots','3pm':'3-PT Made'}
now=dt.datetime.now().strftime('%Y-%m-%d %H:%M')
def grade(f,acts,propcol,sidecol='side',tf=lambda x:x):
    p='outputs/ticket_log/'+f; d=pd.read_csv(p,dtype={'result':object,'graded_at':object})
    n=0
    for i,r in d.iterrows():
        if str(r['date'])!=D: continue
        pl=r['player'] if 'player' in d.columns else r['Player']; v=acts.get((nm(pl),tf(r[propcol])))
        if v is None or pd.isna(v): d.loc[i,'result']='NO ACTUAL'; d.loc[i,'graded_at']=now; continue
        L=float(r['line'] if 'line' in d.columns else r['Line']); s=str(r[sidecol]).lower()
        res='PUSH' if v==L else ('HIT' if ((v>L) if s.startswith('o') else (v<L)) else 'MISS')
        d.loc[i,'result']=res; d.loc[i,'actual']=v; d.loc[i,'graded_at']=now; n+=1
    d.to_csv(p,index=False); return d[d['date'].astype(str)==D]
t=grade('forward_tracking.csv',nba,'prop')
c=grade('forward_tracking_cbb.csv',cbb,'stat',tf=lambda x:cmap.get(str(x).lower(),x))
r=grade('forward_tracking_tiers.csv',nba,'Prop','Side')
def summ(df,keys):
    g=df[df.result.isin(['HIT','MISS'])].groupby(keys).result.agg(n='count',hits=lambda x:(x=='HIT').sum()); g['hit%']=(100*g.hits/g.n).round(1)
    g['no_actual']=df[df.result=='NO ACTUAL'].groupby(keys).size(); return g.fillna(0)
pd.set_option('display.width',200,'display.max_rows',100)
print('NBA T-groups\n',summ(t,['group']),'\n\nCBB groups\n',summ(c,['group']),'\n\nTiers (NBA)\n',summ(r,['Pick type','Tier']))
