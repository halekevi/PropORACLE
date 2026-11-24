import _r2
import sys,re,unicodedata,pandas as pd
D=sys.argv[1]; base='../PropORACLE_main_cp/outputs/%s/'%D
def nm(s):
    s=unicodedata.normalize('NFKD',str(s)).encode('ascii','ignore').decode().lower()
    s=re.sub(r"[.'’,]",'',s); s=re.sub(r'\b(jr|sr|ii|iii|iv)\b','',s); return re.sub(r'\s+',' ',s).strip()
def acts(sp):
    a=pd.read_csv(base+f'actuals_{sp}_{D}.csv'); a=a[~a.player.astype(str).str.contains(r'\+')].copy(); a['k']=a.player.map(nm)
    return a.drop_duplicates(['k','prop_type']).set_index(['k','prop_type'])['actual']
A={'NBA':acts('nba'),'CBB':acts('cbb')}
x=pd.ExcelFile('outputs/all_props_%s_0413.xlsx'%D)
rows=[]
for s in x.sheet_names:
    if not (s.startswith('NBA') or s.startswith('CBB')): continue
    sp=s[:3]; d=x.parse(s); d['sheet']=s; d['sport']=sp
    d['actual']=[A[sp].get((nm(p),pr)) for p,pr in zip(d.Player,d.Prop)]
    d['pick']=s[4:]; rows.append(d)
d=pd.concat(rows)
d['Line']=pd.to_numeric(d.Line,errors='coerce')
d['res']=None
ok=d.actual.notna()&d.Line.notna()
ov=d.Side.astype(str).str.lower().str.startswith('o')
d.loc[ok,'res']=['PUSH' if a==l else ('HIT' if ((a>l) if o else (a<l)) else 'MISS') for a,l,o in zip(d.actual[ok],d.Line[ok],ov[ok])]
d.to_pickle('/tmp/graded_all.pkl')
print(list(d.columns))
g=d[d.res.isin(['HIT','MISS'])].groupby(['sport','pick']).res.agg(n='count',hits=lambda v:(v=='HIT').sum()); g['hit%']=(100*g.hits/g.n).round(1)
g['board']=d.groupby(['sport','pick']).size(); g['no_result']=g.board-g.n
print(g)
