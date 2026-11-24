"""Standard top-20 per sport from main_cp step8 files (Nov 5+). Both sides scored from L5/L10 over/under counts. Rank: misses (L10 misses + L5 misses), L5 hits, L10 pct. Rows failing the gate are tagged, not dropped. Caps: 2/player, 5/prop."""
import pickle,pandas as pd,numpy as np,re
M=pickle.load(open('/tmp/M.pkl','rb'))
def g(d,*names):
    for n in names:
        if n in d.columns: return pd.to_numeric(d[n],errors='coerce')
    return pd.Series(np.nan,index=d.index)
out={}
for k in ['nhl','tennis','cbb','golf','soccer']:
    d=M[k]; d=d[d.pt.astype(str).str.lower()=='standard'].copy()
    o5=g(d,'L5 Over','l5_over','last5_over'); u5=g(d,'L5 Under','l5_under','last5_under')
    o10=g(d,'L10 Over','l10_over'); u10=g(d,'L10 Under','l10_under'); n10=g(d,'L10 Games','l10_games_played')
    rows=[]
    for side,h5,h10 in [('OVER',o5,o10),('UNDER',u5,u10)]:
        t=d.copy(); t['side']=side; t['h5']=h5; t['h10']=h10; t['n10']=n10
        t['edge_pick']=t.edge*(1 if side=='OVER' else -1)
        rows.append(t)
    t=pd.concat(rows); t=t[t.h5.notna()&t.h10.notna()&(t.n10>=5)]
    t['n10']=t.n10.clip(upper=10)
    t['miss']=(t.n10-t.h10.clip(upper=t.n10))+(5-t.h5)
    t['pct']=t.h10/t.n10
    t['gate']=np.where((t.pct>=.8)&(t.h5>=4)&(t.n10>=7),'','below gate')
    t=t.sort_values(['miss','h5','pct'],ascending=[True,False,False])
    t=t[t.groupby(['player','side']).cumcount()<2]  # one player can appear on both sides
    t=t[t.groupby('player').cumcount()<2]; t=t[t.groupby(['prop','side']).cumcount()<5].head(20)
    out[k]=t
    print(f'\n## {k.upper()} ({len(t)} rows, {(t.gate=="").sum()} pass gate)')
    for i,r in enumerate(t.itertuples(),1):
        ml=f'{r.ml:.2f}' if hasattr(r,'ml') and pd.notna(getattr(r,'ml',np.nan)) else '-'
        cf=f'{r.conf:.0f}' if hasattr(r,'conf') and pd.notna(getattr(r,'conf',np.nan)) else '-'
        print(f'{i}|{r.player}|{r.team if hasattr(r,"team") else ""}|{r.side[0]}|{r.prop}|{r.line:g}|{int(r.h10)}/{int(r.n10)}|{int(r.h5)}/5|proj {r.projection:.1f}|edge {r.edge_pick:+.1f}|ML {ml}|conf {cf}|{r.gate}')
pickle.dump(out,open('/tmp/std20.pkl','wb'))
