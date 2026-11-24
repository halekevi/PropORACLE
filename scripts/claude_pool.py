"""Pool per-sport top30 CSVs into POOLED_{standard,goblin,demon}_top30.csv. Rank: fewest total misses (L10 misses + 5-L5 hits), then L5 hits, L10 pct, cushion%. Caps: 2 rows/player, 5/prop category."""
import pandas as pd
D='outputs/2026-11-03_top30/'
for t in ['standard','goblin','demon']:
    d=pd.concat([pd.read_csv(f'{D}{s}_{t}.csv') for s in ['nfl','cfb','nba','soccer']],ignore_index=True)
    if d.empty: continue
    d=d.drop(columns=[c for c in ['rank'] if c in d])
    a=d.l10.str.split('/',expand=True).astype(int)
    d['miss']=(a[1]-a[0])+(5-d.l5_hits)
    sg=d.side.str.upper().map({'OVER':1,'UNDER':-1})
    d['cush']=d.min_gap_pct
    d=d.sort_values(['miss','l5_hits','l10_pct','cush'],ascending=[True,False,False,False])
    d=d[d.groupby('player').cumcount()<2]
    d=d[d.groupby(['sport','prop']).cumcount()<5].head(30)
    d.insert(0,'rank',range(1,len(d)+1))
    d.to_csv(f'{D}POOLED_{t}_top30.csv',index=False)
    print('\n##',t,len(d))
    print(d[['rank','sport','player','team','side','prop','line','l10','l5','cush']].assign(cush=(d.cush*100).round(0)).to_string(index=False))
