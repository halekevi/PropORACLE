"""Adds timing + early-season tags to NBA ranked lists. Usage: python3 scripts/claude_tags.py 2026-11-01
hours_before_tip = start_time - fetched_at (when the line was pulled). early_season = first 21 days after Oct 15."""
import sys,pandas as pd
D=sys.argv[1] if len(sys.argv)>1 else '2026-11-01'
s=pd.read_csv(f'outputs/{D}/nba/step1_pp_props_today.csv',usecols=['player','prop_type','line','pick_type','start_time','fetched_at'])
s['hrs']=((pd.to_datetime(s.start_time,utc=True)-pd.to_datetime(s.fetched_at,utc=True)).dt.total_seconds()/3600).round(1)
s=s.rename(columns={'prop_type':'prop','pick_type':'pick_type'}).drop_duplicates(['player','prop','line','pick_type'])
dd=pd.Timestamp(D); start=pd.Timestamp(dd.year if dd.month>=9 else dd.year-1,10,15)
early=0<=(dd-start).days<=21
for f in ['ranked_5of5','ranked_4of5','goblin_5of5','goblin_4of5','demon_5of5','demon_4of5']:
    p=f'outputs/{D}/ranked/nba_{f}.csv'; d=pd.read_csv(p).drop(columns=['line_pulled_at','hours_before_tip','early_season'],errors='ignore')
    d=d.merge(s[['player','prop','line','pick_type','fetched_at','hrs']].rename(columns={'fetched_at':'line_pulled_at','hrs':'hours_before_tip'}),on=['player','prop','line','pick_type'],how='left')
    d['early_season']=early; d.to_csv(p,index=False); print(f,len(d),'unmatched',d.hours_before_tip.isna().sum())
print('early_season',early,'days into season',(dd-start).days)
