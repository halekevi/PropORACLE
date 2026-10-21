import pandas as pd
from pathlib import Path
import sys
sys.path[:0]=[r'H:/PropORACLE']
from datetime import date
# import the allow function from step5b
import importlib.util
spec=importlib.util.spec_from_file_location('s5b', r'H:/PropORACLE/Sports/CFB/scripts/pipeline/step5b_attach_boxscore_stats.py')
m=importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
for d in [date(2026,9,20), date(2026,9,21), date(2026,9,25), date(2026,9,26)]:
    print(f'allow_prior_fill({d}) =', m._cfb_allow_prior_season_fallback(d), 'week3_sunday cutoff via', m._cfb_week0_saturday(m._cfb_season_start(d)) + __import__('datetime').timedelta(days=22))

df=pd.read_excel(r'H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx')
print('\nSeason L5 Flag counts:')
print(df['Season L5 Flag'].astype(str).value_counts().head(15).to_string())
print('\nPRIOR_SEASON rows', df['Season L5 Flag'].astype(str).str.contains('PRIOR', case=False, na=False).sum())
print('L5 Sample N dist', pd.to_numeric(df['L5 Sample N'], errors='coerce').value_counts().sort_index().to_dict())
