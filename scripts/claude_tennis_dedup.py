import pandas as pd, shutil, sys
sys.path.insert(0,'.')
from pathlib import Path
L=Path('data/game_logs/tennis_player_games.parquet')
for p in (L,L.with_suffix('.csv')): shutil.copy2(p,str(p)+'.bak_20261115')
df=pd.read_parquet(L); n0=len(df)
df=df.drop_duplicates(['player_key','event_id'],keep='first').reset_index(drop=True)
print(n0,'->',len(df))
try:
    from utils.game_log_windows import add_card_l5_l10, add_rest_b2b
except Exception as e:
    print('import fail',e); sys.exit(1)
stats=("aces","double_faults","service_games","games_won","match_total_games","matches")
drop=[c for c in df.columns if c.startswith(("l5_","l10_")) or c in {"rest_days","back_to_back","is_dnp","in_card_window","dnp_last10"}]
df=df.drop(columns=drop,errors='ignore')
df=add_rest_b2b(df); df=add_card_l5_l10(df,pt_col="matches",stat_cols=stats)
df.to_parquet(L,index=False); df.to_csv(L.with_suffix('.csv'),index=False)
print(df.game_date.min(),df.game_date.max(),len(df))
