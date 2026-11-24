"""Run right after ANY NBA fetch:  python scripts/claude_ab_snapshot.py <day>
Tiers the CURRENT board and saves it as outputs/tiered_all_<day>_snap_<HHMM>.xlsx (HHMM = fetch time ET), so every
fetch of the day is kept and can be graded separately. Skips if that fetch is already saved."""
import sys,os,subprocess,shutil,pandas as pd
D=sys.argv[1]; src=f'../PropORACLE_main_cp/outputs/{D}/nba/step1_pp_props_today.csv'
d=pd.read_csv(src,encoding='utf-8-sig',usecols=['fetched_at']); fa=pd.to_datetime(d.fetched_at,utc=True,errors='coerce').max()
ts=fa.tz_convert('America/New_York').strftime('%H%M'); out=f'outputs/tiered_all_{D}_snap_{ts}.xlsx'
if os.path.exists(out): print('already saved',out); sys.exit()
os.makedirs(f'data/cache/board_archive/{D}',exist_ok=True); shutil.copy(src,f'data/cache/board_archive/{D}/ab_NBA_{ts}.csv')
subprocess.run([sys.executable,'scripts/claude_tier_today.py',D],check=True,stdout=subprocess.DEVNULL)
x=pd.ExcelFile(f'outputs/tiered_picks_{D}.xlsx')
with pd.ExcelWriter(out) as w:
    for s in x.sheet_names:
        if s!='Read me': x.parse(s).to_excel(w,sheet_name=('NBA '+s)[:31],index=False)
print('saved',out)
