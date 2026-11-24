"""Grade NBA PrizePicks legs for slates missing from data/ledger (Oct 29 -> latest) from daily step1 boards + DB box scores.
Writes data/ledger_claude/legs_supplement.parquet (NOT the official ledger). Duplicate-Standard player-props are dropped (mislabeled Demons)."""
import pandas as pd,numpy as np,sqlite3,glob,os,sys,unicodedata,re
START=sys.argv[1] if len(sys.argv)>1 else '2026-10-29'
B='../PropORACLE_main_cp/outputs'
def fold(s): return re.sub(r'[^a-z ]','',unicodedata.normalize('NFKD',str(s)).encode('ascii','ignore').decode().lower().replace('.','').replace("'",'')).strip()
MAP={'points':'pts','rebounds':'reb','assists':'ast','3-pt made':'fg3m','3-pt attempted':'fg3a','pts+rebs+asts':'pra','pts+rebs':'pr','pts+asts':'pa','rebs+asts':'ra',
 'steals':'stl','blocked shots':'blk','blks+stls':'bs','turnovers':'tov','fg made':'fgm','fg attempted':'fga','free throws made':'ftm','free throws attempted':'fta',
 '2-pt made':'fg2m','2-pt attempted':'fg2a','offensive rebounds':'oreb','defensive rebounds':'dreb','fantasy score':'fantasy_score','personal fouls':'pf','two pointers made':'fg2m','two pointers attempted':'fg2a'}
CANON={'pts':'points','reb':'rebounds','ast':'assists','fg3m':'threes','fg3a':'threes_att','pra':'pra','pr':'pts+reb','pa':'pts+ast','ra':'reb+ast','stl':'steals','blk':'blocks','bs':'stocks','tov':'turnovers'}
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",c); n['k']=n.player.map(fold); n['gd']=n.game_date.astype(str).str[:10]
n['minutes']=pd.to_numeric(n.minutes,errors='coerce'); n=n[n.minutes>0].drop_duplicates(['k','gd']); n['opp_db']=np.where(n.team==n.home_team,n.away_team,n.home_team); n['team_db']=n.team
rows=[];unm=set()
for dd in sorted(glob.glob(f'{B}/2026-*')):
    day=os.path.basename(dd)
    if day<START: continue
    f=f'{dd}/nba/step1_pp_props_today.csv'
    if not os.path.exists(f): continue
    s=pd.read_csv(f,encoding='utf-8-sig'); s=s[s.pick_type.isin(['Goblin','Demon','Standard'])].copy()
    s['k']=s.player.map(fold); s['gd']=(s['game_date'] if 'game_date' in s.columns else s['start_time'] if 'start_time' in s.columns else pd.Series(day,index=s.index)).astype(str).str[:10]; s['pt']=s.prop_type.str.lower().str.strip()
    s=s[~s.player.astype(str).str.contains(r' \+ |/',regex=True)]
    s['col']=s.pt.map(MAP); unm|=set(s[s.col.isna()].prop_type.unique()); s=s[s.col.notna()]
    dup=s[s.pick_type=='Standard'].groupby(['k','col']).line.nunique(); dupk=set(dup[dup>1].index)
    s=s[~(s.pick_type.eq('Standard')&s.set_index(['k','col']).index.isin(dupk))].drop_duplicates(['k','col','line','pick_type'])
    m=s.drop(columns=['team','opp_team'],errors='ignore').merge(n[['k','gd','team_db','opp_db']+sorted(set(MAP.values()))],on=['k','gd'],how='left')
    m['actual']=[m.loc[i,c] if pd.notna(c) else np.nan for i,c in zip(m.index,m.col)]
    m=m[m.actual.notna()]
    for side in ('OVER','UNDER'):
        x=m if side=='UNDER' else m
        if side=='UNDER': x=m[m.pick_type=='Standard']
        x=x.copy(); x['direction']=side
        x['result']=np.where(x.actual==x.line,'PUSH',np.where((x.actual>x.line)==(side=='OVER'),'HIT','MISS'))
        rows.append(x.assign(slate_date=day))
o=pd.concat(rows); o=o[o.result!='PUSH']
o['hit']=(o.result=='HIT').astype(float); o['pick_type']=o.pick_type.str.lower(); o['prop_canon']=o.col.map(CANON).fillna(o.col)
o=o.drop(columns=['game_date'],errors='ignore').rename(columns={'opp_team':'opp_team','prop_type':'prop','actual':'actual_value','gd':'game_date'})
o['team']=o.team_db;o['opp_team']=o.opp_db;o['league']='';o['sport']='NBA'; o['source']='claude_grade_gap'
import datetime
NOW=datetime.datetime.utcnow().isoformat(timespec='seconds')+'Z'
KEY=['slate_date','player','prop','line','direction','pick_type']
o['graded_at']=NOW
OLD='data/ledger_claude/legs_supplement.parquet'
if os.path.exists(OLD):
    old=pd.read_parquet(OLD)
    if 'graded_at' in old.columns:
        om=old.drop_duplicates(KEY).set_index(KEY).graded_at; o=o.join(om.rename('ga_old'),on=KEY); o['graded_at']=o.ga_old.fillna(o.graded_at); o=o.drop(columns='ga_old')
keep=['slate_date','league','sport','player','team','opp_team','prop','prop_canon','line','direction','pick_type','result','hit','actual_value','game_date','source','graded_at']
os.makedirs('data/ledger_claude',exist_ok=True); o[keep].to_parquet('data/ledger_claude/legs_supplement.parquet',index=False)
print('legs',len(o),'days',o.slate_date.nunique(),'unmapped props:',sorted(unm)[:15])
print(o.groupby(['pick_type','direction']).hit.agg(['mean','size']).round(3))
print(o.groupby('slate_date').size().to_dict())
