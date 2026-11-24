"""Daily context attach for the NFL lists.

Weather comes from the pre-tip freeze (Sports/NFL/data/nfl_weather_freeze.json).
The older nfl_weather_cache.json is used only when that date has no freeze rows.
Injuries: outputs/<D>/nfl/injuries_nfl_<D>.csv -> claude_nfl_*.csv.
Run AFTER the pipeline each day:  python3 scripts/claude_nfl/attach_daily_context.py 2026-11-01"""
import sys,json,re,pandas as pd
sys.path.insert(0,'.')
D=sys.argv[1] if len(sys.argv)>1 else '2026-11-01'
W={}
try:
    freeze=json.load(open('Sports/NFL/data/nfl_weather_freeze.json'))
except Exception:
    freeze={}
for v in (freeze.get('events') or {}).values():
    if str(v.get('game_date') or '')[:10] != D or not v.get('frozen'):
        continue
    for tm in (v.get('home'), v.get('away')):
        if tm:
            W[tm]=dict(venue=v.get('venue_name'),indoor=v.get('dome'),temp_f=v.get('temp_f'),wind_mph=v.get('wind_mph'),precip_mm=v.get('precip_mm'),weather_flag=v.get('weather_flag'))
if not W:
    try:
        wc=json.load(open('Sports/NFL/data/nfl_weather_cache.json'))['games']
    except Exception:
        wc={}
    for v in wc.values():
        for tm in (v.get('home'), v.get('away')):
            if tm:
                W[tm]=dict(venue=v.get('venue_name'),indoor=v.get('indoor'),temp_f=v.get('temp_f'),wind_mph=v.get('wind_mph'),precip_mm=v.get('precip_mm'),weather_flag=v.get('weather_flag'))
inj=pd.read_csv(f'outputs/{D}/nfl/injuries_nfl_{D}.csv')
n=lambda t:re.sub(r'[^a-z ]','',str(t).lower().replace('.','')).replace(' sr','').replace(' jr','').replace(' ii','').strip()
inj['pn']=inj.player.map(n); inj=inj.drop_duplicates('pn')[['pn','injury_status','injury_type_desc']]
for f in ['standard_5of5','standard_4of5plus','demon_5of5','demon_4of5plus']:
    p=f'outputs/{D}/ranked/claude_nfl_{f}.csv'; d=pd.read_csv(p).drop(columns=['venue','temp_f','wind_mph','precip_mm','weather_flag','injury_status','injury_type_desc','wx_note'],errors='ignore')
    for c in ('venue','temp_f','wind_mph','precip_mm','weather_flag'): d[c]=d.team.map(lambda t:W.get(t,{}).get(c))
    d['pn']=d.player.map(n); d=d.merge(inj,on='pn',how='left').drop(columns='pn')
    kick=d.prop.isin(['fg_made','kicking_points']); air=d.prop.isin(['passing_yards','pass_attempts','pass_completions','pass_rush_yds','passing_tds','receiving_yards','receptions','rec_targets'])
    w=d.wind_mph.fillna(0); pr=d.precip_mm.fillna(0)
    d['wx_note']=''
    d.loc[(kick|air)&(w>=15),'wx_note']='WIND 15+ mph'; d.loc[(kick|air)&(pr>=0.5)&(d.wx_note==''),'wx_note']='RAIN'
    d.loc[d.wind_mph.isna()&(d.weather_flag!='dome'),'wx_note']='WEATHER UNKNOWN'
    d.to_csv(p,index=False); print(f,len(d),'wind filled',d.wind_mph.notna().sum(),'inj tagged',d.injury_status.notna().sum(),'wx notes',(d.wx_note!='').sum())
