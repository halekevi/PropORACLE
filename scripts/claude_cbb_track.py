import _r2
"""CBB log-only forward tracking (rules LOCKED 2026-11-11). Usage: python3 scripts/claude_cbb_track.py [YYYY-MM-DD]
Appends qualifying picks from outputs/<date>/cbb/step1_cbb.csv to outputs/ticket_log/forward_tracking_cbb.csv, then grades any older ungraded rows from box scores."""
import pandas as pd,numpy as np,sqlite3,sys,os,datetime,warnings; warnings.filterwarnings('ignore'); sys.path.insert(0,'scripts'); from claude_gamelog import nm
day=sys.argv[1] if len(sys.argv)>1 else datetime.date.today().isoformat()
OUT='outputs/ticket_log/forward_tracking_cbb.csv'; B='../PropORACLE_main_cp/outputs'
C=['pts','reb','ast','stl','blk','tov','fg3m','minutes']
g1=pd.read_parquet('data/game_logs/cbb_player_games.parquet'); g1['gd']=pd.to_datetime(g1.game_date)
g2=pd.read_sql("select * from cbb",sqlite3.connect('data/cache/proporacle_ref.db')); g2['gd']=pd.to_datetime(g2.game_date)
g=pd.concat([g1[['player','gd']+C],g2[['player','gd']+C]],ignore_index=True)
for c in C: g[c]=pd.to_numeric(g[c],errors='coerce')
g=g[g.minutes>0].drop_duplicates(['player','gd']); g['k']=g.player.map(nm)
g['pra']=g.pts+g.reb+g.ast; g['pr']=g.pts+g.reb; g['pa']=g.pts+g.ast; g['ra']=g.reb+g.ast; g['bs']=g.blk+g.stl
M={'points':'pts','rebounds':'reb','assists':'ast','pts+rebs+asts':'pra','pts+rebs':'pr','pts+asts':'pa','rebs+asts':'ra','3-pt made':'fg3m','steals':'stl','blocked shots':'blk','turnovers':'tov','blks+stls':'bs'}
ser={k:v.sort_values('gd') for k,v in g.groupby('k')}; SEASON=pd.Timestamp('2026-10-01')
def grade(df):
    for i,r in df[df.result.isna()|(df.result=='')].iterrows():
        s=ser.get(nm(r.player)); 
        if s is None: continue
        x=s[s.gd==pd.Timestamp(r.date)]
        if not len(x): continue
        a=float(x.iloc[0][r['stat']]); df.loc[i,'actual']=a
        df.loc[i,'result']='PUSH' if a==r.line else ('HIT' if ((a>r.line)==(r.side=='OVER')) else 'MISS'); df.loc[i,'graded_at']=datetime.datetime.utcnow().isoformat(timespec='seconds')+'Z'
    return df
old=pd.read_csv(OUT) if os.path.exists(OUT) else pd.DataFrame()
f=f'{B}/{day}/cbb/step1_cbb.csv'; rows=[]
if os.path.exists(f):
    s=pd.read_csv(f); s=s[s.duration.eq('Full')|s.duration.isna()]; s['stat']=s.stat_type.str.lower().map(M); s=s[s.stat.notna()]
    dup=s[s.odds_type=='standard'].groupby(['player','stat']).line.nunique(); dk=set(dup[dup>1].index)
    for r in s.drop_duplicates(['player','stat','line','odds_type']).itertuples():
        if r.odds_type=='standard' and ((r.player,r.stat) in dk or r.line<=1.0): continue
        sr=ser.get(nm(r.player))
        if sr is None: continue
        hist=sr[sr.gd<pd.Timestamp(day)]
        if len(hist)<8 or (hist.gd>=SEASON).sum()<2: continue   # need >=8 total games and >=2 this season (early season: history is mostly last season)
        v=hist[r.stat].values.astype(float); sd=max(float(np.std(v[-20:])),0.3)
        for side,sg in (('OVER',1),('UNDER',-1)):
            if r.odds_type!='standard' and side=='UNDER': continue
            hit=(v>r.line) if sg==1 else (v<r.line); h5,h10,h20=hit[-5:].mean(),hit[-10:].mean(),hit[-20:].mean()
            z=min(sg*(v[-w:].mean()-r.line) for w in (5,10,20))/sd; grp=[]
            if r.odds_type=='goblin' and side=='OVER' and h5>=1 and h10>=.9 and h20>=.8: grp.append('CBB_G1'); 
            if r.odds_type=='goblin' and side=='OVER' and h5>=1 and h10>=.9 and h20>=.8 and z>=1: grp.append('CBB_G2')
            if r.odds_type=='standard' and side=='UNDER' and h10>=.7 and h20>=.6 and z>=.5: grp.append('CBB_S1')
            if r.odds_type=='standard' and side=='UNDER' and h5>=.8 and h10>=.8 and h20>=.65 and z>=.5: grp.append('CBB_S2')
            if r.odds_type=='standard' and side=='OVER' and h5>=.8 and h10>=.8 and h20>=.65 and z>=.25: grp.append('CBB_O1')
            for gname in grp: rows.append(dict(date=day,group=gname,player=r.player,team=r.team,stat=r.stat,side=side,line=r.line,pick_type=r.odds_type,h5=round(h5,2),h10=round(h10,2),h20=round(h20,2),zmin=round(z,2),result='',actual='',graded_at=''))
new=pd.DataFrame(rows)
if len(new) and len(old): old=old[old.date!=day]
df=pd.concat([old,new],ignore_index=True) if len(old) or len(new) else pd.DataFrame(columns=['date'])
if len(df) and 'result' in df: df['result']=df.result.astype('object'); df['actual']=df.actual.astype('object'); df['graded_at']=df.graded_at.astype('object'); df=grade(df)
if len(df): os.makedirs('outputs/ticket_log',exist_ok=True); df.to_csv(OUT,index=False)
print(day,'new picks logged:',len(new)); 
if len(new): print(new.groupby('group').size().to_dict())
if len(df) and 'result' in df:
    d2=df[df.result.isin(['HIT','MISS'])]; print('graded so far:',d2.groupby('group').result.apply(lambda s:f'{(s=="HIT").mean():.0%} of {len(s)}').to_dict())
