import pandas as pd,numpy as np,re,unicodedata,os,sys,itertools
import sys, tempfile; from pathlib import Path
R=str(Path(__file__).resolve().parents[2]); D=sys.argv[1] if len(sys.argv)>1 else os.environ.get('PROPORACLE_DATE','2026-11-01')
b=pd.read_excel(f'{R}/outputs/{D}/nfl/step8_nfl_direction_clean.xlsx',sheet_name='ALL')
s=b[b['Pick Type']=='Standard'].copy()
g=pd.read_parquet(f'{R}/data/game_logs/nfl_player_games.parquet')
g=g[(g.game_date.astype(str)<D)&(~g.is_dnp.fillna(False).astype(bool))&(g.get('phase','REGULAR')!='PRESEASON')].copy()
def nm(x):
    x=unicodedata.normalize('NFKD',str(x)).encode('ascii','ignore').decode().lower(); x=re.sub(r"[^a-z ]",'',x)
    return re.sub(r'\b(jr|sr|ii|iii|iv)\b','',x).replace('  ',' ').strip()
g['pn']=g.player_norm.map(nm); s['pn']=s.Player.map(nm)
M={'receiving_yards':['REC_YDS'],'receptions':['REC'],'rec_targets':['REC_TGT'],'rushing_yards':['RUSH_YDS'],'rush_attempts':['RUSH_ATT'],
 'rush_rec_yds':['RUSH_YDS','REC_YDS'],'passing_yards':['PASS_YDS'],'pass_attempts':['PASS_ATT'],'pass_completions':['PASS_CMP'],
 'pass_rush_yds':['PASS_YDS','RUSH_YDS'],'passing_tds':['PASS_TD'],'rush_tds':['RUSH_TD'],'kicking_points':['KICK_PTS'],'fg_made':['KICK_FG']}
inj=pd.read_csv(f'{R}/outputs/{D}/nfl/injuries_nfl_{D}.csv'); inj['pn']=inj.player.map(nm)
bad=set(inj[inj.injury_status.astype(str).str.lower().str.contains('out|injured reserve|ir|doubtful')].pn)
q=dict(zip(inj.pn,inj.injury_status))
g=g.sort_values('game_date'); grp={k:v for k,v in g.groupby('player_key')}
# One log per ESPN athlete id; the board has names only, so a shared name resolves by board team.
last_team=g.groupby('player_key').team_id.last().astype(str); keys_by_pn=g.groupby('pn').player_key.unique()
try:
    import requests
    T={t['team']['abbreviation'].upper():str(t['team']['id']) for t in requests.get('https://site.api.espn.com/apis/site/v2/sports/football/nfl/teams',timeout=20).json()['sports'][0]['leagues'][0]['teams']}
    T.update({k:T[v] for k,v in {'LA':'LAR','WAS':'WSH','JAC':'JAX'}.items() if v in T})
except Exception: T={}
def key_for(pn,team):
    ks=list(keys_by_pn.get(pn,[]))
    if len(ks)<=1: return ks[0] if ks else None
    m=[k for k in ks if last_team.get(k)==T.get(str(team).upper())]
    return m[0] if len(m)==1 else None
s['pk']=[key_for(p,t) for p,t in zip(s.pn,s.Team)]
rows=[]
for _,r in s.iterrows():
    cols=M.get(r.Prop)
    if not cols or r.pk not in grp: continue
    h=grp[r.pk]; v=h[cols].sum(axis=1,min_count=len(cols)).dropna().values
    if len(v)<5: continue
    L=float(r.Line); v10=v[-10:]; v5=v[-5:]
    for side,f in (('OVER',lambda a:a>L),('UNDER',lambda a:a<L)):
        k5=int(f(v5).sum()); k10=int(f(v10).sum()); n10=len(v10)
        if k5<4 or n10<7 or k10/n10<0.8: continue
        cush=(1 if side=='OVER' else -1)*(v10.mean()-L)/max(L,0.5); cush=max(-1,min(1,cush))
        sc=0.5*k10/n10+0.3*k5/5+0.2*(cush+1)/2
        rows.append(dict(player=r.Player,team=r.Team,opp=r.Opp,side=side,prop=r.Prop,line=L,L10=f'{k10}/{n10}',k10=k10,n10=n10,L5=f'{k5}/5',k5=k5,score=round(sc,3),
          last5=','.join(f'{x:g}' for x in v5),game_time=r['Game Time'],inj=q.get(r.pn,''),dnp=bool(r.pn in bad),days_old=(pd.Timestamp(D)-pd.Timestamp(h.game_date.iloc[-min(10,len(h))])).days))
A=pd.DataFrame(rows)
A=A[~A.dnp]
A=A.sort_values(['k10','n10'],ascending=[False,True]) if False else A
A['h10']=A.k10/A.n10
A=A.sort_values(['h10','k5','score'],ascending=False).drop_duplicates(['player','prop']).reset_index(drop=True)
out=f'{R}/outputs/{D}/ranked'; os.makedirs(out,exist_ok=True)
A[A.k5==5].to_csv(f'{out}/claude_nfl_standard_5of5.csv',index=False); A.to_csv(f'{out}/claude_nfl_standard_4of5plus.csv',index=False)
pd.to_pickle(A,os.path.join(tempfile.gettempdir(),'nfl_A.pkl'))
print(len(A),(A.k5==5).sum(),A.side.value_counts().to_dict())
print(A[['player','team','opp','side','prop','line','L10','L5','score','last5','game_time','inj']].to_string(max_colwidth=16))
