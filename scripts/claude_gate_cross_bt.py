import _r2,sys,re,unicodedata,sqlite3,pandas as pd,numpy as np
sys.path.insert(0,'scripts')
def nm(s):
    s=unicodedata.normalize('NFKD',str(s)).encode('ascii','ignore').decode().lower(); s=re.sub(r"[.'’,]",'',s); s=re.sub(r'\b(jr|sr|ii|iii|iv)\b','',s); return re.sub(r'\s+',' ',s).strip()
c=sqlite3.connect('data/cache/proporacle_ref.db')
def lg(parq,tbl,cols):
    fr=[]
    try: fr.append(pd.read_parquet(parq)[['player','game_date']+cols])
    except Exception as e: pass
    try: fr.append(pd.read_sql(f"select player,game_date,{','.join(cols)} from {tbl}",c))
    except Exception as e: pass
    g=pd.concat(fr,ignore_index=True); g['gd']=pd.to_datetime(g.game_date.astype(str),errors='coerce'); g=g.dropna(subset=['gd']).drop_duplicates(['player','gd']); g['k']=g.player.map(nm); return g
NBAc=['pts','reb','ast','stl','blk','tov','fg3m','fga','fantasy_score','minutes']; 
nba=lg('data/game_logs/nba_player_games.parquet','nba',[x for x in NBAc if x!='fga' and x!='fantasy_score']+['fga','fantasy_score']) if False else None
def safe(parq,tbl,cols):
    try: return lg(parq,tbl,cols)
    except Exception as e:
        print('fallback',tbl,str(e)[:80]); 
        fr=[]
        try: fr.append(pd.read_sql(f"select * from {tbl}",c))
        except: pass
        g=pd.concat(fr); g['gd']=pd.to_datetime(g.game_date.astype(str),errors='coerce'); g['k']=g.player.map(nm); return g.drop_duplicates(['player','gd'])
nba=pd.read_sql("select * from nba where coalesce(season_window,'')!='preseason'",c); nba['gd']=pd.to_datetime(nba.game_date.astype(str),errors='coerce')
try: nba=pd.concat([nba,pd.read_parquet('data/game_logs/nba_player_games.parquet')],ignore_index=True)
except Exception as e: print(e)
cbb=pd.read_sql("select * from cbb",c)
try: cbb=pd.concat([cbb,pd.read_parquet('data/game_logs/cbb_player_games.parquet')],ignore_index=True)
except Exception as e: print(e)
nhl=pd.concat([pd.read_sql("select * from nhl",c),pd.read_parquet("data/game_logs/nhl_player_games.parquet")],ignore_index=True)
soc=pd.concat([pd.read_sql("select * from soccer",c),pd.read_parquet("data/game_logs/soccer_player_games.parquet")],ignore_index=True)
soc["minutes_played"]=pd.to_numeric(soc.minutes_played,errors="coerce").fillna(pd.to_numeric(soc.get("minutes"),errors="coerce"))
for df in (nba,cbb,nhl,soc): df['gd']=pd.to_datetime(df.game_date.astype(str),errors='coerce'); df.dropna(subset=['gd'],inplace=True)
nba=nba[nba.get('minutes',pd.Series(1,index=nba.index)).fillna(0)>0]; cbb=cbb[cbb.minutes.fillna(0)>0]
soc=soc[soc.minutes_played.fillna(1)>0]
for df in (nba,cbb): 
    for a,b in [('pr',['pts','reb']),('pa',['pts','ast']),('ra',['reb','ast']),('pra',['pts','reb','ast']),('bs',['blk','stl'])]:
        if a not in df or df[a].isna().all(): df[a]=df[b].sum(axis=1)
soc['ga']=soc.g+soc.a
for df in (nba,cbb,nhl,soc): df.drop_duplicates(['player','gd'],inplace=True); df['k']=df.player.map(nm)
MAP={'NBA':(nba,{'points':'pts','rebounds':'reb','assists':'ast','3-pt made':'fg3m','steals':'stl','blocked shots':'blk','turnovers':'tov','pts+rebs':'pr','pts+asts':'pa','rebs+asts':'ra','pts+rebs+asts':'pra','blks+stls':'bs','fantasy score':'fantasy_score','fg attempted':'fga'}),
 'CBB':(cbb,{'pts':'pts','reb':'reb','ast':'ast','pr':'pr','pa':'pa','ra':'ra','pra':'pra','fg3m':'fg3m','stl':'stl','blk':'blk','tov':'tov'}),
 'NHL':(nhl,{'points':'points','shots_on_goal':'shots_on_goal','shots on goal':'shots_on_goal','goals':'goals','assists':'assists','blocked_shots':'blocked_shots','blocked shots':'blocked_shots','hits':'hits','power play points':'pp_points','plus/minus':'plus_minus'}),
 'Soccer':(soc,{'shots':'sh','shots on target':'sog','goals':'g','assists':'a','goal + assist':'ga','goalie saves':'sv','tackles':'tk','fouls':'fc'})}
IDX={}
for sp,(df,m) in MAP.items():
    IDX[sp]={k:(v.sort_values('gd').gd.values,v.sort_values('gd')) for k,v in df.groupby('k')}
d=pd.read_csv('../PropORACLE_main_cp/data/training/graded_export_20260516.csv',low_memory=False)
d=d[d.sport.isin(MAP)&d.result_binary.isin([0,1])].copy(); d['pt']=d.pick_type.astype(str).str.lower(); d=d[d.pt.isin(['standard','goblin','demon'])]
d['dir']=d.direction.astype(str).str.upper(); d=d[d.dir.isin(['OVER','UNDER'])]; d['dt']=pd.to_datetime(d.file_date); d['k']=d.player.map(nm); d['pl']=d.prop.astype(str).str.lower()
R=[]
for r in d.itertuples():
    df,m=MAP[r.sport]; col=m.get(r.pl)
    if col is None or r.k not in IDX[r.sport]: continue
    dates,x=IDX[r.sport][r.k]; i=np.searchsorted(dates,np.datetime64(r.dt)); 
    if i<10 or col not in x: continue
    a=x[col].values[:i].astype(float); a=a[~np.isnan(a)]
    if len(a)<10: continue
    hit=(a>r.line) if r.dir=='OVER' else (a<r.line)
    R.append((r.Index,hit[-5:].sum(),hit[-10:].sum(),(hit[-20:].sum() if len(a)>=20 else np.nan),len(a)))
F=pd.DataFrame(R,columns=['ix','h5','h10','h20','n']).set_index('ix'); d=d.join(F,how='inner'); d['game']=d.apply(lambda r:'|'.join(sorted([str(r.team),str(r.opp_team)])),axis=1)
d.to_pickle('/tmp/gate_bt.pkl'); print('rows with windows',len(d)); print(d.groupby('sport').size().to_dict())
