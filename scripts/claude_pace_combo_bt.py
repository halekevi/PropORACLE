import pandas as pd,numpy as np,sqlite3,unicodedata
def nm(s): return ''.join(c for c in unicodedata.normalize('NFD',str(s)) if not unicodedata.combining(c)).lower().replace('.','').replace("'",'').replace(' jr','').replace(' iii','').replace(' ii','').strip()
c=sqlite3.connect('data/cache/proporacle_ref.db')
d=pd.read_sql("select game_date,event_id,team,minutes,fga,fta,oreb,tov from nba where coalesce(season_window,'')!='preseason'",c)
for k in ['minutes','fga','fta','oreb','tov']: d[k]=pd.to_numeric(d[k],errors='coerce')
g=d.groupby(['event_id','game_date','team']).agg(fga=('fga','sum'),fta=('fta','sum'),oreb=('oreb','sum'),tov=('tov','sum'),mn=('minutes','sum')).reset_index()
g=g[g.fga>20]; g['poss']=g.fga+0.44*g.fta-g.oreb+g.tov; g['gd']=pd.to_datetime(g.game_date)
g['pace']=g.poss/(g.mn/5).clip(lower=48)*48; g=g.sort_values('gd')   # each team's OWN possessions per 48
TP={t:v[['gd','pace']].reset_index(drop=True) for t,v in g.groupby('team')}
def tp(t,day):
    x=TP.get(t)
    if x is None: return np.nan
    x=x[x.gd<day].tail(10); return x.pace.mean() if len(x)>=5 else np.nan
AB={'NYK':'NY','GSW':'GS','NOP':'NO','SAS':'SA','UTA':'UTAH','WAS':'WSH','PHO':'PHX','BRK':'BKN'}
def m(t): return t if t in TP else AB.get(t,t)
G=pd.read_csv('outputs/pace_bt.csv'); G['d']=pd.to_datetime(G.file_date); G['k']=G.player.map(nm)
sch=pd.read_sql("select distinct game_date,home_team,away_team from nba where coalesce(season_window,'')!='preseason'",c)
OPP={}
for r in sch.itertuples():
    OPP[(r.game_date,r.home_team)]=r.away_team; OPP[(r.game_date,r.away_team)]=r.home_team
G['opp_team']=[OPP.get((str(d0)[:10],m(a))) for a,d0 in zip(G.team,G.d)]
print('opponent found:',G.opp_team.notna().mean().round(3))
G['pa']=[tp(m(a),d0) if isinstance(a,str) else np.nan for a,d0 in zip(G.team,G.d)]; G['pb']=[tp(m(b),d0) if isinstance(b,str) else np.nan for b,d0 in zip(G.opp_team,G.d)]
G=G.dropna(subset=['pa','pb']); allp=pd.concat([G.pa,G.pb]); lo,hi=allp.quantile([.33,.67]); print('team pace cuts (own poss/48):',round(lo,2),round(hi,2),len(G))
fa=(G.pa>=hi).astype(int)+(G.pb>=hi).astype(int); sl=(G.pa<=lo).astype(int)+(G.pb<=lo).astype(int)
G['combo']=np.select([fa==2,sl==2,fa==1],['Both fast','Both slow','One fast'],'Neither (mixed/mid)')
h=pd.read_sql("select player,game_date,minutes,pts from nba where coalesce(season_window,'')!='preseason'",c)
for k in ['minutes','pts']: h[k]=pd.to_numeric(h[k],errors='coerce')
h=h[h.minutes>0].drop_duplicates(['player','game_date']); h['gd']=pd.to_datetime(h.game_date); h['k']=h.player.map(nm); H={k:v.sort_values('gd') for k,v in h.groupby('k')}
mm=[];pp=[]
for k,d0 in zip(G.k,G.d):
    x=H.get(k)
    if x is None: mm.append(np.nan);pp.append(np.nan);continue
    x=x[x.gd<d0].tail(10)
    mm.append(x.minutes.mean() if len(x)>=5 else np.nan);pp.append(x.pts.mean() if len(x)>=5 else np.nan)
G['mpg']=mm;G['ppg']=pp
G.to_csv('outputs/pace_combo_bt.csv',index=False)
order=['Both fast','One fast','Both slow','Neither (mixed/mid)']
def z(a,b):
    p=(a.sum()+b.sum())/(len(a)+len(b)); return (a.mean()-b.mean())/np.sqrt(p*(1-p)*(1/len(a)+1/len(b)))
for name,x in (('ALL PLAYERS',G),('SCORERS ppg>=18',G[G.ppg>=18]),('HIGH MIN (>=30) + SCORERS',G[(G.mpg>=30)&(G.ppg>=18)])):
    print('\n###',name,len(x))
    for pk in ['Standard','Goblin','Demon']:
        y=x[x.pick_type==pk]; t=y.groupby('combo').over.agg(['mean','size']).reindex(order)
        bf=y[y.combo=='Both fast'].over; bs=y[y.combo=='Both slow'].over
        print(pk,' | '.join(f"{k}: {r['mean']*100:.2f}% (n={0 if pd.isna(r['size']) else int(r['size'])})" for k,r in t.iterrows()),'| both fast vs both slow z=%.2f'%z(bf,bs))
