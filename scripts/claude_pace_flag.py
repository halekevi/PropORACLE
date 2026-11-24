"""Daily pace flag. Usage:
  py -3 scripts/claude_pace_flag.py <day>           tag today's NBA props, append to outputs/pace_tracking.csv
  py -3 scripts/claude_pace_flag.py <day> --grade   grade that day's rows from DB actuals (run after DB has the games)
Fast >= 101.83 own poss/48 (last 10 games), slow <= 99.00. Combo: Both fast / One fast / Both slow / Neither.
"""
import sys,os,sqlite3,unicodedata
import pandas as pd,numpy as np
D=sys.argv[1]; GRADE='--grade' in sys.argv
HI,LO=101.83,99.00
TR='outputs/pace_tracking.csv'
def nm(s): return ''.join(c for c in unicodedata.normalize('NFD',str(s)) if not unicodedata.combining(c)).lower().replace('.','').replace("'",'').replace(' jr','').replace(' iii','').replace(' ii','').strip()
AB={'NYK':'NY','GSW':'GS','NOP':'NO','SAS':'SA','UTA':'UTAH','WAS':'WSH','PHO':'PHX','BRK':'BKN'}
c=sqlite3.connect('data/cache/proporacle_ref.db')
if GRADE:
    t=pd.read_csv(TR,dtype={'result':object}); t['k']=t.Player.map(nm)
    a=pd.read_sql("select * from nba where game_date=?",c,params=(D,))
    a['k']=a.player.map(nm); a=a.drop_duplicates('k').set_index('k')
    M={'Points':('pts',),'Rebounds':('reb',),'Assists':('ast',),'Steals':('stl',),'Blocked Shots':('blk',),'Turnovers':('tov',),'3-PT Made':('fg3m',),
      'FG Attempted':('fga',),'FG Made':('fgm',),'Free Throws Attempted':('fta',),'Free Throws Made':('ftm',),'3-PT Attempted':('fg3a',),'Two Pointers Attempted':('fg2a',),
      'Two Pointers Made':('fg2m',),'Personal Fouls':('pf',),'Offensive Rebounds':('oreb',),'Defensive Rebounds':('dreb',),'Pts+Rebs':('pts','reb'),'Pts+Asts':('pts','ast'),
      'Rebs+Asts':('reb','ast'),'Pts+Rebs+Asts':('pts','reb','ast'),'Blks+Stls':('blk','stl')}
    def actual(k,p):
        f=M.get(p)
        if k not in a.index or not f or any(x not in a.columns for x in f): return None
        v=[pd.to_numeric(a.at[k,x],errors='coerce') for x in f]; return None if any(pd.isna(x) for x in v) else float(sum(v))
    m=t.Day.astype(str).eq(D)&t.result.isna()
    for i in t[m].index:
        v=actual(t.at[i,'k'],t.at[i,'Prop'])
        if v is None: continue
        L=float(t.at[i,'Line']); t.at[i,'actual']=v
        t.at[i,'result']='PUSH' if v==L else ('HIT' if ((v>L) if t.at[i,'Side']=='Over' else (v<L)) else 'MISS')
    t.drop(columns='k').to_csv(TR,index=False)
    g=t[t.Day.astype(str).eq(D)&t.result.isin(['HIT','MISS'])]
    print('graded',len(g),'of',int(t.Day.astype(str).eq(D).sum()))
    print(g.groupby(['Pick','Side','Gate','Pace combo','Favorite'],dropna=False).result.agg(n='size',hit=lambda x:round((x=='HIT').mean()*100,2)).to_string())
    sys.exit()
d=pd.read_sql("select game_date,event_id,team,minutes,fga,fta,oreb,tov from nba where coalesce(season_window,'')!='preseason'",c)
for k in ['minutes','fga','fta','oreb','tov']: d[k]=pd.to_numeric(d[k],errors='coerce')
g=d.groupby(['event_id','game_date','team']).agg(fga=('fga','sum'),fta=('fta','sum'),oreb=('oreb','sum'),tov=('tov','sum'),mn=('minutes','sum')).reset_index()
g=g[g.fga>20]; g['pace']=(g.fga+0.44*g.fta-g.oreb+g.tov)/(g.mn/5).clip(lower=48)*48; g['gd']=pd.to_datetime(g.game_date)
day=pd.Timestamp(D); TP={t:v[v.gd<day].sort_values('gd').tail(10).pace.mean() for t,v in g.groupby('team')}
def tp(t): return TP.get(t,TP.get(AB.get(t,t),np.nan))
h=pd.read_sql("select player,game_date,minutes,pts from nba where coalesce(season_window,'')!='preseason'",c)
for k in ['minutes','pts']: h[k]=pd.to_numeric(h[k],errors='coerce')
h=h[(h.minutes>0)&(pd.to_datetime(h.game_date)<day)].drop_duplicates(['player','game_date']); h['k']=h.player.map(nm)
H=h.sort_values('game_date').groupby('k').tail(10).groupby('k').agg(mpg=('minutes','mean'),ppg=('pts','mean'))
HT=pd.read_csv('data/rosters/nba_heights.csv'); HT['k']=HT.player.map(nm); HT=HT.drop_duplicates('k').set_index('k').height_in

def team_spreads(day,sport):
    """team abbr -> that team's spread (negative = favored) from data/cache/game_lines.db (filled by scripts/fetch_game_lines.py)."""
    try:
        gl=sqlite3.connect('data/cache/game_lines.db')
        r=pd.read_sql("select home_team,away_team,spread from game_lines where sport=? and game_date=? and spread is not null",gl,params=(sport,day))
    except Exception as ex:
        print('spread lookup failed:',type(ex).__name__); return {}
    m={}
    for x in r.itertuples(): m[x.home_team]=float(x.spread); m[x.away_team]=-float(x.spread)
    for a,b in {'NY':'NYK','GS':'GSW','NO':'NOP','SA':'SAS','UTAH':'UTA','WSH':'WAS','PHX':'PHO','BKN':'BRK'}.items():
        if a in m and b not in m: m[b]=m[a]
        if b in m and a not in m: m[a]=m[b]
    if not m: print(f'NO SPREADS for {sport} {day}: run  py -3 scripts/fetch_game_lines.py --sport {sport} --date {day}  first')
    else: print(f'spreads loaded for {len(m)//1} {sport} teams')
    return m
def fav_label(s):
    if pd.isna(s): return 'n/a'
    return 'Big fav (7+)' if s<=-7 else 'Fav (4-7)' if s<=-4 else 'Big dog (7+)' if s>=7 else 'Dog (4-7)' if s>=4 else 'Close'
SPR=team_spreads(D,'NBA')
x=pd.ExcelFile(f'outputs/tiered_all_{D}.xlsx'); out={}; rows=[]
for s in x.sheet_names:
    if not s.startswith('NBA'): continue
    df=x.parse(s)
    if 'Opp' not in df.columns: continue
    df['Team pace']=df.Team.map(tp).round(2); df['Opp pace']=df.Opp.map(tp).round(2)
    fa=(df['Team pace']>=HI).astype(int)+(df['Opp pace']>=HI).astype(int); sl=(df['Team pace']<=LO).astype(int)+(df['Opp pace']<=LO).astype(int)
    df['Pace combo']=np.select([df['Team pace'].isna()|df['Opp pace'].isna(),fa==2,sl==2,fa==1],['n/a','Both fast','Both slow','One fast'],'Neither')
    k=df.Player.map(nm); df['MPG L10']=k.map(H.mpg).round(2); df['PPG L10']=k.map(H.ppg).round(2)
    df['Team spread']=df.Team.map(SPR); df['Favorite']=df['Team spread'].map(fav_label)
    df['Scorer 18+']=np.where(df['PPG L10']>=18,'Y','')
    df['Height (in)']=k.map(HT); df['Height grp']=pd.cut(df['Height (in)'],[0,76,79,200],labels=['Short','Mid','Tall']).astype(str).replace('nan','n/a')
    df['Fits theory']=np.where((df['Pace combo']=='Both fast')&((df['Height (in)']>=80)|(df['PPG L10']>=18)),'Y','')
    df['Pace+Height flag']=np.where((df['Pace combo']=='Both fast')&(df['Height grp']=='Tall'),'Tall+Fast'+np.where(df['Scorer 18+']=='Y',' scorer',''),'')
    out[s[:31]]=df
    pk='Goblin' if 'Goblin' in s else ('Demon' if 'Demon' in s else 'Standard'); side='Under' if 'Under' in s else 'Over'
    r=df.copy(); r.insert(0,'Day',D); r['Pick']=pk; r['Side']=side; r['Gate']='Gate list' if 'Gates' in s else 'Full board'
    rows.append(r[['Day','Pick','Side','Gate','Tier','Player','Team','Opp','Prop','Line','Team pace','Opp pace','Pace combo','MPG L10','PPG L10','Height (in)','Height grp','Pace+Height flag','Fits theory','Team spread','Favorite']])
with pd.ExcelWriter(f'outputs/pace_flags_{D}.xlsx') as w:
    for s,df in out.items(): df.to_excel(w,sheet_name=s,index=False)
T=pd.concat(rows); T['actual']=np.nan; T['result']=None
if os.path.exists(TR):
    old=pd.read_csv(TR,dtype={'result':object}); old=old[old.Day.astype(str)!=D]; T=pd.concat([old,T])
T.to_csv(TR,index=False)
G=pd.concat(rows); print('tagged',len(G),'props;',G['Pace combo'].value_counts().to_dict())
print(G[G['Gate']=='Gate list'].groupby(['Pick','Side','Pace combo']).size().to_string())

# add the same columns into the daily all-props workbook (NBA sheets), matched on Player+Prop+Line
import openpyxl
wp=f'outputs/PropORACLE_all_props_{D}.xlsx'
if os.path.exists(wp):
    cols=['Team pace','Opp pace','Pace combo','Height (in)','Height grp','Scorer 18+','Fits theory','Team spread','Favorite']
    wb=openpyxl.load_workbook(wp); n=0
    for ws in wb.worksheets:
        if not ws.title.startswith('NBA'): continue
        hdr=[c.value for c in ws[1]]
        if 'Player' not in hdr or 'Prop' not in hdr or 'Line' not in hdr or 'Pace combo' in hdr: continue
        pi,pr,li=hdr.index('Player'),hdr.index('Prop'),hdr.index('Line')
        src=pd.concat(out.values()).drop_duplicates(['Player','Prop','Line']); src['k']=src.Player.map(nm)
        lk=src.set_index(['k','Prop','Line'])[cols].to_dict('index')
        base=len(hdr)
        for j,cn in enumerate(cols): ws.cell(1,base+1+j,cn)
        for r in range(2,ws.max_row+1):
            v=lk.get((nm(ws.cell(r,pi+1).value),ws.cell(r,pr+1).value,ws.cell(r,li+1).value))
            if v:
                for j,cn in enumerate(cols):
                    x=v[cn]; ws.cell(r,base+1+j,None if (isinstance(x,float) and np.isnan(x)) else x)
        n+=1
    wb.save(wp); print('added pace/height/fits-theory columns to',n,'NBA sheets in',wp)
else: print('no workbook',wp,'- run claude_all_props_xlsx.py first, then rerun this')
