"""Standard NBA board (Nov 6) with L5/L10/L15/L20 hit counts both sides + spread. Consistency = worst hit% across the four windows."""
import pandas as pd,numpy as np,re,os,unicodedata,sys
D=sys.argv[1] if len(sys.argv)>1 else '2026-11-06'
_u=lambda s:''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c))
nm=lambda s:re.sub(r'\b(jr|sr|ii|iii|iv)\b','',re.sub(r'[^a-z ]','',_u(s).lower().replace('.','').replace("'",''))).strip()
g=pd.read_parquet('data/game_logs/nba_player_games.parquet')
g=g[g.phase.astype(str).str.upper()!='PRESEASON']; g=g[~g.is_dnp.astype(bool)]
g=g[g.minutes>0]; g['game_date']=pd.to_datetime(g.game_date)
ra=pd.concat([pd.read_csv('data/game_logs/nba_patch_claude_rest_a.csv'),pd.read_csv('data/game_logs/nba_patch_claude_rest_b.csv')],ignore_index=True)
ra['pf']=[int(c) for c in open('data/game_logs/nba_patch_pf_rest.txt').read().strip()]
pt=pd.concat([pd.read_csv('data/game_logs/nba_patch_claude_20261031_20261102.csv'),pd.read_csv('data/game_logs/nba_patch_claude_board_20261031_20261102.csv'),ra,pd.read_csv('data/game_logs/nba_patch_claude_20261104_phx_mia.csv'),pd.read_csv('data/game_logs/nba_patch_claude_20261104_today_teams.csv')],ignore_index=True).fillna(0)
pt['game_date']=pd.to_datetime(pt.game_date.astype(str),format='%Y%m%d')
for a,b in [('pra',['pts','reb','ast']),('pr',['pts','reb']),('pa',['pts','ast']),('ra',['reb','ast']),('bs',['blk','stl'])]: pt[a]=pt[b].sum(axis=1)
g['k']=g.player.map(nm); pt['k']=pt.player.map(nm); pt=pt.drop_duplicates(['k','game_date'],keep='last')
g=pd.concat([g,pt],ignore_index=True).drop_duplicates(['k','game_date'],keep='last').sort_values(['k','game_date'])
g['fg2m']=g.fgm-g.fg3m; g['fg2a']=g.fga-g.fg3a
mp={'Points':'pts','Rebounds':'reb','Assists':'ast','3-PT Made':'fg3m','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','Pts+Rebs+Asts':'pra','Pts+Rebs':'pr','Pts+Asts':'pa','Rebs+Asts':'ra','Blks+Stls':'bs','FG Made':'fgm','FG Attempted':'fga','3-PT Attempted':'fg3a','Free Throws Made':'ftm','Free Throws Attempted':'fta','Personal Fouls':'pf','Two Pointers Made':'fg2m','Two Pointers Attempted':'fg2a'}
b=pd.read_csv(f'../PropORACLE_main_cp/outputs/{D}/nba/step1_pp_props_today.csv',low_memory=False)
b=b[(b.pick_type=='Standard')&~b.player.str.contains(r'\+')].drop_duplicates(['player','prop_type','line'])
by={k:v for k,v in g.groupby('k')}
rows=[]
for r in b.itertuples():
    c=mp.get(r.prop_type); v=by.get(nm(r.player))
    if c is None or v is None or len(v)<10: continue
    s=v[c].astype(float).values; last=v.game_date.max()
    for side in 'OU':
        h={}
        for n in (5,10,15,20):
            w=s[-n:]; h[n]=(int((w>r.line).sum()) if side=='O' else int((w<r.line).sum()),len(w))
        pc=[h[n][0]/h[n][1] for n in h]
        sg=1 if side=='O' else -1
        av={n:s[-n:].mean() for n in (5,10,15,20)}
        hall=((s>r.line) if side=='O' else (s<r.line)).mean(); aall=sg*(s.mean()-r.line)
        cand=np.arange(np.floor(s[-20:].min())-.5,s[-20:].max()+1.5,.5)
        def ok(L):
            f=(lambda w:(w>L).sum()) if side=='O' else (lambda w:(w<L).sum())
            return f(s[-5:])>=4 and f(s[-10:])>=8 and f(s[-15:])>=11 and f(s[-20:])>=15
        good=[L for L in cand if ok(L)]
        go=(max(good) if side=='O' else min(good)) if good else np.nan
        room=(go-r.line if side=='O' else r.line-go) if good else np.nan
        rows.append(dict(player=r.player,team=r.team,opp=r.opp_team,side=side,prop=r.prop_type,line=r.line,
            L5=h[5][0],L10=f'{h[10][0]}/{h[10][1]}',L15=f'{h[15][0]}/{h[15][1]}',L20=f'{h[20][0]}/{h[20][1]}',
            n20=h[20][1],worst=min(pc),mean_pct=np.mean(pc),
            avg5=av[5],avg10=av[10],avg15=av[15],avg20=av[20],gap5=sg*(av[5]-r.line),gap10=sg*(av[10]-r.line),gap15=sg*(av[15]-r.line),gap20=sg*(av[20]-r.line),med20=np.median(s[-20:]),sd20=s[-20:].std(),
            hs=hall,aS=aall,nS=len(s),go_line=go,room=room,gap=sg*(s[-10:].mean()-r.line)/r.line,close=int((abs(s[-20:]-r.line)<=max(0.5,.1*r.line)).sum()),last=last.date()))
t=pd.DataFrame(rows); t=t[(t.n20>=15)]
t['allpos']=(t[['gap5','gap10','gap15','gap20']]>0.05).all(axis=1)
t['z']=np.minimum(t.gap10,t.gap20)/t.sd20.clip(lower=.5)
t['hitw']=.15*t.L5/5+.3*t.L10.str.split('/').str[0].astype(float)/t.L10.str.split('/').str[1].astype(float)+.25*t.L15.str.split('/').str[0].astype(float)/15+.3*t.L20.str.split('/').str[0].astype(float)/t.L20.str.split('/').str[1].astype(float)
t['score']=t.hitw+0.15*t.z.clip(-1,1.5)+0.05*t.allpos
ALL=t.copy(); t=t[t.room>=0]
t=t.sort_values('score',ascending=False)
t=t[t.groupby('player').cumcount()<2]; t=t[t.groupby(['prop','side']).cumcount()<4]
ALL.to_csv(f"outputs/nba_consistency_all_{D}.csv",index=False)
t.to_csv(f'outputs/nba_consistency_{D}.csv',index=False)
print(len(t),'rows; latest log date',g.game_date.max().date())
for i,r in enumerate(t.head(30).itertuples(),1):
    print(f'| {i} | {r.player} | go {r.go_line:g} | {r.side} {r.prop} {r.line:g} | {r.avg5:.1f} ({r.gap5:+.1f}) | {r.avg10:.1f} ({r.gap10:+.1f}) | {r.avg15:.1f} ({r.gap15:+.1f}) | {r.avg20:.1f} ({r.gap20:+.1f}) | {r.sd20:.1f} |')
print('\n-- players with most props at worst>=.7 --')
x=t[t.worst>=.7].groupby('player').size().sort_values(ascending=False).head(10); print(x.to_string())
