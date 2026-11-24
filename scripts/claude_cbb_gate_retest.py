"""Re-test last season's CBB gate cells (outputs/prop_tiers_cbb_2026-11-11.csv) on THIS season: boards 2026-11-01..<last day with results>.
Features use only games strictly before the prop day. 'Season' = games since 2026-11-01 (need >=5). Goblin counted on all rungs AND top rung per player-prop-day."""
import sys,glob,sqlite3,pandas as pd,numpy as np
sys.path.insert(0,'scripts'); from claude_gamelog import nm
CC=['pts','reb','ast','stl','blk','tov','fg3m']
g1=pd.read_parquet('data/game_logs/cbb_player_games.parquet')[['player','game_date','minutes']+CC]
g2=pd.read_sql(f"select player,game_date,minutes,{','.join(CC)} from cbb",sqlite3.connect('data/cache/proporacle_ref.db'))
h=pd.concat([g1,g2],ignore_index=True); h['game_date']=pd.to_datetime(h.game_date); h=h[pd.to_numeric(h.minutes,errors='coerce')>0].drop_duplicates(['player','game_date'])
for c in CC: h[c]=pd.to_numeric(h[c],errors='coerce')
h['pr']=h.pts+h.reb; h['pa']=h.pts+h.ast; h['pra']=h.pts+h.reb+h.ast; h['ra']=h.reb+h.ast; h['k']=h.player.map(nm); H={k:v.sort_values('game_date') for k,v in h.groupby('k')}
M={'Points':'pts','Rebounds':'reb','Assists':'ast','Steals':'stl','Blocked Shots':'blk','Turnovers':'tov','3-PT Made':'fg3m','Pts+Rebs':'pr','Pts+Asts':'pa','Pts+Rebs+Asts':'pra','Rebs+Asts':'ra'}
S0=pd.Timestamp('2026-11-01'); last=h.game_date.max(); rows=[]
for f in sorted(glob.glob('../PropORACLE_main_cp/outputs/2026-11-*/cbb/step1_cbb.csv')):
    D=pd.Timestamp(f.split('outputs/')[1][:10])
    if D>last: continue
    b=pd.read_csv(f,encoding='utf-8-sig'); b=b[(b.get('duration','Full')=='Full')&b.odds_type.isin(['goblin','standard'])&b.stat_type.isin(M)].drop_duplicates(['player','stat_type','odds_type','line'])
    for r in b.itertuples():
        v=H.get(nm(r.player)); st=M[r.stat_type]
        if v is None: continue
        pre=v[v.game_date<D]; post=v[(v.game_date==D)]
        if len(pre)<20 or post.empty: continue
        s=pre[st].values; ss=pre[pre.game_date>=S0][st].values; a=post[st].iloc[0]
        for side in (['Over','Under'] if r.odds_type=='standard' else ['Over']):
            if a==r.line: continue
            sg=1 if side=='Over' else -1; L=r.line
            hit=lambda x:(np.mean((x>L) if side=='Over' else (x<L)) if len(x) else np.nan)
            gap=lambda x:(sg*(np.mean(x)-L)/L if len(x) else np.nan)
            sd=np.std(ss) if len(ss)>=5 else np.nan
            rows.append(dict(day=D,player=r.player,stat=st,prop=r.stat_type,pick=r.odds_type.title(),side=side,line=L,hit=int((a>L) if side=='Over' else (a<L)),
              l5=int(np.sum((s[-5:]>L) if side=='Over' else (s[-5:]<L))),l10=hit(s[-10:]),l20=hit(s[-20:]),sea=hit(ss) if len(ss)>=5 else np.nan,sea_n=len(ss),
              g5=gap(s[-5:]),g10=gap(s[-10:]),g15=gap(s[-15:]),gs=gap(ss) if len(ss)>=5 else np.nan,sdg=(sg*(np.mean(ss)-L)/sd if sd and sd>0 else np.nan)))
D=pd.DataFrame(rows); D['toprung']=D.line==D.groupby(['day','player','stat','pick','side']).line.transform('max'); D.to_pickle('/tmp/cbb_retest.pkl')
print(len(D),'graded legs',D.day.min().date(),D.day.max().date(),D.day.nunique(),'days')
G={('Goblin','pts','Over'):lambda x:(x.l5==5)&(x.gs>=.10),('Goblin','pra','Over'):lambda x:(x.l20>=.7)&(x.gs>=.20),('Goblin','pr','Over'):lambda x:x.sea>=.55,
('Goblin','ast','Over'):lambda x:(x.sea>=.65)&(x.sdg>=.5),('Goblin','ra','Over'):lambda x:x.gs>=.05,('Goblin','reb','Over'):lambda x:(x.sea>=.65)&(x.g10>=.20),
('Goblin','stl','Over'):lambda x:(x.sea>=.65)&(x.g5>=.20),('Goblin','tov','Over'):lambda x:(x.sea>=.55)&(x.g5>=.05),('Goblin','fg3m','Over'):lambda x:x.l10>=.8,('Goblin','blk','Over'):lambda x:x.g15>=.20,
('Standard','reb','Under'):lambda x:(x.sea>=.65)&(x.g15>=.05),('Standard','pr','Under'):lambda x:x.l20>=.7,('Standard','ast','Over'):lambda x:x.g10>=.10,('Standard','pra','Over'):lambda x:(x.l20>=.6)&(x.gs>=.05),
('Standard','pts','Under'):lambda x:(x.sea>=.65)&(x.g10>=.05),('Standard','pa','Under'):lambda x:(x.l5>=4)&(x.g5>=.05),('Standard','ra','Under'):lambda x:x.l20>=.7}
out=[]
for (pk,st,sd),f in G.items():
    x=D[(D.pick==pk)&(D.stat==st)&(D.side==sd)]
    if x.empty: continue
    m=f(x).fillna(False); y=x[m]; t=y[y.toprung] if pk=='Goblin' else y
    out.append(dict(pick=pk,prop=st,side=sd,base_n=len(x),base_hit=round(x.hit.mean()*100,2),gate_n=len(y),gate_hit=round(y.hit.mean()*100,2) if len(y) else np.nan,top_n=len(t),top_hit=round(t.hit.mean()*100,2) if len(t) else np.nan))
R=pd.DataFrame(out); print(R.to_string(index=False)); R.to_csv('data/reports/cbb_gate_retest_2026-11-20.csv',index=False)
