"""Rank tomorrow's board by L5 avg-vs-line gap. Usage: python3 scripts/claude_l5_gap_rank.py YYYY-MM-DD
Keeps props with L5 hits >=4/5 (Over: value>line, Under: value<line). gap = L5 avg - line (Over) or line - L5 avg (Under); z = gap / SD of last 20 games.
Writes outputs/l5_gap_rank_<day>.xlsx : Goblin, Demon, Standard (Over+Under together)."""
import pandas as pd,numpy as np,sqlite3,sys,re,warnings; warnings.filterwarnings('ignore'); sys.path.insert(0,'scripts')
from claude_gamelog import nm
day=sys.argv[1]; D0=pd.Timestamp(day)
src=open('scripts/claude_tier_today.py',encoding='utf-8').read()
MPN=eval(re.search(r"MPN=(\{.*?\})\n",src,re.S).group(1))
bd=pd.read_csv(f'../PropORACLE_main_cp/outputs/{day}/nba/step1_pp_props_today.csv',encoding='utf-8-sig')
bd['stat']=bd.prop_type.map(MPN); bd['pt']=bd.pick_type.str.capitalize()
bd=bd[bd.stat.notna()&bd.pt.isin(['Goblin','Standard','Demon'])&~bd.player.astype(str).str.contains(r' \+ |/',regex=True)].copy()
cols=sorted(set(bd.stat)); db=sqlite3.connect('data/cache/proporacle_ref.db')
h=pd.read_sql(f"select player,game_date,minutes,{','.join(cols)} from nba where coalesce(season_window,'')!='preseason'",db)
for c in cols+['minutes']: h[c]=pd.to_numeric(h[c],errors='coerce')
h=h[h.minutes>0].drop_duplicates(['player','game_date']); h['gd']=pd.to_datetime(h.game_date); h=h[h.gd<D0]; h['k']=h.player.map(nm)
G={k:v.sort_values('gd') for k,v in h.groupby('k')}
rows=[]
for r in bd.itertuples():
    g=G.get(nm(r.player));
    if g is None: continue
    v=g[r.stat].dropna().values
    if len(v)<5: continue
    l5=v[-5:]; l10=v[-10:]; l20=v[-20:]; L=float(r.line)
    for side in (['Over','Under'] if r.pt=='Standard' else ['Over']):
        hit=(lambda a:(a>L).sum() if side=='Over' else (a<L).sum())
        h5=int(hit(l5))
        if h5<4: continue
        gap=(l5.mean()-L) if side=='Over' else (L-l5.mean())
        sd=l20.std(ddof=1) if len(l20)>=5 else np.nan
        rows.append(dict(Pick=r.pt,Side=side,Player=r.player,Team=r.team,Opp=getattr(r,'opp_team',''),Prop=r.prop_type,Line=L,L5=f'{h5}/5',
          L5_avg=round(l5.mean(),2),Gap=round(gap,2),SD20=round(sd,2) if sd==sd else np.nan,Gap_in_SD=round(gap/sd,2) if sd and sd==sd and sd>0 else np.nan,
          L10=f'{int(hit(l10))}/{len(l10)}',L20=f'{int(hit(l20))}/{len(l20)}',Games=len(v),Step='2s' if r.stat in('pts','pr','pa','pra','fga','fgm') else '1s'))
o=pd.DataFrame(rows)
o['L5n']=o.L5.str[0].astype(int)
o=o.sort_values(['L5n','Gap_in_SD'],ascending=[False,False]).drop(columns='L5n')
with pd.ExcelWriter(f'outputs/l5_gap_rank_{day}.xlsx',engine='openpyxl') as w:
    for pk in ['Goblin','Demon','Standard']:
        d=o[o.Pick==pk].drop(columns='Pick')
        d=d.sort_values('Gap_in_SD',ascending=False); d.to_excel(w,sheet_name=pk,index=False,freeze_panes=(1,0))
        ws=w.sheets[pk]; ws.auto_filter.ref=ws.dimensions
        for i,c in enumerate(d.columns): ws.column_dimensions[chr(65+i)].width=max(8,min(26,int(d[c].astype(str).str.len().max())+2))
print({pk:len(o[o.Pick==pk]) for pk in ['Goblin','Demon','Standard']})
