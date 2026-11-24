import _r2,sys,re,unicodedata,pandas as pd,numpy as np
day=sys.argv[1]
def nm(s):
    s=unicodedata.normalize('NFKD',str(s)).encode('ascii','ignore').decode().lower(); s=re.sub(r"[.'’,]",'',s); s=re.sub(r'\b(jr|sr|ii|iii|iv)\b','',s); return re.sub(r'\s+',' ',s).strip()
import os
if os.path.exists('data/game_logs/cfb_player_games_v2.csv'): g=pd.read_csv('data/game_logs/cfb_player_games_v2.csv',dtype={'athlete_id':str}); print('using v2 logs (athlete-id, appearances only)'); g['gd']=pd.to_datetime(g.game_date.astype(str),errors='coerce'); g=g[g.gd<pd.Timestamp(day)]
else: g=pd.read_parquet('data/game_logs/cfb_player_games.parquet'); g=g[~g.is_dnp.astype(bool)].copy(); print('WARNING old logs (no zero games)'); g['gd']=pd.to_datetime(g.game_date.astype(str),errors='coerce'); g=g[g.gd<pd.Timestamp(day)]
for c in ['pass_yds','rush_yds','rec_yds','rec','rush_att','pass_att','pass_td','rush_td','rec_td']: g[c]=pd.to_numeric(g[c],errors='coerce')
g['py_ry']=g.pass_yds+g.rush_yds; g['ry_cy']=g.rush_yds+g.rec_yds; g['k']=g.athlete_id.astype(str) if 'athlete_id' in g else g.player.map(nm); G={k:v.sort_values('gd') for k,v in g.groupby('k')}
# Board rows carry names only: a name shared by several athletes resolves by board team (latest log team).
_ids=g.groupby(g.player.map(nm)).k.unique(); _team=g.sort_values('gd').groupby('k').team.last().astype(str).str.upper()
def key_for(name,team):
    ks=list(_ids.get(nm(name),[]))
    if len(ks)<=1: return ks[0] if ks else None
    m=[k for k in ks if _team.get(k)==str(team).upper()]
    return m[0] if len(m)==1 else None
M={'pass yards':'pass_yds','rush yards':'rush_yds','receiving yards':'rec_yds','receptions':'rec','rush tds':'rush_td','pass tds':'pass_td','rec tds':'rec_td','pass+rush yds':'py_ry','rush+rec yds':'ry_cy','rush attempts':'rush_att','pass attempts':'pass_att'}
v=pd.read_excel(f'../PropORACLE_main_cp/outputs/{day}/cfb/step8_cfb_direction_clean_{day}.xlsx',sheet_name='ALL')
rows=[];nolog=0
for r in v.to_dict('records'):
    col=M.get(str(r['Prop']).lower()); x=G.get(key_for(r['Player'],r.get('Team','')))
    if col is None or x is None: nolog+=1; continue
    a=x[col].dropna().values
    if len(a)<10: continue
    for s in ['Over','Under']:
        if s=='Under' and r['Pick Type']!='Standard': continue
        L=r['Line']; hit=(a>L) if s=='Over' else (a<L)
        h5=hit[-5:].sum(); h10=hit[-10:].sum(); n20=min(len(a),20); h20=hit[-20:].sum()
        need5=5 if r['Pick Type']=='Goblin' else 4
        if h5>=need5 and h10>=8:
            ok20=(len(a)>=20 and h20>=15)
            seas=int((x.gd>=pd.Timestamp('2026-08-01')).sum())
            rows.append(dict(Player=r['Player'],Team=r['Team'],Opp=r['Opp'],Prop=r['Prop'],Type=r['Pick Type'],Line=L,Side=s,L5=f'{int(h5)}/5',L10=f'{int(h10)}/10',L20=(f'{int(h20)}/20' if len(a)>=20 else f'{int(h20)}/{len(a)} (short)'),Games_total=len(a),Games_2026=seas,L20_gate='PASS' if ok20 else ('short history' if len(a)<20 else 'FAIL'),Def=r['Def Tier'],Time=str(r['Game Time'])[11:16]))
R=pd.DataFrame(rows); R['Tier']='F (unverified)'; R.to_excel(f'outputs/gates_cfb_{day}.xlsx',index=False)
print('no log/unmapped props:',nolog,'| L5+L10 pass:',len(R)); print(R.groupby(['Type','Side','L20_gate']).size().to_string())
P=R[R.L20_gate=='PASS'].sort_values(['Type','Side','Prop','Player'])
for d in P.to_dict('records'): print(f"{d['Type'][0]}|{d['Player']}|{d['Team']} v {d['Opp']}|{d['Prop']}|{d['Line']:g}|{d['Side']}|{d['L5']} {d['L10']} {d['L20']}|g26 {d['Games_2026']}|{d['Def']}|{d['Time']}")
