#!/usr/bin/env python3
"""Board change tracker (read-only; never touches PrizePicks).
Run after any fetch:  py -3 scripts/claude_board_diff.py <day>
 1) archives this fetch's step1 board for NBA and CBB  -> data/cache/board_archive/<day>/<sport>_<HHMM>.csv
 2) diffs it against the previous archive of that day   -> removed / added / line-moved props
 3) checks the day's gate lists (outputs/PropORACLE_all_props_<day>.xlsx) against the CURRENT board and shows
    gate-list props that are no longer on the board (or whose line moved).
 Appends every change to outputs/board_changes.csv (day, sport, seen_at, change, player, prop, pick, old_line, new_line, on_gate_list)."""
import sys,os,glob,unicodedata
import pandas as pd,numpy as np
D=sys.argv[1]; MAIN='../PropORACLE_main_cp/outputs/'+D
def nm(s): return ''.join(c for c in unicodedata.normalize('NFD',str(s)) if not unicodedata.combining(c)).lower().replace('.','').replace("'",'').replace(' jr','').replace(' iii','').replace(' ii','').strip()
SRC={'NBA':('nba/step1_pp_props_today.csv','player','prop_type','pick_type'),'CBB':('cbb/step1_cbb.csv','player','stat_type','odds_type')}
CBBMAP={'standard':'Standard','goblin':'Goblin','demon':'Demon'}
def load(sp):
    f,pc,prc,pkc=SRC[sp]; p=os.path.join(MAIN,f)
    if not os.path.exists(p): return None,None
    d=pd.read_csv(p,encoding='utf-8-sig'); fa=pd.to_datetime(d.get('fetched_at'),utc=True,errors='coerce').max()
    pk=d[pkc].astype(str).str.strip().str.capitalize().replace({'Standard':'Standard'}); pk=pk.map(lambda v:CBBMAP.get(v.lower(),v))
    out=pd.DataFrame({'player':d[pc],'prop':d[prc],'pick':pk,'line':pd.to_numeric(d['line'],errors='coerce'),'team':d.get('team'),'updated':d.get('pp_updated_at')})
    out['k']=out.player.map(nm)+'|'+out.prop.astype(str)+'|'+out.pick.astype(str)
    return out.dropna(subset=['line']),fa
gate=set()
wp=f'outputs/PropORACLE_all_props_{D}.xlsx'
if os.path.exists(wp):
    x=pd.ExcelFile(wp)
    for s in x.sheet_names:
        if 'Gates' in s and s[:3] in ('NBA','CBB'):
            df=x.parse(s); pk='Goblin' if 'Goblin' in s else ('Demon' if 'Demon' in s else 'Standard'); side='Under' if 'Under' in s else 'Over'
            for r in df.itertuples(): gate.add((s[:3],nm(r.Player)+'|'+str(r.Prop)+'|'+pk,float(r.Line),side))
LOG='outputs/board_changes.csv'; rows=[]
for sp in ['NBA','CBB']:
    cur,fa=load(sp)
    if cur is None: print(sp,'no board file'); continue
    ts=(fa.tz_convert('America/New_York') if pd.notna(fa) else pd.Timestamp.now(tz='America/New_York')).strftime('%H%M')
    adir=f'data/cache/board_archive/{D}'; os.makedirs(adir,exist_ok=True); fp=f'{adir}/{sp}_{ts}.csv'
    prev_files=sorted(glob.glob(f'{adir}/{sp}_*.csv')); prev_files=[p for p in prev_files if p!=fp]
    new_archive=not os.path.exists(fp)
    if new_archive: cur.drop(columns=['k']).to_csv(fp,index=False)
    print(f'\n== {sp}: board fetched {ts[:2]}:{ts[2:]} ET, {len(cur)} props ({"archived" if new_archive else "already archived"} {os.path.basename(fp)})')
    if prev_files and new_archive:
        pv=pd.read_csv(prev_files[-1]); pv['k']=pv.player.map(nm)+'|'+pv.prop.astype(str)+'|'+pv.pick.astype(str)
        a=pv.groupby('k').line.apply(list); b=cur.groupby('k').line.apply(list)
        pts=os.path.basename(prev_files[-1])[len(sp)+1:-4]; print(f'   compared with {pts[:2]}:{pts[2:]} ET board ({len(pv)} props)')
        for k in sorted(set(a.index)|set(b.index)):
            old=a.get(k); new=b.get(k)
            if old is None: ch='ADDED'; ol=None; nl=new[0]
            elif new is None: ch='REMOVED'; ol=old[0]; nl=None
            elif sorted(old)!=sorted(new): ch='LINE MOVED'; ol=old[0]; nl=new[0]
            else: continue
            pl,pr,pk=k.split('|'); lines=[l for l in (ol,nl) if l is not None]
            ong=any((sp,k.split('|')[0]+'|'+pr+'|'+pk,float(l),s) in gate for l in lines for s in ('Over','Under'))
            rows.append(dict(day=D,sport=sp,seen_at=ts,change=ch,player=k.split('|')[0],prop=pr,pick=pk,old_line=ol,new_line=nl,on_gate_list='Y' if ong else ''))
    elif not prev_files: print('   first archive of the day - baseline only')
    # gate list vs current board
    cb=cur.groupby('k').line.apply(set).to_dict(); miss=[]; moved=[]
    for (s,k,l,side) in sorted(gate):
        if s!=sp: continue
        kk=k
        if kk not in cb: miss.append((k,l,side))
        elif l not in cb[kk]: moved.append((k,l,side,sorted(cb[kk])))
    print(f'   gate-list props no longer on the board: {len(miss)} | line changed: {len(moved)} (of {sum(1 for g in gate if g[0]==sp)} gate rows)')
    for k,l,side in miss[:60]: print('     GONE ',k.replace('|',' | '),l,side)
    for k,l,side,nl in moved[:60]: print('     MOVED',k.replace('|',' | '),l,side,'->',nl)
    for k,l,side in miss: rows.append(dict(day=D,sport=sp,seen_at=ts,change='GATE ROW GONE',player=k.split('|')[0],prop=k.split('|')[1],pick=k.split('|')[2],old_line=l,new_line=None,on_gate_list='Y'))
    for k,l,side,nl in moved: rows.append(dict(day=D,sport=sp,seen_at=ts,change='GATE ROW MOVED',player=k.split('|')[0],prop=k.split('|')[1],pick=k.split('|')[2],old_line=l,new_line=nl[0],on_gate_list='Y'))
if rows:
    N=pd.DataFrame(rows)
    if os.path.exists(LOG):
        o=pd.read_csv(LOG); N=pd.concat([o,N]).drop_duplicates(['day','sport','seen_at','change','player','prop','pick','old_line','new_line'])
    N.to_csv(LOG,index=False); print('\nlogged',len(rows),'changes ->',LOG)
