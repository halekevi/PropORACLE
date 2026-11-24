"""python scripts/claude_line_change_log.py <day>   (run after each fetch snapshot; compares FIRST vs LAST saved snap of the day)
Writes outputs/line_changes_<day>.xlsx, replaces that day in outputs/line_change_log.csv (per-prop rows) and prints the daily + running tally."""
import sys,glob,pandas as pd
D=sys.argv[1]; sn=sorted(glob.glob(f'outputs/tiered_all_{D}_snap_*.xlsx'),key=lambda f:(int(f.split('snap_')[1][:4])<1200,int(f.split('snap_')[1][:4])))
if len(sn)<2: sys.exit(f'need 2+ snapshots for {D}, have {len(sn)}')
def load(f):
    x=pd.ExcelFile(f); o=[]
    for s in x.sheet_names:
        if not s.startswith('NBA ') or 'Gates' in s: continue
        d=x.parse(s); d['pick']=s[4:].split()[0]; d['side']=s[4:].split()[1]; o.append(d)
    return pd.concat(o).drop_duplicates(['Player','Prop','pick','side'])
I,L=load(sn[0]),load(sn[-1]); k=['Player','Prop','pick','side']
M=I.merge(L,on=k,how='outer',suffixes=('_i','_l'),indicator=True)
def cat(r):
    if r._merge=='left_only': return 'Removed'
    if r._merge=='right_only': return 'New'
    if r.Line_i==r.Line_l: return 'Same'
    return 'Against (harder)' if ((r.Line_l>r.Line_i)==(r.side=='Over')) else 'For (easier)'
M['Change']=M.apply(cat,axis=1)
M['gate_pass']=(M.get('Status_i')=='PASS')&M.get('Tier_i').isin(['S','A','B'])
M.insert(0,'day',D); M['first_snap']=sn[0].split('snap_')[1][:4]; M['last_snap']=sn[-1].split('snap_')[1][:4]
C=M[['day','Player','Prop','pick','side','Line_i','Line_l','Change','gate_pass','first_snap','last_snap']]
f='outputs/line_change_log.csv'
try: C=pd.concat([pd.read_csv(f).query('day!=@D'),C])
except FileNotFoundError: pass
C.to_csv(f,index=False)
ch=C[C.Change.isin(['Against (harder)','For (easier)','Removed'])]
with pd.ExcelWriter(f'outputs/line_changes_{D}.xlsx') as w:
    ch[ch.day==D].to_excel(w,sheet_name='All changes',index=False); ch[(ch.day==D)&ch.gate_pass].to_excel(w,sheet_name='Our gate picks',index=False)
print(C.groupby(['day','pick','Change']).size().unstack(fill_value=0).to_string())
print('\nOUR gate picks, running:'); print(C[C.gate_pass].groupby(['day','Change']).size().unstack(fill_value=0).to_string())
