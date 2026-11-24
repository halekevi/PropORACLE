import _r2,sys,pandas as pd,numpy as np
D=sys.argv[1]; B=f'../PropORACLE_main_cp/outputs/{D}/'
base=pd.read_excel(f'outputs/tiered_all_{D}.xlsx',sheet_name=None); out={}
out['Read me']=pd.DataFrame({'Read me':[f'All props pulled for {D}. NBA/CBB: every prop with L5/L10/L20, tier, minutes, injury; Gates sheets = props clearing the L5/L10/L20 gates. NBA includes attempts/makes props posted so far; NBA 1Q/1H sheets appear when those boards are pulled.','Tier F (unverified) = no tier rule backs it. Goblin, Demon, Standard are separate sheets. Numbers rounded to 2 decimals.','NHL/Soccer/Tennis L5/L10 are pipeline hit rates at the offered line (0-1); no L20 for Soccer/Tennis. CFB L5/L10 are counts.','Injury list captured about 12:13 AM ET; re-check before locking picks.']})
for k,v in base.items():
    if k!='Read me': out[k]=v
nhl=pd.read_csv(B+'nhl/step5_nhl_hit_rates.csv',low_memory=False)
n=pd.DataFrame({'Player':nhl.player_name,'Team':nhl.get('team'),'Opp':nhl.opponent,'Prop':nhl.stat_type,'Line':nhl.line_score,'Pick type':nhl.pick_type.astype(str).str.title()})
for w in ['L5','L10']:
    n[f'{w} Over']=pd.to_numeric(nhl[f'over_{w}'],errors='coerce'); n[f'{w} Under']=pd.to_numeric(nhl[f'under_{w}'],errors='coerce'); n[f'{w} sample']=pd.to_numeric(nhl[f'sample_{w}'],errors='coerce')
n['L20 Over rate']=pd.to_numeric(nhl.get('hit_rate_over_L20'),errors='coerce'); n['Season Over rate']=pd.to_numeric(nhl.get('hit_rate_over_season'),errors='coerce')
def split(g,name):
    for pt in ['Goblin','Demon','Standard']:
        x=g[g['Pick type']==pt]
        if len(x): out[f'{name} {pt}']=x.drop(columns='Pick type')
split(n,'NHL')
def gen(f,pl,pr,li,op,name):
    d=pd.read_csv(B+f,low_memory=False); g=pd.DataFrame({'Player':d[pl],'Team':d.get('team'),'Opp':d[op],'Prop':d[pr],'Line':d[li],'Pick type':d.pick_type.astype(str).str.title()})
    for w in ['5','10']:
        g[f'L{w} Over rate']=pd.to_numeric(d[f'line_hit_rate_over_ou_{w}'],errors='coerce'); g[f'L{w} Under rate']=pd.to_numeric(d[f'line_hit_rate_under_ou_{w}'],errors='coerce')
    split(g,name)
gen('soccer/step5_soccer_hit_rates.csv','player','prop_norm','line','opp_team','Soccer'); gen('tennis/step5_tennis_hit_rates.csv','player_name','prop_norm','line_score','opp_team','Tennis')
cf=pd.read_excel(B+f'cfb/step8_cfb_direction_clean_{D}.xlsx',sheet_name='ALL')
c=cf[['Player','Pos','Team','Opp','Prop','Pick Type','Line','Direction','Hit Rate L5','Hit Rate L10','L5 Over','L5 Under','L10 Over','L10 Under','Def Tier','Game Time']]
for pt in ['Goblin','Demon','Standard']:
    x=c[c['Pick Type']==pt]
    if len(x): out[f'CFB {pt}']=x.drop(columns='Pick Type')
f=f'outputs/PropORACLE_all_props_{D}.xlsx'
with pd.ExcelWriter(f,engine='xlsxwriter') as w:
    for k,v in out.items(): v.to_excel(w,sheet_name=k[:31],index=False); w.sheets[k[:31]].set_column(0,30,16); w.sheets[k[:31]].freeze_panes(1,0)
print({k[:31]:len(v) for k,v in out.items() if k.startswith('NBA')})
