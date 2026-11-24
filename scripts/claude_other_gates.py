import _r2,pandas as pd,numpy as np,sys
day=sys.argv[1]; B=f'../PropORACLE_main_cp/outputs/{day}/'
def gate(df,pcol,l5,l10,l20,out,sp):
    rows=[]
    for s in ['Over','Under']:
        a=df[f'{l5}_{s}']; b=df[f'{l10}_{s}']; c=df[f'{l20}_{s}'] if l20 else pd.Series(np.nan,index=df.index)
        need5=np.where(df.pt.eq('Goblin'),1.0,0.8)
        ok=(a>=need5-1e-9)&(b>=0.8-1e-9)&((c>=0.75-1e-9)|c.isna())
        if s=='Under': ok=ok&df.pt.eq('Standard')   # Goblin/Demon are Over-only
        g=df[ok].copy(); g['Side']=s; g['L5 rate']=a[ok]; g['L10 rate']=b[ok]; g['L20 rate']=c[ok] if l20 else 'n/a in pipeline'
        rows.append(g)
    r=pd.concat(rows); r['Sport']=sp; return r
res={}
# NHL
d=pd.read_csv(B+'nhl/step5_nhl_hit_rates.csv',low_memory=False); d['pt']=d.pick_type.astype(str).str.title()
for w,sm in [('L5','sample_L5'),('L10','sample_L10')]:
    d[f'{w}_Over']=pd.to_numeric(d[f'over_{w}'],errors='coerce')/pd.to_numeric(d[sm],errors='coerce'); d[f'{w}_Under']=pd.to_numeric(d[f'under_{w}'],errors='coerce')/pd.to_numeric(d[sm],errors='coerce')
d['L20_Over']=pd.to_numeric(d['hit_rate_over_L20'],errors='coerce'); d['L20_Under']=1-d['L20_Over']
n=gate(d,'player_name','L5','L10','L20',None,'NHL'); n=n.rename(columns={'player_name':'Player','stat_type':'Prop','line_score':'Line','opponent':'Opp'})
res['NHL']=n
for sp,f,pl,pr,li,op in [('Soccer','soccer/step5_soccer_hit_rates.csv','player','prop_norm','line','opp_team'),('Tennis','tennis/step5_tennis_hit_rates.csv','player_name','prop_norm','line_score','opp_team')]:
    d=pd.read_csv(B+f,low_memory=False); d['pt']=d.pick_type.astype(str).str.title()
    for w in ['5','10']:
        for s in ['over','under']: d[f'L{w}_{s.title()}']=pd.to_numeric(d[f'line_hit_rate_{s}_ou_{w}'],errors='coerce')
    r=gate(d,pl,'L5','L10',None,None,sp).rename(columns={pl:'Player',pr:'Prop',li:'Line',op:'Opp'}); res[sp]=r
with pd.ExcelWriter(f'outputs/gates_other_sports_{day}.xlsx') as w:
    for sp,r in res.items():
        for pt in ['Goblin','Demon','Standard']:
            x=r[r.pt==pt]
            if len(x): x[['Player','Opp','Prop','Line','Side','L5 rate','L10 rate','L20 rate']].assign(Tier='F (unverified)').to_excel(w,sheet_name=f'{sp} {pt}',index=False)
        print(sp,{pt:int((r.pt==pt).sum()) for pt in ['Goblin','Demon','Standard']})
