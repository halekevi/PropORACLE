import pandas as pd, unicodedata, sys, os
D=sys.argv[1]; B0='../PropORACLE_main_cp/'
def n(s): return ''.join(c for c in unicodedata.normalize('NFD',str(s)) if not unicodedata.combining(c)).lower().replace('.','').replace(' jr','').replace(' iii','').replace(' ii','').strip()
L=pd.read_csv(B0+'data/cache/nba_period_logs.csv'); L['k']=L.player.map(n)
for p in ('q1','h1'): L[p+'_pra']=L[p+'_pts']+L[p+'_reb']+L[p+'_ast']
col={'Points':'pts','Pts+Rebs+Asts':'pra','Rebounds':'reb','Assists':'ast'}
res={}
for tag,pre in (('nba1h','h1'),('nba1q','q1')):
    f=B0+f'outputs/{D}/{tag}/step1_{tag}_props.csv'
    if not os.path.exists(f): print(tag,'no board'); continue
    B=pd.read_csv(f)
    if not len(B): print(tag,'board empty (not posted yet)'); continue
    B['k']=B.player.map(n); rows=[]
    for _,r in B.iterrows():
        c=col.get(r.prop_type); d=dict(player=r.player,team=r.team,prop=r.prop_type,pick=r.pick_type,line=r.line)
        if not c: d['note']='no period history for this prop (Fantasy Score etc.)'; rows.append(d); continue
        v=L[L.k==r.k].sort_values('game_date')[f'{pre}_{c}'].tolist()[::-1]
        if not v: d['note']='no games found'; rows.append(d); continue
        d.update(games=len(v),avg_L5=round(sum(v[:5])/len(v[:5]),2),avg_L10=round(sum(v[:10])/len(v[:10]),2),last5=' '.join(str(int(i)) for i in v[:5]))
        for s in ('Over','Under'):
            f_=(lambda i:i>r.line) if s=='Over' else (lambda i:i<r.line)
            a=sum(map(f_,v[:5])); b=sum(map(f_,v[:10])); e=sum(map(f_,v[:20]))
            d[s+'_L5']=f'{a}/{len(v[:5])}'; d[s+'_L10']=f'{b}/{len(v[:10])}'; d[s+'_L20']=f'{e}/{len(v[:20])}'
            need=5 if r.pick_type=='Goblin' else 4
            ok=len(v)>=10 and a>=need and b>=8
            allowed=(s=='Over') or r.pick_type=='Standard'
            d[s+'_gate']=('' if not allowed else ('PASS' if ok and len(v)>=15 and e>=0.75*len(v[:20]) else ('L5/L10 PASS, L20 short' if ok and len(v)<15 else 'F (unverified)')))
        rows.append(d)
    res[tag]=pd.DataFrame(rows)
with pd.ExcelWriter(f'outputs/nba_period_props_{D}.xlsx') as w:
    for tag,df in res.items():
        for c in df.select_dtypes('float'): df[c]=df[c].round(2)
        for pk in ['Standard','Goblin','Demon']:
            x=df[df.pick.astype(str).str.capitalize()==pk]
            if len(x): x.drop(columns='pick').to_excel(w,sheet_name=f'{tag} {pk}',index=False)
        p=df[df.get('Over_gate',pd.Series(dtype=str)).astype(str).str.contains('PASS|short')|df.get('Under_gate',pd.Series(dtype=str)).astype(str).str.contains('PASS|short')]
        if len(p): p.to_excel(w,sheet_name=f'{tag} GATES',index=False)
        print(tag,len(df),len(p))
