import pandas as pd,re,glob,unicodedata
O='../PropORACLE_main_cp/outputs/'
u=lambda s:''.join(c for c in unicodedata.normalize('NFKD',str(s)) if not unicodedata.combining(c))
nm=lambda s:re.sub(r'\b(jr|sr|ii|iii|iv)\b','',re.sub(r'[^a-z ]','',u(s).lower().replace('.','').replace("'",''))).strip()
pn=lambda s:re.sub(r'[^a-z0-9]','',str(s).lower())
def load(kind):
    fr=[]
    for f in sorted(glob.glob(O+f'2026-11-0[5-9]/{kind[0]}')):
        try:
            d=pd.read_excel(f,sheet_name='ALL') if f.endswith('xlsx') else pd.read_csv(f,low_memory=False)
        except Exception: continue
        d=d.rename(columns={k:v for k,v in kind[1].items() if k in d.columns}); fr.append(d)
    return pd.concat(fr,ignore_index=True) if fr else pd.DataFrame()
SRC={'nfl':('nfl/step8_nfl_direction_clean.xlsx',{'Player':'player','Prop':'prop','Line':'line','Direction':'dirn','Projection':'projection','Edge':'edge','Confidence Score':'conf','Pick Type':'pt'}),
'cfb':('cfb/step8_cfb_direction_clean.xlsx',{'Player':'player','Prop':'prop','Line':'line','Direction':'dirn','Projection':'projection','Edge':'edge','Confidence Score':'conf','Pick Type':'pt'}),
'cbb':('cbb/step8_cbb_direction_clean.xlsx',{'prop_type':'prop','bet_direction':'dirn','ml_prob':'ml','confidence_score':'conf','pick_type':'pt'}),
'tennis':('tennis/step8_tennis_direction_clean.xlsx',{'Player':'player','Prop':'prop','Line':'line','Direction':'dirn','Projection':'projection','Edge':'edge','ML Prob':'ml','Confidence Score':'conf','Pick Type':'pt'}),
'nhl':('nhl/step8_nhl_direction_clean.csv',{'prop_type':'prop','direction':'dirn','ml_prob':'ml','confidence_score':'conf','pick_type':'pt'}),
'soccer':('soccer/step8_soccer_direction.csv',{'prop_type':'prop','bet_direction':'dirn','ml_prob':'ml','pick_type':'pt'}),
'golf':('golf/step8_golf_direction.csv',{'prop_type':'prop','bet_direction':'dirn','ml_prob':'ml','confidence_score':'conf','pick_type':'pt'})}
M={k:load(v) for k,v in SRC.items()}
for k,d in M.items():
    print(k,len(d), [c for c in ('player','prop','line','dirn','projection','edge','ml','conf') if c in d.columns])
import pickle; pickle.dump(M,open('/tmp/M.pkl','wb'))
