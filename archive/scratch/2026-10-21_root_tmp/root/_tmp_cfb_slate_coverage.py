import pandas as pd
df=pd.read_excel(r'H:/PropORACLE_main_cp/outputs/2026-09-26/cfb/step8_cfb_direction_clean.xlsx')
df['day']=df['Game Time'].astype(str).str.slice(0,10)
# unique games
if 'Game Id' in df.columns:
    games=df.groupby('Game Id').agg(teams=('Team', lambda s: sorted(set(str(x).upper() for x in s))), n=('Player','count'), tip=('Game Time','first')).reset_index()
    print('unique Game Id', len(games))
else:
    games=None
    print('no Game Id')
# matchup keys
pairs=[]
for _,r in df.iterrows():
    t=str(r.get('Team') or '').upper(); o=str(r.get('Opp') or '').upper()
    if '/' in t: continue
    a,b=sorted([t,o])
    pairs.append((a,b,str(r.get('Game Time'))[:16]))
uniq=sorted(set((a,b) for a,b,_ in pairs if a and b and a!='NAN' and b!='NAN'))
print('unique team-opp pairs', len(uniq))
print('teams', df['Team'].nunique())
print('props', len(df))
# show kickoff buckets
tips=df['Game Time'].astype(str).str.slice(11,16)
print('kickoff HH:MM counts (top):', tips.value_counts().head(12).to_dict())
# confirm games from screenshot are present
want=['IND','NW','TENN','TEX','TTU','BAY','COLO','OSU','ILL','UK','CAL','CLEM','UTSA','BC','VT','TOL','SDSU','LOU','MD','UCLA','UNLV','AKR']
have=set(df['Team'].astype(str).str.upper())|set(df['Opp'].astype(str).str.upper())
print('screenshot teams present:', {t: t in have for t in want})
missing=[t for t in want if t not in have]
print('missing from step8:', missing)
l5o=pd.to_numeric(df['L5 Over'], errors='coerce').fillna(0)
l5u=pd.to_numeric(df['L5 Under'], errors='coerce').fillna(0)
d=df['Direction'].astype(str).str.upper()
dl5=l5o.where(d=='OVER', l5u)
print('L5=5 still', int((dl5>=5).sum()), 'max', float(dl5.max()))
