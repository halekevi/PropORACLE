import _r2
import sqlite3,pandas as pd,numpy as np,warnings; warnings.filterwarnings('ignore')
pd.set_option('display.width',250)
x=pd.read_pickle('/tmp/full_nba.pkl')
c=sqlite3.connect('data/cache/proporacle_ref.db')
n=pd.read_sql('select game_date,player,minutes from nba',c); n['m']=pd.to_numeric(n.minutes,errors='coerce'); n=n[n.m>0]; n['gd']=pd.to_datetime(n.game_date)
fs={}
for p,g in n.sort_values('gd').groupby(n.player.str.lower()):
    m=g.m.values; fs[p]=dict(n=len(g),mn10=m[-10:].mean(),mnsd=m[-10:].std(),mnfl5=m[-5:].min(),trend=m[-5:].mean()-m[-20:].mean(),last=g.gd.iloc[-1])
F=pd.DataFrame(fs).T.reset_index().rename(columns={'index':'pl'})
x['pl']=x.Player.str.lower(); x=x.merge(F,on='pl',how='left')
today=pd.Timestamp('2026-11-10')
x['dsl']=(today-pd.to_datetime(x['last'])).dt.days
x['clean']=(x.n>=20)&(x.mn10>=20)&(x.mnfl5>=12)&(x.mnsd<=6)&(x.trend>-4)&(x.dsl<=7)&(x.Line>=1.5)
# ledger hit rates
L=pd.read_excel('outputs/player_profiles_2026-11-09.xlsx',sheet_name='Ledger_hit_rates')
L['pl']=L.player.str.lower(); L['pk']=L.pt.str.title(); L['sd']=np.where(L.dir=='O','Over','Under'); L['pr']=L.prop.str.lower()
x['pr']=x.Prop.str.lower()
x=x.merge(L[['pl','pr','pk','sd','mean','count']].rename(columns={'mean':'led_hr','count':'led_n'}),left_on=['pl','pr','Type','Side'],right_on=['pl','pr','pk','sd'],how='left')
print('ledger match',x.led_hr.notna().mean().round(3))
c2=['Player','Team','Opp','Prop','Side','Line','L5','L10','L20','avg10','zSD','led_hr','led_n','mn10','mnsd','clean']
def show(t,m,n=14):
    y=x[m].sort_values(['clean','zSD'],ascending=False); print(f'\n{t}: {len(y)} pass, {int(y.clean.sum())} also clean'); print(y[c2].head(n).round(2).to_string(index=False))
G=(x.Type=='Goblin')&(x['L5%']==100)&(x['L10%']>=90)&(x['L20%']>=80)&(x.zSD>=1)
show('GOBLIN',G,16)
show('STD OVER',(x.Type=='Standard')&(x.Side=='Over')&(x['L5%']>=80)&(x['L10%']>=80)&(x['L20%']>=65)&(x.zSD>=.25)&(x.cv20<.4))
show('STD UNDER',(x.Type=='Standard')&(x.Side=='Under')&(x['L5%']>=80)&(x['L10%']>=80)&(x['L20%']>=65)&(x.zSD>=.5))
show('DEMON',(x.Type=='Demon')&(x['L5%']>=80)&(x['L10%']>=70)&(x.zSD>=.25),8)
x.to_pickle('/tmp/rank_today.pkl')
