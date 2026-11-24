import pandas as pd,numpy as np,math,warnings; warnings.filterwarnings('ignore')
t=pd.read_pickle('/tmp/ds3.pkl')
def ci(y):
    n=len(y)
    if n==0: return 'n=0'
    p=y.mean(); h=1.96*math.sqrt(p*(1-p)/n); return f'n={n:4d} {p*100:5.1f}% (±{h*100:.1f})'
D=t[t.pt=='Demon']; SO=t[(t.pt=='Standard')&(t.side==1)]
rules={
 'D base':(D,lambda x:x.y.notna()),
 'D votes>=7':(D,lambda x:x.votes>=7),
 'D mg>=.0667':(D,lambda x:x.mg>=.0667),
 'D mg&hs':(D,lambda x:(x.mg>=.0667)&(x.hs>=.5385)),
 'D tight rung_pct<=.105 & z_line<.12 & nge>=9':(D,lambda x:(x.rung_pct<=.105)&(x.z_line<.12)&(x.n_ge_line20>=9)),
 'D tight & votes>=7':(D,lambda x:(x.rung_pct<=.105)&(x.votes>=7)),
 'D z_line<.12 & nge>=9 & home':(D,lambda x:(x.z_line<.12)&(x.n_ge_line20>=9)&(x.home==1)),
 'D mg&hs&total>=220':(D,lambda x:(x.mg>=.0667)&(x.hs>=.5385)&(x.total>=220.5)),
 'SO base':(SO,lambda x:x.y.notna()),
 'SO nge>=14':(SO,lambda x:x.n_ge_line20>=14),
 'SO votes==10':(SO,lambda x:x.votes==10),
}
for k,(d,f) in rules.items():
    print(k)
    for s in ('train','val','hold'):
        x=d[d.split==s]; print('   ',s.ljust(5),ci(x.y[f(x)]))
