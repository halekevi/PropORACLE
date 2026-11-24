import pandas as pd,os,itertools
import sys, tempfile; from pathlib import Path
R=str(Path(__file__).resolve().parents[2]); D=sys.argv[1] if len(sys.argv)>1 else os.environ.get('PROPORACLE_DATE','2026-11-01')
A=pd.read_pickle(os.path.join(tempfile.gettempdir(),'nfl_A.pkl'))
A['game']=A.apply(lambda r:'@'.join(sorted([r.team,r.opp])),axis=1)
A['tier']=(A.k5<5).astype(int)
P=A.sort_values(['tier','h10','k5','score'],ascending=[True,False,False,False]).reset_index(drop=True)
def build(k):
    used=set(); usedp=set(); out=[]
    while len(out)<6:
        t=[]; pl=set(); gm={}
        for want in range(k):
            side=None if not t else ('OVER' if sum(x.side=='UNDER' for x in t)>sum(x.side=='OVER' for x in t) else 'UNDER')
            pick=None
            for pass_ in (0,1):
                for i,r in P.iterrows():
                    if i in used or r.player in usedp or r.player in pl or gm.get(r.game,0)>=2: continue
                    if pass_==0 and side and r.side!=side: continue
                    pick=(i,r); break
                if pick: break
            if not pick: break
            t.append(pick[1]); pl.add(pick[1].player); gm[pick[1].game]=gm.get(pick[1].game,0)+1
        if len(t)<k: break
        for r in t: used.add(r.name); usedp.add(r.player)
        out.append(t)
    return out
def row(r): return f"| {r.player} ({r.team}) | {r.side} | {r.prop} | {r.line:g} | {r.L10} | {r.L5} | {r.score:.2f} | {str(r.game_time)[11:16]} ET | {r.inj or ''}{' low-usage' if (r.side=='UNDER' and sum(float(x) for x in r.last5.split(','))/5<0.25*r.line) else ''}{' unconfirmed edge' if r.side=='OVER' else ''} |"
L=[f"# NFL Standard tickets for {D} (Claude build, PREVIEW)\n",
 f"Built from the NFL game log (2025+2026 games played, newest first). L5/L10 = last 5 / 10 games played. Score = consistency score, NOT a probability. Flags: UNVERIFIED L5 (PrizePicks card check pending). Inactives not known yet: rebuild after ~11:30 AM ET inactives. NFL Goblins are off tickets by rule.\n",
 f"Rules: one player per ticket and per ticket size, max 2 legs per game, Over/Under mixed where the pool allows. Low-usage = last-5 average under 25% of the line (a depth player; if inactive the leg is voided).\n\nPool: {len(A)} picks ({(A.k5==5).sum()} at 5/5, rest 4/5), {A.side.value_counts().to_dict()}.\n"]
for k in (2,3,4,5,6):
    T=build(k); L.append(f"\n## {k}-leg tickets ({len(T)})\n")
    for n,t in enumerate(T,1):
        L.append(f"\n**{k}-leg #{n}** · Overs: {sum(x.side=='OVER' for x in t)}, Unders: {sum(x.side=='UNDER' for x in t)}\n")
        L.append("| Player | Side | Prop | Line | L10 | L5 | Score | Kick | Tags |\n| --- | --- | --- | ---: | --- | --- | ---: | --- | --- |")
        L+= [row(r) for r in t]
f=f'{R}/outputs/{D}/claude_NFL_STANDARD_TICKETS_{D}.md'; open(f,'w').write('\n'.join(L)); print(f)
print('\n'.join(L[:60]))
