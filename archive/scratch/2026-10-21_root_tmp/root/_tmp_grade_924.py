import json
import re
from collections import defaultdict
from pathlib import Path


def fold(s):
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def norm_prop(s):
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


gp = json.loads(
    Path(r"H:\PropORACLE_main_cp\ui_runner\templates\graded_props_2026-09-24.json").read_text(
        encoding="utf-8"
    )
)
props = gp["props"]

idx: dict = {}
for r in props:
    player = fold(r.get("player"))
    prop = norm_prop(r.get("prop_type") or r.get("prop") or r.get("stat_type"))
    pick = str(r.get("pick_type") or "").strip().lower()[:3]
    line = str(r.get("line"))
    side = str(r.get("direction") or r.get("over_under") or r.get("side") or "").upper()[:5]
    idx.setdefault((player, prop, pick, line, side), []).append(r)
    idx.setdefault(("nol", player, prop, pick, side), []).append(r)

g70 = json.loads(
    Path(r"H:\PropORACLE_main_cp\data\reports\goblin70_tickets_2026-09-24.json").read_text(
        encoding="utf-8"
    )
)
pool = g70.get("goblin_pool") or []
tickets = g70.get("tickets") or []


def match_leg(leg):
    player = fold(leg.get("player"))
    prop = norm_prop(leg.get("prop") or leg.get("prop_type"))
    pick = str(leg.get("pick_type") or "").strip().lower()[:3]
    line = str(leg.get("line"))
    side = str(leg.get("side") or leg.get("direction") or "OVER").upper()[:5]
    cands = idx.get((player, prop, pick, line, side), [])
    if not cands:
        cands = idx.get(("nol", player, prop, pick, side), [])
    if not cands:
        for k, v in idx.items():
            if (
                isinstance(k, tuple)
                and len(k) == 5
                and k[0] == player
                and k[1] == prop
                and k[4] == side
            ):
                cands = v
                break
    if not cands:
        return None
    for c in cands:
        if str(c.get("line")) == line:
            return c
    return cands[0]


def rate(rows, label):
    decided = []
    voids = []
    miss = []
    details = []
    for r in rows:
        m = match_leg(r)
        if not m:
            miss.append(r)
            continue
        res = str(m.get("result") or "").upper()
        hit = m.get("hit")
        if res in ("VOID", "PUSH") or hit is None:
            voids.append((r, m))
            continue
        ok = hit == 1 or res == "HIT"
        decided.append(ok)
        details.append((ok, r, m))
    n = len(decided)
    w = sum(1 for x in decided if x)
    print(f"\n=== {label} ===")
    if n:
        print(
            f"pool={len(rows)} graded={n} voids/push={len(voids)} "
            f"unmatched={len(miss)} hit={w}/{n} = {100 * w / n:.1f}%"
        )
    else:
        print(f"pool={len(rows)} graded=0 voids={len(voids)} unmatched={len(miss)}")
    by_sport = defaultdict(lambda: [0, 0])
    for ok, r, m in details:
        sp = str(r.get("sport") or m.get("sport") or "?")
        by_sport[sp][0] += 1
        by_sport[sp][1] += int(ok)
    for sp, (n2, w2) in sorted(by_sport.items(), key=lambda x: -x[1][0]):
        print(f"  {sp}: {w2}/{n2} = {100 * w2 / n2:.1f}%")
    pts = [
        (ok, r, m)
        for ok, r, m in details
        if "point" in str(r.get("prop") or "").lower()
        and str(r.get("side") or "OVER").upper().startswith("O")
    ]
    if pts:
        pw = sum(ok for ok, _, _ in pts)
        print(f"  Points OVER: {pw}/{len(pts)} = {100 * pw / len(pts):.1f}%")
        for ok, r, m in pts:
            tag = "HIT" if ok else "MISS"
            print(
                f"    {tag} {r.get('player')} {r.get('prop')} "
                f"{r.get('side')}{r.get('line')} actual={m.get('actual_value')}"
            )
    if miss[:8]:
        print(
            "  unmatched:",
            [(x.get("player"), x.get("prop"), x.get("line"), x.get("sport")) for x in miss[:8]],
        )
    return details


rate(pool, "G70 goblin pool")
rate([r for r in pool if str(r.get("sport")).upper() == "WNBA"], "G70 WNBA")
rate([r for r in pool if str(r.get("sport")).upper() == "MLB"], "G70 MLB")
rate([r for r in pool if str(r.get("sport")).upper() == "TENNIS"], "G70 Tennis")

print("\n=== G70 packed tickets ===")
for t in tickets:
    legs = t.get("legs") or []
    outs = []
    for leg in legs:
        m = match_leg(leg)
        if not m:
            outs.append(("?", leg, None))
            continue
        res = str(m.get("result") or "").upper()
        hit = m.get("hit")
        if res in ("VOID", "PUSH") or hit is None:
            outs.append(("V", leg, m))
        elif hit == 1 or res == "HIT":
            outs.append(("W", leg, m))
        else:
            outs.append(("L", leg, m))
    decided = [o for o in outs if o[0] in ("W", "L")]
    wins = sum(1 for o in decided if o[0] == "W")
    sweep = all(o[0] == "W" for o in decided) and decided and not any(o[0] == "?" for o in outs)
    print(
        f"  {t.get('id')} {t.get('product')} {t.get('n_legs')} "
        f"legs {wins}/{len(decided)} decided  sweep={'Y' if sweep else 'N'}  "
        f"{t.get('mix')}"
    )
    for tag, leg, m in outs:
        act = m.get("actual_value") if m else "-"
        print(
            f"    [{tag}] {leg.get('sport')} {leg.get('player')} "
            f"{leg.get('prop')} {leg.get('side')}{leg.get('line')} actual={act}"
        )

# Manual submitted slips from session conversation
manual = [
    ("T1", [
        {"player": "Reynaldo Lopez", "prop": "Hits Allowed", "pick_type": "Goblin", "line": 3.5, "side": "OVER", "sport": "MLB"},
        {"player": "Jackie Young", "prop": "Assists", "pick_type": "Goblin", "line": 3.5, "side": "OVER", "sport": "WNBA"},
        {"player": "Kyle Leahy", "prop": "Hits Allowed", "pick_type": "Goblin", "line": 1.5, "side": "OVER", "sport": "MLB"},
    ]),
]
# load session exposure if present
for p in [
    Path(r"H:\PropORACLE\data\cache\manual_slip_exposure_2026-09-24.json"),
    Path(r"H:\PropORACLE_main_cp\data\cache\manual_slip_exposure_2026-09-24.json"),
]:
    if p.exists():
        print(f"\n=== session file {p} ===")
        sess = json.loads(p.read_text(encoding="utf-8"))
        print(json.dumps(sess, indent=2)[:2500])

print("\n=== Graded board Goblin OVER by sport ===")
gob = [
    r
    for r in props
    if "gob" in str(r.get("pick_type") or "").lower()
    and str(r.get("direction") or r.get("over_under") or "").upper().startswith("O")
]
dec = [r for r in gob if str(r.get("result") or "").upper() in ("HIT", "MISS")]
w = sum(1 for r in dec if str(r.get("result")).upper() == "HIT" or r.get("hit") == 1)
print(f"all Goblin OVER: {w}/{len(dec)} = {100 * w / len(dec) if dec else 0:.1f}%")
by = defaultdict(lambda: [0, 0])
for r in dec:
    sp = str(r.get("sport") or r.get("league") or "?").upper()
    by[sp][0] += 1
    by[sp][1] += int(str(r.get("result")).upper() == "HIT" or r.get("hit") == 1)
for sp, (n, ww) in sorted(by.items(), key=lambda x: -x[1][0]):
    print(f"  {sp}: {ww}/{n} = {100 * ww / n:.1f}%")

# WNBA Points OVER goblin
wnba_pts = [
    r
    for r in dec
    if "WNBA" in str(r.get("sport") or r.get("league") or "").upper()
    and "point" in str(r.get("prop_type") or r.get("prop") or "").lower()
]
if wnba_pts:
    ww = sum(1 for r in wnba_pts if str(r.get("result")).upper() == "HIT" or r.get("hit") == 1)
    print(f"\nWNBA Goblin Points OVER (all graded): {ww}/{len(wnba_pts)} = {100 * ww / len(wnba_pts):.1f}%")

# ticket tracks from grade_history
gh = json.loads(Path(r"H:\PropORACLE_main_cp\data\grade_history.json").read_text(encoding="utf-8"))
rows = [r for r in gh if str(r.get("date", ""))[:10] == "2026-09-24"]
print("\n=== grade_history ticket tracks 2026-09-24 ===")
for r in sorted(rows, key=lambda x: -x.get("n_tickets", 0)):
    print(
        f"  {r['track']}: {r['wins']}/{r['n_tickets']} = {100 * r['win_rate']:.1f}% "
        f"(W{r['wins']} L{r['losses']} voidL{r.get('void_loss_ct', 0)}) "
        f"ROI {r.get('roi_pct')}% net/10 {r.get('net_per_10')}"
    )
    st = r.get("strong_tickets") or {}
    if st.get("count"):
        print(f"    STRONG {st.get('wins')}/{st.get('count')}")
