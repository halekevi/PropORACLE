import json
import re
from pathlib import Path


def fold(s):
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def norm_prop(s):
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


gp = json.loads(
    Path(r"H:\PropORACLE_main_cp\ui_runner\templates\graded_props_2026-09-24.json").read_text(
        encoding="utf-8"
    )
)["props"]
idx = {}
for r in gp:
    key = (
        fold(r.get("player")),
        norm_prop(r.get("prop_type") or r.get("prop")),
        str(r.get("pick_type") or "").lower()[:3],
        str(r.get("line")),
        str(r.get("direction") or "").upper()[:5],
    )
    idx.setdefault(key, []).append(r)


def match(player, prop, pick, line, side="OVER"):
    k = (
        fold(player),
        norm_prop(prop),
        str(pick or "").lower()[:3],
        str(line),
        str(side or "OVER").upper()[:5],
    )
    c = idx.get(k, [])
    if not c:
        for kk, v in idx.items():
            if kk[0] == k[0] and kk[1] == k[1] and kk[4] == k[4]:
                # prefer matching line
                for row in v:
                    if str(row.get("line")) == str(line):
                        return row
                return v[0]
    if not c:
        return None
    for row in c:
        if str(row.get("line")) == str(line):
            return row
    return c[0]


esp = [r for r in gp if "espino" in fold(r.get("player"))]
print("Espino graded rows:")
for r in esp[:15]:
    print(
        " ",
        r.get("player"),
        r.get("prop_type"),
        r.get("pick_type"),
        r.get("line"),
        r.get("result"),
        "actual=",
        r.get("actual_value"),
    )

slips = [
    (
        "T1",
        [
            ("Peter Lambert", "Hits Allowed", "goblin", 3.5, "OVER"),
            ("Jackie Young", "Assists", "goblin", 3.5, "OVER"),
            ("Kyle Leahy", "Hits Allowed", "goblin", 1.5, "OVER"),
        ],
    ),
    (
        "T2",
        [
            ("A'ja Wilson", "Rebs+Asts", "goblin", 9.5, "OVER"),
            ("Tyler Mahle", "Pitching Outs", "goblin", 14.5, "OVER"),
            ("Elise Mertens", "Total Games", "goblin", 19.5, "OVER"),
        ],
    ),
    (
        "T3",
        [
            ("Paul Skenes", "Pitcher Strikeouts", "goblin", 3.5, "OVER"),
            ("Zac Thornton", "Pitcher Strikeouts", "goblin", 2.5, "OVER"),
            # user swapped Rocker->Thornton; third was Espino HA in construction talk
            ("Jhostin Espino", "Hits Allowed", "goblin", 2.5, "OVER"),
        ],
    ),
]

print("\n=== Submitted / session slips ===")
for tid, legs in slips:
    print(f"\n{tid}")
    tags = []
    for pl, pr, pk, ln, sd in legs:
        m = match(pl, pr, pk, ln, sd)
        if not m:
            # try alternate Espino spelling
            print(f"  ? {pl} {pr} {ln}")
            tags.append("?")
            continue
        res = str(m.get("result") or "").upper()
        hit = m.get("hit")
        if res in ("VOID", "PUSH") or hit is None:
            tag = "V"
        elif hit == 1 or res == "HIT":
            tag = "W"
        else:
            tag = "L"
        tags.append(tag)
        print(f"  [{tag}] {pl} {pr} {sd}{ln} actual={m.get('actual_value')}")
    decided = [t for t in tags if t in ("W", "L")]
    sweep = bool(decided) and all(t == "W" for t in decided) and "?" not in tags
    print(
        f"  legs {sum(t == 'W' for t in decided)}/{len(decided)} "
        f"sweep={'YES' if sweep else 'NO'}"
    )

g70 = json.loads(
    Path(r"H:\PropORACLE_main_cp\data\reports\goblin70_tickets_2026-09-24.json").read_text(
        encoding="utf-8"
    )
)


def match_leg(leg):
    return match(
        leg.get("player"),
        leg.get("prop") or leg.get("prop_type"),
        leg.get("pick_type") or "",
        leg.get("line"),
        leg.get("side") or "OVER",
    )


core = []
yolo = []
for t in g70.get("tickets") or []:
    tid = str(t.get("id") or "")
    if tid.startswith("Y") or tid.startswith("SF"):
        yolo.append(t)
    else:
        core.append(t)

print("\n=== G70 core tickets (Power/Flex, excl YOLO + NFL SF leftovers) ===")
wins = losses = inc = 0
leg_w = leg_n = 0
for t in core:
    outs = []
    for leg in t.get("legs") or []:
        m = match_leg(leg)
        if not m:
            outs.append("?")
            continue
        res = str(m.get("result") or "").upper()
        hit = m.get("hit")
        if res in ("VOID", "PUSH") or hit is None:
            outs.append("V")
        elif hit == 1 or res == "HIT":
            outs.append("W")
        else:
            outs.append("L")
    decided = [o for o in outs if o in ("W", "L")]
    leg_w += sum(o == "W" for o in decided)
    leg_n += len(decided)
    if "?" in outs or not decided:
        inc += 1
        oc = "incomplete"
    elif all(o == "W" for o in decided):
        wins += 1
        oc = "WIN"
    else:
        losses += 1
        oc = "LOSS"
    print(f"  {t.get('id')}: {oc} {''.join(outs)}")

n = wins + losses
print(
    f"core sweeps {wins}/{n} = {100 * wins / n:.1f}%"
    if n
    else "core sweeps n/a"
)
print(f"incomplete {inc}")
print(
    f"core leg hit {leg_w}/{leg_n} = {100 * leg_w / leg_n:.1f}%"
    if leg_n
    else "no legs"
)

# Power-only vs Flex
for kind, pred in (
    ("Power", lambda t: "power" in str(t.get("product") or "").lower()),
    ("Flex", lambda t: "flex" in str(t.get("product") or "").lower()),
):
    tw = tl = 0
    for t in core:
        if not pred(t):
            continue
        outs = []
        for leg in t.get("legs") or []:
            m = match_leg(leg)
            if not m:
                outs.append("?")
                continue
            res = str(m.get("result") or "").upper()
            hit = m.get("hit")
            if res in ("VOID", "PUSH") or hit is None:
                outs.append("V")
            elif hit == 1 or res == "HIT":
                outs.append("W")
            else:
                outs.append("L")
        decided = [o for o in outs if o in ("W", "L")]
        if "?" in outs or not decided:
            continue
        if all(o == "W" for o in decided):
            tw += 1
        else:
            tl += 1
    nn = tw + tl
    if nn:
        print(f"  {kind} sweeps {tw}/{nn} = {100 * tw / nn:.1f}%")

gh = json.loads(
    Path(r"H:\PropORACLE_main_cp\data\grade_history.json").read_text(encoding="utf-8")
)
print("\n=== Mixer grade_history tracks ===")
for r in sorted(
    [r for r in gh if str(r.get("date"))[:10] == "2026-09-24"],
    key=lambda x: -x["n_tickets"],
):
    print(
        f"  {r['track']}: {r['wins']}/{r['n_tickets']} = {100 * r['win_rate']:.1f}% "
        f"ROI {r.get('roi_pct')}%"
    )
    st = r.get("strong_tickets") or {}
    if st.get("count"):
        print(
            f"    STRONG {st['wins']}/{st['count']} = {100 * st['wins'] / st['count']:.1f}%"
        )
