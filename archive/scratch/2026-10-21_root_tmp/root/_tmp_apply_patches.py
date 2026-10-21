"""Apply remaining pack_yolo + MAIN demote patches."""
from __future__ import annotations

from pathlib import Path

REPO = Path(r"H:\PropORACLE")

# --- FILE 2: pack_yolo ---
p = REPO / "scripts" / "build_goblin70_tickets.py"
text = p.read_text(encoding="utf-8")
old = '''def pack_yolo(
    gob: list[dict],
    std: list[dict] | None = None,
    *,
    max_tickets: int = YOLO_MAX_TICKETS,
) -> list[list[dict]]:
    """Power 6, then 5, then 4. One Standard at the front when available.

    Own taken set so Power 3 / Flex 4 keep their legs. Pool order is
    Diamond/Platinum first (goblin_sort_key).
    """
    taken: set[str] = set()
    out: list[list[dict]] = []
    std_pool = list(std or [])
    for n in YOLO_PREF_LEGS:
        if len(out) >= max(0, int(max_tickets)):
            break
        seeds: list[dict] = []
        for s in std_pool:
            pn = fold_name(s.get("player"))
            if pn and pn not in taken:
                seeds.append(s)
                break
        seeds.extend(r for r in gob if fold_name(r.get("player")) not in taken)
        combo = pack(seeds, n, taken, mix_sports=True, wnba_cap=1)
        if len(combo) != n:
            combo = pack(seeds, n, taken, mix_sports=True, wnba_cap=2)
        if len(combo) != n:
            continue
        for r in combo:
            taken.add(fold_name(r.get("player")))
        out.append(combo)
    return out
'''
new = '''def pack_yolo(
    gob: list[dict],
    std: list[dict] | None = None,
    *,
    max_tickets: int = YOLO_MAX_TICKETS,
) -> list[list[dict]]:
    """Power 6, then 5, then 4. One Standard at the front when available.

    Own taken set so Power 3 / Flex 4 keep their legs.

    Premium sleeve seeds first: WNBA FGA / reb+ast / threes / threes_att that
    already clear goblin_70_eligible (premium_goblin_sort_key ranks them above
    Diamond/Platinum). Remaining Goblin-70 pool follows goblin_sort_key order.
    """
    taken: set[str] = set()
    out: list[list[dict]] = []
    std_pool = list(std or [])
    # Premium YOLO seed order (comment marker for sleeve entry point).
    premium = sorted(
        (r for r in gob if premium_goblin_eligible(r)),
        key=premium_goblin_sort_key,
    )
    rest = [r for r in gob if not premium_goblin_eligible(r)]
    ordered_gob = premium + rest
    for n in YOLO_PREF_LEGS:
        if len(out) >= max(0, int(max_tickets)):
            break
        seeds: list[dict] = []
        for s in std_pool:
            pn = fold_name(s.get("player"))
            if pn and pn not in taken:
                seeds.append(s)
                break
        # Premium pool seeds in here (before Diamond/Platinum rest of gob).
        seeds.extend(r for r in ordered_gob if fold_name(r.get("player")) not in taken)
        combo = pack(seeds, n, taken, mix_sports=True, wnba_cap=1)
        if len(combo) != n:
            combo = pack(seeds, n, taken, mix_sports=True, wnba_cap=2)
        if len(combo) != n:
            continue
        for r in combo:
            taken.add(fold_name(r.get("player")))
        out.append(combo)
    return out
'''
if old not in text:
    if "premium_goblin_eligible(r)" in text and "Premium sleeve seeds first" in text:
        print("pack_yolo already patched")
    else:
        raise SystemExit("pack_yolo block not found / unexpected")
else:
    text = text.replace(old, new, 1)
    print("pack_yolo patched")

old2 = '''    # YOLO sleeve: Power 4/5/6. Mix one Standard when available. Own taken
    # set so Power 3 / Flex 4 still get those legs. Off ticket win-rate.
    for i, combo in enumerate(pack_yolo(gob, std), start=1):
'''
new2 = '''    # YOLO sleeve: Power 4/5/6. Mix one Standard when available. Own taken
    # set so Power 3 / Flex 4 still get those legs. Off ticket win-rate.
    # Premium WNBA (FGA / reb+ast / threes / threes_att) seeds before
    # Diamond/Platinum inside pack_yolo.
    for i, combo in enumerate(pack_yolo(gob, std), start=1):
'''
if old2 in text:
    text = text.replace(old2, new2, 1)
    print("yolo comment patched")
elif "Premium WNBA (FGA" in text:
    print("yolo comment already patched")
else:
    raise SystemExit("yolo comment not found")

p.write_text(text, encoding="utf-8")
print("wrote", p)
