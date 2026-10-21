import json
from pathlib import Path
from collections import Counter

p = Path(r"H:\PropORACLE_main_cp\ui_runner\templates\tickets_latest.json")
d = json.loads(p.read_text(encoding="utf-8"))
lines = []
lines.append(f"date={d.get('date')} mode={d.get('mode')}")
src = Counter()
for g in d.get("groups") or []:
    gname = g.get("group_name")
    for t in g.get("tickets") or []:
        pay = t.get("payout") if isinstance(t.get("payout"), dict) else {}
        ps = str(pay.get("payout_source") or pay.get("source") or t.get("payout_source") or "")
        src[ps or "blank"] += 1
        lines.append(
            f"{gname} | power={t.get('power_payout')} disp={t.get('display_min_x')} "
            f"pay.disp={pay.get('display_min_x')} pay.power_min={pay.get('power_min_x')} "
            f"src={ps!r} note={pay.get('payout_note')!r}"
        )

lines.append("--- sources ---")
lines.append(str(dict(src)))

# CDP ladder presence
for lp in [
    Path(r"H:\PropORACLE_main_cp\ui_runner\data\payout_ladder_live_cdp.json"),
    Path(r"H:\PropORACLE\ui_runner\data\payout_ladder_live_cdp.json"),
    Path(r"H:\PropORACLE_main_cp\data\payout_rate_cards.json"),
]:
    if not lp.exists():
        lines.append(f"missing {lp}")
        continue
    raw = json.loads(lp.read_text(encoding="utf-8"))
    lines.append(f"ladder {lp.name} type={type(raw).__name__}")
    if isinstance(raw, dict):
        lines.append(f"  keys={list(raw.keys())[:15]}")
        # count live_cdp-ish
        s = json.dumps(raw)[:500]
        lines.append(f"  head={s[:300]}")

Path(r"H:\PropORACLE\_tmp_payout_diag.txt").write_text("\n".join(lines), encoding="utf-8")
print("wrote", len(lines))
