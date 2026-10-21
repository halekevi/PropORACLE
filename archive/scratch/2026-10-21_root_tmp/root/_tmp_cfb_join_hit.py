"""Join-hit rate: graded CFB vs step8 last5_over on 2026-09-12."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
import discover_gates_sport as D  # noqa: E402
import prop_hit_tiers as T  # noqa: E402

DATE = "2026-09-12"
path = D.graded_by_date()[DATE]
raw = json.loads(path.read_text(encoding="utf-8"))
step_path = D.find_step8("CFB", DATE)
idx = D.load_step8_index(step_path, "CFB")
print("graded file", path)
print("step8", step_path, "keys", len(idx))
n = hit = l5_on_s8 = l5_on_grade = 0
for r in raw.get("props") or []:
    if T.norm_sport(str(r.get("sport") or "")) != "CFB":
        continue
    if str(r.get("result") or "").upper() not in ("HIT", "MISS"):
        continue
    n += 1
    if r.get("l5_over") not in (None, ""):
        l5_on_grade += 1
    prop = T.canon_prop("CFB", str(r.get("prop") or ""))
    side = D._side(r.get("direction") or r.get("over_under"))
    if not prop or not side:
        continue
    pick = (
        "Goblin"
        if "goblin" in str(r.get("pick_type") or "").lower()
        else (
            "Demon"
            if "demon" in str(r.get("pick_type") or "").lower()
            else "Standard"
        )
    )
    key = (D._norm_player(r.get("player")), prop, pick, D._num(r.get("line")), side)
    s8 = idx.get(key)
    if s8:
        hit += 1
        if s8.get("l5_over") not in (None, ""):
            l5_on_s8 += 1
print(f"CFB decided {n}  step8 key hit {hit} ({100*hit/max(n,1):.1f}%)")
print(f"l5 on graded JSON {l5_on_grade}  l5 from matched step8 {l5_on_s8}")
print(f"missed join {n-hit}")
