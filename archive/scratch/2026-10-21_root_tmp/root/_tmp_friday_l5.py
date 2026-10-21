from __future__ import annotations

from pathlib import Path

import pandas as pd

root = Path(r"H:\PropORACLE_main_cp\outputs\2026-09-24")


def l5_val(r) -> float | None:
    for k in ("l5_over", "last5_over", "line_hits_over_5"):
        if k in r and pd.notna(r.get(k)):
            try:
                return float(r.get(k))
            except (TypeError, ValueError):
                continue
    return None


def l10_val(r) -> float | None:
    for k in ("l10_over", "last10_over", "line_hits_over_10"):
        if k in r and pd.notna(r.get(k)):
            try:
                return float(r.get(k))
            except (TypeError, ValueError):
                continue
    return None


ten = pd.read_csv(root / "tennis" / "step8_tennis_direction.csv", low_memory=False)
ten["_st"] = ten["start_time"].astype(str)
fri_t = ten[ten["_st"].str.contains("2026-09-25")].copy()
fri_t["_prop"] = fri_t["prop_type"].astype(str).str.lower()
fri_t["_pick"] = fri_t["pick_type"].astype(str).str.lower()
keep_t = fri_t[
    (fri_t["_pick"] == "goblin")
    & fri_t["_prop"].isin(["total games", "games won", "total games won"])
].copy()
keep_t["l5"] = keep_t.apply(l5_val, axis=1)
keep_t["l10"] = keep_t.apply(l10_val, axis=1)

print(f"TENNIS Friday Goblin TG/GW faces in step8: {len(keep_t)}")
print(f"  L5==5: {(keep_t['l5']==5).sum()}")
print(f"  L5==4: {(keep_t['l5']==4).sum()}")
print(f"  L5<=3 or missing: {((keep_t['l5'].isna()) | (keep_t['l5']<=3)).sum()}")
print("  L5=5 list:")
eq5 = keep_t[keep_t["l5"] == 5].sort_values(["_st", "player", "line"])
for _, r in eq5.iterrows():
    print(
        f"    {str(r['_st'])[11:16]}  {r['player']:28} {r['prop_type']:18} "
        f"G{r['line']}  L5={int(r['l5'])} L10={r['l10']}"
    )
print("  NOT L5=5:")
n5 = keep_t[keep_t["l5"] != 5].sort_values(["l5", "_st", "player"], ascending=[False, True, True])
for _, r in n5.iterrows():
    print(
        f"    {str(r['_st'])[11:16]}  {r['player']:28} {r['prop_type']:18} "
        f"G{r['line']}  L5={r['l5']} L10={r['l10']}"
    )

soc = pd.read_csv(root / "soccer" / "step8_soccer_direction.csv", low_memory=False)
soc["_st"] = soc["start_time"].astype(str)
gd = soc["game_date"].astype(str) if "game_date" in soc.columns else pd.Series([""] * len(soc))
fri_s = soc[soc["_st"].str.contains("2026-09-25") | gd.eq("2026-09-25")].copy()
fri_s["_prop"] = fri_s["prop_type"].astype(str).str.lower()
fri_s["_pick"] = fri_s["pick_type"].astype(str).str.lower()
keep_s = fri_s[
    (fri_s["_pick"] == "goblin")
    & fri_s["_prop"].isin(["shots", "shots on target", "saves"])
].copy()
keep_s["l5"] = keep_s.apply(l5_val, axis=1)
keep_s["l10"] = keep_s.apply(l10_val, axis=1)
print()
print(f"SOCCER Friday Goblin Shots/SOT/Saves in step8: {len(keep_s)}")
if len(keep_s) == 0:
    print("  (none — Friday soccer may not be in Thursday step8)")
else:
    print(f"  L5==5: {(keep_s['l5']==5).sum()}")
    print(f"  L5==4: {(keep_s['l5']==4).sum()}")
    print(f"  L5<=3 or missing: {((keep_s['l5'].isna()) | (keep_s['l5']<=3)).sum()}")
    eq5s = keep_s[keep_s["l5"] == 5]
    print(f"  L5=5 count {len(eq5s)}")
    for _, r in eq5s.sort_values(["_st", "player", "line"]).iterrows():
        print(
            f"    {str(r['_st'])[11:16]}  {r['player']:28} {r.get('team','')}  "
            f"{r['prop_type']} G{r['line']}  L5=5 L10={r['l10']}"
        )
