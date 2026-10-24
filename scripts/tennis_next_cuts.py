import json, math, re, sys
from collections import defaultdict
from datetime import date, datetime, timezone
from pathlib import Path

_REPO = Path(r"H:\PropORACLE")
MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(_REPO), str(_REPO / "Sports" / "Tennis" / "scripts")]
from tennis_shared import (
    build_sackmann_player_index, ensure_sackmann_matches, norm_key,
    norm_tennis_prop, parse_sackmann_tourney_date,
)
from utils.tennis_keep_gates import GAMES_WON_L5_SECOND_WON_MIN

OUT = _REPO / "data" / "reports" / "tennis_next_cuts.json"
GRADED_DIRS = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]

def finite(v):
    if v is None or v == "": return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f

def pct(h, n):
    return None if n <= 0 else round(100.0 * h / n, 1)

def wilson(h, n, z=1.96):
    """Wilson score 95% CI for binomial proportion, returned as percents."""
    if n <= 0:
        return None, None, None
    p = h / n
    z2 = z * z
    den = 1 + z2 / n
    center = (p + z2 / (2 * n)) / den
    half = (z / den) * math.sqrt(p * (1 - p) / n + z2 / (4 * n * n))
    lo = max(0.0, center - half)
    hi = min(1.0, center + half)
    return round(100 * p, 1), round(100 * lo, 1), round(100 * hi, 1)

def book(row):
    pt = str(row.get("pick_type") or "").strip().lower()
    d = str(row.get("direction") or row.get("over_under") or "").strip().upper()
    if "demon" in pt: return None
    if "goblin" in pt: return f"Goblin {d if d in ('OVER','UNDER') else 'OVER'}"
    if d in ("OVER", "UNDER"): return f"Standard {d}"
    return None

def iter_props(path):
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if isinstance(data, dict):
        for k in ("props", "legs", "rows", "graded", "items"):
            v = data.get(k)
            if isinstance(v, list) and v and isinstance(v[0], dict):
                return v
        for v in data.values():
            if isinstance(v, list) and v and isinstance(v[0], dict):
                if "player" in v[0] or "prop" in v[0] or "sport" in v[0]:
                    return v
    return []

def l5(row, side):
    keys = ("l5_over","last5_over","L5 Over") if side=="OVER" else ("l5_under","last5_under","L5 Under")
    for k in keys:
        v = finite(row.get(k))
        if v is not None and v <= 10: return v
    return None

def l10(row, side):
    keys = ("l10_over","last10_over","L10 Over","line_hits_over_10") if side=="OVER" else ("l10_under","last10_under","L10 Under","line_hits_under_10")
    for k in keys:
        v = finite(row.get(k))
        if v is not None and v <= 10: return v
    return None

print("Loading Sackmann…")
matches = ensure_sackmann_matches()
index = build_sackmann_player_index(matches)
hand_by = {}
for _, rd in matches.iterrows():
    for nc, hc in (("winner_name","winner_hand"),("loser_name","loser_hand")):
        nm = norm_key(str(rd.get(nc) or ""))
        hd = str(rd.get(hc) or "").strip().upper()
        if nm and hd in {"L","R","U"}:
            hand_by[nm] = hd

seen_names=set(); paths=[]
for d in GRADED_DIRS:
    if not d.exists(): continue
    for p in sorted(d.glob("graded_props_20*.json")):
        if p.name in seen_names: continue
        seen_names.add(p.name); paths.append(p)

legs=[]; seen=set()
for path in paths:
    m=re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
    if not m: continue
    slate=date.fromisoformat(m.group(1))
    tree="main_cp" if "PropORACLE_main_cp" in str(path) else "open"
    for row in iter_props(path):
        if str(row.get("sport") or "").lower()!="tennis": continue
        result=str(row.get("result") or "").upper()
        if result not in ("HIT","MISS"): continue
        b=book(row)
        if not b: continue
        prop_raw=str(row.get("prop") or "").strip()
        pnorm=norm_tennis_prop(prop_raw)
        player=str(row.get("player") or "").strip()
        line=finite(row.get("line"))
        pk=norm_key(player)
        dkey=f"{slate}|{pk}|{pnorm}|{b}|{line}|{result}|{row.get('pp_projection_id')}"
        if dkey in seen: continue
        seen.add(dkey)
        side="OVER" if "OVER" in b else "UNDER"
        prior=[]
        for rec in index.get(pk) or []:
            dt=parse_sackmann_tourney_date(rec.get("date"))
            if dt and dt < slate: prior.append(rec)
        sw_vals=[finite(r.get("second_won_pct")) for r in prior[:5]]
        sw_vals=[v for v in sw_vals if v is not None]
        sw=sum(sw_vals)/len(sw_vals) if sw_vals else None
        opp=norm_key(str(row.get("opp_team") or row.get("opponent") or row.get("opp") or ""))
        today_hand=hand_by.get(opp) if opp else None
        last_hand=str((prior[0].get("opp_hand") if prior else "") or "").strip().upper() or None
        if last_hand and last_hand not in {"L","R","U"}: last_hand=None
        opp_hand=today_hand or last_hand
        lefty=True if opp_hand=="L" else (False if opp_hand in {"R","U"} else None)
        src=str(row.get("actual_source") or "").strip()
        legs.append({
            "slate": slate.isoformat(), "tree": tree, "prop": pnorm, "book": b,
            "hit": 1 if result=="HIT" else 0,
            "l5": l5(row, side), "l10": l10(row, side),
            "sw": sw, "lefty": lefty,
            "blank_src": src=="", "src": src or "(blank)",
            "contaminated_aces": (pnorm=="aces" and b=="Standard OVER" and (src=="" or tree=="main_cp")),
        })

print(f"legs decided={len(legs)}")

def cell(label, subset):
    n=len(subset); h=sum(l["hit"] for l in subset)
    hr, lo, hi = wilson(h, n)
    return {"label": label, "n": n, "hits": h, "hit_pct": hr, "ci95_lo": lo, "ci95_hi": hi,
            "ci95": f"[{lo}, {hi}]%" if lo is not None else None}

# --- 1) CIs on key cells ---
gw = [l for l in legs if l["prop"]=="games_won" and l["book"]=="Goblin OVER"]
tg = [l for l in legs if l["prop"]=="match_total_games" and l["book"]=="Goblin OVER"]
aces_so = [l for l in legs if l["prop"]=="aces" and l["book"]=="Standard OVER"]
aces_su = [l for l in legs if l["prop"]=="aces" and l["book"]=="Standard UNDER"]
df_su = [l for l in legs if l["prop"]=="double_faults" and l["book"]=="Standard UNDER"]

gw_lefty=[l for l in gw if l["l10"] is not None and l["l10"]>=8 and l["lefty"] is True]
gw_sw=[l for l in gw if l["l10"] is not None and l["l10"]>=8 and l["sw"] is not None and l["sw"]>=GAMES_WON_L5_SECOND_WON_MIN]
gw_union=[l for l in gw if l["l10"] is not None and l["l10"]>=8 and (l["lefty"] is True or (l["sw"] is not None and l["sw"]>=GAMES_WON_L5_SECOND_WON_MIN))]
tg_l5l10=[l for l in tg if l["l5"] is not None and l["l5"]>=4 and l["l10"] is not None and l["l10"]>=8]
tg_lefty=[l for l in tg if l["l10"] is not None and l["l10"]>=8 and l["lefty"] is True]
tg_union=[l for l in tg if l["l10"] is not None and l["l10"]>=8 and ((l["l5"] is not None and l["l5"]>=4) or l["lefty"] is True)]

ci_cells = [
    cell("Goblin GW L10≥8 + 2nd-won≥45.7", gw_sw),
    cell("Goblin GW L10≥8+(lefty|2nd-won) union", gw_union),
    cell("Goblin GW L10≥8 + lefty", gw_lefty),
    cell("Goblin TG (L5≥4+L10≥8)|lefty union", tg_union),
    cell("Goblin TG L10≥8 + lefty", tg_lefty),
    cell("Goblin TG L5≥4 + L10≥8", tg_l5l10),
    cell("Std UNDER Aces ungated", aces_su),
    cell("Std UNDER Double Faults ungated", df_su),
    cell("Std OVER Aces pooled (contaminated)", aces_so),
    cell("Std OVER Aces clean (excl blank/main_cp)", [l for l in aces_so if not l["contaminated_aces"]]),
]

print("\n=== 1) Wilson 95% CIs ===")
for c in ci_cells:
    print(f"  {c['hit_pct']}% {c['ci95']:16} n={c['n']:4}  {c['label']}")

# --- 2) L10=10 on Std UNDER Aces/DF ---
def under_windows(prop, label):
    pool=[l for l in legs if l["prop"]==prop and l["book"]==f"Standard UNDER"]
    out=[]
    for name, fn in [
        ("ungated", lambda l: True),
        ("L5≥4", lambda l: l["l5"] is not None and l["l5"]>=4),
        ("L5=5", lambda l: l["l5"] is not None and l["l5"]==5),
        ("L10≥8", lambda l: l["l10"] is not None and l["l10"]>=8),
        ("L10=10", lambda l: l["l10"] is not None and l["l10"]==10),
        ("L10≥9", lambda l: l["l10"] is not None and l["l10"]>=9),
        ("L5≥4+L10≥8", lambda l: l["l5"] is not None and l["l5"]>=4 and l["l10"] is not None and l["l10"]>=8),
        ("L5≥4+L10=10", lambda l: l["l5"] is not None and l["l5"]>=4 and l["l10"] is not None and l["l10"]==10),
    ]:
        sub=[l for l in pool if fn(l)]
        c=cell(f"{label} · {name}", sub)
        out.append(c)
    return out

aces_windows=under_windows("aces", "Std UNDER Aces")
df_windows=under_windows("double_faults", "Std UNDER DF")
print("\n=== 2) Std UNDER Aces/DF L10 windows ===")
for c in aces_windows+df_windows:
    print(f"  {c['hit_pct']}% {c['ci95'] or '':16} n={c['n']:4}  {c['label']}")

# --- 3) Fresh Std OVER Aces after excluding contaminated ---
aces_clean=[l for l in aces_so if not l["contaminated_aces"]]
aces_contam=[l for l in aces_so if l["contaminated_aces"]]
print("\n=== 3) Std OVER Aces re-pull ===")
for c in [
    cell("pooled (as graded)", aces_so),
    cell("contaminated batch only", aces_contam),
    cell("clean excl blank/main_cp", aces_clean),
    cell("clean + slate<2026-08-15", [l for l in aces_clean if l["slate"]<"2026-08-15"]),
    cell("sackmann/TA sourced only", [l for l in aces_so if l["src"] in ("sackmann","tennis_abstract")]),
]:
    print(f"  {c['hit_pct']}% {c['ci95'] or '':16} n={c['n']:4}  {c['label']}")

report={
    "generated_at": datetime.now(timezone.utc).isoformat(),
    "script_note": "scripts inline / tennis next cuts from graded_props",
    "method": "same dedupe as tennis_board_category_hit_rates.py; Wilson 95% CI",
    "confidence_intervals": ci_cells,
    "std_under_aces_windows": aces_windows,
    "std_under_df_windows": df_windows,
    "std_over_aces_rebacfill_proxy": {
        "note": "True re-backfill not run; proxy = exclude contaminated blank/main_cp batch (same 142 rows).",
        "pooled": cell("pooled", aces_so),
        "contaminated": cell("contaminated", aces_contam),
        "clean": cell("clean", aces_clean),
    },
    "takeaways": [],
}
# takeaways
ungated_aces=aces_windows[0]
l10_10_aces=next(c for c in aces_windows if "L10=10" in c["label"] and "L5" not in c["label"])
ungated_df=df_windows[0]
l10_10_df=next(c for c in df_windows if "L10=10" in c["label"] and "L5" not in c["label"])
report["takeaways"]=[
    f"GW 2nd-won keep: {gw_sw and cell('',gw_sw)['hit_pct']}% n={len(gw_sw)} CI {cell('',gw_sw)['ci95']}",
    f"Std UNDER Aces ungated stays best; L10=10 n={l10_10_aces['n']} hit={l10_10_aces['hit_pct']}% vs ungated {ungated_aces['hit_pct']}%",
    f"Std UNDER DF L10=10 n={l10_10_df['n']} hit={l10_10_df['hit_pct']}% vs ungated {ungated_df['hit_pct']}%",
    f"Std OVER Aces clean proxy {cell('',aces_clean)['hit_pct']}% n={len(aces_clean)} (not a live re-backfill)",
]

OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
print(f"\nWrote {OUT}")
