"""Pull Tatum FG Attempted Under 21.5 from rebuilt step8."""
from __future__ import annotations

import sys
from pathlib import Path

_REPO = Path(r"H:\PropORACLE")
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]
import rank_best_props_today as R
from utils.category_hit_rate import load_category_hr_index, lookup_category_hr
from utils.gap_ranker import wilson_low
from utils.leg_flags import collect_leg_flags
from utils.prop_norm import canon_prop

ROOT = Path(r"H:\PropORACLE")
DATE = "2026-10-20"
df = R.load_nba(ROOT, DATE)
print("nba step8 rows", len(df))
sub = df[df["player"].astype(str).str.contains("Tatum", case=False, na=False)]
fga = sub[sub["prop_type"].astype(str).str.contains("FG Attempt", case=False, na=False)]
print("tatum fga rows", len(fga))
for _, raw in fga.iterrows():
    print(
        "RAW",
        raw.get("pick_type"),
        raw.get("prop_type"),
        raw.get("line"),
        "L5",
        raw.get("L5 Over") or raw.get("l5_over") or raw.get("line_hits_over_5"),
        raw.get("L5 Under") or raw.get("l5_under") or raw.get("line_hits_under_5"),
        "avg",
        raw.get("Last 5 Avg") or raw.get("stat_last5_avg") or raw.get("last5_avg"),
    )
    vals = []
    for i in range(1, 6):
        for k in (f"stat_g{i}", f"G{i}", f"g{i}"):
            if k in raw.index and str(raw.get(k) or "").strip() not in {"", "nan", "None"}:
                try:
                    vals.append(float(raw.get(k)))
                    break
                except (TypeError, ValueError):
                    pass
    print("  vals", vals)
    for i in range(1, 6):
        for k in (f"game_date_g{i}", f"Game {i} Date", f"g{i}_date"):
            if k in raw.index and str(raw.get(k) or "").strip() not in {"", "nan", "None"}:
                print(f"  {k}", raw.get(k))
                break

idx = load_category_hr_index()
rows = R.recs(df)
tat = [
    r
    for r in rows
    if "tatum" in str(r.get("player") or "").lower()
    and canon_prop("NBA", r.get("prop")) == "fga"
]
print("recs", len(tat))
for r in tat:
    side = r.get("side") or "UNDER"
    print(R._fmt(r, side))
    o, u = r.get("l5_over"), r.get("l5_under")
    cell = lookup_category_hr(
        idx,
        sport="NBA",
        prop="FG Attempted",
        pick_type=r.get("pick_type") or "Standard",
        direction=side,
    )
    print("  pool", f"{r.get('pick_type')} {side}")
    print("  L5=5 slice", bool(side == "UNDER" and int(u or 0) == 5 and int(o or 0) == 0))
    print("  cell", cell)
    if cell and cell.get("n"):
        hits = cell.get("hit")
        if hits is None:
            hits = cell.get("hits")
        if hits is None and cell.get("hr") is not None:
            hits = round(float(cell["hr"]) * int(cell["n"]))
        print(
            "  wilson_low",
            wilson_low(int(hits or 0), int(cell["n"])),
            "n",
            cell["n"],
            "hr",
            cell.get("hr") or cell.get("rate"),
        )
    else:
        print("  cell used: no ungated NBA fga cell in catalog")
    vals = [13.0, 19.0, 16.0, 17.0, 19.0]
    extra = {
        "game_date_g1": "2026-04-30",
        "game_date_g2": "2026-04-28",
        "game_date_g3": "2026-04-26",
        "game_date_g4": "2026-04-23",
        "game_date_g5": "2026-04-20",
        "sport": "NBA",
        "prop_type": "FG Attempted",
    }
    flags = collect_leg_flags(
        values=vals,
        line=float(r.get("line") or 21.5),
        side=side,
        pick=r.get("pick_type") or "Standard",
        sport="NBA",
        prop="FG Attempted",
        row={**dict(r), **extra},
        wilson=(
            wilson_low(int(cell.get("hit") or 0), int(cell["n"]))
            if cell and cell.get("n")
            else None
        ),
        slice_n=int(cell["n"]) if cell and cell.get("n") else None,
    )
    print("  flags", flags)
    print("  ticketable", r.get("ticketable"))
