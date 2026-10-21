"""Export NFL L5-avg gap browse board to Excel (Sun 2026-09-20)."""
from __future__ import annotations

from pathlib import Path

import pandas as pd

STEP8 = Path(
    r"H:/PropORACLE_main_cp/outputs/2026-09-20/nfl/step8_nfl_direction_clean.xlsx"
)
OUT = Path(r"H:/PropORACLE/outputs/2026-09-20/nfl/nfl_l5_gap_browse_2026-09-20.xlsx")
OUT.parent.mkdir(parents=True, exist_ok=True)

df = pd.read_excel(STEP8)
df = df[~df["Player"].astype(str).str.contains(r"\+", regex=True)].copy()
df = df[
    ~df["Prop"]
    .astype(str)
    .str.contains("combo|per_carry", case=False, na=False)
].copy()
gt = pd.to_datetime(df["Game Time"], utc=True, errors="coerce")
df["game_et"] = gt.dt.tz_convert("America/New_York")
df = df[df["game_et"].dt.date.astype(str) == "2026-09-20"].copy()
df = df[df["Pick Type"].isin(["Standard", "Goblin"])].copy()

df["line"] = pd.to_numeric(df["Line"], errors="coerce")
df["l5_avg"] = pd.to_numeric(df["Projection"], errors="coerce")
df["gap"] = df["l5_avg"] - df["line"]
df["gap_abs"] = df["gap"].abs()
df = df.dropna(subset=["line", "l5_avg", "gap_abs"])

df["prop"] = (
    df["Prop"].astype(str).str.replace("player_", "", regex=False).str.replace("_", " ")
)
df["kick_et"] = df["game_et"].dt.strftime("%H:%M")
df["matchup"] = df["Team"].astype(str) + " vs " + df["Opp"].astype(str)
df["side"] = df["Direction"].astype(str).str.upper()
df["play_gap"] = df.apply(
    lambda r: float(r["gap"]) if r["side"] == "OVER" else -float(r["gap"]), axis=1
)

# one face per player × prop × pick (largest |gap|)
df = df.sort_values("gap_abs", ascending=False)
dedup = df.drop_duplicates(subset=["Player", "Prop", "Pick Type"], keep="first").copy()

cols = [
    "Player",
    "Team",
    "Opp",
    "matchup",
    "kick_et",
    "prop",
    "Pick Type",
    "side",
    "line",
    "l5_avg",
    "gap",
    "gap_abs",
    "play_gap",
    "L5 Over",
    "L5 Under",
    "L10 Over",
    "L10 Under",
    "Def Tier",
    "Games Used",
    "Season L5 Flag",
    "Stat G1",
]

# Sheet 1: all faces ranked by |gap|
all_ranked = dedup[cols].sort_values("gap_abs", ascending=False).reset_index(drop=True)
all_ranked.insert(0, "rank_abs_gap", range(1, len(all_ranked) + 1))

# Sheet 2: mid-board only (safer browse band)
mid = dedup.copy()


def is_mid(r) -> bool:
    prop = str(r["prop"]).lower()
    pg = float(r["play_gap"])
    avg = float(r["l5_avg"])
    if r["side"] == "OVER" and pg <= 0:
        return False
    if r["side"] == "UNDER" and float(r["gap"]) >= 0:
        return False
    if any(x in prop for x in ("yard", "rush", "rec", "pass")):
        if r["side"] == "UNDER" and avg < 8:
            return False
        return 10.0 <= pg <= 45.0
    return 0.5 <= pg <= 8.0


mid = mid[mid.apply(is_mid, axis=1)].copy()
mid = mid.sort_values(["play_gap"], ascending=False).reset_index(drop=True)
mid.insert(0, "rank_play_gap", range(1, len(mid) + 1))

# Sheet 3: Goblin OVER mid
gob = mid[(mid["Pick Type"] == "Goblin") & (mid["side"] == "OVER")][cols].copy()
gob.insert(0, "rank", range(1, len(gob) + 1))

# Sheet 4: Standard mid (both sides)
std = mid[mid["Pick Type"] == "Standard"][cols].copy()
std.insert(0, "rank", range(1, len(std) + 1))

# Sheet 5: largest gap per unique game
seen = set()
per_game_rows = []
for _, r in dedup.sort_values("gap_abs", ascending=False).iterrows():
    key = tuple(sorted([str(r["Team"]), str(r["Opp"])]))
    if key in seen:
        continue
    seen.add(key)
    per_game_rows.append(r)
per_game = pd.DataFrame(per_game_rows)[cols].reset_index(drop=True)
per_game.insert(0, "rank", range(1, len(per_game) + 1))

readme = pd.DataFrame(
    {
        "note": [
            "NFL L5-avg gap browse — 2026-09-20 (Sunday)",
            "Gap = Projection (season-cut L5 avg) − Line. Week 2 ≈ almost all n=1 (Week-1 actual).",
            "DO NOT pull top-N from 'all_by_abs_gap' as picks. Biggest |gap| floats single-game outliers.",
            "Use mid_board / goblin_mid_over / standard_mid as hand-check browse pools.",
            "Mid band: yards play-gap 10–45 (unders need real W1 avg ≥ 8); counting 0.5–8.",
            "Demons/combos/per_carry excluded. One face per player×prop×pick_type.",
            f"Source: {STEP8}",
            f"Wrote: {OUT}",
        ]
    }
)

with pd.ExcelWriter(OUT, engine="openpyxl") as xw:
    readme.to_excel(xw, sheet_name="README", index=False)
    all_ranked.to_excel(xw, sheet_name="all_by_abs_gap", index=False)
    mid_out = mid[["rank_play_gap", *cols]].copy()
    mid_out.to_excel(xw, sheet_name="mid_board", index=False)
    gob.to_excel(xw, sheet_name="goblin_mid_over", index=False)
    std.to_excel(xw, sheet_name="standard_mid", index=False)
    per_game.to_excel(xw, sheet_name="largest_gap_per_game", index=False)

print(f"wrote {OUT}")
print(
    f"rows all={len(all_ranked)} mid={len(mid)} gob={len(gob)} "
    f"std={len(std)} games={len(per_game)}"
)
