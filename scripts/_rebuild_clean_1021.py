"""Rebuild 2026-10-21 with clean_minutes + blended leaders; print clean compact."""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "Sports" / "NBA" / "scripts"))

from step8_add_direction_context import build_clean_xlsx
from utils.nba_minutes_role import (
    load_nba_box_frame,
    role_changed_from_minutes,
    _status_from_mpg,
    player_minutes_profiles,
)

SLATE = "2026-10-21"
AS_OF = date(2026, 10, 21)
SRC = ROOT / "outputs" / SLATE / "nba" / "step8_all_direction_clean.parquet"
XLSX = ROOT / "outputs" / SLATE / "nba" / "step8_all_direction_clean.xlsx"
OUT = ROOT / f"fit_list_{SLATE}_nba_clean_minutes.txt"

load_nba_box_frame.cache_clear()

# Sanity: Bey/Quickley should NOT role_change (starter↔rotation ramp)
profs = player_minutes_profiles(as_of=AS_OF)
for n in ("saddiq bey", "immanuel quickley", "stephon castle", "karl-anthony towns"):
    p = profs.get(n)
    if not p:
        continue
    print(
        n,
        "season",
        round(p.min_season_avg or 0, 1),
        _status_from_mpg(p.min_season_avg),
        "pre",
        round(p.min_preseason_avg or 0, 1),
        _status_from_mpg(p.min_preseason_avg),
        "role_chg",
        role_changed_from_minutes(p.min_season_avg, p.min_preseason_avg),
    )

df = pd.read_parquet(SRC)
for a, b in {
    "Player": "player",
    "Prop": "prop_type",
    "Pick Type": "pick_type",
    "Line": "line",
    "Team": "team",
    "Pos": "pos",
    "Min Tier": "minutes_tier",
    "Opp": "opp_team",
}.items():
    if a in df.columns and b not in df.columns:
        df[b] = df[a]

build_clean_xlsx(df, str(XLSX), source_hint=str(SRC), as_of=AS_OF)

all_df = pd.read_excel(XLSX, sheet_name="ALL")
lead = pd.read_excel(XLSX, sheet_name="Team Leaders")


def _v(x):
    if x is None or (isinstance(x, float) and x != x):
        return "-"
    s = str(x).strip()
    return "-" if s.lower() in {"nan", "none", "null", "<na>"} else s


def _gap_num(x):
    try:
        if x is None or str(x).strip() in {"", "no_minutes_data", "nan"}:
            return 999.0
        return abs(float(x))
    except (TypeError, ValueError):
        return 999.0


tier_ord = {"A": 0, "B": 1, "C": 2, "D": 3}
pool = all_df[
    all_df["Pick Type"].astype(str).str.contains("Standard|Goblin", case=False, na=False)
].copy()
pool = pool[~pool["Pick Type"].astype(str).str.contains("Demon", case=False, na=False)]
# clean_minutes True
cm = pool["Clean Minutes"]
if cm.dtype == object:
    clean = pool[cm.astype(str).str.lower().isin(["true", "1", "yes"])].copy()
else:
    clean = pool[cm.fillna(False).astype(bool)].copy()

clean["_tier_ord"] = clean["Tier"].map(tier_ord).fillna(9)
clean["_gap_abs"] = clean["L5 Min Gap"].map(_gap_num)
clean = clean.sort_values(["_tier_ord", "_gap_abs", "Player", "Prop"])

seen = set()
lines = [
    f"CLEAN MINUTES NBA {SLATE} (|gap|<=3, no up/down, status unchanged)",
    f"n_rows={len(clean)} (unique player|prop|line|pick below)",
    "",
]
n = 0
for _, r in clean.iterrows():
    key = (str(r.Player), str(r.Prop), str(r.Line), str(r["Pick Type"]))
    if key in seen:
        continue
    seen.add(key)
    n += 1
    lines.append(
        f"{n:2d}. [{_v(r.get('Tier'))}] {r.Player} | {r.Prop} {r['Pick Type']} {r.Line} | "
        f"Dir={_v(r.get('Direction'))} | "
        f"MinTier={_v(r['Min Tier'])} Starter={_v(r['Starter'])} StarterPre={_v(r.get('Starter Pre'))} | "
        f"L5={_v(r['Min L5'])} Pre={_v(r['Min Preseason'])} Season={_v(r['Min Season'])} "
        f"Gap={_v(r['L5 Min Gap'])} Flag={_v(r.get('Minutes Flag'))} | "
        f"Rank={_v(r.get('Team Rank'))}/{_v(r.get('Team N'))} Score={_v(r.get('Rank Score'))}"
    )

lines.append("")
lines.append("=== TEAM LEADERS sample (NOP Assists/Points — blended) ===")
from utils.nba_minutes_role import _norm_team

for team in ["NOP", "HOU"]:
    tm = _norm_team(team)
    sub = lead[lead.Team.astype(str) == tm]
    lines.append(f"-- {team} --")
    for stat in ["Assists", "Points"]:
        block = sub[sub.Stat == stat].copy()
        if block.empty:
            continue
        block["_ord"] = block.Side.map({"top": 0, "bottom": 1}).fillna(2)
        block = block.sort_values(["_ord", "Rank"])
        lines.append(f"  {stat}:")
        for _, row in block.iterrows():
            gp_rs = row.get("GP_RS", "")
            gp_pre = row.get("GP_Pre", "")
            lines.append(
                f"    {row.Side:6} #{int(row.Rank):2d} {row.Player:22} "
                f"{row.PerGame:5.2f}  min={row.Minutes:4.1f}  "
                f"gp={int(row.GP)} (rs={gp_rs}/pre={gp_pre})  src={row.Source}"
            )
    lines.append("")

# Spot checks
lines.append("=== SPOT CHECKS ===")
for name in ["Saddiq Bey", "Immanuel Quickley", "Isaiah Collier", "Al Horford"]:
    sub = all_df[all_df.Player == name]
    if sub.empty:
        continue
    r = sub.iloc[0]
    lines.append(
        f"{name}: MinTier={r['Min Tier']} Starter={r['Starter']} StarterPre={r.get('Starter Pre')} "
        f"Clean={r.get('Clean Minutes')} Flag={r.get('Minutes Flag')} "
        f"Gap={r.get('L5 Min Gap')} Season={r.get('Min Season')} Pre={r.get('Min Preseason')}"
    )

text = "\n".join(lines) + "\n"
OUT.write_text(text, encoding="utf-8")
# stdout may choke on unicode; write is enough
print(f"wrote {OUT} clean_unique={n} clean_rows={len(clean)}")
print(text.encode("ascii", "replace").decode("ascii"))
