"""Build NFL O/D rankings by prop category from step8 + unit ranks."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:/PropORACLE")
STEP8 = ROOT / "outputs/2026-09-20/nfl/step8_nfl_direction_clean.xlsx"
DATA = ROOT / "Sports/NFL/data"
OUT_XLSX = ROOT / "outputs/2026-09-20/nfl/nfl_od_rankings_by_prop_week2.xlsx"
OUT_JSON = ROOT / "outputs/2026-09-20/nfl/nfl_od_rankings_by_prop_week2.json"

df = pd.read_excel(STEP8)
df = df[~df["Player"].astype(str).str.contains(r"\+", regex=True)].copy()
df["prop"] = df["Prop"].astype(str).str.lower()
df["team"] = df["Team"].astype(str).str.upper().str.strip()
df["opp"] = df["Opp"].astype(str).str.upper().str.strip()
df["pick"] = df["Pick Type"].astype(str)
df["side"] = df["Direction"].astype(str).str.upper()
df["line"] = pd.to_numeric(df["Line"], errors="coerce")
df["proj"] = pd.to_numeric(df["Projection"], errors="coerce")
df["def_tier"] = df["Def Tier"].astype(str)
df["def_rank"] = pd.to_numeric(df.get("Def Rank"), errors="coerce")
df["opp_pass_def"] = pd.to_numeric(df.get("Opp Pass Def Rank"), errors="coerce")
df["opp_rush_def"] = pd.to_numeric(df.get("Opp Rush Def Rank"), errors="coerce")
df["pass_off"] = pd.to_numeric(df.get("Team Pass Off Rank"), errors="coerce")
df["rush_off"] = pd.to_numeric(df.get("Team Rush Off Rank"), errors="coerce")
df["rec_share"] = pd.to_numeric(df.get("Rec Yds Share"), errors="coerce")
df["rush_share"] = pd.to_numeric(df.get("Rush Yds Share"), errors="coerce")
df["pass_share"] = pd.to_numeric(df.get("Pass Yds Share"), errors="coerce")

# --- unit / defense reference ---
ol = pd.read_csv(DATA / "nfl_ol_ranks.csv")
dl = pd.read_csv(DATA / "nfl_dl_ranks.csv")
sec = pd.read_csv(DATA / "nfl_secondary_ranks.csv")
box = pd.read_csv(DATA / "nfl_box_ranks.csv")
units = pd.read_csv(DATA / "nfl_team_unit_ranks.csv")
defense = pd.read_csv(DATA / "defense_rankings.csv")

for frame in (ol, dl, sec, box, units, defense):
    frame["team"] = frame["team"].astype(str).str.upper().str.strip()

# Keep latest season if present
def latest(frame: pd.DataFrame) -> pd.DataFrame:
    if "season" in frame.columns:
        s = pd.to_numeric(frame["season"], errors="coerce")
        mx = s.max()
        return frame[s == mx].copy()
    return frame


ol, dl, sec, box, units, defense = map(latest, [ol, dl, sec, box, units, defense])

# --- team offense ranks (from unit table + step8 shares) ---
off = units[
    [
        "team",
        "pass_off_rank",
        "rush_off_rank",
        "score_off_rank",
        "pass_off_ypg",
        "rush_off_ypg",
        "pf_pg",
    ]
].copy()

# opportunity: top player share median per team for rec/rush/pass props
share_rows = []
for team, g in df.groupby("team"):
    if team in ("NAN", "") or "/" in team:
        continue
    rec = g[g["prop"].str.contains("receiving_yards|receptions", regex=True)]
    rush = g[g["prop"].str.contains("rushing_yards", regex=True) & ~g["prop"].str.contains("rec")]
    pas = g[g["prop"].str.contains("passing_yards", regex=True)]
    share_rows.append(
        {
            "team": team,
            "top_rec_share": float(rec["rec_share"].max()) if rec["rec_share"].notna().any() else None,
            "top_rush_share": float(rush["rush_share"].max()) if rush["rush_share"].notna().any() else None,
            "qb_pass_share": float(pas["pass_share"].max()) if pas["pass_share"].notna().any() else None,
            "pass_off_r": float(g["pass_off"].dropna().median()) if g["pass_off"].notna().any() else None,
            "rush_off_r": float(g["rush_off"].dropna().median()) if g["rush_off"].notna().any() else None,
        }
    )
shares = pd.DataFrame(share_rows)
off = off.merge(shares, on="team", how="outer")

# tier helper: rank 1 = best offense / stingiest or weakest defense depending on axis
def tier_from_rank(r, n=32):
    if pd.isna(r):
        return ""
    r = float(r)
    if r <= 8:
        return "Elite"
    if r <= 14:
        return "Above"
    if r <= 20:
        return "Avg"
    if r <= 26:
        return "Below"
    return "Weak"


off["pass_off_tier"] = off["pass_off_rank"].map(tier_from_rank)
off["rush_off_tier"] = off["rush_off_rank"].map(tier_from_rank)
off["score_off_tier"] = off["score_off_rank"].map(tier_from_rank)
off = off.sort_values("pass_off_rank", na_position="last")

# --- team defense ranks ---
deff = defense.merge(
    units[["team", "pass_def_rank", "rush_def_rank", "score_def_rank", "pa_pg"]],
    on="team",
    how="outer",
    suffixes=("", "_u"),
)
# prefer defense_rankings pass/rush if present
if "pass_def_rank_u" in deff.columns:
    deff["pass_def_rank"] = deff["pass_def_rank"].fillna(deff["pass_def_rank_u"])
    deff["rush_def_rank"] = deff["rush_def_rank"].fillna(deff["rush_def_rank_u"])

deff = deff.merge(sec[["team", "secondary_rank", "tier"]].rename(columns={"tier": "secondary_tier"}), on="team", how="left")
deff = deff.merge(box[["team", "box_rank", "tier"]].rename(columns={"tier": "box_tier"}), on="team", how="left")
deff = deff.merge(dl[["team", "dl_rank", "pass_rush_rank", "sacks_pg", "tier"]].rename(columns={"tier": "dl_tier"}), on="team", how="left")
deff = deff.merge(ol[["team", "ol_rank", "tier"]].rename(columns={"tier": "ol_tier"}), on="team", how="left")

deff["pass_def_tier"] = deff["pass_def_rank"].map(tier_from_rank)
deff["rush_def_tier"] = deff["rush_def_rank"].map(tier_from_rank)
# for sacks: low pass_rush_rank / high sacks_pg = good pass rush (target QB under pressure overs / sack overs)
deff = deff.sort_values("pass_def_rank", na_position="last")

# --- keep-list prop categories from step8 Def Tier distribution ---
KEEP = {
    "receiving_yards": "pass",
    "receptions": "pass",
    "rushing_yards": "rush",
    "passing_yards": "pass",
    "sacks": "pass_rush",
    "kicking_points": "kick",
}

# For each prop: which opp Def Tiers appear for OVER clears vs UNDER
prop_target = []
for prop_key, axis in KEEP.items():
    sub = df[df["prop"].str.contains(prop_key, regex=False)].copy()
    if sub.empty:
        continue
    # unique team-opp-def for Standard+Goblin
    sub = sub[sub["pick"].isin(["Standard", "Goblin"])]
    # defensive rank to attack for OVER: Weak/Below; for UNDER: Elite/Above
    for tier in ["Weak", "Below Avg", "Avg", "Above Avg", "Elite"]:
        n = int((sub["def_tier"] == tier).sum())
        prop_target.append({"prop": prop_key, "axis": axis, "def_tier": tier, "faces": n})

prop_tier_df = pd.DataFrame(prop_target)

# Target / fade tables: teams ranked for each keep prop
# OVER target = face Weak/Below pass or rush def; FADE UNDER into Weak (don't under) etc.

def team_def_for_prop(axis: str) -> pd.DataFrame:
    if axis == "pass":
        t = deff[["team", "pass_def_rank", "pass_def_tier", "secondary_rank", "secondary_tier", "pass_yards_allowed_pg"]].copy()
        t = t.rename(columns={"pass_def_rank": "def_rank", "pass_def_tier": "def_tier"})
        t["metric"] = "pass def (rec/pass yards)"
    elif axis == "rush":
        t = deff[["team", "rush_def_rank", "rush_def_tier", "box_rank", "box_tier", "rush_yards_allowed_pg"]].copy()
        t = t.rename(columns={"rush_def_rank": "def_rank", "rush_def_tier": "def_tier"})
        t["metric"] = "rush def"
    elif axis == "pass_rush":
        t = deff[["team", "dl_rank", "dl_tier", "pass_rush_rank", "sacks_pg"]].copy()
        t["def_rank"] = t["pass_rush_rank"]
        t["def_tier"] = t["dl_tier"]
        t["metric"] = "pass rush / sacks"
    else:
        t = deff[["team", "pass_def_rank", "pass_def_tier"]].copy()
        t = t.rename(columns={"pass_def_rank": "def_rank", "pass_def_tier": "def_tier"})
        t["metric"] = axis
    return t.dropna(subset=["def_rank"]).sort_values("def_rank")


sheets = {}
sheets["offense"] = off.sort_values("pass_off_rank", na_position="last")
sheets["defense"] = deff.sort_values("pass_def_rank", na_position="last")

for prop_key, axis in KEEP.items():
    t = team_def_for_prop(axis)
    # OVER targets = worst defenses (high rank number = Weak)
    over_tgt = t.nlargest(10, "def_rank").assign(action="TARGET_OVER")
    under_tgt = t.nsmallest(10, "def_rank").assign(action="TARGET_UNDER")
    fade_over = t.nsmallest(8, "def_rank").assign(action="FADE_OVER")
    cat = pd.concat([over_tgt, under_tgt], ignore_index=True)
    cat.insert(0, "prop", prop_key)
    sheets[f"keep_{prop_key[:20]}"] = cat

# --- slate matchup cross-tab (Week 2 board in this file) ---
# unique games
games = (
    df[df["opp"].notna() & (df["opp"] != "NAN") & ~df["team"].str.contains("/")]
    [["team", "opp"]]
    .drop_duplicates()
)
# canonicalize matchup
seen = set()
matchups = []
for _, r in games.iterrows():
    a, b = r.team, r.opp
    key = tuple(sorted([a, b]))
    if key in seen:
        continue
    seen.add(key)
    matchups.append((a, b))

cross = []
off_i = off.set_index("team")
def_i = deff.set_index("team")
for home, away in matchups:
    # treat as team vs opp both ways
    for team, opp in ((home, away), (away, home)):
        if team not in def_i.index or opp not in def_i.index:
            continue
        if team not in off_i.index:
            continue
        row = {
            "team": team,
            "opp": opp,
            "pass_off": off_i.loc[team].get("pass_off_rank"),
            "rush_off": off_i.loc[team].get("rush_off_rank"),
            "opp_pass_def": def_i.loc[opp].get("pass_def_rank"),
            "opp_rush_def": def_i.loc[opp].get("rush_def_rank"),
            "opp_secondary": def_i.loc[opp].get("secondary_rank"),
            "opp_box": def_i.loc[opp].get("box_rank"),
            "opp_pass_rush": def_i.loc[opp].get("pass_rush_rank"),
        }
        # edges: low off rank (good) vs high opp def rank (leaky) = OVER lean
        try:
            row["pass_edge"] = float(row["opp_pass_def"]) - float(row["pass_off"])
            row["rush_edge"] = float(row["opp_rush_def"]) - float(row["rush_off"])
        except (TypeError, ValueError):
            row["pass_edge"] = None
            row["rush_edge"] = None
        # lean labels
        pe, re_ = row["pass_edge"], row["rush_edge"]
        row["rec_pass_lean"] = (
            "OVER" if pe is not None and pe >= 8 else ("UNDER" if pe is not None and pe <= -8 else "NEUTRAL")
        )
        row["rush_lean"] = (
            "OVER" if re_ is not None and re_ >= 8 else ("UNDER" if re_ is not None and re_ <= -8 else "NEUTRAL")
        )
        # sack lean: face good pass rush (low pass_rush_rank)
        pr = row["opp_pass_rush"]
        try:
            row["sack_lean"] = "OVER" if float(pr) <= 10 else ("UNDER" if float(pr) >= 24 else "NEUTRAL")
        except (TypeError, ValueError):
            row["sack_lean"] = "NEUTRAL"
        cross.append(row)

cross_df = pd.DataFrame(cross).sort_values("pass_edge", ascending=False, na_position="last")
sheets["matchup_cross"] = cross_df

# top matchups per lean
best_pass_over = cross_df[cross_df["rec_pass_lean"] == "OVER"].head(12)
best_rush_over = cross_df[cross_df["rush_lean"] == "OVER"].head(12)
best_pass_under = cross_df[cross_df["rec_pass_lean"] == "UNDER"].head(12)
best_rush_under = cross_df[cross_df["rush_lean"] == "UNDER"].head(12)
sheets["best_pass_OVER"] = best_pass_over
sheets["best_rush_OVER"] = best_rush_over
sheets["best_pass_UNDER"] = best_pass_under
sheets["best_rush_UNDER"] = best_rush_under

readme = pd.DataFrame(
    {
        "note": [
            "NFL O/D rankings by prop — derived from step8 2026-09-20 + unit rank CSVs",
            "Offense: nfl_team_unit_ranks (pass/rush/score) + step8 top player shares",
            "Defense: defense_rankings + secondary/box/DL unit files",
            "Rank 1 = best unit. Tier: 1-8 Elite, 9-14 Above, 15-20 Avg, 21-26 Below, 27-32 Weak",
            "OVER target = Weak/Below opp def (high def_rank). UNDER target = Elite/Above (low def_rank)",
            "pass_edge = opp_pass_def_rank - team_pass_off_rank (>=8 OVER lean, <=-8 UNDER lean)",
            "Matchup cross uses Week-2 slate opponents in this step8 (not yet Week-3 schedule)",
            f"Source step8: {STEP8}",
            f"Light CSV: {ROOT / 'outputs/2026-09-20/nfl/step8_nfl_week2_light.csv'}",
        ]
    }
)

with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as xw:
    readme.to_excel(xw, sheet_name="README", index=False)
    sheets["offense"].to_excel(xw, sheet_name="offense_by_team", index=False)
    sheets["defense"].to_excel(xw, sheet_name="defense_by_team", index=False)
    for k, v in sheets.items():
        if k in ("offense", "defense"):
            continue
        name = k[:31]
        v.to_excel(xw, sheet_name=name, index=False)

# JSON summary for canvas
def rows(frame, n=32):
    out = []
    for _, r in frame.head(n).iterrows():
        out.append({k: (None if pd.isna(v) else (float(v) if isinstance(v, (int, float)) else str(v))) for k, v in r.items()})
    return out

payload = {
    "offense_top_pass": rows(off.sort_values("pass_off_rank").head(10)),
    "offense_top_rush": rows(off.sort_values("rush_off_rank").head(10)),
    "defense_worst_pass": rows(deff.sort_values("pass_def_rank", ascending=False).head(10)),
    "defense_best_pass": rows(deff.sort_values("pass_def_rank").head(10)),
    "defense_worst_rush": rows(deff.sort_values("rush_def_rank", ascending=False).head(10)),
    "defense_best_rush": rows(deff.sort_values("rush_def_rank").head(10)),
    "sack_best_rush": rows(deff.sort_values("pass_rush_rank").head(10)),
    "best_pass_over": rows(best_pass_over, 12),
    "best_rush_over": rows(best_rush_over, 12),
    "best_pass_under": rows(best_pass_under, 12),
    "best_rush_under": rows(best_rush_under, 12),
}
OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print("wrote", OUT_XLSX)
print("offense teams", len(off), "defense", len(deff), "matchup rows", len(cross_df))
print("TOP PASS OVER LEANS:")
print(best_pass_over[["team", "opp", "pass_off", "opp_pass_def", "pass_edge"]].head(8).to_string(index=False))
print("TOP RUSH OVER LEANS:")
print(best_rush_over[["team", "opp", "rush_off", "opp_rush_def", "rush_edge"]].head(8).to_string(index=False))
