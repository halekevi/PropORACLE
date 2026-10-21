"""Rank all NFL Sunday props by |L5 avg - line| (Projection as season-cut avg)."""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

STEP8 = Path(
    r"H:/PropORACLE_main_cp/outputs/2026-09-20/nfl/step8_nfl_direction_clean.xlsx"
)
OUT = Path(r"H:/PropORACLE/_tmp_nfl_gap_rank_920.json")

df = pd.read_excel(STEP8)
df = df[~df["Player"].astype(str).str.contains(r"\+", regex=True)].copy()
df = df[
    ~df["Prop"]
    .astype(str)
    .str.contains("combo|per_carry|longest", case=False, na=False)
].copy()
gt = pd.to_datetime(df["Game Time"], utc=True, errors="coerce")
df["game_et"] = gt.dt.tz_convert("America/New_York")
df = df[df["game_et"].dt.date.astype(str) == "2026-09-20"].copy()
df = df[df["Pick Type"].isin(["Standard", "Goblin"])].copy()
df["line"] = pd.to_numeric(df["Line"], errors="coerce")
df["l5_avg"] = pd.to_numeric(df["Projection"], errors="coerce")
df["gap"] = df["l5_avg"] - df["line"]
df["gap_abs"] = df["gap"].abs()
df = df.dropna(subset=["line", "l5_avg"])
df = df.sort_values("gap_abs", ascending=False)
df = df.drop_duplicates(subset=["Player", "Prop", "Pick Type"], keep="first")
df["prop_lbl"] = (
    df["Prop"].astype(str).str.replace("player_", "", regex=False).str.replace("_", " ")
)
df["kick"] = df["game_et"].dt.strftime("%H:%M")
df["matchup"] = df["Team"].astype(str) + " vs " + df["Opp"].astype(str)


def rows(frame: pd.DataFrame) -> list[dict]:
    out = []
    for i, (_, r) in enumerate(frame.iterrows(), 1):
        out.append(
            {
                "rank": i,
                "player": str(r.Player),
                "matchup": str(r.matchup),
                "kick": str(r.kick),
                "prop": str(r.prop_lbl),
                "pick": str(r["Pick Type"]),
                "dir": str(r.Direction),
                "line": float(r.line),
                "l5_avg": round(float(r.l5_avg), 1),
                "gap": round(float(r.gap), 1),
                "gap_abs": round(float(r.gap_abs), 1),
                "games": float(r["Games Used"]) if pd.notna(r["Games Used"]) else None,
                "flag": str(r["Season L5 Flag"])
                if pd.notna(r["Season L5 Flag"])
                else "",
                "def": str(r["Def Tier"]) if pd.notna(r["Def Tier"]) else "",
                "l5o": float(r["L5 Over"]) if pd.notna(r["L5 Over"]) else None,
            }
        )
    return out


top = df.head(50)
over = df[df["gap"] > 0].head(40)
under = df[df["gap"] < 0].sort_values("gap").head(20)

by_matchup = []
for m, g in df.groupby("matchup"):
    r = g.iloc[0]
    by_matchup.append(
        {
            "matchup": m,
            "kick": str(r.kick),
            "player": str(r.Player),
            "prop": str(r.prop_lbl),
            "pick": str(r["Pick Type"]),
            "line": float(r.line),
            "l5_avg": round(float(r.l5_avg), 1),
            "gap": round(float(r.gap), 1),
        }
    )
by_matchup = sorted(by_matchup, key=lambda x: -abs(x["gap"]))

payload = {
    "slate": "2026-09-20",
    "n_faces_deduped": int(len(df)),
    "n_matchups": int(df["matchup"].nunique()),
    "matchups": sorted(df["matchup"].unique().tolist()),
    "n1_share": round(float((df["Games Used"] == 1).mean()), 3),
    "top_abs": rows(top),
    "top_over": rows(over),
    "top_under": rows(under),
    "by_matchup_top": by_matchup,
}
OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
print("deduped", len(df), "matchups", df["matchup"].nunique(), "n1%", payload["n1_share"])
print("TOP ABS:")
for r in payload["top_abs"][:30]:
    print(
        f"{r['rank']:2} {r['gap']:+7.1f}  {r['player']:<22} {r['pick']:<8} "
        f"{r['prop']:<18} L{r['line']:<6} avg={r['l5_avg']:<6} {r['matchup']}  "
        f"n={r['games']} {r['flag']}"
    )
print("\nPER MATCHUP BEST:")
for r in by_matchup:
    print(
        f"  {r['gap']:+7.1f}  {r['matchup']:<14} {r['player']:<20} "
        f"{r['pick']} {r['prop']} L{r['line']} avg={r['l5_avg']}"
    )
