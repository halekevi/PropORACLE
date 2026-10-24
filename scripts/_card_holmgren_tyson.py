"""Print Holmgren 3PTA and Tyson 2PA card windows vs tonight's lines."""
from __future__ import annotations

import sqlite3
from datetime import date
from pathlib import Path

import pandas as pd

db = Path("Sports/NBA/data/cache/proporacle_ref.db")
con = sqlite3.connect(str(db))
df = pd.read_sql_query(
    "select * from nba where player in ('Chet Holmgren','Jaylon Tyson')",
    con,
)
df["game_date"] = pd.to_datetime(df["game_date"])


def classify(ts) -> str:
    d = ts.date()
    y = d.year if d.month >= 10 else d.year - 1
    if d.month == 10 and d.day < 22:
        return "preseason"
    if date(y + 1, 4, 15) <= d <= date(y + 1, 6, 30):
        return "playoffs"
    return "regular"


df["window"] = df["game_date"].apply(classify)


def verdict(v: float, line: float, side: str) -> str:
    if v < line:
        hit = side == "UNDER"
        return "UNDER" + (" *" if hit else "")
    if v > line:
        hit = side == "OVER"
        return "OVER" + (" *" if hit else "")
    return "PUSH"


def show(player: str, prop: str, line: float, side: str) -> None:
    col = "fg3a" if prop == "3PTA" else "fg2a"
    made_col = "fg3m" if prop == "3PTA" else "fg2m"
    lab = prop
    ml = "3PM" if prop == "3PTA" else "2PM"

    g = df[df.player == player].sort_values("game_date", ascending=False)
    card = g[g.window != "preseason"].head(5)
    print("=" * 64)
    print(f"{player} | {prop} {side} {line}  (board: Standard)")
    print("Card window = 5 newest RS+playoffs (no preseason)")
    vals = []
    mix = []
    for _, r in card.iterrows():
        v = float(r[col])
        m = float(r[made_col])
        vals.append(v)
        mix.append("O" if r.window == "playoffs" else "R")
        print(
            f"  {r.game_date.date()}  {r.window:9}  min={r.minutes:4.0f}  "
            f"{lab}={int(v)}  {ml}={int(m)}  -> {verdict(v, line, side)}"
        )
    hits = sum(1 for v in vals if (v < line if side == "UNDER" else v > line))
    print(
        f"  L5 avg {lab}={sum(vals)/len(vals):.1f}  "
        f"UNDER hits={hits}/5  window_mix={''.join(mix)}"
    )

    rs = g[g.window == "regular"].head(5)
    print("Last 5 regular-season only:")
    rs_vals = []
    for _, r in rs.iterrows():
        v = float(r[col])
        m = float(r[made_col])
        rs_vals.append(v)
        print(
            f"  {r.game_date.date()}  min={r.minutes:4.0f}  "
            f"{lab}={int(v)}  {ml}={int(m)}  -> {verdict(v, line, side)}"
        )
    if rs_vals:
        rh = sum(1 for v in rs_vals if (v < line if side == "UNDER" else v > line))
        print(f"  RS L5 avg {lab}={sum(rs_vals)/len(rs_vals):.1f}  UNDER hits={rh}/5")

    pre = g[(g.window == "preseason") & (g.game_date.dt.year == 2026)]
    print(f"2026 preseason (n={len(pre)}):")
    if pre.empty:
        print("  (none in box store)")
    for _, r in pre.iterrows():
        v = float(r[col])
        m = float(r[made_col])
        print(
            f"  {r.game_date.date()}  min={r.minutes:4.0f}  "
            f"{lab}={int(v)}  {ml}={int(m)}  -> {verdict(v, line, side)}"
        )
    print()


show("Chet Holmgren", "3PTA", 2.5, "UNDER")
show("Jaylon Tyson", "2PA", 3.5, "UNDER")
