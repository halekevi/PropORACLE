"""Chronological preseason + last-season (and WNBA playoff) stats for slip legs."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.nba_minutes_role import _norm_name, classify_game, load_nba_box_frame

NBA_LEGS = [
    ("Darius Garland", "Points", "pts", 23.5, "UNDER"),
    ("Isaiah Jackson", "Rebounds", "reb", 7.5, "UNDER"),
    ("Franz Wagner", "Pts+Rebs+Asts", "pra", 32.5, "UNDER"),
    ("Jonathan Kuminga", "Turnovers", "tov", 1.5, "UNDER"),
    ("Dejounte Murray", "Pts+Rebs+Asts", "pra", 30.0, "UNDER"),
]

WNBA_LEGS = [
    ("Kaitlyn Chen", "Points", "pts", 4.5, "OVER"),
    ("Veronica Burton", "Pts+Asts", "pa", 13.5, "UNDER"),
]


def _mark(val: float, line: float, side: str) -> str:
    if side == "OVER":
        return "O" if val > line else ("P" if val == line else "U")
    return "U" if val < line else ("P" if val == line else "O")


def nba_chrono(boxes: pd.DataFrame, player: str, col: str, line: float, side: str) -> None:
    pn = _norm_name(player)
    g = boxes[boxes["_pn"] == pn].copy()
    if g.empty:
        print(f"\n=== {player} | {col} — no boxes ===")
        return
    # Last RS season year from latest regular game, plus this preseason
    g = g.sort_values("game_date")
    # Keep: 2026 preseason + 2025-26 regular (game_date from ~2025-10-22 through 2026-04)
    # Simpler: all preseason in 2026-10 + all regular from 2025-10-22 onward (skip prior preseason/playoffs for clarity)
    rows = []
    for _, r in g.iterrows():
        d = r["game_date"]
        if pd.isna(d):
            continue
        dd = d.date() if hasattr(d, "date") else d
        win = classify_game(dd)
        # Include: 2026 preseason; 2025-26 RS (Oct 22 2025 – Apr 14 2026-ish); skip older
        if win == "preseason" and dd.year == 2026:
            keep = True
        elif win == "regular" and (
            (dd.year == 2025 and (dd.month > 10 or (dd.month == 10 and dd.day >= 22)))
            or (dd.year == 2026 and dd.month < 5)
        ):
            keep = True
        else:
            keep = False
        if not keep:
            continue
        try:
            mins = float(r.get("minutes") or 0)
            val = float(r.get(col))
        except (TypeError, ValueError):
            continue
        if mins <= 0:
            continue
        rows.append((dd.isoformat(), win, mins, val))

    print(f"\n=== {player} | {col} vs {side} {line} (chrono preseason + last RS) ===")
    if not rows:
        print("  (none)")
        return
    for d, win, mins, val in rows:
        tag = _mark(val, line, side)
        print(f"  {d}  {win[:3]:3}  min={mins:5.1f}  {col}={val:5.1f}  [{tag}]")
    # L5 newest-first summary
    newest = list(reversed(rows))[:5]
    vals = [v for *_, v in newest]
    print(
        f"  — newest5: {', '.join(f'{v:g}@{d[5:]}({w[:1].upper()})' for d,w,_,v in newest)}  "
        f"avg={sum(vals)/len(vals):.1f}"
    )


def wnba_chrono(player: str, col: str, line: float, side: str) -> None:
    con = sqlite3.connect(str(ROOT / "data" / "cache" / "proporacle_ref.db"))
    df = pd.read_sql_query(
        "SELECT game_date, team, minutes, pts, ast, reb, pa, pra, season "
        "FROM wnba WHERE lower(player)=? ORDER BY game_date ASC",
        con,
        params=(player.lower(),),
    )
    con.close()
    if df.empty:
        print(f"\n=== {player} — no boxes ===")
        return
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df["minutes"] = pd.to_numeric(df["minutes"], errors="coerce")
    for c in ("pts", "ast", "reb", "pa", "pra"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    if col == "pa" and (df["pa"].isna().all() or df["pa"].isna().any()):
        df["pa"] = df["pts"].fillna(0) + df["ast"].fillna(0)

    print(f"\n=== {player} | {col} vs {side} {line} (chrono — late RS + playoffs) ===")
    sub = df[df["game_date"] >= "2026-05-01"].copy()
    rows = []
    for _, r in sub.iterrows():
        d = r["game_date"]
        if pd.isna(d):
            continue
        dd = d.date()
        win = "playoffs" if dd.month >= 9 else "regular"
        try:
            mins = float(r["minutes"] or 0)
            val = float(r[col])
        except (TypeError, ValueError):
            continue
        if mins <= 0:
            continue
        rows.append((dd.isoformat(), win, mins, val))
    rs = [x for x in rows if x[1] == "regular"]
    po = [x for x in rows if x[1] == "playoffs"]
    # last 10 RS then all playoffs (chrono)
    show = rs[-10:] + po
    print(f"  (showing last {len(rs[-10:])} RS + {len(po)} playoff games)")
    for d, win, mins, val in show:
        tag = _mark(val, line, side)
        print(f"  {d}  {win[:3]:3}  min={mins:5.1f}  {col}={val:5.1f}  [{tag}]")


def main() -> None:
    boxes = load_nba_box_frame()
    print("NBA slip + extras — chronological (oldest → newest)")
    print("[U]=under line  [O]=over line  [P]=push")
    for player, _prop, col, line, side in NBA_LEGS:
        nba_chrono(boxes, player, col, line, side)

    print("\n\nWNBA slip — chronological 2026 RS then playoffs")
    for player, _prop, col, line, side in WNBA_LEGS:
        wnba_chrono(player, col, line, side)


if __name__ == "__main__":
    main()
