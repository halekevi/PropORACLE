"""Chen / Burton WNBA check + NBA dual-window RS print for 2026-10-21."""
from __future__ import annotations

import math
import sqlite3
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.cushion_z import l5_clears, play_side_gap
from utils.l5_ranked_by_cell import five_games, gap_z
from utils.nba_early_l5 import early_season_l5_ok, l5_blend_pre_rs, l5_rs_only
from utils.nba_minutes_role import load_nba_box_frame
from utils.ticket_70_pool import skip_combo_player


def _db() -> Path:
    for p in (
        ROOT / "data" / "cache" / "proporacle_ref.db",
        ROOT / "Sports" / "WNBA" / "data" / "cache" / "proporacle_ref.db",
    ):
        if p.exists():
            return p
    return ROOT / "data" / "cache" / "proporacle_ref.db"


def _norm(s: object) -> str:
    return " ".join(str(s or "").lower().split())


def classify_wnba(d) -> str:
    """Rough WNBA window from calendar date."""
    if hasattr(d, "month"):
        m, day = d.month, d.day
    else:
        return "unknown"
    # WNBA RS ~ May–Sep; playoffs Sep–Oct; preseason April–early May
    if m == 4 or (m == 5 and day < 15):
        return "preseason"
    if m >= 9 or (m == 10):
        # late Sep+ often playoffs; Oct finals
        return "playoffs"
    return "regular"


def load_wnba_boxes() -> pd.DataFrame:
    path = _db()
    con = sqlite3.connect(str(path))
    try:
        # try common table names
        tables = [r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        table = None
        for cand in ("wnba", "WNBA", "wnba_box", "box_wnba"):
            if cand in tables:
                table = cand
                break
        if table is None:
            # fuzzy
            for t in tables:
                if "wnba" in t.lower() and "box" in t.lower():
                    table = t
                    break
        if table is None:
            for t in tables:
                if t.lower() == "wnba":
                    table = t
                    break
        print(f"DB={path} table={table} tables_sample={tables[:20]}")
        if not table:
            return pd.DataFrame()
        cols = [r[1] for r in con.execute(f"PRAGMA table_info({table})").fetchall()]
        print("cols", cols[:40])
        df = pd.read_sql_query(f"SELECT * FROM {table}", con)
    finally:
        con.close()
    if df.empty:
        return df
    # normalize
    rename = {}
    for c in df.columns:
        cl = c.lower()
        if cl in {"player", "athlete", "athlete_name", "player_name"} and "player" not in rename.values():
            rename[c] = "player"
        if cl in {"game_date", "date", "gamedate"}:
            rename[c] = "game_date"
        if cl in {"minutes", "min", "mins"}:
            rename[c] = "minutes"
        if cl in {"pts", "points"}:
            rename[c] = "pts"
        if cl in {"reb", "rebounds"}:
            rename[c] = "reb"
        if cl in {"ast", "assists"}:
            rename[c] = "ast"
        if cl in {"team", "team_abbr", "abbr"}:
            rename[c] = "team"
    df = df.rename(columns=rename)
    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
        df["window"] = df["game_date"].dt.date.map(
            lambda d: classify_wnba(d) if d is not None else "unknown"
        )
    if "player" in df.columns:
        df["_pn"] = df["player"].map(_norm)
    if "pts" in df.columns and "ast" in df.columns:
        df["pa"] = pd.to_numeric(df["pts"], errors="coerce") + pd.to_numeric(
            df["ast"], errors="coerce"
        )
    for c in ("minutes", "pts", "reb", "ast", "pa"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def print_wnba_leg(boxes: pd.DataFrame, step8: pd.DataFrame, player: str, prop: str, side: str, line: float):
    print(f"\n=== WNBA {player} | {prop} {side} {line} (GSV @ ATL 2026-10-21) ===")
    # step8 row
    sub = step8[
        (step8["Player"].astype(str) == player)
        & (step8["Prop"].astype(str) == prop)
        & (step8["Pick Type"].astype(str).str.contains("Standard", case=False, na=False))
    ]
    if sub.empty:
        print("  not on step8 Standard board")
        return
    r = sub.iloc[0]
    d = r.to_dict()
    vals = five_games(d)
    print(f"  step8 Direction={r.get('Direction')} Tier={r.get('Tier')} Team={r.get('Team')} Opp={r.get('Opp')}")
    print(f"  step8 Last5Avg={r.get('Last 5 Avg')} SeasonAvg={r.get('Season Avg')} MinTier={r.get('Min Tier')}")
    print(f"  step8 ShotRole={r.get('Shot Role')} UsageRole={r.get('Usage Role')} StarTier={r.get('Star Tier')}")
    print(f"  step8 TeamStarOut={r.get('Team Star Out')} L5Over={r.get('L5 Over')} L5Under={r.get('L5 Under')}")
    print(f"  step8 card values (stat_g): {vals} clears={bool(vals and l5_clears(vals, line, side))}")

    # combo rule
    fake = {
        "player": player,
        "prop": prop,
        "prop_type": prop,
        "is_combo_player": r.get("is_combo_player"),
        "player_1": r.get("player_1"),
        "player_2": r.get("player_2"),
    }
    # also check name for +
    print(f"  skip_combo_player={skip_combo_player(fake)} (multi-player only; Pts+Asts is a single-player market)")

    if boxes.empty or "_pn" not in boxes.columns:
        print("  no WNBA box frame for dated L5")
        return

    pn = _norm(player)
    g = boxes[boxes["_pn"] == pn].sort_values("game_date", ascending=False)
    if g.empty:
        # try last name
        g = boxes[boxes["_pn"].str.contains(player.split()[-1].lower(), na=False)].sort_values(
            "game_date", ascending=False
        )
    col = "pts" if prop.lower() == "points" else "pa" if "ast" in prop.lower() else None
    if col is None or g.empty:
        print(f"  box lookup empty or bad prop col={col} n={len(g)}")
        return

    print("  recent boxes (date window team min prop):")
    for _, row in g.head(12).iterrows():
        print(
            f"    {str(row['game_date'])[:10]} {row.get('window','?'):9} "
            f"team={row.get('team','?'):4} min={row.get('minutes'):5.1f} "
            f"{col}={row.get(col)}"
        )

    # L5 dated with minutes
    l5 = []
    for _, row in g.iterrows():
        try:
            mins = float(row.get("minutes") or 0)
            val = float(row.get(col))
        except (TypeError, ValueError):
            continue
        if math.isnan(val) or mins <= 0:
            continue
        l5.append(row)
        if len(l5) >= 5:
            break
    if len(l5) < 5:
        print(f"  incomplete L5 n={len(l5)}")
        return

    vals5 = [float(x[col]) for x in l5]
    mins5 = [float(x["minutes"]) for x in l5]
    dates = [str(x["game_date"])[:10] for x in l5]
    wins = [str(x.get("window")) for x in l5]
    teams = [str(x.get("team")) for x in l5]
    avg5 = sum(vals5) / 5
    min_l5 = sum(mins5) / 5

    # season avg: 2026 RS only if possible
    rs = g[g["window"] == "regular"]
    if rs.empty:
        rs = g[g["window"] != "preseason"]
    season_avg = float(rs[col].mean()) if len(rs) else None
    season_min = float(rs["minutes"].mean()) if len(rs) and "minutes" in rs.columns else None

    # starter heuristic: season mpg
    starter = "starter" if (season_min or 0) >= 24 else ("rotation" if (season_min or 0) >= 16 else "bench")

    # roster change: team set in L5 vs current
    uniq_teams = sorted({t for t in teams if t and t != "nan"})
    roster_note = "same_team_in_L5" if len(uniq_teams) <= 1 else f"team_change_in_L5={uniq_teams}"

    print("  CARD L5 (newest first):")
    for i in range(5):
        print(
            f"    {dates[i]} {wins[i]:9} min={mins5[i]:5.1f} {col}={vals5[i]:g}  "
            f"{'CLEAR' if (vals5[i] > line if side=='OVER' else vals5[i] < line) else 'MISS'}"
        )
    print(
        f"  L5avg={avg5:.2f} vs line {line} gap={play_side_gap(avg5, line, side):.2f} "
        f"L5=5={l5_clears(vals5, line, side)}"
    )
    print(
        f"  Min L5avg={min_l5:.1f}  SeasonMin={_fmt(season_min)}  role≈{starter}"
    )
    print(
        f"  SeasonAvg({col})={_fmt(season_avg)} vs line {line}  "
        f"cover={_fmt((season_avg - line) if season_avg is not None and side=='OVER' else (line - season_avg) if season_avg is not None else None)}"
    )
    print(f"  L5 windows={wins}  {roster_note}")


def _fmt(x):
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    return f"{float(x):.2f}"


def print_nba_dual():
    boxes = load_nba_box_frame()
    print("\n=== NBA survivors: blend vs RS-only (dated) ===")
    legs = [
        ("Darius Garland", "Points", "UNDER", 23.5),
        ("Franz Wagner", "Pts+Rebs+Asts", "UNDER", 32.5),
        ("Isaiah Jackson", "Rebounds", "UNDER", 7.5),
    ]
    for player, prop, side, line in legs:
        early = early_season_l5_ok(boxes, player, prop, line, side)
        blend = l5_blend_pre_rs(boxes, player, prop)
        rs = l5_rs_only(boxes, player, prop)
        print(f"\n  {player} | {prop} {side} {line}  dual_ok={early is not None}")
        if blend:
            b = ", ".join(f"{g['val']:g}@{g['date']}({g['tag']})" for g in blend)
            print(f"    BLEND: {b}  avg={sum(g['val'] for g in blend)/5:.2f}")
        if rs:
            r = ", ".join(f"{g['val']:g}@{g['date']}" for g in rs)
            print(f"    RS   : {r}  avg={sum(g['val'] for g in rs)/5:.2f}")
            print(f"    RS L5=5={l5_clears([g['val'] for g in rs], line, side)}")


def main():
    step8 = pd.read_excel(
        Path(r"H:\PropORACLE_main_cp\outputs\2026-10-21\wnba\step8_wnba_direction_clean.xlsx"),
        sheet_name="ALL",
    )
    boxes = load_wnba_boxes()
    print_wnba_leg(boxes, step8, "Kaitlyn Chen", "Points", "OVER", 4.5)
    print_wnba_leg(boxes, step8, "Veronica Burton", "Pts+Asts", "UNDER", 13.5)
    print("\n=== Combos-off rule ===")
    print(
        "skip_combo_player blocks multi-PLAYER props (Name A + Name B), "
        "not single-player markets like Pts+Asts / PRA / Pts+Rebs."
    )
    print(
        f"  Chen Points: skip={skip_combo_player({'player':'Kaitlyn Chen','prop':'Points'})}"
    )
    print(
        f"  Burton Pts+Asts: skip={skip_combo_player({'player':'Veronica Burton','prop':'Pts+Asts'})}"
    )
    print(
        f"  Reese+Williams combo: skip={skip_combo_player({'player':'Angel Reese + Gabby Williams','prop':'Points (Combo)','is_combo_player':True})}"
    )
    print_nba_dual()


if __name__ == "__main__":
    main()
