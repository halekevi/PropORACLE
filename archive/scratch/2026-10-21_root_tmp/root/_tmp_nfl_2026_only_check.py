"""2026-only NFL box check for top L5% candidates (Jones-style)."""
from __future__ import annotations

from datetime import date

import pandas as pd

CACHE = r"H:\PropORACLE\Sports\NFL\data\cache\nfl_boxscore_cache.csv"
STEP5 = r"H:\PropORACLE_main_cp\Sports\NFL\data\outputs\step5_nfl_with_stats.csv"
SEASON_START = date(2026, 9, 1)  # NFL regular season

# name -> (prop keys in cache / step5, line, side)
CHECKS = [
    ("Tyler Shough", "pass", 224.5, "OVER"),
    ("Woody Marks", "rush", 14.5, "OVER"),
    ("Mack Hollins", "rec", 19.5, "OVER"),
    ("Devaughn Vele", "rec", 36.5, "OVER"),
    ("Jacory Croskey-Merritt", "rush", 24.5, "OVER"),
    ("Greg Dulcich", "rec", 7.5, "OVER"),
    ("Dallas Goedert", "rec", None, "OVER"),  # fantasy harder; show rec/yards too
]


def parse_d(s) -> date | None:
    ts = pd.to_datetime(s, errors="coerce")
    if pd.isna(ts):
        return None
    return ts.date()


def main() -> None:
    cache = pd.read_csv(CACHE, low_memory=False)
    print("cache cols sample:", [c for c in cache.columns if any(x in c.lower() for x in ("player", "name", "date", "pass", "rush", "rec", "yard"))][:40])
    print("rows", len(cache))
    # name column
    nc = "player_norm" if "player_norm" in cache.columns else next(
        c for c in cache.columns if "player" in c.lower() or c.lower() == "name"
    )
    dc = "game_date" if "game_date" in cache.columns else next(
        c for c in cache.columns if "date" in c.lower()
    )
    print("namecol", nc, "datecol", dc)
    cache["_d"] = cache[dc].map(parse_d)
    cur = cache[cache["_d"].notna() & (cache["_d"] >= SEASON_START)].copy()
    print("2026+ rows", len(cur), "date range", cur["_d"].min(), cur["_d"].max())

    def pick_col(kind: str) -> str | None:
        if kind == "pass":
            for c in ("PASS_YDS", "passing_yards", "pass_yards"):
                if c in cache.columns:
                    return c
        elif kind == "rush":
            for c in ("RUSH_YDS", "rushing_yards", "rush_yards"):
                if c in cache.columns:
                    return c
        else:
            for c in ("REC_YDS", "receiving_yards", "rec_yards"):
                if c in cache.columns:
                    return c
        return None

    def match_player(df: pd.DataFrame, name: str) -> pd.DataFrame:
        key = (
            name.lower()
            .replace(" jr.", "")
            .replace(" sr.", "")
            .replace(".", "")
            .strip()
        )
        # player_norm is usually collapsed lowercase
        m = df[df[nc].astype(str).str.lower().str.replace(".", "", regex=False) == key.replace(" ", "")]
        if len(m):
            return m
        m = df[df[nc].astype(str).str.lower().str.contains(key.replace(" ", ".*"), case=False, na=False, regex=True)]
        if len(m):
            return m
        parts = key.split()
        last = parts[-1]
        first = parts[0]
        m = df[df[nc].astype(str).str.lower().str.contains(last, na=False)]
        return m[m[nc].astype(str).str.lower().str.contains(first, na=False)]

    print("\n======== 2026-ONLY GAME LOGS ========")
    for name, kind, line, side in CHECKS:
        col = pick_col(kind)
        if not col:
            print(name, "NO COL")
            continue
        m = match_player(cur, name)
        rows = m.sort_values("_d")[[nc, dc, col, "_d"]].drop_duplicates(subset=["_d", col])
        vals = []
        print(f"\n--- {name}  {kind}  line={line} {side}  col={col} ---")
        if rows.empty:
            print("  NO 2026 GAMES IN CACHE")
            continue
        for _, r in rows.iterrows():
            try:
                fv = float(r[col])
            except (TypeError, ValueError):
                continue
            if pd.isna(fv):
                continue
            vals.append(fv)
            hit = ""
            if line is not None:
                if side == "OVER":
                    hit = "CLEAR" if fv > line else ("PUSH" if fv == line else "MISS")
                else:
                    hit = "CLEAR" if fv < line else ("PUSH" if fv == line else "MISS")
            print(f"  {r['_d']}  {r[nc]:28}  {fv:6.1f}  {hit}")
        if line is not None and vals:
            overs = sum(1 for v in vals if v > line)
            unders = sum(1 for v in vals if v < line)
            pushes = sum(1 for v in vals if v == line)
            n = len(vals)
            if overs and not unders:
                flag = "THIN_CLEAR_OVER"
            elif unders and not overs:
                flag = "THIN_CLEAR_UNDER"
            else:
                flag = "THIN_MIXED"
            print(f"  n={n} vs {line}: O{overs}/U{unders}/P{pushes}  => {flag}")
            print(f"  board claimed L5=5/0 padded — real 2026 sample is {n}")

    print("\n======== STEP5 CLAIMED L5 (padded) ========")
    s5 = pd.read_csv(STEP5, low_memory=False)
    for name, prop in [
        ("Tyler Shough", "passing_yards"),
        ("Woody Marks", "rushing_yards"),
        ("Mack Hollins", "receiving_yards"),
        ("Devaughn Vele", "receiving_yards"),
        ("Jacory Croskey-Merritt", "rushing_yards"),
        ("Greg Dulcich", "receiving_yards"),
    ]:
        m = s5[
            (s5["player_name"].astype(str) == name)
            & (s5["prop_type"] == prop)
            & (s5["pick_type"].astype(str).str.lower() == "goblin")
        ]
        if m.empty:
            print(name, prop, "missing")
            continue
        r = m.sort_values("line").iloc[-1]
        games = [r.get(f"stat_g{i}") for i in range(1, 6)]
        print(
            f"{name:26} {prop:18} Goblin {r['line']}  "
            f"L5={r.get('line_hits_over_5')}/{r.get('line_hits_under_5')}  "
            f"games_used={r.get('games_used')}  window={games}"
        )


if __name__ == "__main__":
    main()
