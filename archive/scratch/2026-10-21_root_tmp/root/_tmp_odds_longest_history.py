"""Odds API status + longest_rush UNDER unique-game + no-history prop probe."""
from __future__ import annotations

import os
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd

from utils.nfl_prop_gate_ledger import (
    dedupe_unique_player_games,
    prepare_decided,
    rate,
)

REPO = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
DATA = REPO / "Sports" / "NFL" / "data"

DATES = [
    "2026-09-09",
    "2026-09-10",
    "2026-09-13",
    "2026-09-14",
    "2026-09-20",
    "2026-09-21",
]


def _load_odds_key() -> str:
    key = (os.environ.get("ODDS_API_KEY") or "").strip()
    if key:
        return key
    for p in (REPO / ".env", MAIN / ".env", Path.home() / ".proporacle.env"):
        if not p.is_file():
            continue
        for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
            if line.strip().startswith("ODDS_API_KEY="):
                return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def odds_status() -> None:
    print("=== Odds API (step6b) ===")
    key = _load_odds_key()
    if not key:
        print("ODDS_API_KEY: MISSING (step6b will use ESPN totals fallback)")
        return
    print(f"ODDS_API_KEY: present (len={len(key)})")
    url = (
        "https://api.the-odds-api.com/v4/sports/americanfootball_nfl/odds"
        f"?apiKey={key}&regions=us&markets=spreads,totals&oddsFormat=american"
    )
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "PropORACLE/odds-check"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            rem = resp.headers.get("x-requests-remaining")
            used = resp.headers.get("x-requests-used")
            body = resp.read(200)
            print(f"HTTP {resp.status} remaining={rem} used={used} body_bytes~{len(body)}")
    except urllib.error.HTTPError as exc:
        rem = exc.headers.get("x-requests-remaining") if exc.headers else None
        body = exc.read().decode("utf-8", errors="ignore")[:200]
        print(f"HTTP {exc.code} remaining={rem} body={body}")
    except Exception as exc:
        print(f"Odds probe failed: {exc}")


def load_box(d: str) -> pd.DataFrame | None:
    for root in (MAIN, REPO):
        p = root / "outputs" / d / f"graded_nfl_{d}.xlsx"
        if not p.is_file():
            continue
        df = pd.read_excel(p, sheet_name="Box Raw")
        if df.empty or "result" not in df.columns:
            return None
        if int(df["result"].astype(str).str.upper().isin(["HIT", "MISS", "PUSH"]).sum()) == 0:
            return None
        out = df.copy()
        out["slate"] = d
        return out
    return None


def cell(h: int, n: int) -> str:
    if n <= 0:
        return "--"
    return f"{h}/{n} = {rate(h, n):.0f}%"


def gate_table(uniq: pd.DataFrame, prop: str, side: str, pick: str) -> None:
    g = uniq[(uniq.prop == prop) & (uniq.side == side) & (uniq.pick == pick)]
    if g.empty:
        print(f"  {prop} {pick} {side}: no rows")
        return
    g4 = g[g.l5 >= 4]
    g5 = g[g.l5 == 5]
    g70 = g[g.gate70]
    print(
        f"  {prop:28s} {pick:8s} {side:5s}  "
        f"base {cell(int((g.result=='HIT').sum()), len(g)):16s}  "
        f"L5>=4 {cell(int((g4.result=='HIT').sum()), len(g4)):16s}  "
        f"L5=5 {cell(int((g5.result=='HIT').sum()), len(g5)):16s}  "
        f"G70 {cell(int((g70.result=='HIT').sum()), len(g70))}"
    )


def longest_rush_and_watch(uniq: pd.DataFrame) -> None:
    print("\n=== Unlock / watch unique-game rates ===")
    for prop, side, pick in [
        ("longest_rush", "UNDER", "Standard"),
        ("longest_rush", "OVER", "Standard"),
        ("longest_rush", "OVER", "Goblin"),
        ("yards_on_first_rush_attempt", "OVER", "Standard"),
        ("yards_on_first_reception", "OVER", "Standard"),
        ("int", "UNDER", "Standard"),
        ("int", "OVER", "Standard"),
        ("pass_rush_yds", "UNDER", "Standard"),
        ("sacks_taken", "OVER", "Standard"),
        ("rush_tds", "OVER", "Standard"),
        ("rec_tds", "OVER", "Standard"),
        ("punts_inside_20", "OVER", "Standard"),
    ]:
        gate_table(uniq, prop, side, pick)


def ledger_props(uniq: pd.DataFrame) -> None:
    print("\n=== Props present in W1+W2 unique-game ledger (any book) ===")
    props = sorted(uniq.prop.unique())
    want = [
        "int",
        "interceptions",
        "interceptions_thrown",
        "pass_rush_yds",
        "yards_on_first_reception",
        "yards_on_first_rush_attempt",
        "rush_tds",
        "rec_tds",
        "sacks_taken",
        "punt_touchbacks_combo",
        "shortest_fg_made_yds_combo",
        "longest_fg_made_yds_combo",
        "completions_in_first_10_pass_attempts",
        "longest_rush",
        "player_touchdowns",
    ]
    for p in want:
        n = int((uniq.prop == p).sum())
        print(f"  {p:45s} unique-game n={n}")


def boxscore_probe() -> None:
    print("\n=== Boxscore cache column / season probe ===")
    cache = DATA / "nfl_boxscore_cache.csv"
    alt = list((DATA).glob("*boxscore*.csv")) + list((REPO / "Sports" / "NFL").glob("**/nfl_boxscore*.csv"))
    paths = [cache] if cache.is_file() else []
    paths += [p for p in alt if p not in paths]
    if not paths:
        print("  no boxscore cache found")
        return
    p = paths[0]
    print(f"  using {p}")
    df = pd.read_csv(p, nrows=5)
    cols = list(df.columns)
    print(f"  cols sample ({len(cols)}): {cols[:40]}")
    # full load light
    df = pd.read_csv(p, low_memory=False)
    season_col = next((c for c in df.columns if c.lower() in {"season", "year"}), None)
    # find prop-like columns
    interesting = [
        c
        for c in df.columns
        if any(
            k in c.lower()
            for k in (
                "int",
                "sack",
                "rush",
                "rec",
                "td",
                "punt",
                "fg",
                "complet",
                "first",
                "pass_rush",
                "longest",
            )
        )
    ]
    print(f"  interesting cols ({len(interesting)}): {interesting[:50]}")
    if season_col:
        print(f"  seasons: {sorted(df[season_col].dropna().unique())[:20]}")
        print(f"  rows by season:\n{df[season_col].value_counts().head(10).to_string()}")


def main() -> None:
    odds_status()
    boxes = [b for d in DATES if (b := load_box(d)) is not None]
    dec = prepare_decided(boxes)
    uniq = dedupe_unique_player_games(dec)
    print(f"\nunique-game rows={len(uniq)} slates={sorted(uniq.slate.unique())}")
    longest_rush_and_watch(uniq)
    ledger_props(uniq)
    boxscore_probe()


if __name__ == "__main__":
    main()
