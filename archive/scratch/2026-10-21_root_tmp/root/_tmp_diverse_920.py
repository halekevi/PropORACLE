"""Diverse picks for 2026-09-20 — clear gates, unique players, no NFL (unusable)."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, r"H:\PropORACLE")
from scripts.rank_best_props_today import (  # noqa: E402
    bucket,
    load_nfl,
    load_sport,
    recs,
)

ROOT = Path(r"H:\PropORACLE_main_cp")
TOM = "2026-09-20"
TODAY = "2026-09-19"


def fmt(r: dict) -> str:
    badge = r.get("promo") or r.get("badge") or "—"
    side = str(r.get("side") or "").upper()
    letter = "O" if side == "OVER" else "U"
    return (
        f"  {r.get('prop_tier','?')}/{str(badge):10} "
        f"{str(r.get('player','')):26} {str(r.get('prop','')):22} "
        f"{letter}{r.get('line')}  L5 {r.get('l5_over')}/{r.get('l5_under')}  "
        f"L10 {r.get('l10_over')}/{r.get('l10_under')}  "
        f"cov {r.get('cover')}  D={r.get('def')}  {r.get('matchup')}"
        f"{('  miss '+r['miss_s']) if r.get('miss_s') else ''}"
    )


def dedupe_player(rows: list[dict], n: int = 15) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for r in rows:
        pk = str(r.get("player") or "").strip().lower()
        if not pk or pk in seen:
            continue
        seen.add(pk)
        out.append(r)
        if len(out) >= n:
            break
    return out


def main() -> None:
    print(f"Diverse picks slate {TOM}")
    print("NFL: board exists but L5 pads 2025 — DO NOT PLAY\n")

    # Tennis from today's board, filter start to tomorrow
    df_t = load_sport(ROOT, TODAY, "TENNIS", "tennis", "step8_tennis_direction.csv")
    if "start_time" in df_t.columns:
        st = pd.to_datetime(df_t["start_time"], errors="coerce", utc=True)
        local = st.dt.tz_convert("America/New_York")
        df_t = df_t.loc[local.dt.strftime("%Y-%m-%d") == TOM].copy()
    print(f"TENNIS rows on {TOM}: {len(df_t)}")
    so, su, gob = bucket(recs(df_t), "TENNIS") if len(df_t) else ([], [], [])
    print(f"\n===== TENNIS Standard OVER (n={len(so)}) =====")
    for r in dedupe_player(so, 10):
        print(fmt(r))
    if not so:
        print("  (none clear keep gates)")
    print(f"\n===== TENNIS Standard UNDER (n={len(su)}) =====")
    for r in dedupe_player(su, 10):
        print(fmt(r))
    if not su:
        print("  (none — Aces/DF Standard off; others gated out)")
    print(f"\n===== TENNIS Goblin OVER unique players (n_gated={len(gob)}) =====")
    for r in dedupe_player(gob, 20):
        print(fmt(r))
    if not gob:
        print("  (none that clear L5 4+ / tennis keep)")

    # Other sports: anything starting tomorrow on today's boards?
    for sport, folder, fname in [
        ("SOCCER", "soccer", "step8_soccer_direction.csv"),
        ("MLB", "mlb", "step8_mlb_direction.csv"),
        ("WNBA", "wnba", "step8_wnba_direction.csv"),
    ]:
        df = load_sport(ROOT, TODAY, sport, folder, fname)
        if df.empty:
            print(f"\n===== {sport} =====\n  (no step8)")
            continue
        sc = None
        for c in ("start_time", "Game Time", "game_time"):
            if c in df.columns:
                sc = c
                break
        n_tom = 0
        if sc:
            raw = df[sc].astype(str)
            # tennis-style 09/20 or ISO
            mask = raw.str.contains(r"09/20|2026-09-20", regex=True, na=False)
            if not mask.any() and sc == "start_time":
                st = pd.to_datetime(df[sc], errors="coerce", utc=True)
                try:
                    local = st.dt.tz_convert("America/New_York")
                    mask = local.dt.strftime("%Y-%m-%d") == TOM
                except Exception:
                    mask = pd.Series([False] * len(df))
            df = df.loc[mask].copy()
            n_tom = len(df)
        print(f"\n===== {sport} starting {TOM}: {n_tom} rows =====")
        if n_tom == 0:
            print("  (no tomorrow starts on board)")
            continue
        so, su, gob = bucket(recs(df), sport)
        print(f"  Std O={len(so)} U={len(su)} Goblin={len(gob)}")
        for label, lst in (("OVER", so), ("UNDER", su), ("GOBLIN", gob)):
            uniq = dedupe_player(lst, 8)
            if not uniq:
                continue
            print(f"  -- {label} --")
            for r in uniq:
                print(fmt(r))

    # NFL note only
    df_n = load_nfl(ROOT, TOM)
    so, su, gob = bucket(recs(df_n), "NFL") if len(df_n) else ([], [], [])
    print(
        f"\n***** NFL gated counts O={len(so)} U={len(su)} G={len(gob)} "
        f"but PRIOR-SEASON L5 — unusable *****"
    )


if __name__ == "__main__":
    main()
