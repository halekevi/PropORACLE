"""Probe MLB name join on Sep 27 vs Oct 8 boards."""
from __future__ import annotations

import csv
import sqlite3
import sys
import unicodedata
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]
from build_goblin70_tickets import load_today_board  # noqa: E402

REF = ROOT / "data" / "cache" / "proporacle_ref.db"
CACHE = ROOT / "Sports" / "MLB" / "mlb_id_cache.csv"


def fold(name: object) -> str:
    text = unicodedata.normalize("NFKD", str(name or ""))
    return "".join(ch for ch in text.lower() if ch.isalnum())


def load_ids() -> dict[str, str]:
    ids: dict[str, str] = {}
    with CACHE.open(encoding="utf-8-sig", newline="") as fh:
        for rec in csv.DictReader(fh):
            ids[str(rec.get("mlb_player_id") or "")] = fold(rec.get("player_norm"))
    return ids


def board_rows(day: str) -> list[dict]:
    roots = [ROOT, ROOT.parent / "PropORACLE_main_cp"]
    path = None
    for root in roots:
        cand = root / "outputs" / day / "mlb" / "step8_mlb_direction.csv"
        if cand.is_file():
            path = cand
            break
    if path is None:
        return []
    with path.open(encoding="utf-8-sig", newline="") as fh:
        return list(csv.DictReader(fh))


def probe(day: str, ids: dict[str, str], played: set[str]) -> None:
    mlb = board_rows(day)
    print(" board file rows", len(mlb))
    keys = list(mlb[0].keys())[:25] if mlb else []
    print(f"\n=== {day} mlb rows {len(mlb)} keys {keys}")
    if mlb:
        r0 = mlb[0]
        print(" sample player", repr(r0.get("player")), "team", r0.get("team"), "opp", r0.get("opp_team") or r0.get("opponent"))
        print(" start", r0.get("start_time"), "slate", r0.get("slate_date"), "game_date", r0.get("game_date"))
    names = [fold(r.get("player")) for r in mlb]
    uniq = set(names)
    hit_cache = sum(1 for n in uniq if n in set(ids.values()))
    hit_played = sum(1 for n in uniq if n and n in played)
    row_hit = sum(1 for n in names if n in played)
    print(f" unique names {len(uniq)} cache-name hit {hit_cache} played hit {hit_played} row hit {row_hit}/{len(names)}")
    # show a few misses
    misses = [n for n in list(uniq)[:8]]
    print(" sample folds", misses[:8])
    cache_sample = list(ids.values())[:5]
    print(" cache sample", cache_sample)


def main() -> None:
    ids = load_ids()
    print("cache ids", len(ids), "unique names", len(set(ids.values())))
    conn = sqlite3.connect(REF)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(mlb_gamelog)")]
    print("gamelog cols", cols)
    n = conn.execute("SELECT COUNT(*), MIN(game_date), MAX(game_date) FROM mlb_gamelog").fetchone()
    print("gamelog span", n)
    # season type column?
    played: set[str] = set()
    for pid, game_date in conn.execute(
        "SELECT DISTINCT mlb_player_id, game_date FROM mlb_gamelog WHERE game_date >= '2026-03-01'"
    ):
        name = ids.get(str(pid))
        if name:
            played.add(name)
    print("played names with any 2026 game", len(played))
    # id overlap
    log_ids = {str(r[0]) for r in conn.execute("SELECT DISTINCT mlb_player_id FROM mlb_gamelog")}
    cache_ids = set(ids)
    print("log ids", len(log_ids), "cache overlap", len(log_ids & cache_ids), "log-only", len(log_ids - cache_ids))
    conn.close()
    for day in ("2026-09-27", "2026-10-08"):
        probe(day, ids, played)


if __name__ == "__main__":
    main()
