#!/usr/bin/env python3
"""
Step 2 — Normalize Props + Resolve NHL Player IDs
- Normalizes stat_type names
- Classifies players as SKATER or GOALIE
- Resolves NHL player IDs (cache-first)
- Uses suggest endpoint first; falls back to search.d3.nhle.com
- Supports combo players "A + B" -> "idA|idB"
- Saves persistent id cache to nhl_id_cache.csv

Usage:
    py .\\step2_attach_picktypes_nhl.py --input .\\outputs\\step1_nhl_props.csv --output .\\outputs\\step2_nhl_picktypes.csv --cache .\\cache\\nhl_id_cache.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
import threading
import time
import urllib.parse

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass
try:
    from tqdm import tqdm as _tqdm
except ImportError:
    import subprocess, sys
    subprocess.check_call([sys.executable, "-m", "pip", "install", "tqdm", "--break-system-packages", "-q"])
    from tqdm import tqdm as _tqdm

_PROPORACLE_ROOT = Path(__file__).resolve().parents[3]
if str(_PROPORACLE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROPORACLE_ROOT))
from utils.fantasy_prop_filter import drop_fantasy_rows

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
}

# Stat type normalization
STAT_NORM = {
    "Shots On Goal": "shots_on_goal",
    "Shots on Goal": "shots_on_goal",
    "shots_on_goal": "shots_on_goal",
    "Goals": "goals",
    "goals": "goals",
    "Assists": "assists",
    "assists": "assists",
    "Points": "points",
    "points": "points",
    "Hits": "hits",
    "hits": "hits",
    "Blocked Shots": "blocked_shots",
    "Blocked shots": "blocked_shots",
    "blocked_shots": "blocked_shots",
    "Fantasy Score": "fantasy_score",
    "fantasy_score": "fantasy_score",
    "Saves": "saves",
    "saves": "saves",
    "Goals Allowed": "goals_allowed",
    "goals_allowed": "goals_allowed",
    # PrizePicks label variants → keys expected by step5_add_line_hit_rates_nhl.SKATER_MAP
    "Plus Minus": "plus/minus",
    "Plus-Minus": "plus/minus",
    "+/-": "plus/minus",
    "Power Play Points": "power_play_points",
    "power play points": "power_play_points",
}

# Position classification
GOALIE_POSITIONS = {"G", "Goalie", "Goaltender"}
GOALIE_PROPS = {"saves", "goals_allowed"}

# Combo splitter (PrizePicks style often "A + B")
COMBO_SPLIT_RE = re.compile(r"\s*\+\s*")

_NHL_DIR = Path(__file__).resolve().parents[1]
_LEGACY_CACHE = _NHL_DIR / "nhl_id_cache.csv"
NOT_FOUND = "NOT_FOUND"
MAX_WORKERS = 8
# Suggest + d3 fallback, each capped at 3s.
LOOKUP_RESULT_TIMEOUT = 10


def fetch_json(url: str, retries: int = 1, timeout: int = 3) -> dict | list | None:
    """
    Safe fetch via requests. Never raises.
    None means the HTTP call failed (timeout, DNS, SSL, HTTP error).
    An empty dict/list means the server answered and had nothing.
    """
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except (requests.RequestException, json.JSONDecodeError, ValueError):
            if attempt == retries - 1:
                return None
            time.sleep(0.4)
    return None


def _read_cache_file(cache_path: str) -> dict[str, str]:
    cache: dict[str, str] = {}
    if not cache_path or not os.path.exists(cache_path):
        return cache
    with open(cache_path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            k = (row.get("player_name") or "").strip().lower()
            v = (row.get("nhl_player_id") or row.get("espn_id") or "").strip()
            if k:
                cache[k] = v or NOT_FOUND
    return cache


def load_cache(cache_path: str) -> dict:
    """Merge the tracked cache with the requested file. A real id beats NOT_FOUND."""
    cache: dict[str, str] = {}
    paths = [str(_LEGACY_CACHE)]
    if os.path.abspath(cache_path) != os.path.abspath(str(_LEGACY_CACHE)):
        paths.append(cache_path)
    for path in paths:
        for key, val in _read_cache_file(path).items():
            prev = cache.get(key, "")
            if prev and prev != NOT_FOUND and (not val or val == NOT_FOUND):
                continue
            cache[key] = val
    return cache


def _write_cache_file(cache: dict, cache_path: str):
    parent = os.path.dirname(os.path.abspath(cache_path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = cache_path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["player_name", "nhl_player_id"])
        writer.writeheader()
        for name, pid in sorted(cache.items()):
            writer.writerow({"player_name": name, "nhl_player_id": pid})
    os.replace(tmp, cache_path)


def save_cache(cache: dict, cache_path: str):
    _write_cache_file(cache, cache_path)
    legacy = str(_LEGACY_CACHE)
    if os.path.abspath(cache_path) != os.path.abspath(legacy):
        _write_cache_file(cache, legacy)


def _parse_suggest_payload(data, original_name: str) -> str:
    """
    Suggest format examples:
      {"suggestions":["8478402|McDavid|Connor|1|EDM|C|..."]}
    We return the first suggestion's leading ID if present.
    """
    if not data or not isinstance(data, dict):
        return ""
    suggestions = data.get("suggestions", []) or []
    if not suggestions:
        return ""
    first = str(suggestions[0])
    parts = first.split("|")
    if parts and parts[0].isdigit():
        return parts[0]
    return ""


def _search_suggest(name: str) -> str | None:
    """Return an id, '' when the API answers with no match, or None on transport failure."""
    encoded = urllib.parse.quote(name)
    url = f"https://suggest.svc.nhl.com/svc/suggest/v1/minplayers/{encoded}/99"
    data = fetch_json(url)
    if data is None:
        return None
    return _parse_suggest_payload(data, name)


def _search_d3(name: str) -> str | None:
    """
    Fallback endpoint:
      https://search.d3.nhle.com/api/v1/search/player?culture=en-us&limit=25&q=<name>
    Common response: {"data":[{"playerId":..., "name":"..."}]}
    """
    encoded = urllib.parse.quote(name)
    url = f"https://search.d3.nhle.com/api/v1/search/player?culture=en-us&limit=25&q={encoded}"
    data = fetch_json(url)
    if data is None:
        return None
    if not data:
        return ""

    items = None
    if isinstance(data, dict):
        items = data.get("data") or data.get("results") or data.get("items")
    elif isinstance(data, list):
        items = data

    if not items or not isinstance(items, list):
        return ""

    target = name.strip().lower()
    best = None
    for it in items:
        if not isinstance(it, dict):
            continue
        full = str(it.get("name") or it.get("fullName") or "").strip().lower()
        if full == target:
            best = it
            break

    if best is None:
        best = items[0] if isinstance(items[0], dict) else None
    if not best:
        return ""

    pid = best.get("playerId") or best.get("id") or best.get("player_id")
    return str(pid) if pid else ""


def lookup_player_id(name: str) -> str | None:
    """One player, hard 3s timeout per endpoint.

    '' means both endpoints answered and neither had an id.
    None means every endpoint failed (timeout, DNS, SSL) and must not be cached.
    """
    return search_nhl_player(name)


def search_nhl_player(name: str) -> str | None:
    """Resolve NHL player ID. '' if the APIs answered with no id. None if they failed."""
    name = (name or "").strip()
    if not name:
        return ""

    suggest_id = _search_suggest(name)
    if suggest_id:
        return suggest_id

    d3_id = _search_d3(name)
    if d3_id:
        return d3_id

    if suggest_id is None and d3_id is None:
        return None
    return ""


def resolve_player_id(name: str) -> str:
    """
    Supports combos like "A + B" -> "idA|idB"
    If one side fails, returns the other.
    """
    name = (name or "").strip()
    if not name:
        return ""

    parts = [p.strip() for p in COMBO_SPLIT_RE.split(name) if p.strip()]
    if len(parts) <= 1:
        return search_nhl_player(name)

    ids = []
    for p in parts:
        pid = search_nhl_player(p)
        if pid:
            ids.append(pid)

    # stable + deterministic
    ids = sorted(set(ids), key=lambda x: int(x) if x.isdigit() else 10**18)
    return "|".join(ids)


def read_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(rows: list[dict], path: str, announce: bool = True):
    if not rows:
        print("No rows to write.")
        return
    parent = os.path.dirname(os.path.abspath(path))
    if parent:
        os.makedirs(parent, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    if announce:
        print(f"[OK] Saved {len(rows)} rows -> {path}")


def fetch_nhl_schedule_home_away(slate_date: str) -> dict:
    """
    Calls the NHL Stats API schedule endpoint for slate_date (YYYY-MM-DD).
    Returns a dict keyed by frozenset({away_abbrev, home_abbrev}) ->
        {"away": abbrev, "home": abbrev}
    so we can look up home/away for any two-team matchup.
    Gracefully returns {} on any failure.
    """
    url = f"https://api-web.nhle.com/v1/schedule/{slate_date}"
    print(f"  Fetching NHL schedule for {slate_date} from {url} ...")
    data = fetch_json(url, retries=2, timeout=15)
    if not data:
        print("  [WARN] Schedule fetch failed - is_home will default to 0")
        return {}

    lookup = {}
    game_week = data.get("gameWeek") or []
    for day in game_week:
        if day.get("date") != slate_date:
            continue
        for game in day.get("games") or []:
            away = (game.get("awayTeam") or {}).get("abbrev", "").upper()
            home = (game.get("homeTeam") or {}).get("abbrev", "").upper()
            if away and home:
                key = frozenset({away, home})
                lookup[key] = {"away": away, "home": home}
                print(f"    {away} @ {home}")

    print(f"  [OK] Schedule: {len(lookup)} games found")
    return lookup


def _name_parts(name: str) -> list[str]:
    name = (name or "").strip()
    if not name:
        return []
    parts = [p.strip() for p in COMBO_SPLIT_RE.split(name) if p.strip()]
    return parts or [name]


def _output_player_id(name: str, cache: dict) -> str:
    """Map a cache entry onto the output column. NOT_FOUND stays off the CSV."""
    parts = _name_parts(name)
    if len(parts) <= 1:
        val = cache.get(name.strip().lower(), "")
        return "" if not val or val == NOT_FOUND else val
    ids = []
    for part in parts:
        val = cache.get(part.lower(), "")
        if val and val != NOT_FOUND:
            ids.append(val)
    ids = sorted(set(ids), key=lambda x: int(x) if str(x).isdigit() else 10**18)
    return "|".join(ids)


def _missing_names(rows: list[dict], cache: dict) -> list[str]:
    seen: set[str] = set()
    missing: list[str] = []
    for row in rows:
        for part in _name_parts(row.get("player_name", "") or ""):
            key = part.lower()
            if key in cache or key in seen:
                continue
            seen.add(key)
            missing.append(part)
    return missing


def resolve_missing_ids(names: list[str], cache: dict, cache_path: str) -> int:
    """Look up uncached names with 8 workers. Persist NOT_FOUND so the next run skips them."""
    if not names:
        print("  ID cache covers every player on this slate")
        return 0

    print(f"  Looking up {len(names)} uncached players ({MAX_WORKERS} workers, 3s timeout)")
    lock = threading.Lock()
    done = 0

    def _store(name: str, pid: str) -> None:
        nonlocal done
        key = name.strip().lower()
        snapshot = None
        finished = 0
        with lock:
            cache[key] = pid if pid else NOT_FOUND
            done += 1
            finished = done
            if done % 50 == 0:
                snapshot = dict(cache)
        if snapshot is not None:
            save_cache(snapshot, cache_path)
            print(f"  [OK] Cache checkpoint: {finished} lookups saved")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(lookup_player_id, name): name for name in names}
        stored = 0
        failed = 0
        for future in _tqdm(as_completed(futures), total=len(futures), desc="  ID lookup", unit="player"):
            name = futures[future]
            try:
                result = future.result(timeout=LOOKUP_RESULT_TIMEOUT)
            except Exception:
                result = None
            if result is None:
                failed += 1
                continue
            _store(name, result)
            stored += 1

    if stored:
        save_cache(cache, cache_path)
        print(f"[OK] Cache updated: {stored} new lookups saved to {cache_path}")
    if failed:
        print(f"  [WARN] {failed} lookups failed (timeout/network) and were not cached")
    return stored


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="outputs/step1_nhl_props.csv")
    parser.add_argument("--output", default="outputs/step2_nhl_picktypes.csv")
    parser.add_argument("--cache", default=str(_LEGACY_CACHE))
    parser.add_argument("--sleep", type=float, default=0.15, help="Unused. Lookups run in parallel with a 3s HTTP timeout.")
    args = parser.parse_args()

    rows = read_csv(args.input)
    rows, n_fantasy = drop_fantasy_rows(rows)
    if n_fantasy:
        print(f"  Dropped {n_fantasy} fantasy prop row(s)")
    cache = load_cache(args.cache)
    missing = _missing_names(rows, cache)
    print(f"  ID cache: {len(cache)} entries, {len(missing)} to look up")
    resolve_missing_ids(missing, cache, args.cache)

    # Derive slate date from the first game_start in the data
    slate_date = None
    for row in rows:
        gs = (row.get("game_start") or "").strip()
        if gs:
            slate_date = gs[:10]
            break
    if not slate_date:
        from datetime import date
        slate_date = date.today().isoformat()

    # Fetch NHL schedule once to resolve home/away
    schedule_lookup = fetch_nhl_schedule_home_away(slate_date)

    results = []

    for processed_count, row in enumerate(_tqdm(rows, desc="  Processing players", unit="player"), start=1):
        stat_raw = row.get("stat_type", "") or ""
        stat_norm = STAT_NORM.get(stat_raw, stat_raw.lower().replace(" ", "_"))
        row["stat_norm"] = stat_norm

        # Classify player role
        pos = row.get("position", "") or ""
        if pos in GOALIE_POSITIONS or stat_norm in GOALIE_PROPS:
            row["player_role"] = "GOALIE"
        else:
            row["player_role"] = "SKATER"

        # Determine opponent and home/away
        team = (row.get("team", "") or "").upper()
        away_field = (row.get("away_team", "") or "").upper()
        home_field = (row.get("home_team", "") or "").upper()

        if team and away_field and home_field:
            # PrizePicks provided both — use directly
            if team == away_field:
                row["opponent"] = home_field
                row["is_home"] = "0"
            elif team == home_field:
                row["opponent"] = away_field
                row["is_home"] = "1"
            else:
                row["opponent"] = ""
                row["is_home"] = "0"
        else:
            # PrizePicks description holds the opponent abbreviation
            desc = (row.get("description", "") or "").strip().upper()
            opp = desc if desc else ""
            row["opponent"] = opp

            # Use NHL schedule lookup for authoritative home/away
            if team and opp and schedule_lookup:
                key = frozenset({team, opp})
                game = schedule_lookup.get(key)
                if game:
                    row["away_team"] = game["away"]
                    row["home_team"] = game["home"]
                    row["is_home"] = "1" if team == game["home"] else "0"
                else:
                    row["is_home"] = "0"
            else:
                row["is_home"] = "0"

        name = (row.get("player_name", "") or "").strip()
        nhl_id = _output_player_id(name, cache)
        row["nhl_player_id"] = nhl_id
        is_combo = "|" in (nhl_id or "")
        row["combo_prop"] = "True" if is_combo else "False"
        row["combo_player_ids"] = nhl_id if is_combo else ""
        results.append(row)

        if processed_count % 500 == 0:
            write_csv(results, args.output + ".partial", announce=False)
            print(f"[NHL step2] Checkpoint: {processed_count} players processed")

    write_csv(results, args.output)
    partial = args.output + ".partial"
    if os.path.exists(partial):
        os.remove(partial)

    # Quick summaries
    roles = {}
    for r in results:
        roles[r["player_role"]] = roles.get(r["player_role"], 0) + 1
    print(f"Role breakdown: {roles}")

    stat_counts = {}
    for r in results:
        stat_counts[r["stat_norm"]] = stat_counts.get(r["stat_norm"], 0) + 1
    print("Stat types:")
    for st, cnt in sorted(stat_counts.items(), key=lambda x: -x[1]):
        print(f"  {st}: {cnt}")


if __name__ == "__main__":
    main()