#!/usr/bin/env python3
"""
step5a_attach_espn_ids.py  (v3 — PP ID map as primary key)
-----------------------------------------------------------
Attaches ESPN athlete_id + team_id via four strategies in priority order:

  1. pp_id_map lookup   — Explicit PP player_id → ESPN athlete_id map stored in
                          data/reference/pp_to_espn_id_map.csv. This is the
                          ONLY collision-safe strategy. Built up over time.

  2. master name lookup — (player_norm, team_abbr) → (team_id, espn_athlete_id)
                          from ncaa_football_athletes_master.csv. Used when a player
                          has no entry in the PP map yet. Result is immediately
                          written into the PP map for future runs.

  3. name-only fallback — Same master, no team constraint. Last resort before
                          hitting the live API.

  4. ESPN live search   — Hits ESPN search API for any remaining NO_MATCH rows.
                          Result written into both master and PP map.

The PP map is the source of truth. Name matching is only a bootstrap mechanism
to populate it — once a player is in the map, name matching is never used again
for that player, eliminating all collision risk going forward.

Map file: data/reference/pp_to_espn_id_map_cfb.csv
Columns:  pp_player_id, espn_athlete_id, team_id, player_name, team_abbr,
          source (map|name_team|name_only|espn_api), updated_at

attach_status values:
  OK_MAP      — resolved from pp_id_map (most reliable)
  OK_TEAM     — name+team match from master (written to map)
  OK_NAME     — name-only match from master (written to map)
  OK_ESPN     — ESPN API fallback (written to map + master)
  NO_MATCH    — unresolved; espn_athlete_id left blank

Usage:
    py step5a_attach_espn_ids.py --input step3_cfb.csv --output step5a_cfb.csv
"""

from __future__ import annotations

import argparse
import datetime
import re
import sys
import time
import unicodedata
from pathlib import Path

import pandas as pd
import requests

# Ensure <repo>/PropOracle is on sys.path so we can import PropOracle-level helpers.
_PROPORACLE_ROOT = Path(__file__).resolve().parents[4]
if str(_PROPORACLE_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROPORACLE_ROOT))

from scripts.db_utils import log_pipeline_health

_CFB_ROOT = Path(__file__).resolve().parents[2]
MASTER_PATH  = "data/reference/ncaa_football_athletes_master.csv"
PP_MAP_PATH  = "data/reference/pp_to_espn_id_map_cfb.csv"
ESPN_SEARCH  = "https://site.api.espn.com/apis/common/v3/search"
ESPN_TEAMS   = "https://site.api.espn.com/apis/site/v2/sports/football/college-football/teams"
ESPN_ROSTER  = (
    "https://site.api.espn.com/apis/site/v2/sports/football/"
    "college-football/teams/{team_id}?enable=roster"
)
HEADERS      = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
_SUFFIX_RE   = re.compile(r"\b(jr|sr|ii|iii|iv)\b")
# PrizePicks slate abbr → ESPN abbreviation
_SLATE_TO_ESPN_ABBR = {
    "NCST": "NCSU",
    "HAW": "HAW",
    "JVST": "JVST",
    "SAC": "SAC",
    "NDSU": "NDSU",
}

PP_MAP_COLS  = ["pp_player_id", "espn_athlete_id", "team_id",
                "player_name", "team_abbr", "source", "updated_at"]


# ── Normalizer (must match step2 + step5b exactly) ───────────────────────────

def norm_name(s: str) -> str:
    """Must match step2_normalize.norm_str (including Jr/Sr suffix strip)."""
    s = (s or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = _SUFFIX_RE.sub("", s)
    return re.sub(r"\s+", " ", s).strip()


def _norm_name_keep_suffix(s: str) -> str:
    s = (s or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _clean_id(v) -> str:
    s = str(v or "").strip()
    if s.lower() in ("", "nan", "none"):
        return ""
    if s.endswith(".0") and s[:-2].isdigit():
        return s[:-2]
    return s


# ── PP ID map helpers ─────────────────────────────────────────────────────────

def load_pp_map(map_path: Path) -> dict[str, dict]:
    """Returns {pp_player_id: {espn_athlete_id, team_id, ...}}"""
    if not map_path.exists():
        return {}
    try:
        df = pd.read_csv(map_path, dtype=str).fillna("")
        return {str(r["pp_player_id"]).strip(): r.to_dict()
                for _, r in df.iterrows() if r.get("pp_player_id")}
    except Exception as e:
        print(f"  [MAP] Load failed ({e}) — starting fresh")
        return {}


def save_pp_map(map_path: Path, pp_map: dict[str, dict]) -> None:
    if not pp_map:
        return
    map_path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(list(pp_map.values()))
    # Ensure all expected columns present
    for col in PP_MAP_COLS:
        if col not in df.columns:
            df[col] = ""
    df = df[PP_MAP_COLS].sort_values("pp_player_id")
    df.to_csv(map_path, index=False, encoding="utf-8")


def add_to_pp_map(pp_map: dict, pp_id: str, espn_id: str, team_id: str,
                  player_name: str, team_abbr: str, source: str) -> None:
    """Upsert a row into the in-memory pp_map."""
    pp_map[pp_id] = {
        "pp_player_id":    pp_id,
        "espn_athlete_id": espn_id,
        "team_id":         team_id,
        "player_name":     player_name,
        "team_abbr":       team_abbr,
        "source":          source,
        "updated_at":      datetime.date.today().isoformat(),
    }


# ── ESPN live search ──────────────────────────────────────────────────────────

def espn_search_player(name: str, team_abbr: str = "") -> tuple[str, str]:
    """Returns (team_id, espn_athlete_id) or ('', '')."""
    want = {_SLATE_TO_ESPN_ABBR.get(team_abbr, team_abbr), team_abbr}
    want = {a for a in want if a}
    try:
        r = requests.get(
            ESPN_SEARCH,
            params={"query": name, "limit": "8", "type": "athlete",
                    "sport": "football", "league": "college-football"},
            headers=HEADERS, timeout=10,
        )
        r.raise_for_status()
        data = r.json()
    except Exception:
        log_pipeline_health(
            "cfb.step5a_attach_espn_ids",
            "espn_search_failed",
            extra={"query": name, "team_abbr": team_abbr},
            start=Path(__file__),
        )
        return "", ""

    fallback = ("", "")
    for block in (data.get("results") or []):
        for item in (block.get("contents") or []):
            ath   = item.get("athlete") or item
            aid   = _clean_id(ath.get("id", ""))
            if not aid:
                continue
            tinfo = ath.get("team") or {}
            t_abbr = str(tinfo.get("abbreviation", "")).strip().upper()
            tid    = _clean_id(tinfo.get("id", ""))
            if not tid:
                continue
            if want and t_abbr and t_abbr not in want:
                if not fallback[0]:
                    fallback = (tid, aid)
                continue
            return tid, aid
    return fallback


def _extract_roster_athletes(payload: dict) -> list[dict]:
    athletes_raw: list = []
    if isinstance(payload.get("athletes"), list):
        athletes_raw = payload["athletes"]
    else:
        team = payload.get("team")
        if isinstance(team, dict) and isinstance(team.get("athletes"), list):
            athletes_raw = team["athletes"]
    out: list[dict] = []
    for block in athletes_raw or []:
        if not isinstance(block, dict):
            continue
        items = block.get("items")
        if isinstance(items, list):
            for it in items:
                if not isinstance(it, dict):
                    continue
                a = it.get("athlete") if isinstance(it.get("athlete"), dict) else it
                if isinstance(a, dict) and a.get("id"):
                    out.append(a)
        elif block.get("id"):
            out.append(block)
    return out


def load_espn_team_ids() -> dict[str, str]:
    """abbr → ESPN team_id (FBS + FCS). Rankings first, then ESPN teams list."""
    out: dict[str, str] = {}
    rank_path = _CFB_ROOT / "data" / "reference" / "cfb_team_unit_rankings.csv"
    if rank_path.is_file():
        try:
            rdf = pd.read_csv(rank_path, dtype=str).fillna("")
            for _, r in rdf.iterrows():
                abbr = str(r.get("team_abbr", "")).strip().upper()
                tid = _clean_id(r.get("team_id", ""))
                if abbr and tid:
                    out[abbr] = tid
        except Exception:
            pass
    for slate, espn in _SLATE_TO_ESPN_ABBR.items():
        if espn in out and slate not in out:
            out[slate] = out[espn]
    try:
        r = requests.get(ESPN_TEAMS, params={"limit": "1000"}, headers=HEADERS, timeout=20)
        r.raise_for_status()
        payload = r.json()
        teams = []
        try:
            teams = payload["sports"][0]["leagues"][0]["teams"]
        except (IndexError, KeyError, TypeError):
            teams = payload.get("teams") or []
        for item in teams:
            t = item.get("team", item) if isinstance(item, dict) else {}
            abbr = str(t.get("abbreviation", "")).strip().upper()
            tid = _clean_id(t.get("id", ""))
            if abbr and tid and abbr not in out:
                out[abbr] = tid
    except Exception:
        pass
    return out


def load_roster_name_index(team_id: str, cache: dict) -> dict[str, tuple[str, str]]:
    """norm_name → (team_id, athlete_id) for one ESPN roster."""
    if team_id in cache:
        return cache[team_id]
    idx: dict[str, tuple[str, str]] = {}
    try:
        r = requests.get(ESPN_ROSTER.format(team_id=team_id), headers=HEADERS, timeout=20)
        r.raise_for_status()
        for ath in _extract_roster_athletes(r.json()):
            aid = _clean_id(ath.get("id", ""))
            raw = str(ath.get("displayName") or ath.get("fullName") or "").strip()
            n = norm_name(raw)
            if aid and n and n not in idx:
                idx[n] = (team_id, aid)
            n2 = _norm_name_keep_suffix(raw)
            if aid and n2 and n2 not in idx:
                idx[n2] = (team_id, aid)
        time.sleep(0.12)
    except Exception:
        log_pipeline_health(
            "cfb.step5a_attach_espn_ids",
            "espn_roster_failed",
            extra={"team_id": team_id},
            start=Path(__file__),
        )
    cache[team_id] = idx
    return idx


def load_boxscore_name_index(cache_path: Path) -> dict[str, tuple[str, str]]:
    """player_norm -> (team_id, espn_athlete_id) from CFB boxscore cache."""
    if not cache_path.is_file():
        return {}
    try:
        cdf = pd.read_csv(cache_path, dtype=str).fillna("")
    except Exception:
        return {}
    out: dict[str, tuple[str, str]] = {}
    if "player_norm" not in cdf.columns:
        return {}
    for _, r in cdf.iterrows():
        raw = r.get("player_norm", "")
        aid = _clean_id(r.get("espn_athlete_id", ""))
        tid = _clean_id(r.get("team_id", ""))
        if not aid:
            continue
        for nkey in {norm_name(raw), _norm_name_keep_suffix(raw)}:
            if nkey and nkey not in out:
                out[nkey] = (tid, aid)
    return out

def append_to_master(master_path: Path, new_rows: list[dict]) -> None:
    if not new_rows:
        return
    new_df = pd.DataFrame(new_rows)
    if master_path.exists():
        existing = pd.read_csv(master_path, dtype=str).fillna("")
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset="espn_athlete_id", keep="first")
    else:
        combined = new_df
    combined.to_csv(master_path, index=False, encoding="utf-8")
    print(f"  [MASTER] Appended {len(new_rows)} new athletes → {master_path}")


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",    required=True)
    ap.add_argument("--output",   required=True)
    ap.add_argument("--master",   default=MASTER_PATH)
    ap.add_argument("--pp_map",   default=PP_MAP_PATH)
    ap.add_argument("--no_espn",  action="store_true",
                    help="Skip live ESPN fallback (faster, offline-safe)")
    args = ap.parse_args()

    try:
        df = pd.read_csv(args.input, dtype=str).fillna("")
    except Exception as e:
        log_pipeline_health(
            "cfb.step5a_attach_espn_ids",
            "read_failed",
            extra={"input": args.input, "error": f"{type(e).__name__}: {e}"},
            start=Path(__file__),
        )
        raise
    print(f"-> Loaded slate: {args.input} | rows={len(df)}")

    # ── Load PP ID map (primary source of truth) ──────────────────────────────
    map_path = Path(args.pp_map)
    pp_map   = load_pp_map(map_path)
    print(f"-> PP ID map: {map_path} | entries={len(pp_map)}")

    # ── Load master (fallback name-matching) ──────────────────────────────────
    master_path = Path(args.master)
    if not master_path.exists():
        print(f"  [WARN] Master not found at {master_path} — name lookup disabled")
        master = pd.DataFrame(columns=["espn_athlete_id", "athlete_name",
                                        "athlete_name_norm", "team_id",
                                        "team_name", "team_abbr"])
    else:
        master = pd.read_csv(master_path, dtype=str).fillna("")
        print(f"-> Master: {master_path} | rows={len(master)}")

    name_col = next(
        (c for c in ("athlete_name_norm", "player_norm", "athlete_name", "player_name")
         if c in master.columns),
        None,
    )
    if name_col:
        master["_name_norm"] = master[name_col].astype(str).apply(norm_name)
    else:
        master["_name_norm"] = ""
    if "team_abbr" in master.columns:
        master["_team_norm"] = master["team_abbr"].str.strip().str.upper()
    else:
        master["_team_norm"] = ""

    map_name_team: dict[tuple, tuple] = {}
    map_name_only: dict[str, tuple]   = {}
    for _, r in master.iterrows():
        aid = str(r["espn_athlete_id"]).strip()
        tid = str(r["team_id"]).strip()
        n   = r["_name_norm"]
        t   = r["_team_norm"]
        if n and t:
            map_name_team[(n, t)] = (tid, aid)
        if n and n not in map_name_only:
            map_name_only[n] = (tid, aid)

    cache_idx = load_boxscore_name_index(Path("data/cache/cfb_boxscore_cache.csv"))
    print(f"-> Boxscore cache names: {len(cache_idx)}")
    team_ids_by_abbr = load_espn_team_ids()
    print(f"-> ESPN team abbr map: {len(team_ids_by_abbr)}")
    roster_cache: dict[str, dict[str, tuple[str, str]]] = {}
    search_memo: dict[tuple[str, str], tuple[str, str]] = {}

    # ── Detect slate columns ──────────────────────────────────────────────────
    player_col = next((c for c in ["player_norm", "player", "Player", "player_name"]
                       if c in df.columns), None)
    team_col   = next((c for c in ["team_abbr", "pp_team", "team", "Team"]
                       if c in df.columns), None)
    pid_col    = next((c for c in ["player_id", "playerId"] if c in df.columns), None)

    if not player_col:
        raise SystemExit(f"No player column found. Columns: {list(df.columns)}")

    print(f"  player_col={player_col}  team_col={team_col}  pid_col={pid_col}")

    team_ids:    list[str] = []
    athlete_ids: list[str] = []
    statuses:    list[str] = []
    map_updated  = False
    master_new_rows: list[dict] = []

    for _, row in df.iterrows():
        raw_pid  = str(row.get(pid_col, "")).strip()   if pid_col  else ""
        raw_name = str(row.get(player_col, "")).strip()
        raw_team = str(row.get(team_col, "")).strip().upper() if team_col else ""
        n = norm_name(raw_name)

        # ── Bouncer (row-level): keep row, don't poison downstream ────────────
        # IMPORTANT: do NOT drop rows here. Dropping changes row counts and can
        # cause downstream merges/concats to misalign and look like "blank rows".
        if not raw_name or (team_col and not raw_team):
            team_ids.append("")
            athlete_ids.append("")
            statuses.append("BOUNCED_BLANK_IDENTITY")
            continue

        # ── Strategy 1: PP ID map lookup (collision-safe) ────────────────────
        if raw_pid and raw_pid in pp_map:
            entry = pp_map[raw_pid]
            team_ids.append(entry["team_id"])
            athlete_ids.append(entry["espn_athlete_id"])
            statuses.append("OK_MAP")
            continue

        # ── Strategy 2a: name + team from master ─────────────────────────────
        result = map_name_team.get((n, raw_team))
        if result:
            tid, aid = result
            team_ids.append(tid)
            athlete_ids.append(aid)
            statuses.append("OK_TEAM")
            # Write into PP map so next run skips name matching for this player
            if raw_pid:
                add_to_pp_map(pp_map, raw_pid, aid, tid, raw_name, raw_team, "name_team")
                map_updated = True
            continue

        # ── Strategy 2b: name-only from master ───────────────────────────────
        result = map_name_only.get(n)
        if result:
            tid, aid = result
            team_ids.append(tid)
            athlete_ids.append(aid)
            statuses.append("OK_NAME")
            if raw_pid:
                add_to_pp_map(pp_map, raw_pid, aid, tid, raw_name, raw_team, "name_only")
                map_updated = True
            continue

        # ── Strategy 2c: boxscore cache name (prior-season L5 bootstrap) ─────
        result = cache_idx.get(n) or cache_idx.get(_norm_name_keep_suffix(raw_name))
        if result:
            tid, aid = result
            team_ids.append(tid)
            athlete_ids.append(aid)
            statuses.append("OK_CACHE")
            if raw_pid:
                add_to_pp_map(pp_map, raw_pid, aid, tid, raw_name, raw_team, "box_cache")
                map_updated = True
            continue

        # ── Strategy 2d: current ESPN roster for this team (portal / FCS) ──
        slate_tid = team_ids_by_abbr.get(raw_team, "")
        if not args.no_espn and slate_tid:
            ridx = load_roster_name_index(slate_tid, roster_cache)
            result = ridx.get(n) or ridx.get(_norm_name_keep_suffix(raw_name))
            if result:
                tid, aid = result
                team_ids.append(tid)
                athlete_ids.append(aid)
                statuses.append("OK_ROSTER")
                if raw_pid:
                    add_to_pp_map(pp_map, raw_pid, aid, tid, raw_name, raw_team, "roster")
                    map_updated = True
                master_new_rows.append({
                    "espn_athlete_id":   aid,
                    "athlete_name":      raw_name,
                    "athlete_name_norm": n,
                    "team_id":           tid,
                    "team_name":         "",
                    "team_abbr":         raw_team,
                })
                continue

        # ── Strategy 3: ESPN live search ─────────────────────────────────────
        if not args.no_espn and raw_name:
            key = (raw_name, raw_team)
            if key not in search_memo:
                search_memo[key] = espn_search_player(raw_name, raw_team)
                time.sleep(0.12)
            tid, aid = search_memo[key]
            if not tid and slate_tid and aid:
                tid = slate_tid
            if aid:
                team_ids.append(tid)
                athlete_ids.append(aid)
                statuses.append("OK_ESPN")
                if raw_pid:
                    add_to_pp_map(pp_map, raw_pid, aid, tid, raw_name, raw_team, "espn_api")
                    map_updated = True
                master_new_rows.append({
                    "espn_athlete_id":   aid,
                    "athlete_name":      raw_name,
                    "athlete_name_norm": n,
                    "team_id":           tid,
                    "team_name":         "",
                    "team_abbr":         raw_team,
                })
                continue

        # ── No athlete id — still attach ESPN team_id so box scores can match ─
        team_ids.append(slate_tid)
        athlete_ids.append("")
        statuses.append("NO_MATCH")

    # ── Write outputs ─────────────────────────────────────────────────────────
    df["team_id"]         = team_ids
    df["espn_athlete_id"] = athlete_ids
    df["attach_status"]   = statuses

    df.to_csv(args.output, index=False, encoding="utf-8")
    print(f"\nSaved -> {args.output} | rows={len(df)}")
    print("\nattach_status breakdown:")
    print(df["attach_status"].value_counts().to_string())

    no_tid = (df["team_id"].str.strip() == "").sum()
    if no_tid:
        print(f"\n  ⚠️  {no_tid} rows have no team_id")
        if args.no_espn:
            print("     Rerun without --no_espn to trigger live ESPN fallback")

    # Match-rate health check (log only; don't fail the run)
    if len(df):
        ok = int((df["attach_status"] != "NO_MATCH").sum())
        ok_rate = ok / len(df)
        if ok_rate < 0.80:
            log_pipeline_health(
                "cfb.step5a_attach_espn_ids",
                "low_match_rate",
                extra={"ok": ok, "total": len(df), "ok_rate": round(ok_rate, 3), "no_espn": bool(args.no_espn)},
                start=Path(__file__),
            )

        bounced = int((df["attach_status"] == "BOUNCED_BLANK_IDENTITY").sum())
        if bounced:
            log_pipeline_health(
                "cfb.step5a_attach_espn_ids",
                "bounced_blank_identity_rows",
                extra={"bounced": bounced, "total": len(df)},
                start=Path(__file__),
            )

    # Persist updated PP map and master
    if map_updated:
        save_pp_map(map_path, pp_map)
        new_entries = sum(1 for s in statuses if s in ("OK_TEAM", "OK_NAME", "OK_ESPN", "OK_CACHE", "OK_ROSTER"))
        print(f"  [MAP] Saved {len(pp_map)} entries → {map_path}  (+{new_entries} new this run)")

    append_to_master(master_path, master_new_rows)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log_pipeline_health(
            "cfb.step5a_attach_espn_ids",
            "run_failed",
            extra={"error": f"{type(e).__name__}: {e}"},
            start=Path(__file__),
        )
        print(f"❌ CFB step5a failed (logged). {type(e).__name__}: {e}")
        raise
