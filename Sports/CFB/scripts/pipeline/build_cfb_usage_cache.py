#!/usr/bin/env python3
"""
CFB volume soft via CollegeFootballData.com player usage (not ESPN snaps).

ESPN CFB boxscores expose counting stats only — no snap %. CFBD ``/player/usage``
returns offensive usage share (usg_overall / pass / rush) which can map to
minutes_tier soft when a free API key is present.

Auth (any one):
  CFBD_API_KEY
  CFBD_BEARER
  COLLEGEFOOTBALLDATA_API_KEY

Usage:
  py -3.14 Sports/CFB/scripts/pipeline/build_cfb_usage_cache.py
  py -3.14 Sports/CFB/scripts/pipeline/build_cfb_usage_cache.py --year 2024

Without a key this script probes once, reports CANNOT-OBTAIN, and exits 2.
Does not scrape blocked ESPN endpoints.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

_SCRIPT = Path(__file__).resolve()
_CFB_ROOT = _SCRIPT.parents[2]  # Sports/CFB
_REPO = _SCRIPT.parents[3]
_DEFAULT_OUT = _CFB_ROOT / "data" / "cache" / "cfb_usage_cache.json"
_CFBD_USAGE = "https://api.collegefootballdata.com/player/usage"

# Usage share (0–1) → minutes_tier soft bands (mirrors NFL snap % idea).
_HIGH = 0.18
_MED = 0.08


def _load_dotenv_keys() -> None:
    """Best-effort load of repo .env into os.environ (no overwrite)."""
    env_path = _REPO / ".env"
    if not env_path.is_file():
        return
    try:
        for line in env_path.read_text(encoding="utf-8").splitlines():
            s = line.strip()
            if not s or s.startswith("#") or "=" not in s:
                continue
            k, _, v = s.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            if k and k not in os.environ:
                os.environ[k] = v
    except OSError:
        return


def resolve_cfbd_key() -> str:
    _load_dotenv_keys()
    for name in ("CFBD_API_KEY", "CFBD_BEARER", "COLLEGEFOOTBALLDATA_API_KEY"):
        val = str(os.environ.get(name) or "").strip()
        if val:
            return val
    return ""


def usage_to_minutes_tier(usage: object) -> str:
    try:
        v = float(usage)
    except (TypeError, ValueError):
        return "UNKNOWN"
    if v != v:
        return "UNKNOWN"
    # Accept 0–100 mistakenly stored as percent.
    if v > 1.5:
        v = v / 100.0
    if v >= _HIGH:
        return "HIGH"
    if v >= _MED:
        return "MEDIUM"
    if v >= 0.0:
        return "LOW"
    return "UNKNOWN"


def infer_default_year(now: datetime | None = None) -> int:
    now = now or datetime.now(tz=timezone.utc)
    # CFB season year flips ~August; before Aug use prior completed season.
    return now.year if now.month >= 8 else now.year - 1


def probe_unauthorized() -> int:
    """One honest CFBD hit without a key — expect 401."""
    try:
        r = requests.get(_CFBD_USAGE, params={"year": 2024, "team": "Alabama"}, timeout=20)
        return int(r.status_code)
    except requests.RequestException as exc:
        print(f"[CFB usage] probe network error: {exc}")
        return -1


def fetch_usage(year: int, api_key: str, *, team: str | None = None) -> list[dict[str, Any]]:
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
    }
    params: dict[str, Any] = {"year": year}
    if team:
        params["team"] = team
    r = requests.get(_CFBD_USAGE, params=params, headers=headers, timeout=60)
    if r.status_code == 401:
        raise SystemExit("[CFB usage] CFBD key rejected (401). Check CFBD_API_KEY.")
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list):
        return []
    return data


def aggregate_players(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    players: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row.get("name") or "").strip()
        if not name:
            continue
        usage = row.get("usage") if isinstance(row.get("usage"), dict) else {}
        overall = usage.get("overall", row.get("usg_overall"))
        try:
            overall_f = float(overall) if overall is not None else float("nan")
        except (TypeError, ValueError):
            overall_f = float("nan")
        key = " ".join(name.lower().split())
        rec = {
            "name": name,
            "player_id": str(row.get("id") or row.get("athleteId") or "").strip() or None,
            "team": row.get("team"),
            "position": row.get("position"),
            "usg_overall": round(overall_f, 4) if overall_f == overall_f else None,
            "minutes_tier": usage_to_minutes_tier(overall_f),
        }
        players[key] = rec
        if rec["player_id"]:
            players[rec["player_id"]] = rec
    return players


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--year", type=int, default=0)
    ap.add_argument("--team", default="", help="Optional team filter for a small test pull")
    ap.add_argument("--out", default=str(_DEFAULT_OUT))
    ap.add_argument("--probe-only", action="store_true", help="Probe CFBD auth only")
    args = ap.parse_args()

    year = int(args.year) if args.year else infer_default_year()
    key = resolve_cfbd_key()

    if not key:
        status = probe_unauthorized()
        print(
            "[CFB usage] CANNOT-OBTAIN: no CFBD_API_KEY / CFBD_BEARER / "
            "COLLEGEFOOTBALLDATA_API_KEY in env or .env."
        )
        print(
            f"[CFB usage] Honest probe GET {_CFBD_USAGE}?year=2024&team=Alabama → HTTP {status} "
            "(key required; free tier at https://collegefootballdata.com/key)."
        )
        print(
            "[CFB usage] ESPN CFB remains blocked for snaps; no public dump wired. "
            "minutes_tier soft stays UNKNOWN until a key is provided."
        )
        return 2

    if args.probe_only:
        print(f"[CFB usage] key present — would fetch year={year}")
        return 0

    print(f"[CFB usage] Fetching CFBD player/usage year={year} team={args.team or '*'}")
    rows = fetch_usage(year, key, team=args.team.strip() or None)
    players = aggregate_players(rows)
    payload = {
        "meta": {
            "source": "collegefootballdata.com/player/usage",
            "year": year,
            "built_at": datetime.now(tz=timezone.utc).isoformat(),
            "n_rows": len(rows),
            "n_players": len({k for k, v in players.items() if v.get("name") and k == " ".join(str(v["name"]).lower().split())}),
        },
        "players": players,
    }
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    tiers = {}
    for v in players.values():
        if not isinstance(v, dict) or not v.get("name"):
            continue
        # Count each player once via lowercase name key only.
        continue
    # Recount uniquely by name key
    seen = set()
    for k, v in players.items():
        if not isinstance(v, dict):
            continue
        nk = " ".join(str(v.get("name") or "").lower().split())
        if not nk or nk in seen or k != nk:
            continue
        seen.add(nk)
        mt = str(v.get("minutes_tier") or "UNKNOWN")
        tiers[mt] = tiers.get(mt, 0) + 1
    print(f"[CFB usage] Wrote {out} players={len(seen)} minutes_tier={tiers}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
