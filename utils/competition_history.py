"""Thin-history competition gate for live ticket packing.

Blocks props whose league/competition tag has too little *prior* graded (or
board) history as of the slate date. Designed for World Cup / Olympics / WBC /
Nations League-style regime changes — not a hardcoded tournament allowlist.

History is counted from:
  1) graded_props_*.json ``league`` (preferred, once persisted)
  2) soccer (and other) step1/step8 CSVs that already carry ``league``

Live rows with no league tag pass (don't block unknown). Rows with a league
that fails the prior-days / prior-props thresholds fail.
"""
from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

_REPO = Path(__file__).resolve().parent.parent
_DEFAULT_CFG_PATH = _REPO / "config" / "competition_history.json"
_DEFAULT_CACHE_PATH = _REPO / "data" / "cache" / "competition_history.json"

_DEFAULT_CFG: dict[str, Any] = {
    "enabled": True,
    "min_prior_days": 10,
    "min_prior_props": 8000,
    # Pass if either threshold is met on history strictly before as_of_date.
    "pass_mode": "days_or_props",
    "collapse_prefixes": [
        "WORLDCUP",
        "OLYMPIC",
        "OLYMPICS",
        "WBC",
        "WORLDBASEBALL",
        "NATIONSLEAGUE",
        "EURO",
        "COPAAMERICA",
        "GOLDCUP",
    ],
    "gated_sports": [
        "SOCCER",
        "SOC",
        "MLB",
        "NBA",
        "WNBA",
        "NFL",
        "NHL",
        "TENNIS",
        "GOLF",
        "PGA",
        "CFB",
        "CBB",
    ],
}


def _fold_sport(sport: object) -> str:
    s = str(sport or "").strip().upper()
    if s.startswith("SOCCER") or s.startswith("WORLDCUP") or "WORLD CUP" in s:
        return "SOCCER"
    if s in {"SOC"}:
        return "SOCCER"
    if s.startswith("NBA"):
        return "NBA" if s in {"NBA", "NBA1H", "NBA1Q"} else s
    if s.startswith("WNBA"):
        return "WNBA" if s in {"WNBA", "WNBA1H", "WNBA1Q"} else s
    if s in {"PGA", "GOLF"}:
        return "GOLF"
    return s


def normalize_competition_key(sport: object, league: object) -> str:
    """Stable sport|competition key. Collapses WORLDCUP1H/2H/TRNY → WORLDCUP."""
    sp = _fold_sport(sport)
    raw = str(league or "").strip().upper()
    raw = re.sub(r"\s+", "", raw)
    if not raw or raw in {"NAN", "NONE", "NULL", "—", "-"}:
        return ""
    # Board tags sometimes embed sport; strip.
    for prefix in ("SOCCER", "MLS", "EPL"):
        if raw.startswith(prefix) and raw != prefix:
            # keep MLS/EPL as themselves; SOCCER alone is useless
            pass
    collapsed = raw
    cfg_prefixes = list(_DEFAULT_CFG["collapse_prefixes"])
    for pref in cfg_prefixes:
        if raw.startswith(pref):
            collapsed = pref
            break
    if sp == "SOCCER" and collapsed in {"SOCCER"}:
        return ""  # uninformative
    return f"{sp}|{collapsed}"


def _parse_date(raw: object) -> Optional[str]:
    s = str(raw or "").strip()
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        return s[:10]
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return None


def load_config(path: Path | None = None) -> dict[str, Any]:
    p = path or _DEFAULT_CFG_PATH
    cfg = dict(_DEFAULT_CFG)
    if p.is_file():
        try:
            loaded = json.loads(p.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                cfg.update(loaded)
        except Exception:
            pass
    return cfg


def build_competition_history(
    repo_root: Path | None = None,
    *,
    write_cache: bool = True,
) -> dict[str, Any]:
    """Scan graded_props + step CSVs → {key: {dates: {day: n}, n_props, n_days}}."""
    root = repo_root or _REPO
    # key -> date -> count
    tallies: dict[str, dict[str, int]] = {}

    def _add(key: str, day: str, n: int = 1) -> None:
        if not key or not day:
            return
        bucket = tallies.setdefault(key, {})
        bucket[day] = bucket.get(day, 0) + n

    # 1) graded_props_*.json
    for base in (
        root / "ui_runner" / "templates",
        root / "mobile" / "www",
        root / "ui_runner" / "graded_slate",
    ):
        if not base.is_dir():
            continue
        for fp in base.glob("graded_props_*.json"):
            m = re.match(r"graded_props_(\d{4}-\d{2}-\d{2})\.json$", fp.name)
            file_day = m.group(1) if m else ""
            try:
                data = json.loads(fp.read_text(encoding="utf-8"))
            except Exception:
                continue
            props = data if isinstance(data, list) else (data.get("props") or [])
            payload_day = ""
            if isinstance(data, dict):
                payload_day = _parse_date(data.get("date")) or ""
            day0 = payload_day or file_day
            for p in props:
                if not isinstance(p, dict):
                    continue
                day = _parse_date(p.get("graded_at") or p.get("date") or p.get("game_date")) or day0
                key = normalize_competition_key(p.get("sport"), p.get("league"))
                if key:
                    _add(key, day)

    # 2) step1/step8 CSVs with league (soccer-heavy; also golf/tennis if present)
    try:
        import pandas as pd
    except ImportError:
        pd = None  # type: ignore

    if pd is not None:
        patterns = [
            "**/step1*soccer*.csv",
            "**/step8*soccer*.csv",
            "**/s1_soccer*.csv",
            "outputs/*/soccer/step1*.csv",
            "outputs/*/soccer/step8*.csv",
            "outputs/*/golf/step8*.csv",
            "outputs/*/tennis/step8*.csv",
            "outputs/*/mlb/step1*.csv",
            "outputs/*/nfl/step1*.csv",
            "outputs/*/nfl/step8*.csv",
            "outputs/*/cfb/step1*.csv",
            "outputs/*/nba/step1*.csv",
            "outputs/*/wnba/step1*.csv",
            "outputs/*/nhl/step1*.csv",
        ]
        seen: set[str] = set()
        files: list[Path] = []
        for pat in patterns:
            for fp in root.glob(pat):
                try:
                    rp = str(fp.resolve())
                except OSError:
                    rp = str(fp)
                if rp in seen:
                    continue
                seen.add(rp)
                files.append(fp)

        for fp in files:
            # Infer day from path outputs/YYYY-MM-DD/...
            day = ""
            for part in fp.parts:
                if re.match(r"^\d{4}-\d{2}-\d{2}$", part):
                    day = part
                    break
            try:
                usecols = None
                header = pd.read_csv(fp, nrows=0)
                cols = {c.lower(): c for c in header.columns}
                if "league" not in cols and "tournament" not in cols and "event" not in cols:
                    continue
                want = []
                for k in ("league", "tournament", "event", "sport", "start_time", "game_time", "date"):
                    if k in cols:
                        want.append(cols[k])
                df = pd.read_csv(fp, low_memory=False, usecols=want)
            except Exception:
                continue
            league_c = next((c for c in df.columns if c.lower() in {"league", "tournament", "event"}), None)
            if not league_c:
                continue
            sport_c = next((c for c in df.columns if c.lower() == "sport"), None)
            date_c = next(
                (c for c in df.columns if c.lower() in {"start_time", "game_time", "date"}),
                None,
            )
            default_sport = "SOCCER" if "soccer" in fp.name.lower() else ""
            path_l = str(fp).lower()
            if "golf" in path_l:
                default_sport = "GOLF"
            if "tennis" in path_l:
                default_sport = "TENNIS"
            if "mlb" in path_l:
                default_sport = "MLB"
            if "/nfl/" in path_l.replace("\\", "/") or "nfl" in fp.name.lower():
                default_sport = "NFL"
            if "/cfb/" in path_l.replace("\\", "/"):
                default_sport = "CFB"
            if "/nba/" in path_l.replace("\\", "/") and "/wnba/" not in path_l.replace("\\", "/"):
                default_sport = "NBA"
            if "/wnba/" in path_l.replace("\\", "/"):
                default_sport = "WNBA"
            if "/nhl/" in path_l.replace("\\", "/"):
                default_sport = "NHL"
            for r in df.itertuples(index=False):
                lg = getattr(r, league_c, "")
                sp = getattr(r, sport_c, "") if sport_c else default_sport
                row_day = day
                if date_c:
                    row_day = _parse_date(getattr(r, date_c, "")) or day
                key = normalize_competition_key(sp or default_sport, lg)
                if key and row_day:
                    _add(key, row_day)

    out: dict[str, Any] = {
            "built_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "competitions": {},
    }
    for key, dates in tallies.items():
        out["competitions"][key] = {
            "dates": dict(sorted(dates.items())),
            "n_props": int(sum(dates.values())),
            "n_days": int(len(dates)),
        }

    if write_cache:
        cache = root / "data" / "cache" / "competition_history.json"
        cache.parent.mkdir(parents=True, exist_ok=True)
        cache.write_text(json.dumps(out, indent=2), encoding="utf-8")
    return out


def load_competition_history(
    repo_root: Path | None = None,
    *,
    rebuild_if_missing: bool = True,
) -> dict[str, Any]:
    root = repo_root or _REPO
    cache = root / "data" / "cache" / "competition_history.json"
    if cache.is_file():
        try:
            return json.loads(cache.read_text(encoding="utf-8"))
        except Exception:
            pass
    if rebuild_if_missing:
        return build_competition_history(root, write_cache=True)
    return {"competitions": {}}


@lru_cache(maxsize=1)
def _cached_history_and_cfg() -> tuple[dict[str, Any], dict[str, Any]]:
    return load_competition_history(_REPO), load_config()


def clear_competition_history_cache() -> None:
    _cached_history_and_cfg.cache_clear()


def prior_stats(
    key: str,
    *,
    as_of_date: Optional[str],
    history: dict[str, Any],
) -> tuple[int, int]:
    """Return (prior_days, prior_props) strictly before as_of_date (if set)."""
    comp = (history.get("competitions") or {}).get(key) or {}
    dates = comp.get("dates") or {}
    if not dates:
        return 0, 0
    prior_days = 0
    prior_props = 0
    for day, n in dates.items():
        if as_of_date and str(day) >= str(as_of_date):
            continue
        prior_days += 1
        prior_props += int(n)
    return prior_days, prior_props


def competition_is_thin(
    sport: object,
    league: object,
    *,
    as_of_date: Optional[str] = None,
    history: dict[str, Any] | None = None,
    cfg: dict[str, Any] | None = None,
) -> bool:
    """True when this competition should be gated off the live packer."""
    cfg = cfg or load_config()
    if not cfg.get("enabled", True):
        return False
    sp = _fold_sport(sport)
    gated = {str(x).upper() for x in (cfg.get("gated_sports") or [])}
    if gated and sp not in gated and not any(sp.startswith(g) for g in gated):
        return False
    key = normalize_competition_key(sport, league)
    if not key:
        return False  # no tag → do not block
    hist = history if history is not None else load_competition_history()
    days, props = prior_stats(key, as_of_date=as_of_date, history=hist)
    min_days = int(cfg.get("min_prior_days") or 10)
    min_props = int(cfg.get("min_prior_props") or 8000)
    mode = str(cfg.get("pass_mode") or "days_or_props").lower()
    if mode == "days_and_props":
        ok = days >= min_days and props >= min_props
    else:
        ok = days >= min_days or props >= min_props
    return not ok


def row_as_of_date(row: dict[str, Any]) -> Optional[str]:
    for k in (
        "slate_date",
        "date",
        "game_date",
        "match_date",
        "start_time",
        "game_time",
        "Game Time",
    ):
        d = _parse_date(row.get(k))
        if d:
            return d
    return None


def row_competition_ok(
    row: dict[str, Any],
    *,
    history: dict[str, Any] | None = None,
    cfg: dict[str, Any] | None = None,
    as_of_date: Optional[str] = None,
) -> bool:
    """Packer-facing: False → drop this prop/leg."""
    cfg = cfg or load_config()
    if not cfg.get("enabled", True):
        return True
    league = (
        row.get("league")
        or row.get("League")
        or row.get("competition")
        or row.get("tournament")
        or row.get("event")
        or ""
    )
    sport = row.get("sport") or row.get("Sport") or ""
    thin = competition_is_thin(
        sport,
        league,
        as_of_date=as_of_date or row_as_of_date(row),
        history=history,
        cfg=cfg,
    )
    return not thin


def main() -> None:
    hist = build_competition_history(_REPO, write_cache=True)
    comps = hist.get("competitions") or {}
    print(f"built {len(comps)} competition keys -> data/cache/competition_history.json")
    # Show thinnest / World Cup-ish
    rows = []
    for k, v in comps.items():
        rows.append((v.get("n_days", 0), v.get("n_props", 0), k))
    rows.sort()
    print("thinnest 15:")
    for d, n, k in rows[:15]:
        print(f"  days={d:4d} props={n:6d}  {k}")
    for needle in ("WORLDCUP", "MLS", "EPL", "NWSL", "UCL", "ENG.1"):
        hits = [r for r in rows if needle in r[2]]
        if hits:
            print(f"match {needle}: {hits[:5]}")


if __name__ == "__main__":
    main()
