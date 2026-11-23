#!/usr/bin/env python3
"""
Fetch Underdog Fantasy pick'em lines and write a PrizePicks-shaped CSV
(plus optional ladder CSV with american odds / payout multipliers).

Board API (v5 is broken — 426 upgrade_required / 403):
  GET https://api.underdogfantasy.com/v1/over_under_lines          # mains
  GET https://api.underdogfantasy.com/v3/over_unders/{id}/alternate_projections

Sports match Underdog's sport_id on games/solo_games. Use --sport ALL for every
open line. Aliases: soccer=FIFA/MASL/UFL, tennis=TENNIS, golf=PGA/GOLF, nfl=NFL/NFLP.

Examples:
  py -3 scripts/fetch_underdog_pickem.py --sport NBA --output ud_nba.csv
  py -3 scripts/fetch_underdog_pickem.py --sport ALL --output outputs/DATE/underdog_props.csv \\
      --ladder-output outputs/DATE/books/underdog_props_ladder.csv --slate-date DATE
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
from typing import Any, Dict, List, Optional, Set, Tuple

import pandas as pd

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRIPT_DIR)
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from pickem_step1_schema import OUTPUT_COLUMNS  # noqa: E402
from utils.book_line_ladder import LADDER_COLUMNS, normalize_ladder, utc_now_iso  # noqa: E402

try:
    from curl_cffi.requests import Session as CurlSession

    def _new_session():
        return CurlSession(impersonate="chrome136")

except Exception:  # pragma: no cover
    import requests

    def _new_session():
        return requests.Session()


V1_URL = "https://api.underdogfantasy.com/v1/over_under_lines"
V3_ALT_URL = "https://api.underdogfantasy.com/v3/over_unders/{ou_id}/alternate_projections"

HEADERS_BASE = {
    "Accept": "application/json",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://underdogfantasy.com",
    "Referer": "https://underdogfantasy.com/",
}

SPORT_ALIASES: Dict[str, List[str]] = {
    "nba": ["NBA"],
    "nhl": ["NHL"],
    "mlb": ["MLB"],
    "cbb": ["CBB"],
    "wcbb": ["WCBB"],
    "wnba": ["WNBA"],
    "soccer": ["FIFA", "MASL", "UFL"],
    "ufl": ["UFL"],
    "esports": ["CS", "LOL", "VAL", "ESPORTS"],
    "tennis": ["TENNIS"],
    "nfl": ["NFL", "NFLP"],
    "cfb": ["CFB"],
    "golf": ["PGA", "GOLF"],
    "pga": ["PGA"],
}


def _norm_team(s: Any) -> str:
    return str(s or "").strip().upper()


def _parse_game_abbrs(game: dict) -> Tuple[str, str]:
    raw = (game.get("abbreviated_title") or game.get("title") or "").strip()
    raw = raw.split("(")[0].strip()
    if "@" not in raw:
        return "", ""
    away, home = raw.split("@", 1)
    return _norm_team(away), _norm_team(home)


def _player_team_abbr(game: dict, player_team_id: str) -> str:
    if not player_team_id:
        return ""
    hid = str(game.get("home_team_id") or "")
    aid = str(game.get("away_team_id") or "")
    away_abbr, home_abbr = _parse_game_abbrs(game)
    if player_team_id == hid:
        return home_abbr
    if player_team_id == aid:
        return away_abbr
    return ""


def _opp_team(player_abbr: str, home: str, away: str) -> str:
    if not player_abbr or not home or not away:
        return ""
    if player_abbr == home:
        return away
    if player_abbr == away:
        return home
    return ""


def _over_under_id(line: dict) -> str:
    return str(
        line.get("over_under_id")
        or (line.get("over_under") or {}).get("id")
        or ""
    ).strip()


def _higher_option(options: Any) -> Optional[dict]:
    for opt in options or []:
        if not isinstance(opt, dict):
            continue
        if str(opt.get("choice") or "").lower() == "higher":
            return opt
    return None


def _higher_payout_multiplier(line: dict) -> Optional[float]:
    hi = _higher_option(line.get("options"))
    if not hi:
        return None
    try:
        return float(hi.get("payout_multiplier"))
    except (TypeError, ValueError):
        return None


def _higher_american(line: dict) -> str:
    hi = _higher_option(line.get("options"))
    if not hi:
        return ""
    return str(hi.get("american_price") or "").strip()


def _pick_type(line: dict, balanced_value: object = None) -> str:
    """Map Underdog rungs onto PrizePicks pick_type labels.

    Balanced (~even Higher/Lower) → Standard.
    Alternate below the main line → Goblin (easier Over).
    Alternate above the main line → Demon (harder Over).
    """
    lt = str(line.get("line_type") or "").lower()
    sv = line.get("stat_value")
    if lt == "alternate":
        ref = balanced_value if balanced_value is not None else line.get("non_discounted_stat_value")
        try:
            alt = float(sv)
            bal = float(ref)
            if alt < bal - 1e-9:
                return "Goblin"
            if alt > bal + 1e-9:
                return "Demon"
        except (TypeError, ValueError):
            pass
        higher_mult = _higher_payout_multiplier(line)
        if higher_mult is not None and higher_mult < 0.99:
            return "Goblin"
        if higher_mult is not None and higher_mult > 1.01:
            return "Demon"
        return "Standard"
    nd = line.get("non_discounted_stat_value")
    try:
        if nd is not None and sv is not None and float(nd) != float(sv):
            return "Goblin"
    except (TypeError, ValueError):
        pass
    if "discount" in lt or lt == "boosted":
        return "Goblin"
    return "Standard"


def _balanced_line_by_over_under(payload: dict) -> Dict[str, object]:
    out: Dict[str, object] = {}
    for line in payload.get("over_under_lines") or []:
        if not isinstance(line, dict):
            continue
        lt = str(line.get("line_type") or "").lower()
        # v1 mains often omit line_type; treat active board lines as the balanced ref.
        if lt and lt not in ("balanced", "main", ""):
            continue
        ouid = _over_under_id(line)
        if ouid and ouid not in out:
            out[ouid] = line.get("stat_value")
    return out


def _expand_sports_arg(sports_csv: str) -> Optional[Set[str]]:
    s = sports_csv.strip()
    if not s or s.upper() == "ALL":
        return None
    out: Set[str] = set()
    for part in s.split(","):
        key = part.strip().lower()
        if not key:
            continue
        if key in SPORT_ALIASES:
            out.update(SPORT_ALIASES[key])
        else:
            out.add(key.upper())
    return out


def _fetch_payload(session, url: str, retries: int = 5, timeout: float = 120.0) -> dict:
    last_err: Optional[BaseException] = None
    for attempt in range(1, retries + 1):
        try:
            h = {**HEADERS_BASE, "User-Agent": f"Mozilla/5.0 Chrome/{120 + attempt}.0.0.0"}
            if attempt > 1:
                time.sleep(random.uniform(1.0, 3.0))
            r = session.get(url, headers=h, timeout=timeout)
            if r.status_code == 429:
                time.sleep(random.uniform(30.0, 60.0))
                continue
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(min(30.0, 2**attempt))
    raise RuntimeError(f"Underdog API failed after {retries} tries ({url}): {last_err}")


def _appearance_context(payload: dict, sport_filter: Optional[Set[str]]):
    games = {g["id"]: g for g in (payload.get("games") or [])}
    solo = {s["id"]: s for s in (payload.get("solo_games") or [])}
    players = {p["id"]: p for p in (payload.get("players") or [])}
    appearances = payload.get("appearances") or []

    def appearance_sport(app: dict) -> Optional[str]:
        mt = app.get("match_type")
        mid = app.get("match_id")
        if mt == "Game":
            g = games.get(mid)
            return (g or {}).get("sport_id")
        if mt == "SoloGame":
            sg = solo.get(mid)
            return (sg or {}).get("sport_id")
        return None

    allowed_app_ids: Set[str] = set()
    app_by_id: Dict[str, dict] = {}
    for app in appearances:
        aid = str(app.get("id") or "")
        if not aid:
            continue
        sid = appearance_sport(app)
        if sid is None:
            continue
        if sport_filter is None or sid in sport_filter:
            allowed_app_ids.add(aid)
            app_by_id[aid] = app
    return games, solo, players, allowed_app_ids, app_by_id


def _row_from_line(
    *,
    line: dict,
    ou: dict,
    ast: dict,
    app: dict,
    players: dict,
    games: dict,
    solo: dict,
    balanced_by_ou: Dict[str, object],
    line_type_override: str = "",
) -> Optional[dict]:
    pl = players.get(app.get("player_id")) or {}
    first = str(pl.get("first_name") or "").strip()
    last = str(pl.get("last_name") or "").strip()
    player_name = f"{first} {last}".strip()
    pos = str(pl.get("position_name") or pl.get("position_display_name") or "").strip()
    image_url = str(
        pl.get("image_url") or pl.get("light_image_url") or pl.get("dark_image_url") or ""
    ).strip()
    player_team_id = str(pl.get("team_id") or app.get("team_id") or "")

    mt = app.get("match_type")
    mid = app.get("match_id")
    ud_sport = ""
    start_time = ""
    home_abbr, away_abbr = "", ""
    pp_game_id = ""
    team_abbr = ""

    if mt == "Game":
        g = games.get(mid) or {}
        ud_sport = str(g.get("sport_id") or "")
        start_time = str(g.get("scheduled_at") or "").strip()
        pp_game_id = str(mid)
        away_abbr, home_abbr = _parse_game_abbrs(g)
        team_abbr = _player_team_abbr(g, player_team_id)
    elif mt == "SoloGame":
        sg = solo.get(mid) or {}
        ud_sport = str(sg.get("sport_id") or "")
        start_time = str(sg.get("scheduled_at") or "").strip()
        pp_game_id = f"solo_{mid}"
    else:
        return None

    opp = _opp_team(team_abbr, home_abbr, away_abbr) if team_abbr else ""
    prop_type = str(ast.get("display_stat") or ast.get("stat") or "").strip()
    stat_key = str(ast.get("stat") or "").strip()
    line_val = line.get("stat_value")
    ouid = _over_under_id(line) or str(ou.get("id") or "")
    lt = (line_type_override or str(line.get("line_type") or "balanced")).strip() or "balanced"
    line_id = str(line.get("stable_id") or line.get("id") or f"{ouid}|{lt}|{line_val}").strip()
    synth = dict(line)
    synth["line_type"] = lt
    return {
        "projection_id": line_id,
        "pp_projection_id": line_id,
        "player_id": str(pl.get("id") or "").strip(),
        "pp_game_id": pp_game_id,
        "start_time": start_time,
        "player": player_name,
        "pos": pos,
        "team": team_abbr,
        "opp_team": opp,
        "prop_type": prop_type,
        "line": line_val,
        "pick_type": _pick_type(synth, balanced_by_ou.get(ouid)),
        "pp_home_team": home_abbr,
        "pp_away_team": away_abbr,
        "image_url": image_url,
        "source_book": "underdog",
        "ud_sport_id": ud_sport,
        "ud_line_id": str(line.get("id") or "").strip(),
        "ud_stat_key": stat_key,
        "ud_line_type": lt if lt != "balanced" else ("alternate" if line_type_override == "alternate" else "balanced"),
        "american_odds": _higher_american(line),
        "payout_multiplier": _higher_payout_multiplier(line),
        "kind": "alt" if str(lt).lower() == "alternate" else "main",
        "over_under_id": ouid,
    }


def build_rows(payload: dict, sport_filter: Optional[Set[str]]) -> List[dict]:
    """Build PP-shaped rows from a v1 (or legacy) over_under_lines payload."""
    games, solo, players, allowed_app_ids, app_by_id = _appearance_context(payload, sport_filter)
    balanced_by_ou = _balanced_line_by_over_under(payload)
    rows: List[dict] = []
    for line in payload.get("over_under_lines") or []:
        if str(line.get("status") or "").lower() != "active":
            continue
        ou = line.get("over_under") or {}
        if str(ou.get("category") or "").lower() != "player_prop":
            continue
        ast = ou.get("appearance_stat") or {}
        aid = str(ast.get("appearance_id") or "").strip()
        if not aid or aid not in allowed_app_ids:
            continue
        app = app_by_id.get(aid)
        if not app:
            continue
        row = _row_from_line(
            line=line,
            ou=ou,
            ast=ast,
            app=app,
            players=players,
            games=games,
            solo=solo,
            balanced_by_ou=balanced_by_ou,
        )
        if row:
            rows.append(row)
    return rows


def fetch_alternate_rows(
    session,
    payload: dict,
    sport_filter: Optional[Set[str]],
    *,
    sleep_s: float = 0.04,
    max_alts: int = 0,
) -> List[dict]:
    """Pull v3 alternate_projections for over_unders flagged has_alternates."""
    games, solo, players, allowed_app_ids, app_by_id = _appearance_context(payload, sport_filter)
    balanced_by_ou = _balanced_line_by_over_under(payload)
    seen_ou: Set[str] = set()
    rows: List[dict] = []
    n_fetch = 0
    for line in payload.get("over_under_lines") or []:
        if str(line.get("status") or "").lower() != "active":
            continue
        ou = line.get("over_under") or {}
        if str(ou.get("category") or "").lower() != "player_prop":
            continue
        if not ou.get("has_alternates"):
            continue
        ouid = str(ou.get("id") or _over_under_id(line) or "").strip()
        if not ouid or ouid in seen_ou:
            continue
        ast = ou.get("appearance_stat") or {}
        aid = str(ast.get("appearance_id") or "").strip()
        if not aid or aid not in allowed_app_ids:
            continue
        app = app_by_id.get(aid)
        if not app:
            continue
        seen_ou.add(ouid)
        n_fetch += 1
        if max_alts and n_fetch > max_alts:
            break
        try:
            js = _fetch_payload(session, V3_ALT_URL.format(ou_id=ouid), retries=3, timeout=40.0)
        except Exception as e:
            print(f"  [underdog] WARN alt {ouid}: {e}")
            continue
        for pj in js.get("projections") or []:
            if not isinstance(pj, dict) or pj.get("is_main"):
                continue
            synth = {
                "id": str(pj.get("id") or f"{ouid}|alt|{pj.get('stat_value')}"),
                "stable_id": f"{ouid}|alternate|{pj.get('stat_value')}",
                "line_type": "alternate",
                "stat_value": pj.get("stat_value"),
                "status": "active",
                "over_under_id": ouid,
                "options": pj.get("options") or [],
            }
            row = _row_from_line(
                line=synth,
                ou=ou,
                ast=ast,
                app=app,
                players=players,
                games=games,
                solo=solo,
                balanced_by_ou=balanced_by_ou,
                line_type_override="alternate",
            )
            if row:
                rows.append(row)
        time.sleep(sleep_s)
    return rows


def rows_to_pp_frame(rows: List[dict]) -> pd.DataFrame:
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=OUTPUT_COLUMNS)
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    df = df.drop_duplicates(subset=["projection_id"], keep="first").reset_index(drop=True)
    for c in OUTPUT_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    return df[OUTPUT_COLUMNS]


def rows_to_ladder(
    rows: List[dict],
    *,
    slate_date: str,
    fetched_at: str,
) -> pd.DataFrame:
    if not rows:
        return normalize_ladder(pd.DataFrame(), book="underdog", slate_date=slate_date, fetched_at=fetched_at)
    raw = pd.DataFrame(
        [
            {
                "sport": r.get("ud_sport_id"),
                "player": r.get("player"),
                "prop": r.get("prop_type"),
                "line": r.get("line"),
                "american_odds": r.get("american_odds"),
                "payout_multiplier": r.get("payout_multiplier"),
                "kind": r.get("kind") or "main",
                "source_event_id": r.get("pp_game_id"),
                "side": "OVER",
            }
            for r in rows
        ]
    )
    return normalize_ladder(raw, book="underdog", slate_date=slate_date, fetched_at=fetched_at)


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch Underdog pick'em → PP-shaped CSV (+ ladder)")
    ap.add_argument(
        "--sport",
        default="ALL",
        help="Comma list or ALL. Aliases: nba,nhl,mlb,cbb,wcbb,wnba,soccer,ufl,esports,tennis",
    )
    ap.add_argument("--output", default="step1_underdog_props.csv")
    ap.add_argument("--ladder-output", default="", help="Optional shared-schema ladder CSV")
    ap.add_argument("--slate-date", default="", help="YYYY-MM-DD stamped on ladder rows")
    ap.add_argument("--cache-json", default="", help="Optional path to save raw v1 JSON")
    ap.add_argument("--skip-alts", action="store_true", help="Skip v3 alternate_projections")
    ap.add_argument("--max-alts", type=int, default=0, help="Cap alt over_under fetches (0=all)")
    ap.add_argument("--min-rows", type=int, default=1)
    args = ap.parse_args()

    filt = _expand_sports_arg(args.sport)
    print(f"[underdog] pick'em v1+v3 | sport={args.sport!r} | filter={filt or 'ALL'}")
    session = _new_session()
    fetched_at = utc_now_iso()
    slate_date = (args.slate_date or fetched_at[:10]).strip()

    try:
        payload = _fetch_payload(session, V1_URL)
    except Exception as e:
        print(f"[err] Fetch failed: {e}")
        pd.DataFrame(columns=OUTPUT_COLUMNS).to_csv(args.output, index=False, encoding="utf-8-sig")
        if args.ladder_output:
            os.makedirs(os.path.dirname(os.path.abspath(args.ladder_output)) or ".", exist_ok=True)
            normalize_ladder(pd.DataFrame(), book="underdog", slate_date=slate_date, fetched_at=fetched_at).to_csv(
                args.ladder_output, index=False, encoding="utf-8-sig"
            )
        sys.exit(1)

    if args.cache_json:
        try:
            with open(args.cache_json, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            print(f"[cache] Raw JSON -> {args.cache_json}")
        except OSError as e:
            print(f"  [WARN] cache-json: {e}")

    rows = build_rows(payload, filt)
    if not args.skip_alts:
        print("[underdog] fetching v3 alternate ladders…")
        alt_rows = fetch_alternate_rows(session, payload, filt, max_alts=args.max_alts)
        print(f"[underdog] +{len(alt_rows)} alternate rungs")
        rows.extend(alt_rows)

    df = rows_to_pp_frame(rows)
    if df.empty:
        print("[warn] No rows after filter - writing empty CSV")
    else:
        n_alt = int((df["ud_line_type"].astype(str).str.lower() == "alternate").sum())
        print(
            f"[underdog] {len(df)} rows ({n_alt} alternate rungs, "
            f"{int((df['pick_type'] == 'Goblin').sum())} Goblin, "
            f"{int((df['pick_type'] == 'Demon').sum())} Demon)"
        )

    out_dir = os.path.dirname(os.path.abspath(args.output))
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    df.to_csv(args.output, index=False, encoding="utf-8-sig")
    print(f"[ok] {len(df)} rows -> {args.output}")

    if args.ladder_output:
        ladder = rows_to_ladder(rows, slate_date=slate_date, fetched_at=fetched_at)
        lad_dir = os.path.dirname(os.path.abspath(args.ladder_output))
        if lad_dir:
            os.makedirs(lad_dir, exist_ok=True)
        ladder.to_csv(args.ladder_output, index=False, encoding="utf-8-sig")
        print(f"[ok] ladder {len(ladder)} rows -> {args.ladder_output} ({LADDER_COLUMNS[0]}…)")

    if len(df) < args.min_rows:
        sys.exit(1)


if __name__ == "__main__":
    main()
