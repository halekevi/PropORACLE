"""Compare payout-capture failing legs to day step1 boards + live PP partner API."""
from __future__ import annotations

import json
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import pandas as pd
import urllib.request

ROOT = Path(r"H:\PropORACLE_main_cp")
CAP = ROOT / "data" / "reports" / "payout_capture_2026-09-15.json"


def norm(s: object) -> str:
    s = str(s or "")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().replace(".", "").replace("'", "").replace("-", " ")
    s = re.sub(r"[^a-z0-9 ]+", "", s)
    return re.sub(r"\s+", " ", s).strip()


def prop_key(p: object) -> str:
    p = norm(p).replace(" ", "_")
    aliases = {
        "hitsrunsrbis": "hits_runs_rbis",
        "hits_runs_rbis": "hits_runs_rbis",
        "hitsrunsrbis": "hits_runs_rbis",
        "hrrbi": "hits_runs_rbis",
        "pitcher_strikeouts": "pitcher_strikeouts",
        "strikeouts": "pitcher_strikeouts",
        "hits_allowed": "hits_allowed",
        "pitching_outs": "pitching_outs",
        "walks_allowed": "walks_allowed",
        "total_bases": "total_bases",
        "shots": "shots",
        "shots_on_target": "shots_on_target",
        "total_games": "total_games",
        "total_games_won": "games_won",
        "games_won": "games_won",
        "points": "points",
        "receiving_yards": "receiving_yards",
    }
    return aliases.get(p, p)


def pick_col(df: pd.DataFrame, *names: str) -> str | None:
    cols = {c.lower(): c for c in df.columns}
    for n in names:
        if n in cols:
            return cols[n]
    for k, v in cols.items():
        if any(n in k for n in names):
            return v
    return None


def load_lookup(path: Path) -> tuple[dict, dict]:
    df = pd.read_csv(path, dtype=str, low_memory=False)
    pcol = pick_col(df, "player_name", "player", "name", "athlete")
    propcol = pick_col(df, "prop_type", "stat_type", "market", "prop")
    linecol = pick_col(df, "line", "projection", "value")
    pickcol = pick_col(df, "pick_type", "odds_type", "odds_type_name", "type")
    fetchcol = pick_col(df, "fetched_at", "fetched", "scrape_time")
    meta = {
        "path": str(path),
        "rows": len(df),
        "mtime": datetime.fromtimestamp(path.stat().st_mtime).isoformat(sep=" ", timespec="seconds"),
        "cols": [pcol, propcol, linecol, pickcol, fetchcol],
    }
    if fetchcol:
        vals = df[fetchcol].dropna().astype(str)
        meta["fetched_sample"] = vals.head(3).tolist()
        meta["fetched_nunique"] = int(vals.nunique())
    L: dict[tuple[str, str], list] = defaultdict(list)
    for _, r in df.iterrows():
        key = (norm(r.get(pcol)), prop_key(r.get(propcol)))
        try:
            line = float(r.get(linecol))
        except (TypeError, ValueError):
            line = None
        L[key].append(
            {
                "line": line,
                "pick": str(r.get(pickcol) or ""),
                "raw_prop": r.get(propcol),
            }
        )
    return L, meta


def classify(leg: dict, lookup: dict) -> tuple[str, list, list]:
    pk = (norm(leg.get("player")), prop_key(leg.get("prop_type")))
    cands = lookup.get(pk, [])
    try:
        want = float(leg.get("line"))
    except (TypeError, ValueError):
        want = None
    want_pick = norm(leg.get("pick_type"))
    exact = [
        c
        for c in cands
        if c["line"] is not None
        and want is not None
        and abs(c["line"] - want) < 1e-6
        and (not want_pick or want_pick in norm(c["pick"]))
    ]
    line_any = [
        c
        for c in cands
        if c["line"] is not None and want is not None and abs(c["line"] - want) < 1e-6
    ]
    if exact:
        st = "EXACT"
    elif line_any:
        st = "LINE_OK_PICK_DIFF"
    elif cands:
        st = "PROP_LINE_DRIFT"
    elif any(norm(leg.get("player")) == k[0] for k in lookup):
        st = "PLAYER_ONLY"
    else:
        st = "MISSING"
    lines = sorted({c["line"] for c in cands if c["line"] is not None})
    picks = sorted({c["pick"] for c in cands if c["pick"]})
    return st, lines, picks


def fetch_partner_league(league_id: str) -> list[dict]:
    url = f"https://partner-api.prizepicks.com/projections?league_id={league_id}&per_page=1000"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "PropORACLE-board-check/1.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    rows = []
    included = {str(x.get("id")): x for x in (data.get("included") or []) if isinstance(x, dict)}
    for proj in data.get("data") or []:
        if not isinstance(proj, dict):
            continue
        attrs = proj.get("attributes") or {}
        rel = ((proj.get("relationships") or {}).get("new_player") or {}).get("data") or {}
        pid = str(rel.get("id") or "")
        player = ((included.get(pid) or {}).get("attributes") or {}).get("name") or attrs.get(
            "name"
        )
        rows.append(
            {
                "player": player,
                "prop": attrs.get("stat_type") or attrs.get("projection_type"),
                "line": attrs.get("line_score") or attrs.get("line"),
                "odds": attrs.get("odds_type") or attrs.get("odds_type_name"),
                "start": attrs.get("start_time"),
                "status": attrs.get("status"),
            }
        )
    return rows


def main() -> None:
    cap = json.loads(CAP.read_text(encoding="utf-8"))
    slips = [s for s in (cap.get("slips") or []) if isinstance(s, dict)]
    ats = [s.get("captured_at") for s in slips if s.get("captured_at")]
    print("CAPTURE")
    print("  generated_at UTC:", cap.get("generated_at"))
    print("  slip captured_at:", min(ats) if ats else None, "->", max(ats) if ats else None)
    print("  summary:", cap.get("summary"))

    files = {
        "MLB": ROOT / "outputs" / "2026-09-15" / "mlb" / "step1_mlb_props.csv",
        "SOCCER": ROOT / "outputs" / "2026-09-15" / "soccer" / "step1_soccer_props.csv",
        "TENNIS": ROOT / "outputs" / "2026-09-15" / "tennis" / "step1_tennis_props.csv",
        "NFL": ROOT / "outputs" / "2026-09-15" / "nfl" / "step1_pp_props_today.csv",
    }
    lookups: dict[str, dict] = {}
    print("\nDAY STEP1 BOARDS")
    for sport, path in files.items():
        L, meta = load_lookup(path)
        lookups[sport] = L
        print(
            f"  {sport}: rows={meta['rows']} mtime={meta['mtime']} "
            f"fetched_nunique={meta.get('fetched_nunique')} sample={meta.get('fetched_sample')}"
        )

    seen = set()
    uniq = []
    for s in slips:
        for leg in s.get("legs") or []:
            k = (
                leg.get("sport"),
                norm(leg.get("player")),
                prop_key(leg.get("prop_type")),
                str(leg.get("line")),
                str(leg.get("pick_type")),
            )
            if k in seen:
                continue
            seen.add(k)
            uniq.append(leg)

    print("\nFAILING LEGS vs day step1 (ticket/scrape-era board)")
    counts: Counter[str] = Counter()
    by_sport: dict[str, list] = defaultdict(list)
    for leg in sorted(uniq, key=lambda x: (str(x.get("sport")), str(x.get("player")))):
        sport = str(leg.get("sport") or "").upper()
        if sport == "NBA1Q":
            st, lines, picks = "NO_STEP1_CHECK", [], []
        else:
            st, lines, picks = classify(leg, lookups.get(sport, {}))
        counts[st] += 1
        by_sport[sport].append((st, leg, lines, picks))
        print(
            f"  {st:16} {sport:7} {leg.get('player')} | {leg.get('prop_type')} "
            f"{leg.get('line')} {leg.get('pick_type')}  board_lines={lines[:8]} picks={picks[:5]}"
        )
    print("COUNTS", dict(counts))

    # Per-slip: would all legs still have been clickable on day board?
    print("\nPER-SLIP day-board completeness")
    slip_st = Counter()
    for s in slips:
        legs = s.get("legs") or []
        statuses = []
        for leg in legs:
            sport = str(leg.get("sport") or "").upper()
            if sport == "NBA1Q":
                statuses.append("NO_STEP1_CHECK")
            else:
                statuses.append(classify(leg, lookups.get(sport, {}))[0])
        if all(x == "EXACT" for x in statuses):
            tag = "ALL_EXACT"
        elif all(x in ("EXACT", "LINE_OK_PICK_DIFF", "PROP_LINE_DRIFT") for x in statuses):
            tag = "ALL_ON_BOARD_MAYBE_DRIFT"
        else:
            tag = "HAS_MISSING"
        slip_st[tag] += 1
        if tag != "ALL_EXACT":
            print(
                f"  {tag} | {s.get('ticket_id')} err={s.get('error')} leg_status={statuses}"
            )
    print("SLIP COUNTS", dict(slip_st))

    # Live partner API probe for key leagues
    print("\nLIVE PARTNER-API PROBE (now)")
    league_ids = {
        "MLB": "2",
        # soccer often multi-league; try common
        "SOCCER_EPL": "82",
        "TENNIS": "5",  # may be wrong; resolve via leagues
    }
    try:
        req = urllib.request.Request(
            "https://partner-api.prizepicks.com/leagues",
            headers={"User-Agent": "PropORACLE-board-check/1.0", "Accept": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=30) as resp:
            leagues = json.loads(resp.read().decode("utf-8"))
        rows = leagues.get("data") or leagues if isinstance(leagues, dict) else leagues
        name_hits = []
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            attrs = row.get("attributes") or row
            name = str(attrs.get("name") or attrs.get("league") or "")
            lid = str(row.get("id") or attrs.get("id") or "")
            low = name.lower()
            if any(
                x in low
                for x in (
                    "mlb",
                    "soccer",
                    "epl",
                    "premier",
                    "la liga",
                    "serie a",
                    "bundesliga",
                    "ligue",
                    "tennis",
                    "atp",
                    "wta",
                    "nfl",
                    "nba",
                )
            ):
                name_hits.append((name, lid))
        print("  league name hits:")
        for name, lid in name_hits[:40]:
            print(f"    {lid:>6} {name}")
    except Exception as exc:
        print("  leagues fetch failed:", exc)
        name_hits = []

    # Resolve ids from hits
    resolved = {}
    for name, lid in name_hits:
        low = name.lower()
        if low == "mlb" or low.startswith("mlb "):
            resolved.setdefault("MLB", lid)
        if "atp" in low or "wta" in low or low == "tennis":
            resolved.setdefault("TENNIS", lid)
        if any(x in low for x in ("premier", "epl", "la liga", "serie a", "bundesliga", "ligue 1", "soccer")):
            resolved.setdefault(f"SOCCER:{name}", lid)
        if low == "nfl":
            resolved.setdefault("NFL", lid)

    live_lookups: dict[str, dict] = {}
    for label, lid in sorted(resolved.items()):
        try:
            rows = fetch_partner_league(lid)
        except Exception as exc:
            print(f"  {label} id={lid} FAIL {exc}")
            continue
        L: dict[tuple[str, str], list] = defaultdict(list)
        for r in rows:
            key = (norm(r.get("player")), prop_key(r.get("prop")))
            try:
                line = float(r.get("line"))
            except (TypeError, ValueError):
                line = None
            L[key].append({"line": line, "pick": str(r.get("odds") or ""), "start": r.get("start")})
        live_lookups[label] = L
        print(f"  {label} id={lid} projections={len(rows)} unique_player_props={len(L)}")

    print("\nFAILING LEGS vs LIVE partner API (now)")
    live_counts: Counter[str] = Counter()
    for leg in sorted(uniq, key=lambda x: (str(x.get("sport")), str(x.get("player")))):
        sport = str(leg.get("sport") or "").upper()
        if sport == "MLB":
            keys = ["MLB"]
        elif sport == "TENNIS":
            keys = [k for k in live_lookups if k.startswith("TENNIS") or "ATP" in k or "WTA" in k]
            if not keys:
                keys = [k for k in live_lookups if "TENNIS" in k.upper() or "ATP" in k or "WTA" in k]
            # also try any tennis-ish
            keys = [k for k in live_lookups if "TENNIS" in k or "ATP" in k.upper() or "WTA" in k.upper() or k == "TENNIS"]
            if not keys:
                keys = list(live_lookups.keys())
        elif sport == "SOCCER":
            keys = [k for k in live_lookups if k.startswith("SOCCER")]
        elif sport == "NFL":
            keys = ["NFL"]
        else:
            keys = []
        merged: dict = {}
        for k in keys:
            for kk, vv in (live_lookups.get(k) or {}).items():
                merged.setdefault(kk, []).extend(vv)
        if not keys:
            st, lines, picks = "NO_LIVE_LEAGUE", [], []
        else:
            st, lines, picks = classify(leg, merged)
        live_counts[st] += 1
        print(
            f"  {st:16} {sport:7} {leg.get('player')} | {leg.get('prop_type')} "
            f"{leg.get('line')} {leg.get('pick_type')}  live_lines={lines[:8]} picks={picks[:5]}"
        )
    print("LIVE COUNTS", dict(live_counts))


if __name__ == "__main__":
    main()
