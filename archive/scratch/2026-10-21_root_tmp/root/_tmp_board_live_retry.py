"""Retry live PP board checks for soccer/tennis/NBA1Q with backoff; note game starts."""
from __future__ import annotations

import json
import re
import time
import unicodedata
import urllib.request
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

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


def fetch(league_id: str) -> dict:
    url = f"https://partner-api.prizepicks.com/projections?league_id={league_id}&per_page=1000"
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "PropORACLE-board-check/1.0", "Accept": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.loads(resp.read().decode("utf-8"))


def to_lookup(data: dict) -> dict[tuple[str, str], list]:
    included = {str(x.get("id")): x for x in (data.get("included") or []) if isinstance(x, dict)}
    L: dict[tuple[str, str], list] = defaultdict(list)
    n = 0
    for proj in data.get("data") or []:
        if not isinstance(proj, dict):
            continue
        attrs = proj.get("attributes") or {}
        rel = ((proj.get("relationships") or {}).get("new_player") or {}).get("data") or {}
        pid = str(rel.get("id") or "")
        player = ((included.get(pid) or {}).get("attributes") or {}).get("name") or attrs.get("name")
        try:
            line = float(attrs.get("line_score") if attrs.get("line_score") is not None else attrs.get("line"))
        except (TypeError, ValueError):
            line = None
        key = (norm(player), prop_key(attrs.get("stat_type") or attrs.get("projection_type")))
        L[key].append(
            {
                "line": line,
                "pick": str(attrs.get("odds_type") or attrs.get("odds_type_name") or ""),
                "start": attrs.get("start_time"),
                "status": attrs.get("status"),
                "is_live": attrs.get("is_live"),
            }
        )
        n += 1
    return L, n


def classify(leg, lookup):
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
    if exact:
        st = "EXACT"
        sample = exact[0]
    elif any(c["line"] is not None and want is not None and abs(c["line"] - want) < 1e-6 for c in cands):
        st = "LINE_OK_PICK_DIFF"
        sample = next(c for c in cands if c["line"] is not None and abs(c["line"] - want) < 1e-6)
    elif cands:
        st = "PROP_LINE_DRIFT"
        sample = cands[0]
    elif any(norm(leg.get("player")) == k[0] for k in lookup):
        st = "PLAYER_ONLY"
        sample = None
    else:
        st = "MISSING"
        sample = None
    lines = sorted({c["line"] for c in cands if c["line"] is not None})
    return st, lines, sample, cands


def main():
    cap = json.loads(CAP.read_text(encoding="utf-8"))
    uniq = []
    seen = set()
    for s in cap.get("slips") or []:
        for leg in s.get("legs") or []:
            k = (leg.get("sport"), norm(leg.get("player")), prop_key(leg.get("prop_type")), str(leg.get("line")), str(leg.get("pick_type")))
            if k in seen:
                continue
            seen.add(k)
            uniq.append(leg)

    targets = [
        ("MLB", "2"),
        ("TENNIS", "5"),
        ("SOCCER", "82"),
        ("EPL", "14"),
        ("NBA1Q", "192"),
        ("NFL", "9"),
    ]
    lookups = {}
    for label, lid in targets:
        for attempt in range(4):
            try:
                time.sleep(2.5 + attempt * 2)
                data = fetch(lid)
                L, n = to_lookup(data)
                lookups[label] = L
                print(f"OK {label} id={lid} projections={n} keys={len(L)}")
                break
            except Exception as exc:
                print(f"FAIL {label} attempt={attempt+1} {exc}")
                time.sleep(3)

    sport_to_labels = {
        "MLB": ["MLB"],
        "TENNIS": ["TENNIS"],
        "SOCCER": ["SOCCER", "EPL"],
        "NBA1Q": ["NBA1Q"],
        "NFL": ["NFL"],
    }

    print("\nNOW", datetime.now().astimezone().isoformat(timespec="seconds"))
    print("FAILING LEG LIVE STATUS")
    for leg in sorted(uniq, key=lambda x: (str(x.get("sport")), str(x.get("player")))):
        sport = str(leg.get("sport") or "").upper()
        merged = {}
        for lab in sport_to_labels.get(sport, []):
            for kk, vv in (lookups.get(lab) or {}).items():
                merged.setdefault(kk, []).extend(vv)
        if sport in sport_to_labels and not any(lookups.get(x) for x in sport_to_labels[sport]):
            print(f"  NO_DATA          {sport:7} {leg.get('player')} | {leg.get('prop_type')} {leg.get('line')} {leg.get('pick_type')}")
            continue
        st, lines, sample, cands = classify(leg, merged)
        start = sample.get("start") if sample else (cands[0].get("start") if cands else None)
        status = sample.get("status") if sample else None
        is_live = sample.get("is_live") if sample else None
        print(
            f"  {st:16} {sport:7} {leg.get('player')} | {leg.get('prop_type')} "
            f"{leg.get('line')} {leg.get('pick_type')}  live_lines={lines[:8]} start={start} status={status} is_live={is_live}"
        )

    # slip-level live completeness for Force re-run advice
    print("\nPER-SLIP LIVE COMPLETENESS")
    for s in cap.get("slips") or []:
        statuses = []
        for leg in s.get("legs") or []:
            sport = str(leg.get("sport") or "").upper()
            merged = {}
            for lab in sport_to_labels.get(sport, []):
                for kk, vv in (lookups.get(lab) or {}).items():
                    merged.setdefault(kk, []).extend(vv)
            if sport in sport_to_labels and not any(lookups.get(x) for x in sport_to_labels[sport]):
                statuses.append("NO_DATA")
            else:
                statuses.append(classify(leg, merged)[0])
        ok = all(x == "EXACT" for x in statuses)
        tag = "READY" if ok else "NOT_READY"
        print(f"  {tag:9} {s.get('error'):28} {s.get('ticket_id')} -> {statuses}")


if __name__ == "__main__":
    main()
