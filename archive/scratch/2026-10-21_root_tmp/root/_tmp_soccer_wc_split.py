"""Split May/June Soccer ticket_eval tickets by competition proxy (WC vs domestic).

graded_props lack league; step8 archives for June are thin. Use:
  1) team-name national-team heuristics + ESPN fifa.world roster if present
  2) any historical step1/step8 with league when joinable
"""
from __future__ import annotations

import json
import re
import sys
import unicodedata
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:/PropORACLE")
sys.stdout.reconfigure(encoding="utf-8", errors="replace")

TE_ARTICLE = re.compile(
    r'<article class="ticket-card([^"]*)">(.*?)</article>',
    re.S | re.I,
)


def _fold(s: object) -> str:
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def money_hit(cls: str, body: str) -> bool | None:
    if re.search(r"grade-ticket-result\s+won", body, re.I):
        return True
    if re.search(r"grade-ticket-result\s+lost", body, re.I):
        return False
    if re.search(r"MIN\s*GUARANTEE", body, re.I):
        return True
    low = (cls or "").lower()
    if "all-hit" in low:
        return True
    if "card-missed" in low:
        return False
    return None


def parse_soccer_legs(body: str) -> list[dict]:
    legs = []
    for lm in re.finditer(
        r'<div class="legrow([^"]*)">(.*?)(?=<div class="legrow|<(?:div class="ticket-grade|/article))',
        body,
        re.S | re.I,
    ):
        row, b = lm.group(1), lm.group(2)
        if "leg-pend" in row.lower() or "leg-void" in row.lower():
            continue
        sp = re.search(r'<span class="pill[^"]*">([^<]+)</span>', b, re.I)
        sport = (sp.group(1) if sp else "").strip().upper()
        if sport != "SOCCER":
            continue
        pm = re.search(
            r'class="pl-(?:hit|miss|void|pend)[^"]*"[^>]*>(.*?)</div>',
            b,
            re.S | re.I,
        )
        player = re.sub(r"<[^>]+>", " ", pm.group(1) if pm else "")
        player = re.sub(r"\s+", " ", player).strip()
        player = re.sub(r"\bMISSED\b", "", player, flags=re.I).strip()
        prop_m = re.search(
            r'<div class="leg-prop-col[^"]*">\s*<div>(.*?)</div>\s*<div class="meta-muted">(.*?)</div>',
            b,
            re.S | re.I,
        )
        prop = re.sub(r"<[^>]+>", " ", prop_m.group(1) if prop_m else "")
        prop = re.sub(r"\s+", " ", prop).strip()
        matchup = re.sub(r"<[^>]+>", " ", prop_m.group(2) if prop_m else "")
        matchup = re.sub(r"\s+", " ", matchup).strip()
        parts = re.split(r"\s+vs\.?\s+|\s+@\s+", matchup, maxsplit=1, flags=re.I)
        team = parts[0].strip() if parts else ""
        opp = parts[1].strip() if len(parts) > 1 else ""
        hit = True if "leg-hit" in row.lower() else False if "leg-miss" in row.lower() else None
        legs.append(
            {
                "player": player,
                "prop": prop,
                "team": team,
                "opp": opp,
                "matchup": matchup,
                "hit": hit,
            }
        )
    return legs


# --- national team name set ---
NATIONAL_ALIASES = {
    # common PP / ESPN short labels
    "usa", "united states", "usmnt", "uswnt", "mexico", "canada",
    "brazil", "argentina", "france", "germany", "spain", "england",
    "portugal", "netherlands", "holland", "italy", "belgium", "croatia",
    "uruguay", "colombia", "chile", "ecuador", "peru", "paraguay",
    "japan", "korea", "south korea", "korea republic", "australia",
    "morocco", "senegal", "nigeria", "ghana", "cameroon", "egypt",
    "algeria", "tunisia", "ivory coast", "cote d ivoire", "mali",
    "saudi arabia", "iran", "iraq", "qatar", "uae", "jordan",
    "poland", "switzerland", "austria", "serbia", "denmark", "sweden",
    "norway", "scotland", "wales", "ireland", "republic of ireland",
    "northern ireland", "czech republic", "czechia", "slovakia",
    "hungary", "romania", "ukraine", "turkey", "turkiye", "greece",
    "russia", "china", "new zealand", "costa rica", "panama",
    "jamaica", "honduras", "el salvador", "haiti", "trinidad",
    "venezuela", "bolivia", "south africa", "dr congo", "congo",
    "cape verde", "burkina faso", "guinea", "uganda", "zambia",
    "uzbekistan", "iraq", "palestine", "syria", "lebanon",
    "north macedonia", "bosnia", "slovenia", "albania", "montenegro",
    "iceland", "finland", "georgia", "armenia", "azerbaijan",
    "israel", "kosovo", "luxembourg", "cyprus", "malta",
}


def load_national_teams() -> set[str]:
    names = set(NATIONAL_ALIASES)
    # Prefer ESPN fifa.world roster / team lists if cached
    for p in [
        ROOT / "Sports/Soccer/soccer_roster_cache.csv",
        ROOT / "Sports/Soccer/soccer_espn_id_cache.csv",
        ROOT / "Sports/Soccer/soccer_roster_cache.csv.leagues",
    ]:
        if not p.exists():
            continue
        if p.name.endswith(".leagues"):
            print(f"[nat] leagues file: {p.read_text(encoding='utf-8')[:200]!r}")
            continue
        try:
            df = pd.read_csv(p, low_memory=False)
        except Exception as e:
            print(f"[nat] skip {p.name}: {e}")
            continue
        cols = {c.lower(): c for c in df.columns}
        print(f"[nat] {p.name} cols={list(df.columns)[:15]} n={len(df)}")
        # if league column marks fifa.world
        league_col = cols.get("league") or cols.get("league_slug") or cols.get("espn_league")
        team_col = cols.get("team") or cols.get("team_name") or cols.get("pp_name") or cols.get("name")
        if league_col and team_col:
            mask = df[league_col].astype(str).str.lower().str.contains("fifa|world", regex=True)
            for t in df.loc[mask, team_col].astype(str):
                names.add(_fold(t))
        elif team_col and "fifa" in p.name.lower():
            for t in df[team_col].astype(str):
                names.add(_fold(t))
    return names


CLUB_MARKERS = (
    " fc", " cf", " sc", " united", " city", " town", " athletic", " atletico",
    " real ", " sporting", " rangers", " celtic", " arsenal", " chelsea",
    " liverpool", " tottenham", " barcelona", " madrid", " bayern", " dortmund",
    " juventus", " milan", " inter", " napoli", " roma", " lazio", " ajax",
    " psv", " feyenoord", " porto", " benfica", " sporting", " galaxy",
    " sounders", " crew", " fire", " rapid", " revolution", " dallas",
    " austin", " miami", " orlando", " cincinnati", " charlotte", " nashville",
    " salt lake", " colorado", " vancouver", " montreal", " toronto",
    " dash", " reign", " thistle", " courage", " current", " wave", " angel city",
    " pride", " red stars", " spirit", " Gotham", " bay fc",
)


def classify_side(name: str, nationals: set[str]) -> str:
    f = _fold(name)
    if not f:
        return "unknown"
    if f in nationals:
        return "national"
    # club markers
    padded = f" {f} "
    if any(m.strip() in padded or f.endswith(m.strip()) for m in CLUB_MARKERS):
        return "club"
    # short country-like single token often national in PP soccer
    if f in nationals or f.replace(" ", "") in {x.replace(" ", "") for x in nationals}:
        return "national"
    return "unknown"


def classify_ticket(legs: list[dict], nationals: set[str]) -> str:
    labels = []
    for leg in legs:
        labels.append(classify_side(leg.get("team") or "", nationals))
        labels.append(classify_side(leg.get("opp") or "", nationals))
    if any(x == "national" for x in labels):
        return "world_cup_or_intl"
    if any(x == "club" for x in labels) and not any(x == "national" for x in labels):
        return "domestic_club"
    return "unknown"


def try_league_join() -> dict[tuple, str]:
    """Map (fold player, fold prop, fold team) -> league from any available step files."""
    out: dict[tuple, str] = {}
    paths: list[Path] = []
    paths += list(ROOT.glob("**/step1*soccer*.csv"))
    paths += list(ROOT.glob("**/step8*soccer*.csv"))
    paths += list(ROOT.glob("**/s1_soccer*.csv"))
    paths += list((ROOT / "data/historical").glob("**/*soccer*.csv"))
    seen = set()
    for p in paths:
        key = str(p.resolve())
        if key in seen:
            continue
        seen.add(key)
        try:
            df = pd.read_csv(p, low_memory=False, nrows=0)
        except Exception:
            continue
        cols = {c.lower(): c for c in df.columns}
        if "league" not in cols:
            continue
        try:
            df = pd.read_csv(p, low_memory=False)
        except Exception:
            continue
        player_c = cols.get("player") or cols.get("player_name")
        prop_c = cols.get("prop_type") or cols.get("prop") or cols.get("prop_norm")
        team_c = cols.get("team") or cols.get("team_name")
        league_c = cols["league"]
        if not player_c:
            continue
        n = 0
        for r in df.itertuples(index=False):
            league = str(getattr(r, league_c)).upper()
            if not league or league in {"NAN", "NONE"}:
                continue
            player = _fold(getattr(r, player_c, ""))
            prop = _fold(getattr(r, prop_c, "")) if prop_c else ""
            team = _fold(getattr(r, team_c, "")) if team_c else ""
            if not player:
                continue
            out[(player, prop, team)] = league
            out[(player, "", team)] = league
            out[(player, prop, "")] = league
            n += 1
        print(f"[league-join] {p.relative_to(ROOT)} rows_used~{n} map_keys={len(out)}")
    return out


def main() -> None:
    nationals = load_national_teams()
    print(f"national name set size={len(nationals)}")
    league_map = try_league_join()
    print(f"league join keys={len(league_map)}")

    # Collect soccer-only tickets from TE
    by = defaultdict(lambda: defaultdict(lambda: {"n": 0, "w": 0}))
    team_examples = defaultdict(Counter)
    unknown_teams = Counter()
    league_direct = defaultdict(lambda: defaultdict(lambda: {"n": 0, "w": 0}))

    for fp in sorted((ROOT / "ui_runner/templates").glob("ticket_eval_2026-0[56]-*.html")):
        if not re.match(r"ticket_eval_2026-0[56]-\d{2}\.html$", fp.name):
            continue
        d = fp.name[len("ticket_eval_") : -5]
        month = d[:7]
        text = fp.read_text(encoding="utf-8", errors="replace")
        cut = text.find('class="ticket-bucket sb-default manual-tb')
        if cut > 0:
            text = text[:cut]
        for am in TE_ARTICLE.finditer(text):
            mh = money_hit(am.group(1), am.group(2))
            if mh is None:
                continue
            legs = parse_soccer_legs(am.group(2))
            if len(legs) < 2:
                continue
            # all-soccer ticket (legs list already soccer-only; require >=2)
            bucket = classify_ticket(legs, nationals)
            by[month][bucket]["n"] += 1
            if mh:
                by[month][bucket]["w"] += 1
            for leg in legs:
                for side in (leg["team"], leg["opp"]):
                    lab = classify_side(side, nationals)
                    team_examples[lab][side] += 1
                    if lab == "unknown" and side:
                        unknown_teams[side] += 1

            # optional league join on first leg
            leagues_found = []
            for leg in legs:
                key1 = (_fold(leg["player"]), _fold(leg["prop"]), _fold(leg["team"]))
                key2 = (_fold(leg["player"]), "", _fold(leg["team"]))
                key3 = (_fold(leg["player"]), _fold(leg["prop"]), "")
                lg = league_map.get(key1) or league_map.get(key2) or league_map.get(key3)
                if lg:
                    leagues_found.append(lg)
            if leagues_found:
                # if any WORLDCUP*
                if any(str(x).upper().startswith("WORLDCUP") for x in leagues_found):
                    lg_bucket = "WORLDCUP*"
                else:
                    lg_bucket = "DOMESTIC*"
                league_direct[month][lg_bucket]["n"] += 1
                if mh:
                    league_direct[month][lg_bucket]["w"] += 1

    print("\n=== Soccer tickets by national/club heuristic ===")
    print(f"{'month':8} {'bucket':20} {'n':>5} {'w':>5} {'WR%':>6}")
    for month in sorted(by):
        for bucket in sorted(by[month]):
            s = by[month][bucket]
            wr = 100 * s["w"] / s["n"] if s["n"] else 0
            print(f"{month:8} {bucket:20} {s['n']:5d} {s['w']:5d} {wr:6.1f}")

    print("\n=== League-join buckets (when step file match) ===")
    if not any(league_direct[m] for m in league_direct):
        print("(no joins — league field not recoverable onto TE legs from available CSVs)")
    else:
        for month in sorted(league_direct):
            for bucket, s in sorted(league_direct[month].items()):
                wr = 100 * s["w"] / s["n"] if s["n"] else 0
                print(f"{month:8} {bucket:12} {s['n']:5d} {s['w']:5d} {wr:6.1f}")

    print("\n=== Classification examples ===")
    for lab in ("national", "club", "unknown"):
        print(f"{lab}: {team_examples[lab].most_common(15)}")

    print("\n=== Top unknown teams (need review) ===")
    print(unknown_teams.most_common(30))

    # Weekly soccer WR through June for calendar alignment
    print("\n=== Soccer ticket WR by week ===")
    week = defaultdict(lambda: {"n": 0, "w": 0})
    for fp in sorted((ROOT / "ui_runner/templates").glob("ticket_eval_2026-0[56]-*.html")):
        if not re.match(r"ticket_eval_2026-0[56]-\d{2}\.html$", fp.name):
            continue
        d = fp.name[len("ticket_eval_") : -5]
        text = fp.read_text(encoding="utf-8", errors="replace")
        cut = text.find('class="ticket-bucket sb-default manual-tb')
        if cut > 0:
            text = text[:cut]
        for am in TE_ARTICLE.finditer(text):
            mh = money_hit(am.group(1), am.group(2))
            if mh is None:
                continue
            legs = parse_soccer_legs(am.group(2))
            if len(legs) < 2:
                continue
            from datetime import date as _date

            dt = _date.fromisoformat(d)
            iso = dt.isocalendar()
            key = f"{iso.year}-W{iso.week:02d}"
            week[key]["n"] += 1
            if mh:
                week[key]["w"] += 1
    for k in sorted(week):
        s = week[k]
        print(f"{k}: n={s['n']:4d} WR={100*s['w']/s['n']:5.1f}%")


if __name__ == "__main__":
    main()
