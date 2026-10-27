"""Multi-sport injury collectors → timestamped snapshots under data/injuries/.

Never overwrite: each run writes <sport>_<HHMM>.json. injuries_latest.csv is
the only pointer that may be replaced.
"""
from __future__ import annotations

import csv
import io
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from utils.injury_gate import normalize_inj_status

ET = ZoneInfo("America/New_York")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; PropORACLE/1.0)",
    "Accept": "application/json,application/pdf,*/*",
}
TIMEOUT = 28
NBA_PDF_BASE = "https://ak-static.cms.nba.com/referee/injury/Injury-Report_{date}_{slot}.pdf"
NFLVERSE_INJURIES = (
    "https://github.com/nflverse/nflverse-data/releases/download/injuries/injuries.csv"
)
ESPN_LEAGUE_INJURIES = {
    "NBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/injuries",
    "NHL": "https://site.api.espn.com/apis/site/v2/sports/hockey/nhl/injuries",
    "CFB": "https://site.api.espn.com/apis/site/v2/sports/football/college-football/injuries",
    "CBB": "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/injuries",
    "NFL": "https://site.api.espn.com/apis/site/v2/sports/football/nfl/injuries",
    "WNBA": "https://site.api.espn.com/apis/site/v2/sports/basketball/wnba/injuries",
    "MLB": "https://site.api.espn.com/apis/site/v2/sports/baseball/mlb/injuries",
}
SOCCER_LEAGUES = (
    "usa.1",
    "eng.1",
    "esp.1",
    "ger.1",
    "ita.1",
    "fra.1",
    "uefa.champions",
    "fifa.world",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def injuries_dir(date_str: str, *, root: Path | None = None) -> Path:
    return (root or repo_root()) / "data" / "injuries" / date_str[:10]


def latest_csv_path(*, root: Path | None = None) -> Path:
    return (root or repo_root()) / "data" / "injuries" / "injuries_latest.csv"


def now_et() -> datetime:
    return datetime.now(ET)


def captured_at_iso(now: datetime | None = None) -> str:
    dt = now or datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def hhmm_et(now: datetime | None = None) -> str:
    return (now or now_et()).strftime("%H%M")


def _row(
    *,
    sport: str,
    player: str,
    team: str,
    status: str,
    injury: str,
    source: str,
    captured_at: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    canon = normalize_inj_status(status) or normalize_inj_status(injury) or str(status or "").strip()
    out = {
        "player": str(player or "").strip(),
        "team": str(team or "").strip(),
        "status": canon or str(status or "").strip(),
        "injury": str(injury or "").strip(),
        "source": source,
        "captured_at": captured_at,
        "sport": sport.upper(),
    }
    if extra:
        out.update(extra)
    return out


def _parse_espn_injuries_payload(data: dict, sport: str, source: str, captured_at: str) -> list[dict]:
    rows: list[dict] = []
    for block in data.get("injuries") or []:
        if not isinstance(block, dict):
            continue
        team_obj = block.get("team") or {}
        team = str(
            team_obj.get("abbreviation")
            or block.get("abbreviation")
            or block.get("displayName")
            or ""
        ).strip()
        for inj in block.get("injuries") or []:
            if not isinstance(inj, dict):
                continue
            ath = inj.get("athlete") or {}
            name = str(ath.get("displayName") or ath.get("fullName") or "").strip()
            if not name:
                continue
            typ = inj.get("type") or {}
            status = str(inj.get("status") or typ.get("description") or typ.get("abbreviation") or "").strip()
            detail = str(
                inj.get("shortComment")
                or (inj.get("details") or {}).get("detail")
                or typ.get("description")
                or ""
            ).strip()
            rows.append(
                _row(
                    sport=sport,
                    player=name,
                    team=team,
                    status=status,
                    injury=detail,
                    source=source,
                    captured_at=captured_at,
                )
            )
    return rows


def fetch_espn_league_injuries(sport: str, captured_at: str) -> list[dict]:
    lit = sport.upper()
    url = ESPN_LEAGUE_INJURIES.get(lit)
    if not url:
        return []
    try:
        r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
        if r.status_code != 200:
            return []
        return _parse_espn_injuries_payload(r.json(), lit, f"espn_{lit.lower()}", captured_at)
    except Exception:
        return []


def _nba_pdf_slots(now: datetime) -> list[str]:
    """Probe recent 15-min ET slots (newest first)."""
    slots: list[str] = []
    cur = now.replace(second=0, microsecond=0)
    minute = (cur.minute // 15) * 15
    cur = cur.replace(minute=minute)
    for i in range(0, 96):  # 24h of 15-min slots
        t = cur - timedelta(minutes=15 * i)
        # Injury-Report_YYYY-MM-DD_05_30PM.pdf
        slots.append(t.strftime("%I_%M%p").lstrip("0").replace("AM", "AM").replace("PM", "PM"))
        # Also try hour-only legacy: 05PM
        if t.minute == 0:
            slots.append(t.strftime("%I%p").lstrip("0"))
    # unique preserve order
    seen: set[str] = set()
    out: list[str] = []
    for s in slots:
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out


def _parse_nba_injury_pdf_text(text: str, captured_at: str) -> list[dict]:
    """Best-effort parse of official NBA injury PDF text layout."""
    rows: list[dict] = []
    # "Last, First Status Reason..." patterns after team names are noisy;
    # match Player Last, First + status token.
    status_re = re.compile(
        r"(?P<last>[A-Za-z'.\-]+),\s+(?P<first>[A-Za-z'.\-]+(?:\s+[A-Za-z'.\-]+)?)\s+"
        r"(?P<status>Out|Doubtful|Questionable|Probable|Available|GTD|Day-To-Day)\b"
        r"(?:\s+(?P<reason>.+?))?(?=(?:[A-Za-z'.\-]+,\s+[A-Za-z])|$)",
        re.IGNORECASE | re.DOTALL,
    )
    # Team abbreviation context: keep last seen ALLCAPS 2-3 letter token near matchup
    team = ""
    for line in text.splitlines():
        m_team = re.search(r"\b([A-Z]{2,3})@([A-Z]{2,3})\b", line)
        if m_team:
            # don't know which team the following players belong to; leave blank until
            # a full team name appears
            pass
        # Full team names often precede player rows
        for name, abbr in (
            ("Atlanta Hawks", "ATL"),
            ("Boston Celtics", "BOS"),
            ("Brooklyn Nets", "BKN"),
            ("Charlotte Hornets", "CHA"),
            ("Chicago Bulls", "CHI"),
            ("Cleveland Cavaliers", "CLE"),
            ("Dallas Mavericks", "DAL"),
            ("Denver Nuggets", "DEN"),
            ("Detroit Pistons", "DET"),
            ("Golden State Warriors", "GSW"),
            ("Houston Rockets", "HOU"),
            ("Indiana Pacers", "IND"),
            ("LA Clippers", "LAC"),
            ("Los Angeles Clippers", "LAC"),
            ("Los Angeles Lakers", "LAL"),
            ("Memphis Grizzlies", "MEM"),
            ("Miami Heat", "MIA"),
            ("Milwaukee Bucks", "MIL"),
            ("Minnesota Timberwolves", "MIN"),
            ("New Orleans Pelicans", "NOP"),
            ("New York Knicks", "NYK"),
            ("Oklahoma City Thunder", "OKC"),
            ("Orlando Magic", "ORL"),
            ("Philadelphia 76ers", "PHI"),
            ("Phoenix Suns", "PHX"),
            ("Portland Trail Blazers", "POR"),
            ("Sacramento Kings", "SAC"),
            ("San Antonio Spurs", "SAS"),
            ("Toronto Raptors", "TOR"),
            ("Utah Jazz", "UTA"),
            ("Washington Wizards", "WAS"),
        ):
            if name.lower() in line.lower():
                team = abbr
        for m in status_re.finditer(line):
            player = f"{m.group('first').strip()} {m.group('last').strip()}"
            status = m.group("status")
            reason = (m.group("reason") or "").strip()
            reason = re.sub(r"\s+", " ", reason)
            rows.append(
                _row(
                    sport="NBA",
                    player=player,
                    team=team,
                    status=status,
                    injury=reason,
                    source="nba_official_pdf",
                    captured_at=captured_at,
                )
            )
    return rows


def fetch_nba_official_pdf(date_str: str, captured_at: str, *, raw_dir: Path) -> tuple[list[dict], dict]:
    """Download newest available official NBA injury PDF; save bytes under raw_dir."""
    meta: dict[str, Any] = {"ok": False, "url": "", "slot": "", "n": 0}
    now = now_et()
    try:
        import pdfplumber  # type: ignore
    except ImportError:
        meta["error"] = "pdfplumber_missing"
        return [], meta

    raw_dir.mkdir(parents=True, exist_ok=True)
    for slot in _nba_pdf_slots(now):
        url = NBA_PDF_BASE.format(date=date_str, slot=slot)
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code != 200 or not r.content or len(r.content) < 500:
                continue
            if b"%PDF" not in r.content[:8]:
                continue
            pdf_path = raw_dir / f"nba_official_{date_str}_{slot}.pdf"
            if not pdf_path.is_file():
                pdf_path.write_bytes(r.content)
            text_parts: list[str] = []
            with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                for page in pdf.pages:
                    text_parts.append(page.extract_text() or "")
            text = "\n".join(text_parts)
            rows = _parse_nba_injury_pdf_text(text, captured_at)
            meta.update({"ok": True, "url": url, "slot": slot, "n": len(rows), "pdf": str(pdf_path)})
            return rows, meta
        except Exception as exc:
            meta["error"] = str(exc)[:200]
            continue
    return [], meta


def fetch_nflverse_injuries(captured_at: str, *, season: int | None = None) -> list[dict]:
    """Practice participation + game status from nflverse injuries release CSV."""
    try:
        r = requests.get(NFLVERSE_INJURIES, headers=HEADERS, timeout=60)
        if r.status_code != 200:
            return []
        df = pd.read_csv(io.StringIO(r.text))
    except Exception:
        return []
    if df.empty:
        return []
    # Prefer current / recent season weeks
    if "season" in df.columns and season is not None:
        df = df[df["season"].astype(str) == str(season)]
    elif "season" in df.columns:
        try:
            max_season = int(pd.to_numeric(df["season"], errors="coerce").max())
            df = df[df["season"].astype(str) == str(max_season)]
        except Exception:
            pass
    if "week" in df.columns:
        try:
            max_week = pd.to_numeric(df["week"], errors="coerce").max()
            df = df[pd.to_numeric(df["week"], errors="coerce") >= max(1, int(max_week) - 1)]
        except Exception:
            pass
    rows: list[dict] = []
    for _, rec in df.iterrows():
        player = str(rec.get("full_name") or rec.get("player_name") or "").strip()
        team = str(rec.get("team") or "").strip()
        if not player:
            continue
        status = str(
            rec.get("report_status")
            or rec.get("game_status")
            or rec.get("practice_status")
            or ""
        ).strip()
        injury = str(rec.get("report_primary_injury") or rec.get("report_secondary_injury") or "").strip()
        practice = str(rec.get("practice_status") or "").strip()
        rows.append(
            _row(
                sport="NFL",
                player=player,
                team=team,
                status=status,
                injury=injury or practice,
                source="nflverse",
                captured_at=captured_at,
                extra={"practice_status": practice, "week": rec.get("week"), "season": rec.get("season")},
            )
        )
    return rows


def fetch_soccer_injuries(captured_at: str) -> list[dict]:
    rows: list[dict] = []
    for league in SOCCER_LEAGUES:
        url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/injuries"
        try:
            time.sleep(0.15)
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code != 200:
                continue
            part = _parse_espn_injuries_payload(
                r.json(), "SOCCER", f"espn_soccer_{league}", captured_at
            )
            for row in part:
                row["league"] = league
            rows.extend(part)
        except Exception:
            continue
    return rows


def fetch_tennis_withdrawals(date_str: str, captured_at: str) -> list[dict]:
    """Withdrawals / retirements from ESPN tennis scoreboard competitor notes."""
    rows: list[dict] = []
    stamp = date_str.replace("-", "")
    for tour in ("atp", "wta"):
        url = (
            f"https://site.api.espn.com/apis/site/v2/sports/tennis/{tour}/scoreboard"
            f"?dates={stamp}"
        )
        try:
            r = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
            if r.status_code != 200:
                continue
            data = r.json()
        except Exception:
            continue
        for ev in data.get("events") or []:
            if not isinstance(ev, dict):
                continue
            for comp in ev.get("competitions") or []:
                notes = " ".join(
                    str(n.get("text") or n) if isinstance(n, dict) else str(n)
                    for n in (comp.get("notes") or [])
                ).lower()
                status_type = str(((comp.get("status") or {}).get("type") or {}).get("name") or "").lower()
                for c in comp.get("competitors") or []:
                    if not isinstance(c, dict):
                        continue
                    ath = c.get("athlete") or {}
                    name = str(ath.get("displayName") or c.get("displayName") or "").strip()
                    if not name:
                        continue
                    blob = f"{notes} {status_type} {c.get('status') or ''}".lower()
                    if not any(k in blob for k in ("withdraw", "retired", "walkover", "wo", "retirement")):
                        continue
                    status = "OUT"
                    if "retired" in blob or "retirement" in blob:
                        injury = "retired"
                    elif "walkover" in blob or re.search(r"\bwo\b", blob):
                        injury = "walkover"
                    else:
                        injury = "withdrawal"
                    rows.append(
                        _row(
                            sport="TENNIS",
                            player=name,
                            team=tour.upper(),
                            status=status,
                            injury=injury,
                            source=f"espn_tennis_{tour}",
                            captured_at=captured_at,
                        )
                    )
    return rows


def collect_sport(
    sport: str,
    date_str: str,
    captured_at: str,
    *,
    raw_dir: Path,
) -> tuple[list[dict], dict]:
    lit = sport.upper()
    meta: dict[str, Any] = {"sport": lit, "sources": {}}
    rows: list[dict] = []

    if lit == "NBA":
        pdf_rows, pdf_meta = fetch_nba_official_pdf(date_str, captured_at, raw_dir=raw_dir)
        meta["sources"]["nba_official_pdf"] = pdf_meta
        rows.extend(pdf_rows)
        espn = fetch_espn_league_injuries("NBA", captured_at)
        meta["sources"]["espn"] = {"n": len(espn)}
        rows.extend(espn)
    elif lit == "NFL":
        nv = fetch_nflverse_injuries(captured_at)
        meta["sources"]["nflverse"] = {"n": len(nv)}
        rows.extend(nv)
        espn = fetch_espn_league_injuries("NFL", captured_at)
        meta["sources"]["espn"] = {"n": len(espn)}
        rows.extend(espn)
    elif lit == "SOCCER":
        soc = fetch_soccer_injuries(captured_at)
        meta["sources"]["espn"] = {"n": len(soc)}
        rows.extend(soc)
    elif lit == "TENNIS":
        ten = fetch_tennis_withdrawals(date_str, captured_at)
        meta["sources"]["espn_draw"] = {"n": len(ten)}
        rows.extend(ten)
    elif lit in ESPN_LEAGUE_INJURIES:
        espn = fetch_espn_league_injuries(lit, captured_at)
        meta["sources"]["espn"] = {"n": len(espn)}
        rows.extend(espn)
    else:
        meta["error"] = f"unsupported_sport:{lit}"
        return [], meta

    # Dedupe player+team+source keeping first (prefer earlier = official/nflverse)
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict] = []
    for row in rows:
        key = (
            str(row.get("player") or "").strip().lower(),
            str(row.get("team") or "").strip().upper(),
            str(row.get("source") or ""),
        )
        if key in seen or not key[0]:
            continue
        seen.add(key)
        deduped.append(row)
    meta["n"] = len(deduped)
    return deduped, meta


LIVE_SPORTS = ("NBA", "NFL", "NHL", "CFB", "CBB", "SOCCER", "TENNIS", "WNBA", "MLB")


def write_snapshot(
    sport: str,
    date_str: str,
    rows: list[dict],
    meta: dict,
    *,
    root: Path | None = None,
    hhmm: str | None = None,
) -> Path:
    dest_dir = injuries_dir(date_str, root=root)
    dest_dir.mkdir(parents=True, exist_ok=True)
    stamp = hhmm or hhmm_et()
    path = dest_dir / f"{sport.lower()}_{stamp}.json"
    # Never overwrite: bump suffix if same HHMM rerun
    if path.is_file():
        i = 2
        while True:
            cand = dest_dir / f"{sport.lower()}_{stamp}_{i}.json"
            if not cand.is_file():
                path = cand
                break
            i += 1
    payload = {
        "sport": sport.upper(),
        "date": date_str[:10],
        "captured_at": rows[0]["captured_at"] if rows else captured_at_iso(),
        "n": len(rows),
        "meta": meta,
        "rows": rows,
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return path


def merge_latest_csv(
    all_rows: list[dict],
    *,
    root: Path | None = None,
) -> Path:
    path = latest_csv_path(root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    cols = ["player", "team", "status", "injury", "source", "captured_at", "sport"]
    # Prefer OUT over softer statuses for same player+team+sport; newest captured_at wins ties
    best: dict[tuple[str, str, str], dict] = {}
    rank = {"OUT": 0, "Doubtful": 1, "Questionable": 2, "GTD": 3, "Probable": 4}

    def _key(r: dict) -> tuple[str, str, str]:
        return (
            str(r.get("player") or "").strip().lower(),
            str(r.get("team") or "").strip().upper(),
            str(r.get("sport") or "").strip().upper(),
        )

    for r in all_rows:
        k = _key(r)
        if not k[0]:
            continue
        prev = best.get(k)
        if prev is None:
            best[k] = r
            continue
        pr = rank.get(normalize_inj_status(prev.get("status")), 9)
        cr = rank.get(normalize_inj_status(r.get("status")), 9)
        if cr < pr:
            best[k] = r
        elif cr == pr and str(r.get("captured_at") or "") > str(prev.get("captured_at") or ""):
            best[k] = r

    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
        w.writeheader()
        for r in sorted(best.values(), key=lambda x: (x.get("sport", ""), x.get("team", ""), x.get("player", ""))):
            w.writerow({c: r.get(c, "") for c in cols})
    return path


def load_latest_rows(*, root: Path | None = None) -> list[dict]:
    path = latest_csv_path(root=root)
    if not path.is_file():
        return []
    with path.open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))
