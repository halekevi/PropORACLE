#!/usr/bin/env python3
"""
soccer_defense_report.py — REWRITTEN

Key changes vs original:
  - 13 leagues (added Argentina, Brazil, Liga MX, Eredivisie, Primeira Liga, Scottish Prem)
  - Outputs `pp_name` column = PrizePicks-style short name (e.g. LEIPZIG, CELTA, INTER MIAMI)
  - step3 merges on pp_name instead of team_name → fixes all the NaN defense mismatches
  - Master PP_NAME_MAP here (single source of truth)

Usage:
  py soccer_defense_report.py
  py soccer_defense_report.py --out soccer_defense_summary.csv
  py soccer_defense_report.py --out cache/soccer_defense_summary.csv

shots_conceded_pg comes from ESPN core team statistics (shotsFaced / appearances),
not the empty site.api .../teams/{id}/statistics endpoint.
"""
from __future__ import annotations
import argparse
import sys
import time
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import requests

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
from utils.defense_tiers import def_tier_from_overall_rank

# Full Chrome UA triggers ESPN 403 on standings; simple UA works reliably.
ESPN_HEADERS = {
    "User-Agent": "PropOracle/1.0 (+https://github.com/proporacle)",
    "Accept": "application/json",
}

LEAGUES = [
    ("eng.1",          "EPL"),
    ("eng.2",          "Championship"),
    ("uefa.champions", "UCL"),
    ("usa.1",          "MLS"),
    ("esp.1",          "La Liga"),
    ("ger.1",          "Bundesliga"),
    ("ita.1",          "Serie A"),
    ("fra.1",          "Ligue 1"),
    ("arg.1",          "Argentina"),
    ("bra.1",          "Brazil"),
    ("mex.1",          "Liga MX"),
    ("ned.1",          "Eredivisie"),
    ("por.1",          "Primeira Liga"),
    ("sco.1",          "Scottish Prem"),
    ("usa.nwsl",       "NWSL"),
    ("aus.1",          "A-League"),
    ("eng.w.1",        "WSL"),
    ("tur.1",          "Süper Lig"),
    ("gre.1",          "Super League Greece"),
    ("ksa.1",          "Saudi Pro League"),
    ("col.1",          "Colombia"),
    ("ecu.1",          "Ecuador"),
    ("fifa.world",     "World Cup"),
]

LEAGUE_MIN_GP: Dict[str, int] = {
    "World Cup": 1,
}
MIN_GP = 3  # club leagues default

ESPN_STANDINGS = "https://site.api.espn.com/apis/v2/sports/soccer/{slug}/standings"
# site.api .../teams/{id}/statistics is empty/403 for soccer; core API has shotsFaced.
ESPN_TEAM_STATS_CORE = (
    "https://sports.core.api.espn.com/v2/sports/soccer/leagues/{slug}"
    "/seasons/{season}/types/{season_type}/teams/{team_id}/statistics"
)

PP_NAME_MAP: Dict[str, str] = {
    # EPL
    "Manchester City": "MAN CITY", "Arsenal": "ARSENAL", "Liverpool": "LIVERPOOL",
    "Chelsea": "CHELSEA", "Tottenham Hotspur": "SPURS", "Manchester United": "MAN UNITED",
    "Newcastle United": "NEWCASTLE", "Aston Villa": "ASTON VILLA",
    "Brighton & Hove Albion": "BRIGHTON", "Brighton": "BRIGHTON",
    "West Ham United": "WEST HAM", "Brentford": "BRENTFORD", "Fulham": "FULHAM",
    "Crystal Palace": "CRYSTAL PALACE", "Wolverhampton Wanderers": "WOLVES",
    "Everton": "EVERTON", "Nottingham Forest": "NOTTM FOREST",
    "Bournemouth": "BOURNEMOUTH", "Ipswich Town": "IPSWICH",
    "Leicester City": "LEICESTER", "Southampton": "SOUTHAMPTON",
    # Bundesliga
    "Bayern Munich": "BAYERN MUNICH", "Bayer Leverkusen": "LEVERKUSEN",
    "Borussia Dortmund": "DORTMUND", "RB Leipzig": "LEIPZIG",
    "Eintracht Frankfurt": "FRANKFURT", "SC Freiburg": "FREIBURG",
    "Wolfsburg": "WOLFSBURG", "TSG Hoffenheim": "HOFFENHEIM",
    "Mainz": "MAINZ", "1. FSV Mainz 05": "MAINZ",
    "Werder Bremen": "WERDER BREMEN", "FC Augsburg": "AUGSBURG",
    "VfL Bochum": "BOCHUM", "Borussia Mönchengladbach": "GLADBACH",
    "1. FC Union Berlin": "UNION BERLIN", "VfB Stuttgart": "STUTTGART",
    "1. FC Heidenheim": "HEIDENHEIM", "FC St. Pauli": "ST. PAULI",
    "Holstein Kiel": "KIEL", "Hamburg SV": "HAMBURG",
    "FC Cologne": "KOLN", "1. FC Köln": "KOLN",
    # La Liga
    "Real Madrid": "REAL MADRID", "FC Barcelona": "BARCELONA", "Barcelona": "BARCELONA",
    "Atletico Madrid": "ATLETICO", "Atlético de Madrid": "ATLETICO",
    "Athletic Club": "ATHLETIC BILBAO", "Real Sociedad": "REAL SOCIEDAD",
    "Villarreal": "VILLARREAL", "Real Betis": "BETIS", "Valencia": "VALENCIA",
    "Osasuna": "OSASUNA", "Getafe": "GETAFE", "Celta Vigo": "CELTA",
    "Rayo Vallecano": "RAYO", "Sevilla": "SEVILLA", "RCD Mallorca": "MALLORCA",
    "Mallorca": "MALLORCA", "Girona": "GIRONA", "UD Las Palmas": "LAS PALMAS",
    "Las Palmas": "LAS PALMAS", "Deportivo Alaves": "ALAVES",
    "Real Valladolid": "VALLADOLID", "CD Leganes": "LEGANES", "RCD Espanyol": "ESPANYOL",
    # Serie A
    "Inter Milan": "INTER", "FC Internazionale Milano": "INTER",
    "AC Milan": "MILAN", "Juventus": "JUVENTUS", "Napoli": "NAPOLI",
    "SS Lazio": "LAZIO", "Lazio": "LAZIO", "AS Roma": "ROMA", "Roma": "ROMA",
    "Atalanta": "ATALANTA", "Fiorentina": "FIORENTINA", "Torino": "TORINO",
    "Bologna": "BOLOGNA", "Udinese": "UDINESE", "Genoa": "GENOA",
    "Hellas Verona": "VERONA", "Cagliari": "CAGLIARI", "Lecce": "LECCE",
    "Parma": "PARMA", "Empoli": "EMPOLI", "Venezia": "VENEZIA",
    "Como": "COMO", "AC Monza": "MONZA",
    # Ligue 1
    "Paris Saint-Germain": "PSG", "Olympique de Marseille": "MARSEILLE",
    "Marseille": "MARSEILLE", "Olympique Lyonnais": "LYON", "Lyon": "LYON",
    "AS Monaco": "MONACO", "Monaco": "MONACO", "Lille": "LILLE",
    "OGC Nice": "NICE", "Nice": "NICE", "Lens": "LENS", "RC Lens": "LENS",
    "Stade Rennais": "RENNES", "Rennes": "RENNES", "RC Strasbourg": "STRASBOURG",
    "Toulouse": "TOULOUSE", "Montpellier": "MONTPELLIER", "Nantes": "NANTES",
    "Stade de Reims": "REIMS", "Reims": "REIMS", "Le Havre": "LE HAVRE",
    "AS Saint-Etienne": "ST-ETIENNE", "Saint-Etienne": "ST-ETIENNE",
    "Angers": "ANGERS", "Auxerre": "AUXERRE",
    "Stade Brestois 29": "BREST", "Brest": "BREST",
    # MLS
    "Inter Miami CF": "INTER MIAMI", "Inter Miami": "INTER MIAMI",
    "LA Galaxy": "LA GALAXY", "Los Angeles FC": "LAFC", "LAFC": "LAFC",
    "Seattle Sounders FC": "SEATTLE", "Portland Timbers": "PORTLAND",
    "Colorado Rapids": "COLORADO", "New York City FC": "NYCFC",
    "New York Red Bulls": "NY RED BULLS", "Atlanta United FC": "ATLANTA UNITED",
    "Orlando City SC": "ORLANDO", "Orlando City": "ORLANDO",
    "Columbus Crew": "COLUMBUS", "Chicago Fire FC": "CHICAGO",
    "Toronto FC": "TORONTO", "CF Montréal": "MONTREAL",
    "New England Revolution": "NEW ENGLAND", "Philadelphia Union": "PHILADELPHIA",
    "D.C. United": "DC UNITED", "FC Cincinnati": "CINCINNATI",
    "Nashville SC": "NASHVILLE", "Charlotte FC": "CHARLOTTE",
    "Austin FC": "AUSTIN", "FC Dallas": "FC DALLAS",
    "Houston Dynamo FC": "HOUSTON", "Houston Dynamo": "HOUSTON",
    "Sporting Kansas City": "SPORTING KC", "Minnesota United FC": "MINNESOTA",
    "St. Louis City SC": "ST. LOUIS",
    "St. Louis CITY SC": "ST. LOUIS",
    "St. Louis City": "ST. LOUIS",
    "Vancouver Whitecaps FC": "VANCOUVER", "San Jose Earthquakes": "SAN JOSE",
    "San Diego FC": "SAN DIEGO",
    # EFL Championship
    "Middlesbrough": "MIDDLESBROUGH",
    "Birmingham City": "BIRMINGHAM",
    "Sunderland": "SUNDERLAND",
    "Leeds United": "LEEDS",
    # Argentina — pp_name must match Step 2 opp_team exactly (uppercased)
    "Boca Juniors": "BOCA JUNIORS",
    "River Plate": "RIVER",
    "Racing Club": "RACING",
    "Independiente": "INDEPENDIENTE",
    "San Lorenzo": "SAN LORENZO",
    "Estudiantes de La Plata": "ESTUDIANTES",
    "Estudiantes": "ESTUDIANTES",
    "Vélez Sársfield": "VÉLEZ",
    "Velez Sarsfield": "VÉLEZ",
    "Talleres": "TALLERES",
    "Talleres de Córdoba": "TALLERES",
    "Lanús": "LANÚS",
    "Lanus": "LANÚS",
    "Huracán": "HURACÁN",
    "Huracan": "HURACÁN",
    "Tigre": "TIGRE",
    "Defensa y Justicia": "DEF Y JUSTICIA",
    "Godoy Cruz": "GODOY CRUZ",
    "Belgrano": "BELGRANO",
    "Platense": "PLATENSE",
    "Club Atlético Platense": "PLATENSE",
    "Newell's Old Boys": "NEWELL'S",
    "Rosario Central": "ROSARIO",
    "Atlético Tucumán": "ATLETICO TUCUMAN",
    "Central Córdoba": "CENTRAL CÓRDOBA",
    "Instituto": "INSTITUTO",
    "Instituto Atlético Central Córdoba": "INSTITUTO",
    "Barracas Central": "BARRACAS",
    "Argentinos Juniors": "ARGENTINOS",
    "Unión": "UNIÓN",
    "Union de Santa Fe": "UNIÓN",
    "Riestra": "RIESTRA",
    "Deportivo Riestra": "RIESTRA",
    "Gimnasia y Esgrima La Plata": "GELP",
    "Gimnasia La Plata": "GELP",
    "Gimnasia (La Plata)": "GELP",
    "Gimnasia": "GELP",
    "Aldosivi": "ALDOSIVI",
    "Club Atlético Aldosivi": "ALDOSIVI",
    "Sarmiento": "SARMIENTO",
    "San Martín": "SAN MARTIN",
    # ESPN returns city-qualified names for Argentine teams — add both variants
    "Talleres (Córdoba)": "TALLERES",
    "Unión (Santa Fe)": "UNIÓN",
    "Vélez Sársfield": "VÉLEZ",
    "Vélez Sarsfield": "VÉLEZ",
    "Instituto (Córdoba)": "INSTITUTO",
    "Instituto Atlético Central Córdoba (Córdoba)": "INSTITUTO",
    "Barracas Central (Buenos Aires)": "BARRACAS",
    "Argentinos Juniors (Buenos Aires)": "ARGENTINOS",
    "Riestra (Buenos Aires)": "RIESTRA",
    "Deportivo Riestra": "RIESTRA",
    "River Plate (Buenos Aires)": "RIVER",
    "Boca Juniors (Buenos Aires)": "BOCA JUNIORS",
    "Independiente (Avellaneda)": "INDEPENDIENTE",
    "Racing Club (Avellaneda)": "RACING",
    "San Lorenzo (Buenos Aires)": "SAN LORENZO",
    "Estudiantes (La Plata)": "ESTUDIANTES",
    "Lanús (Lanús)": "LANÚS",
    "Huracán (Buenos Aires)": "HURACÁN",
    "Tigre (Victoria)": "TIGRE",
    "Defensa y Justicia (Florencio Varela)": "DEF Y JUSTICIA",
    "Godoy Cruz (Mendoza)": "GODOY CRUZ",
    "Belgrano (Córdoba)": "BELGRANO",
    "Platense (Buenos Aires)": "PLATENSE",
    "Newell's Old Boys (Rosario)": "NEWELL'S",
    "Rosario Central (Rosario)": "ROSARIO",
    "Atlético Tucumán (San Miguel de Tucumán)": "ATLETICO TUCUMAN",
    "Central Córdoba (Santiago del Estero)": "CENTRAL CÓRDOBA",
    "Aldosivi (Mar del Plata)": "ALDOSIVI",
    "Gimnasia y Esgrima (La Plata)": "GELP",
    "Sarmiento (Junín)": "SARMIENTO",
    # Brazil
    "Flamengo": "FLAMENGO", "Palmeiras": "PALMEIRAS", "Fluminense": "FLUMINENSE",
    "São Paulo FC": "SAO PAULO", "São Paulo": "SAO PAULO",
    "Sport Club Corinthians Paulista": "CORINTHIANS", "Corinthians": "CORINTHIANS",
    "Atlético Mineiro": "ATLETICO MG", "Internacional": "INTERNACIONAL",
    "Grêmio": "GREMIO", "Santos": "SANTOS", "Vasco da Gama": "VASCO",
    "Botafogo": "BOTAFOGO", "Cruzeiro": "CRUZEIRO",
    "Fortaleza": "FORTALEZA", "EC Bahia": "BAHIA", "Bahia": "BAHIA",
    # Liga MX
    "Club América": "AMERICA", "Cruz Azul": "CRUZ AZUL",
    "CD Guadalajara": "CHIVAS", "Tigres UANL": "TIGRES",
    "CF Monterrey": "MONTERREY", "Pachuca": "PACHUCA", "Toluca": "TOLUCA",
    "León": "LEON", "Pumas UNAM": "PUMAS", "Atlas": "ATLAS",
    "Necaxa": "NECAXA", "Mazatlán FC": "MAZATLAN",
    "FC Juárez": "JUAREZ", "Querétaro": "QUERETARO",
    # EFL Championship (additional)
    "Wrexham": "WREXHAM", "Swansea City": "SWANSEA",
    "West Bromwich Albion": "WEST BROM", "Sheffield United": "SHEFF UTD",
    "Preston North End": "PRESTON", "Burnley": "BURNLEY",
    "Blackburn Rovers": "BLACKBURN", "Hull City": "HULL",
    "Watford": "WATFORD", "Oxford United": "OXFORD",
    "Bristol City": "BRISTOL CITY", "Queens Park Rangers": "QPR",
    "Cardiff City": "CARDIFF", "Derby County": "DERBY",
    "Sheffield Wednesday": "SHEFF WED", "Portsmouth": "PORTSMOUTH",
    "Norwich City": "NORWICH", "Luton Town": "LUTON",
    "Plymouth Argyle": "PLYMOUTH", "Millwall": "MILLWALL",
    "Stoke City": "STOKE CITY", "Coventry City": "COVENTRY CITY",
    # Saudi Pro League
    "Al-Ittihad Club": "ITTIHAD", "Al Ittihad": "ITTIHAD",
    "Al-Nassr FC": "NASSR", "Al Nassr": "NASSR",
    "Al-Hilal SFC": "HILAL", "Al Hilal": "HILAL",
    "Al-Ahli Saudi FC": "AHLI", "Al Ahli": "AHLI",
    "Al-Qadsiah FC": "QADSIAH", "Al Qadsiah": "QADSIAH",
    "Al-Ettifaq FC": "ETTIFAQ", "Al Ettifaq": "ETTIFAQ",
    "Al Riyadh": "RIYADH", "Al-Riyadh SC": "RIYADH",
    "Al Fayha": "FAYHA", "Al-Fayha": "FAYHA",
    "Al Shabab": "SHABAB", "Al-Shabab FC": "SHABAB",
    "Al Fateh": "FATEH", "Al-Fateh SC": "FATEH",
    "Al Qadisiyah": "QADISIYAH",
    "Al Hazem": "HAZEM", "Al Khaleej": "KHALEEJ",
    "Al Ta'ee": "TAEE", "Al Okhdood": "OKHDOOD",
    "Damac FC": "DAMAC", "Al Wehda": "WEHDA",
    # NWSL
    "Washington Spirit": "SPIRIT", "Portland Thorns FC": "THORNS",
    "Portland Thorns": "THORNS",
    "North Carolina Courage": "NC COURAGE",
    "OL Reign FC": "REIGN", "OL Reign": "REIGN",
    "Chicago Red Stars": "RED STARS",
    "Orlando Pride": "ORLANDO PRIDE",
    "Houston Dash": "HOUSTON DASH",
    "San Diego Wave FC": "SD WAVE", "San Diego Wave": "SD WAVE",
    "Angel City FC": "ANGEL CITY",
    "Gotham FC": "GOTHAM", "NJ/NY Gotham FC": "GOTHAM",
    "Kansas City Current": "KC CURRENT",
    "Racing Louisville FC": "LOUISVILLE",
    "Bay FC": "BAY FC",
    # A-League (Australia)
    "Perth Glory": "PERTH", "Wellington Phoenix": "WELLINGTON",
    "Melbourne City FC": "MELBOURNE CITY",
    "Melbourne Victory": "MELBOURNE VICTORY",
    "Sydney FC": "SYDNEY", "Western Sydney Wanderers": "WSW",
    "Brisbane Roar": "BRISBANE", "Adelaide United": "ADELAIDE",
    "Macarthur FC": "MACARTHUR", "Central Coast Mariners": "CENTRAL COAST",
    "Newcastle Jets": "NEWCASTLE JETS", "Western United": "WESTERN UNITED",
    "Auckland FC": "AUCKLAND",
    # Süper Lig (Turkey)
    "Galatasaray": "GALATASARAY", "Fenerbahçe": "FENERBAHCE",
    "Besiktas JK": "BESIKTAS", "Beşiktaş": "BESIKTAS",
    "Trabzonspor": "TRABZONSPOR",
    # Super League Greece
    "Olympiacos": "OLYMPIACOS", "Panathinaikos": "PANATHINAIKOS",
    "PAOK": "PAOK", "AEK Athens": "AEK",
    # FIFA World Cup 2026 — ESPN displayName → PP uppercase country label
    "Algeria": "ALGERIA",
    "Argentina": "ARGENTINA",
    "Australia": "AUSTRALIA",
    "Austria": "AUSTRIA",
    "Belgium": "BELGIUM",
    "Bosnia-Herzegovina": "BOSNIA-HERZEGOVINA",
    "Brazil": "BRAZIL",
    "Canada": "CANADA",
    "Cape Verde": "CAPE VERDE",
    "Colombia": "COLOMBIA",
    "Congo DR": "CONGO DR",
    "Croatia": "CROATIA",
    "Curaçao": "CURACAO",
    "Curacao": "CURACAO",
    "Czechia": "CZECHIA",
    "Ecuador": "ECUADOR",
    "Egypt": "EGYPT",
    "England": "ENGLAND",
    "France": "FRANCE",
    "Germany": "GERMANY",
    "Ghana": "GHANA",
    "Haiti": "HAITI",
    "Iran": "IRAN",
    "Iraq": "IRAQ",
    "Ivory Coast": "IVORY COAST",
    "Japan": "JAPAN",
    "Jordan": "JORDAN",
    "Mexico": "MEXICO",
    "Morocco": "MOROCCO",
    "Netherlands": "NETHERLANDS",
    "New Zealand": "NEW ZEALAND",
    "Norway": "NORWAY",
    "Panama": "PANAMA",
    "Paraguay": "PARAGUAY",
    "Portugal": "PORTUGAL",
    "Qatar": "QATAR",
    "Saudi Arabia": "SAUDI ARABIA",
    "Scotland": "SCOTLAND",
    "Senegal": "SENEGAL",
    "South Africa": "SOUTH AFRICA",
    "South Korea": "SOUTH KOREA",
    "Spain": "SPAIN",
    "Sweden": "SWEDEN",
    "Switzerland": "SWITZERLAND",
    "Tunisia": "TUNISIA",
    "Türkiye": "TURKEY",
    "Turkey": "TURKEY",
    "United States": "UNITED STATES",
    "Uruguay": "URUGUAY",
    "Uzbekistan": "UZBEKISTAN",
}


def _pp_name(display_name: str) -> str:
    if display_name in PP_NAME_MAP:
        return PP_NAME_MAP[display_name]
    n = display_name.upper().strip()
    for suffix in (" FC", " SC", " CF", " AC", " SV", " FK", " SK", " AFC"):
        if n.endswith(suffix):
            n = n[: -len(suffix)].strip()
    return n


def _get(url: str, retries: int = 3, *, pause: float = 0.3) -> Optional[dict]:
    for attempt in range(1, retries + 1):
        try:
            if pause > 0:
                time.sleep(pause + random.uniform(0, 0.15))
            r = requests.get(url, headers=ESPN_HEADERS, timeout=20)
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt < retries:
                time.sleep(1.5 * attempt)
    return None


def _stat_map_from_core_payload(data: dict) -> Dict[str, float]:
    """Flatten ESPN core team statistics categories → name→value."""
    out: Dict[str, float] = {}
    splits = data.get("splits") or {}
    for cat in splits.get("categories") or []:
        for stat in cat.get("stats") or []:
            name = str(stat.get("name") or "").strip()
            if not name:
                continue
            try:
                out[name] = float(stat.get("value"))
            except (TypeError, ValueError):
                continue
    return out


def shots_conceded_pg_from_stat_map(stats: Dict[str, float]) -> Optional[float]:
    """Derive shots conceded per game from core-API fields.

    Prefer goalkeeping ``shotsFaced`` / ``appearances`` (team shots conceded proxy).
    """
    if not stats:
        return None
    shots = None
    for key in ("shotsFaced", "shotsAgainst", "shotsagainst"):
        if key in stats:
            try:
                shots = float(stats[key])
            except (TypeError, ValueError):
                shots = None
            if shots is not None:
                break
    if shots is None:
        return None
    apps = None
    for key in ("appearances", "gamesPlayed", "gamesplayed"):
        if key in stats:
            try:
                apps = float(stats[key])
            except (TypeError, ValueError):
                apps = None
            if apps is not None and apps > 0:
                break
    if not apps or apps <= 0:
        return None
    return round(shots / apps, 3)


def resolve_core_stats_season(
    slug: str,
    sample_team_id: str,
    *,
    season_years: Optional[List[int]] = None,
) -> Optional[tuple[int, int]]:
    """Pick (season, season_type) with the richest shotsFaced sample for this league."""
    tid = str(sample_team_id or "").strip()
    if not tid:
        return None
    years = season_years
    if not years:
        y = datetime.now().year
        years = [y, y - 1, y - 2]
    best: Optional[tuple[float, int, int]] = None  # apps, season, type
    for season in years:
        for season_type in (1, 2):
            url = ESPN_TEAM_STATS_CORE.format(
                slug=slug, season=season, season_type=season_type, team_id=tid
            )
            data = _get(url, pause=0.15)
            if not data:
                continue
            stats = _stat_map_from_core_payload(data)
            if shots_conceded_pg_from_stat_map(stats) is None:
                continue
            apps = float(stats.get("appearances") or stats.get("gamesPlayed") or 0)
            if best is None or apps > best[0]:
                best = (apps, season, season_type)
        if best and best[0] >= 3:
            break
    if best is None:
        return None
    return best[1], best[2]


def fetch_team_shots_conceded_pg(
    slug: str,
    team_id: str,
    *,
    season: int,
    season_type: int = 1,
) -> Optional[float]:
    """Pull shotsFaced/appearances from ESPN core API for one team/season."""
    tid = str(team_id or "").strip()
    if not tid:
        return None
    url = ESPN_TEAM_STATS_CORE.format(
        slug=slug, season=season, season_type=season_type, team_id=tid
    )
    data = _get(url, pause=0.12)
    if not data:
        return None
    return shots_conceded_pg_from_stat_map(_stat_map_from_core_payload(data))


def fetch_league_defense(
    slug: str, league_name: str, *, fetch_shots: bool = True
) -> List[dict]:
    data = _get(ESPN_STANDINGS.format(slug=slug))
    if not data:
        print(f"  ⚠️  {league_name}: no standings data")
        return []

    rows = []
    groups = (
        data.get("standings", {}).get("groups")
        or data.get("children")
        or [data]
    )

    for group in groups:
        entries = (
            group.get("standings", {}).get("entries")
            or group.get("entries")
            or []
        )
        for entry in entries:
            ti        = entry.get("team", {})
            team_id   = str(ti.get("id", "")).strip()
            team_name = str(ti.get("displayName", ti.get("name", ""))).strip()

            stats: Dict[str, float] = {}
            for s in (entry.get("stats") or []):
                try:
                    stats[str(s.get("name","")).lower()] = float(s.get("value",""))
                except (TypeError, ValueError):
                    pass

            gp    = int(stats.get("gamesplayed", stats.get("gp", 0)) or 0)
            gc    = float(stats.get("pointsagainst", stats.get("goalsagainst", 0)) or 0)
            # ESPN soccer standings: pointsFor = Goals For (attack), pointsAgainst = Goals Against.
            gf    = float(stats.get("pointsfor", stats.get("goalsfor", 0)) or 0)
            cs    = int(stats.get("cleansheets", 0) or 0)
            wins  = int(stats.get("wins", 0) or 0)
            draws = int(stats.get("draws", stats.get("ties", 0)) or 0)
            losses= int(stats.get("losses", 0) or 0)
            gf_pg = round(gf / max(gp, 1), 3)

            rows.append({
                "team_name":         team_name,
                "pp_name":           _pp_name(team_name),
                "team_id":           team_id,
                "league":            league_name,
                "gp":                gp,
                "goals_conceded":    gc,
                "goals_conceded_pg": round(gc / max(gp, 1), 3),
                "goals_for":         gf,
                "goals_for_pg":      gf_pg,
                # Alias used by step7 opp_pace / defense_db known column.
                "opp_gf_per_game":   gf_pg,
                "clean_sheets":      cs,
                "wins":              wins,
                "draws":             draws,
                "losses":            losses,
                "shots_conceded_pg": None,
                # Venue H/A not on ESPN standings payload — reserved for a later source.
                "venue_split":       "ALL",
                "goals_for_pg_home": None,
                "goals_for_pg_away": None,
                "goals_conceded_pg_home": None,
                "goals_conceded_pg_away": None,
            })

    # shots_conceded_pg via core API (shotsFaced / appearances)
    if fetch_shots:
        sample_tid = next((str(r.get("team_id") or "") for r in rows if r.get("team_id")), "")
        resolved = resolve_core_stats_season(slug, sample_tid) if sample_tid else None
        filled = 0
        if resolved:
            season, season_type = resolved
            print(f"  core stats season={season} type={season_type}")
            for row in rows:
                tid = row.get("team_id", "")
                if not tid:
                    continue
                scpg = fetch_team_shots_conceded_pg(
                    slug, str(tid), season=season, season_type=season_type
                )
                if scpg is not None:
                    row["shots_conceded_pg"] = scpg
                    filled += 1
        if rows:
            print(f"  shots_conceded_pg fill {filled}/{len(rows)}")
    elif rows:
        print("  shots_conceded_pg skipped (--skip-shots)")

    return rows


def _rank_metric_in_league(
    g: pd.DataFrame,
    metric_col: str,
    *,
    ok: pd.Series,
    n: int,
    mid: int,
    min_gp: int,
    gp_num: pd.Series,
    rank_col: str,
    tier_col: str,
    ascending: bool = True,
) -> None:
    """Write ranks + Elite→Weak tiers for one metric.

    ascending=True  → 1 = lowest value (defense: fewest goals/shots conceded).
    ascending=False → 1 = highest value (offense: most goals scored).
    Elite = best end of that axis.
    """
    series = pd.to_numeric(g.get(metric_col), errors="coerce")
    has_metric = series.notna()
    ok_metric = ok & has_metric
    n_metric = int(ok_metric.sum())

    if n_metric < 2:
        # Match legacy goals behavior: mid-rank + Avg when sample too thin.
        g[rank_col] = mid
        g[tier_col] = "Avg"
        # Shots-only: leave blank when the metric itself is missing (do not invent).
        if metric_col.startswith("shots"):
            g.loc[~has_metric, rank_col] = pd.NA
            g.loc[~has_metric, tier_col] = ""
        return

    ranked = series.where(ok_metric, other=float("nan"))
    g[rank_col] = ranked.rank(method="min", ascending=ascending).astype("Int64")
    g.loc[~ok_metric, rank_col] = mid

    def _tier(r, n_teams=n_metric):
        if n_teams < 2 or pd.isna(r):
            return "Avg"
        return def_tier_from_overall_rank(r, n_teams)

    tiers: list[str] = []
    for i in range(len(g)):
        if gp_num.iloc[i] < min_gp or not bool(has_metric.iloc[i]):
            # Goals: Avg for under-min-gp. Shots: blank when metric missing.
            if metric_col.startswith("shots") and not bool(has_metric.iloc[i]):
                tiers.append("")
                g.at[g.index[i], rank_col] = pd.NA
            else:
                tiers.append("Avg")
        else:
            tiers.append(_tier(g.at[g.index[i], rank_col]))
    g[tier_col] = tiers


def add_ranks_and_tiers(df: pd.DataFrame) -> pd.DataFrame:
    """Goals against → DEF_*; goals for → OFF_*; shots conceded → SHOTS_DEF_*."""
    out = []
    for league, grp in df.groupby("league"):
        min_gp = LEAGUE_MIN_GP.get(str(league), MIN_GP)
        g = grp.copy().reset_index(drop=True)
        gp_num = pd.to_numeric(g["gp"], errors="coerce").fillna(0)
        ok = gp_num >= min_gp
        n = int(ok.sum())
        # Early-season slates: if too few teams meet MIN_GP, rank on gp>=1 instead of all tier Avg @ rank 1.
        if n < 5 and min_gp > 1:
            min_gp = 1
            ok = gp_num >= min_gp
            n = int(ok.sum())
        mid = max(1, round((n + 1) / 2)) if n > 0 else 1

        _rank_metric_in_league(
            g,
            "goals_conceded_pg",
            ok=ok,
            n=n,
            mid=mid,
            min_gp=min_gp,
            gp_num=gp_num,
            rank_col="OVERALL_DEF_RANK",
            tier_col="DEF_TIER",
            ascending=True,
        )
        # Explicit goals aliases for prop-aware attach (same values as overall).
        g["GOALS_DEF_RANK"] = g["OVERALL_DEF_RANK"]
        g["GOALS_DEF_TIER"] = g["DEF_TIER"]

        if "goals_for_pg" in g.columns:
            _rank_metric_in_league(
                g,
                "goals_for_pg",
                ok=ok,
                n=n,
                mid=mid,
                min_gp=min_gp,
                gp_num=gp_num,
                rank_col="OFF_RANK",
                tier_col="OFF_TIER",
                ascending=False,
            )
            g["GOALS_OFF_RANK"] = g["OFF_RANK"]
            g["GOALS_OFF_TIER"] = g["OFF_TIER"]
        else:
            g["OFF_RANK"] = pd.Series([pd.NA] * len(g), dtype="Int64")
            g["OFF_TIER"] = ""
            g["GOALS_OFF_RANK"] = g["OFF_RANK"]
            g["GOALS_OFF_TIER"] = ""

        if "shots_conceded_pg" in g.columns:
            _rank_metric_in_league(
                g,
                "shots_conceded_pg",
                ok=ok,
                n=n,
                mid=mid,
                min_gp=min_gp,
                gp_num=gp_num,
                rank_col="SHOTS_DEF_RANK",
                tier_col="SHOTS_DEF_TIER",
                ascending=True,
            )
        else:
            g["SHOTS_DEF_RANK"] = pd.Series([pd.NA] * len(g), dtype="Int64")
            g["SHOTS_DEF_TIER"] = ""

        out.append(g)
    return pd.concat(out, ignore_index=True)


def write_team_context_csv(df: pd.DataFrame, path: Path) -> None:
    """Slim team attack/defense table for keep-gate joins (venue=ALL until H/A sourced)."""
    keep = [
        "pp_name",
        "league",
        "venue_split",
        "gp",
        "goals_for",
        "goals_for_pg",
        "goals_conceded",
        "goals_conceded_pg",
        "opp_gf_per_game",
        "shots_conceded_pg",
        "OFF_RANK",
        "OFF_TIER",
        "GOALS_OFF_RANK",
        "GOALS_OFF_TIER",
        "OVERALL_DEF_RANK",
        "DEF_TIER",
        "GOALS_DEF_RANK",
        "GOALS_DEF_TIER",
        "SHOTS_DEF_RANK",
        "SHOTS_DEF_TIER",
        "goals_for_pg_home",
        "goals_for_pg_away",
        "goals_conceded_pg_home",
        "goals_conceded_pg_away",
    ]
    out = df[[c for c in keep if c in df.columns]].copy()
    if "venue_split" not in out.columns:
        out["venue_split"] = "ALL"
    path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✅ team context → {path}  ({len(out)} rows)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="soccer_defense_summary.csv")
    ap.add_argument(
        "--team-context-out",
        default="",
        help="Optional path for soccer_team_context.csv (default: sibling of --out)",
    )
    ap.add_argument("--top", type=int, default=5)
    ap.add_argument(
        "--skip-shots",
        action="store_true",
        help="Skip core-API shotsFaced fetch (faster GF/OFF refresh)",
    )
    args = ap.parse_args()

    all_rows: List[dict] = []
    for slug, lname in LEAGUES:
        print(f"📡 Fetching {lname} ({slug})...")
        rows = fetch_league_defense(slug, lname, fetch_shots=not args.skip_shots)
        if rows:
            all_rows.extend(rows)
            print(f"  ✅ {lname}: {len(rows)} teams")
        else:
            print(f"  ⚠️  {lname}: no data")
        time.sleep(random.uniform(0.4, 0.8))

    if not all_rows:
        print("❌ No data fetched.")
        return

    df = pd.DataFrame(all_rows)
    if args.skip_shots:
        # Preserve prior shots_conceded_pg when refreshing GF-only.
        try:
            prior = pd.read_csv(args.out, encoding="utf-8-sig")
            if "pp_name" in prior.columns and "shots_conceded_pg" in prior.columns:
                prior = prior[["pp_name", "league", "shots_conceded_pg"]].copy()
                prior["pp_name"] = prior["pp_name"].astype(str).str.strip().str.upper()
                df["_pp"] = df["pp_name"].astype(str).str.strip().str.upper()
                prior = prior.rename(columns={"pp_name": "_pp", "shots_conceded_pg": "_sc_prior"})
                df = df.merge(prior[["_pp", "league", "_sc_prior"]], on=["_pp", "league"], how="left")
                df["shots_conceded_pg"] = df["_sc_prior"].where(df["_sc_prior"].notna(), df.get("shots_conceded_pg"))
                df = df.drop(columns=["_pp", "_sc_prior"], errors="ignore")
                print(f"  preserved shots_conceded_pg from {args.out}")
        except Exception as _e:
            print(f"  ⚠️  could not preserve prior shots: {_e}")

    df = add_ranks_and_tiers(df)

    front = [
        "team_name", "pp_name", "league", "gp",
        "goals_for", "goals_for_pg", "opp_gf_per_game",
        "goals_conceded", "goals_conceded_pg", "shots_conceded_pg",
        "clean_sheets", "wins", "draws", "losses",
        "OFF_RANK", "OFF_TIER", "GOALS_OFF_RANK", "GOALS_OFF_TIER",
        "OVERALL_DEF_RANK", "DEF_TIER",
        "GOALS_DEF_RANK", "GOALS_DEF_TIER",
        "SHOTS_DEF_RANK", "SHOTS_DEF_TIER",
        "venue_split",
        "goals_for_pg_home", "goals_for_pg_away",
        "goals_conceded_pg_home", "goals_conceded_pg_away",
    ]
    cols = front + [c for c in df.columns if c not in front and c != "team_id"]
    df   = df[[c for c in cols if c in df.columns]]

    try:
        df.to_csv(args.out, index=False, encoding="utf-8-sig")
    except PermissionError:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        alt   = args.out.replace(".csv", f"_{stamp}.csv")
        df.to_csv(alt, index=False, encoding="utf-8-sig")
        args.out = alt

    ctx_out = Path(args.team_context_out) if args.team_context_out else (
        Path(args.out).resolve().parent / "soccer_team_context.csv"
    )
    write_team_context_csv(df, ctx_out)

    print("\n" + "=" * 60)
    print("BEST DEFENSES (goals conceded/game)")
    print("=" * 60)
    for league, grp in df.groupby("league"):
        top = grp.sort_values("OVERALL_DEF_RANK").head(args.top)[
            ["pp_name", "OVERALL_DEF_RANK", "DEF_TIER", "goals_conceded_pg", "clean_sheets", "gp"]
        ]
        print(f"\n{league}")
        print(top.to_string(index=False))

    print("\n" + "=" * 60)
    print("STRONGEST ATTACKS (goals for/game)")
    print("=" * 60)
    if "OFF_RANK" in df.columns:
        for league, grp in df.groupby("league"):
            top = grp.sort_values("OFF_RANK").head(args.top)[
                [c for c in ["pp_name", "OFF_RANK", "OFF_TIER", "goals_for_pg", "gp"] if c in grp.columns]
            ]
            print(f"\n{league}")
            print(top.to_string(index=False))

    print(f"\n✅ Saved → {args.out}  ({len(df)} teams, {df['league'].nunique()} leagues)")
    sc = pd.to_numeric(df.get("shots_conceded_pg"), errors="coerce")
    fill = float(sc.notna().mean()) if len(df) else 0.0
    gf = pd.to_numeric(df.get("goals_for_pg"), errors="coerce")
    gf_fill = float(gf.notna().mean()) if len(df) else 0.0
    print(f"shots_conceded_pg fill={fill:.1%} (non-null {int(sc.notna().sum())}/{len(df)})")
    print(f"goals_for_pg fill={gf_fill:.1%} (non-null {int(gf.notna().sum())}/{len(df)})")
    print(f"Sample pp_names: {df['pp_name'].head(10).tolist()}")

    # ── Write to proporacle_ref.db ───────────────────────────────────────────────
    try:
        import sys as _sys
        from pathlib import Path as _Path
        # Search up to 6 levels for scripts/defense_db.py
        _here = _Path(__file__).resolve().parent
        for _ in range(6):
            if (_here / "scripts" / "defense_db.py").exists():
                _sys.path.insert(0, str(_here / "scripts"))
                break
            _here = _here.parent
        from defense_db import write_defense_to_db
        # Soccer uses pp_name as team key — write both pp_name and team columns
        # so load_defense_from_db can find either
        df_db = df.copy()
        df_db["pp_name"] = df_db["pp_name"].astype(str).str.strip().str.upper()
        df_db["team"]    = df_db["pp_name"]   # team = pp_name for soccer
        write_defense_to_db(df_db, sport="soccer")
        print(f"  ✅ defense_db: {len(df_db)} soccer teams written to DB")
    except Exception as _e:
        print(f"  ⚠️  Could not write to DB: {_e}")


if __name__ == "__main__":
    main()
