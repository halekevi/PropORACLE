"""Canonical ESPN soccer league slugs for proporacle_ref.soccer.league.

Older ingest paths wrote display names (MLS, LaLiga, Serie A, …) while
build_boxscore_ref / fetch_actuals write ESPN slugs (usa.1, esp.1, …).
Hit-rate attach is by espn_player_id (not league), but competition gates,
defense joins, and league-sliced studies split history when both forms coexist.

Canonical form is the ESPN slug. Names map onto the matching slug.
"""

from __future__ import annotations

import re
from typing import Optional

# Display / alternate labels → ESPN slug. Keys are uppercased, spaces collapsed.
NAME_TO_SLUG: dict[str, str] = {
    # Big five + Americas
    "MLS": "usa.1",
    "MAJORLEAGUESOCCER": "usa.1",
    "NWSL": "usa.nwsl",
    "LALIGA": "esp.1",
    "LALIGA1": "esp.1",
    "LALIGAEA": "esp.1",
    "LALIGA2": "esp.2",
    "LALIGAHYPERMOTION": "esp.2",
    "SERIEA": "ita.1",
    "SERIEB": "ita.2",
    "BUNDESLIGA": "ger.1",
    "2.BUNDESLIGA": "ger.2",
    "2BUNDESLIGA": "ger.2",
    "LIGUE1": "fra.1",
    "LIGUE2": "fra.2",
    "PREMIERLEAGUE": "eng.1",
    "EPL": "eng.1",
    "CHAMPIONSHIP": "eng.2",
    "EFLCHAMPIONSHIP": "eng.2",
    "EFLCUP": "eng.league_cup",
    "CARABAOCUP": "eng.league_cup",
    "LEAGUECUP": "eng.league_cup",
    "LEAGUEONE": "eng.3",
    "LEAGUETWO": "eng.4",
    "FACUP": "eng.fa",
    # LatAm
    "LIGAMX": "mex.1",
    "LIGAMX,APERTURA": "mex.1",
    "LIGAMX,CLAUSURA": "mex.1",
    "LIGAMXAPERTURA": "mex.1",
    "LIGAMXCLAUSURA": "mex.1",
    "ARGENTINA": "arg.1",
    "LIGAPROFESIONAL": "arg.1",
    "LIGAPROFESIONALDEFUTBOL": "arg.1",
    "BRAZILSERIEA": "bra.1",
    "BRASILEIRAO": "bra.1",
    "BRASILEIRAOBETANO": "bra.1",
    "COPADOBRASIL": "bra.copa_do_brazil",
    "COPABETANODOBRASIL": "bra.copa_do_brazil",
    "PRIMERAA": "col.1",
    "LIGAPROSERIEA": "ecu.1",
    # UEFA / FIFA
    "UCL": "uefa.champions",
    "UEFACHAMPIONSLEAGUE": "uefa.champions",
    "UEL": "uefa.europa",
    "UEFAEUROPALEAGUE": "uefa.europa",
    "UEFACONFERENCELEAGUE": "uefa.europa.conf",
    "UEFANATIONSLEAGUE": "uefa.nations",
    "UEFANATIONS": "uefa.nations",
    "FIFAWORLDCUP": "fifa.world",
    "WORLDCUP": "fifa.world",
    "CLUBFRIENDLYGAMES": "fifa.friendly",
    "INTERNATIONALFRIENDLYGAMES": "fifa.friendly",
    "FIFAFRIENDLIES": "fifa.friendly",
    "WORLDCUPQUAL.UEFA": "fifa.worldq.uefa",
    "WORLDCUPQUALUEFA": "fifa.worldq.uefa",
    # Cups / other tracked
    "LEAGUESCUP": "concacaf.leagues.cup",
    "CONCACAFCHAMPIONSCUP": "concacaf.champions",
    "CONMEBOLLIBERTADORES": "conmebol.libertadores",
    "CONMEBOLSUDAMERICANA": "conmebol.sudamericana",
    "SAUDIPROLEAGUE": "ksa.1",
    "COPPAITALIA": "ita.coppa_italia",
    "DFBPOKAL": "ger.dfb_pokal",
    "COPADELREY": "esp.copa_del_rey",
    "KINGSCUP": "ksa.kings_cup",
    "SCOTTISHPREMIERSHIP": "sco.1",
    "LIGAPORTUGALBETCLIC": "por.1",
    "LIGAPORTUGAL": "por.1",
    "EREDIVISIE": "ned.1",
    "VRIENDENLOTERIJEREDIVISIE": "ned.1",
    "MLSNEXTPRO": "usa.mls.np",
}

# Slug → short display (optional helpers / defense joins).
SLUG_TO_NAME: dict[str, str] = {
    "usa.1": "MLS",
    "usa.nwsl": "NWSL",
    "esp.1": "La Liga",
    "ita.1": "Serie A",
    "ita.2": "Serie B",
    "ger.1": "Bundesliga",
    "fra.1": "Ligue 1",
    "eng.1": "EPL",
    "eng.2": "Championship",
    "eng.league_cup": "EFL Cup",
    "mex.1": "Liga MX",
    "arg.1": "Argentina",
    "bra.1": "Brasileirao",
    "uefa.champions": "UCL",
    "uefa.europa": "UEL",
    "uefa.nations": "UEFA Nations League",
    "fifa.world": "FIFA World Cup",
    "fifa.friendly": "FIFA Friendlies",
    "ksa.1": "Saudi Pro League",
    "col.1": "Primera A",
    "ecu.1": "LigaPro Serie A",
}


def _fold_label(raw: object) -> str:
    s = str(raw or "").strip()
    if not s:
        return ""
    # Unicode fold for Brasileirão etc.
    s = (
        s.replace("ã", "a")
        .replace("Ã", "A")
        .replace("á", "a")
        .replace("é", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ú", "u")
        .replace("ñ", "n")
    )
    s = s.upper()
    s = re.sub(r"\s+", "", s)
    s = s.replace("_", "")
    return s


def is_espn_slug(raw: object) -> bool:
    s = str(raw or "").strip().lower()
    if not s or " " in s:
        return False
    # usa.1, uefa.champions, fifa.worldq.uefa, eng.league_cup
    return bool(re.match(r"^[a-z0-9]+(?:\.[a-z0-9_]+)+$", s))


def normalize_soccer_league(raw: object) -> str:
    """Return ESPN slug when known; otherwise original stripped string."""
    s = str(raw or "").strip()
    if not s or s.upper() in {"NAN", "NONE", "NULL", "SOCCER", "—", "-"}:
        return ""
    if is_espn_slug(s):
        return s.lower()
    folded = _fold_label(s)
    if folded in NAME_TO_SLUG:
        return NAME_TO_SLUG[folded]
    # Soft prefixes for compound labels (e.g. "Liga MX, Apertura").
    # Longest key first; require next char to be non-alnum so "LaLiga 2"
    # does not collapse into "LaLiga".
    for key, slug in sorted(NAME_TO_SLUG.items(), key=lambda kv: -len(kv[0])):
        if len(key) < 6:
            continue
        if folded.startswith(key):
            rest = folded[len(key) :]
            if not rest or not rest[0].isalnum():
                return slug
    return s

def competition_key_league(raw: object) -> str:
    """Upper slug (or folded name) for competition_history keys."""
    norm = normalize_soccer_league(raw)
    if not norm:
        return ""
    if is_espn_slug(norm):
        return norm.upper()
    return _fold_label(norm)


def display_name(raw: object) -> Optional[str]:
    slug = normalize_soccer_league(raw)
    if not slug:
        return None
    return SLUG_TO_NAME.get(slug.lower(), slug)
