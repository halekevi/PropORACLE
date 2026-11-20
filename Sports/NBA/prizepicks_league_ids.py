"""PrizePicks NBA board family.

Verified 2026-10-11 via partner-api.prizepicks.com/leagues
(projections_count at check: NBA=225, NBAP=28, NBASZN=553).

NBA (7) and NBAP preseason (237) are fetched together, same pattern as
NFL (9) + NFLP (44). After preseason the NBAP tab goes away and league
237 returns 0 rows — no seasonal skip is needed.

Period boards (NBA1H/1Q) and WNBA stay single-id fetches when this script
is invoked with those league_ids.
"""

from __future__ import annotations

# Full-game regular season
NBA = "7"

# Preseason game board (live in October; empty once the RS tab takes over)
NBAP = "237"

# Season-long (not wired — Combined drops SZN boards)
NBASZN = "173"
NBASZN2 = "188"

# Period boards
NBA1H = "84"
NBA1Q = "192"
NBA2H = "80"
NBA4Q = "149"

# Summer league (not wired)
NBASL = "40"
NBASL2H = "310"

# WNBA ids this same step1 script is invoked with
WNBA = "3"
WNBA1H = "193"
WNBA1Q = "308"

DEFAULT_NBA_BOARDS: dict[str, str] = {
    NBA: "NBA",
    NBAP: "NBAP",
}

PERIOD_BOARDS: dict[str, str] = {
    NBA1H: "NBA1H",
    NBA1Q: "NBA1Q",
    NBA2H: "NBA2H",
    NBA4Q: "NBA4Q",
}

SEASON_BOARD_IDS = frozenset({NBASZN, NBASZN2})
GAME_FAMILY_IDS = frozenset({NBA, NBAP})
NBAP_LEAGUES = frozenset({"NBAP", "NBA PRESEASON", "NBA-PRESEASON", NBAP})

SPORT_TAG_BY_LEAGUE_ID: dict[str, str] = {
    NBA: "NBA",
    NBAP: "NBAP",
    NBA1H: "NBA1H",
    NBA1Q: "NBA1Q",
    NBA2H: "NBA2H",
    NBA4Q: "NBA4Q",
    NBASZN: "NBASZN",
    WNBA: "WNBA",
    WNBA1H: "WNBA1H",
    WNBA1Q: "WNBA1Q",
}


def is_nbap(league: object = None, league_id: object = None) -> bool:
    lg = str(league or "").strip().upper()
    if lg in NBAP_LEAGUES:
        return True
    lid = str(league_id or "").strip()
    return lid == NBAP


def resolve_nba_fetch_boards(
    *,
    include_preseason: bool = True,
    league_id: str = "",
) -> dict[str, str]:
    """Boards for one NBA step1 call.

    NBA (7) or NBAP (237) still pulls both game boards unless
    include_preseason is False. Any other id (WNBA 3, NBA1H 84, …) is
    that board only.
    """
    lid = str(league_id or "").strip()
    if not lid or lid in GAME_FAMILY_IDS:
        boards = dict(DEFAULT_NBA_BOARDS)
        if not include_preseason:
            boards = {NBA: "NBA"}
        return boards
    name = (
        SPORT_TAG_BY_LEAGUE_ID.get(lid)
        or PERIOD_BOARDS.get(lid)
        or f"NBA_{lid}"
    )
    return {lid: name}
