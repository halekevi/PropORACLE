"""PrizePicks NFL board family.

Verified 2026-08-15 via partner-api.prizepicks.com/leagues
(projections_count at check: NFL=236, NFLP=49, NFLSZN=1345).

NFL (9) and NFLP (44) are always fetched together. After preseason the NFLP
tab goes away and league 44 returns 0 rows — no seasonal skip is needed.
"""

from __future__ import annotations

# Single-game (regular + in-season)
NFL = "9"

# Preseason game board (live in August; empty once preseason ends)
NFLP = "44"

# Season-long regular-season totals (NFLSZN tab). Not used on Combined.
NFLSZN = "163"

# Period boards (empty until regular season kickoff)
NFL1H = "35"
NFL2H = "25"
NFL1Q = "245"
NFL4Q = "152"

# Game-family fetch: NFL + NFLP equally. NFLSZN is opt-in only.
DEFAULT_NFL_BOARDS: dict[str, str] = {
    NFL: "NFL",
    NFLP: "NFLP",
}

PERIOD_BOARDS: dict[str, str] = {
    NFL1H: "NFL1H",
    NFL2H: "NFL2H",
    NFL1Q: "NFL1Q",
    NFL4Q: "NFL4Q",
}

SEASON_BOARD_IDS = frozenset({NFLSZN})
GAME_FAMILY_IDS = frozenset({NFL, NFLP})


def resolve_nfl_fetch_boards(
    *,
    include_season: bool = False,
    include_preseason: bool = True,
    include_halves: bool = False,
    league_id: str = "",
) -> dict[str, str]:
    """Boards for one NFL step1 call.

    NFL (9) and NFLP (44) are the same fetch. Asking for either id still pulls
    both unless include_preseason is False. Empty NFLP after preseason is fine.
    """
    boards = dict(DEFAULT_NFL_BOARDS)
    if not include_preseason:
        boards = {NFL: "NFL"}
    if include_season:
        boards[NFLSZN] = "NFLSZN"
    if include_halves:
        boards.update(PERIOD_BOARDS)
    lid = str(league_id or "").strip()
    if not lid or lid in GAME_FAMILY_IDS:
        return boards
    name = boards.get(lid) or DEFAULT_NFL_BOARDS.get(lid) or PERIOD_BOARDS.get(lid) or f"NFL_{lid}"
    return {lid: name}
