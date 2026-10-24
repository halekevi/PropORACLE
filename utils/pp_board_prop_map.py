"""PrizePicks board prop categories: card label is the source of truth.

One table per sport. Every card ``prop_type`` maps to one internal key with:
  - alias spellings (board chips, short forms, hyphen/plus variants)
  - box-score columns used to compute L5/L10
  - hit-rate cell name

A label with no mapping is never silently skipped — callers write
``unmapped_labels.csv`` and warn.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from utils.prop_norm import fold


@dataclass(frozen=True)
class PropCategory:
    key: str
    """Canonical internal key (matches ``canon_prop`` / HR cells where possible)."""

    card_label: str
    """Primary PrizePicks card wording — source of truth for display."""

    aliases: tuple[str, ...]
    """Other spellings (board chips, short forms). card_label is included implicitly."""

    box_cols: tuple[str, ...]
    """Box-score / DB columns (or SQL fragments) for L5/L10."""

    hr_cell: str
    """Hit-rate catalog / category cell name."""

    kind: str = "single"
    """single | combo | special | combo_player."""

    required_on_board: bool = True
    """If True, a non-empty slate with Points must also fetch this category."""


def _cat(
    key: str,
    card: str,
    *,
    aliases: tuple[str, ...] = (),
    box: tuple[str, ...] = (),
    hr: str = "",
    kind: str = "single",
    required: bool = True,
) -> PropCategory:
    return PropCategory(
        key=key,
        card_label=card,
        aliases=aliases,
        box_cols=box or (key,),
        hr_cell=hr or key,
        kind=kind,
        required_on_board=required,
    )


# ── NBA full-game board (card labels from PrizePicks) ─────────────────────────
NBA_CATEGORIES: tuple[PropCategory, ...] = (
    # Singles
    _cat("points", "Points", aliases=("Pts",), box=("pts",)),
    _cat("rebounds", "Rebounds", aliases=("Reb", "REB"), box=("reb",)),
    _cat("assists", "Assists", aliases=("Ast", "AST"), box=("ast",)),
    _cat("threes", "3-PT Made", aliases=("3PT Made", "3pm", "Three Pointers Made"), box=("fg3m",)),
    _cat(
        "threes_att",
        "3-PT Attempted",
        aliases=("3PT Attempted", "3pa", "Three Pointers Attempted"),
        box=("fg3a",),
    ),
    _cat("fgm", "FG Made", aliases=("Field Goals Made", "FGM"), box=("fgm",)),
    _cat("fga", "FG Attempted", aliases=("Field Goals Attempted", "FGA"), box=("fga",)),
    _cat(
        "fg2m",
        "Two Pointers Made",
        aliases=("2PT Made", "2pm", "2pts Made", "2 Pts Made"),
        box=("fg2m",),
    ),
    _cat(
        "fg2a",
        "Two Pointers Attempted",
        aliases=("2PT Attempted", "2pa", "2pts Attempted", "2 Pts Attempted"),
        box=("fg2a",),
    ),
    _cat("ftm", "Free Throws Made", aliases=("FT Made", "FTM"), box=("ftm",)),
    _cat(
        "fta",
        "Free Throws Attempted",
        aliases=("FT Attempted", "FTA"),
        box=("fta",),
    ),
    _cat(
        "dreb",
        "Defensive Rebounds",
        aliases=("Def Rebounds", "DREB"),
        box=("dreb",),
    ),
    _cat(
        "oreb",
        "Offensive Rebounds",
        aliases=("Off Rebounds", "OREB"),
        box=("oreb",),
    ),
    _cat("turnovers", "Turnovers", aliases=("TO", "TOV"), box=("tov",)),
    _cat("pf", "Personal Fouls", aliases=("Fouls", "PF"), box=("pf",)),
    _cat("steals", "Steals", aliases=("Stl", "STL"), box=("stl",)),
    _cat("blocks", "Blocked Shots", aliases=("Blocks", "Blk", "BLK"), box=("blk",)),
    _cat("dunks", "Dunks", aliases=("Dunk",), box=("dunks",), required=False),
    _cat(
        "fantasy",
        "Fantasy Score",
        aliases=("Fantasy", "FS"),
        box=("fantasy_score",),
        required=False,
    ),
    # Combos (single-player)
    _cat(
        "pra",
        "Pts+Rebs+Asts",
        aliases=("Pts-Rebs-Asts", "Pts+Reb+Ast", "PRA", "Points + Rebounds + Assists"),
        box=("pts", "reb", "ast"),
        kind="combo",
    ),
    _cat(
        "pts+reb",
        "Pts+Rebs",
        aliases=("Pts-Rebs", "PR", "Points + Rebounds"),
        box=("pts", "reb"),
        kind="combo",
    ),
    _cat(
        "pts+ast",
        "Pts+Asts",
        aliases=("Pts-Asts", "PA", "Points + Assists"),
        box=("pts", "ast"),
        kind="combo",
    ),
    _cat(
        "reb+ast",
        "Rebs+Asts",
        aliases=("Rebs-Asts", "RA", "Rebounds + Assists"),
        box=("reb", "ast"),
        kind="combo",
    ),
    _cat(
        "stocks",
        "Blks+Stls",
        aliases=("Blks-Stls", "Stocks", "Blocks + Steals"),
        box=("blk", "stl"),
        kind="combo",
    ),
    # Specials (yes/no) — out of O/U ranker
    _cat(
        "double_double",
        "Double-Double",
        aliases=("Double Double", "DD"),
        box=("pts", "reb", "ast", "stl", "blk"),
        kind="special",
        required=False,
    ),
    _cat(
        "triple_double",
        "Triple-Double",
        aliases=("Triple Double", "TD"),
        box=("pts", "reb", "ast", "stl", "blk"),
        kind="special",
        required=False,
    ),
    # Multi-player combos — out of 5/5 ranker until a rule exists
    _cat(
        "points_combo",
        "Points (Combo)",
        aliases=("Points Combo",),
        box=("pts",),
        kind="combo_player",
        required=False,
    ),
    _cat(
        "rebounds_combo",
        "Rebounds (Combo)",
        aliases=("Rebounds Combo",),
        box=("reb",),
        kind="combo_player",
        required=False,
    ),
    _cat(
        "assists_combo",
        "Assists (Combo)",
        aliases=("Assists Combo",),
        box=("ast",),
        kind="combo_player",
        required=False,
    ),
    _cat(
        "threes_combo",
        "3-PT Made (Combo)",
        aliases=("3-PT Made Combo", "3PT Made (Combo)"),
        box=("fg3m",),
        kind="combo_player",
        required=False,
    ),
)

# ── NHL ──────────────────────────────────────────────────────────────────────
NHL_CATEGORIES: tuple[PropCategory, ...] = (
    _cat("shots_on_goal", "Shots On Goal", aliases=("SOG", "Shots"), box=("sog",)),
    _cat("goals", "Goals", aliases=("G",), box=("goals",)),
    _cat("assists", "Assists", aliases=("A",), box=("assists",)),
    _cat("points", "Points", aliases=("Pts", "P"), box=("points",)),
    _cat("hits", "Hits", box=("hits",)),
    _cat("blocked_shots", "Blocked Shots", aliases=("Blocks", "BLK"), box=("blocks",)),
    _cat("faceoffs_won", "Faceoffs Won", aliases=("FOW", "Faceoffs"), box=("fow",)),
    _cat(
        "goalie_saves",
        "Goalie Saves",
        aliases=("Saves", "Goalie Save", "SV"),
        box=("saves",),
    ),
    _cat("plus_minus", "Plus/Minus", aliases=("Plus Minus", "+/-"), box=("plus_minus",)),
    _cat(
        "ppp",
        "Power Play Points",
        aliases=("PPP", "Power-Play Points"),
        box=("ppp",),
    ),
    _cat("toi", "Time On Ice", aliases=("TOI",), box=("toi",), required=False),
    _cat(
        "fantasy",
        "Fantasy Score",
        aliases=("Fantasy", "FS"),
        box=("fantasy_score",),
        required=False,
    ),
)

# ── Tennis ───────────────────────────────────────────────────────────────────
TENNIS_CATEGORIES: tuple[PropCategory, ...] = (
    _cat("total_games", "Total Games", aliases=("Match Total Games",), box=("total_games",)),
    _cat(
        "games_won",
        "Total Games Won",
        aliases=("Games Won", "Games Won by Player"),
        box=("games_won",),
    ),
    _cat("aces", "Aces", box=("aces",)),
    _cat(
        "double_faults",
        "Double Faults",
        aliases=("Double Fault", "DF"),
        box=("double_faults",),
    ),
    _cat(
        "break_points_won",
        "Break Points Won",
        aliases=("Break Points", "BP Won"),
        box=("bp_won",),
        required=False,
    ),
    _cat(
        "fantasy",
        "Fantasy Score",
        aliases=("Fantasy", "FS"),
        box=("fantasy_score",),
        required=False,
    ),
)

# ── Soccer ───────────────────────────────────────────────────────────────────
SOCCER_CATEGORIES: tuple[PropCategory, ...] = (
    _cat("shots", "Shots", box=("shots",)),
    _cat(
        "shots_on_target",
        "Shots On Target",
        aliases=("SOT", "Shots on Goal"),
        box=("sot",),
    ),
    _cat(
        "goalie_saves",
        "Goalie Saves",
        aliases=("Saves", "Goalie Save"),
        box=("saves",),
        required=False,  # listed; off tickets
    ),
    _cat("goals", "Goals", box=("goals",), required=False),
    _cat("assists", "Assists", box=("assists",), required=False),
    _cat("fouls", "Fouls", box=("fouls",), required=False),
    _cat(
        "fantasy",
        "Fantasy Score",
        aliases=("Fantasy", "Outfield Fantasy Score", "Goalie Fantasy Score"),
        box=("fantasy_score",),
        required=False,
    ),
)

# ── WNBA (same singles/combos shape as NBA; fewer shooting tabs posted) ──────
WNBA_CATEGORIES: tuple[PropCategory, ...] = (
    _cat("points", "Points", aliases=("Pts",), box=("pts",)),
    _cat("rebounds", "Rebounds", aliases=("Reb",), box=("reb",)),
    _cat("assists", "Assists", aliases=("Ast",), box=("ast",)),
    _cat("threes", "3-PT Made", aliases=("3PT Made", "3pm"), box=("fg3m",)),
    _cat("fgm", "FG Made", aliases=("Field Goals Made", "FGM"), box=("fgm",)),
    _cat("fga", "FG Attempted", aliases=("Field Goals Attempted", "FGA"), box=("fga",)),
    _cat(
        "pra",
        "Pts+Rebs+Asts",
        aliases=("Pts-Rebs-Asts", "PRA"),
        box=("pts", "reb", "ast"),
        kind="combo",
    ),
    _cat("pts+reb", "Pts+Rebs", aliases=("Pts-Rebs",), box=("pts", "reb"), kind="combo"),
    _cat("pts+ast", "Pts+Asts", aliases=("Pts-Asts",), box=("pts", "ast"), kind="combo"),
    _cat("reb+ast", "Rebs+Asts", aliases=("Rebs-Asts",), box=("reb", "ast"), kind="combo"),
    _cat("steals", "Steals", aliases=("Stl",), box=("stl",)),
    _cat("blocks", "Blocked Shots", aliases=("Blocks", "Blk"), box=("blk",)),
    _cat("turnovers", "Turnovers", aliases=("TO",), box=("tov",)),
    _cat(
        "fantasy",
        "Fantasy Score",
        aliases=("Fantasy", "FS"),
        box=("fantasy_score",),
        required=False,
    ),
)

# ── NFL ──────────────────────────────────────────────────────────────────────
NFL_CATEGORIES: tuple[PropCategory, ...] = (
    _cat("pass_yds", "Pass Yards", aliases=("Passing Yards",), box=("pass_yds",)),
    _cat("rush_yds", "Rush Yards", aliases=("Rushing Yards",), box=("rush_yds",)),
    _cat(
        "rec_yds",
        "Receiving Yards",
        aliases=("Rec Yards", "Receiving Yds"),
        box=("rec_yds",),
    ),
    _cat("receptions", "Receptions", aliases=("Rec",), box=("receptions",)),
    _cat(
        "pass_comp",
        "Pass Completions",
        aliases=("Completions", "Pass Comp"),
        box=("pass_comp",),
    ),
    _cat(
        "pass_att",
        "Pass Attempts",
        aliases=("Passing Attempts",),
        box=("pass_att",),
    ),
    _cat(
        "rush_att",
        "Rush Attempts",
        aliases=("Rushing Attempts",),
        box=("rush_att",),
    ),
    _cat(
        "rush_rec_yds",
        "Rush+Rec Yds",
        aliases=("Rush + Rec Yds", "Rush+Rec Yards"),
        box=("rush_yds", "rec_yds"),
        kind="combo",
    ),
    _cat(
        "pass_rush_yds",
        "Pass+Rush Yds",
        aliases=("Pass + Rush Yds",),
        box=("pass_yds", "rush_yds"),
        kind="combo",
    ),
    _cat(
        "fantasy",
        "Fantasy Score",
        aliases=("Fantasy", "FS"),
        box=("fantasy_score",),
        required=False,
    ),
)

# ── MLB ──────────────────────────────────────────────────────────────────────
MLB_CATEGORIES: tuple[PropCategory, ...] = (
    _cat("hits", "Hits", aliases=("H",), box=("hits",)),
    _cat("total_bases", "Total Bases", aliases=("TB",), box=("tb",)),
    _cat(
        "pitcher_ks",
        "Pitcher Strikeouts",
        aliases=("Strikeouts", "Pitcher Ks", "Ks"),
        box=("pitcher_ks",),
    ),
    _cat(
        "hits_allowed",
        "Hits Allowed",
        aliases=("HA",),
        box=("hits_allowed",),
    ),
    _cat(
        "pitching_outs",
        "Pitching Outs",
        aliases=("Outs",),
        box=("pitching_outs",),
    ),
    _cat(
        "walks_allowed",
        "Walks Allowed",
        aliases=("BB Allowed",),
        box=("walks_allowed",),
        required=False,  # not always posted on thin/postseason boards
    ),
    _cat(
        "era",
        "Earned Runs",
        aliases=("ER", "ERA", "Earned Run"),
        box=("er",),
        required=False,  # often absent; still listed when present
    ),
    _cat(
        "hrr",
        "Hits+Runs+RBIs",
        aliases=("H+R+RBI", "Hits + Runs + RBIs"),
        box=("hits", "runs", "rbi"),
        kind="combo",
    ),
    _cat(
        "fantasy",
        "Fantasy Score",
        aliases=("Fantasy", "FS"),
        box=("fantasy_score",),
        required=False,
    ),
)

SPORT_CATEGORIES: dict[str, tuple[PropCategory, ...]] = {
    "NBA": NBA_CATEGORIES,
    "NBAP": NBA_CATEGORIES,
    "NHL": NHL_CATEGORIES,
    "TENNIS": TENNIS_CATEGORIES,
    "SOCCER": SOCCER_CATEGORIES,
    "SOC": SOCCER_CATEGORIES,
    "WNBA": WNBA_CATEGORIES,
    "NFL": NFL_CATEGORIES,
    "NFLP": NFL_CATEGORIES,
    "MLB": MLB_CATEGORIES,
}


def _alias_index(cats: Iterable[PropCategory]) -> dict[str, PropCategory]:
    idx: dict[str, PropCategory] = {}
    for cat in cats:
        for label in (cat.card_label, *cat.aliases):
            idx[fold(label)] = cat
        idx[fold(cat.key)] = cat
    return idx


_SPORT_INDEX: dict[str, dict[str, PropCategory]] = {
    sport: _alias_index(cats) for sport, cats in SPORT_CATEGORIES.items()
}


def categories_for_sport(sport: str) -> tuple[PropCategory, ...]:
    return SPORT_CATEGORIES.get(str(sport or "").strip().upper(), ())


def lookup_category(sport: str, label: object) -> PropCategory | None:
    """Map a card/board label to its category. None = unmapped."""
    sport_u = str(sport or "").strip().upper()
    folded = fold(label)
    if not folded:
        return None
    idx = _SPORT_INDEX.get(sport_u)
    if idx is not None:
        return idx.get(folded)
    for cat in categories_for_sport(sport_u):
        if fold(cat.card_label) == folded or fold(cat.key) == folded:
            return cat
        if any(fold(a) == folded for a in cat.aliases):
            return cat
    return None


def board_card_labels(sport: str) -> tuple[str, ...]:
    """Primary card labels we expect on the board (source-of-truth wording)."""
    return tuple(c.card_label for c in categories_for_sport(sport))


def required_keys(sport: str) -> frozenset[str]:
    return frozenset(
        c.key for c in categories_for_sport(sport) if c.required_on_board
    )


def is_ou_ranker_prop(sport: str, label: object) -> bool:
    """False for yes/no specials and multi-player combos."""
    cat = lookup_category(sport, label)
    if cat is None:
        # Unmapped singles still go through the ranker; combo player names are
        # filtered separately via '+' in the player field.
        raw = str(label or "")
        if "(combo)" in raw.lower():
            return False
        return True
    return cat.kind in {"single", "combo"}


def box_cols_for(sport: str, label: object) -> tuple[str, ...]:
    cat = lookup_category(sport, label)
    return cat.box_cols if cat else ()
