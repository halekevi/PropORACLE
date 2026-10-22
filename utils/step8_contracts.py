"""Per-sport step8 board contracts for nightly validation."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class Step8Contract:
    """Minimum schema + fill expectations for one sport's step8 board."""

    sport: str
    folder: str
    # Prefer Title Case (export) then snake_case aliases.
    required_any: tuple[tuple[str, ...], ...] = ()
    min_fill: dict[str, float] = field(default_factory=dict)
    # Columns that must not be stuck at a single non-null value (when n>=20).
    not_constant: tuple[str, ...] = ()
    # ML Prob column aliases; saturated check when present.
    ml_prob_cols: tuple[str, ...] = ("ML Prob", "ml_prob")
    game_date_cols: tuple[str, ...] = (
        "Game Date",
        "game_date",
        "board_date",
        "start_time",
        "game_start",
        "Game Start",
    )
    # Max share of rows allowed at <=0.01 or >=0.99 when ml_prob present.
    max_ml_saturated_share: float = 0.55
    # Require at least this many rows when the file exists (0 = any).
    min_rows: int = 1
    # Game Date may fall on slate_date .. slate_date+date_window_days (tennis).
    date_window_days: int = 0


# Shared core identity columns (any-of groups).
_CORE = (
    ("Player", "player"),
    ("Team", "team"),
    ("Prop", "prop_type", "Prop Type"),
    ("Line", "line"),
    ("Direction", "direction", "bet_direction"),
    ("Pick Type", "pick_type"),
    ("Game Date", "game_date", "board_date", "start_time", "game_start", "Game Start"),
)

_DEFAULT_FILL = {
    "Line": 0.95,
    "line": 0.95,
    "Player": 0.95,
    "player": 0.95,
}


def default_contracts() -> dict[str, Step8Contract]:
    """Sport slug → contract. Slugs match outputs/<date>/<folder>/."""
    specs: list[Step8Contract] = [
        Step8Contract(
            sport="NBA",
            folder="nba",
            required_any=_CORE
            + (
                ("Game Total", "game_total"),
                ("Spread", "spread"),
                ("team_pace", "Team Pace"),
            ),
            min_fill={
                **_DEFAULT_FILL,
                "Game Total": 0.50,
                "game_total": 0.50,
                "Spread": 0.50,
                "spread": 0.50,
                "team_pace": 0.40,
            },
            # Game Date is often constant on a single-slate board — do not flag that.
            not_constant=("Game Total", "game_total", "team_pace", "Line", "line", "Player", "player"),
            max_ml_saturated_share=0.50,
        ),
        Step8Contract(
            sport="NBA1Q",
            folder="nba1q",
            required_any=_CORE,
            min_fill=_DEFAULT_FILL,
            not_constant=("Line", "line", "Player", "player"),
        ),
        Step8Contract(
            sport="NBA1H",
            folder="nba1h",
            required_any=_CORE,
            min_fill=_DEFAULT_FILL,
            not_constant=("Line", "line", "Player", "player"),
        ),
        Step8Contract(
            sport="NHL",
            folder="nhl",
            required_any=_CORE
            + (
                ("goalie_name", "Goalie Name"),
                ("toi_avg_L10", "toi_per_game_api", "TOI"),
                ("game_total", "Game Total"),
            ),
            min_fill={
                **_DEFAULT_FILL,
                "goalie_name": 0.40,
                "toi_per_game_api": 0.40,
                "game_total": 0.40,
            },
            not_constant=("Line", "line", "Player", "player"),
            max_ml_saturated_share=0.45,
        ),
        Step8Contract(
            sport="Soccer",
            folder="soccer",
            required_any=_CORE,
            min_fill=_DEFAULT_FILL,
            not_constant=("Line", "line", "Player", "player"),
            max_ml_saturated_share=0.45,
        ),
        Step8Contract(
            sport="WNBA",
            folder="wnba",
            required_any=_CORE + (("Days Rest", "days_rest"),),
            min_fill={**_DEFAULT_FILL, "Days Rest": 0.80, "days_rest": 0.80},
            not_constant=("Days Rest", "days_rest", "Line", "line"),
        ),
        Step8Contract(
            sport="NFL",
            folder="nfl",
            required_any=_CORE[:-1] + (("Game Date", "game_date", "Game Time", "game_time"),),
            min_fill=_DEFAULT_FILL,
            not_constant=("Line", "line", "Player", "player"),
            game_date_cols=(
                "Game Date",
                "game_date",
                "board_date",
                "start_time",
                "game_start",
                "Game Start",
                "Game Time",
            ),
            date_window_days=7,
        ),
        Step8Contract(
            sport="CFB",
            folder="cfb",
            required_any=_CORE[:-1]
            + (("Game Date", "game_date", "Game Time", "game_time"),)
            + (("Projection", "projection"),),
            min_fill={**_DEFAULT_FILL, "Projection": 0.70},
            not_constant=("Line", "line", "Player", "player"),
            game_date_cols=(
                "Game Date",
                "game_date",
                "board_date",
                "start_time",
                "game_start",
                "Game Start",
                "Game Time",
            ),
            date_window_days=3,
        ),
        Step8Contract(
            sport="Tennis",
            folder="tennis",
            required_any=_CORE,
            min_fill=_DEFAULT_FILL,
            not_constant=("Line", "line", "Player", "player"),
            date_window_days=2,
        ),
        Step8Contract(
            sport="MLB",
            folder="mlb",
            required_any=_CORE,
            min_fill=_DEFAULT_FILL,
            not_constant=("Line", "line", "Player", "player"),
        ),
    ]
    return {c.folder: c for c in specs}
