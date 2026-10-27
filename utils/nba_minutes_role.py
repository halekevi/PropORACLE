"""NBA regular-season / preseason minutes + team leaders (flags only).

Reads `nba` rows from proporacle_ref.db. Classifies games by calendar:
  preseason — October before the data-driven RS opener (ESPN season_type=2)
  playoffs  — Apr 15 through June
  regular   — everything else in the season window

Used by step6 (fill mpg / Min Tier), step8 (Team Leaders sheet + column
order), and not_primary_playmaker (preseason assist ranks on current roster).
"""
from __future__ import annotations

import sqlite3
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from utils.basketball_minutes_tier import tier_minutes_from_mpg

_REPO = Path(__file__).resolve().parents[1]

# Stats shown on the Team Leaders sheet (board-facing labels).
LEADER_STATS: tuple[tuple[str, str], ...] = (
    ("pts", "Points"),
    ("fga", "FGA"),
    ("fgm", "FGM"),
    ("fg2a", "2PA"),
    ("fg2m", "2PM"),
    ("fg3a", "3PA"),
    ("fg3m", "3PM"),
    ("fta", "FTA"),
    ("ftm", "FTM"),
    ("reb", "Rebounds"),
    ("oreb", "OReb"),
    ("dreb", "DReb"),
    ("ast", "Assists"),
    ("tov", "Turnovers"),
    ("stl", "Steals"),
    ("blk", "Blocks"),
    ("pf", "Personal Fouls"),
    ("fantasy_score", "Fantasy Score"),
    ("pra", "PRA"),
    ("pr", "Pts+Rebs"),
    ("pa", "Pts+Asts"),
    ("ra", "Rebs+Asts"),
    ("bs", "Blks+Stls"),
)

ROTATION_MPG_MIN = 10.0
TOP_N = 3
BOTTOM_N = 3

# Slate / PrizePicks abbr → ESPN box `nba.team` codes in proporacle_ref.db
_TEAM_ALIASES = {
    "SAS": "SA",
    "SA": "SA",
    "NYK": "NY",
    "NY": "NY",
    "GSW": "GS",
    "GS": "GS",
    "NOP": "NO",
    "NO": "NO",
    "UTA": "UTAH",
    "UTAH": "UTAH",
    "WAS": "WSH",
    "WSH": "WSH",
    "PHX": "PHO",
    "PHO": "PHO",
    "BKN": "BK",
    "BK": "BK",
}


def _norm_team(v: object) -> str:
    t = str(v or "").strip().upper()
    if not t or t in {"NAN", "NONE", "NULL"}:
        return ""
    return _TEAM_ALIASES.get(t, t)


def _norm_name(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or "").strip().lower())
    return "".join(c for c in t if not unicodedata.combining(c))


def _parse_date(raw: object) -> date | None:
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    s = str(raw).strip()[:10]
    try:
        return date.fromisoformat(s)
    except ValueError:
        return None


def classify_game(d: date) -> str:
    """Return 'preseason' | 'playoffs' | 'regular'.

    Opener comes from ESPN (cached) when available — not a hardcoded Oct 22.
    """
    if (d.month == 4 and d.day >= 15) or d.month in {5, 6}:
        return "playoffs"
    opener = None
    try:
        from utils.nba_board_team import first_regular_season_date_public

        opener = first_regular_season_date_public(as_of=d)
    except Exception:
        opener = None
    if opener is not None:
        if d < opener and d.month in {9, 10}:
            return "preseason"
        return "regular"
    # Legacy fallback only when ESPN/cache unavailable.
    if d.month == 10 and d.day < 22:
        return "preseason"
    return "regular"


# Root cache is canonical. Sports/NBA copy is supplemental (playoff rows) only.
NBA_BOX_ROOT = _REPO / "data" / "cache" / "proporacle_ref.db"
NBA_BOX_MAX_STALE_DAYS = 3


def _db_path() -> Path:
    """Canonical NBA box DB = repo ``data/cache/proporacle_ref.db``.

    Do not fall through to the Sports/NBA copy — that path went stale through
    June while the root DB already had October openers.
    """
    if NBA_BOX_ROOT.exists():
        return NBA_BOX_ROOT
    try:
        from scripts.db_utils import default_db_path

        p = Path(default_db_path())
        if p.exists():
            return p
    except Exception:
        pass
    raise FileNotFoundError(
        f"NBA box DB missing at {NBA_BOX_ROOT}. "
        "Run: py -3.14 Sports/NBA/scripts/build_boxscore_ref.py "
        "--sports nba --backfill --days 7 "
        f"--db {NBA_BOX_ROOT}"
    )


def nba_box_max_date(df: pd.DataFrame | None = None) -> date | None:
    """Newest game_date in the loaded (or on-disk root) NBA boxes."""
    if df is not None and not df.empty and "game_date" in df.columns:
        mx = pd.to_datetime(df["game_date"], errors="coerce").max()
        if pd.notna(mx):
            return mx.date()
    root = _db_path()
    con = sqlite3.connect(str(root))
    try:
        row = con.execute("SELECT max(game_date) FROM nba").fetchone()
    finally:
        con.close()
    if not row or not row[0]:
        return None
    try:
        return date.fromisoformat(str(row[0])[:10])
    except ValueError:
        return None


def assert_nba_box_fresh(
    *,
    as_of: date | None = None,
    max_stale_days: int = NBA_BOX_MAX_STALE_DAYS,
    df: pd.DataFrame | None = None,
) -> date:
    """Raise if root/merged NBA boxes lag ``as_of`` by more than ``max_stale_days``."""
    as_of = as_of or date.today()
    mx = nba_box_max_date(df)
    if mx is None:
        raise RuntimeError(f"NBA box DB has no game_date rows (as_of={as_of})")
    lag = (as_of - mx).days
    if lag > int(max_stale_days):
        raise RuntimeError(
            f"NBA box DB stale: max game_date={mx} as_of={as_of} "
            f"lag={lag}d > {max_stale_days}d. "
            f"Refresh root DB: py -3.14 Sports/NBA/scripts/build_boxscore_ref.py "
            f"--sports nba --date {as_of.isoformat()} --db {NBA_BOX_ROOT}"
        )
    return mx


_NBA_BOX_SQL = (
    "SELECT game_date, player, team, position, minutes, pts, reb, ast, "
    "stl, blk, tov, fgm, fga, fg3m, fg3a, fg2m, fg2a, ftm, fta, "
    "oreb, dreb, pf, pra, pr, pa, ra, bs, fantasy_score, "
    "starter, season_window, preseason_year "
    "FROM nba"
)


def _nba_db_candidates() -> list[Path]:
    """Root DB first (required), then Sports/NBA only as playoff/supplement merge."""
    root = _db_path()  # raises if root missing
    out: list[Path] = [root]
    sports = _REPO / "Sports" / "NBA" / "data" / "cache" / "proporacle_ref.db"
    if sports.exists() and sports.resolve() != root.resolve():
        out.append(sports)
    return out


def _read_nba_boxes(path: Path) -> pd.DataFrame:
    con = sqlite3.connect(str(path))
    try:
        # Older Sports/NBA copies may lack starter / season_window cols.
        have = {r[1] for r in con.execute("PRAGMA table_info(nba)")}
        cols = [
            "game_date",
            "player",
            "team",
            "position",
            "minutes",
            "pts",
            "reb",
            "ast",
            "stl",
            "blk",
            "tov",
            "fgm",
            "fga",
            "fg3m",
            "fg3a",
            "fg2m",
            "fg2a",
            "ftm",
            "fta",
            "oreb",
            "dreb",
            "pf",
            "pra",
            "pr",
            "pa",
            "ra",
            "bs",
            "fantasy_score",
        ]
        for opt in ("starter", "season_window", "preseason_year"):
            if opt in have:
                cols.append(opt)
        sql = f"SELECT {', '.join(cols)} FROM nba"
        return pd.read_sql_query(sql, con)
    except Exception:
        return pd.DataFrame()
    finally:
        con.close()


@lru_cache(maxsize=1)
def load_nba_box_frame() -> pd.DataFrame:
    """Union of NBA box caches (player + game_date).

    Root ``data/cache/proporacle_ref.db`` is required and must be fresh
    (max game_date within ``NBA_BOX_MAX_STALE_DAYS`` of today).
    Sports/NBA copy may still merge in for playoff rows, but never as the
    sole source.
    """
    frames = [_read_nba_boxes(p) for p in _nba_db_candidates()]
    frames = [f for f in frames if f is not None and not f.empty]
    if not frames:
        raise RuntimeError(f"NBA box DB empty at {_db_path()}")
    df = pd.concat(frames, ignore_index=True)
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df["_pn"] = df["player"].map(_norm_name)
    # Prefer the row with more minutes when the same player/date appears twice.
    df["_min_sort"] = pd.to_numeric(df.get("minutes"), errors="coerce").fillna(-1)
    df = (
        df.sort_values(["_pn", "game_date", "_min_sort"], ascending=[True, True, False])
        .drop_duplicates(subset=["_pn", "game_date"], keep="first")
        .drop(columns=["_min_sort"], errors="ignore")
    )
    df["window"] = df["game_date"].dt.date.map(
        lambda d: classify_game(d) if isinstance(d, date) else "regular"
    )
    df["team"] = df["team"].map(_norm_team)
    for c in (
        "minutes",
        "pts",
        "reb",
        "ast",
        "stl",
        "blk",
        "tov",
        "fgm",
        "fga",
        "fg3m",
        "fg3a",
        "fg2m",
        "fg2a",
        "ftm",
        "fta",
        "oreb",
        "dreb",
        "pf",
        "pra",
        "pr",
        "pa",
        "ra",
        "bs",
        "fantasy_score",
    ):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    assert_nba_box_fresh(as_of=date.today(), df=df)
    return df


def clear_nba_box_frame_cache() -> None:
    load_nba_box_frame.cache_clear()


@dataclass
class PlayerMinutesProfile:
    player: str
    team: str
    min_season_avg: float | None
    min_preseason_avg: float | None
    min_per_game: float | None  # alias of season RS
    season_gp: int
    preseason_gp: int
    preseason_ast_avg: float | None
    season_ast_avg: float | None
    role_mpg: float | None  # preseason+season blend for Min Tier
    # ESPN starter share in current-campaign preseason (None if no starter col / games)
    preseason_starter_rate: float | None = None
    preseason_team: str = ""


def _avg(vals: Iterable[float]) -> float | None:
    xs = [float(v) for v in vals if v is not None and v == v]
    if not xs:
        return None
    return sum(xs) / len(xs)


ROLE_CHANGE_MIN_GAP = 8.0


# Role-status bands (starter_flag / role_changed). Wider than Min Tier so
# short preseason ramp minutes (≈16–20) stay "rotation", not false "bench".
# Min Tier itself still uses basketball_minutes_tier (<=20 LOW / <=30 MED / >30 HIGH).
_ROLE_STARTER_MPG = 30.0
_ROLE_BENCH_MPG = 16.0


def _status_from_mpg(mpg: float | None) -> str:
    """starter / rotation / bench for role_changed + Starter Pre print.

    starter >30; rotation 16–30; bench <16. Preseason rest at ~18 mpg is
    rotation (not a starter↔bench flip).
    """
    if mpg is None:
        return ""
    try:
        v = float(mpg)
    except (TypeError, ValueError):
        return ""
    if v != v:  # NaN
        return ""
    if v > _ROLE_STARTER_MPG:
        return "starter"
    if v >= _ROLE_BENCH_MPG:
        return "rotation"
    return "bench"


def _role_mpg(season: float | None, pre: float | None) -> float | None:
    """Min Tier input: season minutes first. Preseason is not blended in.

    Preseason only feeds the role_changed flag (see attach_minutes_role_columns).
    Fall back to preseason only when season mpg is missing.
    """
    if season is not None:
        return season
    return pre


def _is_starter_bench_flip(season_status: str, pre_status: str) -> bool:
    """Only starter↔bench counts (ramp starter↔rotation is not a role change)."""
    pair = {season_status, pre_status}
    return pair == {"starter", "bench"}


def role_changed_from_minutes(
    season: float | None, pre: float | None, *, gap: float = ROLE_CHANGE_MIN_GAP
) -> bool:
    """True when |pre−season| >= gap AND status flips starter↔bench.

    Starter↔rotation (preseason ramp) is NOT a role change.
    """
    if season is None or pre is None:
        return False
    if abs(float(pre) - float(season)) < float(gap):
        return False
    return _is_starter_bench_flip(_status_from_mpg(season), _status_from_mpg(pre))


CLEAN_MINUTES_GAP = 3.0


def clean_minutes_ok(
    *,
    gap: float | None,
    minutes_flag: str,
    season_status: str,
    pre_status: str,
) -> bool:
    """Clean pool: |L5 gap| <= 3, no minutes_up/down, starter status unchanged."""
    if gap is None:
        return False
    if abs(float(gap)) > CLEAN_MINUTES_GAP:
        return False
    toks = {t.strip() for t in str(minutes_flag or "").split("|") if t.strip()}
    if "minutes_up" in toks or "minutes_down" in toks:
        return False
    if "no_minutes_data" in toks:
        return False
    if not season_status or not pre_status:
        # need both sides to confirm status unchanged
        return False
    return season_status == pre_status


def player_minutes_profiles(
    *,
    as_of: date | None = None,
) -> dict[str, PlayerMinutesProfile]:
    """Key = normalized player name. Uses latest team from most recent game.

    min_season_avg / min_per_game = regular-season mpg:
      - once the player has 3+ current-season RS games, use those only
      - otherwise last completed RS year (exposes playoff L5 shrink vs RS)
    Min Tier / role_mpg = season minutes (starter status). Preseason is stored
    separately and only drives the role_changed flag on attach.
    """
    df = load_nba_box_frame()
    if df.empty:
        return {}
    as_of = as_of or date.today()
    # Season year = year of the Oct tip for this campaign.
    season_year = as_of.year if as_of.month >= 10 else as_of.year - 1
    # Current RS window (may be empty / thin in opener week).
    cur_rs_start = date(season_year, 10, 22)
    cur_rs_end = date(season_year + 1, 4, 14)
    # Prior completed RS (fallback until 3 current RS games).
    prior_rs_start = date(season_year - 1, 10, 22)
    prior_rs_end = date(season_year, 4, 14)
    pre_start = date(season_year, 10, 1)
    pre_end = date(season_year, 10, 21)

    out: dict[str, PlayerMinutesProfile] = {}
    for pn, g in df.groupby("_pn"):
        g = g.sort_values("game_date")
        latest = g.iloc[-1]
        team = _norm_team(latest["team"])
        player = str(latest["player"] or "")

        def _rs_slice(start: date, end: date) -> pd.DataFrame:
            sub = g[
                (g["game_date"].dt.date >= start)
                & (g["game_date"].dt.date <= end)
                & (g["window"] == "regular")
            ]
            if sub.empty:
                sub = g[
                    (g["game_date"].dt.date >= start)
                    & (g["game_date"].dt.date <= end)
                    & (g["window"] != "preseason")
                    & (g["window"] != "playoffs")
                ]
            return sub

        cur_rs = _rs_slice(cur_rs_start, min(as_of, cur_rs_end))
        prior_rs = _rs_slice(prior_rs_start, prior_rs_end)
        # Switch to current-season mpg after 3+ RS games.
        if len(cur_rs) >= 3:
            rs = cur_rs
        else:
            rs = prior_rs if len(prior_rs) else cur_rs

        pre = g[
            (g["game_date"].dt.date >= pre_start)
            & (g["game_date"].dt.date <= pre_end)
            & (g["window"] == "preseason")
        ]

        min_s = _avg(rs["minutes"].dropna().tolist())
        min_p = _avg(pre["minutes"].dropna().tolist())
        # Season first for Min Tier — do not dilute starters with ramp-up minutes.
        role = _role_mpg(min_s, min_p)
        ast_s = _avg(rs["ast"].dropna().tolist()) if len(rs) else None
        ast_p = _avg(pre["ast"].dropna().tolist()) if len(pre) else None
        starter_rate = None
        pre_team = ""
        if len(pre) and "starter" in pre.columns:
            st = pd.to_numeric(pre["starter"], errors="coerce").dropna()
            if len(st):
                starter_rate = float(st.mean())
        if len(pre) and "team" in pre.columns:
            # most common preseason team
            vc = pre["team"].dropna().astype(str).value_counts()
            if len(vc):
                pre_team = _norm_team(vc.index[0])
        out[str(pn)] = PlayerMinutesProfile(
            player=player,
            team=team,
            min_season_avg=min_s,
            min_preseason_avg=min_p,
            min_per_game=min_s,
            season_gp=int(len(rs)),
            preseason_gp=int(len(pre)),
            preseason_ast_avg=ast_p,
            season_ast_avg=ast_s,
            role_mpg=role,
            preseason_starter_rate=starter_rate,
            preseason_team=pre_team,
        )
    return out


def _status_from_preseason_starter(
    starter_rate: float | None, mpg: float | None
) -> str:
    """Prefer ESPN starter share; fall back to MPG bands."""
    if starter_rate is not None:
        if starter_rate >= 0.5:
            return "starter"
        # Played but mostly came off the bench
        return "bench"
    return _status_from_mpg(mpg)


def attach_minutes_role_columns(df: pd.DataFrame, *, as_of: date | None = None) -> pd.DataFrame:
    """Fill season/preseason minutes, l5 gap, Min Tier from season mpg.

    Preseason does not pull Min Tier down for rested starters. role_changed
    only when |pre−season| >= 8 and status flips starter↔bench. Blank minute
    fields get no_minutes_data. clean_minutes marks the uncontaminated pool.
    """
    out = df.copy()
    profiles = player_minutes_profiles(as_of=as_of)

    def _ensure(col: str, default=None) -> None:
        if col not in out.columns:
            out[col] = default if default is not None else pd.NA

    for col in (
        "min_season_avg",
        "min_per_game",
        "min_preseason_avg",
        "l5_minutes_gap",
        "role_mpg",
        "starter_flag",
        "starter_flag_preseason",
        "minutes_tier_l5",
        "minutes_flag",
        "clean_minutes",
        "season_gp",
        "preseason_gp",
    ):
        _ensure(col)

    player_col = "player" if "player" in out.columns else ("Player" if "Player" in out.columns else None)
    if player_col is None:
        return out

    min_l5 = pd.to_numeric(out.get("min_last5_avg"), errors="coerce")
    # Preserve L5-based tier in its own column before overwrite
    if "minutes_tier" in out.columns:
        out["minutes_tier_l5"] = out["minutes_tier"]
    else:
        out["minutes_tier_l5"] = min_l5.apply(tier_minutes_from_mpg)

    season_avg: list[Any] = []
    pre_avg: list[Any] = []
    per_game: list[Any] = []
    gaps: list[Any] = []
    role_mpgs: list[Any] = []
    starters: list[str] = []
    starters_pre: list[str] = []
    tiers: list[str] = []
    min_flags: list[str] = []
    clean_flags: list[bool] = []
    s_gp: list[Any] = []
    p_gp: list[Any] = []
    # Also rewrite display blanks on min_last5_avg when missing
    min_l5_out: list[Any] = []

    for i, row in out.iterrows():
        pn = _norm_name(row.get(player_col))
        prof = profiles.get(pn)
        ms = prof.min_season_avg if prof else None
        mp = prof.min_preseason_avg if prof else None
        rm = _role_mpg(ms, mp)
        l5_raw = min_l5.loc[i] if i in min_l5.index else None
        try:
            l5f = None if l5_raw is None or pd.isna(l5_raw) else float(l5_raw)
        except (TypeError, ValueError):
            l5f = None
        gap = (l5f - ms) if (l5f is not None and ms is not None) else None

        missing_bits = []
        if l5f is None:
            missing_bits.append("l5")
        if ms is None:
            missing_bits.append("season")
        if mp is None:
            missing_bits.append("preseason")

        # Display: blank → no_minutes_data token for missing numeric cells
        season_avg.append(round(ms, 2) if ms is not None else "no_minutes_data")
        pre_avg.append(round(mp, 2) if mp is not None else "no_minutes_data")
        per_game.append(round(ms, 2) if ms is not None else "no_minutes_data")
        gaps.append(round(gap, 2) if gap is not None else "no_minutes_data")
        role_mpgs.append(round(rm, 2) if rm is not None else "no_minutes_data")
        min_l5_out.append(round(l5f, 2) if l5f is not None else "no_minutes_data")

        tier = tier_minutes_from_mpg(rm)
        tiers.append(tier if rm is not None else "UNKNOWN")
        status = _status_from_mpg(rm)
        status_pre = _status_from_preseason_starter(
            prof.preseason_starter_rate if prof else None, mp
        )
        starters.append(status if status else "unknown")
        starters_pre.append(status_pre if status_pre else "unknown")

        flags: list[str] = []
        if missing_bits:
            flags.append("no_minutes_data")
        # role_changed: |pre−season| mpg >= 8 AND starter↔bench (ESPN starter preferred)
        if (
            ms is not None
            and mp is not None
            and abs(float(mp) - float(ms)) >= ROLE_CHANGE_MIN_GAP
            and _is_starter_bench_flip(status, status_pre)
        ):
            flags.append("role_changed")
        if (
            prof
            and prof.preseason_team
            and prof.team
            and prof.preseason_team != prof.team
        ):
            flags.append("team_changed")
        if status == "bench" or (l5f is not None and l5f < 20):
            flags.append("bench_minutes_L5")
        if isinstance(gap, (int, float)):
            if gap >= 5:
                flags.append("minutes_up")
            elif gap <= -5:
                flags.append("minutes_down")
        raw_prior = row.get("minutes_flag")
        try:
            if raw_prior is not None and not pd.isna(raw_prior):
                prior = str(raw_prior).strip()
                if prior and prior.lower() not in {"nan", "none", "null", "<na>"}:
                    for tok in prior.split("|"):
                        tok = tok.strip()
                        if tok and tok not in flags:
                            flags.append(tok)
        except (TypeError, ValueError):
            pass
        flag_s = "|".join(flags) if flags else ""
        min_flags.append(flag_s)
        clean_flags.append(
            clean_minutes_ok(
                gap=gap,
                minutes_flag=flag_s,
                season_status=status,
                pre_status=status_pre,
            )
        )
        s_gp.append(int(prof.season_gp) if prof else "no_minutes_data")
        p_gp.append(int(prof.preseason_gp) if prof else "no_minutes_data")

    out["min_last5_avg"] = min_l5_out
    out["min_season_avg"] = season_avg
    out["min_per_game"] = per_game
    out["min_preseason_avg"] = pre_avg
    out["l5_minutes_gap"] = gaps
    out["role_mpg"] = role_mpgs
    out["minutes_tier"] = tiers
    out["starter_flag"] = starters
    out["starter_flag_preseason"] = starters_pre
    out["minutes_flag"] = min_flags
    out["clean_minutes"] = clean_flags
    out["season_gp"] = s_gp
    out["preseason_gp"] = p_gp
    return out


def preseason_assist_ranks_by_team(
    *,
    as_of: date | None = None,
    min_mpg: float = ROTATION_MPG_MIN,
) -> dict[tuple[str, str], int]:
    """(team, player_norm) → 1-based rank by preseason APG among rotation (mpg>=min)."""
    profiles = player_minutes_profiles(as_of=as_of)
    by_team: dict[str, list[tuple[str, float, float]]] = {}
    for pn, prof in profiles.items():
        if prof.preseason_ast_avg is None:
            continue
        mpg = prof.min_preseason_avg
        if mpg is None or mpg < min_mpg:
            # still include if they have preseason assists but low minutes? no — rotation only
            continue
        by_team.setdefault(prof.team, []).append(
            (pn, float(prof.preseason_ast_avg), float(mpg))
        )
    ranks: dict[tuple[str, str], int] = {}
    for team, rows in by_team.items():
        rows.sort(key=lambda t: (-t[1], -t[2], t[0]))
        for i, (pn, _a, _m) in enumerate(rows, 1):
            ranks[(team, pn)] = i
    return ranks


def current_roster_by_team(*, as_of: date | None = None) -> dict[str, set[str]]:
    """Players who appeared for a team in the latest preseason (else latest RS games)."""
    df = load_nba_box_frame()
    if df.empty:
        return {}
    as_of = as_of or date.today()
    if as_of.month >= 10:
        pre = df[(df["window"] == "preseason") & (df["game_date"].dt.year == as_of.year)]
    else:
        pre = df[
            (df["window"] == "preseason") & (df["game_date"].dt.year == as_of.year - 1)
        ]
    src = pre if len(pre) else df[df["window"] == "regular"]
    out: dict[str, set[str]] = {}
    for _, r in src.iterrows():
        out.setdefault(_norm_team(r["team"]), set()).add(str(r["_pn"]))
    return out


def _season_pre_frames(as_of: date) -> tuple[pd.DataFrame, pd.DataFrame]:
    df = load_nba_box_frame()
    if as_of.month >= 10:
        pre = df[(df["window"] == "preseason") & (df["game_date"].dt.year == as_of.year)]
        rs_start = date(as_of.year - 1, 10, 22)
        rs_end = date(as_of.year, 4, 14)
    else:
        pre = df[
            (df["window"] == "preseason") & (df["game_date"].dt.year == as_of.year - 1)
        ]
        rs_start = date(as_of.year - 1, 10, 22)
        rs_end = date(as_of.year, 4, 14)
    rs = df[
        (df["window"] == "regular")
        & (df["game_date"].dt.date >= rs_start)
        & (df["game_date"].dt.date <= rs_end)
    ]
    return pre, rs


def _blended_player_avgs(
    pn: str,
    team: str,
    pre: pd.DataFrame,
    rs: pd.DataFrame,
    *,
    season_weight: float = 0.7,
) -> dict[str, Any] | None:
    """Blend last-season RS per-game with preseason for current-roster players.

    PerGame = 0.7*RS + 0.3*pre when both exist; else whichever exists.
    Rotation filter uses season minutes when available, else preseason.
    """
    pre_g = pre[(pre["_pn"] == pn) & (pre["team"] == team)]
    rs_g = rs[(rs["_pn"] == pn) & (rs["team"] == team)]
    if len(pre_g) == 0 and len(rs_g) == 0:
        pre_g = pre[pre["_pn"] == pn]
        rs_g = rs[rs["_pn"] == pn]
    if len(pre_g) == 0 and len(rs_g) == 0:
        return None

    def _means(g: pd.DataFrame) -> dict[str, float]:
        out: dict[str, float] = {}
        if len(g) == 0:
            return out
        out["minutes"] = float(g["minutes"].mean())
        for col, _label in LEADER_STATS:
            if col in g.columns:
                out[col] = float(g[col].mean())
        return out

    pre_m = _means(pre_g)
    rs_m = _means(rs_g)
    mpg = rs_m.get("minutes", pre_m.get("minutes"))
    if mpg is None or mpg < ROTATION_MPG_MIN:
        return None

    if rs_m and pre_m:
        source = "blend"
        w_s, w_p = float(season_weight), 1.0 - float(season_weight)
        av: dict[str, Any] = {
            "minutes": w_s * rs_m["minutes"] + w_p * pre_m.get("minutes", rs_m["minutes"])
        }
        for col, _label in LEADER_STATS:
            if col in rs_m and col in pre_m:
                av[col] = w_s * rs_m[col] + w_p * pre_m[col]
            elif col in rs_m:
                av[col] = rs_m[col]
            elif col in pre_m:
                av[col] = pre_m[col]
    elif rs_m:
        source = "regular_season"
        av = dict(rs_m)
    else:
        source = "preseason"
        av = dict(pre_m)

    name_src = rs_g if len(rs_g) else pre_g
    av["_player"] = str(name_src.iloc[-1]["player"])
    av["_gp_rs"] = int(len(rs_g))
    av["_gp_pre"] = int(len(pre_g))
    av["_gp"] = int(len(rs_g) + len(pre_g))
    av["_source"] = source
    av["_pn"] = pn
    return av


def build_team_leaders_sheet(
    teams: Iterable[str],
    *,
    as_of: date | None = None,
) -> pd.DataFrame:
    """Top 3 + bottom 3 rotation players: blend last-season RS + preseason."""
    df = load_nba_box_frame()
    if df.empty:
        return pd.DataFrame()
    as_of = as_of or date.today()
    profiles = player_minutes_profiles(as_of=as_of)
    roster = current_roster_by_team(as_of=as_of)
    want = {_norm_team(t) for t in teams if _norm_team(t)}
    pre, rs = _season_pre_frames(as_of)

    rows: list[dict[str, Any]] = []
    for team in sorted(want):
        members = set(roster.get(team) or set())
        for pn, prof in profiles.items():
            if prof.team == team:
                members.add(pn)
        if not members:
            continue

        player_avgs: list[dict[str, Any]] = []
        for pn in members:
            av = _blended_player_avgs(pn, team, pre, rs)
            if av:
                player_avgs.append(av)
        if not player_avgs:
            continue

        for col, label in LEADER_STATS:
            ranked = [p for p in player_avgs if col in p]
            if len(ranked) < 2:
                continue
            ranked.sort(key=lambda p: (-float(p[col]), -float(p["minutes"])))
            n = len(ranked)
            for side, slice_rows in (
                ("top", ranked[:TOP_N]),
                ("bottom", list(reversed(ranked[-BOTTOM_N:]))),
            ):
                for i, p in enumerate(slice_rows, 1):
                    rows.append(
                        {
                            "Team": team,
                            "Stat": label,
                            "stat_key": col,
                            "Side": side,
                            "Rank": i if side == "top" else (n - len(slice_rows) + i),
                            "Player": p["_player"],
                            "PerGame": round(float(p[col]), 2),
                            "Minutes": round(float(p["minutes"]), 1),
                            "GP": int(p["_gp"]),
                            "GP_RS": int(p["_gp_rs"]),
                            "GP_Pre": int(p["_gp_pre"]),
                            "Source": p["_source"],
                            "TeamN": n,
                        }
                    )
    return pd.DataFrame(rows)


def full_rotation_stat_ranks(
    teams: Iterable[str],
    *,
    as_of: date | None = None,
) -> tuple[dict[tuple[str, str, str], int], dict[tuple[str, str], int]]:
    """Full 1..N ranks for every rotation player (blended RS + preseason)."""
    df = load_nba_box_frame()
    if df.empty:
        return {}, {}
    as_of = as_of or date.today()
    profiles = player_minutes_profiles(as_of=as_of)
    roster = current_roster_by_team(as_of=as_of)
    want = {_norm_team(t) for t in teams if _norm_team(t)}
    pre, rs = _season_pre_frames(as_of)

    ranks: dict[tuple[str, str, str], int] = {}
    team_ns: dict[tuple[str, str], int] = {}
    for team in sorted(want):
        members = set(roster.get(team) or set())
        for pn, prof in profiles.items():
            if prof.team == team:
                members.add(pn)
        player_avgs: list[dict[str, Any]] = []
        for pn in members:
            av = _blended_player_avgs(pn, team, pre, rs)
            if av:
                player_avgs.append(av)
        for col, label in LEADER_STATS:
            ranked = [p for p in player_avgs if col in p]
            if len(ranked) < 2:
                continue
            ranked.sort(key=lambda p: (-float(p[col]), -float(p["minutes"])))
            team_ns[(team, label)] = len(ranked)
            for i, p in enumerate(ranked, 1):
                ranks[(team, label, p["_pn"])] = i
    return ranks, team_ns


def team_stat_rank_for_player(
    player: str,
    team: str,
    stat_key: str,
    *,
    as_of: date | None = None,
) -> tuple[int | None, int | None]:
    """Return (rank, team_n) from the full rotation set for this stat."""
    label = dict(LEADER_STATS).get(stat_key, "")
    if not label:
        return None, None
    ranks, team_ns = full_rotation_stat_ranks([team], as_of=as_of)
    tm = _norm_team(team)
    pn = _norm_name(player)
    return ranks.get((tm, label, pn)), team_ns.get((tm, label))
