"""NFL team / league leader and bottom ranks from ESPN boxscore cache.

Ranks are ours (2025 regular-season game logs vs today's line is a later
step). Eligible floors drop zero-volume specialists so "bottom" means the
low end of players who actually do that job.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from utils.nfl_espn_context import canon_nfl_abbr
from utils.nfl_prop_defense import last_completed_nfl_season

_REPO = Path(__file__).resolve().parents[1]
CACHE_PATH = _REPO / "Sports/NFL/data/cache/nfl_boxscore_cache.csv"
DEFENSE_PATH = _REPO / "data/reference/nfl_team_defense.csv"
CSV_PATH = _REPO / "Sports/NFL/data/nfl_stat_leaders.csv"
BOARDS_PATH = _REPO / "Sports/NFL/data/nfl_stat_leaders_boards.json"

NFL_STAT_CATS: tuple[dict[str, Any], ...] = (
    {"id": "pass_yds", "label": "Pass yards", "col": "PASS_YDS", "min_avg": 50.0, "min_games": 4, "threshold": 200.0},
    {"id": "rush_yds", "label": "Rush yards", "col": "RUSH_YDS", "min_avg": 15.0, "min_games": 4, "threshold": 60.0},
    {"id": "rec_yds", "label": "Receiving yards", "col": "REC_YDS", "min_avg": 20.0, "min_games": 4, "threshold": 50.0},
    {"id": "receptions", "label": "Receptions", "col": "REC", "min_avg": 2.0, "min_games": 4, "threshold": 4.0},
    {"id": "pass_td", "label": "Pass TDs", "col": "PASS_TD", "min_avg": 0.4, "min_games": 4, "threshold": 1.5},
    {"id": "rush_td", "label": "Rush TDs", "col": "RUSH_TD", "min_avg": 0.15, "min_games": 4, "threshold": 0.5},
    {"id": "rec_td", "label": "Receiving TDs", "col": "REC_TD", "min_avg": 0.15, "min_games": 4, "threshold": 0.4},
    {"id": "kick_pts", "label": "Kicking points", "col": "KICK_PTS", "min_avg": 4.0, "min_games": 4, "threshold": 7.0},
)

PROP_TO_CAT: dict[str, str] = {
    "passing_yards": "pass_yds",
    "pass_yards": "pass_yds",
    "rushing_yards": "rush_yds",
    "rush_yards": "rush_yds",
    "receiving_yards": "rec_yds",
    "rec_yards": "rec_yds",
    "receptions": "receptions",
    "passing_tds": "pass_td",
    "pass_tds": "pass_td",
    "rushing_tds": "rush_td",
    "rush_tds": "rush_td",
    "receiving_tds": "rec_td",
    "rec_tds": "rec_td",
    "kicking_points": "kick_pts",
    "kicking_pts": "kick_pts",
}

TOP_N = 5
BOTTOM_N = 5
LEAGUE_N = 10


def season_from_game_date(gd: object) -> int | None:
    """NFL league year. Skip August (preseason). Sep–Dec = that year; Jan–Feb = prior year."""
    s = str(gd or "").replace("-", "").strip()[:8]
    if len(s) < 8 or not s.isdigit():
        return None
    year, month = int(s[:4]), int(s[4:6])
    if month < 1 or month > 12:
        return None
    if month == 8:
        return None
    return year if month >= 9 else year - 1


def _norm_name(s: object) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _display_name(player_norm: str) -> str:
    return " ".join(w.capitalize() for w in str(player_norm or "").split())


def team_id_to_abbr(repo: Path | None = None) -> dict[str, str]:
    root = repo or _REPO
    out: dict[str, str] = {}
    path = root / "data/reference/nfl_team_defense.csv"
    if not path.is_file():
        path = root / "Sports/NFL/data/defense_rankings.csv"
    if not path.is_file():
        return out
    df = pd.read_csv(path, encoding="utf-8-sig")
    tid_col = "team_id" if "team_id" in df.columns else ""
    abbr_col = next((c for c in ("team_abbr", "team") if c in df.columns), "")
    if not tid_col or not abbr_col:
        return out
    for tid, abbr in zip(df[tid_col].tolist(), df[abbr_col].tolist()):
        ab = canon_nfl_abbr(abbr)
        key = str(tid).strip()
        if key.endswith(".0") and key[:-2].isdigit():
            key = key[:-2]
        if key and ab:
            out[key] = ab
    return out


def load_nfl_boxscores(repo: Path | None = None, *, season: int | None = None) -> pd.DataFrame:
    root = repo or _REPO
    cache = root / "Sports/NFL/data/cache/nfl_boxscore_cache.csv"
    if not cache.is_file():
        return pd.DataFrame()
    df = pd.read_csv(cache, encoding="utf-8-sig", low_memory=False)
    if df.empty:
        return df
    df["_season"] = df["game_date"].map(season_from_game_date) if "game_date" in df.columns else None
    want = season if season is not None else last_completed_nfl_season()
    # Boxscores are prior completed year until 2026 games exist.
    if "_season" in df.columns and df["_season"].notna().any():
        present = sorted({int(x) for x in df["_season"].dropna().tolist()})
        if want not in present:
            want = max(present)
        df = df[df["_season"] == want].copy()
        df.attrs["season"] = want
    id_map = team_id_to_abbr(root)
    df["team_id"] = df["team_id"].astype(str).str.replace(r"\.0$", "", regex=True)
    df["TEAM"] = df["team_id"].map(id_map)
    df = df[df["TEAM"].notna() & (df["TEAM"].astype(str).str.len() > 0)].copy()
    df["player_norm"] = df["player_norm"].map(_norm_name)
    df["PLAYER"] = df["player_norm"].map(_display_name)
    return df


def _eligible(agg: pd.DataFrame, cat: dict[str, Any]) -> pd.DataFrame:
    work = agg.copy()
    work = work[work["games"] >= int(cat["min_games"])]
    work = work[work["season_avg"] >= float(cat["min_avg"])]
    return work


def rank_stat(df: pd.DataFrame, cat: dict[str, Any]) -> pd.DataFrame:
    col = cat["col"]
    if col not in df.columns:
        return pd.DataFrame()
    raw = df.copy()
    raw["_s"] = pd.to_numeric(raw[col], errors="coerce")
    raw = raw[raw["_s"].notna()]
    if raw.empty:
        return pd.DataFrame()
    agg = raw.groupby(["PLAYER", "player_norm", "TEAM"], as_index=False).agg(
        season_avg=("_s", "mean"),
        games=("_s", "count"),
        season_sum=("_s", "sum"),
    )
    elig = _eligible(agg, cat)
    if elig.empty:
        return pd.DataFrame()
    elig = elig.copy()
    elig["league_rank"] = elig["season_avg"].rank(ascending=False, method="min").astype(int)
    elig["team_rank"] = elig.groupby("TEAM")["season_avg"].rank(ascending=False, method="min").astype(int)
    elig["team_n"] = elig.groupby("TEAM")["player_norm"].transform("count")
    elig["category"] = cat["id"]
    elig["category_label"] = cat["label"]
    return elig


def _slice_for_row(team_rank: int, team_n: int) -> str:
    if team_rank <= TOP_N:
        return "top"
    if team_n - team_rank < BOTTOM_N:
        return "bottom"
    return "mid"


def build_nfl_leader_tables(repo: Path | None = None, *, season: int | None = None) -> dict[str, Any]:
    df = load_nfl_boxscores(repo, season=season)
    used_season = int(df.attrs.get("season") or season or last_completed_nfl_season())
    frames: list[pd.DataFrame] = []
    team_blocks: dict[str, list[dict[str, Any]]] = {}
    league_boards: dict[str, dict[str, list[dict[str, Any]]]] = {}

    for cat in NFL_STAT_CATS:
        ranked = rank_stat(df, cat)
        if ranked.empty:
            league_boards[cat["id"]] = {"top": [], "bottom": []}
            continue
        ranked["leader_slice"] = [
            _slice_for_row(int(r), int(n)) for r, n in zip(ranked["team_rank"], ranked["team_n"])
        ]
        frames.append(ranked)
        cid = cat["id"]
        for team, grp in ranked.groupby("TEAM", sort=False):
            top = grp.nsmallest(TOP_N, "team_rank")
            bottom = grp.nlargest(BOTTOM_N, "team_rank")
            plist: list[dict[str, Any]] = []
            seen: set[str] = set()

            def _row(r: pd.Series, *, slice_name: str, top_rank: int | None, bottom_rank: int | None) -> None:
                pn = str(r["player_norm"])
                if pn in seen:
                    return
                seen.add(pn)
                plist.append(
                    {
                        "player": str(r["PLAYER"]),
                        "player_norm": pn,
                        "pos": "",
                        "rank_on_team": top_rank,
                        "bottom_rank_on_team": bottom_rank,
                        "leader_slice": slice_name,
                        "bottom3_on_team": bottom_rank is not None and bottom_rank <= 3,
                        "season_avg": round(float(r["season_avg"]), 2),
                        "games": int(r["games"]),
                        "league_rank": int(r["league_rank"]),
                        "team_n": int(r["team_n"]),
                        "game_score": round(float(r["season_avg"]) * 1.2, 1),
                        "edge": "NEUTRAL",
                        "notes": f"{used_season} boxscore avg",
                        "overperform_vs_weak": False,
                        "def_boost": None,
                    }
                )

            for i, (_, r) in enumerate(top.iterrows(), start=1):
                _row(r, slice_name="top", top_rank=i, bottom_rank=None)
            bot_i = 1
            for _, r in bottom.iterrows():
                _row(r, slice_name="bottom", top_rank=None, bottom_rank=bot_i)
                bot_i += 1
            if plist:
                team_blocks[f"{team}|{cid}"] = plist

        top_lg = ranked.nsmallest(LEAGUE_N, "league_rank")
        bot_lg = ranked.nlargest(LEAGUE_N, "league_rank")
        league_boards[cid] = {
            "top": [
                {
                    "player": str(r["PLAYER"]),
                    "player_norm": str(r["player_norm"]),
                    "team": str(r["TEAM"]),
                    "season_avg": round(float(r["season_avg"]), 2),
                    "games": int(r["games"]),
                    "league_rank": int(r["league_rank"]),
                    "team_rank": int(r["team_rank"]),
                }
                for _, r in top_lg.iterrows()
            ],
            "bottom": [
                {
                    "player": str(r["PLAYER"]),
                    "player_norm": str(r["player_norm"]),
                    "team": str(r["TEAM"]),
                    "season_avg": round(float(r["season_avg"]), 2),
                    "games": int(r["games"]),
                    "league_rank": int(r["league_rank"]),
                    "team_rank": int(r["team_rank"]),
                }
                for _, r in bot_lg.iterrows()
            ],
        }

    all_rows = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    return {
        "season": used_season,
        "rows": all_rows,
        "team_blocks": team_blocks,
        "league_boards": league_boards,
        "categories": list(NFL_STAT_CATS),
        "player_count": int(all_rows["player_norm"].nunique()) if not all_rows.empty else 0,
        "block_count": len(team_blocks),
    }


def write_nfl_leader_artifacts(repo: Path | None = None, *, season: int | None = None) -> dict[str, Any]:
    root = repo or _REPO
    payload = build_nfl_leader_tables(root, season=season)
    rows: pd.DataFrame = payload["rows"]
    csv_path = root / "Sports/NFL/data/nfl_stat_leaders.csv"
    boards_path = root / "Sports/NFL/data/nfl_stat_leaders_boards.json"
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    if not rows.empty:
        out_cols = [
            "PLAYER",
            "player_norm",
            "TEAM",
            "category",
            "category_label",
            "season_avg",
            "games",
            "team_rank",
            "team_n",
            "league_rank",
            "leader_slice",
        ]
        rows[out_cols].to_csv(csv_path, index=False, encoding="utf-8-sig")
    boards = {
        "sport": "nfl",
        "season": payload["season"],
        "source": "Sports/NFL/data/cache/nfl_boxscore_cache.csv",
        "note": "Our ranks from boxscore averages. Bottoms require a role floor (not zeros).",
        "categories": [{"id": c["id"], "label": c["label"], "min_avg": c["min_avg"]} for c in NFL_STAT_CATS],
        "league_boards": payload["league_boards"],
        "player_count": payload["player_count"],
        "block_count": payload["block_count"],
    }
    boards_path.write_text(json.dumps(boards, indent=2), encoding="utf-8")
    payload["csv_path"] = csv_path
    payload["boards_path"] = boards_path
    return payload


def attach_nfl_stat_ranks(df: pd.DataFrame, repo: Path | None = None) -> pd.DataFrame:
    """Merge team_rank / league_rank / leader_slice onto a prop slate."""
    root = repo or _REPO
    path = root / "Sports/NFL/data/nfl_stat_leaders.csv"
    out = df.copy()
    for c in ("team_stat_rank", "league_stat_rank", "leader_slice", "team_top3_rank", "team_bottom3_rank"):
        if c not in out.columns:
            out[c] = pd.NA
    if not path.is_file() or out.empty:
        return out
    ranks = pd.read_csv(path, encoding="utf-8-sig")
    if ranks.empty:
        return out
    player_col = next((c for c in ("player_name", "player") if c in out.columns), None)
    team_col = next((c for c in ("team", "team_abbr") if c in out.columns), None)
    prop_col = next(
        (c for c in ("prop_type_normalized", "prop_norm", "prop_type", "stat_type") if c in out.columns),
        None,
    )
    if not player_col or not team_col or not prop_col:
        return out
    work = out[[player_col, team_col, prop_col]].copy()
    work["_pn"] = work[player_col].map(_norm_name)
    work["_tm"] = work[team_col].map(canon_nfl_abbr)
    work["_cat"] = (
        work[prop_col]
        .astype(str)
        .str.strip()
        .str.lower()
        .str.replace(r"[^a-z0-9]+", "_", regex=True)
        .map(lambda p: PROP_TO_CAT.get(p, ""))
    )
    ranks = ranks.copy()
    ranks["_pn"] = ranks["player_norm"].map(_norm_name)
    ranks["_tm"] = ranks["TEAM"].map(canon_nfl_abbr)
    ranks["_cat"] = ranks["category"].astype(str)
    m = (
        work.reset_index(drop=True)
        .merge(
            ranks[["_pn", "_tm", "_cat", "team_rank", "league_rank", "leader_slice", "team_n"]],
            on=["_pn", "_tm", "_cat"],
            how="left",
        )
    )
    out = out.copy()
    out["team_stat_rank"] = pd.to_numeric(m["team_rank"].to_numpy(), errors="coerce")
    out["league_stat_rank"] = pd.to_numeric(m["league_rank"].to_numpy(), errors="coerce")
    out["leader_slice"] = pd.Series(m["leader_slice"].to_numpy(), index=out.index)
    top = out["leader_slice"].astype(str).eq("top")
    bot = out["leader_slice"].astype(str).eq("bottom")
    out["team_top3_rank"] = out["team_stat_rank"].where(top & (out["team_stat_rank"] <= 3))
    bot_from_end = pd.to_numeric(m["team_n"].to_numpy(), errors="coerce") - pd.to_numeric(
        m["team_rank"].to_numpy(), errors="coerce"
    ) + 1
    bot_from_end = pd.Series(bot_from_end, index=out.index)
    out["team_bottom3_rank"] = bot_from_end.where(bot & (bot_from_end <= 3))
    return out
