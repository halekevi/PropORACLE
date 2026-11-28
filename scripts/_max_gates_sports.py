"""Team prop ranks + per-prop "max gates" for NHL, CBB, NFL, CFB, Soccer and Tennis (NBA: _nba_max_gates.py).

Tennis ranks each player as their own "team" within tour: offense = what the player produces per match,
defense = what their opponents produce against them. Keyed off prop_type (1st Set props are excluded).

For each sport:
  1. Team prop ranks from the box data: per-game production of each prop stat (offense, rank 1 = most)
     and per-game allowed to opponents (defense, rank 1 = fewest allowed). Soccer ranks within league.
     Latest table -> Sports/<Sport>/data/cache/team_prop_ranks.csv.
  2. Graded legs from dated board files (Goblin + Standard, no Demons, no combos): card-window L5/L10
     and season average from box games strictly before the slate, actual from the slate game, and
     as-of prop ranks for the player's team (offense) and opponent (defense).
  3. Best filter combination per (book, prop, side): highest Wilson low on train slates, honest
     rate on holdout slates. Football has no L10 filter. Minutes filter only where box minutes exist.
  4. Board legs for upcoming games that pass a holding gate -> ui_runner/templates/max_gates_latest.json
     (site highlight). Observe-only: tickets are unchanged.

  py -3.14 scripts/_max_gates_sports.py [--sports nhl,cbb,nfl,cfb,soccer,tennis] [--research]
"""
from __future__ import annotations

import argparse
import itertools
import json
import math
import re
import sqlite3
import unicodedata
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
DB = ROOT / "data" / "cache" / "proporacle_ref.db"
WEB_JSON = ROOT / "ui_runner" / "templates" / "max_gates_latest.json"
ET = ZoneInfo("America/New_York")
MIN_N = {"Standard": 25, "Goblin": 40}
MIN_HOLD_N = 15
BREAK_EVEN = {"Standard": 57.7, "Goblin": 75.0}
TIERS = ["Elite", "Above", "Avg", "Below", "Weak"]
FAV_OFF = {"OVER": ("Elite", "Above"), "UNDER": ("Below", "Weak")}
FAV_DEF = {"OVER": ("Below", "Weak"), "UNDER": ("Elite", "Above")}
RESEARCH_MAX_AGE_H = 18

LEVELS_ALL = {
    "l5": ["any", ">=4", "=5"],
    "l10": ["any", ">=7", ">=8", ">=9"],
    "season": ["any", "right side", ">=5%", ">=10%"],
    "off": ["any", "favorable", "not unfavorable"],
    "def": ["any", "favorable", "not unfavorable"],
    "min": ["any", "not down 2+", "down 2+"],
}
GATE_TEXT = {
    "l5": {">=4": "L5 4+", "=5": "L5 5/5"},
    "l10": {">=7": "L10 7+", ">=8": "L10 8+", ">=9": "L10 9+"},
    "season": {"right side": "season avg on side", ">=5%": "season avg 5%+ past line", ">=10%": "season avg 10%+ past line"},
    "off": {"favorable": "team strong at stat", "not unfavorable": "team not weak at stat"},
    "def": {"favorable": "opp weak vs stat", "not unfavorable": "opp not strong vs stat"},
    "min": {"not down 2+": "minutes steady", "down 2+": "minutes down 2+"},
}


def pkey(s) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s or "").lower())


def name_norm(s) -> str:
    s = unicodedata.normalize("NFKD", str(s or ""))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def id_str(x) -> str:
    s = str(x if x is not None else "").strip()
    s = re.sub(r"\.0$", "", s)
    return "" if s.lower() in ("", "nan", "none") else s


def et_date(s) -> str:
    try:
        ts = pd.to_datetime(s, utc=True)
    except (ValueError, TypeError):
        return ""
    return "" if pd.isna(ts) else ts.tz_convert(ET).strftime("%Y-%m-%d")


def ymd(x) -> str:
    s = re.sub(r"\D", "", str(x or ""))[:8]
    return f"{s[:4]}-{s[4:6]}-{s[6:8]}" if len(s) == 8 else ""


def wilson_lo(k: int, n: int, z: float = 1.96) -> float:
    if not n:
        return float("nan")
    p = k / n
    return (p + z * z / (2 * n) - z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))) / (1 + z * z / n)


def tier_of(rank: float, n: int) -> str | None:
    if pd.isna(rank) or not n:
        return None
    return TIERS[min(int((rank - 1) * 5 // n), 4)]


# ─────────────────────────── sport configs ───────────────────────────

def _sum(*cols):
    return (list(cols), "sum")


def _max(col):
    return ([col], "max")


BB = {"pts": _sum("pts"), "reb": _sum("reb"), "ast": _sum("ast"), "pra": _sum("pts", "reb", "ast"),
      "pr": _sum("pts", "reb"), "pa": _sum("pts", "ast"), "ra": _sum("reb", "ast"), "fg3m": _sum("fg3m"),
      "stl": _sum("stl"), "blk": _sum("blk"), "tov": _sum("tov"), "stocks": _sum("stl", "blk")}
FB_NFL = {
    "receivingyards": _sum("REC_YDS"), "receptions": _sum("REC"), "rushingyards": _sum("RUSH_YDS"),
    "rushattempts": _sum("RUSH_ATT"), "passingyards": _sum("PASS_YDS"), "passattempts": _sum("PASS_ATT"),
    "passcompletions": _sum("PASS_CMP"), "passingtds": _sum("PASS_TD"), "int": _sum("PASS_INT"),
    "rushrecyds": _sum("RUSH_YDS", "REC_YDS"), "passrushyds": _sum("PASS_YDS", "RUSH_YDS"),
    "tacklesast": _sum("TACK_TOT"), "sacks": _sum("SACK"), "kickingpoints": _sum("KICK_PTS"),
    "fgmade": _sum("KICK_FG"), "rectargets": _sum("REC_TGT"), "longestreception": _max("REC_LONG"),
    "longestrush": _max("RUSH_LONG"), "anytimetds": _sum("RUSH_TD", "REC_TD"),
}
FB_CFB = {
    "recyds": _sum("REC_YDS"), "rushyds": _sum("RUSH_YDS"), "rec": _sum("REC"), "passyds": _sum("PASS_YDS"),
    "rectd": _sum("REC_TD"), "passtd": _sum("PASS_TD"), "rushtd": _sum("RUSH_TD"),
    "anytimetds": _sum("RUSH_TD", "REC_TD"), "passrushyds": _sum("PASS_YDS", "RUSH_YDS"),
    "rushrecyds": _sum("RUSH_YDS", "REC_YDS"), "rushattempts": _sum("RUSH_ATT"), "passatt": _sum("PASS_ATT"),
    "passcmp": _sum("PASS_CMP"), "int": _sum("PASS_INT"), "longestreception": _max("REC_LONG"),
    "longestrush": _max("RUSH_LONG"), "kickingpoints": _sum("KICK_PTS"), "fgmade": _sum("KICK_FG"),
}
NHL = {"points": _sum("points"), "shotsongoal": _sum("shots_on_goal"), "assists": _sum("assists"),
       "goals": _sum("goals"), "hits": _sum("hits"), "powerplaypoints": _sum("pp_points"),
       "blockedshots": _sum("blocked_shots"), "goaliesaves": _sum("saves"), "saves": _sum("saves"),
       "goalsallowed": _sum("goals_allowed"), "faceoffswon": _sum("faceoffs_won")}
SOCCER = {"shots": _sum("sh"), "shotsontarget": _sum("sog"), "goals": _sum("g"), "assists": _sum("a"),
          "goalassist": _sum("g", "a"), "saves": _sum("sv"), "goaliesaves": _sum("sv"), "fouls": _sum("fc"),
          "tackles": _sum("tk"), "passes": _sum("pa"), "passesattempted": _sum("pa"),
          "clearances": _sum("clearances"), "crosses": _sum("crosses"), "attempteddribbles": _sum("dribble_attempts")}


TENNIS = {"totalgames": _sum("match_total_games"), "totalgameswon": _sum("games_won"),
          "totalsets": _sum("total_sets"), "totaltiebreaks": _sum("total_tie_breaks")}


def load_box_sql(table: str, cols: dict, since: str) -> pd.DataFrame:
    con = sqlite3.connect(DB)
    b = pd.read_sql(f"SELECT * FROM {table} WHERE game_date >= ?", con, params=(since,))
    con.close()
    return b


def box_nhl() -> pd.DataFrame:
    b = load_box_sql("nhl", {}, "2025-09-01")
    b["pid"] = ""
    b["played"] = True
    return b.rename(columns={"event_id": "event"})


def box_cbb() -> pd.DataFrame:
    b = load_box_sql("cbb", {}, "2025-10-01")
    b["pid"] = b.espn_athlete_id.map(id_str)
    b["played"] = pd.to_numeric(b.minutes, errors="coerce").fillna(0) > 0
    return b.rename(columns={"event_id": "event"})


def box_soccer() -> pd.DataFrame:
    b = load_box_sql("soccer", {}, "2025-07-01")
    b["pid"] = b.espn_player_id.map(id_str)
    m = pd.to_numeric(b.minutes_played, errors="coerce")
    b["played"] = m.isna() | (m > 0)
    b["minutes"] = m
    return b.rename(columns={"event_id": "event", "league": "group"})


def box_football(path: str) -> pd.DataFrame:
    b = pd.read_csv(ROOT / path, low_memory=False)
    b["game_date"] = b.game_date.map(ymd)
    b["pid"] = b.espn_athlete_id.map(id_str)
    b["team"] = b.team_id.map(id_str)
    b["player"] = b.player_norm
    b["played"] = pd.to_numeric(b.MIN, errors="coerce").fillna(0) > 0
    return b.rename(columns={"event_id": "event"})


def box_tennis() -> pd.DataFrame:
    """ESPN singles match logs; each player is their own "team" (ranks within tour)."""
    j = json.loads((ROOT / "Sports" / "Tennis" / "cache" / "tennis_match_games.json").read_text(encoding="utf-8"))
    b = pd.DataFrame([m for v in j.values() for m in (v or []) if isinstance(m, dict)])
    b = b[~b.is_doubles_match.fillna(False).astype(bool)]
    b["team"] = b.player.map(name_norm)
    b = b.drop_duplicates(["team", "competition_id"])
    ts = pd.to_datetime(b.match_date_utc, utc=True, errors="coerce")
    b["game_date"] = ts.dt.tz_convert(ET).dt.strftime("%Y-%m-%d").fillna("")
    b["pid"] = ""
    b["played"] = True
    return b.rename(columns={"competition_id": "event", "tour": "group"})


def boards_tennis() -> pd.DataFrame:
    b = boards("tennis", "step4_tennis_with_stats.csv", {"player": "player", "opp": "opp_team", "desc": "pp_description",
               "prop": "prop_type", "line": "line", "book": "pick_type", "date": "start_time"}, date_from_start=True)
    if b.empty:
        return b
    b = b[~b.player.astype(str).str.contains("/", na=False)]
    desc = b.desc.astype(str).str.replace(r"\s+1st Set$", "", regex=True) if "desc" in b.columns else ""
    b["opp"] = np.where(b.opp.astype(str) != "", b.opp, np.where(desc.isin(["nan", "None"]), "", desc))
    b["team"] = b.nname
    return b


def boards(sport_dir: str, fname: str, cols: dict, date_from_start: bool = False) -> pd.DataFrame:
    frames = []
    for d in sorted((ROOT / "outputs").glob("2026-*")):
        f = d / sport_dir / fname
        if not f.is_file():
            continue
        try:
            b = pd.read_csv(f, low_memory=False, usecols=lambda c: c in set(cols.values()))
        except (ValueError, pd.errors.EmptyDataError):
            continue
        b = b.rename(columns={v: k for k, v in cols.items()})
        if "date" not in b.columns:
            continue
        if date_from_start:
            ts = pd.to_datetime(b.date, utc=True, errors="coerce", format="ISO8601")
            b["date"] = ts.dt.tz_convert(ET).dt.strftime("%Y-%m-%d").fillna("")
        else:
            b["date"] = b.date.astype(str).str[:10]
        b["folder"] = d.name
        frames.append(b)
    if not frames:
        return pd.DataFrame()
    b = pd.concat(frames, ignore_index=True)
    b["book"] = b.book.astype(str).str.strip().str.capitalize()
    b = b[b.book.isin(["Goblin", "Standard"])]
    b = b[~b.player.astype(str).str.contains(r"\+", na=False)]
    b["line"] = pd.to_numeric(b.line, errors="coerce")
    b = b.dropna(subset=["line"])
    b["prop_key"] = b.prop.map(pkey)
    b["nname"] = b.player.map(name_norm)
    b["pid"] = b.pid.map(id_str) if "pid" in b.columns else ""
    for c in ("team", "opp"):
        b[c] = b[c].map(id_str) if c in b.columns else ""
    return b.sort_values("folder").drop_duplicates(["date", "nname", "prop_key", "line", "book"], keep="last")


SPORTS = {
    "nhl": dict(label="NHL", dir="Sports/NHL", box=box_nhl, specs=NHL, season="2026-10-01", l10=True, minutes=False,
                fuzzy_days=0, min_gp=3,
                board=lambda: boards("nhl", "step4_nhl_with_stats.csv", {"player": "player_name", "team": "team",
                                     "opp": "opponent", "prop": "stat_type", "line": "line_score",
                                     "book": "pick_type", "date": "game_date"})),
    "cbb": dict(label="CBB", dir="Sports/CBB", box=box_cbb, specs=BB, season="2026-11-01", l10=True, minutes=True,
                fuzzy_days=0, min_gp=3,
                board=lambda: boards("cbb", "step5b_cbb.csv", {"player": "player", "team": "team_abbr",
                                     "opp": "opp_team_abbr", "prop": "prop_norm", "prop_label": "prop_type",
                                     "line": "line", "book": "pick_type", "date": "start_time",
                                     "pid": "espn_athlete_id"}, date_from_start=True)),
    "nfl": dict(label="NFL", dir="Sports/NFL", box=lambda: box_football("Sports/NFL/data/cache/nfl_boxscore_cache.csv"),
                specs=FB_NFL, season="2026-09-01", l10=False, minutes=False, fuzzy_days=1, min_gp=2,
                board=lambda: boards("nfl", "step3_nfl_with_defense.csv", {"player": "player_name", "team": "team",
                                     "opp": "opp_team", "prop": "prop_type", "line": "line",
                                     "book": "pick_type", "date": "start_time"}, date_from_start=True)),
    "cfb": dict(label="CFB", dir="Sports/CFB", box=lambda: box_football("Sports/CFB/data/cache/cfb_boxscore_cache.csv"),
                specs=FB_CFB, season="2026-08-20", l10=False, minutes=False, fuzzy_days=1, min_gp=2,
                board=lambda: boards("cfb", "step5b_cfb.csv", {"player": "player", "team": "team_abbr",
                                     "opp": "opp_team_abbr", "prop": "prop_norm", "prop_label": "prop_type",
                                     "line": "line", "book": "pick_type", "date": "start_time",
                                     "pid": "espn_athlete_id"}, date_from_start=True)),
    "soccer": dict(label="SOCCER", dir="Sports/Soccer", box=box_soccer, specs=SOCCER, season="2026-07-01", l10=True,
                   minutes=False, fuzzy_days=1, min_gp=3,
                   board=lambda: boards("soccer", "step4_soccer_with_stats.csv", {"player": "player", "team": "team",
                                        "opp": "opp_team", "prop": "prop_norm", "prop_label": "prop_type",
                                        "line": "line", "book": "pick_type", "date": "game_date",
                                        "pid": "espn_player_id"})),
    "tennis": dict(label="TENNIS", dir="Sports/Tennis", box=box_tennis, specs=TENNIS, season="2026-01-01", l10=True,
                   minutes=False, fuzzy_days=1, min_gp=5, individual=True, board=boards_tennis),
}


# ─────────────────────────── team prop ranks ───────────────────────────

def stat_series(df: pd.DataFrame, spec) -> pd.Series:
    cols, _ = spec
    vals = [pd.to_numeric(df[c], errors="coerce") if c in df.columns else pd.Series(np.nan, index=df.index) for c in cols]
    out = vals[0].copy()
    for v in vals[1:]:
        out = out + v
    return out


def team_games(box: pd.DataFrame, specs: dict) -> pd.DataFrame:
    b = box[box.played].copy()
    if "group" not in b.columns:
        b["group"] = ""
    keys = ["event", "game_date", "team", "group"]
    for k, spec in specs.items():
        b[f"s_{k}"] = stat_series(b, spec)
    sum_cols = [f"s_{k}" for k, s in specs.items() if s[1] == "sum"]
    max_cols = [f"s_{k}" for k, s in specs.items() if s[1] == "max"]
    gb = b.groupby(keys)
    t = gb[sum_cols].sum(min_count=1)
    if max_cols:
        t = t.join(gb[max_cols].max())
    t = t.reset_index()
    t = t[t.groupby("event").team.transform("nunique") == 2]
    opp = t[["event", "team"] + [f"s_{k}" for k in specs]].rename(columns={"team": "opp", **{f"s_{k}": f"a_{k}" for k in specs}})
    m = t.merge(opp, on="event")
    return m[m.team != m.opp]


def ranks_asof(tg: pd.DataFrame, specs: dict, asof: str, min_gp: int) -> pd.DataFrame:
    g = tg[tg.game_date < asof]
    if g.empty:
        return pd.DataFrame()
    agg = g.groupby(["group", "team"]).agg(gp=("event", "nunique"), **{f"f_{k}": (f"s_{k}", "mean") for k in specs},
                                            **{f"x_{k}": (f"a_{k}", "mean") for k in specs}).reset_index()
    agg = agg[agg.gp >= min_gp]
    rows = []
    for k in specs:
        sub = agg[["group", "team", "gp", f"f_{k}", f"x_{k}"]].dropna()
        if sub.empty:
            continue
        sub = sub.rename(columns={f"f_{k}": "for_pg", f"x_{k}": "allowed_pg"})
        sub["prop_key"] = k
        sub["for_rank"] = sub.groupby("group").for_pg.rank(ascending=False, method="min")
        sub["allowed_rank"] = sub.groupby("group").allowed_pg.rank(ascending=True, method="min")
        sub["n_teams"] = sub.groupby("group").team.transform("size")
        rows.append(sub)
    if not rows:
        return pd.DataFrame()
    r = pd.concat(rows, ignore_index=True)
    r["off_tier"] = [tier_of(a, n) for a, n in zip(r.for_rank, r.n_teams)]
    r["def_tier"] = [tier_of(a, n) for a, n in zip(r.allowed_rank, r.n_teams)]
    r["slate"] = asof
    return r


# ─────────────────────────── legs ───────────────────────────

class PlayerIndex:
    def __init__(self, box: pd.DataFrame, specs: dict, has_minutes: bool):
        b = box.copy()
        b["nname"] = b.player.map(name_norm)
        b["key"] = np.where(b.pid != "", "id:" + b.pid, "nm:" + b.nname)
        b = b.sort_values("game_date").drop_duplicates(["key", "game_date", "event"])
        self.by_id = {k: g for k, g in b[b.pid != ""].groupby("pid").key.first().items()}
        names = b.groupby("nname").key.nunique()
        first = b.groupby("nname").key.first()
        self.by_name = {n: first[n] for n in names[names == 1].index}
        self.games = {}
        b = b[b.played].sort_values(["key", "game_date"], kind="stable")
        stats = {k: stat_series(b, s).to_numpy(dtype=float) for k, s in specs.items()}
        mins = pd.to_numeric(b.get("minutes"), errors="coerce").to_numpy(dtype=float) if has_minutes else None
        keys = b.key.to_numpy()
        dates, teams, events = b.game_date.to_numpy(), b.team.to_numpy(), b.event.to_numpy()
        uniq, starts = np.unique(keys, return_index=True)
        ends = np.append(starts[1:], len(keys))
        for key, s0, s1 in zip(uniq, starts, ends):
            self.games[key] = {
                "date": dates[s0:s1], "team": teams[s0:s1], "event": events[s0:s1],
                "stats": {k: v[s0:s1] for k, v in stats.items()},
                "min": mins[s0:s1] if mins is not None else None,
            }

    def find(self, pid: str, nname: str) -> str | None:
        key = self.by_id.get(pid) if pid else None
        key = key or self.by_name.get(nname)
        return key if key in self.games else None


def build_legs(cfg: dict, box: pd.DataFrame, board: pd.DataFrame, idx: PlayerIndex, graded: bool) -> pd.DataFrame:
    specs = cfg["specs"]
    ev_teams = box[box.played].groupby("event").team.unique().to_dict()
    rows = []
    for r in board.itertuples(index=False):
        spec_key = r.prop_key if r.prop_key in specs else None
        if spec_key is None:
            continue
        key = idx.find(getattr(r, "pid", ""), r.nname)
        if key is None:
            continue
        g = idx.games[key]
        vals_all = g["stats"][spec_key]
        dates = g["date"]
        cut = np.searchsorted(dates, r.date, side="left")
        prior = vals_all[:cut]
        pmask = ~np.isnan(prior)
        prior_vals = prior[pmask]
        if len(prior_vals) < 5:
            continue
        today_i = None
        if graded:
            hi = (datetime.strptime(r.date, "%Y-%m-%d") + timedelta(days=cfg["fuzzy_days"])).strftime("%Y-%m-%d")
            j = cut
            if j < len(dates) and dates[j] <= hi and not np.isnan(vals_all[j]):
                today_i = j
            if today_i is None:
                continue
        recent = prior_vals[::-1]
        l5, l10 = recent[:5], recent[:10]
        sdates = dates[:cut][pmask]
        season = prior_vals[sdates >= cfg["season"]]
        if len(season) < 3:
            continue
        sa = float(season.mean())
        line = float(r.line)
        label = getattr(r, "prop_label", None)
        row = {
            "slate": r.date, "player": r.player, "prop_key": spec_key,
            "prop_label": label if isinstance(label, str) and label else r.prop,
            "book": r.book, "line": line, "season_avg": round(sa, 3), "rel_gap": (sa - line) / line if line else 0.0,
            "l5_over": int((l5 > line).sum()), "l5_under": int((l5 < line).sum()), "l5_n": len(l5),
            "l10_over": int((l10 > line).sum()), "l10_under": int((l10 < line).sum()), "l10_n": len(l10),
            "l5_vals": "/".join(f"{v:g}" for v in l5),
        }
        if g["min"] is not None:
            pm = g["min"][:cut][pmask]
            pm_season = pm[sdates >= cfg["season"]]
            row["min_l5"] = float(np.nanmean(pm[::-1][:5])) if len(pm) else np.nan
            row["min_season"] = float(np.nanmean(pm_season)) if len(pm_season) else np.nan
        if graded:
            act = float(vals_all[today_i])
            row.update({"actual": act, "over_hit": int(act > line), "under_hit": int(act < line), "push": int(act == line)})
            team = g["team"][today_i]
            others = [t for t in ev_teams.get(g["event"][today_i], []) if t != team]
            if len(others) != 1:
                continue
            row["team_n"], row["opp_n"] = team, others[0]
        else:
            row["board_team"], row["board_opp"] = r.team, r.opp
            row["box_team_last"] = g["team"][cut - 1] if cut else ""
        rows.append(row)
    return pd.DataFrame(rows)


def board_team_map(legs_graded: pd.DataFrame, board: pd.DataFrame) -> dict:
    """Board team label -> box team label, from graded legs (player's box team on that slate)."""
    if legs_graded.empty:
        return {}
    m = legs_graded.merge(board[["date", "player", "team"]].drop_duplicates(), left_on=["slate", "player"],
                          right_on=["date", "player"], how="inner")
    m = m[m.team.astype(str) != ""]
    if m.empty:
        return {}
    return m.groupby("team").team_n.agg(lambda s: s.mode().iat[0]).to_dict()


def attach_ranks(legs: pd.DataFrame, tg: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    if legs.empty:
        return legs
    rk = pd.concat([ranks_asof(tg, cfg["specs"], s, cfg["min_gp"]) for s in sorted(legs.slate.unique())], ignore_index=True)
    if rk.empty:
        return legs.iloc[0:0]
    rk = rk.sort_values("gp").drop_duplicates(["slate", "team", "prop_key"], keep="last")
    own = rk[["slate", "team", "prop_key", "for_rank", "off_tier"]].rename(columns={"team": "team_n", "for_rank": "own_off_rank", "off_tier": "own_off_pt"})
    opp = rk[["slate", "team", "prop_key", "allowed_rank", "def_tier"]].rename(columns={"team": "opp_n", "allowed_rank": "opp_def_rank", "def_tier": "opp_def_pt"})
    legs = legs.merge(own, on=["slate", "team_n", "prop_key"], how="inner")
    return legs.merge(opp, on=["slate", "opp_n", "prop_key"], how="inner")


# ─────────────────────────── gate search ───────────────────────────

def levels_for(cfg: dict) -> dict:
    lv = {k: v for k, v in LEVELS_ALL.items()}
    if not cfg["l10"]:
        lv.pop("l10")
    if not cfg["minutes"]:
        lv.pop("min")
    return lv


def masks(d: pd.DataFrame, side: str, levels: dict) -> dict:
    up = side == "OVER"
    l5 = d.l5_over if up else d.l5_under
    l10 = d.l10_over if up else d.l10_under
    rel = d.rel_gap if up else -d.rel_gap
    other = "UNDER" if up else "OVER"
    off_f, def_f = d.own_off_pt.isin(FAV_OFF[side]).values, d.opp_def_pt.isin(FAV_DEF[side]).values
    off_u, def_u = d.own_off_pt.isin(FAV_OFF[other]).values, d.opp_def_pt.isin(FAV_DEF[other]).values
    t = np.ones(len(d), dtype=bool)
    out = {
        "l5": {"any": t, ">=4": (l5 >= 4).values, "=5": (l5 == 5).values},
        "l10": {"any": t, ">=7": (l10 >= 7).values, ">=8": (l10 >= 8).values, ">=9": (l10 >= 9).values},
        "season": {"any": t, "right side": (rel > 0).values, ">=5%": (rel >= 0.05).values, ">=10%": (rel >= 0.10).values},
        "off": {"any": t, "favorable": off_f, "not unfavorable": ~off_u},
        "def": {"any": t, "favorable": def_f, "not unfavorable": ~def_u},
    }
    if "min" in levels:
        md = (d.min_l5 - d.min_season).values
        out["min"] = {"any": t, "not down 2+": md > -2, "down 2+": md <= -2}
    return out


def gate_text(gate: dict, individual: bool = False) -> str:
    parts = [GATE_TEXT[k][v] for k, v in gate.items() if v != "any"]
    if individual:
        parts = [p.replace("team ", "player ") for p in parts]
    return " · ".join(parts) or "no filter"


def search(d: pd.DataFrame, side: str, book: str, levels: dict, train_end: str, individual: bool = False) -> dict | None:
    hit = (d.over_hit if side == "OVER" else d.under_hit).values.astype(int)
    tr = (d.slate <= train_end).values
    mk = masks(d, side, levels)
    best = None
    for combo in itertools.product(*levels.values()):
        m = np.ones(len(d), dtype=bool)
        for k, lv in zip(levels, combo):
            m &= mk[k][lv]
        mt = m & tr
        n = int(mt.sum())
        if n < MIN_N[book]:
            continue
        lo = wilson_lo(int(hit[mt].sum()), n)
        if best is None or lo > best[0] + 1e-9:
            best = (lo, dict(zip(levels, combo)), m, n, 100 * hit[mt].mean())
    if best is None:
        return None
    lo, gate, m, n, th = best
    mh = m & ~tr
    hn = int(mh.sum())
    hh = round(100 * hit[mh].mean(), 1) if hn else None
    return {"gate": gate, "gate_text": gate_text(gate, individual),
            "train_n": n, "train_hit": round(th, 1), "train_low": round(100 * lo, 1), "hold_n": hn, "hold_hit": hh,
            "holds": bool(hn >= MIN_HOLD_N and hh is not None and hh >= BREAK_EVEN[book] and th >= BREAK_EVEN[book])}


def apply_gate(d: pd.DataFrame, side: str, gate: dict, levels: dict) -> np.ndarray:
    mk = masks(d, side, levels)
    m = np.ones(len(d), dtype=bool)
    for k, lv in gate.items():
        if k in mk:
            m &= mk[k][lv]
    return m


# ─────────────────────────── driver ───────────────────────────

def run_sport(name: str, research: bool) -> dict:
    cfg = SPORTS[name]
    report_path = ROOT / "data" / "reports" / f"max_gates_{name}.json"
    box = cfg["box"]()
    tg = team_games(box, cfg["specs"])
    today = datetime.now(ET).strftime("%Y-%m-%d")
    latest = ranks_asof(tg, cfg["specs"], "9999-12-31", cfg["min_gp"])
    out_dir = ROOT / cfg["dir"] / "data" / "cache"
    if not latest.empty and out_dir.parent.is_dir():
        out_dir.mkdir(exist_ok=True)
        fname = "player_prop_ranks.csv" if cfg.get("individual") else "team_prop_ranks.csv"
        latest.assign(through=tg.game_date.max()).drop(columns=["slate"]).to_csv(out_dir / fname, index=False)

    board = cfg["board"]()
    if board.empty:
        return {"sport": name, "gates": [], "legs": [], "note": "no board files"}
    idx = PlayerIndex(box, cfg["specs"], cfg["minutes"])
    levels = levels_for(cfg)

    prev = json.loads(report_path.read_text(encoding="utf-8")) if report_path.is_file() else {}
    fresh = prev.get("researched_at") and (datetime.now(ET) - datetime.fromisoformat(prev["researched_at"])).total_seconds() < RESEARCH_MAX_AGE_H * 3600
    past = board[board.date < today]
    if not (research or not fresh):
        recent_cut = (datetime.now(ET) - timedelta(days=21)).strftime("%Y-%m-%d")
        past = past[past.date >= recent_cut]
    graded = attach_ranks(build_legs(cfg, box, past, idx, graded=True), tg, cfg)
    graded = graded[graded.push == 0] if not graded.empty else graded
    tmap = board_team_map(graded, past)
    if research or not fresh:
        slates = sorted(graded.slate.unique()) if not graded.empty else []
        train_end = slates[int(len(slates) * 0.65)] if len(slates) >= 6 else ""
        gates = []
        if train_end:
            for (book, prop), grp in graded.groupby(["book", "prop_key"]):
                for side in (["OVER"] if book == "Goblin" else ["OVER", "UNDER"]):
                    g = search(grp.reset_index(drop=True), side, book, levels, train_end, bool(cfg.get("individual")))
                    if g:
                        gates.append({"book": book, "prop": prop, "side": side, **g})
        researched_at = datetime.now(ET).isoformat()
    else:
        gates, train_end, researched_at = prev.get("gates", []), prev.get("train_end", ""), prev["researched_at"]
        for g in gates:
            be = BREAK_EVEN[g["book"]]
            g["holds"] = bool(g["hold_n"] >= MIN_HOLD_N and g["hold_hit"] is not None and g["hold_hit"] >= be
                              and g["train_hit"] >= be)

    upcoming = board[board.date >= today]
    legs_out = []
    if not upcoming.empty and gates:
        cur = build_legs(cfg, box, upcoming, idx, graded=False)
        if not cur.empty:
            if cfg.get("individual"):
                cur["team_n"] = cur.box_team_last
                cur["opp_n"] = cur.board_opp.map(name_norm)
            else:
                cur["team_n"] = [tmap.get(bt) or bl for bt, bl in zip(cur.board_team, cur.box_team_last)]
                cur["opp_n"] = cur.board_opp.map(lambda o: tmap.get(o, ""))
            cur = attach_ranks(cur, tg, cfg)
        for g in (x for x in gates if x["holds"]):
            sub = cur[(cur.book == g["book"]) & (cur.prop_key == g["prop"])].reset_index(drop=True) if not cur.empty else cur
            if sub.empty:
                continue
            for r in sub[apply_gate(sub, g["side"], g["gate"], levels)].itertuples(index=False):
                legs_out.append({
                    "sport": cfg["label"], "date": r.slate, "player": r.player, "prop": r.prop_label,
                    "pick_type": g["book"], "side": g["side"], "line": r.line, "hold_hit": g["hold_hit"],
                    "hold_n": g["hold_n"], "gate": g["gate_text"], "season_avg": r.season_avg,
                    "l5": r.l5_over if g["side"] == "OVER" else r.l5_under, "l5_vals": r.l5_vals,
                    "own_off_rank": int(r.own_off_rank), "opp_def_rank": int(r.opp_def_rank),
                })
    searched = research or not fresh
    rep = {"sport": name, "researched_at": researched_at, "train_end": train_end,
           "graded_legs": int(len(graded)) if searched else prev.get("graded_legs", 0),
           "graded_slates": (int(graded.slate.nunique()) if not graded.empty else 0) if searched else prev.get("graded_slates", 0),
           "gates": gates, "legs": legs_out, "team_map_size": len(tmap)}
    report_path.write_text(json.dumps(rep, indent=1, default=str), encoding="utf-8")
    return rep


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sports", default=",".join(SPORTS))
    ap.add_argument("--research", action="store_true", help="re-run the gate search even if today's is cached")
    args = ap.parse_args()
    run = [x.strip() for x in args.sports.split(",") if x.strip()]
    labels_run = {SPORTS[s]["label"] for s in run if s in SPORTS}
    prev = json.loads(WEB_JSON.read_text(encoding="utf-8")) if WEB_JSON.is_file() else {}
    all_legs = [x for x in prev.get("legs", []) if x.get("sport") not in labels_run]
    for s in run:
        t0 = datetime.now()
        try:
            rep = run_sport(s, args.research)
            print(f"[{s}] {(datetime.now() - t0).total_seconds():.0f}s")
        except Exception as e:  # noqa: BLE001
            print(f"[{s}] FAILED: {type(e).__name__}: {e}")
            continue
        holds = [g for g in rep["gates"] if g.get("holds")]
        print(f"[{s}] graded legs={rep.get('graded_legs', 0)} slates={rep.get('graded_slates', 0)} "
              f"gates={len(rep['gates'])} holding={len(holds)} upcoming passing={len(rep['legs'])}")
        for g in sorted(holds, key=lambda g: -(g["hold_hit"] or 0)):
            print(f"    {g['book']:<8} {g['prop']:<18} {g['side']:<5} train {g['train_hit']:5.1f}% n={g['train_n']:<4} "
                  f"holdout {g['hold_hit']:5.1f}% n={g['hold_n']:<4} {g['gate_text']}")
        all_legs.extend(rep["legs"])
    WEB_JSON.write_text(json.dumps({"generated_at": datetime.now(ET).isoformat(), "legs": all_legs}, indent=1, default=str),
                        encoding="utf-8")
    print(f"Saved -> {WEB_JSON} ({len(all_legs)} legs)")


if __name__ == "__main__":
    main()
