"""NFL scoring-vehicle + game-script leans for pass / rush / kick props."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from utils.nfl_espn_context import canon_nfl_abbr
from utils.nfl_stat_leaders import load_nfl_boxscores
from utils.nfl_prop_defense import last_completed_nfl_season

_REPO = Path(__file__).resolve().parents[1]
VEHICLE_PATH = _REPO / "Sports/NFL/data/nfl_scoring_vehicles.csv"
SCRIPT_PATH = _REPO / "Sports/NFL/data/nfl_game_script_leans.json"

SCRIPT_LEAD = 8


def _team_games_from_box(repo: Path, season: int) -> pd.DataFrame:
    df = load_nfl_boxscores(repo, season=season)
    if df.empty:
        return pd.DataFrame()
    work = df.copy()
    for c in ("PASS_YDS", "RUSH_YDS", "PASS_TD", "RUSH_TD", "REC_TD", "KICK_PTS", "KICK_FG", "KICK_XP"):
        work[c] = pd.to_numeric(work.get(c), errors="coerce").fillna(0.0)
    gid = "event_id" if "event_id" in work.columns else "game_date"
    per = work.groupby(["TEAM", gid], as_index=False).agg(
        pass_yds=("PASS_YDS", "sum"),
        rush_yds=("RUSH_YDS", "sum"),
        pass_td=("PASS_TD", "sum"),
        rush_td=("RUSH_TD", "sum"),
        rec_td=("REC_TD", "sum"),
        kick_pts=("KICK_PTS", "sum"),
        kick_fg=("KICK_FG", "sum"),
        kick_xp=("KICK_XP", "sum"),
    )
    per = per.rename(columns={"TEAM": "team", gid: "event_id"})
    per["event_id"] = per["event_id"].astype(str)
    return per


def _attach_script(per: pd.DataFrame, games: pd.DataFrame) -> pd.DataFrame:
    if per.empty or games.empty:
        return per
    g = games.copy()
    g["event_id"] = g["event_id"].astype(str)
    home = g[["event_id", "home", "home_score", "away_score", "margin", "divisional"]].rename(
        columns={"home": "team", "home_score": "pf", "away_score": "pa"}
    )
    away = g[["event_id", "away", "away_score", "home_score", "margin", "divisional"]].rename(
        columns={"away": "team", "away_score": "pf", "home_score": "pa"}
    )
    sides = pd.concat([home, away], ignore_index=True)
    sides["team"] = sides["team"].map(canon_nfl_abbr)
    out = per.merge(sides, on=["event_id", "team"], how="left")
    margin_signed = pd.to_numeric(out["pf"], errors="coerce") - pd.to_numeric(out["pa"], errors="coerce")
    out["margin_signed"] = margin_signed
    out["script"] = "unknown"
    known = margin_signed.notna()
    out.loc[known & (margin_signed.abs() < SCRIPT_LEAD), "script"] = "close"
    out.loc[known & (margin_signed >= SCRIPT_LEAD), "script"] = "leading"
    out.loc[known & (margin_signed <= -SCRIPT_LEAD), "script"] = "trailing"
    return out


def _classify(row: pd.Series) -> str:
    rush_share = float(row.get("rush_share") or 0)
    rush_td_share = float(row.get("rush_td_share") or 0)
    fg_pg = float(row.get("fg_pg") or 0)
    kick_share = float(row.get("kick_score_share") or 0)
    tags: list[str] = []
    if rush_share >= 0.36 or (rush_share >= 0.33 and rush_td_share >= 0.40):
        tags.append("RUN")
    if float(row.get("pass_share") or 0) >= 0.70 and rush_share < 0.36:
        tags.append("PASS")
    if not tags:
        tags.append("BALANCED")
    # Kick share of scoring is high on bad offenses; FG vehicle is volume of makes.
    if fg_pg >= 2.0:
        tags.append("FG")
    return "+".join(tags)


VEHICLE_LABELS = {
    "RUN": "Rush team",
    "PASS": "Pass team",
    "BALANCED": "Balanced",
    "RUN+FG": "Rush + FG team",
    "PASS+FG": "Pass + FG team",
    "BALANCED+FG": "Balanced + FG team",
}

SHARE_CONTEXT_PROPS: tuple[tuple[str, str], ...] = (
    ("rush_yds_share_pct", "Rush Yards"),
    ("rush_att_share_pct", "Rush Attempts"),
    ("rec_yds_share_pct", "Receiving Yards"),
    ("rec_share_pct", "Receptions"),
    ("rec_tgt_share_pct", "Rec Targets"),
    ("pass_yds_share_pct", "Pass Yards"),
    ("pass_att_share_pct", "Pass Attempts"),
    ("kick_fg_share_pct", "Kick FG"),
    ("kick_pts_share_pct", "Kick Pts"),
)


def vehicle_flags(vehicle: object) -> dict[str, Any]:
    raw = str(vehicle or "").strip().upper()
    tags = {t for t in raw.split("+") if t}
    return {
        "is_run_team": "RUN" in tags,
        "is_pass_team": "PASS" in tags,
        "is_fg_team": "FG" in tags,
        "is_balanced": "BALANCED" in tags,
        "off_identity": VEHICLE_LABELS.get(raw, raw.replace("+", " + ") or ""),
    }


def _apply_vehicle_flags(team: pd.DataFrame) -> pd.DataFrame:
    out = team.copy()
    flags = out["vehicle"].map(vehicle_flags)
    out["is_run_team"] = flags.map(lambda d: d["is_run_team"])
    out["is_pass_team"] = flags.map(lambda d: d["is_pass_team"])
    out["is_fg_team"] = flags.map(lambda d: d["is_fg_team"])
    out["is_balanced"] = flags.map(lambda d: d["is_balanced"])
    out["off_identity"] = flags.map(lambda d: d["off_identity"])
    return out


def attach_nfl_identity_and_share(df: pd.DataFrame, repo: Path | None = None) -> pd.DataFrame:
    """Tag PASS/RUN/FG teams and attach player share % of team for each prop."""
    from utils.team_share import load_share_payload, lookup_player_share, _norm_prop

    root = repo or _REPO
    out = df.copy()
    extra = [
        "team_vehicle",
        "off_identity",
        "is_run_team",
        "is_pass_team",
        "is_fg_team",
        "is_balanced",
        "team_pass_share",
        "team_rush_share",
        "team_fg_pg",
        "prop_share_pct",
        "prop_share_player_avg",
        "prop_share_team_avg",
        *[c for c, _ in SHARE_CONTEXT_PROPS],
    ]
    for c in extra:
        if c not in out.columns:
            out[c] = pd.NA

    vpath = root / "Sports/NFL/data/nfl_scoring_vehicles.csv"
    team_col = next((c for c in ("team", "team_abbr") if c in out.columns), None)
    if team_col and vpath.is_file():
        veh = pd.read_csv(vpath, encoding="utf-8-sig")
        if not veh.empty and "team" in veh.columns and "vehicle" in veh.columns:
            veh["team"] = veh["team"].map(canon_nfl_abbr)
            if "is_run_team" not in veh.columns:
                veh = _apply_vehicle_flags(veh)
            keep = [
                "team",
                "vehicle",
                "off_identity",
                "is_run_team",
                "is_pass_team",
                "is_fg_team",
                "is_balanced",
                "pass_share",
                "rush_share",
                "fg_pg",
            ]
            keep = [c for c in keep if c in veh.columns]
            slim = veh[keep].drop_duplicates("team").rename(
                columns={
                    "vehicle": "team_vehicle",
                    "pass_share": "team_pass_share",
                    "rush_share": "team_rush_share",
                    "fg_pg": "team_fg_pg",
                }
            )
            out["_team_join"] = out[team_col].map(canon_nfl_abbr)
            slim = slim.rename(columns={"team": "_team_join"})
            drop_cols = [c for c in slim.columns if c != "_team_join" and c in out.columns]
            out = out.drop(columns=drop_cols)
            out = out.merge(slim, on="_team_join", how="left").drop(columns=["_team_join"])

    share = load_share_payload("nfl", root)
    if not share or not share.get("applicable"):
        return out
    player_col = next((c for c in ("player_name", "player", "player_norm") if c in out.columns), None)
    prop_col = next(
        (c for c in ("prop_type_normalized", "prop_type", "stat_type") if c in out.columns),
        None,
    )
    if not team_col or not player_col:
        return out

    teams = out[team_col].map(canon_nfl_abbr).tolist()
    players = out[player_col].astype(str).tolist()
    props = out[prop_col].astype(str).tolist() if prop_col else [""] * len(out)

    prop_share: list[Any] = []
    prop_pavg: list[Any] = []
    prop_tavg: list[Any] = []
    ctx: dict[str, list[Any]] = {col: [] for col, _ in SHARE_CONTEXT_PROPS}
    for team, player, prop in zip(teams, players, props):
        hit = lookup_player_share(share, team=team, player=player, prop=_norm_prop(prop))
        if hit:
            prop_share.append(hit.get("share_pct"))
            prop_pavg.append(hit.get("player_avg"))
            prop_tavg.append(hit.get("team_avg"))
        else:
            prop_share.append(pd.NA)
            prop_pavg.append(pd.NA)
            prop_tavg.append(pd.NA)
        for col, share_prop in SHARE_CONTEXT_PROPS:
            ch = lookup_player_share(share, team=team, player=player, prop=share_prop)
            ctx[col].append(ch.get("share_pct") if ch else pd.NA)

    out["prop_share_pct"] = prop_share
    out["prop_share_player_avg"] = prop_pavg
    out["prop_share_team_avg"] = prop_tavg
    for col, _ in SHARE_CONTEXT_PROPS:
        out[col] = ctx[col]
    return out


def build_scoring_vehicles(
    repo: Path | None = None,
    *,
    season: int | None = None,
    games: pd.DataFrame | None = None,
) -> dict[str, Any]:
    root = repo or _REPO
    yr = int(season) if season else last_completed_nfl_season()
    if games is None:
        gpath = root / "Sports/NFL/data/nfl_game_results.csv"
        games = pd.read_csv(gpath, encoding="utf-8-sig") if gpath.is_file() else pd.DataFrame()
    if not games.empty:
        games = games[pd.to_numeric(games["season"], errors="coerce") == yr].copy()
    per = _team_games_from_box(root, yr)
    per = _attach_script(per, games)
    if per.empty:
        return {"season": yr, "teams": pd.DataFrame(), "script": {}}

    per["yards"] = per["pass_yds"] + per["rush_yds"]
    per["tds"] = per["pass_td"] + per["rush_td"]
    team = per.groupby("team", as_index=False).agg(
        games=("event_id", "count"),
        pass_ypg=("pass_yds", "mean"),
        rush_ypg=("rush_yds", "mean"),
        pass_td_pg=("pass_td", "mean"),
        rush_td_pg=("rush_td", "mean"),
        fg_pg=("kick_fg", "mean"),
        xp_pg=("kick_xp", "mean"),
        kick_pts_pg=("kick_pts", "mean"),
    )
    team["pass_share"] = team["pass_ypg"] / (team["pass_ypg"] + team["rush_ypg"])
    team["rush_share"] = team["rush_ypg"] / (team["pass_ypg"] + team["rush_ypg"])
    team["rush_td_share"] = team["rush_td_pg"] / (team["pass_td_pg"] + team["rush_td_pg"]).replace(0, pd.NA)
    team["kick_score_share"] = team["kick_pts_pg"] / (
        team["pass_td_pg"] * 6 + team["rush_td_pg"] * 6 + team["kick_pts_pg"]
    ).replace(0, pd.NA)
    team["vehicle"] = team.apply(_classify, axis=1)
    team = _apply_vehicle_flags(team)

    def _script_means(label: str) -> pd.DataFrame:
        sub = per[per["script"] == label]
        if sub.empty:
            return pd.DataFrame()
        return sub.groupby("team", as_index=False).agg(
            **{
                f"{label}_n": ("event_id", "count"),
                f"{label}_pass": ("pass_yds", "mean"),
                f"{label}_rush": ("rush_yds", "mean"),
                f"{label}_fg": ("kick_fg", "mean"),
                f"{label}_kick_pts": ("kick_pts", "mean"),
            }
        )

    for lab in ("leading", "trailing", "close"):
        sm = _script_means(lab)
        if not sm.empty:
            team = team.merge(sm, on="team", how="left")

    league_script: dict[str, Any] = {}
    for lab in ("leading", "trailing", "close"):
        sub = per[per["script"] == lab]
        if sub.empty:
            continue
        league_script[lab] = {
            "n": int(len(sub)),
            "pass_yds": round(float(sub["pass_yds"].mean()), 1),
            "rush_yds": round(float(sub["rush_yds"].mean()), 1),
            "fg": round(float(sub["kick_fg"].mean()), 2),
            "kick_pts": round(float(sub["kick_pts"].mean()), 2),
            "pass_share": round(float(sub["pass_yds"].sum() / max(sub["yards"].sum(), 1)), 3),
        }
    base = {
        "n": int(len(per)),
        "pass_yds": round(float(per["pass_yds"].mean()), 1),
        "rush_yds": round(float(per["rush_yds"].mean()), 1),
        "fg": round(float(per["kick_fg"].mean()), 2),
        "kick_pts": round(float(per["kick_pts"].mean()), 2),
        "pass_share": round(float(per["pass_yds"].sum() / max(per["yards"].sum(), 1)), 3),
    }
    leans: dict[str, Any] = {"baseline": base, "by_script": league_script}
    if "divisional" in per.columns:
        dflag = per["divisional"].astype(str).str.lower().isin(["true", "1"]) | per["divisional"].eq(True)
        for name, mask in (("divisional", dflag), ("non_divisional", ~dflag)):
            sub = per[mask]
            if sub.empty:
                continue
            leans[name] = {
                "n": int(len(sub)),
                "fg": round(float(sub["kick_fg"].mean()), 2),
                "kick_pts": round(float(sub["kick_pts"].mean()), 2),
                "pass_yds": round(float(sub["pass_yds"].mean()), 1),
                "rush_yds": round(float(sub["rush_yds"].mean()), 1),
            }

    for lab in ("leading", "trailing", "close"):
        pcol, rcol, fcol = f"{lab}_pass", f"{lab}_rush", f"{lab}_fg"
        if pcol in team.columns:
            team[f"{lab}_pass_delta"] = team[pcol] - team["pass_ypg"]
            team[f"{lab}_rush_delta"] = team[rcol] - team["rush_ypg"]
            team[f"{lab}_fg_delta"] = team[fcol] - team["fg_pg"]

    team["season"] = yr
    team = team.sort_values(["vehicle", "rush_share"], ascending=[True, False]).reset_index(drop=True)
    return {"season": yr, "teams": team, "script": leans, "game_n": int(len(per))}


def write_scoring_vehicle_artifacts(repo: Path | None = None, *, season: int | None = None) -> dict[str, Any]:
    root = repo or _REPO
    payload = build_scoring_vehicles(root, season=season)
    teams: pd.DataFrame = payload["teams"]
    vpath = root / "Sports/NFL/data/nfl_scoring_vehicles.csv"
    spath = root / "Sports/NFL/data/nfl_game_script_leans.json"
    vpath.parent.mkdir(parents=True, exist_ok=True)
    if not teams.empty:
        teams.to_csv(vpath, index=False, encoding="utf-8-sig")
    spath.write_text(json.dumps({"season": payload["season"], **payload["script"]}, indent=2), encoding="utf-8")
    payload["vehicle_path"] = vpath
    payload["script_path"] = spath
    return payload


_IDENTITY_CACHE: dict[str, Any] = {"mtime": None, "map": {}}


def _flag(v: object) -> bool:
    if isinstance(v, bool):
        return v
    s = str(v or "").strip().lower()
    return s in ("true", "1", "yes")


def load_team_identity_map(repo: Path | None = None) -> dict[str, dict[str, Any]]:
    root = repo or _REPO
    path = root / "Sports/NFL/data/nfl_scoring_vehicles.csv"
    if not path.is_file():
        return {}
    try:
        mtime = path.stat().st_mtime
    except OSError:
        mtime = None
    if _IDENTITY_CACHE.get("mtime") == mtime and _IDENTITY_CACHE.get("map"):
        return _IDENTITY_CACHE["map"]
    veh = pd.read_csv(path, encoding="utf-8-sig")
    if veh.empty or "team" not in veh.columns:
        _IDENTITY_CACHE.update({"mtime": mtime, "map": {}})
        return {}
    if "is_run_team" not in veh.columns:
        veh = _apply_vehicle_flags(veh)
    out: dict[str, dict[str, Any]] = {}
    for _, r in veh.iterrows():
        team = canon_nfl_abbr(r.get("team"))
        if not team:
            continue
        flags = vehicle_flags(r.get("vehicle") or r.get("off_identity"))
        out[team] = {
            "off_identity": str(r.get("off_identity") or flags["off_identity"] or ""),
            "team_vehicle": str(r.get("vehicle") or ""),
            "is_run_team": _flag(r.get("is_run_team")) or flags["is_run_team"],
            "is_pass_team": _flag(r.get("is_pass_team")) or flags["is_pass_team"],
            "is_fg_team": _flag(r.get("is_fg_team")) or flags["is_fg_team"],
            "is_balanced": _flag(r.get("is_balanced")) or flags["is_balanced"],
            "team_pass_share": r.get("pass_share"),
            "team_rush_share": r.get("rush_share"),
            "team_fg_pg": r.get("fg_pg"),
        }
    _IDENTITY_CACHE.update({"mtime": mtime, "map": out})
    return out


def stamp_nfl_identity(obj: dict[str, Any], repo: Path | None = None) -> dict[str, Any]:
    """Copy PASS/RUN/FG flags + player share onto a slate / ticket / matchup dict."""
    from utils.team_share import load_share_payload, lookup_player_share, _norm_prop

    team = canon_nfl_abbr(obj.get("team") or obj.get("team_abbr") or obj.get("slate_abbr") or "")
    ident = load_team_identity_map(repo).get(team) or {}
    if ident:
        obj["off_identity"] = ident.get("off_identity")
        obj["team_vehicle"] = ident.get("team_vehicle")
        obj["is_run_team"] = bool(ident.get("is_run_team"))
        obj["is_pass_team"] = bool(ident.get("is_pass_team"))
        obj["is_fg_team"] = bool(ident.get("is_fg_team"))
        obj["is_balanced"] = bool(ident.get("is_balanced"))
        obj["team_pass_share"] = ident.get("team_pass_share")
        obj["team_rush_share"] = ident.get("team_rush_share")
        obj["team_fg_pg"] = ident.get("team_fg_pg")
    share = load_share_payload("nfl", repo)
    player = obj.get("player") or obj.get("player_name") or obj.get("player_norm") or ""
    prop = obj.get("prop") or obj.get("prop_type") or obj.get("prop_type_normalized") or ""
    if share and share.get("applicable") and player and prop:
        hit = lookup_player_share(share, team=team, player=str(player), prop=_norm_prop(prop))
        if hit and obj.get("share_pct") is None and obj.get("prop_share_pct") is None:
            obj["share_pct"] = hit.get("share_pct")
            obj["prop_share_pct"] = hit.get("share_pct")
            obj["share_player_avg"] = hit.get("player_avg")
            obj["team_avg"] = hit.get("team_avg")
        elif hit:
            obj.setdefault("prop_share_pct", hit.get("share_pct"))
        for col, share_prop in SHARE_CONTEXT_PROPS:
            if obj.get(col) is not None:
                continue
            ch = lookup_player_share(share, team=team, player=str(player), prop=share_prop)
            if ch:
                obj[col] = ch.get("share_pct")
    return obj


def enrich_nfl_web_rows(rows: list[dict[str, Any]], repo: Path | None = None) -> list[dict[str, Any]]:
    for r in rows:
        if isinstance(r, dict):
            stamp_nfl_identity(r, repo)
    return rows


def enrich_nfl_matchup_edge_payload(payload: dict[str, Any], repo: Path | None = None) -> dict[str, Any]:
    if not isinstance(payload, dict) or payload.get("error"):
        return payload
    ident = load_team_identity_map(repo)
    for t in payload.get("teams") or []:
        if not isinstance(t, dict):
            continue
        team = canon_nfl_abbr(t.get("slate_abbr") or t.get("def_key") or t.get("name") or "")
        hit = ident.get(team)
        if not hit:
            continue
        t["off_identity"] = hit.get("off_identity")
        t["is_run_team"] = bool(hit.get("is_run_team"))
        t["is_pass_team"] = bool(hit.get("is_pass_team"))
        t["is_fg_team"] = bool(hit.get("is_fg_team"))
        t["is_balanced"] = bool(hit.get("is_balanced"))
        t["team_rush_share"] = hit.get("team_rush_share")
        t["team_pass_share"] = hit.get("team_pass_share")
        t["team_fg_pg"] = hit.get("team_fg_pg")
    payload["nfl_identity"] = {
        "applicable": bool(ident),
        "n_teams": len(ident),
    }
    return payload


def nfl_identity_chips_html(obj: dict[str, Any]) -> str:
    """Tiny HTML chips for tickets / server-rendered pages."""
    bits: list[str] = []
    if _flag(obj.get("is_run_team")):
        bits.append('<span class="nfl-id-chip nfl-id-run">Rush</span>')
    if _flag(obj.get("is_pass_team")):
        bits.append('<span class="nfl-id-chip nfl-id-pass">Pass</span>')
    if _flag(obj.get("is_fg_team")):
        bits.append('<span class="nfl-id-chip nfl-id-fg">FG</span>')
    if not bits and _flag(obj.get("is_balanced")):
        bits.append('<span class="nfl-id-chip nfl-id-bal">Bal</span>')
    share = obj.get("share_pct") if obj.get("share_pct") is not None else obj.get("prop_share_pct")
    try:
        if share is not None and str(share).strip() not in ("", "nan", "None"):
            bits.append(f'<span class="nfl-id-chip nfl-id-share">{float(share):.0f}% shr</span>')
    except (TypeError, ValueError):
        pass
    return " ".join(bits)
