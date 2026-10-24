"""Backfill NFL boxscore RUSH_ATT / PASS_ATT from ESPN summaries."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from utils.nfl_stat_leaders import season_from_game_date

_REPO = Path(__file__).resolve().parents[1]
CACHE_PATH = _REPO / "Sports/NFL/data/cache/nfl_boxscore_cache.csv"
ESPN_SUMMARY = "https://site.web.api.espn.com/apis/site/v2/sports/football/nfl/summary"


def _f(val: object) -> float | None:
    try:
        s = str(val or "").strip()
        if s in ("", "--", "nan", "None"):
            return None
        if "/" in s:
            left, right = s.split("/", 1)
            return float(right.strip() or left)
        return float(s)
    except Exception:
        return None


def _cmp_att(raw: object) -> tuple[float | None, float | None]:
    s = str(raw or "").strip()
    if "/" not in s:
        return None, _f(s)
    left, right = s.split("/", 1)
    return _f(left), _f(right)


def parse_volume_from_summary(summary: dict[str, Any]) -> dict[tuple[str, str], dict[str, float]]:
    """Map (espn_athlete_id or player_norm, team_id) -> RUSH_ATT / PASS_ATT."""
    out: dict[tuple[str, str], dict[str, float]] = {}
    box = summary.get("boxscore") or {}
    for tb in box.get("players") or []:
        if not isinstance(tb, dict):
            continue
        team_id = str((tb.get("team") or {}).get("id") or "").strip()
        if not team_id:
            continue
        for grp in tb.get("statistics") or []:
            if not isinstance(grp, dict):
                continue
            cat = str(grp.get("name") or "").strip().lower()
            labels = [str(x).upper() for x in (grp.get("labels") or [])]
            if not labels:
                continue

            def idx(lbl: str) -> int | None:
                return labels.index(lbl) if lbl in labels else None

            for a in grp.get("athletes") or []:
                if not isinstance(a, dict):
                    continue
                ath = a.get("athlete") or {}
                aid = str(ath.get("id") or "").strip()
                pn = " ".join(str(ath.get("displayName") or ath.get("fullName") or "").strip().lower().split())
                st = a.get("stats") or []
                key = (aid or pn, team_id)
                rec = out.setdefault(key, {})
                rec["_player_norm"] = pn
                rec["_aid"] = aid
                rec["_team_id"] = team_id
                if cat == "rushing":
                    car = idx("CAR")
                    if car is None:
                        car = idx("ATT")
                    if car is not None and car < len(st):
                        att = _f(st[car])
                        if att is not None:
                            rec["RUSH_ATT"] = att
                elif cat == "passing":
                    catt = idx("C/ATT")
                    if catt is None:
                        catt = idx("CMP/ATT")
                    att_i = idx("ATT")
                    att_v = None
                    if catt is not None and catt < len(st):
                        _, att_v = _cmp_att(st[catt])
                    elif att_i is not None and att_i < len(st):
                        att_v = _f(st[att_i])
                    if att_v is not None:
                        rec["PASS_ATT"] = att_v
    return out


def fetch_summary(event_id: str, *, timeout: float = 20.0) -> dict[str, Any]:
    r = requests.get(ESPN_SUMMARY, params={"event": event_id}, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    return data if isinstance(data, dict) else {}


def backfill_nfl_box_volume(
    repo: Path | None = None,
    *,
    season: int | None = None,
    limit: int | None = None,
    sleep: float = 0.08,
) -> dict[str, Any]:
    root = repo or _REPO
    path = root / "Sports/NFL/data/cache/nfl_boxscore_cache.csv"
    if not path.is_file():
        return {"ok": False, "reason": f"missing {path}"}
    df = pd.read_csv(path)
    if "game_date" in df.columns and season is not None:
        df["_season"] = df["game_date"].map(season_from_game_date)
        events = (
            df.loc[df["_season"] == int(season), "event_id"]
            .astype(str)
            .str.strip()
            .replace("", pd.NA)
            .dropna()
            .unique()
            .tolist()
        )
    else:
        events = (
            df["event_id"].astype(str).str.strip().replace("", pd.NA).dropna().unique().tolist()
        )
    events = [e for e in events if e and e.lower() not in ("nan", "none")]
    if "RUSH_ATT" in df.columns:
        have_vol = set(
            df.loc[pd.to_numeric(df["RUSH_ATT"], errors="coerce").fillna(0).gt(0), "event_id"]
            .astype(str)
            .tolist()
        )
        events = [e for e in events if e not in have_vol]
    if limit:
        events = events[: int(limit)]

    if "RUSH_ATT" not in df.columns:
        df["RUSH_ATT"] = pd.NA
    if "PASS_ATT" not in df.columns:
        df["PASS_ATT"] = pd.NA

    filled_rush = 0
    filled_pass = 0
    fetched = 0
    failed = 0
    for eid in events:
        try:
            summary = fetch_summary(eid)
            vol = parse_volume_from_summary(summary)
            fetched += 1
        except Exception:
            failed += 1
            continue
        if not vol:
            time.sleep(sleep)
            continue
        mask = df["event_id"].astype(str) == str(eid)
        sub_idx = df.index[mask]
        by_aid = {(str(v.get("_aid") or ""), str(v.get("_team_id") or "")): v for v in vol.values() if v.get("_aid")}
        by_name = {
            (str(v.get("_player_norm") or ""), str(v.get("_team_id") or "")): v
            for v in vol.values()
            if v.get("_player_norm")
        }
        for i in sub_idx:
            aid = str(df.at[i, "espn_athlete_id"] or "").strip() if "espn_athlete_id" in df.columns else ""
            tid = str(df.at[i, "team_id"] or "").strip()
            pn = " ".join(str(df.at[i, "player_norm"] or "").strip().lower().split())
            rec = by_aid.get((aid, tid)) if aid else None
            if rec is None:
                rec = by_name.get((pn, tid))
            if not rec:
                continue
            if rec.get("RUSH_ATT") is not None:
                df.at[i, "RUSH_ATT"] = rec["RUSH_ATT"]
                filled_rush += 1
            if rec.get("PASS_ATT") is not None:
                df.at[i, "PASS_ATT"] = rec["PASS_ATT"]
                filled_pass += 1
        time.sleep(sleep)

    df.to_csv(path, index=False)
    return {
        "ok": True,
        "path": str(path),
        "events": len(events),
        "fetched": fetched,
        "failed": failed,
        "filled_rush_att": filled_rush,
        "filled_pass_att": filled_pass,
        "rush_att_nonzero": int(pd.to_numeric(df["RUSH_ATT"], errors="coerce").fillna(0).gt(0).sum()),
        "pass_att_nonzero": int(pd.to_numeric(df["PASS_ATT"], errors="coerce").fillna(0).gt(0).sum()),
    }
