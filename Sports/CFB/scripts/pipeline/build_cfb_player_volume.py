#!/usr/bin/env python3
"""
Per-player CFB rush-attempt / pass-attempt / reception-target rates.

Built from ESPN boxscore cache after step5b parses:
  RUSH_ATT  — rushing CAR
  PASS_ATT  — passing C/ATT (right side)
  REC_TGT   — drive PBP ``to #N`` / ``intended for #N``

Output: Sports/CFB/data/reference/cfb_player_volume.csv

  py -3.14 Sports/CFB/scripts/pipeline/build_cfb_player_volume.py
  py -3.14 Sports/CFB/scripts/pipeline/build_cfb_player_volume.py --season 2025
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

_SCRIPT = Path(__file__).resolve()
_CFB_ROOT = _SCRIPT.parents[2]
_REPO = _SCRIPT.parents[4]
_DEFAULT_OUT = _CFB_ROOT / "data" / "reference" / "cfb_player_volume.csv"
_DEFAULT_CACHE = _CFB_ROOT / "data" / "cache" / "cfb_boxscore_cache.csv"

VOLUME_COLS = (
    "rush_att_pg",
    "rush_att_share",
    "rec_tgt_pg",
    "rec_tgt_share",
    "rec_pg",
    "pass_att_pg",
    "pass_att_share",
    "volume_games",
    "volume_team_id",
)


def _season_of(game_date: object) -> str:
    s = str(game_date or "").strip().replace("-", "")[:8]
    if len(s) < 8 or not s.isdigit():
        return ""
    year, month = int(s[:4]), int(s[4:6])
    if month < 1 or month > 12:
        return ""
    return str(year if month >= 8 else year - 1)


def _fnum(v: object) -> float | None:
    if v is None:
        return None
    s = str(v).strip()
    if s in ("", "nan", "None", "none"):
        return None
    try:
        x = float(s)
    except (TypeError, ValueError):
        return None
    if x != x:
        return None
    return x


def _id_token(v: object) -> str:
    s = str(v or "").strip()
    if s.lower() in ("", "nan", "none"):
        return ""
    if s.endswith(".0") and s[:-2].isdigit():
        return s[:-2]
    return s


def flatten_histories(hist_aid: dict, hist_name: dict) -> list[dict]:
    seen: set[tuple[str, str, str]] = set()
    rows: list[dict] = []
    for src in (hist_aid, hist_name):
        for glist in src.values():
            for g in glist or []:
                if not isinstance(g, dict):
                    continue
                key = (
                    str(g.get("event_id") or "").strip(),
                    _id_token(g.get("espn_athlete_id")),
                    str(g.get("player_norm") or "").strip(),
                )
                if key in seen:
                    continue
                seen.add(key)
                rows.append(g)
    return rows


def aggregate_player_volume(rows: Iterable[dict[str, Any]]) -> pd.DataFrame:
    """Season rates + game-level share of team attempts/targets."""
    games: dict[tuple[str, str], dict[str, float]] = {}
    players: list[dict[str, Any]] = []
    for rr in rows:
        if not isinstance(rr, dict):
            continue
        eid = str(rr.get("event_id") or "").strip()
        tid = _id_token(rr.get("team_id"))
        if not eid or not tid:
            continue
        gk = (eid, tid)
        bucket = games.setdefault(gk, {"rush": 0.0, "pass": 0.0, "tgt": 0.0})
        ra = _fnum(rr.get("RUSH_ATT"))
        pa = _fnum(rr.get("PASS_ATT"))
        tg = _fnum(rr.get("REC_TGT"))
        if ra is not None:
            bucket["rush"] += ra
        if pa is not None:
            bucket["pass"] += pa
        if tg is not None:
            bucket["tgt"] += tg
        players.append(rr)

    acc: dict[tuple[str, str, str], dict[str, Any]] = {}
    for rr in players:
        eid = str(rr.get("event_id") or "").strip()
        tid = _id_token(rr.get("team_id"))
        season = _season_of(rr.get("game_date"))
        aid = _id_token(rr.get("espn_athlete_id"))
        pn = str(rr.get("player_norm") or "").strip()
        if not tid or not season or (not aid and not pn):
            continue
        key = (aid or pn, tid, season)
        row = acc.setdefault(
            key,
            {
                "espn_athlete_id": aid,
                "player_norm": pn,
                "team_id": tid,
                "team_abbr": str(rr.get("team_abbr") or "").strip(),
                "season": season,
                "games": 0,
                "_rush": [],
                "_rush_share": [],
                "_pass": [],
                "_pass_share": [],
                "_tgt": [],
                "_tgt_share": [],
                "_rec": [],
            },
        )
        if pn and not row["player_norm"]:
            row["player_norm"] = pn
        if aid and not row["espn_athlete_id"]:
            row["espn_athlete_id"] = aid
        ab = str(rr.get("team_abbr") or "").strip()
        if ab and not row["team_abbr"]:
            row["team_abbr"] = ab
        row["games"] += 1
        team = games.get((eid, tid), {})
        ra = _fnum(rr.get("RUSH_ATT"))
        pa = _fnum(rr.get("PASS_ATT"))
        tg = _fnum(rr.get("REC_TGT"))
        rec = _fnum(rr.get("REC"))
        if ra is not None:
            row["_rush"].append(ra)
            tr = float(team.get("rush") or 0.0)
            if tr > 0:
                row["_rush_share"].append(ra / tr)
        if pa is not None:
            row["_pass"].append(pa)
            tp = float(team.get("pass") or 0.0)
            if tp > 0:
                row["_pass_share"].append(pa / tp)
        if tg is not None:
            row["_tgt"].append(tg)
            tt = float(team.get("tgt") or 0.0)
            if tt > 0:
                row["_tgt_share"].append(tg / tt)
        if rec is not None:
            row["_rec"].append(rec)

    out_rows: list[dict[str, Any]] = []
    for row in acc.values():
        def _mean(xs: list[float]) -> float | None:
            return round(sum(xs) / len(xs), 3) if xs else None

        out_rows.append(
            {
                "espn_athlete_id": row["espn_athlete_id"],
                "player_norm": row["player_norm"],
                "team_id": row["team_id"],
                "team_abbr": row["team_abbr"],
                "season": row["season"],
                "volume_games": int(row["games"]),
                "rush_att_pg": _mean(row["_rush"]),
                "rush_att_share": _mean(row["_rush_share"]),
                "pass_att_pg": _mean(row["_pass"]),
                "pass_att_share": _mean(row["_pass_share"]),
                "rec_tgt_pg": _mean(row["_tgt"]),
                "rec_tgt_share": _mean(row["_tgt_share"]),
                "rec_pg": _mean(row["_rec"]),
            }
        )
    if not out_rows:
        return pd.DataFrame()
    df = pd.DataFrame(out_rows)
    return df.sort_values(
        ["season", "team_id", "rush_att_pg", "rec_tgt_pg"],
        ascending=[False, True, False, False],
        na_position="last",
    ).reset_index(drop=True)


def attach_player_volume(
    df: pd.DataFrame,
    vol: pd.DataFrame,
    *,
    season: str = "",
) -> pd.DataFrame:
    """Merge volume rates onto a slate. Prefer same team_id, else most games."""
    if df is None or df.empty or vol is None or vol.empty:
        return df
    work = vol.copy()
    if season:
        seas = work["season"].astype(str).str.strip()
        hit = work[seas == str(season).strip()]
        if not hit.empty:
            work = hit
    work["espn_athlete_id"] = work["espn_athlete_id"].map(_id_token)
    work["team_id"] = work["team_id"].map(_id_token)
    work["player_norm"] = work["player_norm"].astype(str).str.strip()
    work["volume_games"] = pd.to_numeric(work["volume_games"], errors="coerce")

    by_aid: dict[str, pd.DataFrame] = {}
    if "espn_athlete_id" in work.columns:
        for aid, g in work.groupby("espn_athlete_id"):
            if aid:
                by_aid[str(aid)] = g
    by_name: dict[tuple[str, str], pd.Series] = {}
    for _, r in work.iterrows():
        pn = str(r.get("player_norm") or "").strip()
        tid = _id_token(r.get("team_id"))
        if pn and tid:
            by_name[(pn, tid)] = r

    def _pick(aid: str, pn: str, tid: str) -> pd.Series | None:
        g = by_aid.get(aid)
        if g is not None and not g.empty:
            same = g[g["team_id"].astype(str) == tid] if tid else g
            pool = same if not same.empty else g
            if "volume_games" in pool.columns:
                pool = pool.sort_values("volume_games", ascending=False)
            return pool.iloc[0]
        if pn and tid and (pn, tid) in by_name:
            return by_name[(pn, tid)]
        return None

    picked: list[dict[str, Any]] = []
    for _, r in df.iterrows():
        aid = _id_token(r.get("espn_athlete_id"))
        pn = str(r.get("player_norm") or r.get("player") or "").strip()
        tid = _id_token(r.get("team_id"))
        hit = _pick(aid, pn, tid)
        if hit is None:
            picked.append({c: "" for c in VOLUME_COLS})
            continue
        payload = {c: hit[c] if c in hit.index else "" for c in VOLUME_COLS if c != "volume_team_id"}
        payload["volume_team_id"] = _id_token(hit.get("team_id"))
        payload["volume_games"] = hit.get("volume_games", "")
        picked.append(payload)

    extra = pd.DataFrame(picked)
    out = df.reset_index(drop=True).copy()
    for c in extra.columns:
        out[c] = extra[c].values
    return out


def _load_cache_rows() -> list[dict]:
    if str(_REPO) not in sys.path:
        sys.path.insert(0, str(_REPO))
    from scripts.espn_boxscore_cache import load_boxscore_cache

    rows, src = load_boxscore_cache("cfb", str(_DEFAULT_CACHE))
    print(f"[CFB volume] cache source={src} rows={len(rows)}")
    return rows


def refetch_missing_events(*, workers: int = 6) -> list[dict]:
    """Refetch every cached event still missing CAR / C/ATT / PBP targets.

    Does not scan scoreboards. Covers teams that are not on today's slate
    so their player volume is ready when they post.
    """
    import importlib.util
    import random
    import time
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if str(_REPO) not in sys.path:
        sys.path.insert(0, str(_REPO))
    from scripts.espn_boxscore_cache import load_boxscore_cache, save_boxscore_cache

    path = Path(__file__).resolve().parent / "step5b_attach_boxscore_stats.py"
    spec = importlib.util.spec_from_file_location("cfb_step5b_vol", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Cannot load {path}")
    s5 = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(s5)
    s5.ESPN_LEAGUE = "college-football"

    rows, src = load_boxscore_cache("cfb", str(_DEFAULT_CACHE))
    print(f"[CFB volume] cache source={src} rows={len(rows)}")

    def _present(rr: dict, col: str) -> bool:
        return _fnum(rr.get(col)) is not None

    eids_vol: set[str] = set()
    eids_tgt: set[str] = set()
    eids_rec: set[str] = set()
    eid_meta: dict[str, str] = {}
    opp_restore: dict[tuple[str, str], tuple[str, str]] = {}
    for rr in rows:
        eid = str(rr.get("event_id") or "").strip()
        tid = _id_token(rr.get("team_id"))
        if not eid:
            continue
        if eid not in eid_meta:
            ds = str(rr.get("game_date") or "").strip().replace("-", "")[:8]
            eid_meta[eid] = ds
        opp = str(rr.get("opp_team_id") or "").strip()
        oab = str(rr.get("opp_team_abbr") or "").strip()
        if tid and (opp or oab):
            opp_restore[(eid, tid)] = (opp, oab)
        if _present(rr, "RUSH_ATT") or _present(rr, "PASS_ATT"):
            eids_vol.add(eid)
        if _present(rr, "REC_TGT") and float(_fnum(rr.get("REC_TGT")) or 0) > 0:
            eids_tgt.add(eid)
        if _present(rr, "REC") and float(_fnum(rr.get("REC")) or 0) > 0:
            eids_rec.add(eid)

    all_eids = set(eid_meta)
    need = {e for e in all_eids if e not in eids_vol}
    need |= {e for e in all_eids if e in eids_rec and e not in eids_tgt}
    print(
        f"[CFB volume] events={len(all_eids)} have_att={len(eids_vol)} "
        f"have_tgt={len(eids_tgt)} refetch={len(need)}"
    )
    if not need:
        return rows

    def _one(eid: str) -> list[dict]:
        time.sleep(random.uniform(0.04, 0.18))
        summ = s5.pull_summary(eid)
        parsed = s5.parse_players(summ, game_date=eid_meta.get(eid, ""), event_id=eid)
        for rec in parsed:
            key = (eid, _id_token(rec.get("team_id")))
            if key in opp_restore and not str(rec.get("opp_team_id") or "").strip():
                oid, oab = opp_restore[key]
                if oid:
                    rec["opp_team_id"] = oid
                if oab:
                    rec["opp_team_abbr"] = oab
        return parsed

    new_rows: list[dict] = []
    fetched = 0
    with ThreadPoolExecutor(max_workers=max(1, int(workers))) as pool:
        futs = {pool.submit(_one, eid): eid for eid in sorted(need)}
        done = 0
        for fut in as_completed(futs):
            done += 1
            try:
                got = fut.result()
                if got:
                    fetched += 1
                    new_rows.extend(got)
            except Exception as exc:
                print(f"  [WARN] event {futs[fut]}: {exc}")
            if done % 50 == 0 or done == len(need):
                print(f"  ... {done}/{len(need)} events ({len(new_rows)} player-rows)")

    print(f"[CFB volume] fetched {fetched}/{len(need)} events, {len(new_rows)} player-rows")
    keep = [rr for rr in rows if str(rr.get("event_id") or "").strip() not in need]
    all_rows = keep + new_rows
    save_boxscore_cache("cfb", new_rows, str(_DEFAULT_CACHE), "db")
    return all_rows


def main() -> None:
    ap = argparse.ArgumentParser(description="Build CFB player attempt/target volume table")
    ap.add_argument("--season", default="", help="Keep only this season (e.g. 2025)")
    ap.add_argument("--out", default=str(_DEFAULT_OUT))
    ap.add_argument(
        "--refetch-missing",
        action="store_true",
        help="Refetch cached events missing rush/pass att or PBP targets (all teams)",
    )
    ap.add_argument("--workers", type=int, default=6)
    args = ap.parse_args()

    if args.refetch_missing:
        rows = refetch_missing_events(workers=int(args.workers))
    else:
        rows = _load_cache_rows()
    vol = aggregate_player_volume(rows)
    if vol.empty:
        print("[CFB volume] no rows — cache needs RUSH_ATT/PASS_ATT from a step5b refetch")
        return
    n_att = int(pd.to_numeric(vol["rush_att_pg"], errors="coerce").notna().sum())
    n_tgt = int(pd.to_numeric(vol["rec_tgt_pg"], errors="coerce").notna().sum())
    if n_att == 0 and n_tgt == 0:
        print(
            "[CFB volume] cache has no RUSH_ATT / REC_TGT yet. "
            "Re-run with --refetch-missing."
        )
    if str(args.season).strip():
        vol = vol[vol["season"].astype(str) == str(args.season).strip()].copy()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    vol.to_csv(out, index=False)
    print(f"[CFB volume] Wrote {out} players={len(vol)} rush_att={n_att} rec_tgt={n_tgt}")


if __name__ == "__main__":
    main()
