#!/usr/bin/env python3
"""
Golf step4 — ESPN PGA scoreboard round history cache + stat_g1..10 attachment.

Walks dated scoreboards (?dates=YYYYMMDD-YYYYMMDD) and caches per-round stats to
Sports/Golf/cache/golf_round_cache.csv, then joins onto step2 props.

Round counting stats (birdies / pars / bogeys) are derived from hole-by-hole
``scoreType`` when ESPN nests 18 hole linescores. Summary index 5 is **pars**,
not holes (old residual-pars bug). Rows that fail integrity get
``golf_attach_suspect=1`` and are excluded from counting-prop L5 windows.

Strokes also store ``to_par`` and ``vs_field`` (score minus that round's field
average) for course/weather-normalized L5.

Run:
  py -3.14 Sports/Golf/scripts/step4_attach_player_stats_golf.py \\
      --input outputs/2026-06-12/golf/step2_golf_context.csv \\
      --output outputs/2026-06-12/golf/step4_golf_with_stats.csv \\
      --cache Sports/Golf/cache/golf_round_cache.csv
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import requests

_REPO = Path(__file__).resolve().parents[3]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))
from utils.eastern_slate_date import eastern_slate_date
from utils.golf_scorecard import (
    counting_prop_ok,
    integrity_flags,
    is_match_play_event,
    parse_round_stats,
)
from utils.player_name_utils import normalize_player_name

SCOREBOARD_PATH = "apis/site/v2/sports/golf/pga/scoreboard"
SCOREBOARD_HOSTS = (
    "https://site.web.api.espn.com",
    "https://site.api.espn.com",
)
CACHE_COLUMNS = [
    "espn_id",
    "player_name",
    "player_key",
    "tournament_date",
    "tournament_name",
    "event_id",
    "course_id",
    "round",
    "round_date",
    "player_status",
    "strokes",
    "birdies",
    "bogeys",
    "pars",
    "birdies_or_better",
    "bogeys_or_worse",
    "holes_played",
    "to_par",
    "course_par",
    "vs_field",
    "stat_source",
    "attach_suspect",
    "attach_flags",
]

COUNTING_STATS = {"birdies", "pars", "birdies_or_better", "bogeys_or_worse", "bogeys"}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.espn.com/golf/",
    "Origin": "https://www.espn.com",
}


def _player_key(name: str) -> str:
    return normalize_player_name(name).lower()


def prop_stat_key(prop_type: str) -> str | None:
    s = str(prop_type or "").strip().lower()
    if not s or "matchup" in s:
        return None
    if "greens in regulation" in s or s == "gir":
        return None
    if "fairway" in s:
        return None
    if "stroke" in s:
        return "strokes"
    if "birdies" in s and "better" in s:
        return "birdies_or_better"
    if "bogey" in s and "worse" in s:
        return "bogeys_or_worse"
    if "pars" in s or s == "par":
        return "pars"
    return None


def _venue_id(ev: dict, comp: dict) -> str:
    for obj in (comp, ev):
        venue = (obj or {}).get("venue") or {}
        if isinstance(venue, dict):
            vid = str(venue.get("id") or "").strip()
            if vid:
                return vid
    return ""


def _competitor_status(player: dict) -> str:
    st = player.get("status")
    if isinstance(st, dict):
        t = st.get("type") if isinstance(st.get("type"), dict) else {}
        raw = t.get("abbreviation") or t.get("name") or st.get("displayName") or ""
        return str(raw).strip().upper()
    return str(st or "").strip().upper()


def _linescore_date(rnd: dict, event_start: str, round_n: int) -> str:
    raw = str(rnd.get("date") or rnd.get("startDate") or "").strip()
    if raw:
        try:
            return eastern_slate_date(raw)
        except Exception:
            pass
    try:
        d0 = date.fromisoformat(str(event_start)[:10])
        rn = max(1, int(round_n))
        return (d0 + timedelta(days=rn - 1)).isoformat()
    except (TypeError, ValueError):
        return str(event_start)[:10]


def _event_date(ev: dict) -> str:
    raw = str(ev.get("date") or ev.get("startDate") or "").strip()
    if not raw:
        comps = ev.get("competitions") or []
        if comps:
            raw = str(comps[0].get("date") or "").strip()
    return eastern_slate_date(raw)


def _fetch_scoreboard_range(start: date, end: date) -> list[dict]:
    dates = f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
    last_err: Exception | str | None = None
    for host in SCOREBOARD_HOSTS:
        url = f"{host}/{SCOREBOARD_PATH}"
        try:
            r = requests.get(url, params={"dates": dates}, headers=HEADERS, timeout=25)
            if r.status_code == 403:
                last_err = f"403 from {host.split('//', 1)[-1]}"
                continue
            r.raise_for_status()
            payload = r.json()
            if isinstance(payload, dict):
                return list(payload.get("events") or [])
            last_err = "non-dict JSON"
        except Exception as exc:
            last_err = exc
            continue
    print(f"  [WARN] scoreboard fetch failed for {dates}: {last_err}")
    return []


def _rows_from_events(events: list[dict]) -> list[dict]:
    rows: list[dict] = []
    for ev in events:
        t_name = str(ev.get("name") or "").strip()
        if is_match_play_event(t_name):
            # Presidents Cup / Ryder Cup / etc. store hole wins (2–7), not strokes.
            continue
        t_date = _event_date(ev)
        event_id = str(ev.get("id") or "").strip()
        for comp in ev.get("competitions") or []:
            course_id = _venue_id(ev, comp)
            for p in comp.get("competitors") or []:
                ath = p.get("athlete") or {}
                pname = str(ath.get("displayName") or ath.get("shortName") or "").strip()
                if not pname:
                    continue
                pk = _player_key(pname)
                aid = str(ath.get("id") or "").strip()
                player_status = _competitor_status(p)
                for ri, rnd in enumerate(p.get("linescores") or [], start=1):
                    parsed = parse_round_stats(rnd)
                    strokes = parsed.get("strokes")
                    holes = parsed.get("holes_played")
                    # Strokes-only rounds still useful; counting props need integrity later.
                    if strokes is None:
                        continue
                    if holes is not None and holes < 9:
                        continue
                    flags = integrity_flags(parsed)
                    rows.append(
                        {
                            "espn_id": aid,
                            "player_name": pname,
                            "player_key": pk,
                            "tournament_date": t_date,
                            "tournament_name": t_name,
                            "event_id": event_id,
                            "course_id": course_id,
                            "round": ri,
                            "round_date": _linescore_date(rnd, t_date, ri),
                            "player_status": player_status,
                            "strokes": strokes,
                            "birdies": parsed.get("birdies"),
                            "bogeys": parsed.get("bogeys"),
                            "pars": parsed.get("pars"),
                            "birdies_or_better": parsed.get("birdies_or_better"),
                            "bogeys_or_worse": parsed.get("bogeys_or_worse"),
                            "holes_played": holes,
                            "to_par": parsed.get("to_par"),
                            "course_par": parsed.get("course_par"),
                            "vs_field": None,  # filled after field avg join
                            "stat_source": parsed.get("source") or "",
                            "attach_suspect": 1 if flags else 0,
                            "attach_flags": "|".join(flags),
                        }
                    )
    return rows


def _fill_blank_course_ids(df: pd.DataFrame) -> pd.DataFrame:
    """Slug tournament name when ESPN venue id is missing."""
    if df is None or df.empty:
        return df
    out = df.copy()
    if "course_id" not in out.columns:
        out["course_id"] = ""
    empty = out["course_id"].astype(str).str.strip().isin({"", "nan", "none", "None"})
    if empty.any() and "tournament_name" in out.columns:
        from utils.golf_course_baseline import course_key

        out.loc[empty, "course_id"] = out.loc[empty, "tournament_name"].map(
            lambda x: course_key({"tournament_name": x})
        )
    return out


def _fill_blank_round_dates(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    out = df.copy()
    if "round_date" not in out.columns:
        out["round_date"] = ""
    empty = out["round_date"].astype(str).str.strip().isin({"", "nan", "none", "None", "NaT"})
    if not empty.any():
        return out
    for idx in out.index[empty]:
        td = str(out.at[idx, "tournament_date"] if "tournament_date" in out.columns else "")[:10]
        rn = out.at[idx, "round"] if "round" in out.columns else 1
        try:
            d0 = date.fromisoformat(td)
            n = int(float(rn))
            if n < 1:
                n = 1
            out.at[idx, "round_date"] = (d0 + timedelta(days=n - 1)).isoformat()
        except (TypeError, ValueError):
            out.at[idx, "round_date"] = td
    return out


def _field_eligible_mask(df: pd.DataFrame) -> pd.Series:
    """Clean full-18 stroke-play rows only. Suspect / WD / match-play stay out."""
    if df is None or df.empty:
        return pd.Series(dtype=bool)
    strokes = pd.to_numeric(df.get("strokes"), errors="coerce")
    holes = pd.to_numeric(df.get("holes_played"), errors="coerce")
    suspect = pd.to_numeric(df.get("attach_suspect"), errors="coerce").fillna(0)
    ok = strokes.notna() & holes.notna() & (holes.sub(18).abs() < 0.1) & suspect.eq(0)
    if "tournament_name" in df.columns:
        ok &= ~df["tournament_name"].map(is_match_play_event)
    return ok.fillna(False)


def _attach_vs_field(df: pd.DataFrame) -> pd.DataFrame:
    """vs_field = strokes - field average for that tournament_date + round.

    Field mean uses clean completed 18-hole rounds only. A 0-stroke parse, a
    52 on 13 holes, a 9-hole WD, or an attach_suspect card must not move the
    mean — otherwise every clean round's vs_field shifts for that event-round.
    """
    out = df.copy()
    if out.empty or "strokes" not in out.columns:
        return out
    out["strokes"] = pd.to_numeric(out["strokes"], errors="coerce")
    keys = ["tournament_date", "round"]
    for k in keys:
        if k not in out.columns:
            out["vs_field"] = np.nan
            return out
    full = out.loc[_field_eligible_mask(out)].copy()
    if full.empty:
        out["vs_field"] = np.nan
        return out
    field = (
        full.groupby(keys, dropna=False)["strokes"]
        .mean()
        .rename("field_avg_strokes")
    )
    out = out.merge(field, on=keys, how="left")
    out["vs_field"] = out["strokes"] - out["field_avg_strokes"]
    out = out.drop(columns=["field_avg_strokes"], errors="ignore")
    return out


def repair_round_cache_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Drop match-play, re-key names, recompute integrity + vs_field."""
    if df is None or df.empty:
        return df
    out = df.copy()
    if "tournament_name" in out.columns:
        out = out.loc[~out["tournament_name"].map(is_match_play_event)].copy()
    if "player_name" in out.columns:
        out["player_key"] = out["player_name"].map(_player_key)
    out = _backfill_attach_flags(out)
    out = _fill_blank_course_ids(out)
    out = _fill_blank_round_dates(out)
    out = _attach_vs_field(out)
    return out


def _prefer_clean_round_rows(df: pd.DataFrame) -> pd.DataFrame:
    """One row per player/date/round — prefer scorecard + full-18 + non-suspect."""
    if df.empty:
        return df
    out = df.copy()
    if "player_key" not in out.columns and "player_name" in out.columns:
        out["player_key"] = out["player_name"].map(_player_key)
    holes = pd.to_numeric(out.get("holes_played"), errors="coerce")
    suspect = pd.to_numeric(out.get("attach_suspect"), errors="coerce").fillna(1)
    src = out.get("stat_source", pd.Series("", index=out.index)).astype(str).str.lower()
    # Higher score wins; stable sort then keep last.
    out["_pref"] = (
        (src.eq("scorecard").astype(int) * 8)
        + ((holes.sub(18).abs() < 0.1).fillna(False).astype(int) * 4)
        + ((suspect.eq(0)).astype(int) * 2)
        + src.ne("").astype(int)
    )
    out = out.sort_values("_pref", ascending=True, kind="mergesort")
    out = out.drop_duplicates(
        subset=["player_key", "tournament_date", "round"],
        keep="last",
    )
    return out.drop(columns=["_pref"], errors="ignore")


def _cache_is_fresh(cache_path: Path, max_age_hours: float = 24.0) -> bool:
    if not cache_path.is_file():
        return False
    age_h = (time.time() - cache_path.stat().st_mtime) / 3600.0
    return age_h < max_age_hours


def _backfill_attach_flags(df: pd.DataFrame) -> pd.DataFrame:
    """Recompute integrity on legacy cache rows that predate attach_flags."""
    if df.empty:
        return df
    out = df.copy()
    for c in (
        "strokes",
        "birdies",
        "bogeys",
        "pars",
        "birdies_or_better",
        "bogeys_or_worse",
        "holes_played",
        "to_par",
        "course_par",
        "vs_field",
        "attach_suspect",
    ):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    flags_col: list[str] = []
    sus_col: list[int] = []
    for _, row in out.iterrows():
        flags = integrity_flags(row.to_dict())
        # Legacy residual rows often lack eagles/doubles; still flag bad pars/holes.
        flags_col.append("|".join(flags))
        sus_col.append(1 if flags else 0)
    out["attach_flags"] = flags_col
    out["attach_suspect"] = sus_col
    return out


def load_round_cache(cache_path: Path) -> pd.DataFrame:
    if not cache_path.is_file():
        return pd.DataFrame(columns=CACHE_COLUMNS)
    try:
        df = pd.read_csv(cache_path, dtype=str, encoding="utf-8-sig").fillna("")
    except Exception:
        return pd.DataFrame(columns=CACHE_COLUMNS)
    for c in CACHE_COLUMNS:
        if c not in df.columns:
            df[c] = ""
    return repair_round_cache_frame(df)


def refresh_round_cache(cache_path: Path, weeks_back: int = 12) -> pd.DataFrame:
    today = date.today()
    start_floor = today - timedelta(days=weeks_back * 7 + 7)
    fetched: list[dict] = []
    for w in range(weeks_back):
        w_end = today - timedelta(days=w * 7)
        w_start = w_end - timedelta(days=6)
        events = _fetch_scoreboard_range(w_start, w_end)
        fetched.extend(_rows_from_events(events))
        time.sleep(0.25)
    new_df = pd.DataFrame(fetched)
    if new_df.empty:
        print("  [WARN] ESPN round fetch returned 0 rows")
        return load_round_cache(cache_path)
    for c in (
        "strokes",
        "birdies",
        "bogeys",
        "pars",
        "birdies_or_better",
        "bogeys_or_worse",
        "holes_played",
        "round",
        "to_par",
        "course_par",
        "vs_field",
        "attach_suspect",
    ):
        if c in new_df.columns:
            new_df[c] = pd.to_numeric(new_df[c], errors="coerce")
    new_df["player_key"] = new_df["player_name"].map(_player_key)
    new_df["tournament_date"] = new_df["tournament_date"].astype(str).str[:10]
    new_df = _fill_blank_course_ids(new_df)
    new_df = _attach_vs_field(new_df)
    old = load_round_cache(cache_path)
    if not old.empty and "tournament_date" in old.columns:
        old = old.copy()
        old["tournament_date"] = old["tournament_date"].astype(str).str[:10]
    merged = pd.concat([old, new_df], ignore_index=True)
    # Normalize date before dedupe — otherwise ISO vs date-only twins survive.
    merged["tournament_date"] = merged["tournament_date"].astype(str).str[:10]
    if "tournament_name" in merged.columns:
        merged = merged.loc[~merged["tournament_name"].map(is_match_play_event)].copy()
    merged = _prefer_clean_round_rows(merged)
    merged = repair_round_cache_frame(merged)
    merged = merged[merged["tournament_date"] >= start_floor.isoformat()].copy()
    # Recompute vs_field after the date window trim.
    merged = _attach_vs_field(merged)
    merged = merged.sort_values(
        ["tournament_date", "round"],
        ascending=[False, False],
        kind="mergesort",
    )
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(cache_path, index=False, encoding="utf-8-sig")
    n_sus = int(pd.to_numeric(merged.get("attach_suspect"), errors="coerce").fillna(0).sum())
    print(
        f"  [Golf step4] cache refreshed -> {cache_path} "
        f"({len(merged)} round rows, attach_suspect={n_sus})"
    )
    return merged


def ensure_round_cache(cache_path: Path, weeks_back: int, force_refresh: bool) -> pd.DataFrame:
    if not force_refresh and _cache_is_fresh(cache_path):
        df = load_round_cache(cache_path)
        print(f"  [Golf step4] using fresh cache ({len(df)} round rows)")
        return df
    return refresh_round_cache(cache_path, weeks_back=weeks_back)


def _round_frame_for_player(cache: pd.DataFrame, player_key: str) -> pd.DataFrame:
    if not player_key or cache.empty:
        return pd.DataFrame()
    sub = cache[cache["player_key"] == player_key].copy()
    if sub.empty:
        return sub
    return sub.sort_values(
        ["tournament_date", "round"], ascending=[False, False], kind="mergesort"
    )


def _values_from_rounds(
    rounds: pd.DataFrame,
    stat_col: str,
    *,
    counting: bool,
    prefer_vs_field_for_strokes: bool = True,
) -> list[float]:
    """Pull up to chronologically sorted round values; skip suspect counting rows."""
    if rounds.empty or stat_col not in rounds.columns and not (
        stat_col == "strokes" and prefer_vs_field_for_strokes
    ):
        if stat_col not in rounds.columns and stat_col != "strokes":
            return []
    vals: list[float] = []
    for _, rnd in rounds.iterrows():
        row = rnd.to_dict()
        if counting and not counting_prop_ok(row):
            continue
        if not counting:
            # Strokes: allow non-suspect strokes even if counting buckets fail.
            flags = str(row.get("attach_flags") or "")
            if "missing_strokes" in flags or "strokes_out_of_range" in flags:
                continue
            strokes = pd.to_numeric(row.get("strokes"), errors="coerce")
            if pd.isna(strokes):
                continue
            if prefer_vs_field_for_strokes and stat_col == "strokes":
                vf = pd.to_numeric(row.get("vs_field"), errors="coerce")
                # Prefer vs_field when present; fall back to raw strokes.
                if pd.notna(vf):
                    vals.append(float(vf))
                    continue
            vals.append(float(strokes))
            continue
        v = pd.to_numeric(row.get(stat_col), errors="coerce")
        if pd.isna(v):
            continue
        vals.append(float(v))
    return vals


def attach_stats(df: pd.DataFrame, cache: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if "player" not in out.columns:
        out["player"] = out.get("player_name", "")
    out["player_key"] = out["player"].astype(str).map(_player_key)
    for gi in range(1, 11):
        out[f"stat_g{gi}"] = np.nan
    for col in (
        "stat_last5_avg",
        "stat_last10_avg",
        "stat_season_avg",
        "golf_vs_field_last5_avg",
        "golf_to_par_last5_avg",
        "golf_attach_suspect",
        "golf_l5_unit",
        "golf_stat_source",
    ):
        if col not in out.columns:
            if col.startswith("stat_") or col.endswith("_avg"):
                out[col] = np.nan
            elif col.endswith("suspect"):
                out[col] = 0
            else:
                out[col] = ""
    if "unsupported_prop" not in out.columns:
        out["unsupported_prop"] = 0
    if "unsupported_reason" not in out.columns:
        out["unsupported_reason"] = ""

    cache_use = cache.copy()
    if "player_key" not in cache_use.columns and "player_name" in cache_use.columns:
        cache_use["player_key"] = cache_use["player_name"].map(_player_key)

    players_hit = 0
    total_rounds = 0
    suspect_props = 0
    unique_players = out["player_key"].nunique()

    for idx, row in out.iterrows():
        pk = str(row.get("player_key") or "").strip()
        stat_col = prop_stat_key(str(row.get("prop_type") or ""))
        if stat_col is None:
            out.at[idx, "unsupported_prop"] = 1
            out.at[idx, "unsupported_reason"] = "unsupported_golf_prop"
            continue
        rounds = _round_frame_for_player(cache_use, pk)
        counting = stat_col in COUNTING_STATS
        # Keep raw strokes in stat_g* so hit rates still compare to PP lines (68.5).
        # vs_field / to_par are sidecar averages for the future Golf keep-gate.
        vals = _values_from_rounds(
            rounds,
            stat_col,
            counting=counting,
            prefer_vs_field_for_strokes=False,
        )
        out.at[idx, "golf_l5_unit"] = "raw_round"
        if counting and not vals:
            # All recent rounds failed integrity — hold the prop, don't fake L5.
            out.at[idx, "golf_attach_suspect"] = 1
            out.at[idx, "unsupported_prop"] = 1
            out.at[idx, "unsupported_reason"] = "golf_attach_suspect"
            suspect_props += 1
            continue
        if vals:
            players_hit += 1
            total_rounds += len(vals)
            src = ""
            if not rounds.empty and "stat_source" in rounds.columns:
                for _, rnd in rounds.iterrows():
                    s = str(rnd.get("stat_source") or "").strip()
                    if s:
                        src = s
                        break
            out.at[idx, "golf_stat_source"] = src
        for gi, v in enumerate(vals[:10], start=1):
            out.at[idx, f"stat_g{gi}"] = v
        if vals:
            out.at[idx, "stat_last5_avg"] = float(np.mean(vals[:5]))
            out.at[idx, "stat_last10_avg"] = float(np.mean(vals[:10]))
            out.at[idx, "stat_season_avg"] = float(np.mean(vals))
        if stat_col == "strokes" and not rounds.empty:
            vf = _values_from_rounds(
                rounds, "strokes", counting=False, prefer_vs_field_for_strokes=True
            )
            tp_vals: list[float] = []
            for _, rnd in rounds.iterrows():
                tp = pd.to_numeric(rnd.get("to_par"), errors="coerce")
                if pd.notna(tp):
                    tp_vals.append(float(tp))
            if vf:
                out.at[idx, "golf_vs_field_last5_avg"] = float(np.mean(vf[:5]))
            if tp_vals:
                out.at[idx, "golf_to_par_last5_avg"] = float(np.mean(tp_vals[:5]))

    cache_players = cache_use["player_key"].nunique() if not cache_use.empty else 0
    print(
        f"[Golf step4] cache hit: {players_hit}/{len(out)} rows "
        f"({unique_players} slate players, {cache_players} cached players, "
        f"{total_rounds} rounds loaded, attach_suspect_props={suspect_props})"
    )
    return out


def main() -> None:
    print("[Golf step4] Starting...")
    ap = argparse.ArgumentParser(description="Golf step4 — ESPN round cache + stat_g* attachment.")
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--cache", default="Sports/Golf/cache/golf_round_cache.csv")
    ap.add_argument("--weeks-back", type=int, default=12)
    ap.add_argument("--refresh-cache", action="store_true", help="Force ESPN re-fetch even if cache is fresh.")
    args = ap.parse_args()

    inp = Path(args.input)
    if not inp.is_absolute():
        inp = _REPO / inp
    out = Path(args.output)
    if not out.is_absolute():
        out = _REPO / out
    cache_path = Path(args.cache)
    if not cache_path.is_absolute():
        cache_path = _REPO / cache_path

    if not inp.is_file():
        print(f"ERROR [Golf step4] missing input: {inp}")
        sys.exit(1)

    df = pd.read_csv(inp, dtype=str, low_memory=False).fillna("")
    if df.empty:
        print("ERROR [Golf step4] empty input")
        sys.exit(1)

    cache = ensure_round_cache(cache_path, weeks_back=max(1, int(args.weeks_back)), force_refresh=args.refresh_cache)
    enriched = attach_stats(df, cache)

    out.parent.mkdir(parents=True, exist_ok=True)
    enriched.to_csv(out, index=False, encoding="utf-8-sig")
    g1 = int(pd.to_numeric(enriched.get("stat_g1"), errors="coerce").notna().sum())
    l5 = int(pd.to_numeric(enriched.get("stat_last5_avg"), errors="coerce").notna().sum())
    print(f"[Golf step4] Wrote {out} ({len(enriched)} rows) | stat_g1={g1}/{len(enriched)} | stat_last5_avg={l5}/{len(enriched)}")


if __name__ == "__main__":
    main()
