#!/usr/bin/env python3
"""Volume-floor backtests A (ledger) and B (nflverse). Observe only.

  py -3.14 -m ml.volume_floor_backtest
  py -3.14 -m ml.volume_floor_backtest --part A
  py -3.14 -m ml.volume_floor_backtest --part B

Does not import or edit the three frozen forward tracks. A suggested
better cut is a new dated track, not a rewrite of those files.
"""
from __future__ import annotations

import argparse
import gzip
import io
import json
import re
import sys
import urllib.request
from pathlib import Path
from typing import Any

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from ml.volume_floor import (  # noqa: E402
    BOOKS_A,
    BREAK_EVEN,
    FROZEN_TRACKS,
    MIN_N,
    MIN_TGT_SHARE_PP,
    RANK_MAX,
    VOLUME_CANONS,
    assert_prereg,
    bh_keep,
    binom_p_greater,
    cell_stats,
    floor_over,
    is_volume_prop,
    oos_split_dates,
    passes_gate,
    persistence_hit,
    team_share_ranks,
    top2,
)
from utils.face_ladder import face_status_by_sport_date, unverified_sport_dates  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402

import prop_hit_tiers as T  # noqa: E402

MAIN = _REPO.parent / "PropORACLE_main_cp"
OUT_DIR = _REPO / "data" / "reports"
CACHE = _REPO / "Sports" / "NFL" / "data" / "cache"
NFLVERSE = "https://github.com/nflverse/nflverse-data/releases/download"
UA = {"User-Agent": "PropOracle/1.0 (+https://github.com/halekevi/PropORACLE)"}
HEADLINE_SEASONS = (2021, 2022, 2023, 2024, 2025)
EXTRA_SEASONS = (2026,)


def _norm_player(s: object) -> str:
    x = re.sub(r"[^a-z0-9 ]+", " ", str(s or "").lower())
    return re.sub(r"\s+", " ", x).strip()


def _side(raw: object) -> str | None:
    s = str(raw or "").strip().upper()
    if s in ("O", "OVER"):
        return "OVER"
    if s in ("U", "UNDER"):
        return "UNDER"
    return None


def graded_by_date() -> dict[str, Path]:
    by: dict[str, Path] = {}
    for d in (
        MAIN / "ui_runner" / "templates",
        _REPO / "ui_runner" / "templates",
    ):
        if not d.is_dir():
            continue
        for p in sorted(d.glob("graded_props_20*.json")):
            if ".bak_" in p.name:
                continue
            date = p.stem.replace("graded_props_", "")[:10]
            if date not in by:
                by[date] = p
    return by


def _load_json(path: Path) -> dict | None:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


def load_ledger_rows() -> tuple[list[dict[str, Any]], dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    skipped = {"not_hit_miss": 0, "no_prop": 0, "not_over": 0, "not_book": 0}
    for date, path in graded_by_date().items():
        raw = _load_json(path)
        if raw is None:
            continue
        for r in raw.get("props") or []:
            if not isinstance(r, dict):
                continue
            res = str(r.get("result") or "").strip().upper()
            if res not in ("HIT", "MISS"):
                skipped["not_hit_miss"] += 1
                continue
            if str(r.get("minutes_status") or "") == "minutes_unknown":
                continue
            sport = T.norm_sport(str(r.get("sport") or ""))
            if sport not in VOLUME_CANONS:
                continue
            prop = canon_prop(sport, r.get("prop") or r.get("prop_type"))
            if not prop:
                skipped["no_prop"] += 1
                continue
            side = _side(r.get("direction") or r.get("over_under"))
            if side != "OVER":
                skipped["not_over"] += 1
                continue
            book = T.book_label(r.get("pick_type"), side)
            if book not in BOOKS_A:
                skipped["not_book"] += 1
                continue
            rows.append(
                {
                    "date": date,
                    "sport": sport,
                    "player": r.get("player"),
                    "player_norm": _norm_player(r.get("player")),
                    "team": r.get("team") or "",
                    "prop": prop,
                    "book": book,
                    "line": r.get("line"),
                    "hit": 1 if res == "HIT" else 0,
                    "l5_over": r.get("l5_over") or r.get("last5_over"),
                    "l10_over": r.get("l10_over"),
                    "def_tier": r.get("def_tier") or r.get("def") or "",
                    "usage_pct": r.get("usage_pct"),
                    "team_top3_rank": r.get("team_top3_rank"),
                    "target_share_L3": r.get("target_share_L3") or r.get("tgt_share"),
                    "pick_type": r.get("pick_type"),
                    "volume": is_volume_prop(sport, prop),
                }
            )
    face_src = pd.DataFrame(rows)
    if not face_src.empty and "slate_date" not in face_src.columns:
        face_src["slate_date"] = face_src["date"]
    face_df = face_status_by_sport_date(face_src) if not face_src.empty else pd.DataFrame()
    bad = unverified_sport_dates(face_df)
    kept = [r for r in rows if (r["sport"], r["date"]) not in bad]
    meta = {
        "raw_n": len(rows),
        "face_verified_n": len(kept),
        "dropped_unverified": len(rows) - len(kept),
        "skipped": skipped,
        "unverified_sport_dates": sorted(f"{s}|{d}" for s, d in bad),
        "face_status_rows": int(len(face_df)),
    }
    return kept, meta


def _summarize(pool: list[dict[str, Any]], book: str) -> dict[str, Any]:
    hits = sum(int(r["hit"]) for r in pool)
    n = len(pool)
    return cell_stats(hits, n, breakeven=BREAK_EVEN.get(book))


def _overlap(a: list[dict[str, Any]], b: list[dict[str, Any]]) -> dict[str, Any]:
    keys_b = {
        (r["date"], r["player_norm"], r["prop"], r["book"], r.get("line"))
        for r in b
    }
    both = [
        r
        for r in a
        if (r["date"], r["player_norm"], r["prop"], r["book"], r.get("line")) in keys_b
    ]
    return {
        "n": len(both),
        "share_of_floor": round(len(both) / len(a), 4) if a else None,
        "share_of_gate": round(len(both) / len(b), 4) if b else None,
    }


def run_a() -> dict[str, Any]:
    rows, meta = load_ledger_rows()
    ranks = team_share_ranks(rows)
    for r in rows:
        r["top2"] = top2(r, ranks)
        r["floor"] = floor_over(r)
        r["gate"] = passes_gate(r)
    dates = sorted({r["date"] for r in rows})
    train, test = oos_split_dates(dates)
    cells: list[dict[str, Any]] = []
    pvals: list[float | None] = []
    sports = sorted({r["sport"] for r in rows})
    for sport in sports:
        for book in BOOKS_A:
            pool = [r for r in rows if r["sport"] == sport and r["book"] == book]
            if not pool:
                continue
            vol_gate = [r for r in pool if r["volume"] and r["gate"]]
            non_gate = [r for r in pool if (not r["volume"]) and r["gate"]]
            floor_role = [r for r in pool if r["volume"] and r["floor"] and r["top2"]]
            floor_only = [r for r in pool if r["volume"] and r["floor"]]
            gate_all = [r for r in pool if r["gate"]]
            named = [
                ("volume_in_gate", vol_gate),
                ("nonvolume_in_gate", non_gate),
                ("volume_floor", floor_only),
                ("volume_floor_top2", floor_role),
                ("gate", gate_all),
            ]
            oos_named = []
            for name, subset in named:
                oos = [r for r in subset if r["date"] in test]
                rec = {
                    "sport": sport,
                    "book": book,
                    "slice": name,
                    "all": _summarize(subset, book),
                    "oos": _summarize(oos, book),
                }
                cells.append(rec)
                be = BREAK_EVEN.get(book)
                p = None
                if be is not None and len(oos) >= MIN_N:
                    p = binom_p_greater(sum(int(r["hit"]) for r in oos), len(oos), be)
                pvals.append(p)
                oos_named.append((name, oos))
            cells.append(
                {
                    "sport": sport,
                    "book": book,
                    "slice": "overlap_floor_top2_vs_gate",
                    "overlap": _overlap(floor_role, gate_all),
                    "all": None,
                    "oos": None,
                }
            )
            pvals.append(None)
            vg = _summarize(vol_gate, book)
            ng = _summarize(non_gate, book)
            cells.append(
                {
                    "sport": sport,
                    "book": book,
                    "slice": "volume_minus_nonvolume_in_gate",
                    "delta_hr": (
                        round(vg["hr"] - ng["hr"], 4)
                        if vg["hr"] is not None and ng["hr"] is not None
                        else None
                    ),
                    "volume_in_gate": vg,
                    "nonvolume_in_gate": ng,
                }
            )
            pvals.append(None)
    keep = bh_keep(pvals)
    for i, rec in enumerate(cells):
        if rec.get("oos") and rec["oos"].get("n", 0) >= MIN_N:
            rec["bh_keep"] = i in keep
            rec["oos_p"] = pvals[i]
        else:
            rec["bh_keep"] = False
            rec["oos_p"] = pvals[i] if i < len(pvals) else None
    return {
        "part": "A",
        "meta": {
            **meta,
            "dates": dates,
            "train_dates": sorted(train),
            "oos_dates": sorted(test),
            "n_rows": len(rows),
            "frozen_tracks_untouched": list(FROZEN_TRACKS),
            "honest_ev": False,
        },
        "cells": cells,
    }


def _get_bytes(url: str, timeout: float = 180.0) -> bytes:
    req = urllib.request.Request(url, headers=UA)
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read()


def load_stats_week(season: int, *, cache_dir: Path | None = None) -> pd.DataFrame:
    dest_dir = cache_dir or CACHE
    dest_dir.mkdir(parents=True, exist_ok=True)
    cache = dest_dir / f"nflverse_stats_player_week_{season}.csv"
    if cache.is_file() and cache.stat().st_size > 1000:
        df = pd.read_csv(cache)
    else:
        url = f"{NFLVERSE}/stats_player/stats_player_week_{season}.csv.gz"
        raw = gzip.decompress(_get_bytes(url))
        df = pd.read_csv(io.BytesIO(raw))
        df.to_csv(cache, index=False)
    if "season_type" in df.columns:
        reg = df[df["season_type"].astype(str).str.upper().isin(("REG", "REGULAR"))]
        if not reg.empty:
            df = reg
    df["season"] = season
    df["week"] = pd.to_numeric(df.get("week"), errors="coerce")
    df["team"] = df.get("team", pd.Series("", index=df.index)).astype(str).str.upper()
    df["position"] = df.get("position", pd.Series("", index=df.index)).astype(str).str.upper()
    if "carries" in df.columns:
        df["rush_attempts"] = pd.to_numeric(df["carries"], errors="coerce")
    elif "rushing_attempts" in df.columns:
        df["rush_attempts"] = pd.to_numeric(df["rushing_attempts"], errors="coerce")
    else:
        df["rush_attempts"] = pd.NA
    df["receptions"] = pd.to_numeric(df.get("receptions"), errors="coerce")
    df["targets"] = pd.to_numeric(df.get("targets"), errors="coerce")
    share = pd.to_numeric(df.get("target_share"), errors="coerce")
    df["target_share_pp"] = share.where(share.isna() | (share > 1.5), share * 100.0)
    pid = df.get("player_id")
    if pid is None:
        df["player_id"] = df.get("player_display_name", df.get("player_name", "")).astype(str)
    return df


def _asof_l5_min(series: pd.Series, group: pd.Series) -> pd.Series:
    prior = series.groupby(group).shift(1)
    return prior.groupby(group).transform(lambda s: s.rolling(5, min_periods=5).min())


def _asof_l3_mean(series: pd.Series, group: pd.Series) -> pd.Series:
    prior = series.groupby(group).shift(1)
    return prior.groupby(group).transform(lambda s: s.rolling(3, min_periods=1).mean())


def persistence_table(df: pd.DataFrame) -> pd.DataFrame:
    df = df.sort_values(["player_id", "season", "week"]).copy()
    gid = df["player_id"].astype(str) + "|" + df["season"].astype(str)
    df["l5_min_rush"] = _asof_l5_min(df["rush_attempts"], gid)
    df["l5_min_rec"] = _asof_l5_min(df["receptions"], gid)
    team_carries = df.groupby(["season", "week", "team"])["rush_attempts"].transform("sum")
    df["carry_share_pp"] = (df["rush_attempts"] / team_carries.replace(0, pd.NA)) * 100.0
    df["l3_carry_share"] = _asof_l3_mean(df["carry_share_pp"], gid)
    if "target_share_pp" not in df.columns:
        share = pd.to_numeric(df.get("target_share"), errors="coerce")
        df["target_share_pp"] = share.where(share.isna() | (share > 1.5), share * 100.0)
    df["l3_tgt_share"] = _asof_l3_mean(df["target_share_pp"], gid)
    df["rush_rank"] = df.groupby(["season", "week", "team"])["l3_carry_share"].rank(
        method="min", ascending=False
    )
    df["rec_rank"] = df.groupby(["season", "week", "team"])["l3_tgt_share"].rank(
        method="min", ascending=False
    )
    return df


def _persist_cell(mask: pd.Series, actual: pd.Series, floor: pd.Series) -> dict[str, Any]:
    hits = 0
    n = 0
    for a, f, keep in zip(actual.tolist(), floor.tolist(), mask.tolist()):
        if not keep:
            continue
        hit = persistence_hit(
            None if pd.isna(a) else float(a),
            None if pd.isna(f) else float(f),
        )
        if hit is None:
            continue
        n += 1
        hits += int(hit)
    return cell_stats(hits, n, breakeven=None)


def run_b(*, seasons: tuple[int, ...] | None = None, cache_dir: Path | None = None) -> dict[str, Any]:
    use = list(seasons or (HEADLINE_SEASONS + EXTRA_SEASONS))
    frames = []
    errors = []
    for season in use:
        try:
            frames.append(load_stats_week(season, cache_dir=cache_dir))
        except Exception as exc:
            errors.append({"season": season, "error": str(exc)})
    if not frames:
        return {"part": "B", "error": "no nflverse weeks loaded", "errors": errors}
    df = persistence_table(pd.concat(frames, ignore_index=True))
    headline = df[df["season"].isin(HEADLINE_SEASONS)]
    extra = df[df["season"].isin(EXTRA_SEASONS)]
    cells = []
    for label, frame in (("headline_2021_2025", headline), ("extra_2026", extra)):
        if frame.empty:
            continue
        rush_pos = frame["position"].isin(("RB", "FB"))
        rec_pos = frame["position"].isin(("WR", "TE", "RB"))
        specs = [
            ("rush_attempts", "leader", rush_pos & (frame["rush_rank"] <= 1), "rush_attempts", "l5_min_rush"),
            ("rush_attempts", "top2", rush_pos & (frame["rush_rank"] <= RANK_MAX), "rush_attempts", "l5_min_rush"),
            ("receptions", "leader", rec_pos & (frame["rec_rank"] <= 1), "receptions", "l5_min_rec"),
            ("receptions", "top2", rec_pos & (frame["rec_rank"] <= RANK_MAX), "receptions", "l5_min_rec"),
            (
                "receptions",
                "top2_share_ge_15",
                rec_pos
                & (frame["rec_rank"] <= RANK_MAX)
                & (frame["l3_tgt_share"] >= MIN_TGT_SHARE_PP),
                "receptions",
                "l5_min_rec",
            ),
        ]
        by_pos = [
            ("receptions", "WR_top2", rec_pos & frame["position"].eq("WR") & (frame["rec_rank"] <= RANK_MAX), "receptions", "l5_min_rec"),
            ("receptions", "TE_top2", rec_pos & frame["position"].eq("TE") & (frame["rec_rank"] <= RANK_MAX), "receptions", "l5_min_rec"),
            ("receptions", "RB_top2", rec_pos & frame["position"].eq("RB") & (frame["rec_rank"] <= RANK_MAX), "receptions", "l5_min_rec"),
            ("rush_attempts", "RB_leader", rush_pos & frame["position"].eq("RB") & (frame["rush_rank"] <= 1), "rush_attempts", "l5_min_rush"),
        ]
        for family, role, mask, actual_c, floor_c in specs + by_pos:
            stats = _persist_cell(mask, frame[actual_c], frame[floor_c])
            cells.append({"window": label, "family": family, "role": role, **stats})
    return {
        "part": "B",
        "meta": {
            "seasons_loaded": sorted({int(s) for s in df["season"].dropna().unique()}),
            "player_games": int(len(df)),
            "errors": errors,
            "honest_ev": False,
            "price": None,
            "frozen_tracks_untouched": list(FROZEN_TRACKS),
        },
        "cells": cells,
    }


def _md_a(payload: dict[str, Any]) -> str:
    lines = [
        "# Volume-floor backtest A (ledger)",
        "",
        "Pre-registered 2026-10-25. Face-verified OVER rungs only. "
        "No honest Demon EV — quotes were not scraped.",
        "",
        f"Rows: {payload['meta']['n_rows']}. "
        f"Dropped unverified: {payload['meta']['dropped_unverified']}.",
        "",
        "Cells below need n ≥ 50. Thinner slices stay in the JSON.",
        "",
        "| Sport | Book | Slice | HR | n | Wilson lo | vs BE | BH |",
        "|---|---|---|---:|---:|---:|---:|---|",
    ]
    for c in payload.get("cells") or []:
        s = c.get("all")
        if not s:
            if c.get("slice") == "volume_minus_nonvolume_in_gate":
                vg = c.get("volume_in_gate") or {}
                ng = c.get("nonvolume_in_gate") or {}
                if (vg.get("n") or 0) < MIN_N or (ng.get("n") or 0) < MIN_N:
                    continue
                d = c.get("delta_hr")
                lines.append(
                    f"| {c['sport']} | {c['book']} | volume − nonvolume in gate | "
                    f"{'' if d is None else d} |  |  |  |  |"
                )
            elif c.get("overlap") and (c["overlap"].get("n") or 0) >= MIN_N:
                o = c["overlap"]
                lines.append(
                    f"| {c['sport']} | {c['book']} | overlap floor+top2 ∩ gate | "
                    f"{o.get('share_of_floor')} | {o.get('n')} |  |  |  |"
                )
            continue
        if (s.get("n") or 0) < MIN_N:
            continue
        be = s.get("hr_minus_breakeven")
        lines.append(
            f"| {c['sport']} | {c['book']} | {c['slice']} | "
            f"{s.get('hr')} | {s.get('n')} | {s.get('wilson_low')} | "
            f"{be} | {c.get('bh_keep')} |"
        )
    lines += [
        "",
        "## How to read",
        "",
        "- Volume beats non-volume inside the same gate → volume is a filter, not a promotion.",
        "- Floor+top2 is the Demon-track analog on sports that have lines.",
        "- BH keep is OOS vs the approximate rung breakeven. Demon breakeven is unset.",
        "- Do not edit the three frozen forward tracks from this table.",
        "",
    ]
    return "\n".join(lines)


def _md_b(payload: dict[str, Any]) -> str:
    lines = [
        "# Volume-floor backtest B (nflverse, no lines)",
        "",
        "How often a top-2 volume player's next game meets or beats his L5 minimum. "
        "No price attached.",
        "",
        "| Window | Family | Role | HR | n | Wilson lo | thin |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    for c in payload.get("cells") or []:
        lines.append(
            f"| {c.get('window')} | {c.get('family')} | {c.get('role')} | "
            f"{c.get('hr')} | {c.get('n')} | {c.get('wilson_low')} | {c.get('thin')} |"
        )
    lines += [
        "",
        "## How to read",
        "",
        "- 80%+ for target-leader / top-2 receptions → Demon tracks have a strong prior.",
        "- Near 60% → Skattebo and Tucker were mostly a good day.",
        "- This is a probability, not EV. Only the forward Demon logs can promote.",
        "",
    ]
    return "\n".join(lines)


def write_reports(part: str, payload: dict[str, Any]) -> tuple[Path, Path]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    stem = f"volume_floor_backtest_{part.lower()}"
    js = OUT_DIR / f"{stem}.json"
    md = OUT_DIR / f"{stem}.md"
    js.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    text = _md_a(payload) if part == "A" else _md_b(payload)
    md.write_text(text, encoding="utf-8")
    return js, md


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Volume-floor backtests A and B")
    ap.add_argument("--part", choices=("A", "B", "both"), default="both")
    args = ap.parse_args(argv)
    spec = assert_prereg()
    print(f"prereg {spec['prereg_date']} locked; frozen tracks {list(FROZEN_TRACKS)}")
    if args.part in {"A", "both"}:
        print("running A (ledger)…")
        payload = run_a()
        js, md = write_reports("A", payload)
        print(f"  wrote {js}")
        print(f"  wrote {md}")
    if args.part in {"B", "both"}:
        print("running B (nflverse)…")
        payload = run_b()
        js, md = write_reports("B", payload)
        print(f"  wrote {js}")
        print(f"  wrote {md}")
    print("done. no frozen track was opened.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
