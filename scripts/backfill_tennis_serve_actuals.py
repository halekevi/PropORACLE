#!/usr/bin/env python3
"""Re-grade tennis Aces / Double Faults from Sackmann.

ESPN scoreboard often has aces/double_faults as None. Graders used to coerce
that to 0.0, so every OVER vs a 2.5+ line was a MISS (0% HIT on thousands of
rows). This rewrites those actuals from Jeff Sackmann's ATP/WTA match files.

  py -3.14 scripts/backfill_tennis_serve_actuals.py
  py -3.14 scripts/backfill_tennis_serve_actuals.py --dry-run
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import date
from pathlib import Path

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
_TENNIS = _REPO / "Sports" / "Tennis" / "scripts"
if str(_TENNIS) not in sys.path:
    sys.path.insert(0, str(_TENNIS))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from tennis_shared import (  # noqa: E402
    build_sackmann_player_index,
    ensure_sackmann_matches,
    lookup_serve_actual,
    norm_key,
    norm_tennis_prop,
)

_SERVE = frozenset({"aces", "double_faults"})


def _finite(v: object) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    if f != f:
        return None
    return f


def _is_serve(prop: object) -> bool:
    return norm_tennis_prop(str(prop or "")) in _SERVE


def _grade(direction: str, line: float, actual: float | None) -> tuple[str, int | None]:
    if actual is None:
        return "VOID", None
    d = str(direction or "").strip().upper()
    if d == "OVER":
        hit = actual >= line
    elif d == "UNDER":
        hit = actual < line
    else:
        return "VOID", None
    return ("HIT" if hit else "MISS"), (1 if hit else 0)


def _file_date(path: Path, data: dict) -> date | None:
    raw = str(data.get("date") or "")[:10]
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw):
        return date.fromisoformat(raw)
    m = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
    if m:
        return date.fromisoformat(m.group(1))
    return None


def _games_won_hints(props: list[dict]) -> dict[str, float]:
    out: dict[str, float] = {}
    for row in props:
        if not isinstance(row, dict):
            continue
        if str(row.get("sport") or "").strip().lower() != "tennis":
            continue
        if norm_tennis_prop(str(row.get("prop") or row.get("prop_type") or "")) != "games_won":
            continue
        actual = _finite(row.get("actual_value") if row.get("actual_value") not in (None, "") else row.get("actual"))
        if actual is None or actual <= 0:
            continue
        pk = norm_key(str(row.get("player") or ""))
        if pk:
            out[pk] = actual
    return out


def _apply_row(
    row: dict,
    *,
    slate: date,
    sack_index: dict,
    ta_index: dict,
    hints: dict[str, float],
) -> str | None:
    """Patch one prop in place. Returns patched|voided|None."""
    sport = str(row.get("sport") or "").strip().lower()
    if sport and sport != "tennis":
        return None
    prop = row.get("prop") or row.get("prop_type") or ""
    if not _is_serve(prop):
        return None
    player = str(row.get("player") or "").strip()
    if not player:
        return None
    line = _finite(row.get("line"))
    if line is None:
        return None
    pnorm = norm_tennis_prop(str(prop))
    pk = norm_key(player)
    rec, src = lookup_serve_actual(
        player,
        slate,
        games_won_hint=hints.get(pk),
        ta_index=ta_index,
        sack_index=sack_index,
    )
    actual = rec.get(pnorm) if rec else None
    direction = str(row.get("direction") or row.get("over_under") or "")
    result, hit = _grade(direction, line, actual)
    if actual is None:
        row["actual_value"] = ""
        row["result"] = "VOID"
        row["hit"] = ""
        row["void_reason"] = "NO_DATA"
        row["margin"] = ""
        row["actual_source"] = ""
        return "voided"
    row["actual_value"] = str(actual)
    row["result"] = result
    row["hit"] = hit
    row["void_reason"] = ""
    row["margin"] = str(round(actual - line, 2))
    row["actual_source"] = src
    return "patched"


def patch_graded_file(path: Path, sack_index: dict, ta_index: dict, *, dry_run: bool) -> dict[str, int]:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        return {"patched": 0, "voided": 0, "files": 0}
    slate = _file_date(path, data)
    if slate is None:
        return {"patched": 0, "voided": 0, "files": 0}
    stats = {"patched": 0, "voided": 0, "files": 1}
    for key in ("props", "prop_breakdown_rows"):
        rows = data.get(key)
        if not isinstance(rows, list) or not rows:
            continue
        hints = _games_won_hints(rows) if key == "props" else _games_won_hints(data.get("props") or [])
        changed = False
        for row in rows:
            if not isinstance(row, dict):
                continue
            action = _apply_row(
                row, slate=slate, sack_index=sack_index, ta_index=ta_index, hints=hints
            )
            if action:
                stats[action] = stats.get(action, 0) + 1
                changed = True
        if changed and not dry_run:
            path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return stats


def patch_retrain(path: Path, sack_index: dict, ta_index: dict, *, dry_run: bool) -> dict[str, int]:
    if not path.is_file():
        return {"patched": 0, "voided": 0, "rows": 0}
    df = pd.read_csv(path, low_memory=False, encoding="utf-8-sig")
    sport = df["sport"].astype(str).str.strip().str.upper() if "sport" in df.columns else pd.Series("", index=df.index)
    prop = df["prop"] if "prop" in df.columns else df.get("prop_type")
    if prop is None:
        return {"patched": 0, "voided": 0, "rows": 0}
    serve = sport.eq("TENNIS") & prop.map(lambda p: _is_serve(p))
    idx = df.index[serve]
    stats = {"patched": 0, "voided": 0, "rows": int(len(idx))}
    if not len(idx):
        return stats
    if "actual_value" in df.columns:
        df["actual_value"] = pd.to_numeric(df["actual_value"], errors="coerce")
    date_col = "file_date" if "file_date" in df.columns else "game_date"
    hints: dict[tuple[str, str], float] = {}
    tennis = df.loc[sport.eq("TENNIS")]
    if "player" in tennis.columns and prop is not None:
        for i, r in tennis.iterrows():
            if norm_tennis_prop(str(r.get("prop") or r.get("prop_type") or "")) != "games_won":
                continue
            actual = _finite(r.get("actual_value") if "actual_value" in r else r.get("actual"))
            if actual is None or actual <= 0:
                continue
            ds = str(r.get(date_col) or "")[:10]
            pk = norm_key(str(r.get("player") or ""))
            if ds and pk:
                hints[(ds, pk)] = actual
    for i in idx:
        r = df.loc[i]
        ds = str(r.get(date_col) or "")[:10]
        try:
            slate = date.fromisoformat(ds)
        except ValueError:
            continue
        player = str(r.get("player") or "")
        pk = norm_key(player)
        line = _finite(r.get("line"))
        if line is None:
            continue
        pnorm = norm_tennis_prop(str(r.get("prop") or r.get("prop_type") or ""))
        rec, _src = lookup_serve_actual(
            player,
            slate,
            games_won_hint=hints.get((ds, pk)),
            ta_index=ta_index,
            sack_index=sack_index,
        )
        actual = rec.get(pnorm) if rec else None
        result, hit = _grade(str(r.get("direction") or ""), line, actual)
        if actual is None:
            df.at[i, "actual_value"] = float("nan")
            if "result" in df.columns:
                df.at[i, "result"] = "VOID"
            if "result_binary" in df.columns:
                df.at[i, "result_binary"] = float("nan")
            stats["voided"] += 1
            continue
        df.at[i, "actual_value"] = actual
        if "result" in df.columns:
            df.at[i, "result"] = result
        if "result_binary" in df.columns:
            df.at[i, "result_binary"] = float(hit) if hit is not None else float("nan")
        if "hit" in df.columns:
            df.at[i, "hit"] = hit
        stats["patched"] += 1
    if not dry_run and (stats["patched"] or stats["voided"]):
        df.to_csv(path, index=False, encoding="utf-8-sig")
    return stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--skip-retrain", action="store_true")
    args = ap.parse_args()
    print("[serve-backfill] loading Sackmann + Tennis Abstract cache…", flush=True)
    sack_index = build_sackmann_player_index(ensure_sackmann_matches())
    from tennis_ta_matchlog import load_cached_ta_player_index

    ta_index = load_cached_ta_player_index()
    print(
        f"[serve-backfill] sackmann players={len(sack_index):,} ta players={len(ta_index):,}",
        flush=True,
    )

    roots = [
        _REPO / "ui_runner" / "templates",
        _REPO / "ui_runner" / "templates" / "archive",
        _REPO / "mobile" / "www",
    ]
    # Contaminated blank-source Aces OVER rows live on the main_cp tree
    # (preferred by board ledgers). Patch both checkouts when present.
    main_cp = _REPO.parent / "PropORACLE_main_cp"
    if main_cp.is_dir():
        roots.extend(
            [
                main_cp / "ui_runner" / "templates",
                main_cp / "ui_runner" / "templates" / "archive",
                main_cp / "mobile" / "www",
            ]
        )
    files: list[Path] = []
    for root in roots:
        if root.is_dir():
            files.extend(sorted(root.glob("graded_props_*.json")))
    seen: set[Path] = set()
    uniq: list[Path] = []
    for p in files:
        rp = p.resolve()
        if rp in seen:
            continue
        seen.add(rp)
        uniq.append(p)

    tot = {"patched": 0, "voided": 0, "files": 0}
    for path in uniq:
        st = patch_graded_file(path, sack_index, ta_index, dry_run=args.dry_run)
        for k, v in st.items():
            tot[k] = tot.get(k, 0) + v
        if st.get("patched") or st.get("voided"):
            print(f"  {path.name} patched={st.get('patched', 0)} voided={st.get('voided', 0)}", flush=True)
    print(
        f"[serve-backfill] json files={tot.get('files', 0)} patched={tot.get('patched', 0)} "
        f"voided={tot.get('voided', 0)} dry_run={args.dry_run}",
        flush=True,
    )

    if not args.skip_retrain:
        retr = _REPO / "data" / "retrain_dataset.csv"
        rst = patch_retrain(retr, sack_index, ta_index, dry_run=args.dry_run)
        print(
            f"[serve-backfill] retrain rows={rst.get('rows', 0)} patched={rst.get('patched', 0)} "
            f"voided={rst.get('voided', 0)}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
