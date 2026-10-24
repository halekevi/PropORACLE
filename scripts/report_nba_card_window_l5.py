"""Print NBA L5 card window vs preseason; filter 5/5 first, then pre flags.

Card window = 5 newest RS + playoffs (no preseason). Preseason is flag-only.
"""
from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from utils.cushion_z import l5_clears
from utils.nba_early_l5 import (
    card_l5_ok,
    format_side_by_side,
    l5_card_window,
    l5_preseason,
    nba_rs_opener,
    preseason_flag,
    window_mix,
)
from utils.nba_minutes_role import load_nba_box_frame


def _norm_side(s: str) -> str:
    u = str(s or "").strip().upper()
    if u in {"O", "OVER", "MORE"}:
        return "OVER"
    if u in {"U", "UNDER", "LESS"}:
        return "UNDER"
    return u


def _pick_type(row: pd.Series) -> str:
    for c in ("pick_type", "Pick Type", "projection_type", "odds_type"):
        if c in row.index and str(row.get(c) or "").strip():
            return str(row.get(c)).strip().lower()
    return ""


def _load_step8(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in {".xlsx", ".xls"}:
        xl = pd.ExcelFile(path)
        sheet = "ALL" if "ALL" in xl.sheet_names else xl.sheet_names[-1]
        raw = pd.read_excel(xl, sheet_name=sheet, header=None)
        header_i = 0
        for i in range(min(8, len(raw))):
            cells = [str(x).strip().lower() for x in raw.iloc[i].tolist()[:20]]
            if "player" in cells or "prop" in cells or "prop_type" in cells:
                header_i = i
                break
        df = pd.read_excel(xl, sheet_name=sheet, header=header_i)
        return df.dropna(how="all")
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _card_line(dates: list, vals: list) -> str:
    parts = [f"{d}:{v:g}" for d, v in zip(dates, vals)]
    return " ".join(parts)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default="2026-10-22")
    ap.add_argument("--step8", default="")
    ap.add_argument("--limit", type=int, default=0)
    ap.add_argument("--spot-check", type=int, default=10)
    ap.add_argument("--out", default="")
    ap.add_argument("--goblin-only", action="store_true", default=True)
    ap.add_argument("--include-standard", action="store_true")
    args = ap.parse_args()

    slate = str(args.date)[:10]
    step8 = Path(args.step8) if args.step8 else (
        ROOT / "outputs" / slate / f"step8_nba_direction_clean_{slate}.xlsx"
    )
    if not step8.exists():
        alt = ROOT / "Sports" / "NBA" / "data" / "outputs" / "step8_all_direction_clean.xlsx"
        step8 = alt if alt.exists() else step8
    if not step8.exists():
        print(f"missing step8: {step8}", file=sys.stderr)
        return 1

    df = _load_step8(step8)
    boxes = load_nba_box_frame()
    if boxes.empty:
        print("empty nba box frame", file=sys.stderr)
        return 1

    opener = nba_rs_opener(slate)
    opener_s = str(opener) if opener else None

    player_c = next((c for c in ("player", "Player", "name") if c in df.columns), None)
    prop_c = next(
        (
            c
            for c in ("prop", "Prop", "prop_type", "prop_norm", "stat_type", "market")
            if c in df.columns
        ),
        None,
    )
    line_c = next((c for c in ("line", "Line", "projection_line") if c in df.columns), None)
    side_c = next(
        (c for c in ("side", "Side", "direction", "recommended_side") if c in df.columns),
        None,
    )
    if not all([player_c, prop_c, line_c]):
        print(f"step8 missing columns: {list(df.columns)[:20]}", file=sys.stderr)
        return 1

    rows_out: list[dict] = []
    lines: list[str] = []
    lines.append(f"NBA card-window L5 report  slate={slate}  step8={step8.name}")
    lines.append("GATE ORDER (strict):")
    lines.append("  1) card window = 5 newest RS+playoffs (no preseason)")
    lines.append("  2) require directional L5=5 on that card window")
    lines.append(
        "  3) pre_ bridge only while current-season RS < 5; "
        "else pre_retired (hidden from rank lists)"
    )
    lines.append(
        "  4) while bridge active: split pre_confirms / pre_thin / pre_conflicts"
    )
    lines.append(
        f"RS opener={opener_s}  "
        "(stale = newest card game before Apr 1 of opener year — "
        "missing late-season/playoff tape; prior-year Apr–Jun is NOT stale)"
    )
    lines.append("")

    goblin_only = bool(args.goblin_only) and not bool(args.include_standard)

    for _, row in df.iterrows():
        pt = _pick_type(row)
        if "demon" in pt:
            continue
        is_goblin = "goblin" in pt
        is_std = "standard" in pt
        if goblin_only:
            if not is_goblin:
                continue
        elif not is_goblin and not is_std:
            continue

        player = str(row.get(player_c) or "").strip()
        prop = str(row.get(prop_c) or "").strip()
        try:
            line = float(row.get(line_c))
        except (TypeError, ValueError):
            continue
        side = _norm_side(row.get(side_c) if side_c else "")
        if not side:
            side = "OVER" if is_goblin else ""
        if side not in {"OVER", "UNDER"}:
            continue
        if is_goblin and side != "OVER":
            continue

        # Hard gate: card_l5_ok is None unless card window clears 5/5.
        detail = card_l5_ok(boxes, player, prop, line, side, slate=slate)
        if detail is None:
            continue

        newest = detail["card_dates"][0] if detail["card_dates"] else ""
        # Early-season card windows are prior-year RS/playoffs by design (Allen).
        # STALE = no late-season/playoff tape (newest before Apr 1 of opener year).
        stale_cut = None
        if opener is not None:
            stale_cut = f"{opener.year}-04-01"
        stale = bool(stale_cut and newest and newest < stale_cut)
        rec = {
            "player": player,
            "prop": prop,
            "side": side,
            "line": line,
            "pick_type": pt,
            "window_mix": detail["window_mix"],
            "pre_flag": detail["pre_flag"],
            "card_vals": detail["card_vals"],
            "card_dates": detail["card_dates"],
            "pre_vals": detail["pre_vals"],
            "pre_dates": detail["pre_dates"],
            "stale": stale,
            "side_by_side": format_side_by_side(detail),
        }
        rows_out.append(rec)

    rank = {
        "pre_confirms": 0,
        "pre_thin": 1,
        "pre_conflicts": 2,
        "pre_retired": 3,
    }
    rows_out.sort(
        key=lambda r: (
            rank.get(r["pre_flag"], 9),
            1 if r["stale"] else 0,
            r["player"],
            r["prop"],
            r["line"],
        )
    )

    if args.limit and args.limit > 0:
        rows_out = rows_out[: args.limit]

    for flag in ("pre_confirms", "pre_thin", "pre_conflicts", "pre_retired"):
        chunk = [r for r in rows_out if r["pre_flag"] == flag]
        lines.append(f"===== {flag} (n={len(chunk)}) =====")
        for i, r in enumerate(chunk, 1):
            stale_tag = "  STALE" if r["stale"] else ""
            lines.append(
                f"{i:3d}. {r['player']}  {r['prop']}  "
                f"{r['side'][0]}{r['line']}  mix={r['window_mix']}{stale_tag}"
            )
            lines.append(
                f"     card_vals  {_card_line(r['card_dates'], r['card_vals'])}"
            )
            if flag == "pre_retired":
                lines.append("     pre_vals   (retired — ≥5 current-season RS)")
            elif r["pre_dates"]:
                lines.append(
                    f"     pre_vals   {_card_line(r['pre_dates'], r['pre_vals'])}"
                )
            else:
                lines.append("     pre_vals   (none)")
        lines.append("")

    lines.append(f"total card L5=5 clears: {len(rows_out)}")
    for flag in ("pre_confirms", "pre_thin", "pre_conflicts", "pre_retired"):
        n = sum(1 for r in rows_out if r["pre_flag"] == flag)
        lines.append(f"  {flag}: {n}")
    n_stale = sum(1 for r in rows_out if r["stale"])
    lines.append(f"  stale (newest card game before Apr 1): {n_stale}")

    # Explainer for disputed / missing legs
    lines.append("")
    lines.append("=== NOTES (Mitchell / Williams / Wallace / Simons) ===")
    notes = [
        (
            "Ajay Mitchell",
            "Rebs+Asts",
            3.5,
            "USER earlier: 6,2,9,7,14 (has miss). "
            "Our ESPN card window (RS+PO) is different and clears 5/5.",
        ),
        (
            "Jalen Williams",
            "Pts+Asts",
            16.5,
            "USER earlier: 2,4,29,23,28 (has misses). "
            "Our ESPN card window clears 5/5 — those user vals are not a "
            "contiguous sequence in our box DB.",
        ),
        (
            "Jalen Williams",
            "Pts+Rebs",
            16.5,
            "USER earlier: 1,5,33,20,29 (has misses). Same: our card clears.",
        ),
        (
            "Cason Wallace",
            "3-PT Made",
            0.5,
            "NOT in any bucket because card window FAILS 5/5. "
            "Playoff L5 includes two 0s (Apr 22, Apr 19). "
            "Board RS-only attach was [1,1,4,2,1] L5=5 — that was the old gate.",
        ),
        (
            "Anfernee Simons",
            "Points",
            3.5,
            "Clears card 5/5 but STALE: newest card game is Feb (before RS opener). "
            "No 2026-27 RS/playoff boxes in cache yet.",
        ),
    ]
    for player, prop, line, note in notes:
        detail = card_l5_ok(boxes, player, prop, line, "OVER", slate=slate)
        card = l5_card_window(boxes, player, prop, n=5)
        lines.append(f"* {player} {prop} O{line}")
        lines.append(f"  {note}")
        if card:
            lines.append(
                f"  our_card  mix={window_mix(card)}  "
                f"{_card_line([g['date'] for g in card], [g['val'] for g in card])}"
            )
            lines.append(
                f"  clears_5of5={l5_clears([g['val'] for g in card], line, 'OVER')}  "
                f"in_list={detail is not None}  "
                f"pre_flag={None if detail is None else detail['pre_flag']}"
            )
        else:
            lines.append("  our_card  (insufficient RS+PO games)")

    # Spot-check
    spot_n = min(int(args.spot_check), len(rows_out))
    spot = rows_out[:spot_n]
    match = 0
    lines.append("")
    lines.append(f"=== spot-check ({spot_n} legs) ===")
    for r in spot:
        card = l5_card_window(boxes, r["player"], r["prop"], n=5)
        ok = (
            card is not None
            and "P" not in window_mix(card)
            and l5_clears([g["val"] for g in card], r["line"], r["side"])
            and preseason_flag(boxes, r["player"], r["prop"], r["line"], r["side"])
            == r["pre_flag"]
        )
        if ok:
            match += 1
        status = "MATCH" if ok else "MISS"
        lines.append(
            f"  {status}  {r['player']} {r['prop']} {r['side'][0]}{r['line']}  "
            f"vals={r['card_vals']}  dates={r['card_dates']}  {r['pre_flag']}"
        )
    rate = (100.0 * match / spot_n) if spot_n else 0.0
    lines.append(f"spot-check match rate: {match}/{spot_n} ({rate:.0f}%)")

    text = "\n".join(lines) + "\n"
    out = Path(args.out) if args.out else (
        ROOT / "data" / "reports" / f"nba_card_window_l5_{slate}.txt"
    )
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")

    # Sync into outputs/<slate>/ for ticket work
    out_dir = ROOT / "outputs" / slate
    out_dir.mkdir(parents=True, exist_ok=True)
    synced = out_dir / out.name
    shutil.copy2(out, synced)

    meta = {
        "slate": slate,
        "gate": "card_l5eq5_then_pre_flag",
        "n_clears": len(rows_out),
        "spot_check_n": spot_n,
        "spot_check_match": match,
        "spot_check_rate": rate,
        "n_stale": n_stale,
        "pre_flags": {
            f: sum(1 for r in rows_out if r["pre_flag"] == f)
            for f in ("pre_confirms", "pre_thin", "pre_conflicts")
        },
        "paths": {"report": str(out), "synced": str(synced)},
    }
    meta_path = out.with_suffix(".json")
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    shutil.copy2(meta_path, out_dir / meta_path.name)

    print(text)
    print(f"wrote {out}")
    print(f"synced {synced}")
    print(f"wrote {meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
