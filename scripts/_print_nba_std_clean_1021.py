"""Print 2026-10-21 NBA Standard: dual L5=5 (blend + RS) ∩ clean_minutes."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.cushion_z import play_side_gap
from utils.l5_ranked_by_cell import gap_z
from utils.nba_early_l5 import early_season_l5_ok
from utils.nba_minutes_role import load_nba_box_frame

SLATE = "2026-10-21"
XLSX = ROOT / "outputs" / SLATE / "nba" / "step8_all_direction_clean.xlsx"
OUT = ROOT / "outputs" / SLATE / "nba_standard_l5eq5_dual_window_clean_1021.txt"

TIER_ORD = {"A": 0, "B": 1, "C": 2, "D": 3}


def _v(x) -> str:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return "-"
    s = str(x).strip()
    return "-" if s.lower() in {"nan", "none", "null", "<na>"} else s


def _fmt(x, nd=1) -> str:
    try:
        if x is None or (isinstance(x, float) and math.isnan(x)):
            return "-"
        return f"{float(x):.{nd}f}"
    except (TypeError, ValueError):
        return "-"


def _side_from_row(r: pd.Series) -> str | None:
    for key in ("Direction", "final_bet_direction", "bet_direction", "side"):
        if key not in r.index:
            continue
        s = str(r.get(key) or "").strip().upper()
        if s in {"OVER", "UNDER", "O", "U"}:
            return "OVER" if s in {"OVER", "O"} else "UNDER"
    return None


def _is_clean(val) -> bool:
    if isinstance(val, bool):
        return val
    if isinstance(val, (int, float)) and not (isinstance(val, float) and math.isnan(val)):
        return bool(val)
    s = str(val or "").strip().lower()
    return s in {"true", "1", "yes"}


def main() -> None:
    all_df = pd.read_excel(XLSX, sheet_name="ALL")
    boxes = load_nba_box_frame()

    pool = all_df[
        all_df["Pick Type"].astype(str).str.contains("Standard", case=False, na=False)
    ].copy()
    pool = pool[~pool["Pick Type"].astype(str).str.contains("Demon", case=False, na=False)]
    pool = pool[pool["Clean Minutes"].map(_is_clean)].copy()

    rows: list[dict] = []
    skipped = {"no_dual": 0, "bad_line": 0}
    for _, r in pool.iterrows():
        side = _side_from_row(r)
        if side is None:
            continue
        try:
            line = float(r["Line"])
        except (TypeError, ValueError):
            skipped["bad_line"] += 1
            continue
        if math.isnan(line):
            skipped["bad_line"] += 1
            continue

        player = str(r.get("Player") or "").strip()
        prop = str(r.get("Prop") or "").strip()
        early = early_season_l5_ok(boxes, player, prop, line, side, slate=SLATE)
        if early is None:
            skipped["no_dual"] += 1
            continue

        values = early.get("ticket_vals") or early["rs_vals"]
        l5_avg = early.get("ticket_avg") or early["rs_avg"]
        gap = play_side_gap(l5_avg, line, side)
        gz = gap_z(values, line, side)
        if gap is None:
            continue

        flags = []
        mf = _v(r.get("Minutes Flag"))
        if mf != "-":
            flags.append(mf)
        mode = early.get("mode") or "dual"
        if mode == "rs_only_early_season":
            flags.append("RS_only_early")
            flags.append(f"diag_blend={early['n_pre']}P+{early.get('n_rs',0)}R")
        else:
            flags.append(f"blend={early['n_pre']}P+{early['n_rs']}R")
            flags.append("RS_L5=5")

        tier = str(r.get("Tier") or "").strip().upper() or "?"
        ticket_games = early["rs"] if early.get("mode") == "rs_only_early_season" else early["blend"]
        dated = ",".join(
            f"{g['val']:g}@{g['date'][5:]}{g['tag']}" for g in (ticket_games or [])
        )
        rs_dated = ",".join(f"{g['val']:g}@{g['date'][5:]}" for g in early["rs"])
        rows.append(
            {
                "player": player,
                "prop": prop,
                "side": side,
                "line": line,
                "dated": dated,
                "rs_dated": rs_dated,
                "l5_avg": l5_avg,
                "rs_avg": early["rs_avg"],
                "gap": gap,
                "scaled_gap": gz,
                "tier": tier,
                "min_l5": r.get("Min L5"),
                "min_season": r.get("Min Season"),
                "min_pre": r.get("Min Preseason"),
                "starter": _v(r.get("Starter")),
                "flags": "|".join(flags),
                "_tier_ord": TIER_ORD.get(tier, 9),
                "_scaled": gz if gz is not None else -1e9,
            }
        )

    rows.sort(key=lambda x: (x["_tier_ord"], -x["_scaled"], -x["gap"], x["player"], x["prop"]))
    best: list[dict] = []
    seen: set[str] = set()
    for rec in rows:
        key = rec["player"].casefold()
        if key in seen:
            continue
        seen.add(key)
        best.append(rec)

    lines = [
        f"NBA Standard | early=RS-only L5=5 else dual blend+RS | clean_minutes | {SLATE}",
        "sort: tier, then scaled gap on ticket window; 1 best prop/player",
        "Early season: preseason excluded from L5. Later: blend AND RS-only both 5/5.",
        f"n_legs={len(best)} (from {len(rows)} dual-clear rows; skipped_no_dual={skipped['no_dual']})",
        "",
        f"{'#':>3} {'Tier':<4} {'Player':<22} {'Prop':<16} {'Side':<5} {'Line':>5} "
        f"{'Blend L5 (dated)':<48} {'Bavg':>5} {'Ravg':>5} {'Gap':>5} {'Gz':>5} "
        f"{'MinL5':>5} {'MinPre':>5} {'MinSz':>5} Flags",
        "-" * 175,
    ]
    for i, rec in enumerate(best, 1):
        gz_s = f"{rec['scaled_gap']:.2f}" if rec["scaled_gap"] is not None else "-"
        lines.append(
            f"{i:3d} {rec['tier']:<4} {rec['player']:<22} {rec['prop']:<16} {rec['side']:<5} "
            f"{rec['line']:5.1f} {rec['dated']:<48} {rec['l5_avg']:5.1f} {rec['rs_avg']:5.1f} "
            f"{rec['gap']:5.1f} {gz_s:>5} "
            f"{_fmt(rec['min_l5']):>5} {_fmt(rec['min_pre']):>5} {_fmt(rec['min_season']):>5} "
            f"{rec['flags']}"
        )
        lines.append(f"     RS-only L5: {rec['rs_dated']}")

    text = "\n".join(lines) + "\n"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text, encoding="utf-8")
    print(text.encode("ascii", "replace").decode("ascii"))
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
