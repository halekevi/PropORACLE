"""Rebuild usable L5=5 list: card-window NBA, same-day CFB lines, Golf Strokes only.

Rank by line / L5avg (OVER ascending = more cushion; UNDER descending).
One prop per player; single stats preferred over combos.
Print five card values + preseason bucket for each leg.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.cushion_z import l5_clears
from utils.nba_early_l5 import (
    card_l5_ok,
    l5_card_window,
    l5_preseason,
    preseason_flag,
    window_mix,
)
from utils.nba_minutes_role import clear_nba_box_frame_cache, load_nba_box_frame

SLATE = "2026-10-22"

# Explicit card-window misses — never list (user-verified).
NBA_DENY = {
    "jarrett allen",
    "james harden",
    "christian braun",
    "jamal murray",
    "tyrese maxey",
    "donovan mitchell",
}

# User-verified card-window survivors for the usable short list.
# Full card-clear audit is still printed separately.
NBA_KEEP = {
    ("vj edgecombe", "points", 5.5),
    ("sam merrill", "points", 3.5),
    ("jared mccain", "rebounds", 0.5),
}

# Single-stat props preferred when choosing one leg per player.
SINGLE_STATS = {
    "points",
    "rebounds",
    "assists",
    "steals",
    "blocked shots",
    "blocks",
    "turnovers",
    "3-pt made",
    "pass yards",
    "receiving yards",
    "rush yards",
    "receptions",
    "strokes",
    "birdies or better",
}


def _is_single(prop: str) -> bool:
    p = str(prop or "").strip().lower().replace("(combo)", "").strip()
    if "+" in p or "combo" in p:
        return False
    return p in SINGLE_STATS or p.replace(" ", "_") in {
        "pass_yards",
        "receiving_yards",
        "rush_yards",
    }


def _ratio(line: float, l5avg: float, side: str) -> float:
    if l5avg <= 0:
        return 999.0
    return line / l5avg


def _fmt_vals(dates: list, vals: list) -> str:
    return " ".join(f"{d}:{v:g}" for d, v in zip(dates, vals))


def _nba_row_dict(row, detail: dict, line: float, prop: str) -> dict:
    player = str(row.get("player") or "").strip()
    vals = detail["card_vals"]
    avg = sum(vals) / 5.0
    return {
        "sport": "NBA",
        "bucket": "Goblin OVER",
        "player": player,
        "team": str(row.get("team") or ""),
        "opp": str(row.get("opp_team") or row.get("opp") or ""),
        "prop": prop,
        "side": "OVER",
        "line": line,
        "l5avg": avg,
        "ratio": _ratio(line, avg, "OVER"),
        "single": _is_single(prop),
        "card_vals": vals,
        "card_dates": detail["card_dates"],
        "window_mix": detail["window_mix"],
        "pre_flag": detail["pre_flag"],
        "pre_vals": detail["pre_vals"],
        "pre_dates": detail["pre_dates"],
        "tip": str(row.get("start_time") or "")[:16].replace("T", " "),
    }


def nba_legs(boxes: pd.DataFrame) -> tuple[list[dict], list[dict]]:
    """Return (usable keepers, full card-clear audit)."""
    path = ROOT / f"outputs/{SLATE}/step8_nba_direction_clean_{SLATE}.xlsx"
    xl = pd.ExcelFile(path)
    df = pd.read_excel(xl, sheet_name="ALL", header=1)
    today = df.copy()
    if "start_time" in today.columns:
        today = today[today["start_time"].astype(str).str.startswith(SLATE)]
    audit: list[dict] = []
    keep: list[dict] = []
    for _, row in today.iterrows():
        pt = str(row.get("pick_type") or "").strip().lower()
        if "goblin" not in pt:
            continue
        player = str(row.get("player") or "").strip()
        if player.lower() in NBA_DENY:
            continue
        prop = str(row.get("prop_type") or row.get("prop") or "").strip()
        try:
            line = float(row.get("line"))
        except (TypeError, ValueError):
            continue
        detail = card_l5_ok(boxes, player, prop, line, "OVER", slate=SLATE)
        if detail is None:
            continue
        rec = _nba_row_dict(row, detail, line, prop)
        audit.append(rec)
        key = (player.lower(), prop.lower(), float(line))
        if key in NBA_KEEP:
            keep.append(rec)
    return keep, audit


def cfb_legs() -> list[dict]:
    """Same-day step1 lines + Wed step8 L5 windows for confirmed keepers only."""
    s1 = pd.read_csv(ROOT / f"outputs/{SLATE}/cfb/step1_cfb.csv", low_memory=False)
    s1_today = s1[s1["start_time"].astype(str).str.startswith(SLATE)]
    step8 = pd.read_parquet(
        ROOT / "outputs/2026-10-21/cfb/step8_cfb_direction_clean.parquet"
    )

    # (player, prop, odds_type, preferred_line or None to take live goblin/standard)
    wanted = [
        ("Camden Coleman", "Pass Yards", "goblin", 99.5),
        ("Corey Scott", "Receiving Yards", "goblin", 19.5),
        # Standard: live line moved 41.5 -> 44.5; only keep if still L5=5
        ("Corey Scott", "Receiving Yards", "standard", None),
    ]
    out: list[dict] = []
    notes: list[str] = []

    for player, prop, odds, prefer in wanted:
        live = s1_today[
            (s1_today["player"] == player)
            & (s1_today["stat_type"] == prop)
            & (s1_today["odds_type"].astype(str).str.lower() == odds)
        ]
        if live.empty:
            notes.append(f"CFB CUT {player} {prop} {odds}: not on today's step1 board")
            continue
        lines = sorted(float(x) for x in live["line"].tolist())
        if prefer is not None:
            if prefer not in lines:
                notes.append(
                    f"CFB CUT {player} {prop} {odds} O{prefer}: gone from today "
                    f"(live lines={lines})"
                )
                continue
            line = float(prefer)
        else:
            # one standard face — take the live standard line
            line = float(lines[0]) if len(lines) == 1 else float(min(lines))

        # L5 window from step8 (same prop/player; any matching pick row)
        m = step8[
            (step8["Player"] == player)
            & (step8["Prop"] == prop)
            & (step8["Pick Type"].astype(str).str.lower() == odds)
        ]
        if m.empty:
            notes.append(f"CFB CUT {player} {prop}: no step8 L5 window")
            continue
        r = m.iloc[0]
        vals = [float(r[f"Stat G{i}"]) for i in range(1, 6)]
        if not l5_clears(vals, line, "OVER"):
            notes.append(
                f"CFB CUT {player} {prop} {odds} O{line}: L5 vals {vals} "
                f"no longer 5/5 vs today's line (was ok at older line)"
            )
            continue
        avg = sum(vals) / 5.0
        tip = str(live.iloc[0]["start_time"])[:16].replace("T", " ")
        out.append(
            {
                "sport": "CFB",
                "bucket": "Goblin OVER" if odds == "goblin" else "Standard OVER",
                "player": player,
                "team": str(live.iloc[0].get("team") or ""),
                "opp": str(live.iloc[0].get("pp_opp_team") or ""),
                "prop": prop,
                "side": "OVER",
                "line": line,
                "l5avg": avg,
                "ratio": _ratio(line, avg, "OVER"),
                "single": _is_single(prop),
                "card_vals": vals,
                "card_dates": [f"g{i}" for i in range(1, 6)],
                "window_mix": "CFB",
                "pre_flag": str(r.get("Season L5 Flag") or "") or "n/a",
                "pre_vals": [],
                "pre_dates": [],
                "tip": tip,
                "note": f"same-day step1 line; L5 from 10-21 step8; flag={r.get('Season L5 Flag')}",
            }
        )

    # Explicit cuts
    notes.append("CFB CUT Marcus Stokes: user — Goblin lines unreliable / cut")
    notes.append("CFB CUT Henry Hasselbeck: L5avg=24 backup artifact")
    return out, notes


def golf_legs() -> tuple[list[dict], list[str]]:
    """Strokes UNDER only. Pars excluded (known scorecard bug)."""
    notes = [
        "Golf Pars U11.0 EXCLUDED: scorecard index bug "
        "(utils/golf_scorecard.py — index 5 was residual garbage; "
        "stat_g shows 0–8 'pars' instead of ~10–14). Do not use until fixed.",
    ]
    g = pd.read_parquet(
        ROOT / "outputs/2026-10-21/golf/step8_golf_direction_clean.parquet"
    )
    # tip filter to slate if possible
    if "start_time" in g.columns:
        g = g[g["start_time"].astype(str).str.startswith(SLATE)]
    out: list[dict] = []
    m = g[
        (g["prop_type"].astype(str).str.lower() == "strokes")
        & (g["pick_type"].astype(str).str.lower() == "standard")
        & (g["direction"].astype(str).str.upper() == "UNDER")
        & (pd.to_numeric(g["l5_under"], errors="coerce") == 5)
    ]
    for _, r in m.iterrows():
        vals = [float(r[f"stat_g{i}"]) for i in range(1, 6)]
        line = float(r["line"])
        if not l5_clears(vals, line, "UNDER"):
            continue
        avg = sum(vals) / 5.0
        out.append(
            {
                "sport": "GOLF",
                "bucket": "Standard UNDER",
                "player": str(r.get("player") or r.get("player_name") or ""),
                "team": "PGA",
                "opp": "",
                "prop": "Strokes",
                "side": "UNDER",
                "line": line,
                "l5avg": avg,
                "ratio": _ratio(line, avg, "UNDER"),
                "single": True,
                "card_vals": vals,
                "card_dates": [f"r{i}" for i in range(1, 6)],
                "window_mix": "GOLF",
                "pre_flag": "n/a",
                "pre_vals": [],
                "pre_dates": [],
                "tip": str(r.get("start_time") or "")[:16].replace("T", " "),
            }
        )
    return out, notes


def pick_one_per_player(rows: list[dict]) -> list[dict]:
    """Prefer single-stat, then best ratio for the side."""
    by: dict[str, list[dict]] = {}
    for r in rows:
        key = (r["sport"], r["player"].lower())
        by.setdefault(key, []).append(r)
    chosen: list[dict] = []
    for _, group in by.items():
        singles = [g for g in group if g["single"]]
        pool = singles if singles else group
        # OVER: lower line/L5avg better; UNDER: higher line/L5avg better
        if pool[0]["side"] == "OVER":
            pool.sort(key=lambda x: (x["ratio"], x["line"]))
        else:
            pool.sort(key=lambda x: (-x["ratio"], -x["line"]))
        chosen.append(pool[0])
    return chosen


def main() -> int:
    clear_nba_box_frame_cache()
    boxes = load_nba_box_frame()

    nba_keep, nba_audit = nba_legs(boxes)
    cfb, cfb_notes = cfb_legs()
    golf, golf_notes = golf_legs()

    # Golf: keep only the three Strokes U68.5 survivors (thin 2–3 stroke gaps).
    golf_keep_names = {"trace crowe", "seamus power", "austin smotherman"}
    golf = [
        g
        for g in golf
        if g["player"].lower() in golf_keep_names and abs(g["line"] - 68.5) < 0.01
    ]

    # Usable list = verified NBA keepers + same-day CFB + Golf Strokes.
    # Full NBA card-clear audit printed below (deny-list enforced).
    all_rows = nba_keep + cfb + golf
    one = pick_one_per_player(all_rows)

    buckets = {
        "Goblin OVER": [r for r in one if r["bucket"] == "Goblin OVER"],
        "Standard OVER": [r for r in one if r["bucket"] == "Standard OVER"],
        "Standard UNDER": [r for r in one if r["bucket"] == "Standard UNDER"],
    }
    for name, rows in buckets.items():
        if not rows:
            continue
        if rows[0]["side"] == "OVER":
            rows.sort(key=lambda r: (r["ratio"], r["sport"], r["player"]))
        else:
            rows.sort(key=lambda r: (-r["ratio"], r["sport"], r["player"]))

    lines: list[str] = []
    lines.append(f"REBUILT L5=5 usable list  slate={SLATE}")
    lines.append("NBA = card window (RS+playoffs) + preseason flag")
    lines.append("Rank = line/L5avg (OVER asc = more cushion; UNDER desc)")
    lines.append("One prop per player; single stats preferred over combos")
    lines.append("Five values printed per leg")
    lines.append("")
    lines.append("NOTES / CUTS:")
    for n in cfb_notes + golf_notes:
        lines.append(f"  - {n}")
    lines.append(
        "  - NBA deny (card-window miss): Allen, Harden, Braun, Murray, Maxey, Donovan Mitchell"
    )
    lines.append(
        "  - NBA usable short list = verified card survivors only: "
        "Edgecombe Pts O5.5, Merrill Pts O3.5, McCain Reb O0.5"
    )
    lines.append("  - NFL FG combo Unders: ignored (broken averages)")
    lines.append("")

    # Also print preseason-bucket splits for NBA survivors in the list
    for bname, rows in buckets.items():
        lines.append(f"===== {bname} (n={len(rows)}) =====")
        if not rows:
            lines.append("  (none)")
            lines.append("")
            continue
        for i, r in enumerate(rows, 1):
            lines.append(
                f"{i:2d}. [{r['sport']}] {r['player']}  {r['prop']}  "
                f"{r['side'][0]}{r['line']}  "
                f"L5avg={r['l5avg']:.2f}  line/L5avg={r['ratio']:.3f}  "
                f"{r['pre_flag']}  mix={r['window_mix']}"
            )
            lines.append(
                f"    card  {_fmt_vals(r['card_dates'], r['card_vals'])}"
            )
            if r["pre_vals"]:
                lines.append(
                    f"    pre   {_fmt_vals(r['pre_dates'], r['pre_vals'])}"
                )
            else:
                lines.append("    pre   (n/a)")
            if r.get("note"):
                lines.append(f"    note  {r['note']}")
        lines.append("")

    # Extra: all NBA card-clear Goblins (deny enforced; not in usable short list)
    lines.append(
        "===== NBA card-window Goblin OVER audit "
        "(all L5=5 clears after deny; not auto-added to usable list) ====="
    )
    nba_sorted = sorted(
        nba_audit, key=lambda r: (0 if r["single"] else 1, r["ratio"], r["player"])
    )
    for r in nba_sorted:
        lines.append(
            f"  {r['player']:<22} {r['prop']:<18} O{r['line']}  "
            f"avg={r['l5avg']:.1f}  ratio={r['ratio']:.3f}  {r['pre_flag']}  "
            f"card={r['card_vals']}"
        )
    lines.append("")

    text = "\n".join(lines) + "\n"
    out = ROOT / "data" / "reports" / f"l5eq5_card_ratio_{SLATE}.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    sync = ROOT / "outputs" / SLATE / out.name
    sync.write_text(text, encoding="utf-8")
    print(text)
    print(f"wrote {out}")
    print(f"synced {sync}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
