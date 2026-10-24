"""Diff card-window L5=5 before (stale cutoff) vs after (live root+merge).

Stale = drop all boxes after 2026-06-13 (Sports/NBA copy state before Oct backfill).
Also prints Tyson 2PA and Holmgren 3PTA cards from live data.
"""
from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.cushion_z import l5_clears
from utils.nba_early_l5 import card_l5_ok, l5_card_window, window_mix
from utils.nba_minutes_role import (
    clear_nba_box_frame_cache,
    load_nba_box_frame,
    nba_box_max_date,
)

SLATE = "2026-10-22"
STALE_CUT = date(2026, 6, 13)
OUT = ROOT / "data" / "reports" / f"nba_card_l5_stale_vs_live_{SLATE}.txt"
OUT_JSON = OUT.with_suffix(".json")


def _norm_side(s: object) -> str:
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


def _load_step8() -> pd.DataFrame:
    path = ROOT / "outputs" / SLATE / f"step8_nba_direction_clean_{SLATE}.xlsx"
    if not path.exists():
        path = ROOT / "outputs" / SLATE / "nba" / "step8_all_direction_clean.xlsx"
    xl = pd.ExcelFile(path)
    sheet = "ALL" if "ALL" in xl.sheet_names else xl.sheet_names[-1]
    raw = pd.read_excel(xl, sheet_name=sheet, header=None)
    header_i = 0
    for i in range(min(8, len(raw))):
        cells = [str(x).strip().lower() for x in raw.iloc[i].tolist()[:25]]
        if "player" in cells or "prop" in cells or "prop_type" in cells:
            header_i = i
            break
    return pd.read_excel(xl, sheet_name=sheet, header=header_i).dropna(how="all")


def _scan(df: pd.DataFrame, boxes: pd.DataFrame) -> dict[tuple, dict]:
    player_c = next(c for c in ("player", "Player") if c in df.columns)
    prop_c = next(
        c for c in ("prop", "Prop", "prop_type", "prop_norm") if c in df.columns
    )
    line_c = next(c for c in ("line", "Line") if c in df.columns)
    side_c = next(
        (c for c in ("final_bet_direction", "side", "Side", "direction") if c in df.columns),
        None,
    )
    out: dict[tuple, dict] = {}
    for _, row in df.iterrows():
        pt = _pick_type(row)
        if "demon" in pt:
            continue
        is_goblin = "goblin" in pt
        is_std = "standard" in pt
        if not is_goblin and not is_std:
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
        key = (player, prop, side, round(line, 2), pt.split()[0] if pt else "")
        detail = card_l5_ok(boxes, player, prop, line, side, slate=SLATE)
        card = l5_card_window(boxes, player, prop, n=5)
        out[key] = {
            "clears": detail is not None,
            "pre_flag": None if detail is None else detail["pre_flag"],
            "mix": window_mix(card) if card else "",
            "vals": [g["val"] for g in card] if card else [],
            "dates": [g["date"] for g in card] if card else [],
            "player": player,
            "prop": prop,
            "side": side,
            "line": line,
            "pick": pt,
        }
    return out


def _card_print(boxes: pd.DataFrame, player: str, prop: str, line: float, side: str) -> list[str]:
    lines = [f"{player} | {prop} {side} {line}"]
    card = l5_card_window(boxes, player, prop, n=5)
    if not card:
        lines.append("  (no card window)")
        return lines
    for g in card:
        v = g["val"]
        hit = (v < line) if side == "UNDER" else (v > line)
        lines.append(
            f"  {g['date']}  {g.get('window','?'):9}  {prop}={v:g}  "
            f"{'HIT' if hit else 'MISS'}"
        )
    vals = [g["val"] for g in card]
    hits = sum(
        1 for v in vals if ((v < line) if side == "UNDER" else (v > line))
    )
    lines.append(
        f"  L5 avg={sum(vals)/len(vals):.2f}  hits={hits}/5  mix={window_mix(card)}  "
        f"clears_5of5={l5_clears(vals, line, side)}"
    )
    return lines


def main() -> int:
    clear_nba_box_frame_cache()
    live = load_nba_box_frame()
    mx = nba_box_max_date(live)
    print(f"live max game_date={mx}  rows={len(live)}")

    stale = live[live["game_date"].dt.date <= STALE_CUT].copy()
    print(f"stale cutoff <= {STALE_CUT}  rows={len(stale)}")

    df = _load_step8()
    before = _scan(df, stale)
    after = _scan(df, live)

    gained = []
    lost = []
    flag_flip = []
    for key in sorted(set(before) | set(after), key=lambda k: (k[0], k[1], k[3])):
        b = before.get(key)
        a = after.get(key)
        if b is None or a is None:
            continue
        if (not b["clears"]) and a["clears"]:
            gained.append((key, b, a))
        elif b["clears"] and (not a["clears"]):
            lost.append((key, b, a))
        elif b["clears"] and a["clears"] and b["pre_flag"] != a["pre_flag"]:
            flag_flip.append((key, b, a))

    lines: list[str] = []
    lines.append(f"NBA card L5=5 stale vs live  slate={SLATE}")
    lines.append(f"stale_cut={STALE_CUT}  live_max={mx}")
    lines.append(f"before clears={sum(1 for v in before.values() if v['clears'])}")
    lines.append(f"after  clears={sum(1 for v in after.values() if v['clears'])}")
    lines.append("")

    lines.append(f"===== GAINED 5/5 (n={len(gained)}) =====")
    for key, b, a in gained:
        lines.append(
            f"+ {a['player']} | {a['prop']} {a['side'][0]}{a['line']} ({a['pick']})"
        )
        lines.append(
            f"    before: clears=False  vals={b['vals']} dates={b['dates']} mix={b['mix']}"
        )
        lines.append(
            f"    after:  clears=True   vals={a['vals']} dates={a['dates']} "
            f"mix={a['mix']} pre={a['pre_flag']}"
        )
    lines.append("")

    lines.append(f"===== LOST 5/5 (n={len(lost)}) =====")
    for key, b, a in lost:
        lines.append(
            f"- {a['player']} | {a['prop']} {a['side'][0]}{a['line']} ({a['pick']})"
        )
        lines.append(
            f"    before: clears=True   vals={b['vals']} dates={b['dates']} "
            f"mix={b['mix']} pre={b['pre_flag']}"
        )
        lines.append(
            f"    after:  clears=False  vals={a['vals']} dates={a['dates']} mix={a['mix']}"
        )
    lines.append("")

    lines.append(f"===== PRE FLAG FLIP (still 5/5) (n={len(flag_flip)}) =====")
    for key, b, a in flag_flip:
        lines.append(
            f"~ {a['player']} | {a['prop']} {a['side'][0]}{a['line']}  "
            f"{b['pre_flag']} -> {a['pre_flag']}"
        )
        lines.append(f"    before dates={b['dates']} vals={b['vals']}")
        lines.append(f"    after  dates={a['dates']} vals={a['vals']}")
    lines.append("")

    lines.append("===== TICKET SPOT CARDS (live) =====")
    for args in (
        ("Chet Holmgren", "3-PT Attempted", 2.5, "UNDER"),
        ("Jaylon Tyson", "Two Pointers Attempted", 3.5, "UNDER"),
        ("LeBron James", "Two Pointers Attempted", 8.5, "OVER"),
        ("VJ Edgecombe", "Points", 5.5, "OVER"),
    ):
        lines.extend(_card_print(live, *args))
        lines.append("")

    text = "\n".join(lines) + "\n"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text, encoding="utf-8")
    meta = {
        "slate": SLATE,
        "stale_cut": str(STALE_CUT),
        "live_max": str(mx),
        "n_gained": len(gained),
        "n_lost": len(lost),
        "n_flag_flip": len(flag_flip),
        "gained": [
            {
                "player": a["player"],
                "prop": a["prop"],
                "side": a["side"],
                "line": a["line"],
                "before_vals": b["vals"],
                "after_vals": a["vals"],
                "after_dates": a["dates"],
                "pre_flag": a["pre_flag"],
            }
            for _, b, a in gained
        ],
        "lost": [
            {
                "player": a["player"],
                "prop": a["prop"],
                "side": a["side"],
                "line": a["line"],
                "before_vals": b["vals"],
                "after_vals": a["vals"],
            }
            for _, b, a in lost
        ],
    }
    OUT_JSON.write_text(json.dumps(meta, indent=2), encoding="utf-8")
    print(text.encode("ascii", "replace").decode("ascii"))
    print(f"wrote {OUT}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
