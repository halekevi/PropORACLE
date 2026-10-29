"""NBA attempt / volume prop edge scan — card-window L5=5, Demons included.

Tonight: 5/5 attempt+volume Overs on the live board, one prop per player,
ranked by line / L5avg. Print five card values, window_mix, preseason flag,
pick_type (Standard / Goblin / Demon), and flags (not drops).

Backtest: graded history, recompute card-window L5=5 from boxes, compare
attempt props vs points / rebounds / combos by pick_type. Report only —
no gate until n>=100.

  py -3.14 scripts/_scan_nba_attempt_props_l5eq5.py
  py -3.14 scripts/_scan_nba_attempt_props_l5eq5.py --slate 2026-10-22
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.comparison_tracks import wilson_band
from utils.cushion_z import l5_clears
from utils.nba_board_team import team_changed_status
from utils.nba_early_l5 import card_l5_ok, prop_col
from utils.nba_minutes_role import clear_nba_box_frame_cache, load_nba_box_frame

# Role-sticky attempt + minutes-driven shot volume on PrizePicks.
ATTEMPT_PROPS = frozenset(
    {
        "FG Attempted",
        "FG Att",
        "Two Pointers Attempted",
        "2-PT Att",
        "2PT Att",
        "3-PT Attempted",
        "3-PT Att",
        "Free Throws Attempted",
        "FT Att",
        "FG Made",
        "Two Pointers Made",
        "2-PT Made",
        "Free Throws Made",
    }
)
ATTEMPT_KEYS = frozenset(
    {
        "fg attempted",
        "fg att",
        "two pointers attempted",
        "2-pt att",
        "2pt att",
        "3-pt attempted",
        "3-pt att",
        "free throws attempted",
        "ft att",
        "fg made",
        "two pointers made",
        "free throws made",
        "fga",
        "fg2a",
        "fg3a",
        "fta",
        "fgm",
        "fg2m",
        "ftm",
    }
)
COMPARE_CORE = frozenset({"points", "rebounds", "assists"})
COMPARE_COMBO = frozenset(
    {
        "pts+rebs",
        "pts+asts",
        "rebs+asts",
        "pts+rebs+asts",
        "pra",
        "pr",
        "pa",
        "ra",
    }
)

BOARD_CANDIDATES = [
    ROOT / "outputs/{slate}/nba/step1_pp_props_today.csv",
    Path(r"H:\PropORACLE_main_cp") / "outputs" / "{slate}" / "nba" / "step1_pp_props_today.csv",
    ROOT / "Sports/NBA/data/inputs/step1_pp_props_today.csv",
    Path(r"H:\PropORACLE_main_cp") / "Sports" / "NBA" / "data" / "inputs" / "step1_pp_props_today.csv",
]


def _prop_key(prop: str) -> str:
    try:
        from utils.prop_norm import prop_key as _canon_prop_key

        key = _canon_prop_key(prop, "NBA")
        if key:
            return key
    except Exception:
        pass
    return str(prop or "").strip().lower().replace("(combo)", "").strip()


ATTEMPT_CANON = frozenset({"fga", "fg2a", "threes_att", "fta", "fgm", "fg2m", "ftm"})


def _is_attempt(prop: str) -> bool:
    k = _prop_key(prop)
    return k in ATTEMPT_CANON or k in ATTEMPT_KEYS


def _family(prop: str) -> str:
    k = _prop_key(prop)
    if k in ATTEMPT_KEYS:
        return "attempt"
    if k in COMPARE_CORE:
        return "core"
    if k in COMPARE_COMBO or "+" in k:
        return "combo"
    return "other"


def _find_board(slate: str) -> Path:
    for tmpl in BOARD_CANDIDATES:
        p = Path(str(tmpl).format(slate=slate))
        if not p.exists():
            continue
        df = pd.read_csv(p, nrows=5000, low_memory=False)
        if "prop_type" not in df.columns:
            continue
        if df["prop_type"].astype(str).isin(ATTEMPT_PROPS).any():
            return p
    raise FileNotFoundError(
        "No step1 board with attempt props (FG/2PA/3PA/FTA). "
        "Refetch partner-api board into outputs/<slate>/nba/step1_pp_props_today.csv"
    )


def _fmt_vals(dates: list, vals: list) -> str:
    return " ".join(f"{d}:{v:g}" for d, v in zip(dates, vals))


def _ratio(line: float, avg: float) -> float:
    if avg <= 0:
        return 999.0
    return line / avg


def _stale_gap(window_mix: str, n_card_rs: int, ratio: float) -> int:
    """Prior-season playoff pad + soft Demon line (line << L5avg). Flag only."""
    has_po = "O" in str(window_mix)
    soft = ratio <= 0.75  # line ≤ 75% of L5avg
    return int(has_po and n_card_rs < 5 and soft)


def scan_tonight(boxes: pd.DataFrame, slate: str) -> tuple[list[dict], Path, list[str]]:
    board_path = _find_board(slate)
    df = pd.read_csv(board_path, low_memory=False)
    notes: list[str] = [f"board={board_path}"]
    if "fetched_at" in df.columns and len(df):
        notes.append(f"fetched_at={df['fetched_at'].iloc[0]}")

    if "start_time" in df.columns:
        df = df[df["start_time"].astype(str).str.startswith(slate)]

    att = df[df["prop_type"].astype(str).isin(ATTEMPT_PROPS)].copy()
    notes.append(f"attempt+volume board rows={len(att)}")

    out: list[dict] = []
    for _, row in att.iterrows():
        player = str(row.get("player") or "").strip()
        if not player or "+" in player:
            continue
        prop = str(row.get("prop_type") or "").strip()
        if prop_col(prop) is None:
            continue
        pick = str(row.get("pick_type") or "").strip() or "?"
        try:
            line = float(row.get("line"))
        except (TypeError, ValueError):
            continue
        # Demons / Goblins / Standards are all OVER ladders on attempts.
        side = "OVER"
        detail = card_l5_ok(boxes, player, prop, line, side, slate=slate)
        if detail is None:
            continue
        vals = detail["card_vals"]
        avg = sum(vals) / 5.0
        ratio = _ratio(line, avg)
        team = str(row.get("team") or "")
        tc = team_changed_status(player, team, slate=slate)
        flags = []
        l5_is_playoffs = int("O" in detail["window_mix"])
        if l5_is_playoffs:
            flags.append("l5_is_playoffs")
        if tc.get("team_changed"):
            flags.append(
                f"team_changed:{tc.get('prior_team')}->{tc.get('board_team')}"
            )
        if detail["pre_flag"] == "pre_thin":
            flags.append("pre_thin")
        if detail["pre_flag"] == "pre_conflicts":
            flags.append("pre_conflicts")
        sg = _stale_gap(detail["window_mix"], detail["n_card_rs"], ratio)
        if sg:
            flags.append("stale_gap")
        out.append(
            {
                "player": player,
                "team": team,
                "opp": str(row.get("opp_team") or ""),
                "prop": prop,
                "pick_type": pick,
                "side": side,
                "line": line,
                "l5avg": avg,
                "ratio": ratio,
                "card_vals": vals,
                "card_dates": detail["card_dates"],
                "window_mix": detail["window_mix"],
                "pre_flag": detail["pre_flag"],
                "pre_vals": detail["pre_vals"],
                "pre_dates": detail["pre_dates"],
                "flags": flags,
                "tip": str(row.get("start_time") or "")[:16].replace("T", " "),
            }
        )
    return out, board_path, notes


_TRUE_ATTEMPT = frozenset(
    {
        "FG Attempted",
        "Two Pointers Attempted",
        "3-PT Attempted",
        "Free Throws Attempted",
    }
)


def pick_one_per_player(rows: list[dict]) -> list[dict]:
    """One leg per player: true attempts first, then best line/L5avg.

    Among equal prop preference, prefer Demon when it clears with ratio<=0.75
    (soft Demon / stale_gap exploit); else deepest cushion.
    """
    by: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by[r["player"].lower()].append(r)
    chosen: list[dict] = []
    for group in by.values():
        attempts = [g for g in group if g["prop"] in _TRUE_ATTEMPT]
        pool = attempts if attempts else group
        soft_demons = [
            g
            for g in pool
            if str(g["pick_type"]).lower() == "demon" and g["ratio"] <= 0.75
        ]
        # Prefer soft Demon on a true attempt prop (the exploit case).
        soft_attempt_demons = [g for g in soft_demons if g["prop"] in _TRUE_ATTEMPT]
        pick_pool = soft_attempt_demons or soft_demons or pool
        pick_pool.sort(key=lambda x: (x["ratio"], x["line"], x["prop"]))
        chosen.append(pick_pool[0])
    chosen.sort(key=lambda x: (x["ratio"], x["player"]))
    return chosen


def _norm_pick(pt: Any) -> str:
    s = str(pt or "").strip().lower()
    if "demon" in s:
        return "demon"
    if "goblin" in s:
        return "goblin"
    if "standard" in s:
        return "standard"
    return s or "unknown"


def _parse_hit(result: Any) -> bool | None:
    s = str(result or "").strip().upper()
    if s in {"HIT", "W", "WIN", "CORRECT"}:
        return True
    if s in {"MISS", "L", "LOSS", "INCORRECT"}:
        return False
    return None


def backtest(boxes: pd.DataFrame, graded_dir: Path) -> dict[str, Any]:
    """Card-window L5=5 replay on graded NBA legs. Report only."""
    buckets: dict[tuple[str, str, str], list[bool]] = defaultdict(list)
    # (family, pick_type, prop_display) -> hits
    samples_lebron: list[dict] = []
    n_files = 0
    n_checked = 0
    n_no_box = 0

    for path in sorted(graded_dir.glob("graded_props_*.json")):
        if ".bak_" in path.name:
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        slate = str(raw.get("date") or path.stem.replace("graded_props_", ""))[:10]
        chunk = raw.get("props") or raw.get("rows") or []
        if not isinstance(chunk, list):
            continue
        n_files += 1
        for r in chunk:
            if not isinstance(r, dict):
                continue
            sport = str(r.get("sport") or "").upper()
            if sport not in {"NBA", "NBAP"}:
                continue
            prop = str(r.get("prop") or r.get("prop_type") or "").strip()
            fam = _family(prop)
            if fam == "other":
                continue
            if prop_col(prop) is None:
                continue
            direction = str(r.get("direction") or r.get("over_under") or "OVER").upper()
            if direction not in {"OVER", "UNDER"}:
                continue
            # Attempt Demons/Goblins are OVER ladders; keep Unders for Standard core.
            pick = _norm_pick(r.get("pick_type"))
            if fam == "attempt" and direction != "OVER":
                continue
            try:
                line = float(r.get("line"))
            except (TypeError, ValueError):
                continue
            player = str(r.get("player") or "").strip()
            if not player or "+" in player:
                continue
            hit = _parse_hit(r.get("result"))
            if hit is None:
                continue
            detail = card_l5_ok(boxes, player, prop, line, direction, slate=slate)
            n_checked += 1
            if detail is None:
                # Not L5=5 on card window (or missing boxes)
                if prop_col(prop) is not None:
                    # distinguish miss vs no box roughly via empty card
                    from utils.nba_early_l5 import l5_card_window

                    if l5_card_window(boxes, player, prop) is None:
                        n_no_box += 1
                continue
            key = (fam, pick, prop if fam == "attempt" else fam)
            buckets[key].append(hit)
            if "lebron" in player.lower() and _is_attempt(prop):
                samples_lebron.append(
                    {
                        "slate": slate,
                        "prop": prop,
                        "pick": pick,
                        "line": line,
                        "hit": hit,
                        "card": detail["card_vals"],
                        "mix": detail["window_mix"],
                    }
                )

    def _summ(hits: list[bool]) -> dict[str, Any]:
        n = len(hits)
        h = sum(1 for x in hits if x)
        hr = (h / n) if n else None
        band = wilson_band(h, n) if n else None
        return {
            "hits": h,
            "n": n,
            "hit_rate": round(hr, 4) if hr is not None else None,
            "wilson_low": round(band[0], 4) if band else None,
            "wilson_high": round(band[1], 4) if band else None,
            "gate_ready": bool(n >= 100),
        }

    by_family_pick: dict[str, Any] = {}
    by_attempt_prop: dict[str, Any] = {}
    for (fam, pick, prop), hits in sorted(buckets.items()):
        if fam == "attempt":
            by_attempt_prop[f"{prop}|{pick}"] = _summ(hits)
        label = f"{fam}|{pick}"
        # roll up family+pick
        if label not in by_family_pick:
            by_family_pick[label] = []
        by_family_pick[label].extend(hits)

    rolled = {k: _summ(v) for k, v in sorted(by_family_pick.items())}
    return {
        "n_files": n_files,
        "n_checked": n_checked,
        "n_no_box": n_no_box,
        "by_family_pick": rolled,
        "by_attempt_prop_pick": by_attempt_prop,
        "lebron_attempt_l5eq5_samples": samples_lebron[-12:],
        "note": "Report only. No ticket gate until family|pick n>=100.",
    }


def render(
    slate: str,
    one: list[dict],
    all_clears: list[dict],
    notes: list[str],
    bt: dict[str, Any],
) -> str:
    lines: list[str] = []
    lines.append(f"NBA attempt/volume L5=5 edge scan  slate={slate}")
    lines.append("Card window = RS+playoffs (no preseason). Preseason = flag only.")
    lines.append("Rank = line/L5avg (OVER asc). One prop per player.")
    lines.append("Pick types include Demon (harder OVER). Flags are not drops.")
    lines.append("No gate until backtest n>=100 — report only.")
    lines.append("")
    lines.append("NOTES:")
    for n in notes:
        lines.append(f"  - {n}")
    lines.append(
        "  - Test case: LeBron Two Pointers Attempted Demon O8.5 "
        "card should be 10,12,13,14,11 (avg 12.0, ratio 0.708)"
    )
    lines.append("")

    lines.append(f"===== Tonight 5/5 attempt+volume OVER (1/player, n={len(one)}) =====")
    if not one:
        lines.append("  (none)")
    for i, r in enumerate(one, 1):
        flag_s = ",".join(r["flags"]) if r["flags"] else "-"
        lines.append(
            f"{i:2d}. [{r['pick_type']}] {r['player']} ({r['team']})  "
            f"{r['prop']}  O{r['line']}  "
            f"L5avg={r['l5avg']:.2f}  line/L5avg={r['ratio']:.3f}  "
            f"{r['pre_flag']}  mix={r['window_mix']}  flags={flag_s}"
        )
        lines.append(f"    card  {_fmt_vals(r['card_dates'], r['card_vals'])}")
        if r["pre_vals"]:
            lines.append(f"    pre   {_fmt_vals(r['pre_dates'], r['pre_vals'])}")
        else:
            lines.append("    pre   (n/a)")
    lines.append("")

    lines.append(
        f"===== All 5/5 clears before 1/player (n={len(all_clears)}) ====="
    )
    for r in sorted(all_clears, key=lambda x: (x["ratio"], x["player"], x["prop"])):
        lines.append(
            f"  [{r['pick_type']:<8}] {r['player']:<22} {r['prop']:<24} "
            f"O{r['line']:<5} avg={r['l5avg']:.1f} ratio={r['ratio']:.3f} "
            f"{r['pre_flag']} mix={r['window_mix']} card={r['card_vals']}"
        )
    lines.append("")

    lines.append("===== Backtest (card-window L5=5 replay on graded NBA) =====")
    lines.append(
        f"files={bt['n_files']} checked={bt['n_checked']} no_box≈{bt['n_no_box']}"
    )
    lines.append(
        "OBSERVE-ONLY for tickets until cell n>=100. "
        "Demon attempt L5=5 legs are allowed on this list."
    )
    lines.append("")
    lines.append(
        "Hit rate matrix — family (rows) x pick_type (cols). "
        "Compare attempts vs core/combo at the SAME pick type."
    )
    fam_order = ("attempt", "core", "combo")
    pick_order = ("goblin", "standard", "demon")
    by = bt.get("by_family_pick") or {}
    header = f"{'family':<10}" + "".join(f"{p:>22}" for p in pick_order)
    lines.append(header)
    for fam in fam_order:
        cells = []
        for pk in pick_order:
            v = by.get(f"{fam}|{pk}") or {}
            n = int(v.get("n") or 0)
            h = int(v.get("hits") or 0)
            if n <= 0:
                cells.append(f"{'—':>22}")
                continue
            hr = 100.0 * h / n
            tag = "ok" if n >= 100 else "obs"
            cells.append(f"{hr:5.1f}% {h}/{n} ({tag})".rjust(22))
        lines.append(f"{fam:<10}" + "".join(cells))
    lines.append("")
    lines.append("by family|pick_type (detail):")
    for k, v in by.items():
        ready = "n>=100" if v["gate_ready"] else "observe"
        hr = f"{100 * v['hit_rate']:.1f}%" if v["hit_rate"] is not None else "n/a"
        band = ""
        if v["wilson_low"] is not None:
            band = f"  Wilson[{100 * v['wilson_low']:.1f}-{100 * v['wilson_high']:.1f}]"
        lines.append(
            f"  {k:<28} {v['hits']}/{v['n']} = {hr}{band}  ({ready})"
        )
    lines.append("by attempt prop|pick:")
    for k, v in sorted(bt["by_attempt_prop_pick"].items()):
        ready = "n>=100" if v["gate_ready"] else "observe"
        hr = f"{100 * v['hit_rate']:.1f}%" if v["hit_rate"] is not None else "n/a"
        lines.append(f"  {k:<42} {v['hits']}/{v['n']} = {hr}  ({ready})")
    lines.append("")
    return "\n".join(lines) + "\n"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slate", default="2026-10-22")
    ap.add_argument(
        "--graded-dir",
        default=str(ROOT / "ui_runner" / "templates"),
    )
    ap.add_argument("--skip-backtest", action="store_true")
    args = ap.parse_args()
    slate = args.slate

    clear_nba_box_frame_cache()
    boxes = load_nba_box_frame()

    # Sanity: LeBron 2PA card window
    leb = card_l5_ok(
        boxes, "LeBron James", "Two Pointers Attempted", 8.5, "OVER", slate=slate
    )
    notes_pre: list[str] = []
    if leb:
        notes_pre.append(
            f"LeBron 2PA Demon O8.5 card={leb['card_vals']} "
            f"avg={leb['ticket_avg']:.2f} mix={leb['window_mix']} "
            f"pre={leb['pre_flag']}"
        )
    else:
        notes_pre.append("LeBron 2PA O8.5 did NOT clear card-window L5=5")

    all_clears, board_path, notes = scan_tonight(boxes, slate)
    notes = notes_pre + notes
    one = pick_one_per_player(all_clears)

    if args.skip_backtest:
        bt = {
            "n_files": 0,
            "n_checked": 0,
            "n_no_box": 0,
            "by_family_pick": {},
            "by_attempt_prop_pick": {},
            "lebron_attempt_l5eq5_samples": [],
        }
    else:
        bt = backtest(boxes, Path(args.graded_dir))

    text = render(slate, one, all_clears, notes, bt)
    out = ROOT / "data" / "reports" / f"nba_attempt_l5eq5_{slate}.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    sync = ROOT / "outputs" / slate / out.name
    sync.parent.mkdir(parents=True, exist_ok=True)
    sync.write_text(text, encoding="utf-8")
    bt_json = ROOT / "data" / "reports" / f"nba_attempt_l5eq5_{slate}.json"
    bt_json.write_text(
        json.dumps(
            {
                "slate": slate,
                "board": str(board_path),
                "tonight_one_per_player": one,
                "tonight_all_clears": all_clears,
                "backtest": bt,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(text)
    print(f"wrote {out}")
    print(f"synced {sync}")
    print(f"json {bt_json}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
