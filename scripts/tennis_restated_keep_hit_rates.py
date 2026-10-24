#!/usr/bin/env python3
"""Compute fixed thresholds + restated keep hit rates for tennis factor gates."""
from __future__ import annotations

import json
import re
import sys
from datetime import date
from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(_REPO), str(_REPO / "Sports" / "Tennis" / "scripts"), str(_REPO / "scripts")]

from tennis_shared import (  # noqa: E402
    build_sackmann_player_index,
    ensure_sackmann_matches,
    norm_key,
    norm_tennis_prop,
    parse_sackmann_tourney_date,
)

OUT = _REPO / "data" / "reports" / "tennis_keep_gates_restated.json"
GRADED_DIRS = [
    MAIN / "ui_runner" / "templates",
    MAIN / "mobile" / "www",
    _REPO / "ui_runner" / "templates",
    _REPO / "mobile" / "www",
]


def _finite(v: object) -> float | None:
    if v is None or v == "":
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return None if f != f else f


def _mean(xs: list[float]) -> float | None:
    return sum(xs) / len(xs) if xs else None


def _pct(h: int, n: int) -> float | None:
    return round(100.0 * h / n, 1) if n else None


def _book(row: dict) -> str | None:
    pt = str(row.get("pick_type") or row.get("book") or "").strip().lower()
    d = str(row.get("direction") or row.get("over_under") or "").strip().upper()
    if "demon" in pt:
        return None
    if "goblin" in pt:
        return f"Goblin {d if d in ('OVER', 'UNDER') else 'OVER'}"
    if d in ("OVER", "UNDER"):
        return f"Standard {d}"
    return None


def _iter_props(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)]
    if not isinstance(data, dict):
        return []
    for key in ("props", "legs", "rows", "graded", "items"):
        v = data.get(key)
        if isinstance(v, list) and v and isinstance(v[0], dict):
            return v
    for v in data.values():
        if isinstance(v, list) and v and isinstance(v[0], dict):
            if "player" in v[0] or "prop" in v[0] or "sport" in v[0]:
                return v
    return []


def _l5(row: dict, side: str) -> float | None:
    keys = (
        ("l5_over", "last5_over", "L5 Over", "line_hits_over_5")
        if side == "OVER"
        else ("l5_under", "last5_under", "L5 Under", "line_hits_under_5")
    )
    for k in keys:
        v = _finite(row.get(k))
        if v is not None and v <= 10:
            return v
    return None


def _l10(row: dict, side: str) -> float | None:
    keys = (
        ("l10_over", "last10_over", "L10 Over", "line_hits_over_10")
        if side == "OVER"
        else ("l10_under", "last10_under", "L10 Under", "line_hits_under_10")
    )
    for k in keys:
        v = _finite(row.get(k))
        if v is not None and v <= 10:
            return v
    return None


def _std_discount(row: dict, side: str) -> float | None:
    line = _finite(row.get("line"))
    std = None
    for k in ("standard_line", "Standard Line", "std_line"):
        std = _finite(row.get(k))
        if std is not None:
            break
    if line is None or std is None:
        return None
    return (line - std) if side == "UNDER" else (std - line)


def _l5_gap(row: dict) -> float | None:
    for k in ("dist_l5", "Dist_L5"):
        v = _finite(row.get(k))
        if v is not None:
            return v
    avg = None
    for k in ("stat_last5_avg", "Last 5 Avg", "last5_avg", "l5_avg"):
        avg = _finite(row.get(k))
        if avg is not None:
            break
    line = _finite(row.get("line"))
    if avg is None or line is None:
        return None
    return avg - line


def main() -> int:
    print("Loading Sackmann…")
    matches = ensure_sackmann_matches()
    index = build_sackmann_player_index(matches)

    hand_by: dict[str, str] = {}
    for _, rd in matches.iterrows():
        for name_col, hand_col in (("winner_name", "winner_hand"), ("loser_name", "loser_hand")):
            nm = norm_key(str(rd.get(name_col) or ""))
            hd = str(rd.get(hand_col) or "").strip().upper()
            if nm and hd in {"L", "R", "U"}:
                hand_by[nm] = hd
    print(f"  hands={len(hand_by)} lefties={sum(1 for v in hand_by.values() if v == 'L')}")

    seen: set[str] = set()
    legs: list[dict] = []
    paths: list[Path] = []
    seen_names: set[str] = set()
    for d in GRADED_DIRS:
        if not d.exists():
            continue
        for p in sorted(d.glob("graded_props_20*.json")):
            if p.name in seen_names:
                continue
            seen_names.add(p.name)
            paths.append(p)

    for path in paths:
        rows = _iter_props(path)
        m = re.search(r"(\d{4}-\d{2}-\d{2})", path.name)
        if not m:
            continue
        slate = date.fromisoformat(m.group(1))
        for row in rows:
            if str(row.get("sport") or "").strip().lower() != "tennis":
                continue
            result = str(row.get("result") or "").strip().upper()
            if result not in ("HIT", "MISS"):
                continue
            book = _book(row)
            if not book:
                continue
            player = str(row.get("player") or "").strip()
            prop_raw = str(row.get("prop") or row.get("prop_type") or "").strip()
            if not player or not prop_raw:
                continue
            pnorm = norm_tennis_prop(prop_raw)
            side = "OVER" if "OVER" in book else "UNDER"
            line = _finite(row.get("line"))
            pk = norm_key(player)
            dkey = f"{slate}|{pk}|{pnorm}|{book}|{line}|{result}"
            if dkey in seen:
                continue
            seen.add(dkey)

            prior: list[dict] = []
            for rec in index.get(pk) or []:
                dt = parse_sackmann_tourney_date(rec.get("date"))
                if dt and dt < slate:
                    prior.append(rec)

            sw_vals = [_finite(r.get("second_won_pct")) for r in prior[:5]]
            sw = _mean([v for v in sw_vals if v is not None])

            # Prefer today's opponent hand; fall back to last-match opp_hand.
            opp_name = norm_key(str(row.get("opp_team") or row.get("opponent") or row.get("opp") or ""))
            today_hand = hand_by.get(opp_name) if opp_name else None
            last_hand = str((prior[0].get("opp_hand") if prior else "") or "").strip().upper() or None
            if last_hand and last_hand not in {"L", "R", "U"}:
                last_hand = None
            opp_hand = today_hand or last_hand
            lefty = True if opp_hand == "L" else (False if opp_hand in {"R", "U"} else None)

            legs.append(
                {
                    "book": book,
                    "prop": pnorm,
                    "hit": 1 if result == "HIT" else 0,
                    "l5": _l5(row, side),
                    "l10": _l10(row, side),
                    "std_disc": _std_discount(row, side),
                    "l5_gap": _l5_gap(row),
                    "sw": sw,
                    "lefty": lefty,
                    "opp_hand_src": "today" if today_hand else ("last" if last_hand else None),
                }
            )

    print(f"legs={len(legs)}")

    # Threshold search on Goblin OVER Games Won L10>=8
    gw = [l for l in legs if l["book"] == "Goblin OVER" and l["prop"] == "games_won" and l["l10"] is not None and l["l10"] >= 8]
    sws = sorted(l["sw"] for l in gw if l["sw"] is not None)
    med = sws[len(sws) // 2] if sws else 52.0
    print(f"Goblin GW L10>=8 n={len(gw)} median_sw={med:.2f}")

    thr_rows = []
    for thr in [48, 50, 51, 52, 53, 54, 55, 56, round(med, 1)]:
        sub = [l for l in gw if l["sw"] is not None and l["sw"] >= thr]
        if len(sub) < 25:
            continue
        thr_rows.append({"thr": thr, "n": len(sub), "hit_pct": _pct(sum(l["hit"] for l in sub), len(sub))})
        print(f"  sw>={thr}: n={len(sub)} hit={thr_rows[-1]['hit_pct']}%")

    # Pick thr nearest median with n>=100 and hit>=82 if possible, else median
    SW_THR = 52.0
    for cand in sorted(thr_rows, key=lambda r: (-(r["hit_pct"] or 0), -r["n"])):
        if cand["n"] >= 100 and (cand["hit_pct"] or 0) >= 82:
            SW_THR = float(cand["thr"])
            break
    else:
        SW_THR = float(round(med, 1))
    print(f"SELECTED SW_THR={SW_THR}")

    def rate(label: str, subset: list[dict]) -> dict:
        n = len(subset)
        h = sum(l["hit"] for l in subset)
        return {"label": label, "n": n, "hits": h, "hit_pct": _pct(h, n)}

    keeps = []

    # --- Existing keeps ---
    # Goblin GW std discount — only rows with std_line on graded file (often missing)
    gw_all = [l for l in legs if l["book"] == "Goblin OVER" and l["prop"] == "games_won"]
    keeps.append(rate("Goblin OVER Games Won · Std−Goblin≥4", [l for l in gw_all if l["std_disc"] is not None and l["std_disc"] >= 4]))
    keeps.append(rate("Goblin OVER Games Won · L10≥8 + vs lefty", [l for l in gw if l["lefty"] is True]))
    keeps.append(rate(f"Goblin OVER Games Won · L10≥8 + L5 2nd-won%≥{SW_THR:g}", [l for l in gw if l["sw"] is not None and l["sw"] >= SW_THR]))
    keeps.append(
        rate(
            f"Goblin OVER Games Won · L10≥8 + (lefty OR 2nd-won≥{SW_THR:g})",
            [l for l in gw if l["lefty"] is True or (l["sw"] is not None and l["sw"] >= SW_THR)],
        )
    )
    keeps.append(
        rate(
            f"Goblin OVER Games Won · KEEP union (gap≥4 OR L10≥8+(lefty|2nd-won≥{SW_THR:g}))",
            [
                l
                for l in gw_all
                if (l["std_disc"] is not None and l["std_disc"] >= 4)
                or (
                    l["l10"] is not None
                    and l["l10"] >= 8
                    and (l["lefty"] is True or (l["sw"] is not None and l["sw"] >= SW_THR))
                )
            ],
        )
    )

    tg = [l for l in legs if l["book"] == "Goblin OVER" and l["prop"] == "match_total_games"]
    tg_l10 = [l for l in tg if l["l10"] is not None and l["l10"] >= 8]
    keeps.append(
        rate(
            "Goblin OVER Total Games · L5≥4 + L10≥8",
            [l for l in tg_l10 if l["l5"] is not None and l["l5"] >= 4],
        )
    )
    keeps.append(rate("Goblin OVER Total Games · L10≥8 + vs lefty", [l for l in tg_l10 if l["lefty"] is True]))
    keeps.append(
        rate(
            "Goblin OVER Total Games · KEEP union (L5≥4+L10≥8 OR L10≥8+lefty)",
            [
                l
                for l in tg_l10
                if (l["l5"] is not None and l["l5"] >= 4) or l["lefty"] is True
            ],
        )
    )

    # Std UNDER serve
    for prop, label in (("aces", "Aces"), ("double_faults", "Double Faults")):
        sub = [l for l in legs if l["book"] == "Standard UNDER" and l["prop"] == prop]
        keeps.append(rate(f"Standard UNDER {label} · ungated", sub))

    # Std OVER Games Won existing + new
    so_gw = [l for l in legs if l["book"] == "Standard OVER" and l["prop"] == "games_won"]
    so_l10 = [l for l in so_gw if l["l10"] is not None and l["l10"] >= 8]
    keeps.append(
        rate(
            "Standard OVER Games Won · L10≥8 + L5gap≥5",
            [l for l in so_l10 if l["l5_gap"] is not None and l["l5_gap"] >= 5],
        )
    )
    keeps.append(
        rate(
            f"Standard OVER Games Won · L10≥8 + L5 2nd-won%≥{SW_THR:g}",
            [l for l in so_l10 if l["sw"] is not None and l["sw"] >= SW_THR],
        )
    )
    keeps.append(rate("Standard OVER Games Won · L10≥8 + vs lefty", [l for l in so_l10 if l["lefty"] is True]))

    # Fade baselines for restatement table
    fades = []
    for book, prop, label in (
        ("Goblin OVER", "aces", "Goblin OVER Aces · ungated"),
        ("Goblin OVER", "double_faults", "Goblin OVER Double Faults · ungated"),
        ("Standard OVER", "aces", "Standard OVER Aces · ungated"),
        ("Standard OVER", "double_faults", "Standard OVER Double Faults · ungated"),
        ("Standard OVER", "match_total_games", "Standard OVER Total Games · ungated"),
        ("Standard UNDER", "games_won", "Standard UNDER Games Won · ungated"),
        ("Standard UNDER", "match_total_games", "Standard UNDER Total Games · ungated"),
        ("Goblin OVER", "games_won", "Goblin OVER Games Won · ungated"),
        ("Goblin OVER", "match_total_games", "Goblin OVER Total Games · ungated"),
    ):
        sub = [l for l in legs if l["book"] == book and l["prop"] == prop]
        fades.append(rate(label, sub))

    # Coverage of lefty source
    lefty_src = {"today": 0, "last": 0, "none": 0}
    for l in gw:
        if l["lefty"] is None:
            lefty_src["none"] += 1
        else:
            # recount from legs - we didn't store src on final; approximate via hand
            lefty_src["last"] += 1  # placeholder fixed below

    lefty_src = {"today": 0, "last": 0, "none": 0}
    for l in legs:
        if l["book"] != "Goblin OVER" or l["prop"] != "games_won":
            continue
        if l["l10"] is None or l["l10"] < 8:
            continue
        src = l.get("opp_hand_src")
        if src == "today":
            lefty_src["today"] += 1
        elif src == "last":
            lefty_src["last"] += 1
        else:
            lefty_src["none"] += 1

    report = {
        "generated": date.today().isoformat(),
        "legs": len(legs),
        "sw_thr": SW_THR,
        "sw_thr_search": thr_rows,
        "lefty_src_goblin_gw_l10": lefty_src,
        "keeps": keeps,
        "baselines": fades,
        "notes": [
            "opp_lefty prefers today's opp_team hand from Sackmann; falls back to last-match opp_hand.",
            "L5 2nd-won% is mean of player's prior 5 Sackmann second_won_pct.",
            "Std−Goblin gap often missing on graded_props (no standard_line) — gap keep rate here is undercounted vs catalog join.",
        ],
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(f"\nWrote {OUT}")
    print("\n=== KEEP restated ===")
    for r in keeps:
        print(f"  {r['hit_pct']}%  n={r['n']:4}  {r['label']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
