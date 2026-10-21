"""Rush Yards Standard UNDER: joint Off x D 2x2 + rush-share overlay.

Unique L5=5 only. Observe-only. Does not change keep-gates.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import _tmp_cfb_l5_recut as R  # noqa: E402
from _tmp_cfb_od_lift import collect_rows, pack  # noqa: E402

VOL = ROOT / "Sports" / "CFB" / "data" / "reference" / "cfb_player_volume.csv"
PART = ROOT / "Sports" / "CFB" / "data" / "cfb_route_participation.csv"
OUT = ROOT / "data" / "reports" / "cfb_rush_under_joint.json"

# Same floors as the earlier inverted-share audit.
SHARE_BUCKETS = [
    ("<25%", 0.0, 0.25),
    ("25-50%", 0.25, 0.50),
    ("50%+", 0.50, 1.01),
]


def to_frac(v) -> float | None:
    n = R.num(v)
    if n is None:
        return None
    try:
        x = float(n)
    except (TypeError, ValueError):
        return None
    if x != x:  # NaN
        return None
    if x > 1.5:
        return x / 100.0
    return x


def share_bucket(frac: float | None) -> str:
    if frac is None:
        return "blank"
    for lab, lo, hi in SHARE_BUCKETS:
        if lo <= frac < hi:
            return lab
    return "blank"


def load_vol() -> dict[tuple[str, str], float]:
    """(player_norm, season) -> rush_att_share as 0-1."""
    df = pd.read_csv(VOL)
    out: dict[tuple[str, str], float] = {}
    for rec in df.to_dict("records"):
        pn = R.norm_player(rec.get("player_norm"))
        if not pn:
            continue
        seas = str(rec.get("season") or "").strip()
        frac = to_frac(rec.get("rush_att_share"))
        if frac is None:
            continue
        key = (pn, seas)
        games = R.num(rec.get("volume_games")) or 0
        prev = out.get(key)
        if prev is None or games >= 0:
            out[key] = frac
    return out


def load_part() -> dict[str, float]:
    if not PART.exists():
        return {}
    df = pd.read_csv(PART)
    out: dict[str, float] = {}
    for rec in df.to_dict("records"):
        pn = R.norm_player(rec.get("player_norm") or rec.get("player"))
        frac = to_frac(rec.get("rush_att_share"))
        if pn and frac is not None:
            out[pn] = frac
    return out


def collapse_slate_dupes(pool: list[dict]) -> tuple[list[dict], int]:
    """Same player+line on adjacent slate stamps is one game, not two."""
    from collections import defaultdict

    groups: dict[tuple, list] = defaultdict(list)
    for r in pool:
        groups[(r["player_norm"], r["prop"], r["book"], r["side"])].append(r)
    out: list[dict] = []
    dropped = 0
    for g in groups.values():
        g = sorted(g, key=lambda r: r["date"])
        kept: list[dict] = []
        for r in g:
            if kept and abs((r.get("line") or 0) - (kept[-1].get("line") or 0)) <= 1.0:
                dropped += 1
                prev = kept[-1]
                r_has = r.get("share") is not None
                p_has = prev.get("share") is not None
                if r_has and not p_has:
                    kept[-1] = r
                elif r["date"] >= prev["date"] and (r_has or not p_has):
                    kept[-1] = r
            else:
                kept.append(r)
        out.extend(kept)
    return out, dropped


def attach_share(rows: list[dict]) -> list[dict]:
    vol = load_vol()
    part = load_part()
    for r in rows:
        pn = r["player_norm"]
        s8 = to_frac(r.get("rush_share_step8"))
        y26 = vol.get((pn, "2026"))
        y25 = vol.get((pn, "2025"))
        # Prefer as-of-slate step8, then 2025 season mean (the prior audit),
        # then route-participation file, then thin 2026 mean.
        chosen = s8 if s8 is not None else (y25 if y25 is not None else part.get(pn, y26))
        r["share_s8"] = s8
        r["share_2025"] = y25
        r["share_2026"] = y26
        r["share"] = chosen
        r["share_src"] = (
            "step8"
            if s8 is not None
            else (
                "vol_2025"
                if y25 is not None
                else ("part" if pn in part else ("vol_2026" if y26 is not None else "blank"))
            )
        )
        r["share_b"] = share_bucket(chosen)
    return rows


def cell(pool, pred) -> dict:
    sub = [r for r in pool if pred(r)]
    return pack("", sub)


def hn(d: dict) -> str:
    if not d["n"]:
        return "empty"
    return f"{d['hr']:.1f}% {d['h']}/{d['n']}"


def main() -> None:
    unique, n_fbs = collect_rows()
    pool = [
        r
        for r in unique
        if r["prop"] == "rush_yds" and r["book"] == "Standard UNDER" and r["l5"] == 5
    ]
    attach_share(pool)
    pool, n_dup = collapse_slate_dupes(pool)
    print(f"Rush Yards Std UNDER L5=5 unique n={len(pool)} (dropped {n_dup} slate-stamp dupes) FBS={n_fbs}")
    print(
        "share filled",
        sum(1 for r in pool if r["share"] is not None),
        "src",
        {s: sum(1 for r in pool if r["share_src"] == s) for s in ("step8", "vol_2025", "part", "vol_2026", "blank")},
    )

    # --- true 2x2 (Avg dropped from the joint cells) ---
    print("\n==== JOINT 2x2 own Off x opp D (Avg dropped) ====")
    print("UNDER wants weak own Off x stingy opp D")
    joints = []
    for off in ("strong", "weak"):
        for d in ("weak", "strong"):
            sub = [r for r in pool if r["own_off_b"] == off and r["opp_def_b"] == d]
            dlab = "leaky" if d == "weak" else "stingy"
            lab = f"own Off {off} x opp D {dlab}"
            p = pack(lab, sub)
            joints.append(p)
            tag = ""
            if off == "weak" and d == "strong":
                tag = "  << aligned UNDER"
            elif off == "strong" and d == "weak":
                tag = "  << opposite UNDER"
            print(f"  {lab:40} {hn(p):16}{tag}")
    leftover = [
        r
        for r in pool
        if r["own_off_b"] not in {"strong", "weak"} or r["opp_def_b"] not in {"strong", "weak"}
    ]
    left = pack("Avg leftover (Off or D is Avg/blank)", leftover)
    print(f"  {left['label']:40} {hn(left)}")

    print("\n==== 3x3 (mass not hidden) ====")
    grid = []
    for off in ("strong", "avg", "weak", "blank"):
        for d in ("weak", "avg", "strong", "blank"):
            sub = [r for r in pool if r["own_off_b"] == off and r["opp_def_b"] == d]
            if not sub and off == "blank":
                continue
            p = pack(f"Off {off} x D {d}", sub)
            grid.append(p)
            if sub:
                print(f"  Off {off:6} x D {d:6}  {hn(p)}")

    # --- share overlay ---
    print("\n==== rush_att_share buckets on L5=5 (third cut) ====")
    share_rows = []
    for lab, lo, hi in SHARE_BUCKETS:
        p = pack(lab, [r for r in pool if r["share_b"] == lab])
        share_rows.append(p)
        print(f"  {lab:10} {hn(p)}")
    blank_s = pack("share blank", [r for r in pool if r["share_b"] == "blank"])
    print(f"  {'blank':10} {hn(blank_s)}")

    print("\n==== share x opp D (joint, Avg D dropped) ====")
    share_d = []
    for slab, _, _ in SHARE_BUCKETS:
        for d, dlab in (("weak", "leaky"), ("strong", "stingy")):
            sub = [r for r in pool if r["share_b"] == slab and r["opp_def_b"] == d]
            p = pack(f"{slab} x D {dlab}", sub)
            share_d.append(p)
            print(f"  {p['label']:28} {hn(p)}")

    print("\n==== share on D-aligned only (stingy opp D, the third cut) ====")
    d_ok = [r for r in pool if r["opp_def_b"] == "strong"]
    print(f"  stingy D pool {hn(pack('stingy D', d_ok))}")
    share_on_d = []
    for lab, _, _ in SHARE_BUCKETS:
        p = pack(f"stingy D + {lab}", [r for r in d_ok if r["share_b"] == lab])
        share_on_d.append(p)
        print(f"  {p['label']:28} {hn(p)}")

    print("\n==== share on Off x D aligned cell (weak Off x stingy D) ====")
    both = [r for r in pool if r["own_off_b"] == "weak" and r["opp_def_b"] == "strong"]
    print(f"  aligned cell {hn(pack('aligned', both))}")
    share_on_both = []
    for lab, _, _ in SHARE_BUCKETS:
        p = pack(f"aligned + {lab}", [r for r in both if r["share_b"] == lab])
        share_on_both.append(p)
        print(f"  {p['label']:28} {hn(p)}")

    print("\n==== roster (unique L5=5) ====")
    roster = []
    for r in sorted(pool, key=lambda x: (x["date"], str(x.get("player") or x["player_norm"]))):
        rec = {
            "date": r["date"],
            "player": r.get("player") or r["player_norm"],
            "team": r.get("team"),
            "opp": r.get("opp"),
            "line": r.get("line"),
            "hit": r["hit"],
            "own_off": r["own_off"],
            "own_off_b": r["own_off_b"],
            "opp_def": r["opp_def"],
            "opp_def_b": r["opp_def_b"],
            "board_d": r.get("board_d"),
            "share": None if r["share"] is None else round(100 * r["share"], 1),
            "share_b": r["share_b"],
            "share_src": r["share_src"],
            "l10": r.get("l10"),
        }
        roster.append(rec)
        mark = "HIT" if r["hit"] else "MISS"
        sh = "—" if rec["share"] is None else f"{rec['share']:.1f}%"
        print(
            f"  {r['date']} {mark:4} {(rec['player'] or '')[:22]:22} "
            f"line={r.get('line')} Off={r['own_off_b']:6} D={r['opp_def_b']:6} "
            f"share={sh:7} {r['share_src']}"
        )

    report = {
        "chip": "rush_yds Standard UNDER",
        "n_l5eq5_unique": len(pool),
        "baseline": pack("L5=5", pool),
        "joint_2x2": joints,
        "avg_leftover": left,
        "grid_3x3": grid,
        "share_buckets": share_rows,
        "share_x_d": share_d,
        "share_on_stingy_d": share_on_d,
        "share_on_aligned_cell": share_on_both,
        "roster": roster,
    }
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("wrote", OUT)


if __name__ == "__main__":
    main()
