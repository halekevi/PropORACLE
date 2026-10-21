"""CFB Standards only for 2026-09-19 (post season-cutoff board)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, r"H:\PropORACLE")
from scripts.rank_best_props_today import bucket, load_cfb, recs  # noqa: E402

CORE_KEYS = (
    "pass_yard",
    "passing_yard",
    "rush_yard",
    "rushing_yard",
    "rec_yard",
    "receiving_yard",
    "reception",
    "pass_attempt",
    "pass_completion",
    "rush_attempt",
    "fantasy",
    "kicking",
    "anytime",
    "player_td",
    "pass_td",
    "rush_td",
)
BAD = ("first", "longest", "per_carry", "percentage", "combo", "yards_on")


def is_core(prop: str) -> bool:
    p = (prop or "").lower().replace(" ", "_")
    if any(b in p for b in BAD):
        return False
    return any(k in p for k in CORE_KEYS)


def side_of(r: dict) -> str:
    return str(r.get("side") or r.get("model_dir") or "").upper()


def fmt(r: dict) -> str:
    side = side_of(r)
    letter = "O" if side == "OVER" else "U" if side == "UNDER" else "?"
    badge = r.get("promo") or r.get("badge") or "—"
    return (
        f"  {r.get('prop_tier', '?')}/{str(badge):10} "
        f"{str(r.get('player', '')):24} {str(r.get('prop', '')):22} "
        f"{letter}{r.get('line')}  L5 {r.get('l5_over')}/{r.get('l5_under')}  "
        f"cov {r.get('cover')}  {r.get('def')}  {r.get('matchup')}"
        f"{('  miss ' + r['miss_s']) if r.get('miss_s') else ''}"
    )


def main() -> None:
    root = Path(r"H:\PropORACLE_main_cp")
    df = load_cfb(root, "2026-09-19")
    print("CFB rows", len(df), "root", root)
    so, su, gob = bucket(recs(df), "CFB")
    print("\n===== CFB Standard OVER (core) =====")
    core_o = [r for r in so if is_core(str(r.get("prop") or ""))]
    for r in core_o[:30]:
        print(fmt(r))
    print(f"  core={len(core_o)} gated={len(so)}")
    print("\n===== CFB Standard UNDER (core) =====")
    core_u = [r for r in su if is_core(str(r.get("prop") or ""))]
    for r in core_u[:30]:
        print(fmt(r))
    print(f"  core={len(core_u)} gated={len(su)}")
    print("\n(Goblin count gated, not listed):", len(gob))


if __name__ == "__main__":
    main()
