"""Curate NFL best Standards/Goblins for 2026-09-20 (core markets)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, r"H:\PropORACLE")
from scripts.rank_best_props_today import bucket, load_nfl, recs  # noqa: E402

CORE_KEYS = (
    "passing_yard",
    "rushing_yard",
    "receiving_yard",
    "reception",
    "fantasy",
    "kicking",
    "tackle",
    "pass_completion",
    "rush_attempt",
    "passing_td",
)
BAD = ("first", "longest", "per_carry", "percentage", "combo", "yards_on")
BADGE_OK = {"Diamond", "Platinum", "Gold"}


def is_core(prop: str) -> bool:
    p = (prop or "").lower().replace(" ", "_")
    if any(b in p for b in BAD):
        return False
    return any(k in p for k in CORE_KEYS)


def side_of(r: dict) -> str:
    return str(r.get("side") or r.get("model_dir") or "").upper()


def l5_str(r: dict) -> str:
    return f"{r.get('l5_over')}/{r.get('l5_under')}"


def dedupe(lst: list[dict], keyfn) -> list[dict]:
    seen: set = set()
    out: list[dict] = []
    for r in lst:
        k = keyfn(r)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def fmt(r: dict) -> str:
    side = side_of(r)
    letter = "O" if side == "OVER" else "U" if side == "UNDER" else "?"
    return (
        f"  {r.get('prop_tier', '?')}/{str(r.get('badge') or '—'):10} "
        f"{str(r.get('player', '')):24} {str(r.get('prop', '')):22} "
        f"{letter}{r.get('line')}  L5 {l5_str(r)}  "
        f"cov {r.get('cover')}  {r.get('def')}  {r.get('matchup')}"
    )


def show(title: str, lst: list[dict], n: int = 20, goblin_dedupe: bool = False, hot_only: bool = True) -> None:
    print(f"\n=== {title} ===")
    core = [r for r in lst if is_core(str(r.get("prop") or ""))]
    if hot_only:
        core = [r for r in core if str(r.get("badge") or "") in BADGE_OK]
    if goblin_dedupe:
        def sort_key(r: dict):
            hits = int(r.get("l5_over") or 0)
            return (-hits, -float(r.get("line") or 0))

        core = sorted(core, key=sort_key)
        core = dedupe(core, lambda r: (r.get("player"), str(r.get("prop") or "").lower()))
    for r in core[:n]:
        print(fmt(r))
    print(f"  shown={min(n, len(core))}  core_hot={len(core)}  gated_all={len(lst)}")


def main() -> None:
    root = Path(r"H:\PropORACLE_main_cp")
    df = load_nfl(root, "2026-09-20")
    so, su, gob = bucket(recs(df), "NFL")
    print("NFL 2026-09-20 best props (core markets; Platinum/Gold only)")
    print("Board: PropORACLE_main_cp day-ahead fetch ~2026-09-19 01:16 ET")
    show("Standard OVER", so, 20)
    show("Standard UNDER", su, 20)
    show("Goblin OVER (deduped hardest line)", gob, 30, goblin_dedupe=True)


if __name__ == "__main__":
    main()
