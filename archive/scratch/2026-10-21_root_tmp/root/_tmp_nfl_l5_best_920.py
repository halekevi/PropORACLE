"""NFL 2026-09-20 best props by directional L5%, core markets, diverse players."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, r"H:\PropORACLE")
from scripts.rank_best_props_today import bucket, load_nfl, recs  # noqa: E402

CORE = (
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


def is_core(prop: str) -> bool:
    p = (prop or "").lower().replace(" ", "_")
    if any(b in p for b in BAD):
        return False
    return any(k in p for k in CORE)


def l5_hits(r: dict) -> int:
    side = str(r.get("side") or "").upper()
    if side == "UNDER":
        return int(r.get("l5_under") or 0)
    return int(r.get("l5_over") or 0)


def l5_pct(r: dict) -> float:
    return l5_hits(r) / 5.0


def fmt(r: dict) -> str:
    side = str(r.get("side") or "").upper()
    letter = "O" if side == "OVER" else "U"
    badge = r.get("promo") or r.get("badge") or "—"
    return (
        f"  {r.get('prop_tier','?')}/{str(badge):10} "
        f"{str(r.get('player','')):26} {str(r.get('prop','')):22} "
        f"{letter}{r.get('line')}  L5 {r.get('l5_over')}/{r.get('l5_under')} "
        f"({l5_pct(r):.0%})  cov {r.get('cover')}  D={r.get('def')}  "
        f"{r.get('matchup')}"
        f"{('  miss '+r['miss_s']) if r.get('miss_s') else ''}"
    )


def dedupe(rows: list[dict], n: int, *, hardest_goblin: bool = False) -> list[dict]:
    """One row per player; for goblins keep highest line among L5=5 first."""
    rows = [r for r in rows if is_core(str(r.get("prop") or ""))]
    if hardest_goblin:
        rows = sorted(
            rows,
            key=lambda r: (-l5_hits(r), -float(r.get("line") or 0), -float(r.get("cover") or 0)),
        )
    else:
        rows = sorted(
            rows,
            key=lambda r: (
                -l5_hits(r),
                -float(r.get("cover") or 0) if str(r.get("side")) == "OVER" else float(r.get("cover") or 0),
            ),
        )
    seen: set[str] = set()
    out: list[dict] = []
    for r in rows:
        pk = str(r.get("player") or "").strip().lower()
        if not pk or pk in seen:
            continue
        # skip projection-hole completions (szn/cover weirdness already known)
        prop = str(r.get("prop") or "").lower()
        if "completion" in prop and str(r.get("side")) == "UNDER":
            continue
        seen.add(pk)
        out.append(r)
        if len(out) >= n:
            break
    return out


def main() -> None:
    root = Path(r"H:\PropORACLE_main_cp")
    df = load_nfl(root, "2026-09-20")
    so, su, gob = bucket(recs(df), "NFL")
    print("NFL 2026-09-20 — ranked by directional L5%")
    print(
        "CAVEAT: L5 still pads 2025 regular-season games (Week 2). "
        "Treat as L5% screen, not current-season verified.\n"
    )

    # L5=5 only first (100%)
    so5 = [r for r in so if l5_hits(r) >= 5]
    su5 = [r for r in su if l5_hits(r) >= 5]
    gob5 = [r for r in gob if l5_hits(r) >= 5]
    so4 = [r for r in so if l5_hits(r) == 4]
    su4 = [r for r in su if l5_hits(r) == 4]
    gob4 = [r for r in gob if l5_hits(r) == 4]

    print(f"===== Goblin OVER L5=5/5 (100%) core unique  n_raw={len(gob5)} =====")
    for r in dedupe(gob5, 25, hardest_goblin=True):
        print(fmt(r))

    print(f"\n===== Standard OVER L5=5/5 core unique  n_raw={len(so5)} =====")
    for r in dedupe(so5, 15):
        print(fmt(r))

    print(f"\n===== Standard UNDER L5=5/5 core unique  n_raw={len(su5)} =====")
    for r in dedupe(su5, 15):
        print(fmt(r))

    print(f"\n===== Goblin OVER L5=4/5 (80%) top fills  n_raw={len(gob4)} =====")
    for r in dedupe(gob4, 12, hardest_goblin=True):
        print(fmt(r))

    print(f"\n===== Standard OVER L5=4/5  n_raw={len(so4)} =====")
    for r in dedupe(so4, 10):
        print(fmt(r))

    print(f"\n===== Standard UNDER L5=4/5 (skip completions)  n_raw={len(su4)} =====")
    for r in dedupe(su4, 10):
        print(fmt(r))


if __name__ == "__main__":
    main()
