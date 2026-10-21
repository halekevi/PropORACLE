"""Rush Goblin OVER: is disagreement 14/16 vs agree 8/11 distinguishable from noise?"""
from __future__ import annotations

import json
import math
from collections import defaultdict
from pathlib import Path

import numpy as np
from scipy import stats

REP = Path(r"H:\PropORACLE\data\reports\cfb_nat_conf_disagreement.json")


def wilson(h: int, n: int, z: float = 1.96):
    if n == 0:
        return None, None, None
    p = h / n
    den = 1 + z * z / n
    center = (p + z * z / (2 * n)) / den
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return p, center - half, center + half


def report(label: str, h: int, n: int) -> None:
    p, lo, hi = wilson(h, n)
    print(f"{label}: {h}/{n} = {100 * p:.1f}%  Wilson95% [{100 * lo:.1f}, {100 * hi:.1f}]")


def main() -> None:
    print("=== Rush Yards Goblin OVER ===")
    report("agree", 8, 11)
    report("disagree", 14, 16)

    p1, p2 = 8 / 11, 14 / 16
    se = math.sqrt(p1 * (1 - p1) / 11 + p2 * (1 - p2) / 16)
    diff = p2 - p1
    print(f"diff (dis - agree) = {100 * diff:+.1f} pp  SE={100 * se:.1f}  z={diff / se:.2f}")
    print(
        f"approx 95% CI on diff: "
        f"[{100 * (diff - 1.96 * se):+.1f}, {100 * (diff + 1.96 * se):+.1f}] pp"
    )

    # Fisher: rows = disagree / agree, cols = hit / miss
    odds, p = stats.fisher_exact([[14, 2], [8, 3]])
    print(f"Fisher exact OR={odds:.2f} two-sided p={p:.3f}")
    chi2, p2c, dof, exp = stats.chi2_contingency([[14, 2], [8, 3]])
    print(f"Chi2 p={p2c:.3f} expected hits/misses dis={exp[0]} agree={exp[1]}")

    rng = np.random.default_rng(0)
    a = np.array([1] * 8 + [0] * 3)
    d = np.array([1] * 14 + [0] * 2)
    diffs = np.array(
        [
            rng.choice(d, 16, replace=True).mean() - rng.choice(a, 11, replace=True).mean()
            for _ in range(30000)
        ]
    )
    print(
        f"bootstrap 95% CI on diff: "
        f"[{100 * np.percentile(diffs, 2.5):+.1f}, {100 * np.percentile(diffs, 97.5):+.1f}] pp"
    )
    print(f"bootstrap P(diff <= 0) = {(diffs <= 0).mean():.3f}")
    print(f"Wilson intervals overlap? agree hi={100 * wilson(8, 11)[2]:.1f} "
          f"dis lo={100 * wilson(14, 16)[1]:.1f} -> "
          f"{'YES overlap' if wilson(8, 11)[2] >= wilson(14, 16)[1] else 'NO overlap'}")

    rep = json.loads(REP.read_text(encoding="utf-8"))
    chip = next(c for c in rep["chips"] if c["prop"] == "rush_yds" and c["book"] == "Goblin OVER")
    print("\n=== disagreement roster ===")
    by: dict[str, dict] = defaultdict(lambda: {"h": 0, "n": 0, "rows": []})
    for e in chip["dis_roster"]:
        by[e["dis"]]["n"] += 1
        by[e["dis"]]["h"] += e["hit"]
        by[e["dis"]]["rows"].append(e)
        mark = "HIT" if e["hit"] else "MISS"
        print(
            f"  {e['date']} {mark} {str(e['player'])[:22]:22} "
            f"Off {e['own_off']}  D {e['opp_def']}  [{e['dis']}]"
        )

    print("\n=== by disagreement shape ===")
    for k, v in sorted(by.items(), key=lambda x: -x[1]["n"]):
        print(f"  {k}: {v['h']}/{v['n']}")

    # Structural hypothesis pieces
    # oppD conf_stronger: nat weaker than conf = nationally mediocre, conference-strong D
    # oppD nat_stronger: nat looks stingier than conf says
    # ownOff nat_stronger: team looks better nationally than in-conference
    print("\n=== structural slices (opp D only) ===")
    for shape in ("oppD:conf_stronger", "oppD:nat_stronger"):
        rows = [e for e in chip["dis_roster"] if shape in e["dis"]]
        h = sum(e["hit"] for e in rows)
        print(f"  any with {shape}: {h}/{len(rows)}")
    # Pure oppD conf_stronger alone (no ownOff dis)
    pure = [e for e in chip["dis_roster"] if e["dis"] == "oppD:conf_stronger"]
    print(f"  pure oppD:conf_stronger: {sum(e['hit'] for e in pure)}/{len(pure)}")
    pure_nat = [e for e in chip["dis_roster"] if e["dis"] == "oppD:nat_stronger"]
    print(f"  pure oppD:nat_stronger: {sum(e['hit'] for e in pure_nat)}/{len(pure_nat)}")

    # Agree misses for context
    print("\n=== all misses on this chip ===")
    for e in chip["miss_roster"]:
        print(
            f"  {e['date']} MISS {str(e['player'])[:22]:22} "
            f"Off {e['own_off']}  D {e['opp_def']}  [{e['dis']}]"
        )


if __name__ == "__main__":
    main()
