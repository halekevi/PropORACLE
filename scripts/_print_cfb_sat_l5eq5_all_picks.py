"""CFB Saturday L5=5 list — Goblin, Standard, Demon (all props, no keep gate).

Uses step5b (L5 windows already attached). Filters start_time to slate date.

  py -3.14 scripts/_print_cfb_sat_l5eq5_all_picks.py
  py -3.14 scripts/_print_cfb_sat_l5eq5_all_picks.py 2026-10-24
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]


def _vals(row: pd.Series) -> list[float]:
    out: list[float] = []
    for i in range(1, 6):
        v = pd.to_numeric(row.get(f"stat_g{i}"), errors="coerce")
        if pd.isna(v):
            break
        out.append(float(v))
    return out


def _avg(vals: list[float]) -> float | None:
    return (sum(vals) / len(vals)) if vals else None


def main() -> int:
    slate = sys.argv[1] if len(sys.argv) > 1 else "2026-10-24"
    path = ROOT / "outputs" / slate / "cfb" / "step5b_cfb.csv"
    if not path.exists():
        print(f"missing {path}", file=sys.stderr)
        return 1

    df = pd.read_csv(path, low_memory=False)
    tip = df[df["start_time"].astype(str).str.startswith(slate)].copy()
    lines: list[str] = []
    lines.append(f"CFB L5=5 ALL picks  slate={slate}")
    lines.append(f"source={path.relative_to(ROOT)}  tip_rows={len(tip)}")
    lines.append("Includes Goblin / Standard / Demon. No keep-gate filter.")
    lines.append("Goblin+Demon = OVER only when L5 Over=5. Standard OVER/UNDER when directional L5=5.")
    lines.append("")

    buckets: dict[str, list[dict]] = {
        "Goblin OVER": [],
        "Demon OVER": [],
        "Standard OVER": [],
        "Standard UNDER": [],
    }

    for _, r in tip.iterrows():
        pt = str(r.get("pick_type") or r.get("odds_type") or "").strip().lower()
        if pt not in {"goblin", "demon", "standard"}:
            continue
        prop = str(r.get("prop_type") or r.get("stat_type") or "").strip()
        if not prop:
            continue
        try:
            line = float(r.get("line"))
        except (TypeError, ValueError):
            continue
        l5o = pd.to_numeric(r.get("l5_over"), errors="coerce")
        l5u = pd.to_numeric(r.get("l5_under"), errors="coerce")
        vals = _vals(r)
        if len(vals) < 5:
            continue
        # All-zero windows are missing-box artifacts, not real L5=5.
        if all(v == 0 for v in vals):
            continue
        avg = _avg(vals)
        player = str(r.get("player") or "").strip()
        if not player or "+" in player:
            continue
        team = str(r.get("team_abbr") or r.get("team") or "")
        opp = str(r.get("opp_team_abbr") or r.get("pp_opp_team") or "")
        flag = str(r.get("season_l5_flag") or "") or "-"
        tip_s = str(r.get("start_time") or "")[:16].replace("T", " ")
        def_tier = str(
            r.get("opp_def_tier")
            or r.get("OPP_OVERALL_DEF_TIER")
            or r.get("opp_overall_def_tier_nat")
            or ""
        )

        def add(bucket: str, side: str) -> None:
            ratio = (line / avg) if avg and avg > 0 else None
            buckets[bucket].append(
                {
                    "player": player,
                    "team": team,
                    "opp": opp,
                    "prop": prop,
                    "side": side,
                    "line": line,
                    "vals": vals,
                    "avg": avg,
                    "ratio": ratio,
                    "flag": flag,
                    "def_tier": def_tier,
                    "tip": tip_s,
                }
            )

        if pt == "goblin" and float(l5o) == 5.0:
            add("Goblin OVER", "OVER")
        elif pt == "demon" and float(l5o) == 5.0:
            add("Demon OVER", "OVER")
        elif pt == "standard":
            if float(l5o) == 5.0:
                add("Standard OVER", "OVER")
            if float(l5u) == 5.0:
                add("Standard UNDER", "UNDER")

    for name, rows in buckets.items():
        # OVER: lower line/avg first; UNDER: higher line/avg first
        if "UNDER" in name:
            rows.sort(
                key=lambda x: (
                    -(x["ratio"] or 0),
                    x["tip"],
                    x["player"],
                    x["prop"],
                    x["line"],
                )
            )
        else:
            rows.sort(
                key=lambda x: (
                    x["ratio"] if x["ratio"] is not None else 999,
                    x["tip"],
                    x["player"],
                    x["prop"],
                    x["line"],
                )
            )
        lines.append(f"===== {name} (n={len(rows)}) =====")
        if not rows:
            lines.append("  (none)")
            lines.append("")
            continue
        for i, r in enumerate(rows, 1):
            side_ch = "O" if r["side"] == "OVER" else "U"
            ratio_s = f"{r['ratio']:.3f}" if r["ratio"] is not None else "n/a"
            lines.append(
                f"{i:3d}. {r['player']} ({r['team']} vs {r['opp']})  "
                f"{r['prop']}  {side_ch}{r['line']:g}  "
                f"L5avg={r['avg']:.1f}  line/avg={ratio_s}  "
                f"flag={r['flag']}  D={r['def_tier'] or '-'}  tip={r['tip']}"
            )
            lines.append(
                f"     L5  {', '.join(f'{v:g}' for v in r['vals'])}"
            )
        lines.append("")

    text = "\n".join(lines) + "\n"
    out = ROOT / "outputs" / slate / "cfb" / f"l5eq5_all_picks_{slate}.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    rep = ROOT / "data" / "reports" / f"cfb_l5eq5_all_picks_{slate}.txt"
    rep.parent.mkdir(parents=True, exist_ok=True)
    rep.write_text(text, encoding="utf-8")
    print(text)
    print(f"wrote {out}")
    print(f"wrote {rep}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
