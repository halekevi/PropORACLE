"""Print CFB L5=5 keep props tipping on a slate date (from prior-day step8 if needed)."""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.cfb_keep_gates import cfb_list_eligible, cfb_pass_yards_goblin_skip_d, cfb_ticket_fade


def main() -> int:
    slate = sys.argv[1] if len(sys.argv) > 1 else "2026-10-22"
    # Prefer same-day folder; else date-1 parquet (Thursday board often lives on Wed folder).
    cands = [
        ROOT / "outputs" / slate / "cfb" / "step8_cfb_direction_clean.parquet",
        ROOT / "outputs" / slate / f"step8_cfb_direction_clean_{slate}.parquet",
    ]
    # also try slate-1
    y, m, d = map(int, slate.split("-"))
    from datetime import date, timedelta

    prev = (date(y, m, d) - timedelta(days=1)).isoformat()
    cands += [
        ROOT / "outputs" / prev / "cfb" / "step8_cfb_direction_clean.parquet",
        ROOT / "outputs" / prev / f"step8_cfb_direction_clean_{prev}.parquet",
    ]
    path = next((p for p in cands if p.exists()), None)
    if path is None:
        print(f"no CFB step8 for {slate} / {prev}", file=sys.stderr)
        return 1

    df = pd.read_parquet(path)
    today = df[df["Game Time"].astype(str).str.startswith(slate)].copy()
    print(f"CFB best-props L5=5  tip_date={slate}  source={path.relative_to(ROOT)}")
    print(f"board rows tipping today: {len(today)}")
    print()

    buckets: dict[str, list[dict]] = {
        "Goblin OVER": [],
        "Standard OVER": [],
        "Standard UNDER": [],
    }
    for _, r in today.iterrows():
        pt = str(r.get("Pick Type") or "").strip().lower()
        if pt == "demon":
            continue
        side = str(r.get("Direction") or "").strip().upper()
        if pt == "goblin" and side != "OVER":
            continue
        l5o = pd.to_numeric(r.get("L5 Over"), errors="coerce")
        l5u = pd.to_numeric(r.get("L5 Under"), errors="coerce")
        hits = l5o if side == "OVER" else l5u
        if float(hits) != 5.0:
            continue
        rec = {
            "sport": "CFB",
            "player": str(r.get("Player") or ""),
            "team": str(r.get("Team") or ""),
            "opp": str(r.get("Opp") or ""),
            "prop": str(r.get("Prop") or ""),
            "pick_type": pt,
            "side": side,
            "line": float(r["Line"]),
            "l5_over": float(l5o) if pd.notna(l5o) else None,
            "l5_under": float(l5u) if pd.notna(l5u) else None,
            "def_tier": str(r.get("Def Tier") or ""),
            "def_rank": r.get("Def Rank"),
            "tip": str(r.get("Game Time"))[:16].replace("T", " "),
            "season_l5_flag": str(r.get("Season L5 Flag") or ""),
            "Prop": str(r.get("Prop") or ""),
            "Pick Type": pt,
            "Direction": side,
            "Line": float(r["Line"]),
            "L5 Over": l5o,
            "L5 Under": l5u,
            "Def Tier": str(r.get("Def Tier") or ""),
            "Season L5 Flag": str(r.get("Season L5 Flag") or ""),
        }
        if not cfb_list_eligible(rec):
            continue
        skip_d = cfb_pass_yards_goblin_skip_d(rec)
        d = str(rec["def_tier"]).strip()
        d_pass = skip_d or (
            (side == "OVER" and d in {"Weak", "Below Avg"})
            or (side == "UNDER" and d in {"Elite", "Above Avg"})
        )
        ticket_fade = cfb_ticket_fade(rec)
        key = (
            "Goblin OVER"
            if pt == "goblin"
            else ("Standard OVER" if side == "OVER" else "Standard UNDER")
        )
        buckets[key].append(
            {
                **rec,
                "d_pass": d_pass,
                "skip_d": skip_d,
                "ticket_fade": ticket_fade,
            }
        )

    for key, rows in buckets.items():
        rows.sort(
            key=lambda x: (
                0 if x["d_pass"] else 1,
                1 if x["ticket_fade"] else 0,
                x["player"],
                x["prop"],
                x["line"],
            )
        )
        print(f"===== {key} (n={len(rows)}) =====")
        if not rows:
            print("  (none L5=5)")
            print()
            continue
        for rec in rows:
            if rec["skip_d"]:
                dtag = "D N/A (pass yds)"
            elif rec["d_pass"]:
                dtag = f"D-pass {rec['def_tier']}"
            else:
                dtag = f"no-D {rec['def_tier'] or '?'}"
            fade = "  [ticket-fade]" if rec["ticket_fade"] else ""
            flag = rec["season_l5_flag"]
            flag_s = (
                f"  [{flag}]"
                if flag and flag not in ("", "nan", "None")
                else ""
            )
            print(
                f"  {rec['player']:<22} {rec['team']:<5} vs {rec['opp']:<5}  "
                f"{rec['prop']:<22} {rec['side'][0]}{rec['line']:<6}  "
                f"L5 5/0  {dtag:<22}  tip {rec['tip']}{flag_s}{fade}"
            )
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
