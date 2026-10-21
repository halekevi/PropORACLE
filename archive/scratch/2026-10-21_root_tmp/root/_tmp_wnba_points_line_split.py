"""WNBA Goblin Points OVER G70: hit vs miss line split (catalog + 9/18-22)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from all_ranked_prop_hit_rates import graded_paths, l10_dir, l5_dir, num  # noqa: E402
from build_goblin70_tickets import attach_wnba_ticket_priors  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402

CUT = "2026-09-18"


def stats(vals: list[float]) -> str:
    if not vals:
        return "n=0"
    s = sorted(vals)
    n = len(s)
    mean = sum(s) / n
    med = s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2
    bands = (
        f"<15={sum(1 for x in s if x < 15)} "
        f"15-22={sum(1 for x in s if 15 <= x <= 22)} "
        f"22.5-29.5={sum(1 for x in s if 22 < x < 30)} "
        f"30+={sum(1 for x in s if x >= 30)}"
    )
    return f"n={n} avg={mean:.1f} med={med:.1f} min={s[0]} max={s[-1]} {bands}"


def dump(label: str, rows: list) -> None:
    hits = [r["line"] for r in rows if r["hit"] == 1]
    miss = [r["line"] for r in rows if r["hit"] == 0]
    print(label)
    print("  HIT ", stats(hits))
    print("  MISS", stats(miss))
    print()


def main() -> None:
    seen = set()
    rows = []
    for path in graded_paths():
        date = path.stem.replace("graded_props_", "")[:10]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        props = raw.get("props") or raw.get("rows") or []
        if not isinstance(props, list):
            continue
        for r in props:
            if not isinstance(r, dict):
                continue
            if str(r.get("result") or "").strip().upper() not in {"HIT", "MISS"}:
                continue
            if str(r.get("sport") or "").upper() != "WNBA":
                continue
            if str(r.get("pick_type") or "").strip().title() != "Goblin":
                continue
            side = str(r.get("direction") or r.get("over_under") or "").strip().upper()
            if side not in {"OVER", "O"}:
                continue
            prop = canon_prop("WNBA", str(r.get("prop") or r.get("prop_type") or ""))
            if prop != "points":
                continue
            player = str(r.get("player") or "").strip()
            line = num(r.get("line"))
            key = (date, player, prop, line)
            if key in seen:
                continue
            seen.add(key)
            rec = {
                "sport": "WNBA",
                "player": player,
                "prop": r.get("prop"),
                "side": "OVER",
                "line": line,
                "pick_type": "Goblin",
                "l5_over": l5_dir(r, "OVER"),
                "l10_over": l10_dir(r, "OVER"),
                "cover": num(r.get("edge") or r.get("cover")),
                "def": r.get("def_tier") or r.get("def"),
                "usage_tier": r.get("usage_tier"),
                "minutes_tier": r.get("minutes_tier"),
                "prop_tier": r.get("tier") or r.get("prop_tier"),
            }
            attach_wnba_ticket_priors(rec, date)
            if not goblin_70_eligible(rec):
                continue
            rows.append(
                {
                    "date": date,
                    "player": player,
                    "line": line,
                    "hit": 1 if str(r.get("result")).strip().upper() == "HIT" else 0,
                    "actual": num(r.get("actual_value")),
                    "cover": rec.get("cover"),
                    "avg": rec.get("avg_season"),
                    "min": rec.get("minutes_tier"),
                }
            )

    hist = [r for r in rows if r["date"] < CUT]
    rec = [r for r in rows if r["date"] >= CUT]
    dump("catalog G70 Points OVER through 9/17", hist)
    dump("9/18-22 G70 Points OVER", rec)

    print("9/18-22 faces:")
    for r in sorted(rec, key=lambda x: (x["date"], x["line"] or 0)):
        mark = "HIT" if r["hit"] else "MISS"
        print(
            f"  {r['date']} {mark:4} {r['player'][:22]:22} "
            f"line={r['line']} actual={r['actual']} cover={r['cover']} "
            f"avg={r['avg']} min={r['min']}"
        )

    sep23 = Path(r"H:\PropORACLE_main_cp\ui_runner\templates\graded_props_2026-09-23.json")
    print()
    print("9/23 graded_props exists:", sep23.exists())


if __name__ == "__main__":
    main()
