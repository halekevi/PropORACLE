"""9/22 WNBA Goblin Points OVER G70: hit vs miss by line."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from build_goblin70_tickets import attach_wnba_ticket_priors  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402

DATE = "2026-09-22"
GP = Path(r"H:\PropORACLE_main_cp\ui_runner\templates") / f"graded_props_{DATE}.json"


def pf(v):
    try:
        if v in (None, ""):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def main() -> None:
    gp = json.loads(GP.read_text(encoding="utf-8"))
    rows = []
    for r in gp["props"]:
        if str(r.get("sport") or "").upper() != "WNBA":
            continue
        if str(r.get("pick_type") or "").strip().title() != "Goblin":
            continue
        side = str(r.get("direction") or r.get("side") or "").upper()
        if side not in {"OVER", "O"}:
            continue
        rec = {
            "sport": "WNBA",
            "player": r.get("player"),
            "prop": r.get("prop"),
            "side": "OVER",
            "line": pf(r.get("line")),
            "pick_type": "Goblin",
            "l5_over": pf(r.get("l5_over")),
            "l10_over": pf(r.get("l10_over")),
            "cover": pf(r.get("edge") or r.get("cover")),
            "def": r.get("def_tier") or r.get("def"),
            "usage_tier": r.get("usage_tier"),
            "minutes_tier": r.get("minutes_tier"),
            "prop_tier": r.get("tier") or r.get("prop_tier"),
        }
        attach_wnba_ticket_priors(rec, DATE)
        if not goblin_70_eligible(rec):
            continue
        prop = canon_prop("WNBA", str(rec["prop"] or ""))
        if prop not in {"points", "pts+ast", "pts+reb"}:
            continue
        res = str(r.get("result") or "").upper()
        rows.append(
            {
                "res": res,
                "player": rec["player"],
                "prop": prop,
                "line": rec["line"],
                "actual": pf(r.get("actual_value")),
                "cover": rec.get("cover"),
                "avg": rec.get("avg_season"),
                "min": rec.get("minutes_tier"),
                "team": r.get("team"),
                "opp": r.get("opp_team"),
            }
        )

    print(
        f"{'res':6} {'player':22} {'prop':8} {'line':6} {'actual':7} "
        f"{'cover':7} {'avg':7} {'min':8} team opp"
    )
    for x in sorted(rows, key=lambda t: (t["prop"], t["line"] or 0)):
        print(
            f"{x['res']:6} {str(x['player'])[:22]:22} {x['prop']:8} "
            f"{x['line']!s:6} {x['actual']!s:7} {x['cover']!s:7} "
            f"{x['avg']!s:7} {str(x['min'] or ''):8} {x['team']} {x['opp']}"
        )
    pts = [x for x in rows if x["prop"] == "points" and x["res"] in {"HIT", "MISS"}]
    hits = [x["line"] for x in pts if x["res"] == "HIT" and x["line"] is not None]
    miss = [x["line"] for x in pts if x["res"] == "MISS" and x["line"] is not None]
    print()
    print("Points HIT  n", len(hits), "avg line", round(sum(hits) / len(hits), 2) if hits else None, hits)
    print("Points MISS n", len(miss), "avg line", round(sum(miss) / len(miss), 2) if miss else None, miss)


if __name__ == "__main__":
    main()
