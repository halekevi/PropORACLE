"""Compare the price-file WNBA cells to the Feb–Sep master catalog."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from utils.prop_norm import canon_prop  # noqa: E402


def main() -> None:
    price = json.loads(
        (ROOT / "data/reports/pp_prop_hit_rates_by_sport.json").read_text(encoding="utf-8")
    )
    master = json.loads(
        (ROOT / "data/reports/all_ranked_prop_hit_rates.json").read_text(encoding="utf-8")
    )
    idx = {}
    for r in master["rows"]:
        if r.get("book") != "Goblin OVER":
            continue
        idx[(r["sport"], r["prop"])] = r
    print("master window", master.get("from"), "->", master.get("to"), "days", master.get("n_days"))
    print(f"{'prop':22} {'price':18} {'master':18} {'ratio':6} joint")
    for block in price["sports"]["WNBA"]["props"]:
        for cell in block["cells"]:
            if cell["pick_type"] != "Goblin" or cell["direction"] != "OVER":
                continue
            pn = canon_prop("WNBA", block.get("prop_norm") or block["prop"])
            m = idx.get(("WNBA", pn))
            if not m:
                print("NO MASTER", block["prop"], pn)
                continue
            ratio = cell["decided"] / m["n"] if m["n"] else 0
            print(
                f"{block['prop'][:22]:22} "
                f"{cell['hit']}/{cell['decided']} ({cell['hit_rate_pct']})  "
                f"{m['hits']}/{m['n']} ({m['hr']:.1%})  "
                f"{ratio:.3f}  "
                f"{m['l5eq5_l10ge8_hits']}/{m['l5eq5_l10ge8_n']}"
            )


if __name__ == "__main__":
    main()
