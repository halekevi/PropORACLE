"""Which price-file Goblin OVER cells match the master catalog, and as which window."""
from __future__ import annotations

import json
import sys
from collections import Counter
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
    kind = Counter()
    mismatches = []
    for sport, sv in price["sports"].items():
        for block in sv.get("props") or []:
            for cell in block["cells"]:
                if cell["pick_type"] != "Goblin" or cell["direction"] != "OVER":
                    continue
                pn = canon_prop(sport, block.get("prop_norm") or block["prop"])
                m = idx.get((sport, pn))
                if m is None:
                    kind["no_master"] += 1
                    if sport == "WNBA":
                        mismatches.append(f"NO {sport} {block['prop']} canon={pn}")
                    continue
                h, n = cell["hit"], cell["decided"]
                if (h, n) == (m["hits"], m["n"]):
                    kind["raw_book"] += 1
                elif (h, n) == (m.get("l5eq5_l10ge8_hits"), m.get("l5eq5_l10ge8_n")):
                    kind["joint"] += 1
                elif (h, n) == (m.get("l5eq5_hits"), m.get("l5eq5_n")):
                    kind["l5eq5"] += 1
                elif (h, n) == (m.get("listed_hits"), m.get("listed_n")):
                    kind["listed"] += 1
                elif (h, n) == (m.get("hr_use_hits"), m.get("hr_use_n")):
                    kind["hr_use"] += 1
                else:
                    kind["other"] += 1
                    if sport in ("WNBA", "Tennis", "MLB", "Soccer") and n >= 20:
                        mismatches.append(
                            f"{sport} {block['prop']}: price {h}/{n} "
                            f"raw {m['hits']}/{m['n']} "
                            f"l5 {m.get('l5eq5_hits')}/{m.get('l5eq5_n')} "
                            f"joint {m.get('l5eq5_l10ge8_hits')}/{m.get('l5eq5_l10ge8_n')} "
                            f"hr_use {m.get('hr_use_hits')}/{m.get('hr_use_n')} {m.get('hr_window')}"
                        )
    print(dict(kind))
    print("--- mismatches (focus sports, n>=20) ---")
    for line in mismatches:
        print(line)


if __name__ == "__main__":
    main()
