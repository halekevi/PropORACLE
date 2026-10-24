"""Check whether price-file cells are the master's L5=5+L10>=8+D joint."""
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
        idx[(r["sport"], r["book"], r["prop"])] = r
    books = {
        ("Goblin", "OVER"): "Goblin OVER",
        ("Standard", "OVER"): "Standard OVER",
        ("Standard", "UNDER"): "Standard UNDER",
    }
    hit = 0
    miss = 0
    examples = []
    for sport, sv in price["sports"].items():
        for block in sv.get("props") or []:
            pn = canon_prop(sport, block.get("prop_norm") or block["prop"])
            for cell in block["cells"]:
                book = books.get((cell["pick_type"], cell["direction"]))
                if not book:
                    continue
                m = idx.get((sport, book, pn))
                pair = (cell["hit"], cell["decided"])
                label = None
                if m:
                    for name, hk, nk in (
                        ("raw", "hits", "n"),
                        ("listed L5>=4", "listed_hits", "listed_n"),
                        ("L5=5", "l5eq5_hits", "l5eq5_n"),
                        ("L5=5+L10>=8", "l5eq5_l10ge8_hits", "l5eq5_l10ge8_n"),
                        ("L5=5+L10>=8+D", "l5eq5_l10ge8_d_hits", "l5eq5_l10ge8_d_n"),
                        ("L5=5+D", "l5eq5_d_hits", "l5eq5_d_n"),
                        ("L5>=4+D", "l5ge4_d_hits", "l5ge4_d_n"),
                    ):
                        if pair == (m.get(hk), m.get(nk)) and m.get(nk):
                            label = name
                            break
                if label:
                    hit += 1
                else:
                    miss += 1
                    if sport in ("WNBA", "Tennis", "MLB") and cell["decided"] >= 20:
                        raw = f"{m['hits']}/{m['n']}" if m else "no row"
                        examples.append(
                            f"UNMATCHED {sport} {book} {block['prop']} price {pair[0]}/{pair[1]} master raw {raw}"
                        )
                if sport == "WNBA" and cell["pick_type"] == "Goblin" and cell["direction"] == "OVER":
                    print(
                        f"WNBA Goblin {block['prop']}: price {pair[0]}/{pair[1]} -> {label or 'NONE'}"
                    )
    print(f"matched {hit} unmatched {miss}")
    for line in examples[:40]:
        print(line)


if __name__ == "__main__":
    main()
