"""Label the Oct 7 category cells the builder would have used."""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from utils.prop_norm import canon_prop  # noqa: E402

WANT = [
    ("WNBA", "threes"),
    ("WNBA", "pts+ast"),
    ("WNBA", "ptsast"),
    ("WNBA", "rebounds"),
    ("Tennis", "match_total_games"),
    ("Tennis", "matchtotalgames"),
    ("MLB", "pitcher_ks"),
    ("MLB", "pitching_outs"),
    ("MLB", "hits_allowed"),
    ("MLB", "earned_runs"),
]


def main() -> None:
    price = json.loads(
        (ROOT / "data/reports/pp_prop_hit_rates_by_sport.json").read_text(encoding="utf-8")
    )
    master = json.loads(
        (ROOT / "data/reports/all_ranked_prop_hit_rates.json").read_text(encoding="utf-8")
    )
    print("master", master.get("from"), master.get("to"))
    idx = {(r["sport"], r["book"], r["prop"]): r for r in master["rows"]}
    for sport, sv in price["sports"].items():
        for block in sv.get("props") or []:
            pn = canon_prop(sport, block.get("prop_norm") or block["prop"])
            if (sport, pn) not in WANT and (sport, block.get("prop_norm")) not in WANT:
                continue
            for cell in block["cells"]:
                if cell["pick_type"] != "Goblin" or cell["direction"] != "OVER":
                    continue
                m = idx.get((sport, "Goblin OVER", pn))
                pair = (cell["hit"], cell["decided"])
                label = "unlabeled"
                if m:
                    for name, hk, nk in (
                        ("raw book", "hits", "n"),
                        ("listed L5>=4", "listed_hits", "listed_n"),
                        ("L5=5", "l5eq5_hits", "l5eq5_n"),
                        ("L5=5+L10>=8", "l5eq5_l10ge8_hits", "l5eq5_l10ge8_n"),
                        ("L5=5+L10>=8+D", "l5eq5_l10ge8_d_hits", "l5eq5_l10ge8_d_n"),
                    ):
                        if pair == (m.get(hk), m.get(nk)):
                            label = name
                            break
                    raw = f"raw {m['hits']}/{m['n']}"
                    joint = f"L5=5+L10 {m.get('l5eq5_l10ge8_hits')}/{m.get('l5eq5_l10ge8_n')}"
                    djoint = f"D {m.get('l5eq5_l10ge8_d_hits')}/{m.get('l5eq5_l10ge8_d_n')}"
                else:
                    raw = joint = djoint = "no master row"
                print(
                    f"{sport} {block['prop']} ({pn}): price {pair[0]}/{pair[1]} "
                    f"= {label} | {raw} | {joint} | {djoint}"
                )


if __name__ == "__main__":
    main()
