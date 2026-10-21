"""Roll up live-book hit rates from the master catalog + CFB graded JSON."""
from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

LIVE = ("Goblin OVER", "Standard OVER", "Standard UNDER")
REPO = Path(r"H:/PropORACLE")
MASTER = REPO / "data" / "reports" / "all_ranked_prop_hit_rates.json"


def pct(h: int, n: int) -> str:
    return f"{100 * h / n:.1f}% ({h}/{n})" if n else "—"


def main() -> None:
    d = json.loads(MASTER.read_text(encoding="utf-8"))
    print("MASTER", d.get("from"), "->", d.get("to"), "decided", d.get("n_decided"))
    print("generated", d.get("generated_at"))
    print("by_sport keys", sorted(d.get("by_sport") or {}))

    agg: dict = defaultdict(
        lambda: defaultdict(
            lambda: {
                "n": 0,
                "h": 0,
                "l5n": 0,
                "l5h": 0,
                "j8n": 0,
                "j8h": 0,
                "dn": 0,
                "dh": 0,
                "j8dn": 0,
                "j8dh": 0,
            }
        )
    )
    keep_rows = []
    for r in d.get("rows") or []:
        b = r.get("book")
        if b not in LIVE:
            continue
        a = agg[r["sport"]][b]
        a["n"] += int(r.get("n") or 0)
        a["h"] += int(r.get("hits") or 0)
        a["l5n"] += int(r.get("l5eq5_n") or 0)
        a["l5h"] += int(r.get("l5eq5_hits") or 0)
        a["j8n"] += int(r.get("l5eq5_l10ge8_n") or 0)
        a["j8h"] += int(r.get("l5eq5_l10ge8_hits") or 0)
        a["dn"] += int(r.get("d_n") or 0)
        a["dh"] += int(r.get("d_hits") or 0)
        a["j8dn"] += int(r.get("l5eq5_l10ge8_d_n") or 0)
        a["j8dh"] += int(r.get("l5eq5_l10ge8_d_hits") or 0)
        keep_rows.append(r)

    order = [
        "WNBA",
        "WNBA1Q",
        "WNBA1H",
        "MLB",
        "Soccer",
        "Tennis",
        "NBA",
        "NBA1Q",
        "NBA1H",
        "NFL",
        "CFB",
        "NHL",
        "CBB",
        "Golf",
    ]
    print()
    hdr = f"{'sport':8} {'book':16} {'ungated':24} {'L5=5':22} {'L5=5+L10>=8':22} {'G70 L5=5+L10>=8+D':24}"
    print(hdr)
    sports = order + [s for s in sorted(agg) if s not in order]
    for sp in sports:
        if sp not in agg:
            continue
        for b in LIVE:
            a = agg[sp][b]
            if a["n"] == 0:
                continue
            print(
                f"{sp:8} {b:16} {pct(a['h'], a['n']):24} "
                f"{pct(a['l5h'], a['l5n']):22} {pct(a['j8h'], a['j8n']):22} "
                f"{pct(a['j8dh'], a['j8dn']):24}"
            )

    # Top keep-ish Goblin cells by L5=5+L10>=8 n
    print("\nTOP Goblin OVER by L5=5+L10>=8 n (ticket-like, no D):")
    gob = [r for r in keep_rows if r.get("book") == "Goblin OVER" and int(r.get("l5eq5_l10ge8_n") or 0) >= 20]
    gob.sort(key=lambda r: -int(r.get("l5eq5_l10ge8_n") or 0))
    for r in gob[:25]:
        n = int(r["l5eq5_l10ge8_n"])
        h = int(r["l5eq5_l10ge8_hits"])
        dn = int(r.get("l5eq5_l10ge8_d_n") or 0)
        dh = int(r.get("l5eq5_l10ge8_d_hits") or 0)
        print(
            f"  {r['sport']:8} {(r.get('prop_display') or r['prop']):22} "
            f"{pct(h, n):20} D {pct(dh, dn)}"
        )


if __name__ == "__main__":
    main()
