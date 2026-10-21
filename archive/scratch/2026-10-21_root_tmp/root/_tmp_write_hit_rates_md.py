import json
from pathlib import Path

root = Path(r"H:/PropORACLE")
master = json.loads((root / "data/reports/all_ranked_prop_hit_rates.json").read_text(encoding="utf-8"))


def pct(h, n):
    h = h or 0
    n = n or 0
    if not n:
        return "—"
    return f"{100 * h / n:.1f}% ({h}/{n})"


BOOK_ORDER = ["Goblin OVER", "Standard OVER", "Standard UNDER"]
SPORT_ORDER = [
    "WNBA",
    "MLB",
    "Soccer",
    "Tennis",
    "NBA",
    "NBA1Q",
    "NBA1H",
    "NHL",
    "CBB",
    "NFL",
    "CFB",
]

rows = [r for r in master["rows"] if r.get("book") in BOOK_ORDER]
by = {}
for r in rows:
    by.setdefault(r["sport"], {}).setdefault(r["book"], []).append(r)

out = []
out.append("# Tracked prop hit rates")
out.append("")
out.append(
    f"Source: `data/reports/all_ranked_prop_hit_rates.json` · "
    f"{master['from']} → {master['to']} · {master['n_decided']:,} decided"
)
out.append("")
out.append("Columns: **Ungated** | L5≥4 (listed) | L5=5 | L10≥8 | L5=5+L10≥8")
out.append("")
out.append(
    "NFL Week-1 / CFB Sep 3–5 appends live in the canvas; this file is the graded master catalog."
)
out.append("")

sports = sorted(
    by.keys(),
    key=lambda s: (SPORT_ORDER.index(s) if s in SPORT_ORDER else 99, s),
)
for sport in sports:
    out.append(f"## {sport}")
    out.append("")
    for book in BOOK_ORDER:
        props = by[sport].get(book) or []
        if not props:
            out.append(f"### {book}")
            out.append("")
            out.append("_none_")
            out.append("")
            continue
        props = sorted(
            props,
            key=lambda r: (
                -(r.get("n") or 0),
                r.get("prop_display") or r.get("prop") or "",
            ),
        )
        out.append(f"### {book}")
        out.append("")
        out.append(
            "| Prop | Tier | Ungated | L5≥4 | L5=5 | L10≥8 | L5=5+L10≥8 | n |"
        )
        out.append("|---|---|---|---|---|---|---|---|")
        for r in props:
            name = r.get("prop_display") or r.get("prop") or "?"
            tier = r.get("prop_tier") or "—"
            out.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    name,
                    tier,
                    pct(r.get("hits"), r.get("n")),
                    pct(r.get("listed_hits"), r.get("listed_n")),
                    pct(r.get("l5eq5_hits"), r.get("l5eq5_n")),
                    pct(r.get("l10ge8_hits"), r.get("l10ge8_n")),
                    pct(r.get("l5eq5_l10ge8_hits"), r.get("l5eq5_l10ge8_n")),
                    r.get("n") or 0,
                )
            )
        out.append("")

path = root / "data/reports/tracked_prop_hit_rates.md"
path.write_text("\n".join(out), encoding="utf-8")
print("wrote", path, "lines", len(out), "bytes", path.stat().st_size)
