import json
from pathlib import Path

p = Path(r"H:\PropORACLE\data\reports\all_ranked_prop_hit_rates.json")
d = json.loads(p.read_text(encoding="utf-8"))
print("catalog", d.get("from"), "->", d.get("to"), "n_days", d.get("n_days"))
print("WNBA", d["by_sport"].get("WNBA"))
print("WNBA1H", d["by_sport"].get("WNBA1H"))
print("WNBA1Q", d["by_sport"].get("WNBA1Q"))

order = {"Goblin OVER": 0, "Standard OVER": 1, "Standard UNDER": 2, "Demon OVER": 3}


def pct(x):
    return "   —" if x is None else f"{100 * x:5.1f}"


rows = [r for r in d["rows"] if r.get("sport") == "WNBA"]
# sport-level by book
print("\n=== by book ===")
from collections import defaultdict

books = defaultdict(lambda: {"n": 0, "hits": 0, "listed_n": 0, "listed_hits": 0})
for r in rows:
    b = r.get("book") or ""
    books[b]["n"] += r.get("n") or 0
    books[b]["hits"] += r.get("hits") or 0
    books[b]["listed_n"] += r.get("listed_n") or 0
    books[b]["listed_hits"] += r.get("listed_hits") or 0
for b, c in sorted(books.items(), key=lambda kv: order.get(kv[0], 9)):
    n, h = c["n"], c["hits"]
    ln, lh = c["listed_n"], c["listed_hits"]
    print(
        f"{b:16} all {100*h/n:5.1f}% n={n:6}   L5>=4 {100*lh/ln:5.1f}% n={ln}"
        if n and ln
        else f"{b:16} all {100*h/n:5.1f}% n={n}"
    )

print(
    f"\n{'book':16} {'prop':18} {'t':2} {'n':6} {'all':6} {'L5>=4':6} {'L5=5':6} "
    f"{'L5+L10':6} {'ticket D':6} {'nT':5} {'nL4':5}"
)
for r in sorted(
    rows,
    key=lambda x: (order.get(x["book"], 9), -(x.get("listed_n") or 0), x["prop"]),
):
    if r.get("book") not in {"Goblin OVER", "Standard OVER", "Standard UNDER"}:
        continue
    if (r.get("n") or 0) < 15 and not r.get("live_book"):
        continue
    sh = " W" if r.get("shadow") else ""
    print(
        f"{r['book']:16} {r['prop']:18} {str(r.get('prop_tier') or ''):2} "
        f"{r['n']:6} {pct(r.get('hr')):6} {pct(r.get('listed_hr')):6} "
        f"{pct(r.get('l5eq5_hr')):6} {pct(r.get('l5eq5_l10ge8_hr')):6} "
        f"{pct(r.get('l5eq5_l10ge8_d_hr')):6} {r.get('l5eq5_l10ge8_d_n') or 0:5} "
        f"{r.get('listed_n') or 0:5}{sh}"
    )
