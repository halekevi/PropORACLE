import _tmp_cfb_l5_recut as R
import _tmp_cfb_live_stack as S

R.TAPE = [
    "2026-08-28",
    "2026-08-29",
    "2026-09-03",
    "2026-09-04",
    "2026-09-05",
    "2026-09-12",
    "2026-09-26",
]
rows = S.collect()
chips = [
    ("rush_yds", "Goblin OVER"),
    ("rec_yds", "Goblin OVER"),
    ("rush_yds", "Standard UNDER"),
]


def pack(xs):
    if not xs:
        return "empty"
    return f"{sum(r['hit'] for r in xs)}/{len(xs)}"


for prop, book in chips:
    l5 = [r for r in rows if r["prop"] == prop and r["book"] == book and r["l5"] == 5]
    kept = [r for r in l5 if r["d"]]
    drop = [r for r in l5 if not r["d"]]
    print(prop, book, "L5", pack(l5), "D-kept", pack(kept), "D-drop", pack(drop))
