"""Diagnose CBB injury CSV vs CFB slate join."""
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")


def main() -> None:
    print("=== injuries_cbb files ===")
    for root in (ROOT, MAIN):
        hits = sorted(root.glob("outputs/**/injuries_cbb_*.csv"))
        print(root.name, len(hits))
        for p in hits[-8:]:
            print(" ", p, "bytes", p.stat().st_size)

    print("=== injuries_cfb files ===")
    for root in (ROOT, MAIN):
        hits = sorted(root.glob("outputs/**/injuries_cfb_*.csv"))
        print(root.name, len(hits))

    # Overlap windows: CFB Aug-Sep + any CBB that exists same date
    print("=== same-date CBB injury + CFB step6 ===")
    dates = set()
    for p in ROOT.glob("outputs/*/cfb/step6_ranked_cfb.parquet"):
        dates.add(p.parent.parent.name)
    for p in MAIN.glob("outputs/*/cfb/step6_ranked_cfb.parquet"):
        dates.add(p.parent.parent.name)
    for day in sorted(dates):
        inj = None
        for root in (MAIN, ROOT):
            cand = root / "outputs" / day / f"injuries_cbb_{day}.csv"
            if cand.is_file():
                inj = cand
                break
        if inj is None:
            continue
        print(f"OVERLAP {day} injury={inj}")

    # If no overlap, still show latest cbb injury date vs latest cfb slate
    cbb_inj = sorted(MAIN.glob("outputs/*/injuries_cbb_*.csv")) + sorted(ROOT.glob("outputs/*/injuries_cbb_*.csv"))
    if cbb_inj:
        print("latest cbb injury", cbb_inj[-1])
        inj = pd.read_csv(cbb_inj[-1], nrows=5)
        print("cbb injury cols", list(inj.columns))
        print(inj.head().to_string())


if __name__ == "__main__":
    main()
