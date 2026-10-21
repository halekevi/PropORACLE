"""Diagnose whether CFB/NFL L5=5 emptiness is missing cross-season fill."""
from __future__ import annotations

from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")

CFB_DATES = [
    "2026-08-28",
    "2026-08-29",
    "2026-09-03",
    "2026-09-04",
    "2026-09-05",
    "2026-09-12",
    "2026-09-24",
]
NFL_DATES = ["2026-09-20", "2026-09-21", "2026-09-24"]


def find_step8(root: Path, date: str, folder: str) -> Path | None:
    d = root / "outputs" / date / folder
    names = [
        f"step8_{folder}_direction_clean_{date}.xlsx",
        f"step8_{folder}_direction_clean.xlsx",
        f"step8_{folder}_direction_clean_{date}.csv",
    ]
    for n in names:
        p = d / n
        if p.exists():
            return p
    return None


def colmap(df):
    return {str(c).strip().lower(): c for c in df.columns}


def pick(cm, *names):
    for n in names:
        if n.lower() in cm:
            return cm[n.lower()]
    return None


def summarize(path: Path, sport: str) -> None:
    df = pd.read_excel(path) if path.suffix.lower() in {".xlsx", ".xls"} else pd.read_csv(path)
    cm = colmap(df)
    c_l5o = pick(cm, "last5_over", "l5_over", "L5 Over", "line_hits_over_5")
    c_l5u = pick(cm, "last5_under", "l5_under", "L5 Under")
    c_l10o = pick(cm, "line_hits_over_10", "l10_over", "L10 Over")
    c_flag = pick(cm, "season_l5_flag", "Season L5 Flag")
    c_n = pick(cm, "l5_sample_n", "L5 Sample N", "games_used")
    c_win = pick(cm, "l5_window_n", "L5 Window N")
    c_src = pick(cm, "l5_source_counts", "L5 Source Counts")
    c_pick = pick(cm, "pick type", "Pick Type", "pick_type")
    print(f"\n=== {sport} {path}  rows={len(df)} ===")
    print("cols l5o/flag/n/win/src", bool(c_l5o), bool(c_flag), bool(c_n), bool(c_win), bool(c_src))
    if c_flag:
        print("season_l5_flag", dict(Counter(df[c_flag].fillna("").astype(str).str.strip().replace("nan", ""))))
    if c_l5o:
        s = pd.to_numeric(df[c_l5o], errors="coerce")
        filled = int(s.notna().sum())
        print(
            f"last5_over filled={filled}/{len(df)}  "
            f"eq5={(s == 5).sum()}  ge4={(s >= 4).sum()}  "
            f"max={s.max()}  median={s.median()}  "
            f"value_counts={dict(s.value_counts().sort_index())}"
        )
    if c_l10o:
        s = pd.to_numeric(df[c_l10o], errors="coerce")
        print(f"l10_over filled={int(s.notna().sum())}  ge8={(s >= 8).sum()}  eq5_and_l10ge8 would need join")
        print(f"l10_over counts {dict(s.value_counts().sort_index())}")
    if c_n:
        s = pd.to_numeric(df[c_n], errors="coerce")
        print(f"l5_sample_n/games_used median={s.median()}  ge5={(s >= 5).sum()}  lt5={(s < 5).sum()}  max={s.max()}")
    if c_win:
        s = pd.to_numeric(df[c_win], errors="coerce")
        print(f"l5_window_n filled={int(s.notna().sum())}  ge5={(s >= 5).sum()}  value_counts={dict(s.value_counts().sort_index())}")
    if c_src:
        nonempty = df[c_src].fillna("").astype(str).str.strip()
        nonempty = nonempty[nonempty.isin(["", "nan"]) == False]
        print("l5_source_counts sample", nonempty.head(8).tolist())
        print("l5_source_counts nonempty", int((nonempty != "").sum()))
    if c_l5o and c_flag:
        s = pd.to_numeric(df[c_l5o], errors="coerce")
        flag = df[c_flag].fillna("").astype(str)
        eq5 = df[s == 5]
        print("among last5_over==5, flags", dict(Counter(eq5[c_flag].fillna("").astype(str))))
        prior = flag.str.contains("PRIOR|CROSS", case=False, na=False)
        print(f"PRIOR/CROSS rows={int(prior.sum())}  of which last5_over==5 {(s[prior] == 5).sum()}")
        thin = ~prior & (pd.to_numeric(df[c_n], errors="coerce") < 5 if c_n else False)
        if c_n:
            n = pd.to_numeric(df[c_n], errors="coerce")
            print(f"current n<5 AND last5_over==5 (should be 0 without fill): {int(((n < 5) & (s == 5)).sum())}")


def main():
    print("CFB step8 boards (fill window = through 2026-09-20)")
    for date in CFB_DATES:
        for root, tag in ((ROOT, "cursor"), (MAIN, "main_cp")):
            p = find_step8(root, date, "cfb")
            if p:
                print(f"-- {tag} {date}")
                summarize(p, "CFB")
                break
        else:
            print(f"NO CFB step8 {date}")

    print("\n\nNFL step8 boards (cross-season L5 active through week 4)")
    for date in NFL_DATES:
        for root, tag in ((ROOT, "cursor"), (MAIN, "main_cp")):
            p = find_step8(root, date, "nfl")
            if p:
                print(f"-- {tag} {date}")
                summarize(p, "NFL")
                break
        else:
            print(f"NO NFL step8 {date}")


if __name__ == "__main__":
    main()
