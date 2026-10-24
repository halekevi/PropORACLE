#!/usr/bin/env python3
"""Per-prop-category hit rates by L5/L10/season, avg-to-line gap, D, minutes, proficiency.

Joins graded HIT/MISS to dated step8 boards (WNBA/MLB/Soccer/Tennis).
Proficiency = tertile of season avg in that exact prop (Elite = high producers).

  py -3.14 scripts/prop_category_factor_rates.py
"""
from __future__ import annotations

import json
import math
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

OPEN = Path(r"H:\PropORACLE")
MAIN = Path(r"H:\PropORACLE_main_cp")
sys.path.insert(0, str(OPEN / "scripts"))
sys.path.insert(0, str(OPEN))
import _avg_gap_by_badge as B  # noqa: E402
import prop_hit_tiers as T  # noqa: E402

GRADED = MAIN / "ui_runner" / "templates"
OUT = OPEN / "data" / "reports" / "prop_category_factor_rates.json"
BOOKS = ("Goblin OVER", "Standard OVER", "Standard UNDER")
MIN_CAT = 40

DEF_ORDER = ("Elite", "Above Avg", "Avg", "Below Avg", "Weak")
GAP_ORDER = ("against", "0-0.5", "0.5-1", "1-2", "2+")
MIN_ORDER = ("HIGH", "MEDIUM", "LOW")
PROF_ORDER = ("Elite", "Mid", "Weak")


def rate(h, n):
    if n <= 0:
        return None
    return round(100.0 * h / n, 1)


def cell(df) -> dict:
    n = int(len(df))
    h = int(df["hit"].sum()) if n else 0
    return {"hits": h, "n": n, "rate": rate(h, n)}


def fmt(c: dict | None) -> str:
    if not c or not c.get("n"):
        return "--"
    return f"{c['rate']:.1f}% {c['hits']}/{c['n']}"


def find_step8(date: str, sport: str) -> Path | None:
    p = B.find_step8(date, sport)
    if p:
        return p
    folder = B.SPORT_FOLDER[sport]
    slug = folder
    for root in (B.MAIN, B.OPEN):
        d = root / "outputs" / date
        for cand in (
            d / folder / f"step8_{slug}_direction.csv",
            d / f"step8_{slug}_direction.csv",
            d / folder / f"step8_{slug}_direction_clean.csv",
        ):
            if cand.exists():
                return cand
    return None


def load_step8(path: Path, sport: str, date: str) -> pd.DataFrame:
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, low_memory=False, encoding="utf-8-sig")
        tmp = OPEN / "data" / "reports" / f"_tmp_{sport}_{date}.xlsx"
        tmp.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(tmp, index=False)
        try:
            return load_step8(tmp, sport, date)
        finally:
            tmp.unlink(missing_ok=True)
    df = pd.read_excel(path)
    c_player = B._col(df, "Player", "player", "player_name")
    c_prop = B._col(df, "Prop", "prop", "prop_type")
    c_line = B._col(df, "Line", "line", "line_score")
    c_dir = B._col(df, "Direction", "direction", "bet_direction")
    if not all([c_player, c_prop, c_line, c_dir]):
        return pd.DataFrame()
    base = B.load_step8(path, sport, date)
    extra = _game_hits(df, sport)
    extra["player_n"] = B._norm_name_s(df[c_player])
    extra["prop_n"] = B._norm_prop_s(df[c_prop])
    extra["direction"] = B._norm_dir_s(df[c_dir])
    extra["line_r"] = pd.to_numeric(df[c_line], errors="coerce").round(2)
    extra["shot_role"] = B._series(
        df, B._col(df, "Shot Role", "shot_role"), numeric=False
    ).astype(str)
    extra["usage_role"] = B._series(
        df, B._col(df, "Usage Role", "usage_role"), numeric=False
    ).astype(str)
    keep = extra[
        [
            "player_n",
            "prop_n",
            "direction",
            "line_r",
            "l5_hits_g",
            "l10_hits_g",
            "szn_hits_g",
            "szn_n_g",
            "shot_role",
            "usage_role",
        ]
    ].drop_duplicates(["player_n", "prop_n", "line_r", "direction"], keep="last")
    return base.merge(keep, on=["player_n", "prop_n", "line_r", "direction"], how="left")


def _game_hits(df: pd.DataFrame, sport: str) -> pd.DataFrame:
    line = pd.to_numeric(df[B._col(df, "Line", "line", "line_score")], errors="coerce")
    direction = B._norm_dir_s(df[B._col(df, "Direction", "direction", "bet_direction")])
    gcols = []
    for i in range(1, 11):
        c = B._col(df, f"G{i}", f"g{i}", f"stat_g{i}", f"stat_G{i}")
        if c:
            gcols.append(c)
    out = pd.DataFrame(index=df.index)
    if not gcols:
        out["l5_hits_g"] = np.nan
        out["l10_hits_g"] = np.nan
        out["szn_hits_g"] = np.nan
        out["szn_n_g"] = np.nan
        return out
    gmat = df[gcols].apply(pd.to_numeric, errors="coerce")
    over = direction.eq("OVER")
    ln = line
    def hits_n(mat, k):
        m = mat.iloc[:, :k]
        ho = m.gt(ln, axis=0)
        hu = m.lt(ln, axis=0)
        nn = m.notna().sum(axis=1)
        h = pd.Series(np.where(over, ho.sum(axis=1), hu.sum(axis=1)), index=df.index)
        h = h.where(nn >= max(3, min(k, 5)), np.nan)
        return h, nn
    out["l5_hits_g"], _ = hits_n(gmat, 5)
    out["l10_hits_g"], n10 = hits_n(gmat, min(10, gmat.shape[1]))
    out["l10_hits_g"] = out["l10_hits_g"].where(n10 >= 8, np.nan)
    out["szn_hits_g"], out["szn_n_g"] = hits_n(gmat, gmat.shape[1])
    return out


def norm_def(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip()
    low = x.str.lower()
    out = pd.Series("", index=s.index, dtype=object)
    out = out.mask(low.str.contains("elite", na=False) | low.eq("top"), "Elite")
    out = out.mask(low.str.contains("above", na=False), "Above Avg")
    out = out.mask(low.str.contains("below", na=False), "Below Avg")
    out = out.mask(low.str.contains("weak", na=False) | low.str.contains("bottom", na=False), "Weak")
    out = out.mask(low.eq("avg") | low.eq("average") | low.str.contains(r"^avg", na=False), "Avg")
    # don't overwrite already set
    already = out.ne("")
    rest = ~already
    avg_mask = rest & (low.eq("avg") | low.eq("average") | low.eq("medium"))
    out = out.mask(avg_mask, "Avg")
    return out


def gap_bucket(g: pd.Series) -> pd.Series:
    out = pd.Series("", index=g.index, dtype=object)
    out = out.mask(g < -1e-9, "against")
    out = out.mask((g >= 0) & (g < 0.5), "0-0.5")
    out = out.mask((g >= 0.5) & (g < 1), "0.5-1")
    out = out.mask((g >= 1) & (g < 2), "1-2")
    out = out.mask(g >= 2, "2+")
    return out


def min_bucket(df: pd.DataFrame) -> pd.Series:
    mt = df["min_tier"].astype(str).str.upper().str.strip()
    out = pd.Series("", index=df.index, dtype=object)
    out = out.mask(mt.str.contains("HIGH", na=False), "HIGH")
    out = out.mask(mt.str.contains("MED", na=False), "MEDIUM")
    out = out.mask(mt.str.contains("LOW", na=False), "LOW")
    am = pd.to_numeric(df.get("avg_min"), errors="coerce")
    unknown = out.eq("")
    out = out.mask(unknown & (am >= 80), "HIGH")
    out = out.mask(unknown & (am >= 45) & (am < 80), "MEDIUM")
    out = out.mask(unknown & am.notna() & (am < 45), "LOW")
    st = df.get("starter_tier", pd.Series("", index=df.index)).astype(str).str.upper()
    unknown = out.eq("")
    out = out.mask(unknown & st.str.contains("START", na=False), "HIGH")
    out = out.mask(unknown & st.str.contains("BENCH", na=False), "LOW")
    return out


def slices(df: pd.DataFrame) -> dict:
    over = df["direction"].eq("OVER")
    l5 = pd.to_numeric(df["l5_hits"], errors="coerce")
    l10 = pd.to_numeric(df["l10_hits"], errors="coerce")
    szn_hr = pd.to_numeric(df["szn_hr"], errors="coerce")
    out = {
        "all": cell(df),
        "L5>=4": cell(df[l5 >= 4]),
        "L5=5": cell(df[l5 == 5]),
        "L5<4": cell(df[l5.notna() & (l5 < 4)]),
        "L10>=8": cell(df[l10 >= 8]),
        "L10 6-7": cell(df[l10.notna() & (l10 >= 6) & (l10 < 8)]),
        "L10<6": cell(df[l10.notna() & (l10 < 6)]),
        "szn cover": cell(df[df["szn_cover"] == True]),  # noqa: E712
        "szn against": cell(df[df["szn_cover"] == False]),  # noqa: E712
        "szn HR>70": cell(df[szn_hr > 0.70]),
        "szn HR 50-70": cell(df[szn_hr.notna() & (szn_hr >= 0.50) & (szn_hr <= 0.70)]),
        "szn HR<50": cell(df[szn_hr.notna() & (szn_hr < 0.50)]),
    }
    for name, col in (("gap L5", "gap_l5_b"), ("gap L10", "gap_l10_b"), ("gap szn", "gap_szn_b")):
        out[name] = {b: cell(df[df[col] == b]) for b in GAP_ORDER}
    out["D"] = {t: cell(df[df["def_b"] == t]) for t in DEF_ORDER}
    out["minutes"] = {t: cell(df[df["min_b"] == t]) for t in MIN_ORDER}
    out["proficiency"] = {t: cell(df[df["prof"] == t]) for t in PROF_ORDER}
    return out


def print_block(title: str, sl: dict) -> None:
    print(f"\n===== {title}  all {fmt(sl['all'])} =====")
    print(
        f"  L5>=4 {fmt(sl['L5>=4'])}   L5=5 {fmt(sl['L5=5'])}   L5<4 {fmt(sl['L5<4'])}"
    )
    print(
        f"  L10>=8 {fmt(sl['L10>=8'])}   L10 6-7 {fmt(sl['L10 6-7'])}   L10<6 {fmt(sl['L10<6'])}"
    )
    print(
        f"  szn cover {fmt(sl['szn cover'])}   against {fmt(sl['szn against'])}   "
        f"HR>70 {fmt(sl['szn HR>70'])}   50-70 {fmt(sl['szn HR 50-70'])}   <50 {fmt(sl['szn HR<50'])}"
    )
    for name in ("gap L5", "gap L10", "gap szn"):
        bits = "  ".join(f"{b} {fmt(sl[name][b])}" for b in GAP_ORDER)
        print(f"  {name}: {bits}")
    bits = "  ".join(f"{t} {fmt(sl['D'][t])}" for t in DEF_ORDER)
    print(f"  D: {bits}")
    bits = "  ".join(f"{t} {fmt(sl['minutes'][t])}" for t in MIN_ORDER)
    print(f"  min: {bits}")
    bits = "  ".join(f"{t} {fmt(sl['proficiency'][t])}" for t in PROF_ORDER)
    print(f"  prof (szn tertile in this prop): {bits}")


def main() -> None:
    dates = sorted(p.name[len("graded_props_") : -5] for p in GRADED.glob("graded_props_20*.json"))
    frames = []
    for date in dates:
        for sport in B.SPORTS:
            p = find_step8(date, sport)
            if not p:
                continue
            print(f"load {date} {sport} {p.name}", flush=True)
            frames.append(load_step8(p, sport, date))
    if not frames:
        raise SystemExit("no step8 boards")
    step8 = pd.concat(frames, ignore_index=True)

    rows = []
    for date in dates:
        gp = GRADED / f"graded_props_{date}.json"
        if not gp.is_file():
            continue
        raw = json.loads(gp.read_text(encoding="utf-8"))
        for g in raw.get("props", []):
            sport = str(g.get("sport") or "")
            if sport not in B.SPORTS:
                continue
            result = str(g.get("result") or "").upper()
            if result not in ("HIT", "MISS"):
                continue
            pk = str(g.get("pick_type") or "").strip().lower()
            if "goblin" in pk:
                pick = "Goblin"
            elif "dem" in pk:
                continue
            else:
                pick = "Standard"
            d = str(g.get("direction") or g.get("over_under") or "").strip().upper()
            if d in ("O", "OVER"):
                d = "OVER"
            elif d in ("U", "UNDER"):
                d = "UNDER"
            else:
                continue
            try:
                line_v = float(g.get("line"))
            except (TypeError, ValueError):
                line_v = math.nan
            rows.append(
                {
                    "date": date,
                    "sport": sport,
                    "player_n": re.sub(
                        r"\s+",
                        " ",
                        re.sub(r"[^a-z0-9 ]+", " ", str(g.get("player") or "").strip().lower()),
                    ).strip(),
                    "prop_n": re.sub(r"\s+", " ", str(g.get("prop") or "").strip().lower()),
                    "line": line_v,
                    "direction": d,
                    "pick": pick,
                    "hit": 1 if result == "HIT" else 0,
                }
            )
    graded = pd.DataFrame(rows)
    graded["line_r"] = graded["line"].round(2)
    keys = ["date", "sport", "player_n", "prop_n", "line_r", "direction"]
    merged = graded.merge(step8, on=keys, how="inner")
    leftover = graded.merge(merged[keys].drop_duplicates().assign(_ok=1), on=keys, how="left")
    leftover = leftover[leftover["_ok"].isna()].drop(columns="_ok")
    loose = leftover.merge(
        step8.drop(columns=["line_r"]).drop_duplicates(
            ["date", "sport", "player_n", "prop_n", "direction"], keep="last"
        ),
        on=["date", "sport", "player_n", "prop_n", "direction"],
        how="inner",
    )
    if len(loose):
        merged = pd.concat([merged, loose], ignore_index=True)

    line = pd.to_numeric(merged["line"], errors="coerce")
    over = merged["direction"].eq("OVER")
    for src, dest in (("l5", "g_l5"), ("l10", "g_l10"), ("szn", "g_szn")):
        avg = pd.to_numeric(merged[src], errors="coerce")
        gap = pd.Series(np.nan, index=merged.index)
        gap = gap.mask(over, avg - line)
        gap = gap.mask(~over, line - avg)
        merged[dest] = gap
    merged["book"] = merged["pick"] + " " + merged["direction"]
    l5_col = np.where(over, merged["l5_over"], merged["l5_under"])
    l10_col = np.where(over, merged.get("l10_over", np.nan), merged.get("l10_under", np.nan))
    merged["l5_hits"] = pd.to_numeric(pd.Series(l5_col, index=merged.index), errors="coerce")
    merged["l10_hits"] = pd.to_numeric(pd.Series(l10_col, index=merged.index), errors="coerce")
    if "l5_hits_g" in merged.columns:
        merged["l5_hits"] = merged["l5_hits"].fillna(pd.to_numeric(merged["l5_hits_g"], errors="coerce"))
    if "l10_hits_g" in merged.columns:
        merged["l10_hits"] = merged["l10_hits"].fillna(pd.to_numeric(merged["l10_hits_g"], errors="coerce"))
    hr = pd.to_numeric(merged.get("hit_rate"), errors="coerce")
    merged["szn_hr"] = np.where(hr > 1.0, hr / 100.0, hr)
    if "szn_hits_g" in merged.columns:
        nh = pd.to_numeric(merged["szn_hits_g"], errors="coerce")
        nn = pd.to_numeric(merged["szn_n_g"], errors="coerce")
        from_g = nh / nn.replace(0, np.nan)
        merged["szn_hr"] = pd.Series(merged["szn_hr"], index=merged.index).fillna(from_g)
    szn = pd.to_numeric(merged["szn"], errors="coerce")
    merged["szn_cover"] = np.where(szn.isna() | line.isna(), np.nan, np.where(over, szn > line, szn < line))
    merged["szn_cover"] = merged["szn_cover"].map({True: True, False: False, 1: True, 0: False})
    merged["gap_l5_b"] = gap_bucket(merged["g_l5"])
    merged["gap_l10_b"] = gap_bucket(merged["g_l10"])
    merged["gap_szn_b"] = gap_bucket(merged["g_szn"])
    merged["def_b"] = norm_def(merged["def_tier"])
    merged["min_b"] = min_bucket(merged)
    merged["prop"] = [T.canon_prop(s, p) for s, p in zip(merged["sport"], merged["prop_n"])]
    pct = merged.groupby(["sport", "prop"])["szn"].rank(pct=True)
    merged["prof"] = np.where(pct.isna(), "", np.where(pct >= 0.67, "Elite", np.where(pct <= 0.33, "Weak", "Mid")))
    merged = merged[merged["book"].isin(BOOKS)]

    payload = {
        "n_joined": int(len(merged)),
        "from": min(merged["date"]) if len(merged) else None,
        "to": max(merged["date"]) if len(merged) else None,
        "note": (
            "Joined graded HIT/MISS to step8. L5/L10 are directional hit counts vs today's line. "
            "Gaps are covering-side (OVER: avg-line, UNDER: line-avg). Proficiency = tertile of "
            "season avg within that sport×prop (Elite = high producers of that stat)."
        ),
        "by_book": {},
        "by_prop": [],
    }
    print(f"joined {len(merged)} rows {payload['from']} -> {payload['to']}")
    for book in BOOKS:
        sl = slices(merged[merged["book"] == book])
        payload["by_book"][book] = sl
        print_block(book, sl)

    cats = (
        merged.groupby(["sport", "book", "prop"])
        .size()
        .reset_index(name="n")
        .sort_values(["sport", "book", "n"], ascending=[True, True, False])
    )
    for _, row in cats.iterrows():
        if row["n"] < MIN_CAT:
            continue
        sub = merged[
            (merged["sport"] == row["sport"])
            & (merged["book"] == row["book"])
            & (merged["prop"] == row["prop"])
        ]
        sl = slices(sub)
        rec = {
            "sport": row["sport"],
            "book": row["book"],
            "prop": row["prop"],
            **sl,
        }
        payload["by_prop"].append(rec)
        print_block(f"{row['sport']} {row['book']} {row['prop']}", sl)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    print("\nwrote", OUT)


if __name__ == "__main__":
    main()
