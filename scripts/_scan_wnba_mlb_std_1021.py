"""Scan WNBA/MLB for Standard directional L5=5 on available step8 boards."""
from __future__ import annotations

import math
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.cushion_z import l5_clears, play_side_gap
from utils.l5_ranked_by_cell import five_games, gap_z

BOARDS = {
    "WNBA": Path(r"H:\PropORACLE_main_cp")
    / "outputs"
    / "2026-10-21"
    / "wnba"
    / "step8_wnba_direction_clean.xlsx",
    "MLB": Path(r"H:\PropORACLE_main_cp")
    / "outputs"
    / "2026-10-21"
    / "mlb"
    / "step1_mlb_props.csv",
}


def _col(df: pd.DataFrame, *names: str) -> str | None:
    for n in names:
        if n in df.columns:
            return n
    return None


def _side(r: pd.Series) -> str | None:
    for k in ("Direction", "final_bet_direction", "bet_direction", "side", "Dir"):
        if k not in r.index:
            continue
        s = str(r.get(k) or "").strip().upper()
        if s in {"OVER", "UNDER", "O", "U"}:
            return "OVER" if s in {"OVER", "O"} else "UNDER"
    return None


def _date_hint(df: pd.DataFrame) -> str:
    bits = []
    for c in df.columns:
        cl = str(c).lower()
        if any(x in cl for x in ("fetch", "board_date", "game_date", "start_time")):
            vc = df[c].astype(str).value_counts().head(2).to_dict()
            bits.append(f"{c}={vc}")
    return "; ".join(bits) if bits else "(no date cols)"


def scan(sport: str, path: Path) -> None:
    print(f"==== {sport} ====")
    if not path.exists():
        print(f"  missing {path}")
        return
    xl = pd.ExcelFile(path)
    sheet = "ALL" if "ALL" in xl.sheet_names else xl.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet)
    print(f"  file={path.name} sheet={sheet} rows={len(df)}")
    print(f"  dates: {_date_hint(df)}")

    pc = _col(df, "Pick Type", "pick_type")
    pl = _col(df, "Player", "player")
    pr = _col(df, "Prop", "prop_type", "prop")
    ln = _col(df, "Line", "line")
    tc = _col(df, "Tier", "tier")
    if not all([pc, pl, pr, ln]):
        print("  missing core columns")
        return

    picks = df[pc].astype(str).value_counts().head(8).to_dict()
    print(f"  pick_types: {picks}")

    std = df[df[pc].astype(str).str.contains("Standard", case=False, na=False)].copy()
    std = std[~std[pc].astype(str).str.contains("Demon", case=False, na=False)]
    print(f"  standard_rows={len(std)}")

    clears: list[dict] = []
    no_vals = 0
    for _, r in std.iterrows():
        side = _side(r)
        if side is None:
            continue
        try:
            line = float(r[ln])
        except (TypeError, ValueError):
            continue
        if math.isnan(line):
            continue
        d = r.to_dict()
        for i in range(1, 6):
            if d.get(f"stat_g{i}") in (None, "") or (
                isinstance(d.get(f"stat_g{i}"), float) and math.isnan(d.get(f"stat_g{i}"))
            ):
                for alt in (f"Stat G{i}", f"G{i}", f"g{i}"):
                    if alt in d and d.get(alt) not in (None, ""):
                        d[f"stat_g{i}"] = d[alt]
                        break
        vals = five_games(d)
        if vals is None:
            no_vals += 1
            # fallback: directional hit count == 5
            lo = _col(std, "l5_over", "L5 Over", "last5_over")
            lu = _col(std, "l5_under", "L5 Under", "last5_under")
            hit_col = lo if side == "OVER" else lu
            if hit_col is None:
                continue
            try:
                hits = float(r[hit_col])
            except (TypeError, ValueError):
                continue
            if hits != 5:
                continue
            clears.append(
                {
                    "player": str(r[pl]),
                    "prop": str(r[pr]),
                    "side": side,
                    "line": line,
                    "vals": None,
                    "l5a": None,
                    "gap": None,
                    "gz": -1e9,
                    "tier": str(r[tc]) if tc else "?",
                    "via": "hit_count",
                }
            )
            continue
        if not l5_clears(vals, line, side):
            continue
        l5a = sum(vals) / 5.0
        gap = play_side_gap(l5a, line, side)
        gz = gap_z(vals, line, side)
        clears.append(
            {
                "player": str(r[pl]),
                "prop": str(r[pr]),
                "side": side,
                "line": line,
                "vals": vals,
                "l5a": l5a,
                "gap": gap,
                "gz": gz if gz is not None else -1e9,
                "tier": str(r[tc]) if tc else "?",
                "via": "values",
            }
        )

    print(f"  L5=5 clears={len(clears)} (no_series_rows={no_vals})")
    clears.sort(key=lambda x: (-x["gz"], x["player"], x["prop"]))
    seen: set[str] = set()
    best: list[dict] = []
    for c in clears:
        k = c["player"].casefold()
        if k in seen:
            continue
        seen.add(k)
        best.append(c)

    print(f"  unique_players={len(best)}")
    if not best:
        print("  (none)")
        return
    for i, c in enumerate(best, 1):
        if c["vals"] is not None:
            vals = ",".join(f"{v:g}" for v in c["vals"])
            l5a = f"{c['l5a']:.2f}"
            gap = f"{c['gap']:.2f}" if c["gap"] is not None else "-"
            gz = f"{c['gz']:.2f}" if c["gz"] > -1e8 else "-"
        else:
            vals, l5a, gap, gz = "-", "-", "-", "-"
        print(
            f"  {i:2d}. [{c['tier']}] {c['player']} | {c['prop']} {c['side']} {c['line']} | "
            f"L5={vals} avg={l5a} gap={gap} gz={gz} ({c['via']})"
        )


def main() -> None:
    # Prefer dated outputs if present
    for sport, fallback in BOARDS.items():
        dated = list((ROOT / "outputs" / "2026-10-21").glob(f"**/*{sport.lower()}*step8*"))
        dated += list((ROOT / "outputs").glob(f"*step8*{sport.lower()}*"))
        path = fallback
        for p in dated:
            if p.suffix.lower() in {".xlsx", ".parquet", ".csv"} and p.exists():
                path = p
                break
        # also main_cp
        mcp = Path(r"H:\PropORACLE_main_cp") / "outputs" / "2026-10-21" / sport.lower()
        if mcp.exists():
            for p in mcp.glob("step8*"):
                if p.suffix.lower() == ".xlsx":
                    path = p
                    break
        scan(sport, path if path.exists() else fallback)


if __name__ == "__main__":
    main()
