"""All-sport L5=5 props for a tip date, by Goblin / Std O / Std U, ranked by gap.

Gap = directional distance of L5 average to the posted line:
  OVER:  L5avg - line
  UNDER: line - L5avg
Larger gap = more cushion on the play side.
"""
from __future__ import annotations

import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from utils.cfb_keep_gates import cfb_list_eligible  # noqa: E402
from utils.soccer_keep_gates import soccer_list_eligible  # noqa: E402
from utils.tennis_keep_gates import tennis_list_eligible  # noqa: E402

try:
    from utils.mlb_keep_gates import mlb_list_eligible
except Exception:
    mlb_list_eligible = None  # type: ignore


def _as_float(x):
    try:
        v = float(x)
        if v != v:
            return None
        return v
    except (TypeError, ValueError):
        return None


def _load_any(path: Path) -> pd.DataFrame:
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path, low_memory=False)
    # xlsx — handle banner/meta sheets
    xl = pd.ExcelFile(path)
    sheet = "ALL" if "ALL" in xl.sheet_names else xl.sheet_names[-1]
    raw = pd.read_excel(xl, sheet_name=sheet, header=None)
    header_i = 0
    for i in range(min(8, len(raw))):
        cells = [str(x).strip().lower() for x in raw.iloc[i].tolist()[:25]]
        if "player" in cells or "prop" in cells or "prop_type" in cells:
            header_i = i
            break
    df = pd.read_excel(xl, sheet_name=sheet, header=header_i)
    return df.dropna(how="all")


def _find_board(sport: str, slate: str) -> Path | None:
    """Prefer same-day clean parquet/xlsx; fall back to slate-1."""
    y, m, d = map(int, slate.split("-"))
    prev = (date(y, m, d) - timedelta(days=1)).isoformat()
    names = {
        "nba": [
            f"outputs/{slate}/nba/step8_all_direction_clean.parquet",
            f"outputs/{slate}/step8_nba_direction_clean_{slate}.parquet",
            f"outputs/{slate}/step8_nba_direction_clean_{slate}.xlsx",
            f"outputs/{slate}/nba/step8_all_direction_clean.xlsx",
            f"outputs/{prev}/nba/step8_all_direction_clean.parquet",
            f"outputs/{prev}/step8_nba_direction_clean_{prev}.parquet",
        ],
        "cfb": [
            f"outputs/{slate}/cfb/step8_cfb_direction_clean.parquet",
            f"outputs/{prev}/cfb/step8_cfb_direction_clean.parquet",
        ],
        "soccer": [
            f"outputs/{slate}/soccer/step8_soccer_direction_clean.parquet",
            f"outputs/{prev}/soccer/step8_soccer_direction_clean.parquet",
            f"outputs/{prev}/soccer/step8_soccer_direction.parquet",
        ],
        "tennis": [
            f"outputs/{slate}/tennis/step8_tennis_direction_clean.parquet",
            f"outputs/{prev}/tennis/step8_tennis_direction_clean.parquet",
            f"outputs/{prev}/tennis/step8_tennis_direction_clean.xlsx",
        ],
        "nhl": [
            f"outputs/{slate}/nhl/step8_nhl_direction_clean.parquet",
            f"outputs/{prev}/nhl/step8_nhl_direction_clean.parquet",
        ],
        "nfl": [
            f"outputs/{slate}/nfl/step8_nfl_direction_clean.parquet",
            f"outputs/{prev}/nfl/step8_nfl_direction_clean.parquet",
        ],
        "wnba": [
            f"outputs/{slate}/wnba/step8_wnba_direction_clean.parquet",
            f"outputs/{prev}/wnba/step8_wnba_direction_clean.parquet",
        ],
        "mlb": [
            f"outputs/{slate}/mlb/step8_mlb_direction_clean.parquet",
            f"outputs/{slate}/step8_mlb_direction_clean_{slate}.xlsx",
            f"outputs/{prev}/mlb/step8_mlb_direction_clean.parquet",
            f"Sports/MLB/step8_mlb_direction_clean.xlsx",
        ],
        "golf": [
            f"outputs/{slate}/golf/step8_golf_direction_clean.parquet",
            f"outputs/{prev}/golf/step8_golf_direction_clean.parquet",
        ],
    }
    for rel in names.get(sport, []):
        p = ROOT / rel
        if p.exists() and p.stat().st_size > 2000:
            return p
    return None


def _col(df: pd.DataFrame, *cands: str) -> str | None:
    lower = {str(c).strip().lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in lower:
            return lower[c.lower()]
    return None


def _tip_ok(row: pd.Series, tip_col: str | None, slate: str, sport: str) -> bool:
    """Keep rows tipping on slate (ET date). Tennis keeps slate..slate+2."""
    if tip_col is None:
        return True
    raw = str(row.get(tip_col) or "")
    day = raw[:10]
    if len(day) < 10:
        return True
    if sport == "tennis":
        try:
            d0 = date.fromisoformat(slate)
            d = date.fromisoformat(day)
            return d0 <= d <= d0 + timedelta(days=2)
        except ValueError:
            return True
    return day == slate


def _l5_avg(row: pd.Series, side: str, df_cols: list[str]) -> float | None:
    # Prefer explicit last-5 average columns
    for c in (
        "stat_last5_avg",
        "Last 5 Avg",
        "last5_avg",
        "l5_avg",
        "L5 Avg",
        "Hit Rate L5",  # not avg — skip later
    ):
        if c in row.index and c != "Hit Rate L5":
            v = _as_float(row.get(c))
            if v is not None:
                return v
    # Reconstruct from stat_g1..g5 when present
    vals = []
    for i in range(1, 6):
        for name in (f"stat_g{i}", f"Stat G{i}", f"g{i}"):
            if name in row.index:
                v = _as_float(row.get(name))
                if v is not None:
                    vals.append(v)
                break
    if len(vals) >= 5:
        return sum(vals[:5]) / 5.0
    return None


def _l5_hits(row: pd.Series, side: str) -> float | None:
    if side == "OVER":
        for c in ("L5 Over", "l5_over", "last5_over", "Last5 Over"):
            if c in row.index:
                return _as_float(row.get(c))
    else:
        for c in ("L5 Under", "l5_under", "last5_under", "Last5 Under"):
            if c in row.index:
                return _as_float(row.get(c))
    return None


def _keep(sport: str, rec: dict) -> bool:
    if sport == "cfb":
        return cfb_list_eligible(rec)
    if sport == "soccer":
        try:
            return soccer_list_eligible(rec)
        except Exception:
            return True
    if sport == "tennis":
        try:
            return tennis_list_eligible(rec)
        except Exception:
            return True
    if sport == "mlb" and mlb_list_eligible is not None:
        try:
            return mlb_list_eligible(rec)
        except Exception:
            return True
    return True


def extract(sport: str, slate: str) -> list[dict]:
    path = _find_board(sport, slate)
    if path is None:
        return []
    try:
        df = _load_any(path)
    except Exception as e:
        print(f"[{sport}] load fail {path.name}: {e}", file=sys.stderr)
        return []
    if df.empty or len(df.columns) < 5:
        return []

    player_c = _col(df, "player", "Player", "name")
    prop_c = _col(df, "prop", "Prop", "prop_type", "prop_norm", "stat_type")
    line_c = _col(df, "line", "Line", "projection_line")
    pick_c = _col(df, "pick_type", "Pick Type", "projection_type")
    side_c = _col(df, "side", "Side", "direction", "Direction", "recommended_side")
    tip_c = _col(df, "Game Time", "game_time", "start_time", "Game Date", "game_date")
    team_c = _col(df, "team", "Team")
    opp_c = _col(df, "opp", "Opp", "opp_team", "Opp Team")
    if not all([player_c, prop_c, line_c]):
        print(f"[{sport}] missing cols in {path.name}", file=sys.stderr)
        return []

    out: list[dict] = []
    for _, row in df.iterrows():
        if not _tip_ok(row, tip_c, slate, sport):
            continue
        pt = str(row.get(pick_c) if pick_c else "").strip().lower()
        if "demon" in pt:
            continue
        side = str(row.get(side_c) if side_c else "").strip().upper()
        if side in {"O", "MORE"}:
            side = "OVER"
        if side in {"U", "LESS"}:
            side = "UNDER"
        if not side:
            side = "OVER" if "goblin" in pt else ""
        if side not in {"OVER", "UNDER"}:
            continue
        if "goblin" in pt and side != "OVER":
            continue
        if pt and ("goblin" not in pt) and ("standard" not in pt):
            # blank pick_type: treat as unknown — skip unless clearly goblin/standard
            continue

        hits = _l5_hits(row, side)
        if hits != 5.0:
            continue
        line = _as_float(row.get(line_c))
        if line is None:
            continue
        l5avg = _l5_avg(row, side, list(df.columns))
        if l5avg is None:
            # fall back: use season avg if present (worse)
            for c in ("stat_season_avg", "Season Avg", "season_avg", "szn"):
                if c in row.index:
                    l5avg = _as_float(row.get(c))
                    break
        if l5avg is None:
            continue
        gap = (l5avg - line) if side == "OVER" else (line - l5avg)

        rec = {
            "sport": sport.upper(),
            "player": str(row.get(player_c) or "").strip(),
            "team": str(row.get(team_c) or "").strip() if team_c else "",
            "opp": str(row.get(opp_c) or "").strip() if opp_c else "",
            "prop": str(row.get(prop_c) or "").strip(),
            "pick_type": "goblin" if "goblin" in pt else "standard",
            "side": side,
            "line": line,
            "l5avg": l5avg,
            "gap": gap,
            "tip": str(row.get(tip_c) or "")[:16].replace("T", " ") if tip_c else "",
            "source": str(path.relative_to(ROOT)),
            "Prop": str(row.get(prop_c) or "").strip(),
            "Pick Type": pt,
            "Direction": side,
            "Line": line,
            "L5 Over": row.get(_col(df, "L5 Over", "l5_over", "last5_over") or "L5 Over")
            if _col(df, "L5 Over", "l5_over", "last5_over")
            else None,
            "L5 Under": row.get(_col(df, "L5 Under", "l5_under", "last5_under") or "L5 Under")
            if _col(df, "L5 Under", "l5_under", "last5_under")
            else None,
            "Def Tier": str(
                row.get(_col(df, "Def Tier", "def_tier") or "Def Tier") or ""
            )
            if _col(df, "Def Tier", "def_tier")
            else "",
        }
        if not _keep(sport, rec):
            continue
        # Soccer Saves off list/tickets
        if sport == "soccer" and "save" in rec["prop"].lower():
            continue
        out.append(rec)
    return out


def main() -> int:
    slate = sys.argv[1] if len(sys.argv) > 1 else "2026-10-22"
    sports = ["nba", "cfb", "soccer", "tennis", "nhl", "nfl", "wnba", "mlb", "golf"]
    all_rows: list[dict] = []
    sources: list[str] = []
    for sp in sports:
        rows = extract(sp, slate)
        if rows:
            sources.append(f"{sp}:{rows[0]['source']} n={len(rows)}")
        else:
            path = _find_board(sp, slate)
            sources.append(f"{sp}:{'missing' if path is None else path.name+' (0 L5=5)'}")
        all_rows.extend(rows)

    buckets = {
        "Goblin OVER": [r for r in all_rows if r["pick_type"] == "goblin"],
        "Standard OVER": [
            r for r in all_rows if r["pick_type"] == "standard" and r["side"] == "OVER"
        ],
        "Standard UNDER": [
            r for r in all_rows if r["pick_type"] == "standard" and r["side"] == "UNDER"
        ],
    }

    lines: list[str] = []
    lines.append(f"L5=5 props  tip/slate={slate}  ranked by gap (L5avg vs line, play-side)")
    lines.append("gap OVER = L5avg-line | gap UNDER = line-L5avg")
    lines.append("sources: " + " | ".join(sources))
    lines.append("")

    for name, rows in buckets.items():
        rows.sort(key=lambda r: (-r["gap"], r["sport"], r["player"], r["prop"], r["line"]))
        lines.append(f"===== {name} (n={len(rows)}) =====")
        if not rows:
            lines.append("  (none)")
            lines.append("")
            continue
        for i, r in enumerate(rows, 1):
            lines.append(
                f"{i:3d}. [{r['sport']:<6}] {r['player']:<24} {r['prop']:<22} "
                f"{r['side'][0]}{r['line']:<7.1f}  L5avg {r['l5avg']:>6.2f}  "
                f"gap {r['gap']:+6.2f}  {r['team']} vs {r['opp']}  {r['tip']}"
            )
        lines.append("")

    text = "\n".join(lines) + "\n"
    out = ROOT / "data" / "reports" / f"l5eq5_by_gap_{slate}.txt"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(text, encoding="utf-8")
    sync = ROOT / "outputs" / slate
    sync.mkdir(parents=True, exist_ok=True)
    (sync / out.name).write_text(text, encoding="utf-8")
    print(text)
    print(f"wrote {out}")
    print(f"synced {sync / out.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
