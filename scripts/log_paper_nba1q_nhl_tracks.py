"""Start forward paper tracks for the two product candidates.

Tracks (placed=false, off /tickets):
  - nba1q_goblin_reb_ast  — NBA1Q Goblin OVER Rebounds/Assists, L5=5
  - nhl_std_under_pts_ast — NHL Standard UNDER Points/Assists, L5=5+L10>=8,
                            max 1 leg per game

Promotion bar (existing shadow rule): n>=100 forward legs AND Wilson lower
bound above break-even at real quotes (Goblin Power ~84.5%; Standard ~57-59%
for 2-3 pick — use quote when scraped).

Writes / appends to:
  - data/reports/track_comparison.jsonl (both worktrees if main_cp exists)
  - data/reports/paper_tracks_registry.json
  - PropORACLE_main_cp mirrors when present

  py -3.14 scripts/log_paper_nba1q_nhl_tracks.py
  py -3.14 scripts/log_paper_nba1q_nhl_tracks.py --date 2026-10-23
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
_MAIN = _REPO.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(_REPO), str(_REPO / "scripts")]

from utils.comparison_tracks import wilson_band  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402

ET = ZoneInfo("America/New_York")
GOBLIN_POWER_BAR = 0.845
STD_BE_BAR = 0.58  # ~2–3 pick Standard break-even band midpoint

TRACK_NBA1Q = "nba1q_goblin_reb_ast"
TRACK_NHL = "nhl_std_under_pts_ast"
NBA1Q_PROPS = frozenset({"rebounds", "assists", "reb", "ast"})
NHL_PROPS = frozenset({"points", "assists"})


def _today_et() -> str:
    return datetime.now(ET).strftime("%Y-%m-%d")


def _ci(hits: int, n: int) -> dict:
    band = wilson_band(hits, n) if n else None
    return {
        "hits": hits,
        "n": n,
        "hit_rate": round(hits / n, 4) if n else None,
        "wilson_low": round(band[0], 4) if band else None,
        "wilson_high": round(band[1], 4) if band else None,
    }


def _num(v):
    try:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            return None
        return float(v)
    except Exception:
        return None


def _s(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return ""
    return str(v).strip()


def _l5(row, over: bool):
    keys = ("l5_over", "last5_over", "L5 Over") if over else ("l5_under", "last5_under", "L5 Under")
    for k in keys:
        if k in row.index:
            n = _num(row.get(k))
            if n is not None:
                return n
    return None


def _l10(row, over: bool):
    keys = ("l10_over", "last10_over", "L10 Over") if over else ("l10_under", "last10_under", "L10 Under")
    for k in keys:
        if k in row.index:
            n = _num(row.get(k))
            if n is not None:
                return n
    return None


def _pick(row) -> str:
    return _s(row.get("pick_type") or row.get("Pick Type")).lower()


def _load_step8(root: Path, date: str, sport_folder: str, patterns: list[str]) -> pd.DataFrame | None:
    base = root / "outputs" / date
    candidates: list[Path] = []
    for pat in patterns:
        candidates.append(base / sport_folder / pat)
        candidates.append(base / pat)
    for p in candidates:
        if p.is_file() and p.stat().st_size > 0:
            try:
                if p.suffix.lower() == ".csv":
                    return pd.read_csv(p, low_memory=False)
                xl = pd.ExcelFile(p)
                sheet = "ALL" if "ALL" in xl.sheet_names else (
                    "All Props" if "All Props" in xl.sheet_names else xl.sheet_names[-1]
                )
                raw = pd.read_excel(p, sheet_name=sheet, header=None)
                header_i = 0
                for i in range(min(8, len(raw))):
                    cells = [str(x).strip().lower() for x in raw.iloc[i].tolist()[:20]]
                    if "player" in cells or "prop" in cells:
                        header_i = i
                        break
                return pd.read_excel(p, sheet_name=sheet, header=header_i).dropna(how="all")
            except Exception as e:
                print(f"  WARN read {p}: {e}")
    return None


def _append_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # drop prior same-track+date rows
    dates = {r["date"] for r in rows}
    tracks = {r["track"] for r in rows}
    keep: list[str] = []
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
            except Exception:
                keep.append(line)
                continue
            if obj.get("track") in tracks and obj.get("date") in dates:
                continue
            keep.append(line)
    with path.open("w", encoding="utf-8") as f:
        for line in keep:
            f.write(line + "\n")
        for r in rows:
            f.write(json.dumps(r, default=str) + "\n")


def collect_nba1q(root: Path, date: str) -> list[dict]:
    df = _load_step8(
        root,
        date,
        "nba1q",
        [
            "step8_nba1q_direction_clean.xlsx",
            f"step8_nba1q_direction_clean_{date}.xlsx",
            "step8_nba1q_direction_clean.csv",
        ],
    )
    if df is None or df.empty:
        return []
    cols = {str(c).strip().lower(): c for c in df.columns}
    pc = cols.get("player") or cols.get("player_name")
    propc = cols.get("prop") or cols.get("prop_type") or cols.get("stat_type")
    linec = cols.get("line") or cols.get("line_score")
    teamc = cols.get("team")
    oppc = cols.get("opponent") or cols.get("opp_team")
    rows: list[dict] = []
    for _, r in df.iterrows():
        if "goblin" not in _pick(r):
            continue
        prop = canon_prop("NBA1Q", _s(r.get(propc)))
        prop_l = prop.lower().replace("_", " ")
        if prop_l not in {"rebounds", "assists"} and prop not in {"rebounds", "assists"}:
            continue
        l5 = _l5(r, True)
        if l5 is None or int(l5) != 5:
            continue
        line = _num(r.get(linec))
        player = _s(r.get(pc))
        rows.append(
            {
                "date": date,
                "id": f"{TRACK_NBA1Q}-{date}-{player}-{prop}-{line}",
                "track": TRACK_NBA1Q,
                "kind": "leg",
                "placed": False,
                "exclude_from_book": True,
                "ev_label": "paper",
                "sport": "NBA1Q",
                "player": player,
                "prop": prop,
                "prop_canon": prop,
                "pick_type": "Goblin",
                "direction": "OVER",
                "line": line,
                "team": _s(r.get(teamc)) if teamc else "",
                "opponent": _s(r.get(oppc)) if oppc else "",
                "l5_over": l5,
                "p": 0.931,  # audit after-filter reb cell; update when n grows
                "p_source": "audit_nba1q_reb_after_leak_proxy",
                "stake": 20,
                "result": None,
                "promotion_bar": {
                    "n_min": 100,
                    "wilson_low_min": GOBLIN_POWER_BAR,
                    "note": "Goblin Power ~84.5% at real quotes",
                },
                "logged_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return rows


def collect_nhl(root: Path, date: str) -> list[dict]:
    df = _load_step8(
        root,
        date,
        "nhl",
        [
            "step8_nhl_direction_clean.xlsx",
            f"step8_nhl_direction_clean_{date}.xlsx",
            "step8_nhl_direction_clean.csv",
        ],
    )
    if df is None or df.empty:
        return []
    cols = {str(c).strip().lower(): c for c in df.columns}
    pc = cols.get("player") or cols.get("player_name")
    propc = cols.get("prop") or cols.get("prop_type") or cols.get("stat_type")
    linec = cols.get("line") or cols.get("line_score")
    teamc = cols.get("team")
    oppc = cols.get("opponent") or cols.get("opp_team")
    gdc = cols.get("game_date") or cols.get("game date")
    gidc = cols.get("game_id")
    side_c = cols.get("recommended_side") or cols.get("final_bet_direction") or cols.get("side")
    candidates: list[dict] = []
    for _, r in df.iterrows():
        pt = _pick(r)
        if pt and "standard" not in pt and pt not in {"", "-"}:
            # empty often means standard on NHL boards
            if "goblin" in pt or "demon" in pt:
                continue
        prop = canon_prop("NHL", _s(r.get(propc)))
        prop_l = prop.lower().replace("_", " ")
        if prop_l not in NHL_PROPS:
            continue
        # prefer explicit UNDER
        side = _s(r.get(side_c)).upper() if side_c else ""
        if side and side not in {"U", "UNDER", "LESS"}:
            continue
        if not side:
            # if no side column, require l5_under==5
            pass
        l5u = _l5(r, False)
        l10u = _l10(r, False)
        if l5u is None or int(l5u) != 5:
            continue
        if l10u is None or float(l10u) < 8:
            continue
        line = _num(r.get(linec))
        player = _s(r.get(pc))
        team = _s(r.get(teamc)) if teamc else ""
        opp = _s(r.get(oppc)) if oppc else ""
        gd = _s(r.get(gdc))[:10] if gdc else date
        gid = _s(r.get(gidc)) if gidc else f"{gd}|{'-'.join(sorted([team, opp]))}"
        candidates.append(
            {
                "date": date,
                "game_id": gid,
                "player": player,
                "prop": prop,
                "line": line,
                "team": team,
                "opponent": opp,
                "l5_under": l5u,
                "l10_under": l10u,
            }
        )
    # max 1 per game — prefer Points over Assists, then lower line
    by_game: dict[str, list[dict]] = {}
    for c in candidates:
        by_game.setdefault(c["game_id"], []).append(c)
    rows: list[dict] = []
    for gid, group in by_game.items():
        group.sort(
            key=lambda x: (
                0 if x["prop"].lower() == "points" else 1,
                x["line"] if x["line"] is not None else 99,
                x["player"],
            )
        )
        c = group[0]
        rows.append(
            {
                "date": date,
                "id": f"{TRACK_NHL}-{date}-{c['player']}-{c['prop']}-{c['line']}",
                "track": TRACK_NHL,
                "kind": "leg",
                "placed": False,
                "exclude_from_book": True,
                "ev_label": "paper",
                "sport": "NHL",
                "player": c["player"],
                "prop": c["prop"],
                "prop_canon": c["prop"],
                "pick_type": "Standard",
                "direction": "UNDER",
                "line": c["line"],
                "team": c["team"],
                "opponent": c["opponent"],
                "game_id": gid,
                "l5_under": c["l5_under"],
                "l10_under": c["l10_under"],
                "p": 0.835,
                "p_source": "audit_nhl_under_points_after_leak",
                "stake": 20,
                "result": None,
                "slip_rule": "max_1_leg_per_game",
                "promotion_bar": {
                    "n_min": 100,
                    "wilson_low_min": STD_BE_BAR,
                    "note": "Standard BE ~57-59%; raise when real Flex/Power quotes attached",
                },
                "logged_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return rows


def _update_registry(roots: list[Path], date: str, counts: dict) -> None:
    for root in roots:
        path = root / "data" / "reports" / "paper_tracks_registry.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        reg = {}
        if path.is_file():
            try:
                reg = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                reg = {}
        tracks = reg.setdefault("tracks", {})
        for track, n in counts.items():
            entry = tracks.setdefault(
                track,
                {
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "placed": False,
                    "promotion": {
                        "n_min": 100,
                        "wilson_low_min": GOBLIN_POWER_BAR
                        if track == TRACK_NBA1Q
                        else STD_BE_BAR,
                    },
                    "dates": {},
                    "forward_n": 0,
                    "forward_hits": None,
                    "status": "active_paper",
                },
            )
            entry["dates"][date] = {"legs_logged": n, "logged_at": datetime.now(timezone.utc).isoformat()}
            entry["forward_n"] = sum(v.get("legs_logged", 0) for v in entry["dates"].values())
            entry["updated_at"] = datetime.now(timezone.utc).isoformat()
        reg["updated_at"] = datetime.now(timezone.utc).isoformat()
        path.write_text(json.dumps(reg, indent=2), encoding="utf-8")
        print(f"registry → {path}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=_today_et())
    args = ap.parse_args()
    date = args.date

    roots = [_REPO]
    if _MAIN.is_dir():
        roots.append(_MAIN)

    nba_rows = collect_nba1q(_REPO, date)
    if not nba_rows and _MAIN.is_dir():
        nba_rows = collect_nba1q(_MAIN, date)
    nhl_rows = collect_nhl(_REPO, date)
    if not nhl_rows and _MAIN.is_dir():
        nhl_rows = collect_nhl(_MAIN, date)

    all_rows = nba_rows + nhl_rows
    for root in roots:
        log = root / "data" / "reports" / "track_comparison.jsonl"
        _append_jsonl(log, all_rows)
        print(f"jsonl → {log} (+{len(all_rows)} rows for {date})")

    _update_registry(
        roots,
        date,
        {TRACK_NBA1Q: len(nba_rows), TRACK_NHL: len(nhl_rows)},
    )

    summary = {
        "date": date,
        TRACK_NBA1Q: {"legs": len(nba_rows), "note": "needs NBA1Q step8; 0 if board missing"},
        TRACK_NHL: {"legs": len(nhl_rows), "rule": "max 1 per game"},
        "promotion": "n>=100 forward + Wilson low > BE at real quotes",
    }
    out = _REPO / "data" / "reports" / f"paper_track_seed_{date}.json"
    out.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    if _MAIN.is_dir():
        (_MAIN / "data" / "reports" / f"paper_track_seed_{date}.json").write_text(
            json.dumps(summary, indent=2), encoding="utf-8"
        )
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
