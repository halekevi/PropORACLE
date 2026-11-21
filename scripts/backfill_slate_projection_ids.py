"""Backfill projection_id / pp_projection_id onto slate_latest.json from step8 boards.

Enables PrizePicks deep links: app.prizepicks.com/?projections={id}-o|{u}-{line}
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _norm(s: object) -> str:
    return " ".join(str(s or "").strip().lower().split())


def _line_key(v: object) -> str:
    try:
        f = float(v)
        if f == int(f):
            return str(int(f))
        return f"{f:.3f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(v or "").strip()


def _row_key(player: object, prop: object, line: object, pick: object, direction: object = "") -> str:
    return "|".join(
        [
            _norm(player),
            _norm(prop),
            _line_key(line),
            _norm(pick),
            _norm(direction)[:1],  # o/u enough when pick+line unique
        ]
    )


def _load_step8_map(sport: str) -> dict[str, dict]:
    sport = sport.lower()
    candidates: list[Path] = []
    sports_dir = ROOT / "Sports" / sport.upper()
    if sport.startswith("nba"):
        sports_dir = ROOT / "Sports" / "NBA"
    elif sport.startswith("wnba"):
        sports_dir = ROOT / "Sports" / "WNBA"
    elif sport.startswith("nfl"):
        sports_dir = ROOT / "Sports" / "NFL"
    elif sport.startswith("nhl"):
        sports_dir = ROOT / "Sports" / "NHL"
    elif sport in ("cfb", "cfb1h"):
        sports_dir = ROOT / "Sports" / "CFB"
    elif sport in ("cbb", "wcbb"):
        sports_dir = ROOT / "Sports" / "CBB"

    patterns = [
        f"step8_{sport}_direction_clean.xlsx",
        f"step8_{sport}_direction_clean.csv",
        f"step8_{sport}_direction.csv",
        "step8_all_direction_clean.xlsx" if sport == "nba" else "",
    ]
    for name in patterns:
        if not name:
            continue
        p = sports_dir / name
        if p.exists():
            candidates.append(p)
        out = ROOT / "outputs"
        if out.exists():
            for hit in sorted(out.glob(f"*/{sport}/{name}"), reverse=True)[:2]:
                candidates.append(hit)

    out_map: dict[str, dict] = {}
    for path in candidates:
        try:
            if path.suffix.lower() == ".xlsx":
                sheet = "ALL" if "all_direction" in path.name.lower() else 0
                df = pd.read_excel(path, sheet_name=sheet)
            else:
                df = pd.read_csv(path)
        except Exception:
            continue
        id_col = next((c for c in ("pp_projection_id", "projection_id") if c in df.columns), None)
        if not id_col:
            continue
        player_c = next((c for c in ("player", "Player") if c in df.columns), None)
        prop_c = next((c for c in ("prop_type", "prop", "Prop") if c in df.columns), None)
        line_c = next((c for c in ("line", "Line") if c in df.columns), None)
        pick_c = next((c for c in ("pick_type", "Pick Type") if c in df.columns), None)
        dir_c = next((c for c in ("direction", "dir", "Direction") if c in df.columns), None)
        if not player_c or not prop_c or not line_c:
            continue
        for _, r in df.iterrows():
            pid = r.get("pp_projection_id")
            if pid is None or (isinstance(pid, float) and pd.isna(pid)):
                pid = r.get("projection_id")
            if pid is None or (isinstance(pid, float) and pd.isna(pid)):
                continue
            try:
                pid_s = str(int(float(pid)))
            except (TypeError, ValueError):
                pid_s = str(pid).strip()
            if not pid_s or pid_s.lower() == "nan":
                continue
            player = r.get(player_c)
            prop = r.get(prop_c)
            line = r.get(line_c)
            pick = r.get(pick_c) if pick_c else ""
            direction = r.get(dir_c) if dir_c else ""
            payload = {"projection_id": pid_s, "pp_projection_id": pid_s}
            # Prefer exact pick_type match; also index by player|prop|line for o/u deep links.
            out_map[_row_key(player, prop, line, pick, direction)] = payload
            loose = _row_key(player, prop, line, "", "")
            out_map.setdefault(loose, payload)
        if out_map:
            break
    return out_map


def backfill(path: Path, *, write: bool) -> dict:
    payload = json.loads(path.read_text(encoding="utf-8"))
    sports = payload.get("sports") or {}
    stats = {"sports": 0, "rows": 0, "filled": 0, "already": 0, "miss": 0}
    for sport, rows in list(sports.items()):
        if not isinstance(rows, list):
            continue
        stats["sports"] += 1
        id_map = _load_step8_map(str(sport))
        for r in rows:
            if not isinstance(r, dict):
                continue
            stats["rows"] += 1
            if r.get("pp_projection_id") or r.get("projection_id"):
                stats["already"] += 1
                continue
            player = r.get("player")
            prop = r.get("prop") or r.get("prop_type")
            line = r.get("line")
            pick = r.get("pick_type")
            direction = r.get("dir")
            hit = (
                id_map.get(_row_key(player, prop, line, pick, direction))
                or id_map.get(_row_key(player, prop, line, pick, ""))
                or id_map.get(_row_key(player, prop, line, "", ""))
            )
            if not hit:
                stats["miss"] += 1
                continue
            r["projection_id"] = hit["projection_id"]
            r["pp_projection_id"] = hit["pp_projection_id"]
            stats["filled"] += 1
    if write:
        path.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
    return stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--path",
        default=str(ROOT / "ui_runner" / "templates" / "slate_latest.json"),
    )
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    path = Path(args.path)
    if not path.exists():
        print(f"missing {path}")
        return 1
    stats = backfill(path, write=bool(args.write))
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
