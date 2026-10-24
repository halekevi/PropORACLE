"""Fetch Jaylon Tyson box scores from ESPN for CLE 2026 preseason games."""
from __future__ import annotations

import json
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "data" / "reports" / "jaylon_tyson_preseason_boxes_2026.txt"

EVENTS = [
    ("2026-10-08", "401898392", "BOS @ CLE"),
    ("2026-10-11", "401898401", "ORL @ CLE"),
    ("2026-10-13", "401898403", "CLE @ ORL"),
]

SUMMARY = (
    "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/"
    "summary?event={eid}"
)


def fetch_event(eid: str) -> dict:
    url = SUMMARY.format(eid=eid)
    req = urllib.request.Request(url, headers={"User-Agent": "PropORACLE/1.0"})
    with urllib.request.urlopen(req, timeout=45) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _split_made_att(s: str) -> tuple[float | None, float | None]:
    parts = str(s).split("-")
    if len(parts) != 2:
        return None, None
    try:
        return float(parts[0]), float(parts[1])
    except ValueError:
        return None, None


def parse_tyson(summary: dict) -> dict | None:
    box = summary.get("boxscore") or {}
    for team in box.get("players") or []:
        team_abbr = (team.get("team") or {}).get("abbreviation")
        for group in team.get("statistics") or []:
            labels = [str(x) for x in (group.get("labels") or group.get("names") or [])]
            keys = [str(x).lower() for x in (group.get("keys") or [])]
            cols = keys if keys else [x.lower() for x in labels]
            for ath in group.get("athletes") or []:
                name = (ath.get("athlete") or {}).get("displayName") or ""
                if "tyson" not in name.lower():
                    continue
                raw = ath.get("stats") or []
                mapped = {c: raw[i] if i < len(raw) else None for i, c in enumerate(cols)}
                # Also map by label position if keys empty
                if not cols and labels:
                    mapped = {
                        str(labels[i]).lower(): raw[i] if i < len(raw) else None
                        for i in range(len(labels))
                    }

                def g(*names: str) -> str | None:
                    for n in names:
                        if n in mapped and mapped[n] not in (None, ""):
                            return str(mapped[n])
                    return None

                mins = g("min", "minutes")
                pts = g("pts", "points")
                reb = g("reb", "rebounds", "totalrebounds")
                ast = g("ast", "assists")
                stl = g("stl", "steals")
                blk = g("blk", "blocks")
                tov = g("to", "tov", "turnovers")
                fg_s = g("fg", "fieldgoalsmade-fieldgoalsattempted", "fieldgoals")
                fg3_s = g(
                    "3pt",
                    "fg3",
                    "threepointfieldgoalsmade-threepointfieldgoalsattempted",
                    "threepointfieldgoals",
                )
                ft_s = g(
                    "ft",
                    "freethrowsmade-freethrowsattempted",
                    "freethrows",
                )
                # ESPN sometimes uses short labels: MIN, FG, 3PT, FT, OREB, DREB, REB, AST, STL, BLK, TO, PF, +/- , PTS
                if fg_s is None and len(raw) >= 14 and not cols:
                    # positional fallback from dump: MIN, PTS?, FG, 3PT, FT... wait dump was:
                    # "17","14","6-8","1-2","1-1","3","2","0","0","0","0","3","2","-6"
                    # That looks like MIN, PTS, FG, 3PT, FT, OREB, DREB, REB, AST, STL, BLK, TO, PF, +/-
                    mins, pts = raw[0], raw[1]
                    fg_s, fg3_s, ft_s = raw[2], raw[3], raw[4]
                    reb, ast, stl, blk, tov = raw[7], raw[8], raw[9], raw[10], raw[11]

                fgm, fga = _split_made_att(fg_s or "")
                fg3m, fg3a = _split_made_att(fg3_s or "")
                ftm, fta = _split_made_att(ft_s or "")
                fg2a = (fga - fg3a) if fga is not None and fg3a is not None else None
                fg2m = (fgm - fg3m) if fgm is not None and fg3m is not None else None

                def fnum(x: object) -> float | None:
                    try:
                        return float(x) if x is not None and str(x) != "" else None
                    except ValueError:
                        return None

                return {
                    "player": name,
                    "team": team_abbr,
                    "starter": bool(ath.get("starter")),
                    "labels": labels or cols,
                    "raw": raw,
                    "minutes": fnum(mins),
                    "pts": fnum(pts),
                    "reb": fnum(reb),
                    "ast": fnum(ast),
                    "stl": fnum(stl),
                    "blk": fnum(blk),
                    "tov": fnum(tov),
                    "fgm": fgm,
                    "fga": fga,
                    "fg3m": fg3m,
                    "fg3a": fg3a,
                    "fg2m": fg2m,
                    "fg2a": fg2a,
                    "ftm": ftm,
                    "fta": fta,
                }
    return None


def main() -> None:
    lines = ["Jaylon Tyson — ESPN preseason boxes (CLE 2026)", ""]
    rows = []
    for day, eid, matchup in EVENTS:
        print(f"fetch {day} {eid}...")
        summary = fetch_event(eid)
        t = parse_tyson(summary)
        if not t:
            lines.append(f"{day}  {matchup}  (Tyson not in box)")
            continue
        lines.append(
            f"{day}  {matchup}  starter={t['starter']}  min={t['minutes']:g}  "
            f"PTS={t['pts']:g} REB={t['reb']:g} AST={t['ast']:g}  "
            f"FG={t['fgm']:g}-{t['fga']:g}  3PT={t['fg3m']:g}-{t['fg3a']:g}  "
            f"2PT={t['fg2m']:g}-{t['fg2a']:g}  FT={t['ftm']:g}-{t['fta']:g}  "
            f"STL={t['stl']:g} BLK={t['blk']:g} TOV={t['tov']:g}"
        )
        rows.append({"date": day, "matchup": matchup, **t})

    lines.append("")
    lines.append("=== vs Two Pointers Attempted Under 3.5 ===")
    vals = []
    for r in rows:
        v = r.get("fg2a")
        vals.append(v)
        tag = "UNDER" if v is not None and v < 3.5 else (
            "PUSH" if v == 3.5 else "OVER" if v is not None else "?"
        )
        lines.append(f"{r['date']}: FG2A={v} -> {tag}")
    if vals and all(v is not None for v in vals):
        lines.append(
            f"preseason n={len(vals)}  avg FG2A={sum(vals)/len(vals):.2f}  "
            f"Under 3.5 hits={sum(1 for v in vals if v < 3.5)}/{len(vals)}"
        )

    # Compare to root DB
    import sqlite3

    con = sqlite3.connect(str(ROOT / "data" / "cache" / "proporacle_ref.db"))
    lines.append("")
    lines.append("=== root DB rows (same dates) ===")
    for day, _, _ in EVENTS:
        row = con.execute(
            "select minutes, pts, reb, ast, fga, fg3a, fg2a, fgm, fg3m, fg2m "
            "from nba where player='Jaylon Tyson' and game_date=?",
            (day,),
        ).fetchone()
        lines.append(f"{day}: {row}")

    text = "\n".join(lines) + "\n"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(text, encoding="utf-8")
    print(text)
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
