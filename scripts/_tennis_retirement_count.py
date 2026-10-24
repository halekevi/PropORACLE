"""Count graded tennis retirements and which readings flip the grade."""
from __future__ import annotations

import json
import sys
import urllib.request
from collections import Counter
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

import prop_hit_tiers as T  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.tennis_retirement import grade_retirement, grade_walkover  # noqa: E402

CACHE = ROOT / "data" / "cache" / "espn_tennis_scoreboard"
MANIFEST = ROOT / "data" / "reports" / "gated_goblin_slip_manifest_20261007.json"
OUT = ROOT / "data" / "reports" / "tennis_retirement_count.json"
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
GAME_PROPS = {"games_won", "match_total_games"}


def _norm(name: object) -> str:
    return " ".join(str(name or "").casefold().split())


def _espn(tour: str, ymd: str) -> dict:
    CACHE.mkdir(parents=True, exist_ok=True)
    path = CACHE / f"{tour}_{ymd.replace('-', '')}.json"
    if path.is_file() and path.stat().st_size > 20:
        return json.loads(path.read_text(encoding="utf-8"))
    url = (
        "https://site.api.espn.com/apis/site/v2/sports/tennis/"
        f"{tour}/scoreboard?dates={ymd.replace('-', '')}"
    )
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=25) as resp:
            payload = json.loads(resp.read().decode())
    except Exception:
        payload = {}
    path.write_text(json.dumps(payload), encoding="utf-8")
    return payload


def _comps(payload):
    for ev in payload.get("events") or []:
        groupings = ev.get("groupings") or []
        comps = []
        if groupings:
            for grp in groupings:
                comps.extend(grp.get("competitions") or [])
        else:
            comps.extend(ev.get("competitions") or [])
        for comp in comps:
            yield comp


def _board(ymd: str) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for tour in ("atp", "wta"):
        payload = _espn(tour, ymd)
        for comp in _comps(payload):
            athletes = list(comp.get("competitors") or [])
            if len(athletes) < 2:
                continue
            status = str(((comp.get("status") or {}).get("type") or {}).get("name") or "")
            detail = str(((comp.get("status") or {}).get("type") or {}).get("detail") or "")
            blob = (status + " " + detail).upper()
            kind = ""
            if "WALKOVER" in blob or "W/O" in blob:
                kind = "WALKOVER"
            elif "RET" in blob or "ABANDON" in blob:
                kind = "RETIRED"
            for i, athlete in enumerate(athletes):
                name = str(((athlete.get("athlete") or {}).get("displayName") or "")).strip()
                if not name:
                    continue
                lines = athlete.get("linescores") or []
                other = athletes[1 - i]
                olines = other.get("linescores") or []
                sets = [int(float((x or {}).get("value") or 0)) for x in lines]
                opp_sets = [int(float((x or {}).get("value") or 0)) for x in olines]
                winner = athlete.get("winner")
                player_won = None if winner is None else bool(winner)
                prev = out.get(_norm(name))
                if prev and prev.get("kind") == "RETIRED" and kind != "RETIRED":
                    continue
                out[_norm(name)] = {
                    "kind": kind,
                    "status": status,
                    "sets": sets,
                    "opp_sets": opp_sets,
                    "player_won": player_won,
                    "day": ymd,
                }
    return out


def _graded():
    files = {}
    for folder in (MAIN / "ui_runner" / "templates", ROOT / "ui_runner" / "templates"):
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_20*.json")):
            if ".bak_" in path.name:
                continue
            files.setdefault(path.stem.replace("graded_props_", "")[:10], path)
    rows = []
    for day, path in files.items():
        raw = json.loads(path.read_text(encoding="utf-8"))
        for row in raw.get("props") or []:
            if not isinstance(row, dict):
                continue
            if T.norm_sport(str(row.get("sport") or "")).lower() != "tennis":
                continue
            prop = canon_prop("TENNIS", str(row.get("prop") or ""))
            if prop not in GAME_PROPS:
                continue
            if str(row.get("result") or "").upper() not in {"HIT", "MISS", "PUSH", "UNRESOLVED"}:
                continue
            try:
                line = float(row.get("line"))
            except (TypeError, ValueError):
                continue
            rows.append(
                {
                    "day": day,
                    "player": str(row.get("player") or ""),
                    "prop": prop,
                    "line": line,
                    "direction": str(row.get("direction") or "OVER"),
                    "result": str(row.get("result") or "").upper(),
                    "path": path,
                }
            )
    return rows


def _manifest_keys() -> set[tuple[str, str, str]]:
    if not MANIFEST.is_file():
        return set()
    man = json.loads(MANIFEST.read_text(encoding="utf-8"))
    keys = set()
    for slip in man.get("slips") or []:
        if not slip.get("resolved"):
            continue
        players = slip.get("players") or []
        props = slip.get("props") or []
        sports = slip.get("sports") or []
        for i, player in enumerate(players):
            sport = sports[i] if i < len(sports) else ""
            prop = props[i] if i < len(props) else ""
            if not str(sport).upper().startswith("TENNIS"):
                continue
            canon = canon_prop("TENNIS", str(prop))
            if canon not in GAME_PROPS:
                continue
            keys.add((slip.get("date") or "", _norm(player), canon))
    return keys


def _apply(rows_by_file: dict[Path, list[dict]]) -> int:
    """Mark walkovers and reading-disagreements UNRESOLVED. Record an agreed grade."""
    changed_files = 0
    for path, edits in rows_by_file.items():
        raw = json.loads(path.read_text(encoding="utf-8"))
        changed = False
        index = {}
        for r in raw.get("props") or []:
            if not isinstance(r, dict):
                continue
            try:
                line_key = round(float(r.get("line")), 2)
            except (TypeError, ValueError):
                continue
            index[(
                " ".join(str(r.get("player") or "").casefold().split()),
                canon_prop("TENNIS", str(r.get("prop") or "")),
                line_key,
                str(r.get("direction") or "OVER").upper(),
            )] = r
        for edit in edits:
            key = (
                " ".join(edit["player"].casefold().split()),
                edit["prop"],
                round(float(edit["line"]), 2),
                edit["direction"].upper(),
            )
            row = index.get(key)
            if row is None:
                continue
            if row.get("result") != edit["result"] or row.get("retirement_reading") != edit.get("reading"):
                row["result"] = edit["result"]
                row["retirement_reading"] = edit.get("reading")
                if edit["result"] == "UNRESOLVED":
                    row["hit"] = None
                changed = True
        if changed:
            path.write_text(json.dumps(raw, ensure_ascii=False), encoding="utf-8")
            changed_files += 1
    return changed_files


def main() -> None:
    rows = _graded()
    days = sorted({r["day"] for r in rows})
    boards = {d: _board(d) for d in days}
    need_neighbors = set()
    for row in rows:
        if (boards.get(row["day"]) or {}).get(_norm(row["player"])):
            continue
        dd = date.fromisoformat(row["day"])
        need_neighbors.add((dd + timedelta(days=1)).isoformat())
        need_neighbors.add((dd - timedelta(days=1)).isoformat())
    for d in sorted(need_neighbors):
        if d not in boards:
            boards[d] = _board(d)
    book = _manifest_keys()
    edits: dict[Path, list[dict]] = {}
    retired = walkover = flips = agreed = book_retired = book_flips = 0
    by_prop = Counter()
    book_by_prop = Counter()
    for row in rows:
        hand = None
        for d in (
            row["day"],
            (date.fromisoformat(row["day"]) + timedelta(days=1)).isoformat(),
            (date.fromisoformat(row["day"]) - timedelta(days=1)).isoformat(),
        ):
            hand = (boards.get(d) or {}).get(_norm(row["player"]))
            if hand and hand.get("kind"):
                break
            hand = None
        if not hand or not hand.get("kind"):
            continue
        in_book = (row["day"], _norm(row["player"]), row["prop"]) in book
        if hand["kind"] == "WALKOVER":
            marked = grade_walkover(row["prop"])
            if marked:
                walkover += 1
                by_prop[f"walkover {row['prop']}"] += 1
                edits.setdefault(row["path"], []).append(
                    {**row, "result": "UNRESOLVED", "reading": "walkover_chart_silent"}
                )
                if in_book:
                    book_by_prop[f"walkover {row['prop']}"] += 1
            continue
        graded = grade_retirement(
            row["line"],
            row["direction"],
            hand["sets"],
            hand["opp_sets"],
            player_won=hand["player_won"],
            prop=row["prop"],
        )
        retired += 1
        by_prop[row["prop"]] += 1
        if graded["result"] == "UNRESOLVED":
            flips += 1
            by_prop[f"flip {row['prop']}"] += 1
            reading = "readings_differ"
        else:
            agreed += 1
            reading = "both_readings"
        edits.setdefault(row["path"], []).append(
            {**row, "result": graded["result"], "reading": reading}
        )
        if in_book:
            book_retired += 1
            book_by_prop[row["prop"]] += 1
            if graded["result"] == "UNRESOLVED":
                book_flips += 1
    report = {
        "graded_game_rows": len(rows),
        "retired": retired,
        "walkover_no_grade": walkover,
        "readings_agree": agreed,
        "readings_flip_unresolved": flips,
        "by_prop": dict(by_prop),
        "book_127_retired_legs": book_retired,
        "book_127_flips": book_flips,
        "book_by_prop": dict(book_by_prop),
        "files_updated": _apply(edits),
    }
    OUT.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
