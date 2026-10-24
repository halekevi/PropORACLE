"""Resolve multi-event soccer rows with the stored start time."""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from _soccer_box_vs_stored import (  # noqa: E402
    CACHE,
    _load_actuals,
    _load_step8,
    _norm,
    _player_match,
    _team_match,
)
from fetch_actuals import parse_soccer_boxscore  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402


def _p(*a):
    print(*a, flush=True)


def _keeper_role(sum_path: Path, player: str) -> dict:
    """Whether a zero-save row is the starting goalkeeper or a teammate who sat."""
    out = {
        "starter": "",
        "position": "",
        "teammate_gk_saves": "",
        "bench_gk": "",
    }
    if not sum_path.is_file():
        return out
    try:
        summary = json.loads(sum_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return out
    rosters = summary.get("rosters") or []
    target = None
    team_entries: list[dict] = []
    for block in rosters:
        if not isinstance(block, dict):
            continue
        for entry in block.get("roster") or []:
            if not isinstance(entry, dict):
                continue
            name = str((entry.get("athlete") or {}).get("displayName") or "")
            if _player_match(player, name):
                target = entry
                team_entries = [e for e in (block.get("roster") or []) if isinstance(e, dict)]
                break
        if target is not None:
            break
    if target is None:
        return out
    pos = target.get("position") or {}
    pos_abbr = ""
    if isinstance(pos, dict):
        pos_abbr = str(pos.get("abbreviation") or pos.get("name") or "")
    elif pos:
        pos_abbr = str(pos)
    starter = target.get("starter")
    out["starter"] = "" if starter is None else str(bool(starter))
    out["position"] = pos_abbr
    mate_saves = []
    for entry in team_entries:
        if entry is target:
            continue
        epos = entry.get("position") or {}
        abbr = ""
        if isinstance(epos, dict):
            abbr = str(epos.get("abbreviation") or "")
        if abbr.upper() not in {"G", "GK"} and "GOAL" not in str(epos).upper():
            continue
        saves = None
        for stat in entry.get("stats") or []:
            if not isinstance(stat, dict):
                continue
            label = str(stat.get("abbreviation") or stat.get("name") or "").upper()
            if label in {"SV", "SAVES"}:
                try:
                    saves = float(stat.get("value"))
                except (TypeError, ValueError):
                    saves = None
        if saves is not None:
            mate_saves.append(saves)
    out["teammate_gk_saves"] = ",".join(str(v) for v in mate_saves)
    # Bench if this keeper did not start, or a teammate keeper recorded the saves.
    started = starter is True
    mate_played = any(v > 0 for v in mate_saves)
    out["bench_gk"] = "yes" if (not started and mate_played) or (starter is False) else "no"
    return out


def _parse_dt(text: object) -> datetime | None:
    raw = str(text or "").strip()
    if not raw:
        return None
    raw = raw.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _event_names(ev: dict) -> list[str]:
    comps = (ev.get("competitions") or [{}])[0].get("competitors") or []
    names = []
    for c in comps:
        team = c.get("team") or {}
        names.extend(
            [
                team.get("displayName") or "",
                team.get("shortDisplayName") or "",
                team.get("abbreviation") or "",
                team.get("name") or "",
            ]
        )
    return names


def main() -> None:
    start, end = "2026-09-01", "2026-09-27"
    actual, league_of, _, _, _ = _load_actuals(start, end)
    # step8 loader does not keep start_time; read it here by reusing rows' keys
    rows = _load_step8(start, end)
    # attach start_time from a second pass
    from datetime import date, timedelta

    import pandas as pd

    MAIN = ROOT.parent / "PropORACLE_main_cp"
    starts: dict[tuple[str, str, str, str], str] = {}
    day = date.fromisoformat(start)
    stop = date.fromisoformat(end)
    while day <= stop:
        stamp = day.isoformat()
        path = MAIN / "outputs" / stamp / "soccer" / "step8_soccer_direction.csv"
        if not path.is_file():
            path = ROOT / "outputs" / stamp / "soccer" / "step8_soccer_direction.csv"
        day += timedelta(days=1)
        if not path.is_file():
            continue
        df = pd.read_csv(path, dtype=str, encoding_errors="replace")
        for _, r in df.iterrows():
            prop = canon_prop("SOCCER", str(r.get("prop_type") or ""))
            player = _norm(r.get("player"))
            starts[(stamp, player, prop, str(r.get("pick_type") or "").lower())] = str(
                r.get("start_time") or ""
            )

    want = []
    seen = set()
    for r in rows:
        if not r["opp"]:
            continue
        act = actual.get((r["player"], r["day"], r["prop"]))
        if act is None:
            continue
        key = (r["player"], r["day"], r["prop"])
        if key in seen:
            continue
        seen.add(key)
        league = league_of.get((r["player"], r["day"]), "")
        if not league or league.upper() == "SOCCER":
            continue
        kind = "goblin" if r["kind"] == "Goblin" else "standard"
        st = starts.get((r["day"], r["player"], r["prop"], kind), "")
        want.append({**r, "actual": act, "league": league, "start": st})

    by_ld: dict[tuple[str, str], list] = defaultdict(list)
    for r in want:
        by_ld[(r["day"], r["league"])].append(r)

    boxes: dict[str, list] = {}
    unresolved: list[dict] = []
    ambiguous_rows = 0
    resolved = 0
    no_start = 0
    still_multi = 0
    no_close = 0
    player_miss = 0
    agree = 0
    mismatch = 0
    by_prop = defaultdict(lambda: [0, 0])
    in_box_actuals: dict[str, list[float]] = defaultdict(list)
    saves_zero_inbox: list[dict] = []

    for (day, league), group in sorted(by_ld.items()):
        sb_path = CACHE / f"sb_{league}_{day.replace('-', '')}.json"
        if not sb_path.is_file():
            continue
        events = json.loads(sb_path.read_text(encoding="utf-8")).get("events") or []
        for r in group:
            hits = []
            for ev in events:
                if _team_match(r["opp"], _event_names(ev)):
                    eid = ev.get("id")
                    if eid:
                        hits.append(ev)
            uniq = {ev.get("id"): ev for ev in hits}
            if len(uniq) <= 1:
                continue
            ambiguous_rows += 1
            start_dt = _parse_dt(r["start"])
            if start_dt is None:
                no_start += 1
                continue
            ranked = []
            for ev in uniq.values():
                ev_dt = _parse_dt(ev.get("date"))
                if ev_dt is None:
                    continue
                ranked.append((abs((ev_dt - start_dt).total_seconds()), ev))
            ranked.sort(key=lambda t: t[0])
            if not ranked or ranked[0][0] > 3 * 3600:
                no_close += 1
                continue
            # A second event has to be at least 30 minutes farther from the
            # stored start than the closest one.
            if len(ranked) > 1 and ranked[1][0] - ranked[0][0] < 30 * 60:
                still_multi += 1
                unresolved.append({**r, "bucket": "ambiguous_30min"})
                continue
            eid = str(ranked[0][1].get("id"))
            resolved += 1
            if eid not in boxes:
                sum_path = CACHE / f"sum_{league}_{eid}.json"
                if not sum_path.is_file():
                    boxes[eid] = []
                else:
                    summary = json.loads(sum_path.read_text(encoding="utf-8"))
                    boxes[eid] = parse_soccer_boxscore(summary, league)
            found = None
            for er in boxes[eid]:
                if canon_prop("SOCCER", er.get("prop_type") or "") != r["prop"]:
                    continue
                if _player_match(r["player_raw"], er.get("player") or ""):
                    found = er
                    break
            if found is None:
                player_miss += 1
                unresolved.append({**r, "bucket": "player_not_in_box"})
                continue
            in_box_actuals[r["prop"]].append(float(r["actual"]))
            if r["prop"] == "saves" and float(r["actual"]) == 0.0:
                sum_path = CACHE / f"sum_{league}_{eid}.json"
                role = _keeper_role(sum_path, r["player_raw"])
                saves_zero_inbox.append({**r, **role, "event_id": eid})
            diff = abs(float(r["actual"]) - float(found["actual"]))
            if diff < 0.51:
                agree += 1
                by_prop[r["prop"]][0] += 1
            else:
                mismatch += 1
                by_prop[r["prop"]][1] += 1

    compared = agree + mismatch
    _p(f"multi-event rows {ambiguous_rows}")
    _p(f"  no stored start {no_start}")
    _p(f"  no event within 3h {no_close}")
    _p(f"  still tied after start time {still_multi}")
    _p(f"  start time picked one event {resolved}")
    _p(f"  player missing on that event {player_miss}")
    _p(f"compared {compared} agree {agree} mismatch {mismatch}")
    if compared:
        _p(f"mismatch rate {mismatch / compared:.1%}")
    for prop, (a, m) in sorted(by_prop.items()):
        n = a + m
        _p(f"  {prop}: agree {a}/{n} mismatch {m}/{n} ({(m / n if n else 0):.1%})")

    out = ROOT / "data" / "reports" / "soccer_unresolved_multi_event.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    import csv

    fields = [
        "bucket", "day", "player_raw", "prop", "kind", "line", "actual",
        "opp", "team", "league", "start",
    ]
    with out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in unresolved:
            writer.writerow(row)
    _p(f"unresolved rows written {len(unresolved)} -> {out}")
    from collections import Counter

    counts = Counter(row["bucket"] for row in unresolved)
    _p(f"  buckets {dict(counts)}")
    by_bucket_prop: dict[tuple[str, str], list[float]] = defaultdict(list)
    for row in unresolved:
        try:
            by_bucket_prop[(row["bucket"], row["prop"])].append(float(row["actual"]))
        except (TypeError, ValueError):
            continue
    def _zero_line(label: str, vals: list[float]) -> None:
        if not vals:
            return
        zeros = sum(1 for v in vals if v == 0)
        _p(f"  {label}: n={len(vals)} zeros={zeros} ({zeros / len(vals):.1%}) mean={sum(vals) / len(vals):.2f}")

    bench = sum(1 for row in saves_zero_inbox if row.get("bench_gk") == "yes")
    started = sum(1 for row in saves_zero_inbox if row.get("starter") == "True")
    _p(
        f"in-box saves zeros {len(saves_zero_inbox)} "
        f"starter={started} bench_gk={bench}"
    )
    for row in saves_zero_inbox:
        _p(
            f"  {row.get('day')} {row.get('player_raw')} starter={row.get('starter')} "
            f"pos={row.get('position')} mate_sv={row.get('teammate_gk_saves')} "
            f"bench={row.get('bench_gk')}"
        )

    _p("zero rate, player in the matched box (stored actual)")
    for prop, vals in sorted(in_box_actuals.items()):
        _zero_line(prop, vals)
    _p("zero rate, player not in that box")
    for prop in ("saves", "shots", "sog"):
        vals = by_bucket_prop.get(("player_not_in_box", prop), [])
        _zero_line(prop, vals)

    for (bucket, prop), vals in sorted(by_bucket_prop.items()):
        zeros = sum(1 for v in vals if v == 0)
        mean = sum(vals) / len(vals) if vals else 0
        _p(
            f"  {bucket} {prop}: n={len(vals)} mean={mean:.2f} "
            f"zeros={zeros} min={min(vals):.1f} max={max(vals):.1f}"
        )


if __name__ == "__main__":
    main()
