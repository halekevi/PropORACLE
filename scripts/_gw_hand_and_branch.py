"""Hand-verify Games Won from ESPN groupings; gate branches from archive legs."""
from __future__ import annotations

import json
import sys
import urllib.request
from collections import Counter, defaultdict
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from report_gated_construction_efficiency import discover_g70_archives, payload_from_g70  # noqa: E402
from utils.gated_construction_tracks import GATED_GOBLIN, construction_track  # noqa: E402
from utils.leg_start_window import is_placeholder_start, usable_start_date  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.tennis_keep_gates import (  # noqa: E402
    GAMES_WON_L5_SECOND_WON_MIN,
    GAMES_WON_STD_DISCOUNT_MIN,
    tennis_games_won_big_favorite_fade,
    tennis_games_won_factor_keep,
    tennis_goblin_keep_eligible,
    tennis_l5_second_won_pct,
    tennis_opp_lefty,
    tennis_rank_gap,
    tennis_std_discount,
)
from utils.ticket_70_pool import norm_sport  # noqa: E402

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
MAIN = ROOT.parent / "PropORACLE_main_cp"
PHASE2 = json.loads((ROOT / "data" / "reports" / "gw_phase2_20261007.json").read_text(encoding="utf-8"))


def _p(*a):
    print(*a, flush=True)


def _espn(url: str):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=25) as resp:
        return json.loads(resp.read().decode())


def _iter_comps(payload: dict):
    for ev in payload.get("events") or []:
        # Tennis boards nest under groupings; some sports use top-level competitions.
        groupings = ev.get("groupings") or []
        if groupings:
            for grp in groupings:
                for comp in grp.get("competitions") or []:
                    yield ev, comp
        else:
            for comp in ev.get("competitions") or []:
                yield ev, comp


def _hand(player: str, day: str) -> dict | None:
    want = " ".join(player.casefold().split())
    for tour in ("atp", "wta"):
        for ymd in (day, (date.fromisoformat(day) + timedelta(days=1)).isoformat(),
                     (date.fromisoformat(day) - timedelta(days=1)).isoformat()):
            url = (
                f"https://site.api.espn.com/apis/site/v2/sports/tennis/{tour}/scoreboard"
                f"?dates={ymd.replace('-', '')}"
            )
            try:
                payload = _espn(url)
            except Exception as exc:
                _p(f"  ESPN fail {tour} {ymd}: {exc}")
                continue
            for ev, comp in _iter_comps(payload):
                comps = list(comp.get("competitors") or [])
                names = [
                    str(((c.get("athlete") or {}).get("displayName") or ""))
                    for c in comps
                ]
                if not any(want in " ".join(n.casefold().split()) for n in names):
                    continue
                status = str(((comp.get("status") or {}).get("type") or {}).get("name") or "")
                detail = str(((comp.get("status") or {}).get("type") or {}).get("detail") or "")
                mine = next(
                    (
                        c
                        for c in comps
                        if want
                        in " ".join(
                            str(((c.get("athlete") or {}).get("displayName") or "")).casefold().split()
                        )
                    ),
                    None,
                )
                if mine is None:
                    continue
                lines = mine.get("linescores") or []
                games = sum(float((x or {}).get("value") or 0) for x in lines)
                set_bits = [str(int(float((x or {}).get("value") or 0))) for x in lines]
                other = next((c for c in comps if c is not mine), None)
                other_bits = []
                if other is not None:
                    ols = other.get("linescores") or []
                    other_bits = [str(int(float((x or {}).get("value") or 0))) for x in ols]
                score_line = "-".join(
                    f"{a}-{b}" for a, b in zip(set_bits, other_bits or ["?"] * len(set_bits))
                )
                return {
                    "tour": tour,
                    "event": ev.get("id"),
                    "comp": comp.get("id"),
                    "date": comp.get("date") or ev.get("date"),
                    "status": status,
                    "detail": detail,
                    "games_won_hand": games,
                    "score_line": score_line,
                    "opp": str(((other or {}).get("athlete") or {}).get("displayName") or ""),
                    "retired": any(
                        tok in (status + " " + detail).upper()
                        for tok in ("RET", "ABANDON", "WALKOVER", "W/O")
                    ),
                }
    return None


def _starts(day: str) -> dict[str, str]:
    from collections import Counter, defaultdict

    import pandas as pd

    by: dict[str, Counter] = defaultdict(Counter)
    root = MAIN / "outputs" / day
    for path in list(root.glob("tennis/step1_tennis_props.csv")) + list(
        root.glob("step1_tennis_props.csv")
    ):
        df = pd.read_csv(path, dtype=str, encoding_errors="replace")
        cols = {str(c).lower(): c for c in df.columns}
        pc, sc = cols.get("player") or cols.get("player_name"), cols.get("start_time")
        if not (pc and sc):
            continue
        for player, start in zip(df[pc], df[sc], strict=False):
            if player and start and str(start).lower() != "nan":
                by[str(player).strip().lower()][str(start).strip()] += 1
    return {n: c.most_common(1)[0][0] for n, c in by.items()}


def _branch_from_leg(leg: dict) -> dict:
    row = dict(leg)
    row.setdefault("sport", "TENNIS")
    # archive uses l5/l10 not l5_over
    if row.get("l5") is not None and row.get("l5_over") is None:
        if str(row.get("side") or "OVER").upper() == "OVER":
            row["l5_over"] = row["l5"]
        else:
            row["l5_under"] = row["l5"]
    if row.get("l10") is not None and row.get("l10_over") is None:
        if str(row.get("side") or "OVER").upper() == "OVER":
            row["l10_over"] = row["l10"]
        else:
            row["l10_under"] = row["l10"]
    disc = tennis_std_discount(row)
    lefty = tennis_opp_lefty(row)
    sw = tennis_l5_second_won_pct(row)
    gap = tennis_rank_gap(row)
    fade = tennis_games_won_big_favorite_fade(row)
    factor = tennis_games_won_factor_keep(row)
    keep = tennis_goblin_keep_eligible(row)
    branch = "none"
    if fade:
        branch = "fade_50plus"
    elif disc is not None and disc >= GAMES_WON_STD_DISCOUNT_MIN:
        branch = "std_discount"
    elif factor:
        if lefty is True:
            branch = "l10_lefty"
        elif sw is not None and sw >= GAMES_WON_L5_SECOND_WON_MIN:
            branch = "l10_2nd_won"
        else:
            branch = "factor_other"
    return {
        "branch": branch,
        "keep": keep,
        "disc": disc,
        "lefty": lefty,
        "second_won": sw,
        "rank_gap": gap,
        "fade": fade,
        "l5": row.get("l5"),
        "l10": row.get("l10"),
        "standard_line": row.get("standard_line"),
        "inputs_blank": {
            "lefty": lefty is None,
            "second_won": sw is None,
            "disc": disc is None,
            "l10": row.get("l10") is None,
        },
    }


def main() -> None:
    # Build map of decided GW legs from phase2 dates/players, attach archive fields
    gw_meta = PHASE2  # has gw list? only counts — re-walk archives
    decided = []  # filled below from phase2 hand_verify list of players if needed

    # Walk archives: every gated GW leg with archive fields + branch
    _p("=== GATE BRANCHES FROM ARCHIVE LEGS ===")
    branch_hits: dict[str, Counter] = defaultdict(Counter)
    blank_lefty = blank_2nd = 0
    legs_out = []
    starts_cache = {}
    # Need grades — load from phase2 by collecting via actuals quickly
    import build_ticket_eval as te

    te.PROP_ALIAS_INDEX = True
    idx_cache = {}

    def indices(day):
        if day not in idx_cache:
            idx_cache[day] = (
                te._load_actuals_indices({}, [day], slate_date=day, payload={}),
                te._injury_dnp_by_sport([day]),
            )
        return idx_cache[day]

    for date_s, path in discover_g70_archives(start=None, end=None):
        payload = payload_from_g70(path, date_s)
        # payload_from_g70 may wrap tickets at top
        starts = starts_cache.setdefault(date_s, _starts(date_s))
        tickets = []
        if payload.get("groups"):
            for g in payload["groups"]:
                gname = str(g.get("group_name") or "")
                for t in g.get("tickets") or []:
                    tickets.append((gname, t))
        else:
            for t in payload.get("tickets") or []:
                tickets.append((str(t.get("group_name") or "Power 3 · goblin"), t))
        base_idx, base_inj = indices(date_s)
        for gname, ticket in tickets:
            if not isinstance(ticket, dict):
                continue
            if construction_track(ticket, gname) != GATED_GOBLIN:
                continue
            for leg in ticket.get("legs") or []:
                if not isinstance(leg, dict):
                    continue
                if norm_sport(str(leg.get("sport") or "")).lower() != "tennis":
                    continue
                prop = str(leg.get("prop") or leg.get("prop_type") or "")
                if canon_prop("TENNIS", prop) != "games_won":
                    continue
                player = str(leg.get("player") or "")
                start = starts.get(player.lower(), "")
                use = dict(leg)
                if not use.get("direction") and use.get("side"):
                    use["direction"] = use["side"]
                if not use.get("prop_type"):
                    use["prop_type"] = prop
                matched = te._match_leg_to_row_multi(use, base_idx)
                try:
                    line = float(use.get("line"))
                except (TypeError, ValueError):
                    line = None
                direction = str(use.get("direction") or use.get("side") or "OVER").upper()
                actual = matched["actual"] if matched else None
                g = te._leg_grade(
                    actual,
                    line,
                    direction,
                    matched["grade_raw"] if matched else "",
                    matched.get("void_note", "") if matched else "",
                )
                g = te._resolve_void_pending_if_injury_dnp(
                    g,
                    use,
                    actual,
                    line,
                    matched["grade_raw"] if matched else "",
                    matched.get("void_note", "") if matched else "",
                    base_inj,
                )
                source = "slate"
                if g not in {"HIT", "MISS"} and start and not is_placeholder_start(start):
                    day = usable_start_date(start)
                    if day and day != date_s:
                        idx2, inj2 = indices(day)
                        m2 = te._match_leg_to_row_multi(use, idx2)
                        a2 = m2["actual"] if m2 else None
                        g2 = te._leg_grade(
                            a2,
                            line,
                            direction,
                            m2["grade_raw"] if m2 else "",
                            m2.get("void_note", "") if m2 else "",
                        )
                        g2 = te._resolve_void_pending_if_injury_dnp(
                            g2,
                            use,
                            a2,
                            line,
                            m2["grade_raw"] if m2 else "",
                            m2.get("void_note", "") if m2 else "",
                            inj2,
                        )
                        if g2 in {"HIT", "MISS"}:
                            g, actual, source = g2, a2, f"fill:{day}"
                if g not in {"HIT", "MISS"}:
                    continue
                info = _branch_from_leg(leg)
                if info["inputs_blank"]["lefty"]:
                    blank_lefty += 1
                if info["inputs_blank"]["second_won"]:
                    blank_2nd += 1
                branch_hits[info["branch"]][g] += 1
                rec = {
                    "date": date_s,
                    "player": player,
                    "line": line,
                    "grade": g,
                    "actual": actual,
                    "start": start,
                    "source": source,
                    **info,
                }
                legs_out.append(rec)
                _p(
                    f"  {date_s} {player} line={line} std={info['standard_line']} "
                    f"grade={g} branch={info['branch']} keep={info['keep']} "
                    f"disc={info['disc']} l5={info['l5']} l10={info['l10']} "
                    f"lefty={info['lefty']} 2nd={info['second_won']} blank={info['inputs_blank']}"
                )

    _p(f"\nGATED_GW n={len(legs_out)} hits={sum(1 for r in legs_out if r['grade']=='HIT')}")
    _p("BRANCH_SUMMARY")
    for b, c in sorted(branch_hits.items()):
        n = c["HIT"] + c["MISS"]
        _p(f"  {b}: {c['HIT']}/{n} ({c['HIT']/n if n else 0:.1%})")
    _p(f"blank lefty on {blank_lefty}/{len(legs_out)}; blank 2nd-won on {blank_2nd}/{len(legs_out)}")
    keep_true = sum(1 for r in legs_out if r["keep"])
    _p(f"tennis_goblin_keep_eligible True on archive fields: {keep_true}/{len(legs_out)}")

    # Hand verify 10 — prefer mix + include a retirement if any
    _p("\n=== HAND VERIFY (ESPN groupings) ===")
    fills = [r for r in legs_out if str(r["source"]).startswith("fill")]
    misses = [r for r in legs_out if r["grade"] == "MISS"]
    hits = [r for r in legs_out if r["grade"] == "HIT"]
    sample = []
    for bucket in (fills, misses, hits):
        for r in bucket:
            if r not in sample:
                sample.append(r)
            if len(sample) >= 10:
                break
        if len(sample) >= 10:
            break

    match_n = 0
    found_n = 0
    retired_n = 0
    for rec in sample:
        day = usable_start_date(rec["start"]) or (
            rec["source"].split(":", 1)[1] if rec["source"].startswith("fill:") else rec["date"]
        )
        hand = _hand(rec["player"], day)
        if hand is None:
            _p(f"  {rec['date']} {rec['player']} stored={rec['actual']} ESPN=NOT_FOUND day={day}")
            continue
        found_n += 1
        hand_g = hand["games_won_hand"]
        line = rec["line"]
        hand_grade = "HIT" if line is not None and hand_g > line else "MISS"
        ok = (
            rec["actual"] is not None
            and abs(float(rec["actual"]) - hand_g) < 0.01
            and hand_grade == rec["grade"]
        )
        if ok:
            match_n += 1
        if hand["retired"]:
            retired_n += 1
        _p(
            f"  {rec['date']} {rec['player']} vs {hand['opp']} line={line} "
            f"stored={rec['actual']} hand={hand_g} score={hand['score_line']} "
            f"status={hand['status']} retired={hand['retired']} "
            f"grade={rec['grade']} hand_grade={hand_grade} MATCH={ok}"
        )

    # force-find one retirement in the 47 if sample missed
    if retired_n == 0:
        _p("  scanning all 47 for a retirement...")
        for rec in legs_out:
            day = usable_start_date(rec["start"]) or rec["date"]
            if rec["source"].startswith("fill:"):
                day = rec["source"].split(":", 1)[1]
            hand = _hand(rec["player"], day)
            if hand and hand["retired"]:
                _p(
                    f"  RETIREMENT {rec['date']} {rec['player']} vs {hand['opp']} "
                    f"stored={rec['actual']} hand={hand['games_won_hand']} "
                    f"score={hand['score_line']} status={hand['status']}"
                )
                retired_n += 1
                break
        if retired_n == 0:
            _p("  no retirement found among decided GW legs with ESPN match")

    _p(f"HAND_VERIFY found={found_n}/10 match={match_n} retirements_seen={retired_n}")

    out = ROOT / "data" / "reports" / "gw_hand_branch_20261007.json"
    out.write_text(
        json.dumps(
            {
                "legs": legs_out,
                "branch_summary": {
                    b: dict(c) for b, c in branch_hits.items()
                },
                "blank_lefty": blank_lefty,
                "blank_2nd": blank_2nd,
                "keep_true": keep_true,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    _p(f"WROTE {out}")


if __name__ == "__main__":
    main()
