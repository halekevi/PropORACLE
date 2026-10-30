"""Golf round integrity + scorecard-derived counting (ESPN PGA linescores).

ESPN scoreboard ``linescores[].statistics`` summary slots are unnamed and
positional. Historical PropORACLE code treated index 5 as ``holes_played`` and
then derived ``pars = holes - birdies_or_better - bogeys_or_worse``. Index 5 is
actually **pars**, so the residual was garbage (often 0–6 instead of ~6–15).

Prefer hole-by-hole ``linescores[].linescores[]`` with ``scoreType.displayValue``
(E / -1 / +1 / …). Fall back to named/positional summary only when the
scorecard is missing, then consistency-check.

Integrity flags soft per-stat ranges (pars 6–15, birdies ≤10) that marked real
rounds suspect (Yu 16 pars, Henley 18 pars, Lower 9 birdies, Scheffler 61).
Use composition + stroke identity instead; keep ranges only for impossible values.
"""

from __future__ import annotations

import re
from typing import Any

HOLES_FULL = 18
# Full-18 stroke play: below ~50 or above ~100 is not a real PGA card.
STROKES_IMPOSSIBLE_LO = 50.0
STROKES_IMPOSSIBLE_HI = 100.0

# Team / match-play events store hole wins (2–7), not stroke totals.
MATCH_PLAY_EVENT_RE = re.compile(
    r"presidents\s*cup|ryder\s*cup|solheim\s*cup|walker\s*cup|"
    r"curtis\s*cup|match\s*play|olympic\s+games",
    re.IGNORECASE,
)


def _f(val: object) -> float | None:
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def is_match_play_event(name: object) -> bool:
    s = str(name or "").strip()
    return bool(s and MATCH_PLAY_EVENT_RE.search(s))


def parse_score_type(raw: object) -> int | None:
    """Map ESPN hole scoreType display (E, -1, +2, …) to to-par integer."""
    s = str(raw or "").strip().upper()
    if not s:
        return None
    if s in {"E", "EVEN", "0"}:
        return 0
    try:
        return int(s.replace("+", ""))
    except ValueError:
        return None


def counts_from_hole_scorecard(holes: list[dict] | None) -> dict[str, float | None]:
    """Count birdies/pars/bogeys from ESPN nested hole linescores."""
    empty = {
        "strokes": None,
        "birdies": None,
        "eagles": None,
        "pars": None,
        "bogeys": None,
        "doubles": None,
        "other": None,
        "birdies_or_better": None,
        "bogeys_or_worse": None,
        "holes_played": None,
        "to_par": None,
        "course_par": None,
        "source": "empty",
    }
    if not holes:
        return empty
    birdies = eagles = pars = bogeys = doubles = other = 0
    strokes = 0.0
    to_par_sum = 0
    n = 0
    for h in holes:
        if not isinstance(h, dict):
            continue
        st = _f(h.get("value"))
        tp = parse_score_type((h.get("scoreType") or {}).get("displayValue"))
        if st is None or tp is None:
            continue
        n += 1
        strokes += st
        to_par_sum += tp
        if tp <= -2:
            eagles += 1
        elif tp == -1:
            birdies += 1
        elif tp == 0:
            pars += 1
        elif tp == 1:
            bogeys += 1
        elif tp == 2:
            doubles += 1
        else:
            other += 1
    if n == 0:
        return empty
    bob = float(birdies + eagles)
    bow = float(bogeys + doubles + other)
    course_par = strokes - float(to_par_sum)
    return {
        "strokes": strokes,
        "birdies": float(birdies),
        "eagles": float(eagles),
        "pars": float(pars),
        "bogeys": float(bogeys),
        "doubles": float(doubles),
        "other": float(other),
        "birdies_or_better": bob,
        "bogeys_or_worse": bow,
        "holes_played": float(n),
        "to_par": float(to_par_sum),
        "course_par": course_par,
        "source": "scorecard",
    }


def counts_from_summary_stats(
    stats: list[dict] | None,
    *,
    strokes: float | None = None,
    round_to_par: float | None = None,
) -> dict[str, float | None]:
    """Positional ESPN summary: birdies, bogeys, eagles, doubles, other, pars."""
    out = {
        "strokes": strokes,
        "birdies": None,
        "eagles": None,
        "pars": None,
        "bogeys": None,
        "doubles": None,
        "other": None,
        "birdies_or_better": None,
        "bogeys_or_worse": None,
        "holes_played": None,
        "to_par": round_to_par,
        "course_par": None,
        "source": "summary",
    }
    if not stats or len(stats) < 6:
        return out
    birdies = _f(stats[0].get("value"))
    bogeys = _f(stats[1].get("value"))
    eagles = _f(stats[2].get("value"))
    doubles = _f(stats[3].get("value"))
    other = _f(stats[4].get("value"))
    pars = _f(stats[5].get("value"))  # NOT holes — old bug treated this as holes
    out.update(
        {
            "birdies": birdies,
            "bogeys": bogeys,
            "eagles": eagles,
            "doubles": doubles,
            "other": other,
            "pars": pars,
        }
    )
    if birdies is not None or eagles is not None:
        out["birdies_or_better"] = (birdies or 0.0) + (eagles or 0.0)
    if bogeys is not None:
        out["bogeys_or_worse"] = (bogeys or 0.0) + (doubles or 0.0) + (other or 0.0)
    parts = [birdies, bogeys, eagles, doubles, other, pars]
    if all(v is not None for v in parts):
        out["holes_played"] = float(sum(parts))  # type: ignore[arg-type]
    if strokes is not None and round_to_par is not None:
        out["course_par"] = float(strokes) - float(round_to_par)
    return out


def parse_round_display_to_par(rnd: dict) -> float | None:
    raw = str(rnd.get("displayValue") or "").strip().upper()
    if not raw:
        return None
    if raw in {"E", "EVEN"}:
        return 0.0
    try:
        return float(raw.replace("+", ""))
    except ValueError:
        return None


def parse_round_stats(rnd: dict) -> dict[str, float | None]:
    """Prefer scorecard hole counts; fall back to fixed summary slots."""
    strokes = _f(rnd.get("value"))
    round_tp = parse_round_display_to_par(rnd)
    holes = rnd.get("linescores") or []
    scored = counts_from_hole_scorecard(holes if isinstance(holes, list) else None)
    if scored.get("holes_played") and float(scored["holes_played"] or 0) >= 9:
        # Prefer scorecard strokes when present; else board value.
        if scored.get("strokes") is None:
            scored["strokes"] = strokes
        elif strokes is not None and abs(float(scored["strokes"]) - float(strokes)) > 0.5:
            # Prefer explicit round total when scorecard sum disagrees.
            scored["strokes"] = strokes
            if round_tp is not None:
                scored["to_par"] = round_tp
                scored["course_par"] = float(strokes) - float(round_tp)
        elif scored.get("to_par") is None and round_tp is not None:
            scored["to_par"] = round_tp
        return scored

    cats = (rnd.get("statistics") or {}).get("categories") or []
    stats = (cats[0].get("stats") if cats else None) or []
    return counts_from_summary_stats(stats, strokes=strokes, round_to_par=round_tp)


def integrity_flags(row: dict[str, Any]) -> list[str]:
    """Return reason codes; empty list means the round is usable for counting props.

    Soft ranges on pars/birdies are retired — they flagged real cards. Flag
    composition failures and impossible values only.
    """
    flags: list[str] = []
    holes = _f(row.get("holes_played"))
    strokes = _f(row.get("strokes"))
    pars = _f(row.get("pars"))
    bob = _f(row.get("birdies_or_better"))
    bow = _f(row.get("bogeys_or_worse"))
    birdies = _f(row.get("birdies"))
    bogeys = _f(row.get("bogeys"))
    eagles = _f(row.get("eagles"))
    doubles = _f(row.get("doubles"))
    other = _f(row.get("other"))

    if strokes is None:
        flags.append("missing_strokes")
    elif strokes <= 0:
        flags.append("strokes_impossible")
    elif holes is not None and abs(holes - HOLES_FULL) < 0.1:
        if strokes < STROKES_IMPOSSIBLE_LO or strokes > STROKES_IMPOSSIBLE_HI:
            flags.append("strokes_impossible")

    if holes is None or holes < 9:
        flags.append("missing_holes")
    elif abs(holes - HOLES_FULL) > 0.1 and abs(holes - 9.0) > 0.1:
        # 17-hole scorecard parse gaps stay flagged here.
        flags.append("holes_not_9_or_18")

    for label, val in (
        ("birdies_negative", birdies),
        ("pars_negative", pars),
        ("bogeys_negative", bogeys),
        ("bob_negative", bob),
        ("bow_negative", bow),
    ):
        if val is not None and val < 0:
            flags.append(label)

    if holes is not None and holes >= 9:
        if bob is not None and bow is not None and pars is not None:
            if abs((bob + bow + pars) - holes) > 0.1:
                flags.append("bob_bow_pars_ne_holes")
        elif None not in (birdies, pars, bogeys) and eagles is not None:
            total = (
                (birdies or 0)
                + (eagles or 0)
                + (pars or 0)
                + (bogeys or 0)
                + (doubles or 0)
                + (other or 0)
            )
            if abs(total - holes) > 0.1:
                flags.append("hole_buckets_ne_holes")

        course_par = _f(row.get("course_par"))
        to_par = _f(row.get("to_par"))
        if strokes is not None and course_par is not None and to_par is not None:
            if abs((course_par + to_par) - strokes) > 0.6:
                flags.append("strokes_ne_par_plus_to_par")

    return flags


def is_attach_suspect(row: dict[str, Any]) -> bool:
    return bool(integrity_flags(row))


def counting_prop_ok(row: dict[str, Any]) -> bool:
    """Birdies / pars / bogeys props require a clean full-18 scorecard."""
    if is_attach_suspect(row):
        return False
    holes = _f(row.get("holes_played"))
    return holes is not None and abs(holes - HOLES_FULL) < 0.1
