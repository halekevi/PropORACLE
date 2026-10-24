"""Face (pick_type) verification by ladder shape.

PrizePicks posts one Standard line per player-prop, with Goblin rungs below
and Demon rungs above. When a graded set shows the *same* player, prop, and
date at two or more lines that are all labeled Standard, the label is a
default, not a face. NFL Weeks 1-6 2026 looked exactly like that (step1 never
stored pick_type; step7 filled "Standard"), and the ledger scan on 2026-10-24
found the same shape on older NHL, Soccer, Tennis, CBB and NBA-playoff dates.

Two sport-date signals, both warn-only in the ledger checks:

  pick_type_default   n >= MIN_ROWS and zero Goblin / Demon rows
                      (NFL 09-09..10-19, NBA1H Feb-Apr, blank NHL / Tennis days)
  standard_ladder     Standard rows >= MIN_ROWS and >= LADDER_SHARE of them sit
                      on a player-prop with 2+ distinct Standard lines.
                      Intraday line moves also produce ladders, so the normal
                      share is 0-7%; 20%+ is rungs, not moves.

Per-leg verdicts for the ticket archive (``classify_leg_face``) use the same
ladder plus the leg's own ``standard_line`` / cross-book lines when present.
Only rung verdicts stamp ``invalid_reason = unverified_face``; a single-line
player-prop on a default-label date is tagged ``face_unverified`` but left
valid, because a lone line is usually the Standard.
"""
from __future__ import annotations

import math
import re
from typing import Any, Iterable

import pandas as pd

INVALID_REASON = "unverified_face"
MIN_ROWS = 100
LADDER_SHARE = 0.20

RUNG_VERDICTS = frozenset({"goblin_rung", "demon_rung", "ambiguous_rung"})


def _fold(s: object) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s or "").lower())


def _f(raw: object) -> float | None:
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return v if math.isfinite(v) else None


# "\u2014" (em dash) is what graded_props_2026-09-05.json (CFB) carries for 1310
# rows whose prop name was lost; "\ufffd" is the decode-failure marker.
LADDER_MIN_SPREAD = 1.0
LADDER_MIN_REL_SPREAD = 0.20
GARBAGE_PROPS = frozenset({"", "nan", "none", "\ufffd", "\u2014", "-", "?"})


def norm_pick(raw: object) -> str:
    k = str(raw if raw is not None else "").strip().lower()
    if not k or k in {"nan", "none", "null", "<na>"}:
        return ""
    if "gob" in k:
        return "goblin"
    if "dem" in k:
        return "demon"
    return "standard"


# ---------------------------------------------------------------- sport-date


def face_status_by_sport_date(
    legs: pd.DataFrame,
    *,
    min_rows: int = MIN_ROWS,
    ladder_share: float = LADDER_SHARE,
) -> pd.DataFrame:
    """One row per (sport, slate_date) with the two signals and a status.

    ``legs`` needs sport, slate_date, player (or player_fold), prop (or
    prop_canon), line, and pick_type (or pick_norm).
    """
    if legs is None or legs.empty:
        return pd.DataFrame()
    df = legs.copy()
    df["_pt"] = (df["pick_norm"] if "pick_norm" in df.columns else df["pick_type"]).map(norm_pick)
    df["_pl"] = df["player_fold"] if "player_fold" in df.columns else df["player"].map(_fold)
    df["_pr"] = (df["prop_canon"] if "prop_canon" in df.columns else df["prop"]).astype(str).str.lower()
    df["_ln"] = pd.to_numeric(df["line"], errors="coerce")
    df = df[df["_ln"].notna()]
    # A blank / dash / replacement-character prop (CFB graded_props_2026-09-05.json
    # lost 1310 prop names to an em dash) lumps every prop of a player into one
    # key and fakes a ladder. Count it separately; never use it for the share.
    df["_garbage_prop"] = df["_pr"].isin(GARBAGE_PROPS) | df["_pr"].str.contains("\ufffd", regex=False)
    out: list[dict[str, Any]] = []
    for (sport, ds), sub in df.groupby(["sport", "slate_date"], sort=True):
        n = len(sub)
        cnt = sub["_pt"].value_counts().to_dict()
        garbage = int(sub["_garbage_prop"].sum())
        std = sub[(sub["_pt"] == "standard") & ~sub["_garbage_prop"]]
        lad = 0
        if len(std):
            g = std.groupby(["_pl", "_pr"])["_ln"].agg(["nunique", "min", "max"])
            spread = g["max"] - g["min"]
            rel = spread / g["max"].abs().clip(lower=0.5)
            # Two Standard lines 0.5-1.0 apart (TOI 21.2->22.0, faceoffs 9->10,
            # points 24.5->26.5) are intraday moves of one face across fetches.
            # A rung ladder is wide: Goblin/Demon sit >= 1 unit and >= 20% of the
            # line away from Standard (sacks 0.5 vs 1.5, rec yds 4.5 vs 58.5).
            keys = set(g[(g["nunique"] >= 2) & (spread >= LADDER_MIN_SPREAD) & (rel >= LADDER_MIN_REL_SPREAD)].index)
            idx = pd.MultiIndex.from_frame(std[["_pl", "_pr"]])
            lad = int(idx.isin(keys).sum())
        share = (lad / len(std)) if len(std) else 0.0
        goblin = int(cnt.get("goblin", 0))
        demon = int(cnt.get("demon", 0))
        blank = int(cnt.get("", 0))
        status = "verified"
        if n >= min_rows and goblin == 0 and demon == 0:
            status = "blank_labels" if blank == n else "default_labels"
        elif len(std) >= min_rows and share >= ladder_share:
            status = "laddered_standard"
        out.append(
            {
                "sport": str(sport),
                "slate_date": str(ds)[:10],
                "n": n,
                "standard": int(cnt.get("standard", 0)),
                "goblin": goblin,
                "demon": demon,
                "blank": blank,
                "garbage_prop": garbage,
                "standard_ladder": lad,
                "ladder_share": round(share, 4),
                "status": status,
            }
        )
    return pd.DataFrame(out)


def unverified_sport_dates(status: pd.DataFrame) -> set[tuple[str, str]]:
    if status is None or status.empty:
        return set()
    bad = status[status["status"] != "verified"]
    return {(str(r.sport).upper(), str(r.slate_date)) for r in bad.itertuples(index=False)}


# ---------------------------------------------------------------- per leg


def ladder_index(rows: Iterable[dict[str, Any]]) -> dict[tuple[str, str], list[float]]:
    """(player_fold, prop) -> sorted distinct lines seen for that date, any label."""
    seen: dict[tuple[str, str], set[float]] = {}
    for r in rows:
        line = _f(r.get("line"))
        if line is None:
            continue
        key = (_fold(r.get("player")), str(r.get("prop_canon") or r.get("prop") or "").lower())
        seen.setdefault(key, set()).add(round(line, 2))
    return {k: sorted(v) for k, v in seen.items()}


def _cross_book_line(leg: dict[str, Any]) -> float | None:
    vals = [
        _f(leg.get(k))
        for k in ("line_draftkings", "line_underdog", "best_cross_line", "standard_projection")
    ]
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    vals.sort()
    return vals[len(vals) // 2]


def classify_leg_face(
    leg: dict[str, Any],
    lines: list[float] | None,
    *,
    date_status: str,
) -> dict[str, Any]:
    """Verdict for one ticket leg labeled Standard.

    date_status is the sport-date status from ``face_status_by_sport_date``.
    On a ``verified`` date the label is trusted. Otherwise the ladder decides:
      standard_line != line           -> rung by sign
      3+ lines, leg at min / max      -> goblin_rung / demon_rung
      3+ lines, leg in the middle     -> standard_likely
      2 lines                         -> cross-book nearest if available, else ambiguous_rung
      1 line                          -> single_line_unverified
    """
    line = _f(leg.get("line"))
    pick = norm_pick(leg.get("pick_type"))
    rec: dict[str, Any] = {
        "verdict": "verified_label",
        "date_status": date_status,
        "ladder_lines": lines or [],
        "evidence": "",
    }
    if pick != "standard":
        rec["verdict"] = f"labeled_{pick or 'blank'}"
        return rec
    if date_status == "verified":
        return rec
    std_line = _f(leg.get("standard_line"))
    if line is not None and std_line is not None and abs(std_line - line) > 1e-9:
        rec["verdict"] = "goblin_rung" if line < std_line else "demon_rung"
        rec["evidence"] = f"standard_line {std_line} != line {line}"
        return rec
    if not lines or line is None:
        rec["verdict"] = "no_board_rows"
        return rec
    n = len(lines)
    pos = min(range(n), key=lambda i: abs(lines[i] - line))
    if n >= 3:
        if pos == 0:
            rec["verdict"] = "goblin_rung"
        elif pos == n - 1:
            rec["verdict"] = "demon_rung"
        else:
            rec["verdict"] = "standard_likely"
        rec["evidence"] = f"ladder {lines} position {pos + 1}/{n}"
        return rec
    if n == 2:
        xb = _cross_book_line(leg)
        if xb is not None:
            nearest = min(range(2), key=lambda i: abs(lines[i] - xb))
            if nearest == pos:
                rec["verdict"] = "standard_likely"
            else:
                rec["verdict"] = "goblin_rung" if pos == 0 else "demon_rung"
            rec["evidence"] = f"2-rung {lines}; cross-book {xb} nearest rung {nearest + 1}"
            return rec
        rec["verdict"] = "ambiguous_rung"
        rec["evidence"] = f"2-rung {lines}, no cross-book line"
        return rec
    rec["verdict"] = "single_line_unverified"
    rec["evidence"] = f"single line {lines[0]}"
    return rec


def annotate_unverified_face_ticket(ticket: dict[str, Any], verdicts: list[dict[str, Any]]) -> bool:
    """Stamp invalid/unverified_face when any leg is a rung. Never VOID.

    Returns True when the ticket was stamped invalid. Legs that are merely
    unverified (single line) get ``face_unverified`` on the leg only.
    """
    stamped = False
    legs = ticket.get("legs") or []
    for leg, v in zip(legs, verdicts):
        if not isinstance(leg, dict):
            continue
        leg["face_verdict"] = v.get("verdict")
        if v.get("evidence"):
            leg["face_evidence"] = v["evidence"]
        if v.get("verdict") in RUNG_VERDICTS:
            leg["face_unverified"] = True
            leg["invalid_reason"] = INVALID_REASON
            stamped = True
        elif v.get("verdict") in {"single_line_unverified", "standard_likely", "no_board_rows"}:
            leg["face_unverified"] = True
    if stamped:
        ticket["invalid"] = True
        ticket["invalid_reason"] = INVALID_REASON
        ticket["exclude_from_book"] = True
        ticket["exclude_from_winrate"] = True
        ticket["exclude_from_forward"] = True
    return stamped
