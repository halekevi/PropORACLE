"""Golf course-baseline projection (observe-only).

Labels only — no ticket gate. Same promotion path as NBA projection:
walk-forward calibration first, then a keep-gate cell, then tickets.

See ``Sports/Golf/GOLF_WINDOWS_RECONCILE_PROJECTION.md`` #2.

Strokes: µ = B + S, with B = par + shrunk field to-par at this course and
S = shrunk mean vs_field over the skill window. Birdies use Poisson with a
course-ease multiplier. Pars use a normal with explicit push mass.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd

from utils.golf_windows import SKILL_WINDOW_N, normalize_rounds, select_window
from utils.stat_distributions import (
    is_integer_line,
    p_over_normal,
    p_over_poisson,
    p_push_normal,
    p_push_poisson,
)

SKILL_HOLD_MIN = 8
K_SKILL = 8.0
K_COURSE = 12.0
K_SD = 10.0
COURSE_SKILL_BLEND = 0.15
POOLED_SD_DEFAULT = 2.8
POOLED_SD_FLOOR = 1.5
TOUR_MEAN_TO_PAR_DEFAULT = 0.0
TOUR_MEAN_BIRDIES_DEFAULT = 3.5
TOUR_MEAN_PARS_DEFAULT = 10.5
PAR_DEFAULT = 72.0

PROJ_COLS = (
    "golf_course_baseline",
    "golf_skill_vs_field",
    "golf_proj_mu",
    "golf_proj_sd",
    "golf_proj_p_over",
    "golf_proj_p_push",
    "golf_proj_edge",
    "golf_proj_hold",
    "no_course_history",
)


def _parse_date(val: object) -> date | None:
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    s = str(val).strip()[:10]
    if not s or s.lower() in {"nan", "none", "nat"}:
        return None
    try:
        return date.fromisoformat(s)
    except ValueError:
        ts = pd.to_datetime(s, errors="coerce")
        if pd.isna(ts):
            return None
        return ts.date()


def course_key(row: pd.Series | dict[str, Any] | None, explicit: str | None = None) -> str:
    """Venue ID if present, else a slug of tournament/course name."""
    if explicit and str(explicit).strip():
        return str(explicit).strip()
    rec = row if isinstance(row, dict) else (row.to_dict() if row is not None else {})
    cid = str(rec.get("course_id") or rec.get("venue_id") or rec.get("espn_course_id") or "").strip()
    if cid and cid.lower() not in {"nan", "none"}:
        return cid
    name = str(
        rec.get("tournament_name")
        or rec.get("tournament")
        or rec.get("event")
        or rec.get("course")
        or rec.get("team")
        or ""
    ).strip()
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return slug


def fill_course_ids(rounds: pd.DataFrame) -> pd.DataFrame:
    """Backfill blank ``course_id`` from tournament name so l5_course can run."""
    work = normalize_rounds(rounds)
    if work.empty:
        return work
    empty = work["course_id"].astype(str).str.strip().isin({"", "nan", "none", "None"})
    if not empty.any():
        return work
    names = None
    for col in ("tournament_name", "tournament", "event"):
        if col in work.columns:
            names = work[col]
            break
    if names is None:
        return work
    work.loc[empty, "course_id"] = names.loc[empty].map(lambda x: course_key({"tournament_name": x}))
    return work


def _as_of(val: object) -> date | None:
    return _parse_date(val)


def _full18(work: pd.DataFrame) -> pd.DataFrame:
    if work.empty:
        return work
    holes = pd.to_numeric(work.get("holes_played"), errors="coerce")
    if holes is None or holes.isna().all():
        return work
    keep = holes.isna() | (holes.sub(18).abs() < 0.1)
    return work.loc[keep].copy()


def _before(work: pd.DataFrame, as_of: date | None) -> pd.DataFrame:
    if as_of is None or work.empty:
        return work
    dates = work["round_date"] if "round_date" in work.columns else work.get("tournament_date")
    parsed = dates.map(_parse_date) if dates is not None else None
    if parsed is None:
        return work
    return work.loc[parsed.notna() & (parsed < as_of)].copy()


def tour_means(cache: pd.DataFrame, *, as_of: date | None = None) -> dict[str, float]:
    work = _full18(_before(fill_course_ids(cache), as_of))
    if work.empty:
        return {
            "to_par": TOUR_MEAN_TO_PAR_DEFAULT,
            "birdies": TOUR_MEAN_BIRDIES_DEFAULT,
            "pars": TOUR_MEAN_PARS_DEFAULT,
            "sd_vs_field": POOLED_SD_DEFAULT,
            "n": 0,
        }
    tp = pd.to_numeric(work.get("to_par"), errors="coerce")
    vf = pd.to_numeric(work.get("vs_field"), errors="coerce")
    bird = pd.to_numeric(work.get("birdies_or_better"), errors="coerce")
    if bird.isna().all():
        bird = pd.to_numeric(work.get("birdies"), errors="coerce")
    pars = pd.to_numeric(work.get("pars"), errors="coerce")
    sd = float(vf.std(ddof=1)) if vf.notna().sum() >= 8 else POOLED_SD_DEFAULT
    return {
        "to_par": float(tp.mean()) if tp.notna().any() else TOUR_MEAN_TO_PAR_DEFAULT,
        "birdies": float(bird.mean()) if bird.notna().any() else TOUR_MEAN_BIRDIES_DEFAULT,
        "pars": float(pars.mean()) if pars.notna().any() else TOUR_MEAN_PARS_DEFAULT,
        "sd_vs_field": max(POOLED_SD_FLOOR, sd if math.isfinite(sd) else POOLED_SD_DEFAULT),
        "n": int(len(work)),
    }


def course_field_stats(
    cache: pd.DataFrame,
    course_id: str,
    *,
    as_of: date | None = None,
) -> dict[str, float | int | bool]:
    """Historical field mean to-par / birdies / pars at this course (walk-forward)."""
    work = _full18(_before(fill_course_ids(cache), as_of))
    cid = str(course_id or "").strip()
    tour = tour_means(cache, as_of=as_of)
    empty = {
        "par": PAR_DEFAULT,
        "mean_to_par": float(tour["to_par"]),
        "mean_birdies": float(tour["birdies"]),
        "mean_pars": float(tour["pars"]),
        "n": 0,
        "no_course_history": True,
        "B": float(PAR_DEFAULT + tour["to_par"]),
    }
    if work.empty or not cid:
        return empty
    at = work[work["course_id"].astype(str) == cid].copy()
    if at.empty:
        return empty
    # One observation per event-round (field mean), then average those.
    grp_keys = [c for c in ("event_id", "tournament_date", "round_num") if c in at.columns]
    if not grp_keys:
        grp_keys = ["tournament_date", "round_num"]
    agg: dict[str, tuple[str, str]] = {"to_par": ("to_par", "mean"), "pars": ("pars", "mean")}
    if "course_par" in at.columns:
        agg["par"] = ("course_par", "median")
    bird_col = "birdies_or_better" if "birdies_or_better" in at.columns else "birdies"
    if bird_col in at.columns:
        agg["birdies"] = (bird_col, "mean")
    field = at.groupby(grp_keys, dropna=False).agg(**agg)
    n = int(len(field))
    w = n / (n + K_COURSE)
    mean_tp = float(field["to_par"].mean()) if "to_par" in field and field["to_par"].notna().any() else float(tour["to_par"])
    shrunk_tp = w * mean_tp + (1.0 - w) * float(tour["to_par"])
    par = float(field["par"].median()) if "par" in field and field["par"].notna().any() else PAR_DEFAULT
    if not math.isfinite(par) or par < 68 or par > 74:
        par = PAR_DEFAULT
    mean_bird = (
        float(field["birdies"].mean()) if "birdies" in field and field["birdies"].notna().any() else float(tour["birdies"])
    )
    mean_pars = (
        float(field["pars"].mean()) if "pars" in field and field["pars"].notna().any() else float(tour["pars"])
    )
    return {
        "par": par,
        "mean_to_par": shrunk_tp,
        "mean_birdies": mean_bird,
        "mean_pars": mean_pars,
        "n": n,
        "no_course_history": False,
        "B": par + shrunk_tp,
    }


def skill_vs_field(
    player_rounds: pd.DataFrame,
    *,
    as_of: date | None = None,
) -> dict[str, float | int]:
    win = select_window(player_rounds, kind="skill_window", as_of=as_of)
    vf = pd.to_numeric(win.get("vs_field"), errors="coerce").dropna()
    n = int(len(vf))
    if n == 0:
        return {"S": 0.0, "n": 0, "sd": float("nan"), "hold": 1}
    mean = float(vf.mean())
    s = (n / (n + K_SKILL)) * mean
    sd = float(vf.std(ddof=1)) if n >= 2 else float("nan")
    return {"S": s, "n": n, "sd": sd, "hold": 1 if n < SKILL_HOLD_MIN else 0}


def _shrink_sd(player_sd: float, n: int, pooled_sd: float) -> float:
    pooled = pooled_sd if math.isfinite(pooled_sd) and pooled_sd > 0 else POOLED_SD_DEFAULT
    if not math.isfinite(player_sd) or player_sd <= 0 or n < 2:
        return max(POOLED_SD_FLOOR, pooled)
    w = n / (n + K_SD)
    var = w * (player_sd ** 2) + (1.0 - w) * (pooled ** 2)
    return max(POOLED_SD_FLOOR, math.sqrt(var))


def _prop_family(prop_type: str) -> str:
    s = str(prop_type or "").strip().lower()
    if "stroke" in s:
        return "strokes"
    if "birdie" in s:
        return "birdies"
    if "par" in s:
        return "pars"
    if "bogey" in s:
        return "bogeys"
    return ""


def project_strokes(
    *,
    B: float,
    S: float,
    sd: float,
    line: float,
    course_skill: float | None = None,
) -> dict[str, float]:
    extra = 0.0
    if course_skill is not None and math.isfinite(course_skill):
        extra = COURSE_SKILL_BLEND * (float(course_skill) - float(S))
    mu = float(B) + float(S) + extra
    p_over = p_over_normal(mu, sd, line)
    p_push = p_push_normal(mu, sd, line) if is_integer_line(line) else 0.0
    return {
        "golf_proj_mu": mu,
        "golf_proj_sd": sd,
        "golf_proj_p_over": p_over,
        "golf_proj_p_push": p_push,
        "golf_proj_edge": p_over - 0.5,
    }


def project_count(
    *,
    mu: float,
    sd: float | None,
    line: float,
    family: str,
) -> dict[str, float]:
    if family == "poisson":
        p_over = p_over_poisson(max(0.05, mu), line)
        p_push = p_push_poisson(max(0.05, mu), line)
        used_sd = math.sqrt(max(mu, 0.05))
    else:
        used_sd = sd if sd is not None and math.isfinite(sd) and sd > 0 else 2.0
        p_over = p_over_normal(mu, used_sd, line)
        p_push = p_push_normal(mu, used_sd, line)
    return {
        "golf_proj_mu": mu,
        "golf_proj_sd": used_sd,
        "golf_proj_p_over": p_over,
        "golf_proj_p_push": p_push,
        "golf_proj_edge": p_over - 0.5,
    }


def project_leg(
    cache: pd.DataFrame,
    *,
    player_key: str,
    prop_type: str,
    line: float,
    course_id: str = "",
    as_of: date | str | None = None,
) -> dict[str, Any]:
    """Walk-forward projection for one player/prop. Uses only rounds before ``as_of``."""
    cutoff = _as_of(as_of)
    work = fill_course_ids(cache)
    if "player_key" not in work.columns and "player_name" in work.columns:
        from utils.player_name_utils import normalize_player_name

        work = work.copy()
        work["player_key"] = work["player_name"].map(lambda n: normalize_player_name(n).lower())
    pk = str(player_key or "").strip().lower()
    player = work[work["player_key"].astype(str).str.lower() == pk].copy() if not work.empty else work
    cid = str(course_id or "").strip()
    field = course_field_stats(work, cid, as_of=cutoff)
    skill = skill_vs_field(player, as_of=cutoff)
    tour = tour_means(work, as_of=cutoff)
    sd = _shrink_sd(float(skill["sd"]), int(skill["n"]), float(tour["sd_vs_field"]))
    hold = int(skill["hold"])
    family = _prop_family(prop_type)
    out: dict[str, Any] = {
        "golf_course_baseline": field["B"],
        "golf_skill_vs_field": skill["S"],
        "golf_proj_hold": hold,
        "no_course_history": 1 if field["no_course_history"] else 0,
        "golf_proj_mu": float("nan"),
        "golf_proj_sd": sd,
        "golf_proj_p_over": float("nan"),
        "golf_proj_p_push": 0.0,
        "golf_proj_edge": float("nan"),
    }
    if not math.isfinite(line):
        return out
    course_skill = None
    if cid:
        cwin = select_window(player, kind="l5_course", course_id=cid, as_of=cutoff)
        vf_c = pd.to_numeric(cwin.get("vs_field"), errors="coerce").dropna()
        if len(vf_c) >= 2:
            course_skill = float(vf_c.mean())
    if family == "strokes":
        priced = project_strokes(
            B=float(field["B"]),
            S=float(skill["S"]),
            sd=sd,
            line=float(line),
            course_skill=course_skill,
        )
        out.update(priced)
        return out
    ease_bird = (
        float(field["mean_birdies"]) / float(tour["birdies"])
        if tour["birdies"] and math.isfinite(float(tour["birdies"])) and float(tour["birdies"]) > 0
        else 1.0
    )
    ease_pars = (
        float(field["mean_pars"]) / float(tour["pars"])
        if tour["pars"] and math.isfinite(float(tour["pars"])) and float(tour["pars"]) > 0
        else 1.0
    )
    if family == "birdies":
        col = "birdies_or_better" if "better" in str(prop_type).lower() else "birdies"
        if col not in player.columns and "birdies_or_better" in player.columns:
            col = "birdies_or_better"
        sw = select_window(player, kind="skill_window", as_of=cutoff)
        rate = pd.to_numeric(sw.get(col), errors="coerce").dropna()
        n = int(len(rate))
        raw = float(rate.mean()) if n else float(tour["birdies"])
        shrunk = (n / (n + K_SKILL)) * raw + (K_SKILL / (n + K_SKILL)) * float(tour["birdies"])
        mu = max(0.05, shrunk * ease_bird)
        out.update(project_count(mu=mu, sd=None, line=float(line), family="poisson"))
        out["golf_proj_hold"] = 1 if n < SKILL_HOLD_MIN else hold
        return out
    if family == "pars":
        sw = select_window(player, kind="skill_window", as_of=cutoff)
        rate = pd.to_numeric(sw.get("pars"), errors="coerce").dropna()
        n = int(len(rate))
        raw = float(rate.mean()) if n else float(tour["pars"])
        shrunk = (n / (n + K_SKILL)) * raw + (K_SKILL / (n + K_SKILL)) * float(tour["pars"])
        mu = shrunk * ease_pars
        player_sd = float(rate.std(ddof=1)) if n >= 2 else 2.2
        out.update(project_count(mu=mu, sd=_shrink_sd(player_sd, n, 2.2), line=float(line), family="normal"))
        out["golf_proj_hold"] = 1 if n < SKILL_HOLD_MIN else hold
        return out
    return out


def apply_course_baseline(
    props: pd.DataFrame,
    cache: pd.DataFrame,
    *,
    as_of: date | str | None = None,
) -> pd.DataFrame:
    """Attach observe-only projection columns. Never changes the ticket gate."""
    out = props.copy()
    for col in PROJ_COLS:
        if col not in out.columns:
            if col in {"golf_proj_hold", "no_course_history"}:
                out[col] = 0
            else:
                out[col] = np.nan
    if out.empty:
        return out
    if "player_key" not in out.columns:
        from utils.player_name_utils import normalize_player_name

        name = out["player"] if "player" in out.columns else out.get("player_name", "")
        out["player_key"] = pd.Series(name, index=out.index).astype(str).map(
            lambda n: normalize_player_name(n).lower()
        )
    n_hold = 0
    for idx, row in out.iterrows():
        line = pd.to_numeric(row.get("line"), errors="coerce")
        line_f = float(line) if pd.notna(line) else float("nan")
        cid = course_key(row)
        row_as_of = as_of
        if row_as_of is None:
            for k in ("start_time", "game_date", "date"):
                if k in row.index and str(row.get(k) or "").strip():
                    row_as_of = row.get(k)
                    break
        got = project_leg(
            cache,
            player_key=str(row.get("player_key") or ""),
            prop_type=str(row.get("prop_type") or ""),
            line=line_f,
            course_id=cid,
            as_of=row_as_of,
        )
        for k, v in got.items():
            out.at[idx, k] = v
        if int(got.get("golf_proj_hold") or 0) == 1:
            n_hold += 1
    print(
        f"[Golf projection] rows={len(out)} hold_skill<{SKILL_HOLD_MIN}={n_hold} "
        f"(observe-only)"
    )
    return out


def walk_forward_strokes(cache: pd.DataFrame, *, min_prior: int = 8) -> pd.DataFrame:
    """Project every full-18 strokes round using only earlier rounds.

    Used to check µ/σ calibration. Does not need PrizePicks lines.
    """
    work = _full18(fill_course_ids(cache))
    if work.empty:
        return pd.DataFrame()
    work = work.sort_values(["round_date", "round_num"], kind="mergesort")
    rows: list[dict[str, Any]] = []
    for _, rec in work.iterrows():
        as_of = rec.get("round_date")
        pk = str(rec.get("player_key") or "").strip()
        if not pk or as_of is None:
            continue
        cid = course_key(rec)
        actual = pd.to_numeric(rec.get("strokes"), errors="coerce")
        if pd.isna(actual):
            continue
        got = project_leg(
            work,
            player_key=pk,
            prop_type="Strokes",
            line=float(actual) - 0.5,
            course_id=cid,
            as_of=as_of,
        )
        mu = got.get("golf_proj_mu")
        sd = got.get("golf_proj_sd")
        if mu is None or not math.isfinite(float(mu or 0)):
            continue
        skill_n = int(
            skill_vs_field(work[work["player_key"] == pk], as_of=_as_of(as_of))["n"]
        )
        if skill_n < int(min_prior):
            continue
        z = (float(actual) - float(mu)) / float(sd) if sd and float(sd) > 0 else float("nan")
        rows.append(
            {
                "player_key": pk,
                "round_date": str(as_of),
                "course_id": cid,
                "actual": float(actual),
                "mu": float(mu),
                "sd": float(sd) if sd is not None else float("nan"),
                "z": z,
                "p_over_half": got.get("golf_proj_p_over"),
                "golf_proj_hold": got.get("golf_proj_hold"),
                "skill_n": skill_n,
            }
        )
    return pd.DataFrame(rows)


__all__ = [
    "COURSE_SKILL_BLEND",
    "PROJ_COLS",
    "SKILL_HOLD_MIN",
    "apply_course_baseline",
    "course_field_stats",
    "course_key",
    "fill_course_ids",
    "project_leg",
    "project_strokes",
    "skill_vs_field",
    "tour_means",
    "walk_forward_strokes",
]
