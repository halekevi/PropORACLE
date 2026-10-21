"""Golf PP chart reconcile (#1).

Board scrape (step1) does not currently capture PP chart series or the chart
filter (all-courses vs venue). Until the scrape grows those fields, reconcile
reads a manual fixture:

  Sports/Golf/data/fixtures/pp_chart_manual.json

Compare by date order against ``l5_course`` when the chart is venue-filtered,
else ``l5_all``. Integer stats must match exactly. ``mismatch`` is a hold
(``void_reason=golf_pp_mismatch``); ``no_pp_chart`` is a flag only.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from utils.golf_windows import select_window
from utils.player_name_utils import normalize_player_name

_REPO = Path(__file__).resolve().parents[1]
DEFAULT_FIXTURE = _REPO / "Sports" / "Golf" / "data" / "fixtures" / "pp_chart_manual.json"

STAT_COL_FOR_PROP = {
    "strokes": "strokes",
    "pars": "pars",
    "par": "pars",
    "birdies": "birdies",
    "birdies or better": "birdies_or_better",
    "bogeys": "bogeys_plus",
    "bogeys or worse": "bogeys_plus",
}


def _player_key(name: str) -> str:
    return normalize_player_name(name).lower()


def _prop_stat_col(prop_type: str, *, birdies_or_better: bool = False) -> str | None:
    s = str(prop_type or "").strip().lower()
    if not s:
        return None
    if "birdie" in s:
        return "birdies_or_better" if birdies_or_better or "better" in s else "birdies"
    for key, col in STAT_COL_FOR_PROP.items():
        if key in s:
            return col
    return None


def load_pp_chart_fixture(path: Path | None = None) -> list[dict[str, Any]]:
    p = path or DEFAULT_FIXTURE
    if not p.is_file():
        return []
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(raw, dict):
        legs = raw.get("legs") or raw.get("charts") or []
    elif isinstance(raw, list):
        legs = raw
    else:
        legs = []
    out: list[dict[str, Any]] = []
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        vals = leg.get("values") or leg.get("chart") or []
        if not isinstance(vals, list):
            continue
        out.append(
            {
                "player_key": _player_key(str(leg.get("player") or "")),
                "player": str(leg.get("player") or "").strip(),
                "prop_type": str(leg.get("prop_type") or leg.get("prop") or "").strip(),
                "line": leg.get("line"),
                "filter": str(leg.get("filter") or "all").strip().lower(),
                "course_id": str(leg.get("course_id") or "").strip(),
                "values": [float(v) for v in vals if v is not None and str(v).strip() != ""],
                "birdies_or_better": bool(leg.get("birdies_or_better", False)),
            }
        )
    return out


def _find_fixture_leg(
    fixtures: list[dict[str, Any]],
    *,
    player: str,
    prop_type: str,
    line: float | None,
) -> dict[str, Any] | None:
    pk = _player_key(player)
    prop_l = str(prop_type or "").strip().lower()
    for leg in fixtures:
        if leg.get("player_key") != pk:
            continue
        if str(leg.get("prop_type") or "").strip().lower() != prop_l:
            # allow substring (Pars vs Par)
            lp = str(leg.get("prop_type") or "").strip().lower()
            if lp not in prop_l and prop_l not in lp:
                continue
        if line is not None and leg.get("line") is not None:
            try:
                if abs(float(leg["line"]) - float(line)) > 0.01:
                    continue
            except (TypeError, ValueError):
                pass
        return leg
    return None


def compare_chart_values(
    ours: list[float],
    pp: list[float],
) -> tuple[str, list[int]]:
    """Exact integer compare by position (newest-first). Returns status + mismatch idxs."""
    if not pp:
        return "no_pp_chart", []
    n = min(len(ours), len(pp))
    if n == 0:
        return "mismatch", list(range(len(pp)))
    bad: list[int] = []
    for i in range(n):
        if abs(float(ours[i]) - float(pp[i])) > 1e-9:
            bad.append(i)
    # Length mismatch on the overlapping window still counts as mismatch.
    if len(ours) < len(pp):
        bad.extend(range(len(ours), len(pp)))
    if bad:
        return "mismatch", bad
    return "match", []


def reconcile_leg(
    rounds: pd.DataFrame,
    *,
    prop_type: str,
    line: float | None,
    fixture_leg: dict[str, Any] | None,
    as_of: str | None = None,
) -> dict[str, Any]:
    if fixture_leg is None:
        return {
            "golf_pp_reconcile": "no_pp_chart",
            "pp_mismatch_rounds": "",
            "pp_chart_filter": "",
            "pp_chart_n": 0,
        }
    bob = bool(fixture_leg.get("birdies_or_better"))
    stat_col = _prop_stat_col(prop_type, birdies_or_better=bob)
    if stat_col is None:
        stat_col = _prop_stat_col(str(fixture_leg.get("prop_type") or ""), birdies_or_better=bob)
    filt = str(fixture_leg.get("filter") or "all").lower()
    course_id = str(fixture_leg.get("course_id") or "").strip() or None
    kind = "l5_course" if filt in {"course", "venue", "l5_course"} else "l5_all"
    if kind == "l5_course" and not course_id:
        # Can't compare yet — venue IDs not on the cache. Flag only, not a hold.
        return {
            "golf_pp_reconcile": "no_pp_chart",
            "pp_mismatch_rounds": "missing_course_id",
            "pp_chart_filter": filt,
            "pp_chart_n": len(fixture_leg.get("values") or []),
        }
    win = select_window(
        rounds,
        kind=kind,  # type: ignore[arg-type]
        course_id=course_id,
        n_course=max(len(fixture_leg.get("values") or []), 5),
        as_of=as_of,
    )
    if kind == "l5_course" and win.empty:
        return {
            "golf_pp_reconcile": "no_pp_chart",
            "pp_mismatch_rounds": "no_course_rows",
            "pp_chart_filter": filt,
            "pp_chart_n": len(fixture_leg.get("values") or []),
        }
    if win.empty or not stat_col or stat_col not in win.columns:
        return {
            "golf_pp_reconcile": "mismatch",
            "pp_mismatch_rounds": "no_window",
            "pp_chart_filter": filt,
            "pp_chart_n": len(fixture_leg.get("values") or []),
        }
    if bob and "birdies_or_better" in win.columns:
        ours = pd.to_numeric(win["birdies_or_better"], errors="coerce").dropna().tolist()
    else:
        ours = pd.to_numeric(win[stat_col], errors="coerce").dropna().tolist()
    pp_vals = list(fixture_leg.get("values") or [])
    status, bad = compare_chart_values(ours, pp_vals)
    return {
        "golf_pp_reconcile": status,
        "pp_mismatch_rounds": "|".join(str(i) for i in bad),
        "pp_chart_filter": filt,
        "pp_chart_n": len(pp_vals),
        "pp_ours_window": ours[: len(pp_vals)],
    }


def apply_pp_reconcile(
    props: pd.DataFrame,
    rounds_cache: pd.DataFrame,
    *,
    fixture_path: Path | None = None,
) -> pd.DataFrame:
    """Attach reconcile columns; set void hold on mismatch."""
    out = props.copy()
    fixtures = load_pp_chart_fixture(fixture_path)
    for col, default in (
        ("golf_pp_reconcile", "no_pp_chart"),
        ("pp_mismatch_rounds", ""),
        ("pp_chart_filter", ""),
        ("pp_chart_n", 0),
    ):
        if col not in out.columns:
            out[col] = default

    if "player" not in out.columns and "player_name" in out.columns:
        out["player"] = out["player_name"]
    if "player_key" not in out.columns:
        out["player_key"] = out["player"].astype(str).map(_player_key)

    cache = rounds_cache.copy()
    if not cache.empty and "player_key" not in cache.columns and "player_name" in cache.columns:
        cache["player_key"] = cache["player_name"].map(_player_key)

    n_match = n_mis = n_none = 0
    for idx, row in out.iterrows():
        player = str(row.get("player") or row.get("player_name") or "")
        prop = str(row.get("prop_type") or "")
        line = pd.to_numeric(row.get("line"), errors="coerce")
        line_f = float(line) if pd.notna(line) else None
        leg = _find_fixture_leg(fixtures, player=player, prop_type=prop, line=line_f)
        pk = _player_key(player)
        rounds = cache[cache["player_key"] == pk] if not cache.empty else cache
        result = reconcile_leg(
            rounds,
            prop_type=prop,
            line=line_f,
            fixture_leg=leg,
        )
        for k, v in result.items():
            if k == "pp_ours_window":
                continue
            out.at[idx, k] = v
        status = result["golf_pp_reconcile"]
        if status == "match":
            n_match += 1
        elif status == "mismatch":
            n_mis += 1
            # Hold — data error, same class as attach_suspect.
            out.at[idx, "void_reason"] = "golf_pp_mismatch"
            if "unsupported_reason" in out.columns:
                prev = str(out.at[idx, "unsupported_reason"] or "").strip()
                if not prev:
                    out.at[idx, "unsupported_reason"] = "golf_pp_mismatch"
        else:
            n_none += 1
    print(
        f"[Golf PP reconcile] match={n_match} mismatch={n_mis} "
        f"no_pp_chart={n_none} fixture_legs={len(fixtures)}"
    )
    return out
