"""PP chart DNP-as-zero detector.

PrizePicks charts sometimes show a 0 for a game the player did not play
(no boxscore row, or 0 with no minutes). That inflates Unders. When chart
values exist (scrape or fixture), flag ``pp_shows_dnp_zero`` and **hold**
the leg until verified — same class as attach / PP reconcile mismatches,
not a flag-only soft warn.

NBA wiring is dormant until ``pp_chart_values`` (or a fixture leg) is
present. Until then, verify Unders on injured/returning players by eye
against boxscore L5.

See ``Sports/NBA/PP_DNP_ZERO.md``.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Sequence

import pandas as pd

from utils.player_name_utils import normalize_player_name

_REPO = Path(__file__).resolve().parents[1]
DEFAULT_NBA_FIXTURE = _REPO / "Sports" / "NBA" / "data" / "fixtures" / "pp_chart_manual.json"

VOID_REASON = "pp_shows_dnp_zero"


@dataclass(frozen=True)
class DnpZeroHit:
    reason: str  # no_boxscore_row | zero_no_minutes | leading_dnp_zero
    chart_index: int
    chart_value: float
    chart_date: str | None = None

    def as_dict(self) -> dict[str, object]:
        return asdict(self)


def _f(val: object) -> float | None:
    v = pd.to_numeric(val, errors="coerce")
    if pd.isna(v):
        return None
    return float(v)


def _date10(val: object) -> str:
    s = str(val or "").strip()
    return s[:10] if s and s.lower() not in {"nan", "none", "nat"} else ""


def _minutes(val: object) -> float | None:
    return _f(val)


def check_pp_dnp_zeros(
    chart_entries: Sequence[dict[str, Any]] | Sequence[object],
    boxscore_rows: Sequence[dict[str, Any]],
) -> list[DnpZeroHit]:
    """Return hits when the PP chart shows a DNP-style zero.

    ``chart_entries`` newest-first: either floats or dicts with
    ``value`` / ``date`` / optional ``minutes``.
    ``boxscore_rows`` newest-first dicts with ``game_date``, ``minutes``,
    and the stat (any extra keys ok).
    """
    charts: list[dict[str, Any]] = []
    for i, raw in enumerate(chart_entries):
        if isinstance(raw, dict):
            charts.append(
                {
                    "value": _f(raw.get("value", raw.get("stat", raw.get("reb")))),
                    "date": _date10(raw.get("date") or raw.get("game_date")),
                    "minutes": _minutes(raw.get("minutes") or raw.get("min")),
                }
            )
        else:
            charts.append({"value": _f(raw), "date": "", "minutes": None})

    boxes: list[dict[str, Any]] = []
    by_date: dict[str, dict[str, Any]] = {}
    for raw in boxscore_rows:
        if not isinstance(raw, dict):
            continue
        d = _date10(raw.get("game_date") or raw.get("date"))
        row = {
            "date": d,
            "minutes": _minutes(raw.get("minutes") or raw.get("min")),
            "value": _f(
                raw.get("value")
                or raw.get("stat")
                or raw.get("reb")
                or raw.get("pts")
                or raw.get("actual")
            ),
        }
        boxes.append(row)
        if d:
            by_date[d] = row

    hits: list[DnpZeroHit] = []
    dated = any(c["date"] for c in charts)

    if dated:
        for i, c in enumerate(charts):
            val = c["value"]
            if val is None:
                continue
            d = c["date"]
            if not d:
                continue
            box = by_date.get(d)
            if box is None:
                # Chart lists a game our DB has no row for.
                hits.append(
                    DnpZeroHit(
                        reason="no_boxscore_row",
                        chart_index=i,
                        chart_value=float(val),
                        chart_date=d,
                    )
                )
                continue
            mins = box["minutes"]
            if float(val) == 0.0 and (mins is None or mins <= 0.0):
                hits.append(
                    DnpZeroHit(
                        reason="zero_no_minutes",
                        chart_index=i,
                        chart_value=0.0,
                        chart_date=d,
                    )
                )
        return hits

    # Value-only chart (no dates): detect leading 0 that is not in boxscore L5,
    # where chart[1:6] matches box[:5] (Jaren / Cooley shape).
    box_vals = [b["value"] for b in boxes if b["value"] is not None]
    chart_vals = [c["value"] for c in charts if c["value"] is not None]
    if (
        len(chart_vals) >= 2
        and len(box_vals) >= 4
        and float(chart_vals[0]) == 0.0
        and box_vals
        and float(box_vals[0]) != 0.0
    ):
        # PP: 0 + our L5 without the oldest → 0,4,5,4,6 vs box 4,5,4,6,9
        truncated = [float(x) for x in box_vals[:4]]
        rest = [float(x) for x in chart_vals[1:5]]
        if rest == truncated:
            hits.append(
                DnpZeroHit(
                    reason="leading_dnp_zero",
                    chart_index=0,
                    chart_value=0.0,
                    chart_date=None,
                )
            )
    return hits


def load_nba_chart_fixture(path: Path | None = None) -> list[dict[str, Any]]:
    p = path or DEFAULT_NBA_FIXTURE
    if not p.is_file():
        return []
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except Exception:
        return []
    legs = raw.get("legs") if isinstance(raw, dict) else raw
    if not isinstance(legs, list):
        return []
    out: list[dict[str, Any]] = []
    for leg in legs:
        if not isinstance(leg, dict):
            continue
        vals = leg.get("values") or leg.get("chart") or []
        dates = leg.get("dates") or []
        entries = []
        for i, v in enumerate(vals):
            entry: dict[str, Any] = {"value": v}
            if i < len(dates):
                entry["date"] = dates[i]
            entries.append(entry)
        out.append(
            {
                "player_key": normalize_player_name(str(leg.get("player") or "")).lower(),
                "player": str(leg.get("player") or "").strip(),
                "prop_type": str(leg.get("prop_type") or leg.get("prop") or "").strip(),
                "line": leg.get("line"),
                "entries": entries,
                "values": [float(v) for v in vals if v is not None and str(v).strip() != ""],
            }
        )
    return out


def _player_key(name: str) -> str:
    return normalize_player_name(name).lower()


def _find_fixture(
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
        lp = str(leg.get("prop_type") or "").strip().lower()
        if lp != prop_l and lp not in prop_l and prop_l not in lp:
            continue
        if line is not None and leg.get("line") is not None:
            try:
                if abs(float(leg["line"]) - float(line)) > 0.01:
                    continue
            except (TypeError, ValueError):
                pass
        return leg
    return None


def boxscore_rows_from_db(
    con: Any,
    *,
    espn_id: str,
    stat_col: str = "reb",
    n: int = 10,
) -> list[dict[str, Any]]:
    """Load newest-first NBA box rows for DNP checks."""
    if not espn_id or not str(espn_id).strip():
        return []
    sql = f"""
        SELECT game_date, team, home_team, away_team, minutes, {stat_col} AS value
        FROM nba
        WHERE cast(espn_athlete_id AS TEXT)=?
          AND {stat_col} IS NOT NULL
        ORDER BY game_date DESC
        LIMIT ?
    """
    try:
        cur = con.execute(sql, (str(espn_id).strip(), int(n)))
    except Exception:
        return []
    rows = []
    for r in cur.fetchall():
        if hasattr(r, "keys"):
            rows.append(dict(r))
        else:
            rows.append(
                {
                    "game_date": r[0],
                    "team": r[1],
                    "home_team": r[2],
                    "away_team": r[3],
                    "minutes": r[4],
                    "value": r[5],
                }
            )
    return rows


_PROP_STAT = {
    "rebounds": "reb",
    "points": "pts",
    "assists": "ast",
    "steals": "stl",
    "blocked shots": "blk",
    "blocks": "blk",
    "turnovers": "tov",
}


def prop_to_stat_col(prop_type: str) -> str | None:
    s = str(prop_type or "").strip().lower()
    for k, col in _PROP_STAT.items():
        if k in s:
            return col
    return None


def apply_pp_dnp_zero_holds(
    df: pd.DataFrame,
    *,
    boxscore_con: Any | None = None,
    fixture_path: Path | None = None,
    chart_values_col: str = "pp_chart_values",
    chart_dates_col: str = "pp_chart_dates",
) -> pd.DataFrame:
    """Attach ``pp_shows_dnp_zero`` / hold when chart data exists.

    Chart sources (first wins per row):
    1. Row columns ``pp_chart_values`` (+ optional ``pp_chart_dates``) from scrape
    2. Manual fixture ``Sports/NBA/data/fixtures/pp_chart_manual.json``

    No chart → leave alone (scrape not ready). Hit → ``void_reason=pp_shows_dnp_zero``.
    """
    if df is None or df.empty:
        return df
    out = df.copy()
    if "pp_shows_dnp_zero" not in out.columns:
        out["pp_shows_dnp_zero"] = 0
    if "pp_dnp_zero_reason" not in out.columns:
        out["pp_dnp_zero_reason"] = ""
    if "void_reason" not in out.columns:
        out["void_reason"] = ""

    fixtures = load_nba_chart_fixture(fixture_path)
    n_hold = 0
    n_skip = 0

    for idx, row in out.iterrows():
        player = str(row.get("player") or row.get("player_name") or "")
        prop = str(row.get("prop_type") or row.get("prop") or "")
        line = pd.to_numeric(row.get("line"), errors="coerce")
        line_f = float(line) if pd.notna(line) else None

        entries: list[dict[str, Any]] = []
        # 1) scrape columns
        raw_vals = row.get(chart_values_col) if chart_values_col in out.columns else None
        if raw_vals is not None and str(raw_vals).strip() not in {"", "nan", "None"}:
            if isinstance(raw_vals, str):
                parts = [p for p in raw_vals.replace(";", "|").split("|") if p.strip() != ""]
                vals = [_f(p) for p in parts]
            elif isinstance(raw_vals, (list, tuple)):
                vals = [_f(p) for p in raw_vals]
            else:
                vals = []
            dates_raw = row.get(chart_dates_col) if chart_dates_col in out.columns else None
            dates: list[str] = []
            if isinstance(dates_raw, str) and dates_raw.strip():
                dates = [d.strip()[:10] for d in dates_raw.replace(";", "|").split("|")]
            elif isinstance(dates_raw, (list, tuple)):
                dates = [_date10(d) for d in dates_raw]
            for i, v in enumerate(vals):
                if v is None:
                    continue
                e: dict[str, Any] = {"value": v}
                if i < len(dates) and dates[i]:
                    e["date"] = dates[i]
                entries.append(e)

        # 2) fixture
        if not entries:
            leg = _find_fixture(fixtures, player=player, prop_type=prop, line=line_f)
            if leg:
                entries = list(leg.get("entries") or [])
                if not entries and leg.get("values"):
                    entries = [{"value": v} for v in leg["values"]]

        if not entries:
            n_skip += 1
            continue

        boxes: list[dict[str, Any]] = []
        espn = str(row.get("ESPN_ATHLETE_ID") or row.get("espn_athlete_id") or "").strip()
        stat_col = prop_to_stat_col(prop) or "reb"
        if boxscore_con is not None and espn and espn.lower() != "nan":
            boxes = boxscore_rows_from_db(boxscore_con, espn_id=espn, stat_col=stat_col, n=12)
        if not boxes:
            # Fall back to attached stat_g* as played window (no dates).
            gvals = []
            for i in range(1, 6):
                v = _f(row.get(f"stat_g{i}"))
                if v is not None:
                    gvals.append({"value": v, "game_date": "", "minutes": 1.0})
            boxes = gvals

        hits = check_pp_dnp_zeros(entries, boxes)
        if not hits:
            continue
        n_hold += 1
        out.at[idx, "pp_shows_dnp_zero"] = 1
        out.at[idx, "pp_dnp_zero_reason"] = "|".join(
            f"{h.reason}@{h.chart_date or h.chart_index}" for h in hits
        )
        prev = str(out.at[idx, "void_reason"] or "").strip()
        if not prev:
            out.at[idx, "void_reason"] = VOID_REASON
        # Blank direction on hold (flags-not-drops: keep row).
        for col in ("final_bet_direction", "direction", "bet_direction"):
            if col in out.columns:
                out.at[idx, col] = ""

    print(
        f"[pp_dnp_zero] held={n_hold} no_chart_skip={n_skip} "
        f"fixture_legs={len(fixtures)}"
    )
    return out
