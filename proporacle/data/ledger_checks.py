"""Warn-only ledger checks (Pandera when installed, pandas otherwise).

These are the fill-by-date / ticket-join rules that would have caught the
October NBA usage blank, the Standard-face join gap, and a missing ticket-run
archive the day they started. They do not fail the pipeline unless --strict.
"""

from __future__ import annotations

import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))

from proporacle.data.leg_ledger import (
    EDGE_RATIO_FLAG,
    USAGE_REQUIRED_SPORTS,
    fill_by_date,
    ticket_join_summary,
)

try:
    import pandera.pandas as pa

    HAS_PANDERA = True
except ImportError:  # pragma: no cover
    pa = None  # type: ignore
    HAS_PANDERA = False

NO_ML_SPORTS = frozenset({"CFB", "NFL"})
JOIN_FLOOR = 0.90
USAGE_FLOOR = 0.50
ML_FLOOR = 0.50
EDGE_RATIO_SPORT_FLOOR = 0.01


@dataclass
class LedgerWarning:
    rule: str
    detail: str
    slate_date: str = ""
    sport: str = ""


@dataclass
class LedgerCheckReport:
    warnings: list[LedgerWarning] = field(default_factory=list)
    since: str = ""
    pandera: bool = HAS_PANDERA

    @property
    def ok(self) -> bool:
        return not self.warnings


def _since_default(days: int = 14) -> str:
    return (date.today() - timedelta(days=days)).isoformat()


def _in_window(slate: str, since: str | None) -> bool:
    if not since:
        return True
    return str(slate or "")[:10] >= since[:10]


def run_ledger_checks(
    root: Path | None = None,
    *,
    since: str | None = None,
    repo: Path | None = None,
) -> LedgerCheckReport:
    since = (since or _since_default()).strip()[:10]
    report = LedgerCheckReport(since=since, pandera=HAS_PANDERA)
    by = fill_by_date(root)
    if not by.empty:
        for row in by.itertuples(index=False):
            ds = str(getattr(row, "slate_date", "") or "")[:10]
            if not _in_window(ds, since):
                continue
            sport = str(getattr(row, "sport", "") or "")
            n = int(getattr(row, "n", 0) or 0)
            if n <= 0:
                continue
            league = float(getattr(row, "league_fill", 0) or 0)
            usage = float(getattr(row, "usage_fill", 0) or 0)
            ml = float(getattr(row, "ml_prob_fill", 0) or 0)
            edge = float(getattr(row, "edge_ratio_gt5", 0) or 0)
            if league <= 0:
                report.warnings.append(
                    LedgerWarning("league_fill", f"{sport} league fill {league:.3f} on {ds}", ds, sport)
                )
            if sport.upper() in USAGE_REQUIRED_SPORTS and usage < USAGE_FLOOR:
                report.warnings.append(
                    LedgerWarning(
                        "usage_fill",
                        f"{sport} usage fill {usage:.3f} < {USAGE_FLOOR:.2f} on {ds}",
                        ds,
                        sport,
                    )
                )
            if sport.upper() not in NO_ML_SPORTS and ml < ML_FLOOR:
                report.warnings.append(
                    LedgerWarning(
                        "ml_prob_fill",
                        f"{sport} ml_prob fill {ml:.3f} < {ML_FLOOR:.2f} on {ds}",
                        ds,
                        sport,
                    )
                )
            if edge > EDGE_RATIO_SPORT_FLOOR:
                report.warnings.append(
                    LedgerWarning(
                        "edge_ratio",
                        f"{sport} edge_ratio>{EDGE_RATIO_FLAG} share {edge:.3f} on {ds}",
                        ds,
                        sport,
                    )
                )
    joins = ticket_join_summary(root)
    if not joins.empty:
        for row in joins.itertuples(index=False):
            ds = str(getattr(row, "slate_date", "") or "")[:10]
            if not _in_window(ds, since):
                continue
            rate = float(getattr(row, "match_rate", 0) or 0)
            n = int(getattr(row, "ticket_leg_rows", 0) or 0)
            if n > 0 and rate < JOIN_FLOOR:
                report.warnings.append(
                    LedgerWarning(
                        "ticket_join",
                        f"ticket-to-graded join {rate:.3f} < {JOIN_FLOOR:.2f} on {ds} (n={n})",
                        ds,
                    )
                )
    try:
        from ticket_leg_index import dates_with_tickets_missing_runs
    except ImportError:
        from scripts.ticket_leg_index import dates_with_tickets_missing_runs  # type: ignore

    for ds in dates_with_tickets_missing_runs(repo):
        if _in_window(ds, since):
            report.warnings.append(
                LedgerWarning(
                    "missing_ticket_run",
                    f"tickets exist for {ds} but no ticket_runs archive",
                    ds,
                )
            )
    if HAS_PANDERA and pa is not None and not by.empty:
        try:
            schema = pa.DataFrameSchema(
                {
                    "slate_date": pa.Column(str, nullable=False),
                    "sport": pa.Column(str, nullable=False),
                    "n": pa.Column("int64", nullable=False),
                    "league_fill": pa.Column(float, nullable=False),
                    "usage_fill": pa.Column(float, nullable=False),
                    "ml_prob_fill": pa.Column(float, nullable=False),
                    "edge_ratio_gt5": pa.Column(float, nullable=False),
                },
                coerce=True,
                strict=False,
            )
            schema.validate(by, lazy=True)
        except Exception as exc:
            report.warnings.append(LedgerWarning("pandera_schema", str(exc)[:300]))
    return report


def format_report(report: LedgerCheckReport) -> str:
    lines = [
        f"ledger checks since {report.since}  pandera={'on' if report.pandera else 'off (pandas)'}  "
        f"warnings={len(report.warnings)}"
    ]
    if not report.warnings:
        lines.append("  (none)")
        return "\n".join(lines)
    for w in report.warnings:
        loc = " ".join(x for x in (w.slate_date, w.sport) if x)
        lines.append(f"  WARN [{w.rule}] {loc}: {w.detail}".rstrip())
    return "\n".join(lines)
