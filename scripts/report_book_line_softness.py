#!/usr/bin/env python3
"""Summarize FD/DK/UD vs PrizePicks same-line softness from book_line_history."""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))

from utils.book_line_ladder import PP_NEED, load_history  # noqa: E402


def _fmt_pct(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return f"{100.0 * float(x):.0f}%"


def summarize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "pp_pick",
                "book",
                "n_same_line",
                "book_median_chance",
                "share_book_below_pp_need",
                "share_book_above_pp_need",
            ]
        )
    rows = []
    for (pick, book), g in df.groupby(["pp_pick", "book"], dropna=False):
        p = pd.to_numeric(g["book_implied_p"], errors="coerce").dropna()
        need = PP_NEED.get(str(pick), float("nan"))
        rows.append(
            {
                "pp_pick": pick,
                "book": book,
                "n_same_line": int(len(p)),
                "book_median_chance": float(p.median()) if len(p) else None,
                "share_book_below_pp_need": float((p < need).mean()) if len(p) else None,
                "share_book_above_pp_need": float((p > need).mean()) if len(p) else None,
            }
        )
    return pd.DataFrame(rows).sort_values(["pp_pick", "book"]).reset_index(drop=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="Report book-line softness vs PP")
    ap.add_argument("--repo-root", default=str(_ROOT))
    ap.add_argument("--last-days", type=int, default=7)
    args = ap.parse_args()
    root = Path(args.repo_root)

    hist = load_history(root, "pp_same_line")
    report_dir = root / "data" / "reports"
    report_dir.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=max(1, args.last_days))
    if not hist.empty and "fetched_at" in hist.columns:
        ts = pd.to_datetime(hist["fetched_at"], utc=True, errors="coerce")
        recent = hist[ts >= cutoff]
    else:
        recent = hist

    all_sum = summarize(hist)
    recent_sum = summarize(recent)

    payload = {
        "generated_at": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "pp_need": {k: round(v, 4) for k, v in PP_NEED.items()},
        "n_history_rows": int(len(hist)),
        "n_last_days_rows": int(len(recent)),
        "last_days": args.last_days,
        "cumulative": all_sum.to_dict(orient="records"),
        "last_days_table": recent_sum.to_dict(orient="records"),
        "note": "Reference tape only. Goblins vs exact same-line book rungs; Standards vs book mains at exact line.",
    }
    json_path = report_dir / "book_line_softness_latest.json"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Book line softness (FD / DK / UD vs PrizePicks)",
        "",
        f"Generated: `{payload['generated_at']}`",
        "",
        f"PP need per leg: Standard 3-pick 6x = {_fmt_pct(PP_NEED['Standard'])} | "
        f"Goblin 3-pick 1.65x = {_fmt_pct(PP_NEED['Goblin'])}",
        "",
        f"History rows: **{len(hist)}** | last {args.last_days}d: **{len(recent)}**",
        "",
        "## Cumulative",
        "",
        "| pick | book | n | median chance | share below PP need | share above |",
        "|---|---|---:|---:|---:|---:|",
    ]
    for r in all_sum.to_dict(orient="records"):
        lines.append(
            f"| {r['pp_pick']} | {r['book']} | {r['n_same_line']} | "
            f"{_fmt_pct(r['book_median_chance'])} | {_fmt_pct(r['share_book_below_pp_need'])} | "
            f"{_fmt_pct(r['share_book_above_pp_need'])} |"
        )
    lines.extend(
        [
            "",
            f"## Last {args.last_days} days",
            "",
            "| pick | book | n | median chance | share below PP need | share above |",
            "|---|---|---:|---:|---:|---:|",
        ]
    )
    for r in recent_sum.to_dict(orient="records"):
        lines.append(
            f"| {r['pp_pick']} | {r['book']} | {r['n_same_line']} | "
            f"{_fmt_pct(r['book_median_chance'])} | {_fmt_pct(r['share_book_below_pp_need'])} | "
            f"{_fmt_pct(r['share_book_above_pp_need'])} |"
        )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Reference tape only — not a ticket gate.",
            "- Underdog american odds are market tape; UD payout multipliers live on ladder rows.",
            "- DraftKings NBA pregame props may be empty until tip; NFL alts use N+ labels.",
            "- Kalshi / bet365 are out of scope.",
            "",
        ]
    )
    md_path = report_dir / "book_line_softness_latest.md"
    md_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"[ok] {md_path}")
    print(f"[ok] {json_path}")
    if not all_sum.empty:
        print(all_sum.to_string(index=False))


if __name__ == "__main__":
    main()
