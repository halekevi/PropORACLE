#!/usr/bin/env python3
"""Weekly comparison of gated Goblin, mixed, and Standard tracks.

ROI / promotion counters use real scraped quotes only (scraped payout_source
+ non-null captured_at). Ungraded slips stay out of the win rate.
"""
from __future__ import annotations

import json
from pathlib import Path

from utils.comparison_tracks import summarize_tracks
from utils.l5_only_shadow import summarize_shadow_props
try:
    from utils.l5_ranked_by_cell import COMPARE_AFTER, TRACK as L5_RANKED_TRACK, stamped_slip_count
except ImportError:  # live tree may not carry the ranked-by-cell track yet
    COMPARE_AFTER, L5_RANKED_TRACK, stamped_slip_count = None, None, None

_LOG = Path(__file__).resolve().parents[1] / "data" / "reports" / "track_comparison.jsonl"


def load_rows(path: Path | None = None) -> list[dict]:
    dest = path or _LOG
    if not dest.is_file():
        return []
    rows = []
    for line in dest.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(rec, dict):
            rows.append(rec)
    return rows


def main() -> None:
    rows = load_rows()
    if not rows:
        print("No track log yet.")
        return
    from utils.scraped_quote import log_real_quote_coverage

    # Slip rows only — legs are paper / shadow and usually have no quote.
    slips = [r for r in rows if str(r.get("kind") or "slip") != "leg"]
    log_real_quote_coverage(slips, label="track_log")
    for rec in summarize_tracks(rows):
        wr = rec["win_rate"]
        wr_s = f"{wr:.1%}" if wr is not None else "n/a"
        band = ""
        if rec["wilson_low"] is not None:
            band = f" ({rec['wilson_low']:.1%}–{rec['wilson_high']:.1%})"
        pred = rec["predicted_sweep"]
        pred_s = f"{pred:.1%}" if pred is not None else "n/a"
        roi = rec["roi_scraped"]
        roi_s = f"{roi:.1%}" if roi is not None else "n/a"
        sports = ", ".join(rec["standard_sports"]) or "none"
        rq = rec.get("real_quote_n") or 0
        rq_wr = rec.get("real_quote_win_rate")
        rq_wr_s = f"{rq_wr:.1%}" if rq_wr is not None else "n/a"
        rq_band = ""
        if rec.get("real_quote_wilson_low") is not None:
            rq_band = (
                f" ({rec['real_quote_wilson_low']:.1%}–"
                f"{rec['real_quote_wilson_high']:.1%})"
            )
        print(
            f"{rec['track']}: slips {rec['slips']}, graded {rec['graded']}, "
            f"win {wr_s}{band}, predicted {pred_s}, "
            f"real-quote graded {rq} win {rq_wr_s}{rq_band}, "
            f"ROI on {rec['scraped_n']} scraped {roi_s}, standard sports {sports}"
        )
    print("\nL5=5 shadow by sport and prop (forward grades)")
    for rec in summarize_shadow_props(rows):
        wr = rec["win_rate"]
        wr_s = f"{wr:.1%}" if wr is not None else "ungraded"
        band = ""
        if rec["wilson_low"] is not None:
            band = f" ({rec['wilson_low']:.1%}–{rec['wilson_high']:.1%})"
        cell = rec["cell_p"]
        cell_s = f"{cell:.1%}" if cell is not None else "no cell"
        flag = "CLEARS break-even" if rec["clears_breakeven"] else "under break-even"
        print(
            f"  {rec['sport']} {rec['prop']} {rec['pick_type']} {rec['direction']}: "
            f"legs {rec['legs']}, graded {rec['graded']}, win {wr_s}{band}, "
            f"cell {cell_s}, bar {rec['breakeven']:.1%} at {rec['median_quote']}x, {flag}"
        )
    from utils.nfl_std_under_shadow import PREREG_DATE as NFLU_PREREG, summarize as nflu_summary

    nflu = nflu_summary()
    bar = nflu["bar"]
    print(
        f"\nNFL Standard UNDER shadow (prereg {NFLU_PREREG}; tackles only, sacks held 2026-10-25 until attach, L10>=8 + cover>=1, "
        f"2-leg Power): bar {bar['decided_min']} decided, breakeven {bar['breakeven']:.1%} "
        f"({bar['quote_basis']}, real quotes {bar['real_quotes_n']})"
    )
    for arm, rec in nflu["arms"].items():
        wr = rec["hit_rate"]
        wr_s = f"{wr:.1%}" if wr is not None else "ungraded"
        band = ""
        if rec["wilson_low"] is not None:
            band = f" ({rec['wilson_low']:.1%}–{rec['wilson_high']:.1%})"
        status = "BAR MET" if rec["bar_met"] else (rec["bar_note"] or "not yet")
        print(
            f"  {arm}: legs {rec['legs']}, decided {rec['decided']}, win {wr_s}{band}, "
            f"not on board {rec['not_on_board']}, slips {rec['slips']} "
            f"({rec['slip_wins']}/{rec['slips_decided']} decided), {status}"
        )
    from utils.nfl_demon_volume_shadow import PREREG_DATE as NFLD_PREREG, summarize as nfld_summary

    nfld = nfld_summary()
    dbar = nfld["bar"]
    print(
        f"\nNFL Demon volume observe (prereg {NFLD_PREREG}; rush-attempt lead backs + "
        f"reception target leaders, favored-or-close, line under recent volume, "
        f"Demon OVER only): bar {dbar['decided_min']} "
        f"decided, breakeven {dbar['breakeven']}, {dbar['quote_basis']} "
        f"(real quotes {dbar['real_quotes_n']})"
    )
    for arm, rec in nfld["arms"].items():
        wr = rec["hit_rate"]
        wr_s = f"{wr:.1%}" if wr is not None else "ungraded"
        band = ""
        if rec["wilson_low"] is not None:
            band = f" ({rec['wilson_low']:.1%}–{rec['wilson_high']:.1%})"
        status = "BAR MET" if rec["bar_met"] else (rec["bar_note"] or "not yet")
        print(
            f"  {arm}: legs {rec['legs']}, decided {rec['decided']}, win {wr_s}{band}, "
            f"not on board {rec['not_on_board']}, {status}"
        )
    from utils.nfl_demon_rec_floor_shadow import (
        PREREG_DATE as NFLDF_PREREG,
        summarize as nfldf_summary,
    )

    nfldf = nfldf_summary()
    fbar = nfldf["bar"]
    print(
        f"\nNFL Demon rec-floor observe (prereg {NFLDF_PREREG}; top-2 target "
        f"share ≥15pp, Demon receptions OVER under L5 min; not the leader gate): "
        f"bar {fbar['decided_min']} decided, breakeven {fbar['breakeven']}, "
        f"{fbar['quote_basis']} (real quotes {fbar['real_quotes_n']})"
    )
    for arm, rec in nfldf["arms"].items():
        wr = rec["hit_rate"]
        wr_s = f"{wr:.1%}" if wr is not None else "ungraded"
        band = ""
        if rec["wilson_low"] is not None:
            band = f" ({rec['wilson_low']:.1%}–{rec['wilson_high']:.1%})"
        status = "BAR MET" if rec["bar_met"] else (rec["bar_note"] or "not yet")
        print(
            f"  {arm}: legs {rec['legs']}, decided {rec['decided']}, win {wr_s}{band}, "
            f"not on board {rec['not_on_board']}, {status}"
        )
    if stamped_slip_count is None:
        print("\nl5_ranked_by_cell: module not on this tree, section skipped")
        return
    stamped = stamped_slip_count()
    print(f"\n{L5_RANKED_TRACK}: stamped slips {stamped}")
    if stamped < COMPARE_AFTER:
        print(f"  compare with the gated build after {COMPARE_AFTER} stamped slips")
        return
    by_track = {rec["track"]: rec for rec in summarize_tracks(rows)}
    ranked = by_track.get(L5_RANKED_TRACK)
    if ranked is None:
        return
    for other in ("gated_goblin", "gated_standard"):
        base = by_track.get(other)
        if base is None or base["win_rate"] is None or ranked["win_rate"] is None:
            print(f"  {other}: not enough graded slips to compare")
            continue
        print(
            f"  vs {other}: ranked win {ranked['win_rate']:.1%} "
            f"({ranked['graded']} graded), gated win {base['win_rate']:.1%} "
            f"({base['graded']} graded)"
        )


if __name__ == "__main__":
    main()
