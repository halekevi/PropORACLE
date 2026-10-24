#!/usr/bin/env python3
"""Replay ce04cad8c against the real 2026-09-15 pool/card.

Proves Mesa / Palacios / Wood / Jensen are excluded by the CORE pool filter
and by filter_main_high_prob_payload on the published MAIN/CORE slips.

  py -3.14 scripts/replay_sep15_mlb_keep_fix.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parents[1]
MAIN_CP = REPO.parent / "PropORACLE_main_cp"
sys.path.insert(0, str(REPO))
sys.path.insert(0, str(REPO / "scripts"))

from combined_slate_tickets import (  # noqa: E402
    _leg_mlb_construction_banned,
    _prepare_core_pipeline_pool,
    filter_main_high_prob_payload,
)
from utils.mlb_keep_gates import mlb_goblin_keep_eligible, player_slash  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible  # noqa: E402

FLAGGED = ("mesa", "palacios", "james wood", "carter jensen")
DATE = "2026-09-15"


def _is_flagged(player: object) -> bool:
    pl = str(player or "").lower()
    return any(x in pl for x in FLAGGED)


def _load_mlb_step8() -> pd.DataFrame:
    path = MAIN_CP / "outputs" / DATE / "mlb" / "step8_mlb_direction.csv"
    if not path.is_file():
        path = REPO / "outputs" / DATE / "mlb" / "step8_mlb_direction.csv"
    df = pd.read_csv(path, low_memory=False)
    df = df.copy()
    df["sport"] = "MLB"
    if "direction" not in df.columns and "bet_direction" in df.columns:
        df["direction"] = df["bet_direction"]
    if "l5_over" not in df.columns and "last5_over" in df.columns:
        df["l5_over"] = df["last5_over"]
    if "l10_over" not in df.columns and "last10_over" in df.columns:
        df["l10_over"] = df["last10_over"]
    return df


def _flagged_in_df(df: pd.DataFrame, label: str) -> list[str]:
    if df is None or df.empty or "player" not in df.columns:
        print(f"  [{label}] empty")
        return []
    hits = []
    for _, r in df.iterrows():
        if not _is_flagged(r.get("player")):
            continue
        pt = str(r.get("pick_type") or "")
        prop = str(r.get("prop_type") or r.get("prop") or "")
        if "goblin" not in pt.lower():
            continue
        if not any(x in prop.lower() for x in ("hit", "base", "rbi")):
            continue
        hits.append(f"{r.get('player')}|{prop}|{r.get('line')}|L5={r.get('l5_over')}")
    print(f"  [{label}] flagged goblin counting legs: {len(hits)}")
    for h in hits:
        print(f"    {h}")
    return hits


def _iter_ticket_legs(payload: dict):
    for g in payload.get("groups") or []:
        gname = str(g.get("group_name") or "")
        for t in g.get("tickets") or []:
            for leg in t.get("legs") or []:
                yield gname, t, leg


def replay_core_pool() -> bool:
    print("\n=== 1) CORE pool prep on Sep-15 MLB step8 ===")
    df = _load_mlb_step8()
    gob = df[df["pick_type"].astype(str).str.lower().eq("goblin")].copy()
    before = _flagged_in_df(gob, "raw goblin board")
    core = _prepare_core_pipeline_pool("MLB", gob)
    after = _flagged_in_df(core if core is not None else pd.DataFrame(), "after _prepare_core_pipeline_pool")
    ok = len(after) == 0
    print(f"  RESULT: {'PASS' if ok else 'FAIL'} — flagged in core pool before={len(before)} after={len(after)}")
    return ok


def replay_main_filter() -> bool:
    print("\n=== 2) filter_main_high_prob_payload on published Sep-15 card ===")
    live = MAIN_CP / "ui_runner" / "templates" / "tickets_latest.json"
    payload = json.loads(live.read_text(encoding="utf-8"))

    def count_flagged(p: dict, family: str | None = None) -> list[str]:
        out = []
        for gname, t, leg in _iter_ticket_legs(p):
            if not _is_flagged(leg.get("player")):
                continue
            if family == "core" and not t.get("core_build") and "Core" not in gname:
                continue
            if family == "g70" and "Goblin-70" not in gname:
                continue
            if family == "mixer" and ("Goblin-70" in gname or "Core" in gname):
                continue
            out.append(
                f"{gname}|{leg.get('player')}|{leg.get('prop_type') or leg.get('prop')}|"
                f"core={bool(t.get('core_build'))}"
            )
        return out

    before_all = count_flagged(payload)
    print(f"  published flagged legs: all={len(before_all)}")
    for x in before_all:
        print(f"    {x}")

    # Dual-card path: pool_mode=goblin70 must still scrub CORE/mixer hygiene.
    dual = filter_main_high_prob_payload(dict(payload))
    dual_core_mixer = [
        f"{gname}|{leg.get('player')}|{leg.get('prop_type') or leg.get('prop')}"
        for gname, t, leg in _iter_ticket_legs(dual)
        if _is_flagged(leg.get("player")) and "Goblin-70" not in gname
    ]
    print(
        f"  dual-card filter (pool_mode={dual.get('pool_mode')!r}) "
        f"CORE/mixer flagged remaining: {len(dual_core_mixer)} "
        f"hygiene_dropped={dual.get('construction_hygiene_dropped')}"
    )
    for x in dual_core_mixer:
        print(f"    {x}")
    ok_dual = len(dual_core_mixer) == 0 and str(dual.get("pool_mode") or "") == "goblin70"

    # MAIN-mode path on CORE+mixer groups only.
    main_groups = [
        g
        for g in payload.get("groups") or []
        if "Goblin-70" not in str(g.get("group_name") or "")
        and "YOLO" not in str(g.get("group_name") or "").upper()
    ]
    main_like = dict(payload)
    main_like["pool_mode"] = "high_prob_std_gob"
    main_like["goblin_only"] = False
    main_like["groups"] = main_groups
    filtered = filter_main_high_prob_payload(main_like)
    remaining = [
        f"{gname}|{leg.get('player')}|{leg.get('prop_type') or leg.get('prop')}"
        for gname, t, leg in _iter_ticket_legs(filtered)
        if _is_flagged(leg.get("player"))
    ]
    remaining = list(dict.fromkeys(remaining))
    print(f"  MAIN-mode flagged remaining: {len(remaining)}")
    for x in remaining:
        print(f"    {x}")
    ok_main = len(remaining) == 0
    ok = ok_dual and ok_main
    print(
        f"  RESULT: {'PASS' if ok else 'FAIL'} — "
        f"dual_hygiene={'PASS' if ok_dual else 'FAIL'} "
        f"main_filter={'PASS' if ok_main else 'FAIL'}"
    )
    return ok


def replay_g70_eligibility() -> None:
    print("\n=== 3) Goblin-70: published legs vs current keep (staleness check) ===")
    live = json.loads(
        (MAIN_CP / "ui_runner" / "templates" / "tickets_latest.json").read_text(encoding="utf-8")
    )
    fail = []
    for gname, t, leg in _iter_ticket_legs(live):
        if "Goblin-70" not in gname:
            continue
        if str(leg.get("sport") or "").upper() != "MLB":
            continue
        if "goblin" not in str(leg.get("pick_type") or "").lower():
            continue
        row = {
            "sport": "MLB",
            "player": leg.get("player"),
            "prop": leg.get("prop_type") or leg.get("prop"),
            "pick_type": "Goblin",
            "side": str(leg.get("direction") or leg.get("side") or "OVER").upper(),
            "line": leg.get("line"),
            "l5_over": leg.get("l5_over"),
            "l10_over": leg.get("l10_over"),
            "def": leg.get("def_tier") or leg.get("def"),
            "batting_avg": leg.get("batting_avg") or leg.get("ba"),
            "mlb_player_id": leg.get("mlb_player_id"),
        }
        keep = mlb_goblin_keep_eligible(row)
        g70 = goblin_70_eligible(row)
        banned = _leg_mlb_construction_banned({**row, "prop_type": row["prop"], "direction": row["side"]})
        if not keep or not g70 or banned:
            fail.append(
                {
                    "group": gname,
                    "player": row["player"],
                    "prop": row["prop"],
                    "slash": player_slash(row),
                    "keep": keep,
                    "g70": g70,
                    "banned": banned,
                }
            )
    print(f"  published G70 MLB goblin legs failing keep NOW: {len(fail)}")
    for f in fail[:25]:
        print(f"    {f}")
    print(
        "  NOTE: G70-family slips are not scrubbed from published web legs "
        "(incomplete L10/own-def false-fails). They drop on the next board "
        "rebuild. CORE/mixer keep hygiene now runs on every dual-card merge."
    )


def main() -> int:
    print(f"Replay date={DATE} fix=ce04cad8c (+ CORE/STRONG ban-first)")
    ok1 = replay_core_pool()
    ok2 = replay_main_filter()
    replay_g70_eligibility()
    print("\n=== SUMMARY ===")
    print(f"  core pool excludes flagged: {'PASS' if ok1 else 'FAIL'}")
    print(f"  MAIN filter excludes flagged: {'PASS' if ok2 else 'FAIL'}")
    if ok1 and ok2:
        print("  ce04cad8c confirmed against real Sep-15 data.")
        return 0
    print("  FIX NOT CONFIRMED — investigate remaining hits.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
