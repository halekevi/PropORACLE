"""Diagnose Boyd vs McGreevy under slate exposure ledger; explain 29→8."""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, r"H:\PropORACLE")
from utils.slate_exposure_ledger import (  # noqa: E402
    SlateExposureLedger,
    fold_player,
    pick_anchor_leg,
    player_atoms,
    prop_token,
    direction_token,
)

SRC = Path(r"H:\PropORACLE_main_cp\ui_runner\runtime\tickets_latest.json")


def main() -> None:
    d = json.loads(SRC.read_text(encoding="utf-8"))

    print("=== BOYD TICKETS IN WALK ORDER ===")
    led = SlateExposureLedger()
    for g in d.get("groups") or []:
        for t in g.get("tickets") or []:
            legs = [leg for leg in (t.get("legs") or []) if isinstance(leg, dict)]
            is_boyd = any(fold_player(leg.get("player")) == "matthew boyd" for leg in legs)
            ok, reason = led.can_add(legs)
            if is_boyd:
                anchor = pick_anchor_leg(legs)
                ancs = player_atoms(anchor.get("player")) if anchor else set()
                role = "ANCHOR" if "matthew boyd" in ancs else "NONANCHOR"
                boyd_props = [
                    (leg.get("prop_type"), leg.get("direction"), leg.get("line"))
                    for leg in legs
                    if fold_player(leg.get("player")) == "matthew boyd"
                ]
                roster = [leg.get("player") for leg in legs]
                print(
                    f"{'KEEP' if ok else 'DROP':4} role={role:9} "
                    f"{str(g.get('group_name'))[:28]:28} "
                    f"boyd={boyd_props}"
                )
                print(f"     roster={roster}")
                print(f"     anchor={anchor.get('player') if anchor else None}")
                print(f"     reason={reason or 'ok'}")
                exp = led.players.get("matthew boyd")
                print(
                    f"     ledger_before={exp}"
                )
            if ok:
                led.register(legs)

    print("\n=== MCGREEVY TICKETS IN WALK ORDER ===")
    led2 = SlateExposureLedger()
    for g in d.get("groups") or []:
        for t in g.get("tickets") or []:
            legs = [leg for leg in (t.get("legs") or []) if isinstance(leg, dict)]
            is_m = any(fold_player(leg.get("player")) == "michael mcgreevy" for leg in legs)
            ok, reason = led2.can_add(legs)
            if is_m:
                anchor = pick_anchor_leg(legs)
                ancs = player_atoms(anchor.get("player")) if anchor else set()
                role = "ANCHOR" if "michael mcgreevy" in ancs else "NONANCHOR"
                props = [
                    (leg.get("prop_type"), leg.get("line"))
                    for leg in legs
                    if fold_player(leg.get("player")) == "michael mcgreevy"
                ]
                print(
                    f"{'KEEP' if ok else 'DROP':4} role={role:9} "
                    f"{str(g.get('group_name'))[:28]:28} mcg={props} reason={reason or 'ok'}"
                )
            if ok:
                led2.register(legs)

    print("\n=== DROP REASON BREAKDOWN ===")
    led3 = SlateExposureLedger()
    reasons: Counter[str] = Counter()
    kept = dropped = 0
    for g in d.get("groups") or []:
        for t in g.get("tickets") or []:
            legs = [leg for leg in (t.get("legs") or []) if isinstance(leg, dict)]
            ok, reason = led3.can_add(legs)
            if ok:
                led3.register(legs)
                kept += 1
            else:
                dropped += 1
                reasons[(reason or "unknown").split(":")[0]] += 1
    print(f"kept={kept} dropped={dropped} reasons={dict(reasons)}")

    print("\n=== SYNTHETIC: Boyd Hits anchors A, Boyd Outs non-anchor on B ===")
    led4 = SlateExposureLedger()
    a = [
        {
            "player": "Matthew Boyd",
            "prop_type": "Hits Allowed",
            "direction": "OVER",
            "badge": "Platinum",
            "rank_score": 10,
        },
        {
            "player": "Fill A",
            "prop_type": "Points",
            "direction": "OVER",
            "badge": "Silver",
            "rank_score": 1,
        },
    ]
    b = [
        {
            "player": "Fill Star",
            "prop_type": "Rebounds",
            "direction": "OVER",
            "badge": "Diamond",
            "rank_score": 99,
        },
        {
            "player": "Matthew Boyd",
            "prop_type": "Pitching Outs",
            "direction": "OVER",
            "badge": "Silver",
            "rank_score": 1,
        },
    ]
    ok_a, r_a = led4.can_add(a)
    led4.register(a)
    ok_b, r_b = led4.can_add(b)
    print(f"ticket A Hits as anchor: {ok_a} {r_a or 'ok'}")
    print(f"ticket B Outs as non-anchor: {ok_b} {r_b or 'ok'}")
    print(f"ledger={led4.snapshot().get('matthew boyd')}")

    print("\n=== WHY 9/19 BOYD OUTS DIDN'T SURVIVE (if DROP as ANCHOR) ===")
    # Show rank/badge on each Boyd ticket so we see if he was always the anchor
    for g in d.get("groups") or []:
        for t in g.get("tickets") or []:
            legs = [leg for leg in (t.get("legs") or []) if isinstance(leg, dict)]
            if not any(fold_player(leg.get("player")) == "matthew boyd" for leg in legs):
                continue
            print(f"-- {g.get('group_name')} {t.get('ticket_id')}")
            for leg in legs:
                print(
                    f"   {leg.get('player'):22} {leg.get('prop_type'):20} "
                    f"badge={leg.get('badge') or leg.get('promo')!r:12} "
                    f"rank={leg.get('rank_score')} cover={leg.get('cover') or leg.get('abs_edge') or leg.get('edge')}"
                )
            anc = pick_anchor_leg(legs)
            print(f"   => pick_anchor={anc.get('player') if anc else None}")

    print("\n=== COUNTERFACTUALS ON 9/19 CARD ===")
    for label, kwargs in [
        ("default 1+1 same_prop", dict(max_anchor=1, max_nonanchor=1, block_same_prop=True)),
        ("1+1 same_prop OFF", dict(max_anchor=1, max_nonanchor=1, block_same_prop=False)),
        ("1+2 same_prop ON", dict(max_anchor=1, max_nonanchor=2, block_same_prop=True)),
        ("anchor1 non99 same_prop ON", dict(max_anchor=1, max_nonanchor=99, block_same_prop=True)),
        ("flat total only via high non", dict(max_anchor=1, max_nonanchor=1, block_same_prop=True)),
    ]:
        ledx = SlateExposureLedger(**kwargs)
        k = dr = 0
        boyd_n = 0
        mcg_n = 0
        for g in d.get("groups") or []:
            for t in g.get("tickets") or []:
                legs = [leg for leg in (t.get("legs") or []) if isinstance(leg, dict)]
                ok, _ = ledx.can_add(legs)
                if ok:
                    ledx.register(legs)
                    k += 1
                    for leg in legs:
                        pn = fold_player(leg.get("player"))
                        if pn == "matthew boyd":
                            boyd_n += 1
                        if pn == "michael mcgreevy":
                            mcg_n += 1
                else:
                    dr += 1
        print(
            f"{label:28} kept={k:2} dropped={dr:2} "
            f"boyd_legs={boyd_n} mcg_legs={mcg_n} "
            f"boyd_snap={ledx.snapshot().get('matthew boyd')}"
        )


if __name__ == "__main__":
    main()
