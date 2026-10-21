"""Tonight's G70 clearing faces + catalog hit rate for each prop."""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(r"H:\PropORACLE")
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "scripts"))

from all_ranked_prop_hit_rates import graded_paths, l10_dir, l5_dir, num  # noqa: E402
from build_goblin70_tickets import attach_wnba_ticket_priors  # noqa: E402
from utils.prop_norm import canon_prop, display_prop  # noqa: E402
from utils.ticket_70_pool import goblin_70_eligible, standard_ticket_eligible  # noqa: E402

POOL = Path(r"H:\PropORACLE_main_cp\data\reports\goblin70_tickets_2026-09-23.json")


def _pick(r: dict) -> str:
    p = str(r.get("pick_type") or "").strip().lower()
    if "goblin" in p:
        return "Goblin"
    if "standard" in p:
        return "Standard"
    return ""


def _side(r: dict) -> str:
    s = str(r.get("direction") or r.get("over_under") or r.get("side") or "").strip().upper()
    if s in {"O", "OVER", "HIGHER"}:
        return "OVER"
    if s in {"U", "UNDER", "LOWER"}:
        return "UNDER"
    return s


def main() -> None:
    live = json.loads(POOL.read_text(encoding="utf-8"))
    faces = live.get("goblin_pool") or []
    std = live.get("standard_pool") or []

    catalog: dict[tuple[str, str, str, str], list[int]] = defaultdict(list)
    seen = set()
    for path in graded_paths():
        date = path.stem.replace("graded_props_", "")[:10]
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        props = raw.get("props") or raw.get("rows") or []
        if not isinstance(props, list):
            continue
        for r in props:
            if not isinstance(r, dict):
                continue
            res = str(r.get("result") or "").strip().upper()
            if res not in {"HIT", "MISS"}:
                continue
            sport = str(r.get("sport") or "").strip().upper()
            if sport in {"WNBA1Q", "WNBA1H", "NBA1Q", "NBA1H"}:
                pass
            pick = _pick(r)
            side = _side(r)
            if not pick or side not in {"OVER", "UNDER"}:
                continue
            player = str(r.get("player") or "").strip()
            prop_raw = str(r.get("prop") or r.get("prop_type") or "")
            prop = canon_prop(sport, prop_raw)
            line = num(r.get("line"))
            key = (date, sport, player, prop, pick, side, line)
            if key in seen:
                continue
            seen.add(key)
            rec = {
                "sport": sport,
                "player": player,
                "prop": prop_raw,
                "side": side,
                "line": line,
                "pick_type": pick,
                "l5_over": l5_dir(r, "OVER"),
                "l5_under": l5_dir(r, "UNDER"),
                "l10_over": l10_dir(r, "OVER"),
                "l10_under": l10_dir(r, "UNDER"),
                "cover": num(r.get("edge") or r.get("cover")),
                "def": r.get("def_tier") or r.get("def"),
                "usage_tier": r.get("usage_tier"),
                "minutes_tier": r.get("minutes_tier"),
                "prop_tier": r.get("tier") or r.get("prop_tier"),
                "opp_team": r.get("opp_team"),
                "team": r.get("team"),
            }
            if sport == "WNBA":
                attach_wnba_ticket_priors(rec, date)
            ok = goblin_70_eligible(rec) if pick == "Goblin" else standard_ticket_eligible(rec)
            if not ok:
                continue
            catalog[(sport, pick, side, prop)].append(1 if res == "HIT" else 0)

    def rate(sport: str, pick: str, side: str, prop_label: str) -> str:
        prop = canon_prop(sport, prop_label)
        hits = catalog.get((sport.upper(), pick, side, prop))
        if not hits:
            return "n=0 (no G70 catalog)"
        h = sum(hits)
        n = len(hits)
        return f"{100.0 * h / n:.1f}% ({h}/{n})"

    print(f"slate {live.get('date')} built {live.get('built_at')}")
    print(f"G70 faces {len(faces)}  Standard {len(std)}")
    print()
    print(
        f"{'#':>2}  {'sport':7} {'player':22} {'prop':22} {'line':6} "
        f"{'L5':>3} {'L10':>3} {'badge':9} {'prop HR':22}"
    )
    for i, r in enumerate(faces, 1):
        sport = str(r.get("sport") or "")
        pick = str(r.get("pick_type") or "Goblin")
        side = str(r.get("side") or "OVER")
        prop = str(r.get("prop") or "")
        print(
            f"{i:2}  {sport:7} {str(r.get('player') or '')[:22]:22} "
            f"{prop[:22]:22} {r.get('line')!s:6} "
            f"{r.get('l5')!s:>3} {r.get('l10')!s:>3} "
            f"{str(r.get('badge') or ''):9} {rate(sport, pick, side, prop)}"
        )

    print()
    print("catalog G70 hit rate by prop (live faces only):")
    seen_props = []
    for r in faces:
        key = (
            str(r.get("sport") or ""),
            str(r.get("pick_type") or "Goblin"),
            str(r.get("side") or "OVER"),
            str(r.get("prop") or ""),
        )
        if key in seen_props:
            continue
        seen_props.append(key)
        sport, pick, side, prop = key
        n_live = sum(
            1
            for x in faces
            if (x.get("sport"), x.get("pick_type"), x.get("side"), x.get("prop"))
            == (sport, pick, side, prop)
        )
        print(f"  {n_live} live  {sport:7} {pick} {side:5} {prop:22}  {rate(sport, pick, side, prop)}")


if __name__ == "__main__":
    main()
