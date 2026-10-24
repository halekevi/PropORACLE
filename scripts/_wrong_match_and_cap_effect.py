"""Board-opp vs attached-match opp; count slips removed by GW 0.62 cap."""
from __future__ import annotations

import json
import sys
import urllib.request
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
MAIN = ROOT.parent / "PropORACLE_main_cp"
sys.path[:0] = [str(ROOT), str(ROOT / "scripts")]

from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import (  # noqa: E402
    P_GOBLIN_L5EQ5,
    P_TENNIS_GAMES_WON_GOBLIN_LIVE,
    goblin_70_eligible,
    goblin_ticket_p,
)
from utils.ticket_ev_build import attach_leg_p, build_ev_slips  # noqa: E402

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}


def _p(*a):
    print(*a, flush=True)


def _norm(s: object) -> str:
    return " ".join(str(s or "").casefold().split())


def _last(s: object) -> str:
    parts = _norm(s).split()
    return parts[-1] if parts else ""


def _opp_match(a: str, b: str) -> bool:
    na, nb = _norm(a), _norm(b)
    if not na or not nb:
        return False
    if na == nb:
        return True
    la, lb = _last(a), _last(b)
    if la and lb and la == lb and len(la) >= 4:
        return True
    # either contains the other (José vs Jose, accents already folded away)
    return na in nb or nb in na


def _espn(url: str):
    req = urllib.request.Request(url, headers=HEADERS)
    with urllib.request.urlopen(req, timeout=25) as resp:
        return json.loads(resp.read().decode())


def _iter_comps(payload):
    for ev in payload.get("events") or []:
        groupings = ev.get("groupings") or []
        if groupings:
            for grp in groupings:
                for comp in grp.get("competitions") or []:
                    yield ev, comp
        else:
            for comp in ev.get("competitions") or []:
                yield ev, comp


def _board_matches(ymd: str) -> dict[str, list[dict]]:
    out: dict[str, list[dict]] = defaultdict(list)
    for tour in ("atp", "wta"):
        url = (
            f"https://site.api.espn.com/apis/site/v2/sports/tennis/{tour}/scoreboard"
            f"?dates={ymd.replace('-', '')}"
        )
        try:
            payload = _espn(url)
        except Exception:
            continue
        for _ev, comp in _iter_comps(payload):
            comps = list(comp.get("competitors") or [])
            if len(comps) < 2:
                continue
            status = str(((comp.get("status") or {}).get("type") or {}).get("name") or "")
            for i, c in enumerate(comps):
                name = str(((c.get("athlete") or {}).get("displayName") or "")).strip()
                if not name:
                    continue
                lines = c.get("linescores") or []
                games = sum(float((x or {}).get("value") or 0) for x in lines)
                other = comps[1 - i]
                ols = other.get("linescores") or []
                opp_games = sum(float((x or {}).get("value") or 0) for x in ols)
                opp = str(((other.get("athlete") or {}).get("displayName") or ""))
                out[_norm(name)].append(
                    {
                        "games": games,
                        "match_total": games + opp_games,
                        "opp": opp,
                        "status": status,
                        "ymd": ymd,
                        "retired": any(
                            tok in status.upper()
                            for tok in ("RET", "ABANDON", "WALKOVER", "W/O")
                        ),
                    }
                )
    return out


def _load_board_opp_map(day: str) -> dict[str, str]:
    """player_norm -> opponent for one slate day (step8 preferred, else step1)."""
    root = MAIN / "outputs" / day
    if not root.is_dir():
        root = ROOT / "outputs" / day
    files = (
        list(root.glob("**/step8*tennis*clean*.parquet"))
        + list(root.glob("**/step8*tennis*clean*.xlsx"))
        + list(root.glob("tennis/step1_tennis_props.csv"))
        + list(root.glob("step1_tennis_props.csv"))
    )
    out: dict[str, str] = {}
    for path in files:
        try:
            if path.suffix == ".parquet":
                df = pd.read_parquet(path)
            elif path.suffix in {".xlsx", ".xls"}:
                df = pd.read_excel(path)
            else:
                df = pd.read_csv(path, dtype=str, encoding_errors="replace")
        except Exception:
            continue
        cols = {str(c).lower(): c for c in df.columns}
        pc = cols.get("player") or cols.get("player_name")
        if not pc:
            continue
        opp_c = (
            cols.get("opp")
            or cols.get("opponent")
            or cols.get("opp_name")
            or cols.get("opp_team")
        )
        mc = cols.get("matchup")
        for _, r in df.iterrows():
            player = str(r.get(pc) or "").strip()
            if not player:
                continue
            key = _norm(player)
            if key in out:
                continue
            opp = ""
            if opp_c:
                raw = str(r.get(opp_c) or "").strip()
                if raw and raw.lower() != "nan":
                    opp = raw
            if not opp and mc:
                val = str(r.get(mc) or "")
                for sep in (" vs ", " VS ", " @ ", " v "):
                    if sep in val:
                        a, b = val.split(sep, 1)
                        if _norm(a) == key:
                            opp = b.strip()
                        elif _norm(b) == key:
                            opp = a.strip()
                        break
            if opp:
                out[key] = opp
        if out and "step8" in path.name:
            break
    return out


def wrong_match_audit() -> dict:
    _p("=== WRONG-MATCH: board opp vs attached ESPN opp ===")
    # Load gw+mtg pairs
    pairs: dict[tuple[str, str], dict] = {}
    seen = set()
    for root in (MAIN / "outputs", ROOT / "outputs"):
        if not root.is_dir():
            continue
        for path in sorted(root.glob("**/actuals_tennis_*.csv")):
            if path.name in seen:
                continue
            seen.add(path.name)
            day = path.stem.replace("actuals_tennis_", "")[:10]
            try:
                df = pd.read_csv(path, dtype=str)
            except Exception:
                continue
            if df.empty or "player" not in df.columns:
                continue
            cols = {str(c).lower(): c for c in df.columns}
            pc = cols.get("player")
            prop_c = cols.get("prop_type") or cols.get("prop")
            act_c = cols.get("actual")
            if not (pc and prop_c and act_c):
                continue
            for _, r in df.iterrows():
                prop = canon_prop("TENNIS", str(r.get(prop_c) or ""))
                if prop not in {"games_won", "match_total_games"}:
                    continue
                try:
                    actual = float(r.get(act_c))
                except (TypeError, ValueError):
                    continue
                player = str(r.get(pc) or "").strip()
                key = (_norm(player), day)
                rec = pairs.setdefault(key, {"player": player, "day": day})
                rec[prop] = actual

    rows = [r for r in pairs.values() if "games_won" in r]
    _p(f"player-days with games_won: {len(rows)}")

    days = sorted({r["day"] for r in rows})
    pull = set()
    for d in days:
        dd = date.fromisoformat(d)
        for off in (-1, 0, 1):
            pull.add((dd + timedelta(days=off)).isoformat())
    boards: dict[str, dict[str, list]] = {}
    for i, d in enumerate(sorted(pull)):
        boards[d] = _board_matches(d)
        if (i + 1) % 15 == 0 or i == len(pull) - 1:
            _p(f"  espn {i+1}/{len(pull)}")

    # Preload board opponents once per day (±1 for day-ahead).
    day_maps: dict[str, dict[str, str]] = {}
    need_days = set()
    for d in days:
        dd = date.fromisoformat(d)
        for off in (-1, 0, 1):
            need_days.add((dd + timedelta(days=off)).isoformat())
    for i, d in enumerate(sorted(need_days)):
        day_maps[d] = _load_board_opp_map(d)
        if (i + 1) % 20 == 0 or i == len(need_days) - 1:
            _p(f"  board-opp maps {i+1}/{len(need_days)}")

    def board_opp(day: str, player: str) -> str:
        key = _norm(player)
        for d in (
            day,
            (date.fromisoformat(day) - timedelta(days=1)).isoformat(),
            (date.fromisoformat(day) + timedelta(days=1)).isoformat(),
        ):
            found = (day_maps.get(d) or {}).get(key, "")
            if found:
                return found
        return ""

    n_check = n_ok = n_wrong = n_no_board = n_no_espn = n_no_attach = 0
    samples = []
    for r in rows:
        board = board_opp(r["day"], r["player"])
        if not board:
            n_no_board += 1
            continue
        key = _norm(r["player"])
        cands = []
        for d in (
            r["day"],
            (date.fromisoformat(r["day"]) + timedelta(days=1)).isoformat(),
            (date.fromisoformat(r["day"]) - timedelta(days=1)).isoformat(),
        ):
            cands.extend(boards.get(d, {}).get(key) or [])
        if not cands:
            n_no_espn += 1
            continue
        gw = float(r["games_won"])
        mtg = r.get("match_total_games")
        # Attach = ESPN match whose linescore gw matches stored actual
        # Prefer mtg agree when available, then non-retired
        scored = []
        for c in cands:
            if abs(c["games"] - gw) > 0.01:
                continue
            mtg_ok = (
                mtg is None
                or abs(c["match_total"] - float(mtg)) < 0.01
            )
            scored.append((0 if mtg_ok else 1, 1 if c["retired"] else 0, c))
        if not scored:
            n_no_attach += 1
            continue
        scored.sort(key=lambda t: (t[0], t[1]))
        attached = scored[0][2]
        n_check += 1
        if _opp_match(board, attached["opp"]):
            n_ok += 1
        else:
            n_wrong += 1
            if len(samples) < 20:
                samples.append(
                    {
                        "day": r["day"],
                        "player": r["player"],
                        "board_opp": board,
                        "attached_opp": attached["opp"],
                        "gw": gw,
                        "mtg": mtg,
                        "status": attached["status"],
                    }
                )

    _p(f"checked (board opp + attached match): {n_check}")
    _p(f"  opp agree: {n_ok}")
    _p(f"  opp disagree (wrong-match): {n_wrong}")
    _p(
        f"  wrong-match rate: {n_wrong / n_check if n_check else None:.2%}"
        if n_check
        else "  wrong-match rate: n/a"
    )
    _p(f"skip no_board_opp={n_no_board} no_espn={n_no_espn} no_gw_attach={n_no_attach}")
    _p("wrong-match samples:")
    for s in samples:
        _p(
            f"  {s['day']} {s['player']} board={s['board_opp']!r} "
            f"attached={s['attached_opp']!r} gw={s['gw']} status={s['status']}"
        )
    return {
        "checked": n_check,
        "agree": n_ok,
        "wrong": n_wrong,
        "no_board": n_no_board,
        "no_espn": n_no_espn,
        "no_attach": n_no_attach,
        "samples": samples,
    }


def _has_gw(legs: list[dict]) -> bool:
    for leg in legs:
        sport = str(leg.get("sport") or "").upper()
        prop = canon_prop("TENNIS", str(leg.get("prop") or leg.get("prop_type") or ""))
        if sport.startswith("TENNIS") and prop == "games_won":
            return True
    return False


def cap_effect() -> dict:
    """Rebuild EV card for recent slate dates at old vs live GW p."""
    _p("\n=== CAP EFFECT: slips with Games Won at 0.763 vs 0.62 ===")
    import build_goblin70_tickets as g70
    from utils.ticket_keep_registry import rule_for
    from utils.category_hit_rate import load_category_hr_index

    # Prefer dates that had GW on archived cards
    dates = []
    from report_gated_construction_efficiency import discover_g70_archives, payload_from_g70

    for date_s, path in discover_g70_archives(start=None, end=None):
        payload = payload_from_g70(path, date_s)
        tickets = list(payload.get("tickets") or [])
        if not tickets:
            for grp in payload.get("groups") or []:
                tickets.extend(grp.get("tickets") or [])
        if any(_has_gw(t.get("legs") or []) for t in tickets if isinstance(t, dict)):
            dates.append(date_s)
    dates = sorted(set(dates))
    _p(f"archive dates with GW legs: {len(dates)} {dates}")

    hr_index = load_category_hr_index()
    total_old = total_new = removed = 0
    per_day = []
    for date_s in dates:
        try:
            board = g70.load_today_board(date_s, skip_golf=True)
        except Exception as exc:
            _p(f"  {date_s} board load fail: {exc}")
            continue
        gob = [
            dict(r)
            for r in board
            if goblin_70_eligible(r) and g70.fold_name(r.get("player")) not in g70.SKIP_PLAYERS
        ]
        if not gob:
            continue

        def _recipe(r: dict) -> str:
            rule = rule_for(r.get("sport"))
            mode = rule[0] if rule else "off"
            return f"{r.get('sport')}:{mode}"

        def _price(pool: list[dict], *, live_gw: bool) -> list[dict]:
            out = []
            for r in pool:
                rec = dict(r)
                attach_leg_p(rec, recipe=_recipe(rec), index=hr_index)
                floor = P_GOBLIN_L5EQ5
                try:
                    cur = float(rec["p"]) if rec.get("p") is not None else None
                except (TypeError, ValueError):
                    cur = None
                if cur is None or cur < floor:
                    rec["p"] = floor
                prop = canon_prop("TENNIS", str(rec.get("prop") or ""))
                sport = str(rec.get("sport") or "").upper()
                if live_gw and sport.startswith("TENNIS") and prop == "games_won":
                    rec["p"] = P_TENNIS_GAMES_WON_GOBLIN_LIVE
                elif (not live_gw) and sport.startswith("TENNIS") and prop == "games_won":
                    # force cell-ish prior (old behavior used L5eq5 / category)
                    if cur is None or cur < P_GOBLIN_L5EQ5:
                        rec["p"] = P_GOBLIN_L5EQ5
                    # leave category HR if higher
                out.append(rec)
            return out

        old_pool = _price(gob, live_gw=False)
        new_pool = _price(gob, live_gw=True)
        old_slips = build_ev_slips(old_pool, taken=set(), fold=g70.fold_name)
        new_slips = build_ev_slips(new_pool, taken=set(), fold=g70.fold_name)
        old_gw = sum(1 for s in old_slips if _has_gw(s["legs"]))
        new_gw = sum(1 for s in new_slips if _has_gw(s["legs"]))
        old_n, new_n = len(old_slips), len(new_slips)
        drop = old_n - new_n
        drop_gw = old_gw - new_gw
        total_old += old_n
        total_new += new_n
        removed += max(0, drop)
        per_day.append(
            {
                "date": date_s,
                "old": old_n,
                "new": new_n,
                "old_gw_slips": old_gw,
                "new_gw_slips": new_gw,
                "drop": drop,
                "drop_gw_slips": drop_gw,
                "pool": len(gob),
                "gw_legs_in_pool": sum(
                    1
                    for r in gob
                    if str(r.get("sport") or "").upper().startswith("TENNIS")
                    and canon_prop("TENNIS", str(r.get("prop") or "")) == "games_won"
                ),
            }
        )
        _p(
            f"  {date_s}: slips {old_n}->{new_n} (drop {drop}); "
            f"GW-containing {old_gw}->{new_gw}; pool={len(gob)} gw_legs={per_day[-1]['gw_legs_in_pool']}"
        )

    _p(f"TOTAL slips {total_old}->{total_new} removed {total_old - total_new}")
    _p(f"sum per-day drop {removed}")
    return {"per_day": per_day, "old": total_old, "new": total_new}


def main() -> None:
    wrong = wrong_match_audit()
    cap = cap_effect()
    out = ROOT / "data" / "reports" / "wrong_match_and_cap_20261007.json"
    out.write_text(
        json.dumps({"wrong_match": wrong, "cap_effect": cap}, indent=2, default=str),
        encoding="utf-8",
    )
    _p(f"\nWROTE {out}")


if __name__ == "__main__":
    main()
