#!/usr/bin/env python3
"""Build Goblin-70 tickets and publish the playable card to /tickets.

Main card (app): Goblin OVER with L5=5 + L10>=8 + directional D (tennis keep
gates: Games Won Standard-Goblin >=4, Total Games L5>=4 and L10>=8, no D; golf L5=5+L10>=8,
no opponent D). Cover floor, no Demons, no shadow, no hitter Ks. Standard
Over/Under that clear the same gate can share a slip with Goblins. Tennis
Standard UNDER Aces/DF faded; Games Won OVER keep only.

The playable card prices 2- and 3-leg slips at the N-correct payout for
that composition. Each round takes the best-EV group that clears its
composition test. The dollar-for-dollar test is stored on every slip.
All-Goblin slips clear on an all-hit payout of at least 1.4x; that test
is recorded and does not drop them. Mixed and all-Standard slips must
clear it at their own payout. One player-prop once. The same
player can add a second prop and stops at 2. A sport fills its own slips
first; legs that cannot fill mix across sports. Flat $20.

NFLP stays on its own playing-time track. N-correct / To Win only.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import subprocess
import sys
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

_REPO = Path(__file__).resolve().parents[1]
if str(_REPO / "scripts") not in sys.path:
    sys.path.insert(0, str(_REPO / "scripts"))
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import rank_best_props_today as R  # noqa: E402
from utils.n_correct_payout import PAY_FALLBACK as PAY, resolve_n_correct  # noqa: E402
from utils.postseason_slate import (  # noqa: E402
    apply_postseason_price,
    counts_toward_gated_book,
    is_postseason_ticket_leg,
    mark_placed,
    postseason_daily_cap,
    shortlist_postseason,
    POSTSEASON_TRACK,
)
from utils.ticket_ev_build import attach_leg_p, build_sport_then_mix  # noqa: E402
from utils.ticket_keep_registry import rule_for  # noqa: E402
from utils.ui_live_json import write_paths as live_json_write_paths  # noqa: E402
from utils.prop_norm import canon_prop  # noqa: E402
from utils.ticket_70_pool import (  # noqa: E402
    P_TENNIS_GAMES_WON_GOBLIN_LIVE,
    P_TENNIS_MATCH_TOTAL_GOBLIN,
    _is_tennis,
    _prop,
    directional_l5,
    goblin_70_eligible,
    goblin_sort_key,
    goblin_ticket_p,
    is_pitcher_prop,
    keep_hardest_goblin_over_faces,
    nflp_std_over_eligible,
    nflp_ticket_eligible,
    nflp_ticket_p,
    premium_goblin_eligible,
    premium_goblin_sort_key,
    standard_flex_kind,
    standard_sort_key,
    standard_ticket_eligible,
    standard_ticket_p,
    standard_yolo_kind,
    yolo_ticket_track,
)
from utils.wnba_pra_off_observe import log_board as log_wnba_pra_off_observe  # noqa: E402
from utils.wnba_threes_vol_observe import log_board as log_wnba_threes_vol_observe  # noqa: E402
from utils.wnba_std_under_watch import log_board as log_wnba_std_under_watch  # noqa: E402
from utils.wnba_ticket_cuts import attach_wnba_ticket_priors  # noqa: E402

OUT_DIR = _REPO / "data" / "reports"
ET = ZoneInfo("America/New_York")
G70_CORE_SPORTS = ("MLB", "WNBA", "Soccer", "Tennis")
_GOLF_SKIP_ENV = ("1", "true", "yes", "on")


def golf_xlsx_candidates(root: Path, date: str) -> list[Path]:
    return [
        root / "outputs" / date / "golf" / "step8_golf_direction_clean.xlsx",
        root / "outputs" / date / "golf" / f"step8_golf_direction_clean_{date}.xlsx",
        root / "Sports" / "Golf" / "outputs" / "step8_golf_direction_clean.xlsx",
    ]


def golf_xlsx_missing(root: Path, date: str) -> bool:
    """True when the dated golf xlsx step8 (G70 golf input) is not on disk."""
    return not any(p.is_file() for p in golf_xlsx_candidates(root, date))


def skip_golf_g70(
    root: Path | None,
    date: str,
    *,
    flag: bool = False,
    env: dict[str, str] | None = None,
) -> bool:
    """Golf xlsx miss is a sport skip, not a full G70 halt."""
    if flag:
        return True
    src = env if env is not None else os.environ
    if str(src.get("PROPORACLE_SKIP_GOLF_G70") or "").strip().lower() in _GOLF_SKIP_ENV:
        return True
    if root is None:
        return False
    return golf_xlsx_missing(root, date)


def _sport_label(raw: object) -> str:
    u = str(raw or "").strip().upper()
    return {
        "SOCCER": "Soccer",
        "SOC": "Soccer",
        "TENNIS": "Tennis",
        "WNBA": "WNBA",
        "MLB": "MLB",
        "NFL": "NFL",
        "CFB": "CFB",
        "GOLF": "Golf",
    }.get(u, str(raw or "").strip() or u)


def remaining_g70_sports(rows: list[dict]) -> list[str]:
    """Sports still in the G70 *core* pool after golf is dropped.

    NFL/CFB/NBA can sit on the same step8 board; they are not part of this
    warning. Their own gates (NFL Goblin held, NFLP league check, etc.)
    still apply inside ``build()`` and are independent of ``--skip-golf``.
    """
    present: list[str] = []
    seen: set[str] = set()
    labels = [_sport_label(r.get("sport")) for r in rows]
    for want in G70_CORE_SPORTS:
        if any(lab == want for lab in labels) and want not in seen:
            present.append(want)
            seen.add(want)
    return present


def warn_missing_football_step8(root: Path, date: str) -> None:
    """Surface step1-without-step8 so football pipeline failure ≠ empty slate."""
    reason_fn = getattr(R, "step8_empty_reason", None)
    if not callable(reason_fn):
        return
    for sport, folder, fname in (
        ("NFL", "nfl", "step8_nfl_direction_clean.xlsx"),
        ("CFB", "cfb", "step8_cfb_direction_clean.xlsx"),
    ):
        msg = str(reason_fn(root, date, sport, folder, fname) or "")
        if "WARN:" in msg:
            print(msg.strip())


def golf_skip_warning(rows: list[dict]) -> str:
    keep = remaining_g70_sports(rows) or list(G70_CORE_SPORTS)
    return (
        "WARNING: Golf step8 xlsx missing — golf excluded from G70 pool, "
        f"continuing with {len(keep)} remaining sports: {', '.join(keep)}"
    )


def g70_json_valid(path: Path) -> tuple[bool, str]:
    if not path.is_file():
        return False, "missing"
    try:
        if path.stat().st_size <= 0:
            return False, "empty"
        json.loads(path.read_text(encoding="utf-8"))
    except OSError as exc:
        return False, str(exc)
    except json.JSONDecodeError as exc:
        return False, f"invalid JSON ({exc})"
    return True, "ok"


def bump_g70_latest_pointer(
    dated: Path,
    date: str,
    *,
    peer_roots: list[Path] | None = None,
) -> list[Path]:
    """Copy a validated dated G70 JSON onto latest.json (local + main_cp).

    Independent of run_daily PipelineFailed / D-G70 skip flags.
    """
    ok, why = g70_json_valid(dated)
    if not ok:
        print(
            f"WARNING: G70 dated JSON invalid ({why}) — latest pointer not updated "
            f"path={dated}"
        )
        return []
    text = dated.read_text(encoding="utf-8")
    written: list[Path] = []
    local = dated.parent / "goblin70_tickets_latest.json"
    try:
        local.write_text(text, encoding="utf-8")
        written.append(local)
        print(f"LAST_G70_REBUILD ok=true -> latest pointer updated to {date} ({local})")
    except OSError as exc:
        print(f"WARNING: could not bump G70 latest pointer at {local}: {exc}")
    roots = peer_roots
    if roots is None:
        roots = []
        peer = _REPO.parent / "PropORACLE_main_cp"
        if peer.is_dir() and peer.resolve() != _REPO.resolve():
            roots.append(peer)
    for root in roots:
        dest = Path(root) / "data" / "reports" / "goblin70_tickets_latest.json"
        try:
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_text(text, encoding="utf-8")
            written.append(dest)
            print(f"LAST_G70_REBUILD ok=true -> latest pointer updated to {date} ({dest})")
        except OSError as exc:
            print(f"WARNING: could not bump G70 latest pointer at {dest}: {exc}")
    return written


def fold_name(s: object) -> str:
    t = unicodedata.normalize("NFKD", str(s or ""))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = t.casefold()
    t = re.sub(r"\b(jr|sr|iii|ii|iv)\b\.?", " ", t)
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return " ".join(t.split())


def tennis_match_key(mu: object) -> str:
    s = str(mu or "").split("(")[0]
    parts = re.split(r"\s+vs\.?\s+", s, flags=re.I)
    if len(parts) == 2:
        a, b = fold_name(parts[0]), fold_name(parts[1])
        if a and b:
            return " | ".join(sorted((a, b)))
    return fold_name(s)


def _attach_tennis_espn_event_ids(board: list[dict], date: str) -> None:
    """Stamp espn_event_id / espn_competition_id from today's live scoreboard.

    Historical ESPN ``?dates=`` boards go empty within days. Capturing the id at
    build time lets grading look up the match later without a name search.
    """
    tennis = [
        r
        for r in board
        if str(r.get("sport") or "").upper().startswith("TENNIS")
    ]
    if not tennis:
        return
    try:
        import urllib.request

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}

        def _norm(s: object) -> str:
            return " ".join(str(s or "").casefold().split())

        by_player: dict[str, tuple[str, str]] = {}
        ymd = str(date).replace("-", "")
        for tour in ("atp", "wta"):
            url = (
                f"https://site.api.espn.com/apis/site/v2/sports/tennis/{tour}/scoreboard"
                f"?dates={ymd}"
            )
            try:
                req = urllib.request.Request(url, headers=headers)
                with urllib.request.urlopen(req, timeout=20) as resp:
                    payload = json.loads(resp.read().decode())
            except Exception:
                continue
            for ev in payload.get("events") or []:
                eid = str(ev.get("id") or "").strip()
                groupings = ev.get("groupings") or []
                comps_iter = []
                if groupings:
                    for grp in groupings:
                        comps_iter.extend(grp.get("competitions") or [])
                else:
                    comps_iter = list(ev.get("competitions") or [])
                for comp in comps_iter:
                    cid = str(comp.get("id") or "").strip()
                    for c in comp.get("competitors") or []:
                        name = _norm(((c.get("athlete") or {}).get("displayName") or ""))
                        if name and eid:
                            by_player[name] = (eid, cid)
        if not by_player:
            return
        hit = 0
        for r in tennis:
            key = _norm(r.get("player"))
            ids = by_player.get(key)
            if not ids:
                continue
            r["espn_event_id"] = ids[0]
            if ids[1]:
                r["espn_competition_id"] = ids[1]
            hit += 1
        if hit:
            print(f"tennis espn event ids attached: {hit}/{len(tennis)}")
    except Exception as exc:
        print(f"WARNING: tennis espn event id attach failed: {exc}")


def load_today_board(date: str, *, skip_golf: bool = False) -> list[dict]:
    candidates = [_REPO]
    sibling = _REPO.parent / "PropORACLE_main_cp"
    if sibling.is_dir() and sibling.resolve() != _REPO.resolve():
        candidates.append(sibling)
    choose = getattr(R, "_choose_step8_root", None)
    root = choose(candidates, date) if callable(choose) else None
    if root is None:
        for cand in candidates:
            out = cand / "outputs" / date
            if (out / "wnba").is_dir() or (out / "mlb").is_dir():
                root = cand
                break
    if root is None:
        return []
    warn_missing_football_step8(root, date)
    drop_golf = skip_golf_g70(root, date, flag=skip_golf)
    rows: list[dict] = []
    for sport, folder, fname in R.SPORTS:
        df = R.load_sport(root, date, sport, folder, fname)
        if df.empty:
            continue
        rows.extend(R.recs(df))
    for loader_name in (
        "load_nfl",
        "load_nhl",
        "load_cfb",
        "load_cfb1h",
        "load_golf",
        "load_cbb",
        "load_wcbb",
        "load_nba",
        "load_nba1q",
        "load_nba1h",
        "load_wnba1q",
        "load_wnba1h",
    ):
        if drop_golf and loader_name == "load_golf":
            continue
        if loader_name == "load_nba1q":
            from utils.ticket_sport_holds import NBA1Q_HOLD_REASON, nba1q_tickets_held

            if nba1q_tickets_held():
                print(NBA1Q_HOLD_REASON, flush=True)
                continue
        loader = getattr(R, loader_name, None)
        if not callable(loader):
            continue
        extra = loader(root, date)
        if extra is not None and not extra.empty:
            rows.extend(R.recs(extra))
    if drop_golf:
        rows = [r for r in rows if str(r.get("sport") or "").upper() != "GOLF"]
        print(golf_skip_warning(rows), flush=True)
    from utils.ticket_sport_holds import NBA1Q_HOLD_REASON, nba1q_tickets_held

    if nba1q_tickets_held():
        n_q = sum(1 for r in rows if str(r.get("sport") or "").upper() == "NBA1Q")
        if n_q:
            rows = [r for r in rows if str(r.get("sport") or "").upper() != "NBA1Q"]
            print(f"{NBA1Q_HOLD_REASON} dropped {n_q} board row(s)", flush=True)
    return rows

# PAY lives in utils.n_correct_payout. Resolve order: CDP scrape (matching
# Goblin-Δ) → mix_by_delta from those scrapes → slip pin → fallback.

SKIP_PLAYERS = frozenset({"jacob fearnley"})

SPORT_PURE = (
    ("WNBA", 3, "Power", 3),
    ("MLB", 3, "Power", 3),
    ("Tennis", 3, "Power", 2),
    ("Soccer", 3, "Power", 2),
    ("NBA", 3, "Power", 2),
    ("NHL", 3, "Power", 2),
    ("CFB", 3, "Power", 2),
    ("CFB1H", 3, "Power", 2),
    ("NFL", 3, "Power", 2),
    ("CBB", 3, "Power", 2),
    ("GOLF", 3, "Power", 2),
    ("WNBA1Q", 3, "Power", 2),
    ("WNBA1H", 3, "Power", 2),
    ("NBA1Q", 3, "Power", 2),
    ("NBA1H", 3, "Power", 2),
    ("WCBB", 3, "Power", 2),
)
# Period sports cannot share WNBA[:3]/NBA[:3] ticket ids.
SPORT_TID_PREFIX = {
    "WNBA1Q": "W1Q",
    "WNBA1H": "W1H",
    "NBA1Q": "N1Q",
    "NBA1H": "N1H",
    "CFB1H": "C1H",
    "WCBB": "WCB",
    "GOLF": "GLF",
    "TENNIS": "TEN",
    "SOCCER": "SOC",
}

MAIN_CARD = (
    ("P3-1", 3, "Power", "goblin"),
    ("P3-2", 3, "Power", "goblin"),
    ("P3-3", 3, "Power", "goblin"),
    ("P3-4", 3, "Power", "goblin"),
    ("P3-5", 3, "Power", "goblin"),
    ("F4-1", 4, "Flex", "goblin"),
    ("F4-2", 4, "Flex", "goblin"),
    ("F4-3", 4, "Flex", "goblin"),
    ("F4-4", 4, "Flex", "goblin"),
)
# Long-shot sleeve only. Prefer 6, fall back to 5. Max two slips.
YOLO_MAX_TICKETS = 3
YOLO_PREF_LEGS = (6, 5, 4)
# All-Standard YOLO: gate-cleared Standards only; skip the day if < 3 clear.
STANDARD_YOLO_MAX_TICKETS = 1
STANDARD_YOLO_PREF_LEGS = (6, 5, 4, 3)
STANDARD_YOLO_MIN_LEGS = 3

WEB_SPORT = {
    "TENNIS": "Tennis",
    "SOCCER": "Soccer",
    "WNBA": "WNBA",
    "MLB": "MLB",
    "NFL": "NFL",
    "CFB": "CFB",
    "CBB": "CBB",
    "GOLF": "Golf",
    "PGA": "Golf",
    "NBA": "NBA",
    "NHL": "NHL",
    "NFL": "NFL",
    "CFB": "CFB",
    "CBB": "CBB",
    "WCBB": "WCBB",
    "WNBA1Q": "WNBA1Q",
    "WNBA1H": "WNBA1H",
    "NBA1Q": "NBA1Q",
    "NBA1H": "NBA1H",
}

# runtime (disk) + templates (GitHub raw) + data snapshot. Not docs/ or mobile/www.
WEB_JSON_PATHS = tuple(
    live_json_write_paths("tickets_latest.json", _REPO, include_data_snapshot=True)
)


def unique_by_player(rows: list[dict], key_fn) -> list[dict]:
    ordered = sorted(rows, key=key_fn)
    seen: set[str] = set()
    out: list[dict] = []
    for r in ordered:
        pn = fold_name(r.get("player"))
        if not pn or pn in seen:
            continue
        seen.add(pn)
        out.append(r)
    return out


def player_prop_key(r: dict) -> str:
    """Same player, different prop, is a different leg."""
    player = fold_name(r.get("player"))
    prop = canon_prop(str(r.get("sport") or ""), str(r.get("prop") or ""))
    if not player or not prop:
        return ""
    return f"{player}|{prop}"


def unique_by_player_prop(rows: list[dict], key_fn) -> list[dict]:
    ordered = sorted(rows, key=key_fn)
    seen: set[str] = set()
    out: list[dict] = []
    for r in ordered:
        key = player_prop_key(r)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


def matchup_key(r: dict) -> str:
    mu = str(r.get("matchup") or "").strip()
    sport = str(r.get("sport") or "").upper()
    if sport in {"WNBA", "MLB"}:
        parts = re.split(r"\s+vs\.?\s+", mu, flags=re.I)
        if len(parts) == 2:
            return " | ".join(sorted(p.strip().upper() for p in parts))
    return mu


def mlb_conflict(a: dict, b: dict) -> bool:
    if a.get("sport") != "MLB" or b.get("sport") != "MLB":
        return False
    if matchup_key(a) != matchup_key(b) or not matchup_key(a):
        return False
    return is_pitcher_prop(a) != is_pitcher_prop(b)


def can_add(combo: list[dict], r: dict, *, wnba_cap: int) -> bool:
    pn = fold_name(r.get("player"))
    if any(fold_name(x.get("player")) == pn for x in combo):
        return False
    sport = str(r.get("sport") or "")
    mu = matchup_key(r)
    if sport == "WNBA" and mu:
        same = sum(1 for x in combo if x.get("sport") == "WNBA" and matchup_key(x) == mu)
        if same >= wnba_cap:
            return False
    if sport == "NFL" and mu:
        same = sum(1 for x in combo if x.get("sport") == "NFL" and matchup_key(x) == mu)
        if same >= 2:
            return False
    # NHL Std UNDER Pts/Ast: low/high-scoring games move together — one per game.
    if sport == "NHL" and mu:
        side = str(r.get("side") or r.get("direction") or "").strip().upper()
        prop = str(r.get("prop") or r.get("prop_type") or "").strip().lower()
        is_under_pts_ast = side in {"UNDER", "U", "LESS"} and any(
            p in prop for p in ("point", "assist")
        )
        if is_under_pts_ast:
            same_under = sum(
                1
                for x in combo
                if x.get("sport") == "NHL"
                and matchup_key(x) == mu
                and str(x.get("side") or x.get("direction") or "").strip().upper()
                in {"UNDER", "U", "LESS"}
                and any(
                    p in str(x.get("prop") or x.get("prop_type") or "").lower()
                    for p in ("point", "assist")
                )
            )
            if same_under >= 1:
                return False
    if sport == "TENNIS":
        tk = tennis_match_key(mu)
        if tk and any(
            x.get("sport") == "TENNIS" and tennis_match_key(matchup_key(x)) == tk
            for x in combo
        ):
            return False
    if any(mlb_conflict(r, x) for x in combo):
        return False
    return True


def order_by_gate_hr(pool: list[dict], tie_key) -> list[dict]:
    """Best measured hit rate first. Gate-cleared rows with no sample follow.

    A preset ``gate_hr`` + ``gate_hr_n`` is kept when the sample is large
    enough. Otherwise the sport × prop × pick × side category rate is used.
    """
    from utils.category_hit_rate import (
        CATEGORY_HR_MIN_N,
        load_category_hr_index,
        lookup_category_hr,
    )

    idx = load_category_hr_index()
    min_n = float(CATEGORY_HR_MIN_N)
    ranked: list[tuple[float, dict]] = []
    for r in pool:
        hr: float | None = None
        preset_n = r.get("gate_hr_n")
        if r.get("gate_hr") is not None and preset_n is not None:
            try:
                if float(preset_n) >= min_n:
                    hr = float(r["gate_hr"])
            except (TypeError, ValueError):
                hr = None
        if hr is None:
            hit = lookup_category_hr(
                idx,
                sport=r.get("sport"),
                prop=r.get("prop"),
                pick_type=r.get("pick_type"),
                direction=r.get("side") or r.get("direction"),
            )
            if hit and float(hit.get("n") or 0) >= min_n:
                hr = float(hit["hr"])
                r["gate_hr"] = hr
                r["gate_hr_n"] = float(hit["n"])
        if hr is None and r.get("gate_hr") is None:
            try:
                r["gate_hr"] = float(r.get("p") or 0)
            except (TypeError, ValueError):
                r["gate_hr"] = None
        ranked.append((hr if hr is not None else -1.0, r))
    ranked.sort(key=lambda item: (-item[0], tie_key(item[1])))
    return [r for _, r in ranked]


def drain_power3(
    pool: list[dict],
    taken: set[str] | None = None,
    *,
    wnba_cap: int = 1,
) -> list[list[dict]]:
    """3-leg Power slips until the pool cannot fill another. No preset count.

    ``pool`` must already be best-hit-rate first. Sports are not mixed ahead
    of a higher-rate leg. Same-game caps in ``can_add`` still apply.
    """
    used = taken if taken is not None else set()
    out: list[list[dict]] = []
    while True:
        combo = pack(pool, 3, used, mix_sports=False, wnba_cap=wnba_cap)
        if len(combo) != 3:
            return out
        for r in combo:
            used.add(fold_name(r.get("player")))
        out.append(combo)


def slip_group(combo: list[dict], family: str) -> str:
    sports = {str(x.get("sport") or "") for x in combo}
    if family == "mix":
        if len(sports) == 1:
            return f"{next(iter(sports))} Goblin-70 Mix Power 3"
        return "X-Sport Goblin-70 Mix Power 3"
    if family == "standard":
        if len(sports) == 1:
            return f"{next(iter(sports))} Standard Power 3"
        return "X-Sport Standard Power 3"
    if len(sports) == 1:
        return f"{next(iter(sports))} Goblin-70 Power 3"
    return "X-Sport Goblin-70 Power 3"


def pack(
    pool: list[dict],
    n: int,
    taken: set[str],
    *,
    mix_sports: bool = True,
    wnba_cap: int = 1,
) -> list[dict]:
    combo: list[dict] = []
    sports: set[str] = set()

    def add_from(prefer_new_sport: bool) -> None:
        nonlocal combo, sports
        for r in pool:
            if len(combo) == n:
                return
            pn = fold_name(r.get("player"))
            if pn in taken:
                continue
            if not can_add(combo, r, wnba_cap=wnba_cap):
                continue
            if prefer_new_sport and r.get("sport") in sports:
                continue
            combo.append(r)
            sports.add(str(r.get("sport") or ""))

    if mix_sports:
        add_from(True)
    add_from(False)
    return combo if len(combo) == n else []


def pack_yolo(
    gob: list[dict],
    std: list[dict] | None = None,
    *,
    max_tickets: int = YOLO_MAX_TICKETS,
) -> list[list[dict]]:
    """Power 6, then 5, then 4. Mix gate-cleared Standards when available.

    Own taken set so Power 3 / Flex 4 keep their legs.

    Standard mix-ins must already be ``standard_yolo_eligible`` (validated
    keep, not gap-sort / not a sport quota). Prefer stacking multiple
    cleared Standards before Goblin fill — never invent a Standard to hit
    a count. Premium WNBA Goblin sleeve still seeds the Goblin half first.
    """
    taken: set[str] = set()
    out: list[list[dict]] = []
    # Gate-cleared Standards only for YOLO mix (caller should pre-filter).
    std_pool = list(std or [])
    premium = sorted(
        (r for r in gob if premium_goblin_eligible(r)),
        key=premium_goblin_sort_key,
    )
    rest = [r for r in gob if not premium_goblin_eligible(r)]
    ordered_gob = premium + rest
    for n in YOLO_PREF_LEGS:
        if len(out) >= max(0, int(max_tickets)):
            break
        seeds: list[dict] = []
        # All cleared Standards first (no one-per-sport cap), then Goblins.
        for s in std_pool:
            pn = fold_name(s.get("player"))
            if pn and pn not in taken:
                seeds.append(s)
        seeds.extend(r for r in ordered_gob if fold_name(r.get("player")) not in taken)
        combo = pack(seeds, n, taken, mix_sports=True, wnba_cap=1)
        if len(combo) != n:
            combo = pack(seeds, n, taken, mix_sports=True, wnba_cap=2)
        if len(combo) != n:
            continue
        for r in combo:
            taken.add(fold_name(r.get("player")))
        out.append(combo)
    return out


def pack_standard_yolo(
    std: list[dict],
    *,
    max_tickets: int = STANDARD_YOLO_MAX_TICKETS,
) -> list[list[dict]]:
    """All-Standard YOLO Power from gate-cleared legs only.

    No per-sport quota. Combine whatever cleared that day; if fewer than
    STANDARD_YOLO_MIN_LEGS survive, return empty (sit out / fold elsewhere).
    Pack order is ``standard_sort_key`` among keep survivors — never gap-sort.
    """
    ordered = sorted(std or [], key=standard_sort_key)
    if len(ordered) < STANDARD_YOLO_MIN_LEGS:
        return []
    taken: set[str] = set()
    out: list[list[dict]] = []
    for n in STANDARD_YOLO_PREF_LEGS:
        if len(out) >= max(0, int(max_tickets)):
            break
        if n > len(ordered):
            continue
        seeds = [r for r in ordered if fold_name(r.get("player")) not in taken]
        combo = pack(seeds, n, taken, mix_sports=True, wnba_cap=99)
        if len(combo) != n:
            combo = pack(seeds, n, taken, mix_sports=False, wnba_cap=99)
        if len(combo) != n:
            continue
        for r in combo:
            taken.add(fold_name(r.get("player")))
        out.append(combo)
    return out


def binomial(n: int, k: int, p: float) -> float:
    if k < 0 or k > n:
        return 0.0
    return math.comb(n, k) * (p**k) * ((1 - p) ** (n - k))


def ticket_math(legs: list[dict], product: str, family: str, *, date: str = "") -> dict:
    n = len(legs)
    pay = resolve_n_correct(legs, product, family, date=date)
    table = pay["n_correct"]
    ps = [float(x["p"]) for x in legs]
    p = sum(ps) / n
    ev = sum(binomial(n, k, p) * m for k, m in table.items())
    cash = sum(binomial(n, k, p) for k, m in table.items() if m > 0)
    return {
        "mean_leg_p": round(p, 4),
        "sweep_pct": round(100 * binomial(n, n, p), 1),
        "cash_pct": round(100 * cash, 1),
        "ev_n_correct": round(ev, 3),
        "n_correct": table,
        "payout_note": pay["note"],
        "payout_source": pay.get("payout_source") or "n_correct_median",
        "captured_at": pay.get("captured_at"),
    }


def compact(r: dict, *, std: bool = False) -> dict:
    over = str(r.get("side")) == "OVER"
    l5 = r.get("l5_over") if over else r.get("l5_under")
    l10 = r.get("l10_over") if over else r.get("l10_under")
    if l10 is None:
        l10 = r.get("l10")
    # CFB/NFL early-season L5 pad. Hit rates by fill status need this on the
    # archived leg; step8 carries Season L5 Flag / l5_sample_n.
    season_flag = (
        r.get("season_l5_flag")
        or r.get("Season L5 Flag")
        or r.get("season_l5_flag".upper())
        or ""
    )
    sample_n = r.get("l5_sample_n")
    if sample_n is None:
        sample_n = r.get("L5 Sample N")
    l10_current_n = r.get("l10_current_n")
    if l10_current_n is None:
        l10_current_n = r.get("L10 Current N")
    out = {
        "sport": r.get("sport"),
        "player": r.get("player"),
        "prop": r.get("prop"),
        "side": r.get("side"),
        "line": r.get("line"),
        "pick_type": r.get("pick_type"),
        "l5": l5,
        "l10": l10,
        "cover": r.get("cover"),
        "d": r.get("def"),
        "badge": r.get("promo") or r.get("badge"),
        "tier": r.get("prop_tier"),
        "matchup": r.get("matchup") or "",
        "team": r.get("team") or "",
        "p": r.get("p"),
        "ml_prob": r.get("ml_prob"),
        "hit_rate": r.get("hit_rate"),
        "gate_hr": r.get("gate_hr"),
        "p_cell": r.get("p_cell"),
        "p_n": r.get("p_n"),
        "p_source": r.get("p_source"),
        "gate_recipe": r.get("gate_recipe"),
        "standard_line": r.get("standard_line"),
        "gap": r.get("gap"),
        "line_underdog": r.get("line_underdog"),
        "line_draftkings": r.get("line_draftkings"),
        "line_vegas": r.get("line_vegas"),
        "best_cross_book": r.get("best_cross_book"),
        "best_cross_line": r.get("best_cross_line"),
        "cross_edge_vs_pp": r.get("cross_edge_vs_pp"),
        "start_time": str(r.get("start_time") or "").strip(),
        "pp_game_id": str(r.get("pp_game_id") or "").strip(),
        "espn_event_id": str(r.get("espn_event_id") or "").strip(),
        "espn_competition_id": str(r.get("espn_competition_id") or "").strip(),
        "standard_line_source": str(r.get("standard_line_source") or "").strip(),
        "season_l5_flag": (
            ""
            if str(season_flag or "").strip().lower() in {"", "nan", "none", "null"}
            else str(season_flag).strip()
        ),
        "l5_sample_n": (
            None
            if sample_n is not None and str(sample_n).lower() in {"nan", "none", "null"}
            else sample_n
        ),
        "l10_current_n": (
            None
            if l10_current_n is not None and str(l10_current_n).lower() in {"nan", "none", "null"}
            else l10_current_n
        ),
        "board_opponent": str(
            r.get("board_opponent") or r.get("opponent") or r.get("opp") or ""
        ).strip(),
        "fill_status": (
            ""
            if str(season_flag or "").strip().lower() in {"", "nan", "none", "null"}
            else str(season_flag).strip()
        ),
        "gate_branch": str(r.get("gate_recipe") or r.get("gate_branch") or "").strip(),
    }
    if std:
        out["std_kind"] = r.get("std_kind")
    return out


def playable_tickets(payload: dict) -> list[dict]:
    """App card: Goblin-70, Standard O/U that cleared the gate, NFLP groups."""
    return [
        t
        for t in (payload.get("tickets") or [])
        if t.get("family") in {"goblin", "standard", "mix", "nflp", "nflp_std"}
    ]


def web_sport(sport: object) -> str:
    raw = str(sport or "").strip()
    return WEB_SPORT.get(raw.upper(), raw)


def split_matchup(matchup: object, player: object, team: object = "") -> tuple[str, str]:
    if str(team or "").strip():
        s = str(matchup or "").split("(")[0].strip()
        parts = re.split(r"\s+vs\.?\s+", s, flags=re.I)
        opp = parts[1].strip() if len(parts) == 2 else ""
        return str(team).strip(), opp
    s = str(matchup or "").split("(")[0].strip()
    parts = re.split(r"\s+vs\.?\s+", s, flags=re.I)
    if len(parts) != 2:
        return str(player or "").strip(), ""
    a, b = parts[0].strip(), parts[1].strip()
    pn = fold_name(player)
    if pn and fold_name(b) == pn:
        return b, a
    if pn and fold_name(a) == pn:
        return a, b
    return a, b


def _sweep_x(ticket: dict) -> float:
    n = int(ticket.get("n_legs") or 0)
    table = ticket.get("n_correct") or {}
    try:
        return float(table.get(n) or table.get(str(n)))
    except (TypeError, ValueError):
        return 0.0


def _leg_to_web(
    leg: dict, *, ticket_id: str, date: str, ticket_track: str = "goblin70"
) -> dict:
    player = str(leg.get("player") or "").strip()
    sport = web_sport(leg.get("sport"))
    team, opp = split_matchup(leg.get("matchup"), player, leg.get("team"))
    line = leg.get("line")
    try:
        line_f = float(line)
        line_key = f"{line_f:.3f}"
    except (TypeError, ValueError):
        line_f = None
        line_key = ""
    side = str(leg.get("side") or "OVER").upper()
    prop = str(leg.get("prop") or "")
    p = float(leg.get("p") or 0.0)
    l5 = leg.get("l5")
    try:
        l5_f = float(l5) if l5 is not None else None
    except (TypeError, ValueError):
        l5_f = None
    cover = leg.get("cover")
    try:
        cover_f = float(cover) if cover is not None else None
    except (TypeError, ValueError):
        cover_f = None
    try:
        hr = float(leg.get("hit_rate")) if leg.get("hit_rate") is not None else None
    except (TypeError, ValueError):
        hr = None
    if hr is None and l5_f is not None:
        hr = l5_f / 5.0
    try:
        ml = float(leg.get("ml_prob")) if leg.get("ml_prob") is not None else None
    except (TypeError, ValueError):
        ml = None
    try:
        std_line = float(leg.get("standard_line")) if leg.get("standard_line") is not None else None
    except (TypeError, ValueError):
        std_line = None
    try:
        ud_line = float(leg.get("line_underdog")) if leg.get("line_underdog") is not None else None
    except (TypeError, ValueError):
        ud_line = None
    try:
        dk_line = float(leg.get("line_draftkings")) if leg.get("line_draftkings") is not None else None
    except (TypeError, ValueError):
        dk_line = None
    try:
        lv_line = float(leg.get("line_vegas")) if leg.get("line_vegas") is not None else None
    except (TypeError, ValueError):
        lv_line = None
    best_book = str(leg.get("best_cross_book") or "").strip()
    try:
        best_line = float(leg.get("best_cross_line")) if leg.get("best_cross_line") is not None else None
    except (TypeError, ValueError):
        best_line = None
    try:
        cross_edge = float(leg.get("cross_edge_vs_pp")) if leg.get("cross_edge_vs_pp") is not None else None
    except (TypeError, ValueError):
        cross_edge = None
    if not best_book and line_f is not None:
        best_book = "PP"
        best_line = line_f if best_line is None else best_line
        if cross_edge is None:
            cross_edge = 0.0
    material = "|".join(
        [
            sport.lower(),
            player.lower(),
            team.lower(),
            opp.lower(),
            prop.lower(),
            line_key,
            side.lower(),
            date,
        ]
    )
    return {
        "ticket_id": ticket_id,
        "ticket_track": ticket_track,
        "sport": sport,
        "player": player,
        "team": team,
        "opp": opp,
        "prop_type": prop,
        "pick_type": str(leg.get("pick_type") or "Goblin"),
        "direction": side,
        "line": line_f,
        "edge": cover_f,
        "abs_edge": None if cover_f is None else abs(cover_f),
        "pick_platform": "prizepicks",
        "hit_rate": hr,
        "ml_prob": ml,
        "standard_line": std_line,
        "gap": leg.get("gap"),
        "line_underdog": ud_line,
        "line_draftkings": dk_line,
        "line_vegas": lv_line,
        "best_cross_book": best_book,
        "best_cross_line": best_line,
        "cross_edge_vs_pp": cross_edge,
        "tier": str(leg.get("tier") or ""),
        "def_tier": str(leg.get("d") or ""),
        "l5_over": l5_f if side == "OVER" else None,
        "l5_under": l5_f if side == "UNDER" else None,
        "l5_side_hit_rate": None if l5_f is None else l5_f / 5.0,
        "leg_prob_used": p,
        "leg_prob_source": "goblin70_hist",
        "canonical_leg_id": "leg_" + hashlib.sha1(material.encode("utf-8")).hexdigest()[:20],
        "game_date": date,
        "badge": str(leg.get("badge") or ""),
        "cover": cover_f,
        "matchup": str(leg.get("matchup") or ""),
        "season_l5_flag": str(leg.get("season_l5_flag") or "").strip(),
        "l5_sample_n": leg.get("l5_sample_n"),
        "l10_current_n": leg.get("l10_current_n"),
        "start_time": str(leg.get("start_time") or "").strip(),
        "pp_game_id": str(leg.get("pp_game_id") or "").strip(),
        "espn_event_id": str(leg.get("espn_event_id") or "").strip(),
        "espn_competition_id": str(leg.get("espn_competition_id") or "").strip(),
        "standard_line_source": str(leg.get("standard_line_source") or "").strip(),
    }


def _ticket_to_web(ticket: dict, *, date: str, ticket_no: int, group_name: str) -> dict:
    sweep = _sweep_x(ticket)
    mean_p = float(ticket.get("mean_leg_p") or 0.0)
    sweep_pct = float(ticket.get("sweep_pct") or 0.0) / 100.0
    cash_pct = float(ticket.get("cash_pct") or 0.0) / 100.0
    ev_mult = float(ticket.get("ev_n_correct") or 0.0)
    ev = round(ev_mult - 1.0, 4)
    n = int(ticket.get("n_legs") or 0)
    product = str(ticket.get("product") or "Power")
    tt = "flex" if product.lower() == "flex" else "power"
    gn = re.sub(r"[|]+", "_", group_name)[:80]
    ticket_id = f"{date}|{gn}|{ticket_no}"
    n_correct = {int(k): float(v) for k, v in (ticket.get("n_correct") or {}).items()}
    yolo_track = yolo_ticket_track(ticket, group_name)
    yolo = yolo_track is not None
    track = yolo_track or str(ticket.get("ticket_track") or "goblin70")
    legs = [
        _leg_to_web(leg, ticket_id=ticket_id, date=date, ticket_track=track)
        for leg in ticket.get("legs") or []
    ]
    hrs = [float(x["hit_rate"]) for x in legs if x.get("hit_rate") is not None]
    avg_hr = (sum(hrs) / len(hrs)) if hrs else mean_p
    if track == "standard_yolo":
        core_recipe = "standard_yolo"
        core_label = (
            "Standard YOLO (gate-cleared Standards only; off ticket win-rate)"
        )
    elif yolo:
        core_recipe = "goblin70_yolo"
        core_label = (
            "YOLO Power 4/5/6 (Goblin-70 + Standard mix; off ticket win-rate)"
        )
    else:
        core_recipe = "goblin70"
        core_label = "Goblin-70 (L5=5 + L10>=8 + D + cover; Platinum preferred)"
    # Ticket win% is the historical Goblin-70 gate rate (mean_leg_p), not XGBoost.
    # ml_prob stays on each leg for display only. YOLO is display-only vs win-rate.
    return {
        "web_group_name": group_name,
        "ticket_id": ticket_id,
        "ticket_no": ticket_no,
        "ticket_track": track,
        "mode": "goblin70",
        "exclude_from_winrate": bool(yolo),
        "avg_hit_rate": round(avg_hr, 4),
        "est_win_prob": round(sweep_pct, 4),
        "est_flex_cash_prob": round(cash_pct, 4) if tt == "flex" else None,
        "power_payout": sweep if tt == "power" else None,
        "flex_payout": sweep if tt == "flex" else None,
        "base_power_payout": sweep if tt == "power" else None,
        "ev_power": ev,
        "display_min_x": sweep,
        "core_build": True,
        "core_recipe": core_recipe,
        "core_label": core_label,
        "pool_policy": "goblin70",
        "strong_builder": False,
        "legs": legs,
        "n_legs": n,
        "play": product,
        "payout": {
            "ticket_type": tt,
            "payout": sweep,
            "min_guarantee": sweep,
            "min_payout_x": sweep,
            "display_min_x": sweep,
            "p_all_win": round(sweep_pct, 4),
            "p_miss_1": round(cash_pct - sweep_pct, 4) if tt == "flex" else 0.0,
            "ev": ev,
            "recommendation": "OK",
            "entry_10_to_win_guarantee": round(10 * sweep, 2),
            "entry_20_to_win_guarantee": round(20 * sweep, 2),
            "sweep_payout": sweep,
            "sweep_payout_x": sweep,
            "audit_all_hit_x": sweep,
            "payout_source": ticket.get("payout_source") or "n_correct_median",
            "payout_note": ticket.get("payout_note") or "",
            "n_correct": n_correct,
            "ev_formula": "E[N-correct] - 1",
            "captured_at": ticket.get("captured_at"),
        },
    }


def to_web_payload(payload: dict) -> dict:
    date = str(payload.get("date") or "")[:10]
    goblin = playable_tickets(payload)
    groups: list[dict] = []
    by_group: dict[str, list[dict]] = {}
    for t in goblin:
        n = int(t.get("n_legs") or 0)
        product = str(t.get("product") or "Power")
        gname = str(t.get("web_group") or "").strip() or (
            f"X-Sport Goblin-70 {product} {n}"
        )
        by_group.setdefault(gname, []).append(t)
    for gname, slips in by_group.items():
        web_slips = [
            _ticket_to_web(t, date=date, ticket_no=i, group_name=gname)
            for i, t in enumerate(slips, start=1)
        ]
        n_legs = int(slips[0].get("n_legs") or 0)
        product = str(slips[0].get("product") or "Power")
        sweep = _sweep_x(slips[0])
        groups.append(
            {
                "group_name": gname,
                "n_legs": n_legs,
                "power_payout": sweep if product.lower() == "power" else None,
                "flex_payout": sweep if product.lower() == "flex" else None,
                "tickets": web_slips,
            }
        )
    return {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
        "date": date,
        "tennis_date": date,
        "ticket_track": "goblin70",
        "mode": "goblin70",
        "pool_mode": "goblin70",
        "allow_standard": True,
        "filters": {
            "pick_types": "Goblin OVER + Standard O/U",
            "allow_standard": True,
            "goblin_only": False,
            "pool_mode": "goblin70",
            "min_l5": 5,
            "min_l10": 8,
            "cover_floor": "per-prop unit (PRA 3.7, pass yds 15, else sport floor)",
            "note": (
                "Keep-file gates only; a sport with no rule is off. "
                "Leg p is the shrunk gate-cell hit rate. "
                "2- or 3-leg, Power unless Flex N-correct EV is higher. "
                "All-Goblin slips need all-hit payout of at least 1.4x; the dollar test is stored and does not drop them. "
                "Mixed and all-Standard slips must clear the dollar test at their own payout. "
                "Goblin and Standard stay separate. Flat $10. N-correct / To Win only."
            ),
        },
        "payout_note": payload.get("payout_note"),
        "step8_root": payload.get("step8_root"),
        "pool": payload.get("pool"),
        "groups": groups,
    }


def is_g70_group(group: dict) -> bool:
    name = str(group.get("group_name") or "")
    if "Goblin-70" in name:
        return True
    if name.startswith("NFL Power"):
        return True
    tickets = group.get("tickets") or []
    if tickets and isinstance(tickets[0], dict):
        tr = str(tickets[0].get("ticket_track") or "").lower()
        return tr in {
            "goblin70",
            "goblin70_yolo",
            "standard_yolo",
            "standard_ev",
            "mixed_ev",
            "gated_goblin",
            "gated_mixed",
            "gated_standard",
        }
    return False


def _read_json(path: Path) -> dict | None:
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def grade_pool_paths(date: str) -> list[Path]:
    name = f"combined_slate_tickets_{date}.json"
    out = [
        _REPO / "ui_runner" / "data" / name,
        _REPO / "ui_runner" / "templates" / name,
    ]
    sibling = _REPO.parent / "PropORACLE_main_cp"
    if sibling.is_dir() and sibling.resolve() != _REPO.resolve():
        out.extend(
            [
                sibling / "ui_runner" / "data" / name,
                sibling / "ui_runner" / "templates" / name,
            ]
        )
    return out


def _first_mixer_payload(date: str, candidates: list[Path]) -> tuple[dict | None, list[dict]]:
    seen: set[str] = set()
    for path in candidates:
        key = str(path.resolve()) if path.exists() else str(path)
        if key in seen:
            continue
        seen.add(key)
        data = _read_json(path)
        if not data:
            continue
        if str(data.get("date") or "")[:10] != date:
            continue
        groups = [g for g in (data.get("groups") or []) if isinstance(g, dict) and not is_g70_group(g)]
        if groups:
            return data, groups
    return None, []


def union_mixer_groups(*group_lists: list[dict]) -> list[dict]:
    """Keep every unique mixer group_name. Live card first, then grade-pool extras."""
    out: list[dict] = []
    seen: set[str] = set()
    for groups in group_lists:
        for g in groups or []:
            if not isinstance(g, dict):
                continue
            name = str(g.get("group_name") or "").strip()
            if not name or name in seen:
                continue
            seen.add(name)
            out.append(g)
    return out


def load_graded_main(date: str) -> tuple[dict | None, list[dict]]:
    """Mixer groups for the live card.

    Union the live /tickets mixer with the grade-pool JSON. A tennis-only pool
    must not replace CORE / STRONG / MLB groups already on the card, and a
    Goblin-70-only tickets_latest must not wipe the mixer.
    """
    live: list[Path] = list(WEB_JSON_PATHS)
    sibling = _REPO.parent / "PropORACLE_main_cp"
    if sibling.is_dir() and sibling.resolve() != _REPO.resolve():
        live.extend(
            live_json_write_paths("tickets_latest.json", sibling, include_data_snapshot=True)
        )
    pool = _first_mixer_payload(date, grade_pool_paths(date))
    live_payload = _first_mixer_payload(date, live)
    groups = union_mixer_groups(live_payload[1], pool[1])
    meta = live_payload[0] or pool[0]
    return meta, groups


def _sport_token(raw: object) -> str:
    mapped = web_sport(raw).strip().upper()
    if mapped.startswith("NFLP"):
        return "NFL"
    return mapped.split()[0] if mapped else ""


def _board_leg_key(
    player: object,
    sport: object,
    prop: object,
    pick_type: object,
    side: object,
) -> tuple[str, str, str, str, str]:
    d = str(side or "").strip().upper()
    if "UNDER" in d:
        d = "UNDER"
    else:
        d = "OVER"
    pt = str(pick_type or "Standard").strip().title()
    if pt not in {"Goblin", "Demon", "Standard"}:
        pt = "Standard"
    return (
        fold_name(player),
        _sport_token(sport),
        str(prop or "").strip().lower(),
        pt,
        d,
    )


def index_board_legs(board: list[dict]) -> dict[tuple[str, str, str, str, str], list[dict]]:
    idx: dict[tuple[str, str, str, str, str], list[dict]] = {}
    for r in board or []:
        if not isinstance(r, dict):
            continue
        k = _board_leg_key(
            r.get("player"),
            r.get("sport"),
            r.get("prop") or r.get("prop_type"),
            r.get("pick_type"),
            r.get("side") or r.get("direction"),
        )
        if not k[0] or not k[2]:
            continue
        idx.setdefault(k, []).append(r)
    return idx


def _pick_board_row(rows: list[dict], old_line: float | None) -> dict | None:
    if not rows:
        return None
    if old_line is not None:
        for r in rows:
            try:
                if abs(float(r.get("line")) - old_line) < 1e-9:
                    return r
            except (TypeError, ValueError):
                continue
    return rows[0]


def _apply_board_row_to_leg(leg: dict, row: dict) -> bool:
    """Copy fetch fields onto a live ticket leg. True if the line moved."""
    changed = False
    try:
        new_line = float(row.get("line"))
    except (TypeError, ValueError):
        new_line = None
    try:
        old_line = float(leg.get("line"))
    except (TypeError, ValueError):
        old_line = None
    if new_line is not None and (old_line is None or abs(new_line - old_line) > 1e-9):
        leg["line"] = new_line
        changed = True
    side = str(row.get("side") or leg.get("direction") or "OVER").upper()
    cover = row.get("cover")
    if cover is not None:
        try:
            cover_f = float(cover)
            leg["edge"] = cover_f
            leg["abs_edge"] = abs(cover_f)
            leg["cover"] = cover_f
        except (TypeError, ValueError):
            pass
    l5 = row.get("l5_over") if side == "OVER" else row.get("l5_under")
    if l5 is not None:
        try:
            l5_f = float(l5)
            if side == "OVER":
                leg["l5_over"] = l5_f
            else:
                leg["l5_under"] = l5_f
            leg["l5_side_hit_rate"] = l5_f / 5.0
        except (TypeError, ValueError):
            pass
    if row.get("def") is not None:
        leg["def_tier"] = str(row.get("def") or "")
    if row.get("matchup"):
        leg["matchup"] = str(row.get("matchup"))
    start = str(row.get("start_time") or "").strip()
    if start and start.lower() not in {"nan", "none", "nat"}:
        leg["start_time"] = start
    for key in ("pp_game_id", "espn_event_id", "espn_competition_id"):
        val = str(row.get(key) or "").strip()
        if val and val.lower() not in {"nan", "none"}:
            leg[key] = val
    return changed


def patch_mixer_groups(
    groups: list[dict],
    board: list[dict],
) -> tuple[list[dict], dict[str, int]]:
    """Update mixer legs when the fetch moved a line. Keep the slip if it is off-board.

    Live /tickets must keep Goblin-70 and graded-main together. Dropping unmatched
    legs emptied the mixer after afternoon games left the PP board.
    """
    idx = index_board_legs(board)
    stats = {"updated": 0, "dropped": 0, "unchanged": 0}
    out: list[dict] = []
    for group in groups or []:
        if not isinstance(group, dict):
            continue
        kept: list[dict] = []
        for ticket in group.get("tickets") or []:
            if not isinstance(ticket, dict):
                continue
            legs = ticket.get("legs") or []
            if not isinstance(legs, list) or not legs:
                kept.append(ticket)
                stats["unchanged"] += 1
                continue
            new_legs: list[dict] = []
            changed = False
            for leg in legs:
                if not isinstance(leg, dict):
                    new_legs.append(leg)
                    continue
                k = _board_leg_key(
                    leg.get("player"),
                    leg.get("sport"),
                    leg.get("prop_type") or leg.get("prop"),
                    leg.get("pick_type"),
                    leg.get("direction") or leg.get("side"),
                )
                rows = idx.get(k) or []
                try:
                    old_line = float(leg.get("line"))
                except (TypeError, ValueError):
                    old_line = None
                row = _pick_board_row(rows, old_line)
                if row is None:
                    new_legs.append(leg)
                    continue
                patched = dict(leg)
                if _apply_board_row_to_leg(patched, row):
                    changed = True
                new_legs.append(patched)
            nxt = dict(ticket)
            nxt["legs"] = new_legs
            kept.append(nxt)
            if changed:
                stats["updated"] += 1
            else:
                stats["unchanged"] += 1
        if kept:
            nxt_g = dict(group)
            nxt_g["tickets"] = kept
            out.append(nxt_g)
    return out, stats


def mark_dup_players(payload: dict) -> dict:
    """Tag a later slip that repeats a player already on the merged card.

    Goblin-70 groups are scanned first, then the mixer. Does not drop a
    group. ``dup_player_tickets`` is the count before any later hygiene trim.
    """
    from utils.slate_exposure_ledger import fold_player

    if not isinstance(payload, dict):
        return payload
    seen: dict[str, str] = {}
    n_tickets = 0
    for group in payload.get("groups") or []:
        if not isinstance(group, dict):
            continue
        for ticket in group.get("tickets") or []:
            if not isinstance(ticket, dict):
                continue
            dups: list[str] = []
            for leg in ticket.get("legs") or []:
                if not isinstance(leg, dict):
                    continue
                name = fold_player(leg.get("player"))
                if not name:
                    continue
                if name in seen:
                    leg["dup_player"] = "DUP_PLAYER"
                    dups.append(str(leg.get("player") or name))
                else:
                    seen[name] = str(ticket.get("ticket_id") or ticket.get("id") or "")
            if dups:
                ticket["dup_player"] = "DUP_PLAYER"
                ticket["dup_players"] = dups
                n_tickets += 1
    payload["dup_player_tickets"] = n_tickets
    return payload


def merge_web_payload(
    g70: dict,
    main_payload: dict | None = None,
    main_groups: list[dict] | None = None,
) -> dict:
    """Goblin-70 groups first, then the graded-main mixer. Both stay on /tickets."""
    g70_groups = list(g70.get("groups") or [])
    mixer = list(main_groups or [])
    out = dict(main_payload) if main_payload else {}
    out.update({k: v for k, v in g70.items() if k != "groups"})
    out["groups"] = g70_groups + mixer
    out["ticket_track"] = "goblin70"
    out["mode"] = "goblin70+graded_main" if mixer else "goblin70"
    out["tracks"] = ["goblin70", "graded_main"] if mixer else ["goblin70"]
    return out


def write_web(payload: dict, board: list[dict] | None = None) -> list[Path]:
    g70 = to_web_payload(payload)
    date = str(g70.get("date") or "")[:10]
    main_payload, main_groups = load_graded_main(date)
    rows = board if board is not None else load_today_board(date)
    if rows:
        main_groups, patch_stats = patch_mixer_groups(main_groups, rows)
        print(
            "mixer patch from fetch: "
            f"updated={patch_stats['updated']} dropped={patch_stats['dropped']} "
            f"unchanged={patch_stats['unchanged']}"
        )
    web = merge_web_payload(g70, main_payload, main_groups)
    web = mark_dup_players(web)
    dup_n = int(web.get("dup_player_tickets") or 0)
    print(
        f"web merge goblin70_groups={len(g70.get('groups') or [])} "
        f"graded_main_groups={len(main_groups)} dup_player_tickets={dup_n}"
    )
    # Dual-card pool_mode=goblin70 must not disable CORE/mixer keep hygiene.
    # Scrub construction bans + enforce MAIN player-slip cap on every merge.
    try:
        from combined_slate_tickets import (
            filter_main_high_prob_payload,
            trim_payload_max_slips_per_player,
        )

        before_n = sum(len(g.get("tickets") or []) for g in web.get("groups") or [])
        web = filter_main_high_prob_payload(web)
        web = trim_payload_max_slips_per_player(web)
        after_n = sum(len(g.get("tickets") or []) for g in web.get("groups") or [])
        dropped = before_n - after_n
        if dropped:
            print(
                f"dual-card hygiene: dropped {dropped} slip(s) "
                f"(keep/construction or slate exposure anchor-cap); "
                f"pool_mode={web.get('pool_mode')!r} preserved"
            )
    except Exception as exc:
        print(f"WARN: dual-card hygiene skipped ({exc})")
    try:
        from combined_slate_tickets import apply_payout_patch_to_payload

        n_scrape = apply_payout_patch_to_payload(web)
        if n_scrape:
            print(f"scrape patch: {n_scrape} tickets -> live_cdp")
    except Exception as exc:
        print(f"WARN: scrape patch skipped ({exc})")
    if not main_groups:
        print(
            "WARN: mixer groups empty — /tickets would be Goblin-70 only. "
            "Need ui_runner/data/combined_slate_tickets_{date}.json (or same-date "
            "tickets_latest) with graded_main groups."
        )
    text = json.dumps(web, ensure_ascii=False, indent=2, allow_nan=False)
    written: list[Path] = []
    paths = list(WEB_JSON_PATHS)
    main_cp = _REPO.parent / "PropORACLE_main_cp"
    if main_cp.is_dir():
        paths.extend(
            live_json_write_paths("tickets_latest.json", main_cp, include_data_snapshot=True)
        )
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
        written.append(path)
        print("web", path)
    try:
        from g70_health import stamp_g70_rebuild

        stamp_g70_rebuild(
            ok=True,
            slate_date=date,
            detail={
                "goblin70_groups": len(g70.get("groups") or []),
                "graded_main_groups": len(main_groups),
                "paths": [str(p) for p in written[:6]],
            },
        )
        print("g70 health stamp -> logs/LAST_G70_REBUILD.json")
    except Exception as stamp_exc:
        print(f"WARN: G70 health stamp skipped ({stamp_exc})")
    try:
        from utils.ui_live_json import refresh_pipeline_status_from_slate, sync_live_json_pairs

        sync_live_json_pairs(_REPO)
        refresh_pipeline_status_from_slate(_REPO)
        if main_cp.is_dir() and main_cp.resolve() != _REPO.resolve():
            sync_live_json_pairs(main_cp)
            refresh_pipeline_status_from_slate(main_cp)
    except Exception as sync_exc:
        print(f"WARN: live JSON sync skipped ({sync_exc})")
    try:
        from ticket_run_archive import archive_and_merge_grade_pool, new_run_id

        rid = str(web.get("run_id") or new_run_id())
        web["run_id"] = rid
        meta = archive_and_merge_grade_pool(
            web, date_str=date, run_id=rid, source="goblin70_write_web"
        )
        print(
            f"ticket-run archive {meta.get('date')} run_id={meta.get('run_id')} "
            f"(goblin70_write_web)"
        )
    except Exception as arch_exc:
        print(f"WARN: ticket run archive skipped ({arch_exc})")
    return written


def publish_live(date: str) -> int:
    """Commit/push tickets JSON to origin/main so Railway + GitHub raw stay current."""
    ps1 = _REPO / "scripts" / "Publish-LiveSite.ps1"
    if not ps1.is_file():
        print("WARN: Publish-LiveSite.ps1 missing — live site not updated")
        return 1
    msg = f"chore: Goblin-70 tickets {date}"
    cmd = [
        "pwsh",
        "-NoProfile",
        "-File",
        str(ps1),
        "-RepoRoot",
        str(_REPO),
        "-CommitMessage",
        msg,
    ]
    print("publish-live", " ".join(cmd))
    proc = subprocess.run(cmd, cwd=str(_REPO))
    print(f"publish-live exit {proc.returncode}")
    return int(proc.returncode)


def make_ticket(
    tid: str,
    legs: list[dict],
    product: str,
    family: str,
    note: str,
    *,
    web_group: str = "",
    date: str = "",
) -> dict:
    n = len(legs)
    out = {
        "id": tid,
        "name": f"{product} {n} · {family}",
        "note": note,
        "n_legs": n,
        "product": product,
        "family": family,
        "play": product,
        "mix": f"{sum(1 for x in legs if x.get('pick_type')=='Goblin')} Goblin / "
        f"{sum(1 for x in legs if x.get('pick_type')=='Standard')} Standard",
        **ticket_math(legs, product, family, date=date),
        "sports": sorted({str(x.get("sport")) for x in legs}),
        "legs": legs,
    }
    if web_group:
        out["web_group"] = web_group
    return out


def build(date: str, *, l5_eq_5: bool = False, skip_golf: bool = False) -> dict:
    board = load_today_board(date, skip_golf=skip_golf)
    _attach_tennis_espn_event_ids(board, date)
    for r in board:
        attach_wnba_ticket_priors(r, date)
    gob_raw = [
        r
        for r in board
        if goblin_70_eligible(r) and fold_name(r.get("player")) not in SKIP_PLAYERS
    ]
    # Soft deeper Goblins (SOT 0.5 when 1.5 is posted) stay list-only.
    gob_raw = keep_hardest_goblin_over_faces(board, gob_raw)
    if l5_eq_5:
        gob_raw = [r for r in gob_raw if int(directional_l5(r) or 0) == 5]
    std_raw = []
    for r in board:
        if not standard_ticket_eligible(r):
            continue
        if fold_name(r.get("player")) in SKIP_PLAYERS:
            continue
        rec = dict(r)
        rec["std_kind"] = standard_flex_kind(r) or "gate"
        rec["p"] = standard_ticket_p(rec["std_kind"])
        std_raw.append(rec)
    for r in gob_raw:
        r["p"] = goblin_ticket_p(r)

    gob = unique_by_player_prop(gob_raw, goblin_sort_key)
    std = unique_by_player_prop(std_raw, standard_sort_key)
    from utils.category_hit_rate import load_category_hr_index, lookup_category_hr

    hr_index = load_category_hr_index()

    def _recipe(r: dict) -> str:
        rule = rule_for(r.get("sport"))
        mode = rule[0] if rule else "off"
        return f"{r.get('sport')}:{mode}"

    for r in gob:
        attach_leg_p(r, recipe=_recipe(r), index=hr_index)
        # Own gate cell when one exists. The L5=5 floor is only for a leg
        # with no cell. Do not raise a real cell up to that floor.
        child = None
        if r.get("p_source") == "gate_cell":
            child = lookup_category_hr(
                hr_index,
                sport=r.get("sport"),
                prop=r.get("prop"),
                pick_type=r.get("pick_type"),
                direction=r.get("side") or r.get("direction"),
            )
        if child and child.get("hr") is not None:
            r["p"] = float(child["hr"])
            r["p_n"] = float(child.get("n") or 0)
            r["p_source"] = "gate_cell"
        else:
            r["p"] = goblin_ticket_p(r)
            r["p_source"] = "l5eq5_floor"
        from utils.mlb_keep_gates import pitcher_l10_parent_cell

        parent = pitcher_l10_parent_cell(r)
        if parent is not None:
            r["p"] = parent["hr"]
            r["p_n"] = parent["n"]
            r["p_source"] = "mlb_l10_parent"
            child = {"hr": parent["hr"]}
        if is_postseason_ticket_leg(r):
            cell = float(child["hr"]) if child and child.get("hr") is not None else None
            apply_postseason_price(r, cell)
        # Games Won cap stays 0.62. Total Games uses the corrected keep
        # cell, not the wrong-match catalog rate.
        if _is_tennis(str(r.get("sport") or "")) and _prop(r) == "games_won":
            r["p"] = P_TENNIS_GAMES_WON_GOBLIN_LIVE
            r["p_source"] = "tennis_gw_live"
        elif _is_tennis(str(r.get("sport") or "")) and _prop(r) == "match_total_games":
            r["p"] = P_TENNIS_MATCH_TOTAL_GOBLIN
            r["p_source"] = "tennis_tg_corrected"
    for r in std:
        attach_leg_p(r, recipe=_recipe(r), index=hr_index)
        if is_postseason_ticket_leg(r):
            cell = float(r["p"]) if r.get("p_source") == "gate_cell" and r.get("p") is not None else None
            apply_postseason_price(r, cell)
        elif r.get("p") is None:
            r["p"] = standard_ticket_p(r.get("std_kind") or "gate")
            r["p_source"] = "strict_floor"
        if not is_postseason_ticket_leg(r):
            from utils.comparison_tracks import stamp_standard_cell

            stamp_standard_cell(r, hr_index)

    from utils.gap_ranker import log_slate, score_and_maybe_disable, stamp_rank_fields

    try:
        for line in score_and_maybe_disable():
            print(line)
    except Exception as exc:
        print(f"WARNING: gap ranker score failed: {exc}")
    for r in list(gob) + list(std):
        stamp_rank_fields(r, hr_index)
    try:
        for rec in log_slate(list(gob) + list(std), date=date):
            print(
                f"gap_rank_{str(rec['sport']).lower()} | logged | "
                f"pool {rec['pool_n']} games {rec['games']} | order {rec['order_used']} | "
                f"reordered {len(rec['legs_reordered'])} | ev_gap {rec['ev_gap']}"
            )
    except Exception as exc:
        print(f"WARNING: gap ranker log failed: {exc}")

    # Standard YOLO pool: validated keep only (subset of Flex Standard).
    yolo_std_raw = []
    for r in board:
        kind = standard_yolo_kind(r)
        if not kind:
            continue
        if fold_name(r.get("player")) in SKIP_PLAYERS:
            continue
        rec = dict(r)
        rec["std_kind"] = kind
        rec["p"] = standard_ticket_p(kind)
        yolo_std_raw.append(rec)
    yolo_std = unique_by_player(yolo_std_raw, standard_sort_key)

    taken: set[str] = set()
    tickets: list[dict] = []

    def _ticket(*args, **kwargs):
        kwargs.setdefault("date", date)
        return make_ticket(*args, **kwargs)

    def _emit(scored: dict, *, tid: str, family: str, track: str, comparison_track: str) -> None:
        legs = scored["legs"]
        n = len(legs)
        product = str(scored["product"])
        group = slip_group(legs, family).replace("Power 3", f"{product} {n}")
        slip = _ticket(
            tid,
            [compact(x, std=family == "standard") for x in legs],
            product,
            family,
            scored["payout_note"],
            web_group=group,
        )
        slip["ev_n_correct"] = round(float(scored["expected_multiple"]), 3)
        slip["n_correct"] = {str(k): v for k, v in scored["n_correct"].items()}
        slip["payout_note"] = scored["payout_note"]
        slip["payout_source"] = scored["payout_source"]
        slip["stake"] = scored["stake"]
        slip["ticket_track"] = track
        slip["track"] = comparison_track
        slip["p_win"] = round(float(scored["p_win"]), 4)
        slip["ev"] = round(float(scored["ev"]), 4)
        slip["all_hit_x"] = scored.get("all_hit_x")
        slip["dollar_test"] = bool(scored.get("dollar_test"))
        slip["ev_label"] = "EV-positive" if scored.get("dollar_test") else "EV-negative"
        slip["stake"] = scored.get("stake") or 20
        from utils.comparison_tracks import append_track_log

        append_track_log(slip, date=date)
        if track in {"standard_ev", "mixed_ev"}:
            slip["exclude_from_winrate"] = True
            slip["core_recipe"] = track
        if not counts_toward_gated_book(slip):
            slip["exclude_from_winrate"] = True
            slip["exclude_from_book"] = True
        tickets.append(slip)

    from utils.comparison_tracks import (
        MAX_SLIPS_PER_TRACK,
        TRACK_GOBLIN,
        TRACK_MIXED,
        TRACK_STANDARD,
        build_one_standard_slips,
    )
    from utils.ticket_ev_build import build_ev_slips, leg_id

    post_pool = [
        r
        for r in list(gob) + list(std)
        if r.get("postseason") == 1 and r.get("p") is not None
    ]
    gob = [r for r in gob if r.get("postseason") != 1]
    std = [r for r in std if r.get("postseason") != 1]
    qualified_std = [r for r in std if r.get("p_source") == "standard_cell"]
    g_scored = build_sport_then_mix(
        list(gob),
        taken=taken,
        fold=fold_name,
        max_slips=MAX_SLIPS_PER_TRACK,
    )
    blocked = {
        fold_name(leg.get("player"))
        for scored in g_scored
        for leg in scored["legs"]
        if fold_name(leg.get("player"))
    }
    for i, scored in enumerate(g_scored, start=1):
        n = len(scored["legs"])
        _emit(
            scored,
            tid=f"G{n}-{i}",
            family="goblin",
            track="goblin70",
            comparison_track=TRACK_GOBLIN,
        )
    m_scored: list[dict] = []
    if not l5_eq_5:
        m_scored = build_one_standard_slips(
            gob,
            qualified_std,
            taken=taken,
            fold=fold_name,
            blocked_players=blocked,
            max_slips=MAX_SLIPS_PER_TRACK,
        )
    for i, scored in enumerate(m_scored, start=1):
        n = len(scored["legs"])
        _emit(
            scored,
            tid=f"M{n}-{i}",
            family="mix",
            track="mixed_ev",
            comparison_track=TRACK_MIXED,
        )
    s_pool = [
        r
        for r in qualified_std
        if fold_name(r.get("player")) not in blocked and leg_id(r, fold_name) not in taken
    ]
    s_scored: list[dict] = []
    if not l5_eq_5:
        s_scored = build_ev_slips(
            s_pool,
            taken=taken,
            fold=fold_name,
            family="standard",
            require_dollar=False,
            max_slips=MAX_SLIPS_PER_TRACK,
        )
    for i, scored in enumerate(s_scored, start=1):
        n = len(scored["legs"])
        _emit(
            scored,
            tid=f"S{n}-{i}",
            family="standard",
            track="standard_ev",
            comparison_track=TRACK_STANDARD,
        )

    from utils.l5_only_shadow import record_l5_shadow

    l5_shadow = record_l5_shadow(board, date=date, index=hr_index)

    from utils.card_side import CardBook, load_live_card_book
    from utils.l5_ranked_by_cell import record_l5_ranked

    sports = sorted({str(r.get("sport") or "").strip().upper() for r in board if r.get("sport")})
    offers = load_live_card_book(sports)
    if offers is None:
        print("l5 ranked: live card unavailable, no side confirmed, nothing ranked")
        offers = CardBook.empty()
    l5_ranked = record_l5_ranked(board, date=date, index=hr_index, offers=offers)

    post_groups = shortlist_postseason(post_pool)
    cap = postseason_daily_cap()
    post_emitted = 0
    for i, group in enumerate(post_groups, start=1):
        mark_placed(group, group.get("scraped_x"))
        if not group.get("placed"):
            continue
        if post_emitted >= cap:
            break
        legs = group["legs"]
        for leg in legs:
            leg["postseason"] = 1
            leg["grade_track"] = POSTSEASON_TRACK
        slip = _ticket(
            f"PS{group['n_legs']}-{i}",
            [compact(x, std="goblin" not in str(x.get("pick_type") or "").lower()) for x in legs],
            "Power",
            "goblin" if group["n_goblin"] == group["n_legs"] else "mix",
            (
                f"Postseason cell-3. Placed only after EV stayed positive at the "
                f"scraped payout with every leg 2 points under the haircut cell."
            ),
            web_group="Postseason",
        )
        slip["postseason"] = 1
        slip["ticket_track"] = POSTSEASON_TRACK
        slip["track"] = POSTSEASON_TRACK
        slip["grade_track"] = POSTSEASON_TRACK
        from utils.comparison_tracks import append_track_log

        append_track_log(slip, date=date)
        slip["exclude_from_book"] = True
        slip["exclude_from_winrate"] = True
        slip["placed"] = True
        slip["shortlisted"] = True
        if counts_toward_gated_book(slip):
            continue
        tickets.append(slip)
        post_emitted += 1

    for i, combo in enumerate(pack_standard_yolo(yolo_std), start=1):
        n_use = len(combo)
        slip = _ticket(
            f"YS{n_use}-{i}",
            [compact(x, std=True) for x in combo],
            "Power",
            "standard",
            (
                "Standard YOLO Power. Off Income. Gate-cleared Standards only, "
                f"{n_use} legs, no per-sport quota."
            ),
            web_group=f"YOLO Standard Power {n_use}",
        )
        slip["exclude_from_winrate"] = True
        slip["ticket_track"] = "standard_yolo"
        slip["core_recipe"] = "standard_yolo"
        tickets.append(slip)

    nfl_raw = [r for r in board if nflp_ticket_eligible(r)]
    nfl_family = "nflp"
    if not nfl_raw:
        nfl_raw = [r for r in board if nflp_std_over_eligible(r)]
        nfl_family = "nflp_std"
    for r in nfl_raw:
        r["p"] = nflp_ticket_p(r)
    nfl = order_by_gate_hr(unique_by_player(nfl_raw, goblin_sort_key), goblin_sort_key)
    nfl_taken: set[str] = set()
    i = 1
    while True:
        combo = pack(nfl, 3, nfl_taken, mix_sports=False, wnba_cap=2)
        n_use = 3
        if len(combo) != 3:
            combo = pack(nfl, 2, nfl_taken, mix_sports=False, wnba_cap=2)
            n_use = 2
        if len(combo) < 2:
            break
        for r in combo:
            nfl_taken.add(fold_name(r.get("player")))
        note = (
            "NFLP week-3 Goblin OVER. Kickers L5>=4; backup skill needs D. Not the 70% book."
            if nfl_family == "nflp"
            else "NFLP week-3 Standard OVER. Backup skill with D; kickers L5>=4. Not mixed onto Goblin-70."
        )
        tickets.append(
            _ticket(
                f"NFL{n_use}-{i}",
                [compact(x) for x in combo],
                "Power",
                nfl_family,
                note,
                web_group=f"NFL Power {n_use}",
            )
        )
        i += 1
        if n_use == 2:
            break

    choose = getattr(R, "_choose_step8_root", None)
    root = choose([_REPO, _REPO.parent / "PropORACLE_main_cp"], date) if callable(choose) else _REPO
    return {
        "date": date,
        "built_at": datetime.now(ET).isoformat(timespec="seconds"),
        "step8_root": str(root) if root else None,
        "payout_note": (
            "N-correct / To Win only. Ignore 1st place. "
            "Rates from PrizePicks CDP scrapes (Goblin-Δ), then mix_by_delta."
        ),
        "pool": {
            "goblin_70": len(gob),
            "goblin_70_raw": len(gob_raw),
            "standard_allowlist": len(std),
            "standard_allowlist_raw": len(std_raw),
            "standard_yolo": len(yolo_std),
            "standard_yolo_raw": len(yolo_std_raw),
            "nflp_goblin": len(nfl),
            "nflp_goblin_raw": len(nfl_raw),
        },
        "goblin_pool": [compact(r) for r in gob],
        "standard_pool": [compact(r, std=True) for r in std],
        "standard_yolo_pool": [compact(r, std=True) for r in yolo_std],
        "nflp_pool": [compact(r) for r in nfl],
        "tickets": tickets,
        "l5_only_shadow": {
            "track": l5_shadow["track"],
            "placed": False,
            "n_legs": l5_shadow["n_legs"],
            "n_slips": l5_shadow["n_slips"],
            "legs": [
                {
                    "player": leg.get("player"),
                    "sport": leg.get("sport"),
                    "prop": leg.get("prop"),
                    "side": leg.get("side"),
                    "line": leg.get("line"),
                    "pick_type": leg.get("pick_type"),
                    "p": leg.get("p"),
                    "p_n": leg.get("p_n"),
                    "p_source": leg.get("p_source"),
                }
                for leg in l5_shadow["legs"]
            ],
            "slips": [
                {
                    "placed": False,
                    "p_win": slip.get("p_win"),
                    "ev": slip.get("ev"),
                    "all_hit_x": slip.get("all_hit_x"),
                    "ev_label": slip.get("ev_label"),
                    "legs": [
                        f"{leg.get('player')} {leg.get('prop')} {leg.get('line')}"
                        for leg in slip.get("legs") or []
                    ],
                }
                for slip in l5_shadow["slips"]
            ],
        },
        "l5_ranked_by_cell": {
            "track": l5_ranked["track"],
            "placed": False,
            "n_slips": l5_ranked["n_slips"],
            "n_placed": l5_ranked["n_placed"],
            "stamped_slips": l5_ranked["stamped_slips"],
            "ready_to_compare": l5_ranked["ready_to_compare"],
            "report_only": len(l5_ranked["report_only"]),
            "slips": [
                {
                    "placed": slip.get("placed"),
                    "n_legs": slip.get("n_legs"),
                    "ev": slip.get("ev"),
                    "all_hit_x": slip.get("all_hit_x"),
                    "ev_label": slip.get("ev_label"),
                    "legs": [
                        {
                            "player": leg.get("player"),
                            "prop": leg.get("prop"),
                            "side": leg.get("side"),
                            "line": leg.get("line"),
                            "l5_values": leg.get("l5_values"),
                            "cell_hr": leg.get("cell_hr"),
                            "gap_z": leg.get("gap_z"),
                            "group_rank": leg.get("group_rank"),
                            "unproven": leg.get("unproven"),
                        }
                        for leg in slip.get("legs") or []
                    ],
                }
                for slip in l5_ranked["slips"]
            ],
        },
        "_board": board,
    }


def print_card(payload: dict) -> None:
    print(f"date {payload['date']}  root {payload.get('step8_root')}")
    pool = payload["pool"]
    print(
        f"Goblin-70 unique {pool['goblin_70']} (raw {pool['goblin_70_raw']})  "
        f"Standard gate unique {pool['standard_allowlist']} "
        f"(raw {pool['standard_allowlist_raw']}; "
        f"YOLO keep {pool.get('standard_yolo', 0)})  "
        f"NFLP Goblin unique {pool.get('nflp_goblin', 0)} "
        f"(raw {pool.get('nflp_goblin_raw', 0)})"
    )
    print("\nGOBLIN-70 TOP SEEDS")
    for r in payload["goblin_pool"][:12]:
        print(
            f"  {r['tier']}/{r['badge']}  {r['sport']:6} {r['player']:22} "
            f"{r['prop']:18} O{r['line']}  L5 {r['l5']}  cov {r['cover']:+.1f}"
        )
    print("\nSTANDARD ALLOWLIST")
    if not payload["standard_pool"]:
        print("  (none tonight)")
    for r in payload["standard_pool"]:
        print(
            f"  {r.get('std_kind'):22} {r['sport']:6} {r['player']:22} "
            f"{r['prop']:18} {r['side'][0]}{r['line']}  L5 {r['l5']}  "
            f"p={(r.get('p') if r.get('p') is not None else 0):.3f}  {r.get('matchup') or ''}"
        )
    print("\nTICKETS")
    for t in payload["tickets"]:
        print(
            f"  {t['id']} {t['product']} {t['n_legs']}  {t['mix']}  "
            f"N-correct {t['n_correct']}  sweep {t['sweep_pct']}%  "
            f"EV {t['ev_n_correct']}x  {t['payout_note']}"
        )
        for leg in t["legs"]:
            print(
                f"    [{leg['pick_type'][0]}] {leg['sport']} {leg['player']} "
                f"{leg['prop']} {leg['side'][0]}{leg['line']}"
            )
    shadow = payload.get("l5_only_shadow") or {}
    if shadow:
        print(
            f"\nL5=5 SHADOW (not placed)  legs {shadow.get('n_legs')}  "
            f"2-leg slips {shadow.get('n_slips')}"
        )
        for slip in shadow.get("slips") or []:
            print(
                f"  shadow  p_win {slip.get('p_win')}  "
                f"{slip.get('all_hit_x')}x  {slip.get('ev_label')}  "
                + " | ".join(slip.get("legs") or [])
            )
    ranked = payload.get("l5_ranked_by_cell") or {}
    if ranked:
        print(
            f"\nL5 RANKED BY CELL (off /tickets)  slips {ranked.get('n_slips')}  "
            f"placed {ranked.get('n_placed')}  "
            f"stamped {ranked.get('stamped_slips')}"
        )
        for slip in ranked.get("slips") or []:
            print(
                f"  {slip.get('ev_label')}  {slip.get('n_legs')} legs  "
                f"{slip.get('all_hit_x')}x"
            )
            for leg in slip.get("legs") or []:
                print(
                    f"    {leg.get('player')} {leg.get('prop')} "
                    f"{leg.get('side')} {leg.get('line')}  "
                    f"values {leg.get('l5_values')}  "
                    f"cell {leg.get('cell_hr')}  gap {leg.get('gap_z')}  "
                    f"group {leg.get('group_rank')}"
                )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", default=datetime.now(ET).strftime("%Y-%m-%d"))
    ap.add_argument(
        "--write-web",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Merge Goblin-70 onto /tickets (Goblin-70 first, mixer under). Default: on.",
    )
    ap.add_argument(
        "--l5-eq-5",
        action="store_true",
        help="Restrict Goblin pool to directional L5 = 5 (juice cut).",
    )
    ap.add_argument(
        "--publish-live",
        action="store_true",
        help="After --write-web, run Publish-LiveSite.ps1 (origin/main / Railway).",
    )
    ap.add_argument(
        "--skip-golf",
        action="store_true",
        help="Exclude golf from the G70 pool (golf step8 xlsx missing). Other sports still ticket.",
    )
    args = ap.parse_args()
    payload = build(args.date, l5_eq_5=args.l5_eq_5, skip_golf=args.skip_golf)
    board = payload.pop("_board", None)
    if not args.l5_eq_5:
        obs = log_wnba_pra_off_observe(args.date, board)
        payload["pra_off_observe"] = {
            "n": obs.get("n") or 0,
            "n_already_live": obs.get("n_already_live") or 0,
            "n_proposed": obs.get("n_proposed") or 0,
        }
        print(
            f"  [observe] WNBA PRA Off-without-S/A: {obs['n']} rows "
            f"({obs['n_proposed']} not on live tickets, {obs['n_already_live']} already live)"
        )
        threes_obs = log_wnba_threes_vol_observe(args.date, board)
        payload["threes_vol_observe"] = {
            "n": threes_obs.get("n") or 0,
            "n_already_live": threes_obs.get("n_already_live") or 0,
            "n_proposed": threes_obs.get("n_proposed") or 0,
        }
        print(
            f"  [observe] WNBA 3PM vol-replace: {threes_obs['n']} rows "
            f"({threes_obs['n_proposed']} not on live tickets, "
            f"{threes_obs['n_already_live']} already live)"
        )
        std_u = log_wnba_std_under_watch(args.date, board)
        payload["std_under_watch"] = {
            "n": std_u.get("n") or 0,
            "decided": std_u.get("decided") or 0,
            "hr": std_u.get("hr"),
            "wilson_lo": std_u.get("wilson_lo"),
        }
        print(
            f"  [watch] WNBA Standard UNDER G70: {std_u['n']} rows "
            f"(decided {std_u.get('decided') or 0}, hr {std_u.get('hr')}, "
            f"Wilson LB {std_u.get('wilson_lo')})"
        )
    suffix = "_l5eq5" if args.l5_eq_5 else ""
    out = OUT_DIR / f"goblin70_tickets_{args.date}{suffix}.json"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if not args.l5_eq_5:
        # Local latest always (pre-existing). Peer main_cp only on --write-web so a
        # successful standalone write propagates even when run_daily skipped D-G70.
        bump_g70_latest_pointer(
            out,
            args.date,
            peer_roots=None if args.write_web else [],
        )
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
    print_card(payload)
    print("\nwrote", out)
    if args.write_web and not args.l5_eq_5:
        write_web(payload, board=board)
        if args.publish_live:
            rc = publish_live(args.date)
            if rc != 0:
                return rc
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
