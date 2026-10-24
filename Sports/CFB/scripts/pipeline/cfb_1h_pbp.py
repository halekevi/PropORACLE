"""First-half CFB player stats from ESPN drive PBP (periods 1–2 only).

Full-game boxscore yards/TDs are never copied. CFB1H L5 must use this parser.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

_PASS_COMPLETE = frozenset({"pass reception", "passing touchdown"})
_PASS_INC = frozenset({"pass incompletion"})
_PASS_INT = frozenset({"pass interception return", "interception"})
_RUSH = frozenset({"rush", "rushing touchdown", "run"})
_SACK = frozenset({"sack"})
_FG_GOOD = frozenset({"field goal good"})
_XP_GOOD = frozenset({"extra point good"})

_PASSER_RE = re.compile(
    r"#(\d+)\s+(?:[A-Z]\.\s*)?([A-Za-z'\-]+(?:\s+(?:Jr|Sr|II|III|IV)\.)?)\s+pass\s+"
    r"(complete|incomplete|intercepted)",
    re.I,
)
_SACKED_RE = re.compile(
    r"#(\d+)\s+(?:[A-Z]\.\s*)?([A-Za-z'\-]+(?:\s+(?:Jr|Sr|II|III|IV)\.)?)\s+sacked",
    re.I,
)
_RUSHER_RE = re.compile(
    r"#(\d+)\s+(?:[A-Z]\.\s*)?([A-Za-z'\-]+(?:\s+(?:Jr|Sr|II|III|IV)\.)?)\s+(?:run|rush)",
    re.I,
)
_TARGET_RE = re.compile(
    r"(?:to|intended for)\s+#(\d+)\s+(?:([A-Z])\.)?\s*([A-Za-z'\-]+)?",
    re.I,
)
_KICKER_FG_RE = re.compile(
    r"#(\d+)\s+(?:[A-Z]\.\s*)?([A-Za-z'\-]+(?:\s+(?:Jr|Sr|II|III|IV)\.)?)\s+field goal",
    re.I,
)
_XP_RE = re.compile(
    r"#(\d+)\s+(?:[A-Z]\.\s*)?([A-Za-z'\-]+(?:\s+(?:Jr|Sr|II|III|IV)\.)?)\s+kick attempt good",
    re.I,
)


def _norm_name(s: str) -> str:
    t = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii")
    t = t.lower().strip()
    t = re.sub(r"[^a-z0-9 ]+", " ", t)
    t = re.sub(r"\s+", " ", t).strip()
    t = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", t)
    return re.sub(r"\s+", " ", t).strip()


def _norm_jersey(v) -> str:
    s = str(v or "").strip()
    if not s or s.lower() in ("nan", "none"):
        return ""
    s = re.sub(r"^0+", "", s)
    return s or "0"


def play_period(play: dict) -> int:
    per = play.get("period")
    if isinstance(per, dict):
        try:
            return int(per.get("number") or 0)
        except (TypeError, ValueError):
            return 0
    try:
        return int(per or 0)
    except (TypeError, ValueError):
        return 0


def is_first_half_period(n: int) -> bool:
    return n in (1, 2)


def _yardage(play: dict) -> float:
    try:
        raw = play.get("statYardage")
        if raw is None or raw == "":
            return 0.0
        return float(raw)
    except (TypeError, ValueError):
        return 0.0


def _offense_id(play: dict) -> str:
    for tp in play.get("teamParticipants") or []:
        if str(tp.get("type") or "").strip().lower() == "offense":
            return str(tp.get("id") or "").strip()
    return ""


def _iter_plays(summary: dict) -> List[dict]:
    drives = summary.get("drives") or {}
    items: List[dict] = []
    if isinstance(drives, dict):
        prev = drives.get("previous") or []
        if isinstance(prev, list):
            items.extend(x for x in prev if isinstance(x, dict))
        cur = drives.get("current")
        if isinstance(cur, dict):
            items.append(cur)
        elif isinstance(cur, list):
            items.extend(x for x in cur if isinstance(x, dict))
    out: List[dict] = []
    for dr in items:
        for p in dr.get("plays") or []:
            if isinstance(p, dict) and not p.get("isPenalty"):
                out.append(p)
    return out


def _roster_from_boxscore(summary: dict, game_date: str, event_id: str) -> Dict[str, dict]:
    """Identity-only roster (no full-game counting stats)."""
    box = summary.get("boxscore", {}) or {}
    by_ath: Dict[str, dict] = {}
    for tb in box.get("players", []) or []:
        team_id = str((tb.get("team") or {}).get("id", "")).strip()
        if not team_id:
            continue
        for grp in tb.get("statistics", []) or []:
            if not isinstance(grp, dict):
                continue
            for a in grp.get("athletes") or []:
                ath = (a.get("athlete") or {}) if isinstance(a, dict) else {}
                aid = str(ath.get("id", "")).strip()
                pn = _norm_name(ath.get("displayName") or ath.get("fullName") or "")
                if not pn:
                    continue
                key = aid or f"{pn}|{team_id}"
                row = by_ath.setdefault(
                    key,
                    {
                        "team_id": team_id,
                        "player_norm": pn,
                        "espn_athlete_id": aid,
                        "game_date": game_date,
                        "event_id": event_id,
                        "segment": "1H",
                    },
                )
                jer = _norm_jersey(ath.get("jersey"))
                if jer and not row.get("jersey"):
                    row["jersey"] = jer
    return by_ath


def _bump(row: dict, key: str, amt: float) -> None:
    try:
        row[key] = float(row.get(key) or 0) + float(amt)
    except (TypeError, ValueError):
        row[key] = float(amt)


def _max_stat(row: dict, key: str, amt: float) -> None:
    prev = row.get(key)
    try:
        prev_f = float(prev) if prev not in (None, "") else None
    except (TypeError, ValueError):
        prev_f = None
    if prev_f is None or amt > prev_f:
        row[key] = float(amt)


def _row_for_jersey(by_ath: Dict[str, dict], jersey_index: dict, team_id: str, jersey: str) -> Optional[dict]:
    if not jersey:
        return None
    keys = jersey_index.get((team_id, jersey)) or []
    if len(keys) == 1:
        return by_ath.get(keys[0])
    if not team_id:
        # unique jersey in the game
        hits = [k for (t, j), ks in jersey_index.items() if j == jersey for k in ks]
        hits = list(dict.fromkeys(hits))
        if len(hits) == 1:
            return by_ath.get(hits[0])
    return None


def parse_1h_players(summary: dict, game_date: str = "", event_id: str = "") -> List[dict]:
    """Player rows with Q1+Q2 counting stats only."""
    if not game_date:
        hdr = summary.get("header", {}) or {}
        comps = hdr.get("competitions", [{}])
        game_date = str(comps[0].get("date", "") if comps else "")[:10].replace("-", "")

    by_ath = _roster_from_boxscore(summary, game_date, event_id)
    jersey_index: Dict[Tuple[str, str], List[str]] = {}
    for key, row in by_ath.items():
        jer = _norm_jersey(row.get("jersey"))
        tid = str(row.get("team_id") or "").strip()
        if jer:
            jersey_index.setdefault((tid, jer), []).append(key)

    for play in _iter_plays(summary):
        if not is_first_half_period(play_period(play)):
            continue
        typ = str((play.get("type") or {}).get("text") or "").strip().lower()
        text = str(play.get("text") or "")
        yds = _yardage(play)
        off = _offense_id(play)

        if typ in _PASS_COMPLETE or typ in _PASS_INC or typ in _PASS_INT:
            m = _PASSER_RE.search(text)
            if m:
                qb = _row_for_jersey(by_ath, jersey_index, off, _norm_jersey(m.group(1)))
                if qb is not None:
                    _bump(qb, "PASS_ATT", 1.0)
                    if typ in _PASS_COMPLETE:
                        _bump(qb, "PASS_CMP", 1.0)
                        _bump(qb, "PASS_YDS", yds)
                        _max_stat(qb, "PASS_LONG", yds)
                        if typ == "passing touchdown" or "touchdown" in text.lower():
                            _bump(qb, "PASS_TD", 1.0)
                    if typ in _PASS_INT:
                        _bump(qb, "PASS_INT", 1.0)
            tm = _TARGET_RE.search(text)
            if tm:
                wr = _row_for_jersey(by_ath, jersey_index, off, _norm_jersey(tm.group(1)))
                if wr is not None:
                    _bump(wr, "REC_TGT", 1.0)
                    if typ in _PASS_COMPLETE:
                        _bump(wr, "REC", 1.0)
                        _bump(wr, "REC_YDS", yds)
                        _max_stat(wr, "REC_LONG", yds)
                        if typ == "passing touchdown" or "touchdown" in text.lower():
                            _bump(wr, "REC_TD", 1.0)
            continue

        if typ in _RUSH or (_RUSHER_RE.search(text) and "sack" not in text.lower()):
            m = _RUSHER_RE.search(text)
            if m:
                rb = _row_for_jersey(by_ath, jersey_index, off, _norm_jersey(m.group(1)))
                if rb is not None:
                    _bump(rb, "RUSH_ATT", 1.0)
                    _bump(rb, "RUSH_YDS", yds)
                    _max_stat(rb, "RUSH_LONG", yds)
                    if typ == "rushing touchdown" or (
                        "touchdown" in text.lower() and "pass" not in text.lower()
                    ):
                        _bump(rb, "RUSH_TD", 1.0)
            continue

        if typ in _SACK:
            m = _SACKED_RE.search(text)
            if m:
                qb = _row_for_jersey(by_ath, jersey_index, off, _norm_jersey(m.group(1)))
                if qb is not None:
                    _bump(qb, "SACK", 1.0)
            continue

        if typ in _FG_GOOD:
            m = _KICKER_FG_RE.search(text)
            if m:
                k = _row_for_jersey(by_ath, jersey_index, off, _norm_jersey(m.group(1)))
                if k is not None:
                    _bump(k, "KICK_FG", 1.0)
                    _bump(k, "KICK_PTS", 3.0)
            continue

        if typ in _XP_GOOD:
            m = _XP_RE.search(text) or _KICKER_FG_RE.search(text)
            if m:
                k = _row_for_jersey(by_ath, jersey_index, off, _norm_jersey(m.group(1)))
                if k is not None:
                    _bump(k, "KICK_XP", 1.0)
                    _bump(k, "KICK_PTS", 1.0)
            continue

        xp = _XP_RE.search(text)
        if xp and "kick attempt good" in text.lower():
            k = _row_for_jersey(by_ath, jersey_index, off, _norm_jersey(xp.group(1)))
            if k is not None:
                _bump(k, "KICK_XP", 1.0)
                _bump(k, "KICK_PTS", 1.0)

    _stat_keys = (
        "PASS_YDS", "PASS_TD", "PASS_CMP", "PASS_ATT", "PASS_INT", "PASS_LONG",
        "RUSH_YDS", "RUSH_TD", "RUSH_ATT", "RUSH_LONG",
        "REC_YDS", "REC", "REC_TD", "REC_LONG", "REC_TGT",
        "KICK_PTS", "KICK_FG", "KICK_XP", "SACK",
    )
    rows: List[dict] = []
    for row in by_ath.values():
        if not any(row.get(k) not in (None, "", 0, 0.0) for k in _stat_keys):
            continue
        row["MIN"] = 1.0
        row["segment"] = "1H"
        rows.append(row)
    return rows
