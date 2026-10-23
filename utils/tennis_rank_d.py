"""ATP/WTA ranks for tennis D: opponent rank → Elite–Weak; player rank is context.

Graded tennis rows often have a blank def_tier. Sackmann match files carry
winner_rank / loser_rank on the match, which is the same mapping step8 uses
(Sports/Tennis/scripts/tennis_shared.atp_wta_def_tier).
"""
from __future__ import annotations

import csv
import re
import unicodedata
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

# Keep in lockstep with tennis_shared.atp_wta_def_tier.
ATP_ELITE_MAX = 10
ATP_ABOVE_AVG_MAX = 25
ATP_AVG_MAX = 50
ATP_BELOW_AVG_MAX = 100
UNRANKED_OUTSIDE_TOP150 = 250


def norm_key(s: object) -> str:
    if not s:
        return ""
    t = unicodedata.normalize("NFKD", str(s))
    t = "".join(c for c in t if not unicodedata.combining(c))
    t = re.sub(r"\s+", " ", t.lower().strip())
    t = re.sub(r"[^a-z0-9 ]+", "", t)
    return t


def norm_key_candidates(s: object) -> list[str]:
    k = norm_key(s)
    if not k:
        return []
    out = [k]
    parts = k.split()
    if len(parts) >= 2:
        flipped = " ".join([parts[-1], *parts[:-1]])
        if flipped not in out:
            out.append(flipped)
    return out


def parse_int_rank(v: object) -> int | None:
    try:
        n = int(float(v))
    except (TypeError, ValueError):
        return None
    if n <= 0 or n >= 900:
        return None
    return n


def rank_to_tier(rank: object) -> str:
    """Map individual ATP/WTA rank to Elite→Weak. Blank if unknown."""
    v = parse_int_rank(rank)
    if v is None:
        return ""
    if v <= ATP_ELITE_MAX:
        return "Elite"
    if v <= ATP_ABOVE_AVG_MAX:
        return "Above Avg"
    if v <= ATP_AVG_MAX:
        return "Avg"
    if v <= ATP_BELOW_AVG_MAX:
        return "Below Avg"
    return "Weak"


def _parse_tourney_date(val: object) -> date | None:
    digits = "".join(ch for ch in str(val or "") if ch.isdigit())[:8]
    if len(digits) != 8:
        return None
    try:
        return date(int(digits[:4]), int(digits[4:6]), int(digits[6:8]))
    except ValueError:
        return None


@dataclass(frozen=True)
class TennisMatchRanks:
    player_rank: int | None
    opp_rank: int | None
    player_tier: str
    opp_tier: str
    tourney_date: date | None = None


class TennisOppRankIndex:
    """player_key → (tourney_date, player_rank, opp_rank) from Sackmann CSVs."""

    def __init__(self, by_pk: dict[str, list[tuple[date, int | None, int | None]]]):
        self.by_pk = by_pk

    @classmethod
    def from_repo(cls, repo: Path | None = None) -> "TennisOppRankIndex":
        root = repo or Path(__file__).resolve().parents[1]
        sack = root / "Sports" / "Tennis" / "data" / "sackmann"
        files = []
        for tour in ("atp", "wta"):
            for year in (2026, 2025, 2024):
                p = sack / f"{tour}_matches_{year}.csv"
                if p.is_file():
                    files.append(p)
        by_pk: dict[str, list[tuple[date, int | None, int | None]]] = {}
        for path in files:
            with path.open(encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                for row in reader:
                    dt = _parse_tourney_date(row.get("tourney_date"))
                    if dt is None:
                        continue
                    w_name = row.get("winner_name") or ""
                    l_name = row.get("loser_name") or ""
                    w_rk = parse_int_rank(row.get("winner_rank"))
                    l_rk = parse_int_rank(row.get("loser_rank"))
                    for name, mine, opp in ((w_name, w_rk, l_rk), (l_name, l_rk, w_rk)):
                        pk = norm_key(name)
                        if not pk:
                            continue
                        by_pk.setdefault(pk, []).append((dt, mine, opp))
        for pk, rows in by_pk.items():
            rows.sort(key=lambda t: t[0], reverse=True)
        return cls(by_pk)

    def lookup(
        self,
        player: object,
        slate: date,
        *,
        window_before: int = 18,
        window_after: int = 2,
    ) -> TennisMatchRanks | None:
        lo = slate - timedelta(days=int(window_before))
        hi = slate + timedelta(days=int(window_after))
        cands: list[tuple[date, int | None, int | None]] = []
        seen: set[tuple[date, int | None, int | None]] = set()
        for pk in norm_key_candidates(player):
            for rec in self.by_pk.get(pk) or []:
                dt, mine, opp = rec
                if dt < lo or dt > hi:
                    continue
                if rec in seen:
                    continue
                seen.add(rec)
                cands.append(rec)
        if not cands:
            return None
        dt, mine, opp = min(cands, key=lambda t: (abs((t[0] - slate).days), -t[0].toordinal()))
        return TennisMatchRanks(
            player_rank=mine,
            opp_rank=opp,
            player_tier=rank_to_tier(mine),
            opp_tier=rank_to_tier(opp),
            tourney_date=dt,
        )

    def last_known_player_rank(
        self,
        player: object,
        slate: date,
        *,
        max_age_days: int = 120,
    ) -> tuple[int | None, date | None]:
        """Most recent Sackmann rank for *player* with tourney_date < slate."""
        lo = slate - timedelta(days=int(max_age_days))
        best: tuple[date, int | None] | None = None
        for pk in norm_key_candidates(player):
            for dt, mine, _opp in self.by_pk.get(pk) or []:
                if dt >= slate or dt < lo or mine is None:
                    continue
                if best is None or dt > best[0]:
                    best = (dt, mine)
        if best is None:
            return None, None
        return best[1], best[0]

    def lookup_entering_ranks(
        self,
        player: object,
        opponent: object,
        slate: date,
        *,
        max_age_days: int = 120,
    ) -> TennisMatchRanks:
        """Pre-slate ranks for player and opponent (independent last-known).

        Prefer this when Sackmann match files lag the graded slate window —
        match-window ``lookup`` returns None once coverage ends.
        """
        # Prefer same-match window when available (true matchup ranks).
        match = self.lookup(player, slate)
        if match is not None and (match.player_rank is not None or match.opp_rank is not None):
            pr, pr_dt = match.player_rank, match.tourney_date
            ork, or_dt = match.opp_rank, match.tourney_date
            if pr is None:
                pr, pr_dt = self.last_known_player_rank(player, slate, max_age_days=max_age_days)
            if ork is None:
                ork, or_dt = self.last_known_player_rank(opponent, slate, max_age_days=max_age_days)
            return TennisMatchRanks(
                player_rank=pr,
                opp_rank=ork,
                player_tier=rank_to_tier(pr),
                opp_tier=rank_to_tier(ork),
                tourney_date=pr_dt or or_dt,
            )
        pr, pr_dt = self.last_known_player_rank(player, slate, max_age_days=max_age_days)
        ork, or_dt = self.last_known_player_rank(opponent, slate, max_age_days=max_age_days)
        return TennisMatchRanks(
            player_rank=pr,
            opp_rank=ork,
            player_tier=rank_to_tier(pr),
            opp_tier=rank_to_tier(ork),
            tourney_date=pr_dt or or_dt,
        )


def row_rank_fields(row: dict) -> tuple[int | None, int | None]:
    """Player / opponent ATP-WTA ranks already on the graded or step8 row."""
    player_rk = None
    opp_rk = None
    for k in ("player_atp_rank", "Player Rank", "player_rank"):
        v = parse_int_rank(row.get(k))
        if v is not None:
            player_rk = v
            break
    for k in ("opponent_rank", "Opponent Rank", "opp_rank", "opp_atp_rank"):
        v = parse_int_rank(row.get(k))
        if v is not None:
            opp_rk = v
            break
    return player_rk, opp_rk


def tennis_opp_tier(
    row: dict,
    *,
    slate: date | None = None,
    index: TennisOppRankIndex | None = None,
) -> str:
    """Opponent D tier for a tennis graded row. Blank if unknown."""
    _, opp_rk = row_rank_fields(row)
    if opp_rk is not None:
        return rank_to_tier(opp_rk)
    if index is None or slate is None:
        return ""
    rec = index.lookup(row.get("player") or row.get("Player"), slate)
    if rec is None:
        return ""
    return rec.opp_tier


__all__ = [
    "ATP_ABOVE_AVG_MAX",
    "ATP_AVG_MAX",
    "ATP_BELOW_AVG_MAX",
    "ATP_ELITE_MAX",
    "TennisMatchRanks",
    "TennisOppRankIndex",
    "rank_to_tier",
    "row_rank_fields",
    "tennis_opp_tier",
]
