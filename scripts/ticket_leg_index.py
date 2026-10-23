#!/usr/bin/env python3
"""Build lookup sets from tickets_latest / shadow_tickets_latest JSON leg lists."""

from __future__ import annotations

import json
import os
import re
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[1]


def resolve_shadow_tickets_path(repo_root: Path | None = None) -> Path:
    """Prefer ui_runner/data/shadow_tickets_latest.json; fall back to templates/."""
    root = Path(repo_root) if repo_root else _REPO_ROOT
    data_path = root / "ui_runner" / "data" / "shadow_tickets_latest.json"
    if data_path.is_file():
        return data_path
    return root / "ui_runner" / "templates" / "shadow_tickets_latest.json"


def _norm_player(s: object) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


def _norm_prop(s: object) -> str:
    return re.sub(r"\s+", " ", str(s or "").strip().lower())


def _norm_dir(s: object) -> str:
    d = str(s or "").strip().upper()
    if d in ("UNDER", "U", "LOWER"):
        return "UNDER"
    return "OVER"


def _norm_line(s: object) -> str:
    try:
        return f"{float(s):.4f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(s or "").strip()


def leg_key_from_record(rec: dict[str, Any], *, sport_default: str = "") -> tuple[str, ...]:
    sp = str(rec.get("sport") or sport_default or "").strip().upper()
    player = _norm_player(rec.get("player") or rec.get("Player"))
    prop = _norm_prop(rec.get("prop_type") or rec.get("prop") or rec.get("Prop Type") or rec.get("Prop"))
    direction = _norm_dir(rec.get("direction") or rec.get("dir") or rec.get("Direction"))
    line = _norm_line(rec.get("line") or rec.get("Line"))
    pick = str(rec.get("pick_type") or rec.get("pick") or rec.get("Pick Type") or "Standard").strip().lower()
    pid = str(rec.get("pp_projection_id") or rec.get("projection_id") or "").strip()
    if pid:
        return ("id", pid)
    return (sp, player, prop, line, direction, pick)


def load_ticket_leg_keys(path: Path) -> set[tuple[str, ...]]:
    if not path.is_file():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    keys: set[tuple[str, ...]] = set()
    groups = data.get("groups") if isinstance(data, dict) else data
    if not isinstance(groups, list):
        return keys
    for grp in groups:
        if not isinstance(grp, dict):
            continue
        sport_default = str(grp.get("sport") or "").strip().upper()
        for ticket in grp.get("tickets") or []:
            if not isinstance(ticket, dict):
                continue
            for leg in ticket.get("legs") or []:
                if isinstance(leg, dict):
                    keys.add(leg_key_from_record(leg, sport_default=sport_default))
    return keys


def load_leg_key_to_ticket_id(path: Path) -> dict[tuple[str, ...], str]:
    """Map leg fingerprint -> parent ticket_id from tickets JSON."""
    if not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    out: dict[tuple[str, ...], str] = {}
    groups = data.get("groups") if isinstance(data, dict) else data
    if not isinstance(groups, list):
        return out
    for grp in groups:
        if not isinstance(grp, dict):
            continue
        sport_default = str(grp.get("sport") or "").strip().upper()
        for ticket in grp.get("tickets") or []:
            if not isinstance(ticket, dict):
                continue
            tid = str(ticket.get("ticket_id") or "").strip()
            if not tid:
                continue
            for leg in ticket.get("legs") or []:
                if isinstance(leg, dict):
                    out[leg_key_from_record(leg, sport_default=sport_default)] = tid
    return out


def resolve_ticket_id_for_row(
    row: dict[str, Any],
    sport: str,
    *,
    live_id_map: dict[tuple[str, ...], str] | None = None,
    shadow_id_map: dict[tuple[str, ...], str] | None = None,
) -> str | None:
    """Prefer explicit row ticket_id; else match live pool then shadow."""
    raw = row.get("ticket_id") or row.get("Ticket ID") or row.get("ticketId")
    if raw is not None and str(raw).strip() not in ("", "nan", "none", "null"):
        return str(raw).strip()
    key = leg_key_from_record(row, sport_default=sport)
    live_id_map = live_id_map or {}
    shadow_id_map = shadow_id_map or {}
    if key in live_id_map:
        return live_id_map[key]
    if key[0] != "id":
        sp, player, prop, line, direction = key[0], key[1], key[2], key[3], key[4]
        for m in (live_id_map, shadow_id_map):
            for kk, tid in m.items():
                if kk[0] == "id":
                    continue
                if kk[:5] == (sp, player, prop, line, direction):
                    return tid
    if key in shadow_id_map:
        return shadow_id_map[key]
    return None


def prop_matches_ticket_keys(row: dict[str, Any], sport: str, keys: set[tuple[str, ...]]) -> bool:
    if not keys:
        return False
    k = leg_key_from_record(row, sport_default=sport)
    if k in keys:
        return True
    if k[0] == "id":
        return False
    sp, player, prop, line, direction = k[0], k[1], k[2], k[3], k[4]
    for kk in keys:
        if kk[0] == "id":
            continue
        if kk[:5] == (sp, player, prop, line, direction):
            return True
    return False


def attach_ticket_ids_to_dataframe(
    df: pd.DataFrame,
    *,
    live_json: Path | None = None,
    shadow_json: Path | None = None,
) -> pd.DataFrame:
    """Add ticket_id column from tickets_latest.json leg index (null when not on a ticket)."""
    if df is None or df.empty:
        return df
    out = df.copy()
    live_map = load_leg_key_to_ticket_id(live_json) if live_json else {}
    shadow_map = load_leg_key_to_ticket_id(shadow_json) if shadow_json else {}
    if "ticket_id" not in out.columns:
        out["ticket_id"] = None
    for idx, row in out.iterrows():
        if pd.notna(out.at[idx, "ticket_id"]) and str(out.at[idx, "ticket_id"]).strip():
            continue
        sport = str(row.get("sport") or row.get("Sport") or "").strip()
        tid = resolve_ticket_id_for_row(
            row.to_dict() if hasattr(row, "to_dict") else dict(row),
            sport,
            live_id_map=live_map,
            shadow_id_map=shadow_map,
        )
        out.at[idx, "ticket_id"] = tid
    return out


def ticket_search_roots(repo: Path | None = None) -> list[Path]:
    """This checkout plus sibling live tree / env extra roots."""
    root = Path(repo) if repo else _REPO_ROOT
    out: list[Path] = [root]
    extra = os.environ.get("PROPORACLE_TICKET_RUN_ROOTS") or ""
    for part in extra.replace(",", ";").split(";"):
        raw = part.strip()
        if not raw:
            continue
        p = Path(raw)
        if p.is_dir():
            out.append(p)
    if root.name.lower().startswith("proporacle"):
        sibling = root.parent / "PropORACLE_main_cp"
        try:
            if sibling.is_dir() and sibling.resolve() != root.resolve():
                out.append(sibling)
        except OSError:
            pass
    seen: set[str] = set()
    uniq: list[Path] = []
    for p in out:
        try:
            key = str(p.resolve())
        except OSError:
            key = str(p)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)
    return uniq


def _payload_date(data: dict) -> str:
    for k in ("date", "slate_date", "tennis_date"):
        v = str(data.get(k) or "").strip()[:10]
        if len(v) == 10 and v[4] == "-":
            return v
    return ""


def iter_ticket_json_paths(date_str: str, repo: Path | None = None) -> list[Path]:
    ds = str(date_str or "")[:10]
    paths: list[Path] = []
    for root in ticket_search_roots(repo):
        runs = root / "ui_runner" / "data" / "ticket_runs" / ds
        if runs.is_dir():
            paths.extend(sorted(p for p in runs.glob("*/tickets.json") if p.is_file()))
        pool = root / "ui_runner" / "data" / f"combined_slate_tickets_{ds}.json"
        if pool.is_file():
            paths.append(pool)
        for live in (
            root / "ui_runner" / "runtime" / "tickets_latest.json",
            root / "ui_runner" / "templates" / "tickets_latest.json",
            root / "ui_runner" / "data" / "tickets_latest.json",
        ):
            if live.is_file():
                paths.append(live)
    seen: set[str] = set()
    uniq: list[Path] = []
    for p in paths:
        try:
            key = str(p.resolve())
        except OSError:
            key = str(p)
        if key in seen:
            continue
        seen.add(key)
        uniq.append(p)
    return uniq


def iter_ticket_legs(
    date_str: str,
    *,
    sport: str = "",
    repo: Path | None = None,
) -> list[dict[str, Any]]:
    """Every unique ticketed face for a slate date (board snapshot ∪ archived runs)."""
    want = str(sport or "").strip().upper()
    if want == "SOCCER":
        want = "SOCCER"
    legs: list[dict[str, Any]] = []
    seen: set[tuple[str, ...]] = set()
    for path in iter_ticket_json_paths(date_str, repo):
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        payload_date = _payload_date(data)
        if payload_date and payload_date != str(date_str)[:10] and "ticket_runs" not in str(path).replace("\\", "/"):
            # Live tickets_latest for another day — skip.
            if path.name == "tickets_latest.json":
                continue
        groups = data.get("groups") if isinstance(data.get("groups"), list) else []
        for grp in groups:
            if not isinstance(grp, dict):
                continue
            sport_default = str(grp.get("sport") or "").strip().upper()
            for ticket in grp.get("tickets") or []:
                if not isinstance(ticket, dict):
                    continue
                tid = str(ticket.get("ticket_id") or "").strip()
                for leg in ticket.get("legs") or []:
                    if not isinstance(leg, dict):
                        continue
                    rec = dict(leg)
                    rec.setdefault("ticket_id", tid)
                    rec.setdefault("sport", rec.get("sport") or sport_default)
                    sp = str(rec.get("sport") or "").strip().upper()
                    if want and sp and sp != want:
                        continue
                    key = leg_key_from_record(rec, sport_default=want or sport_default)
                    if key in seen:
                        continue
                    seen.add(key)
                    legs.append(rec)
    return legs


def _set_row_aliases(row: dict[str, Any], columns: Iterable[str], names: tuple[str, ...], value: Any) -> None:
    cols = set(columns)
    for name in names:
        if name in cols:
            row[name] = value


def union_ticket_faces_into_slate(
    df: pd.DataFrame,
    *,
    date_str: str,
    sport: str,
    repo: Path | None = None,
) -> pd.DataFrame:
    """Append ticketed faces that are missing from the board snapshot.

    Actuals are player+prop; any line/pick on that face can be graded once
    the box is in. This closes Standard faces (tennis Games Won, soccer Saves)
    and August snapshot-timing zeros together.
    """
    if df is None:
        df = pd.DataFrame()
    out = df.copy()
    want = str(sport or "").strip().upper()
    ticket_legs = iter_ticket_legs(date_str, sport=want, repo=repo)
    if not ticket_legs:
        return out
    existing: set[tuple[str, ...]] = set()
    for _, row in out.iterrows():
        rec = row.to_dict() if hasattr(row, "to_dict") else dict(row)
        existing.add(leg_key_from_record(rec, sport_default=want))
    cols = list(out.columns) if len(out.columns) else [
        "player",
        "prop_type_norm",
        "prop_type",
        "line",
        "bet_direction",
        "direction",
        "pick_type",
        "sport",
        "ticket_id",
        "from_ticket_face",
    ]
    if "from_ticket_face" not in cols:
        out["from_ticket_face"] = 0
        cols = list(out.columns)
    added_rows: list[dict[str, Any]] = []
    for rec in ticket_legs:
        key = leg_key_from_record(rec, sport_default=want)
        if key in existing:
            continue
        existing.add(key)
        new_row = {c: None for c in cols}
        player = rec.get("player") or rec.get("Player") or ""
        prop = rec.get("prop_type") or rec.get("prop") or rec.get("Prop") or ""
        line = rec.get("line") if rec.get("line") is not None else rec.get("Line")
        direction = rec.get("direction") or rec.get("dir") or rec.get("Direction") or "OVER"
        pick = rec.get("pick_type") or rec.get("Pick Type") or "standard"
        _set_row_aliases(new_row, cols, ("player", "Player"), player)
        _set_row_aliases(new_row, cols, ("prop_type_norm", "prop_type", "Prop", "prop"), prop)
        _set_row_aliases(new_row, cols, ("line", "Line"), line)
        _set_row_aliases(new_row, cols, ("bet_direction", "final_bet_direction", "direction", "Direction"), direction)
        _set_row_aliases(new_row, cols, ("pick_type", "Pick Type"), pick)
        _set_row_aliases(new_row, cols, ("sport", "Sport"), rec.get("sport") or want)
        _set_row_aliases(new_row, cols, ("ticket_id",), rec.get("ticket_id") or None)
        _set_row_aliases(new_row, cols, ("team", "Team"), rec.get("team") or rec.get("Team") or "")
        _set_row_aliases(new_row, cols, ("opp_team", "Opp"), rec.get("opp_team") or rec.get("Opp") or "")
        _set_row_aliases(new_row, cols, ("league", "League"), rec.get("league") or rec.get("League") or "")
        _set_row_aliases(
            new_row,
            cols,
            ("pp_projection_id", "projection_id"),
            rec.get("pp_projection_id") or rec.get("projection_id") or "",
        )
        _set_row_aliases(new_row, cols, ("game_date", "Game Date"), date_str)
        if "from_ticket_face" in cols:
            new_row["from_ticket_face"] = 1
        added_rows.append(new_row)
    if added_rows:
        extra = pd.DataFrame(added_rows)
        out = pd.concat([out, extra], ignore_index=True)
        print(f"  [ticket-face] union appended {len(added_rows)} ticketed face(s) not on the board snapshot")
    return out


def dates_with_tickets_missing_runs(repo: Path | None = None) -> list[str]:
    """Slate dates that have a tickets JSON but no ticket_runs archive in any search root."""
    missing: list[str] = []
    dated: set[str] = set()
    run_dates: set[str] = set()
    for root in ticket_search_roots(repo):
        runs = root / "ui_runner" / "data" / "ticket_runs"
        if runs.is_dir():
            for d in runs.iterdir():
                if d.is_dir() and len(d.name) == 10 and d.name[4] == "-":
                    if any(p.is_file() for p in d.glob("*/tickets.json")):
                        run_dates.add(d.name)
        data = root / "ui_runner" / "data"
        if data.is_dir():
            for path in data.glob("combined_slate_tickets_*.json"):
                stamp = path.stem.replace("combined_slate_tickets_", "")[:10]
                if len(stamp) == 10 and stamp[4] == "-":
                    dated.add(stamp)
        for live in (
            root / "ui_runner" / "runtime" / "tickets_latest.json",
            root / "ui_runner" / "templates" / "tickets_latest.json",
            root / "ui_runner" / "data" / "tickets_latest.json",
        ):
            if not live.is_file():
                continue
            try:
                payload = json.loads(live.read_text(encoding="utf-8"))
            except Exception:
                continue
            ds = _payload_date(payload) if isinstance(payload, dict) else ""
            if ds:
                dated.add(ds)
    for ds in sorted(dated):
        if ds not in run_dates:
            missing.append(ds)
    return missing
