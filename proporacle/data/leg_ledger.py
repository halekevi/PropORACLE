"""Local legs / features / tickets ledger (Parquet + optional DuckDB).

Railway does not need this. Graded JSON and ticket_runs stay the producers.
This is the analysis hop so backtests do not scrape ticket_eval HTML.

``leg_id_v1`` hashes canonical fields only (ET slate date, sport, folded player,
canon prop, fixed-decimal line, direction, pick type). Projection id is stored
as a column, never as the key. Version the function when aliases change.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

from proporacle.data.table_io import parquet_available, write_props_parquet
from utils.eastern_slate_date import eastern_slate_date
from utils.prop_norm import canon_prop

_REPO = Path(__file__).resolve().parents[2]
_SCRIPTS = _REPO / "scripts"
if str(_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS))
from player_name_norm import fold_player_name  # noqa: E402

LEDGER_ROOT = _REPO / "data" / "ledger"
GRADE_POOL_GLOB = "combined_slate_tickets_*.json"
LEG_ID_VERSION = "v1"
_DATE_ONLY = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Fill columns that should not page as alarms.
EXPECTED_FILL_GAPS = {
    "usage_pct": "expected_gap",
    "league": "expected_todo",
}
EXPECTED_SPORT_GAPS = {
    ("ml_prob", "CFB"): "expected_gap",
    ("ml_prob", "NFL"): "expected_gap",
}
USAGE_REQUIRED_SPORTS = frozenset({"NBA", "WNBA", "NBA1H", "NBA1Q"})
# abs(edge) / max(|line|, 1) — Goblin Points cover of ~11 on a 3.5 line is ~3.2.
# NFL 38–150 on a tackle-sized line is 7–25+.
EDGE_RATIO_FLAG = 5.0

_FEATURE_TYPED = (
    "ml_prob",
    "edge",
    "hit_rate",
    "l5_over",
    "l5_under",
    "l10_over",
    "l10_under",
    "l10_games_played",
    "usage_pct",
    "game_total",
    "game_spread",
    "team_win_prob",
    "line_vegas",
    "vegas_gap",
    "rest_days",
    "proj_mu",
    "proj_edge",
)
_FEATURE_TEXT = (
    "tier",
    "def_tier",
    "stat_def_tier",
    "minutes_tier",
    "role_tier",
    "usage_tier",
    "l10_streak",
    "consistency_grade",
    "confidence_tier",
    "line_provider",
)
_LEG_SKIP_EXTRA = frozenset(
    {
        "sport",
        "player",
        "team",
        "opp_team",
        "prop",
        "prop_type",
        "line",
        "direction",
        "over_under",
        "pick_type",
        "pp_projection_id",
        "projection_id",
        "league",
        "result",
        "hit",
        "actual_value",
        "void_reason",
        "ticket_id",
        "on_ticket",
        "on_shadow_ticket",
        "graded_at",
        "margin",
        *_FEATURE_TYPED,
        *_FEATURE_TEXT,
    }
)


def _blank(v: object) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in ("", "nan", "none", "null", "nat", "<na>"):
        return ""
    return s


def _to_float(v: object) -> float | None:
    s = _blank(v)
    if not s:
        return None
    try:
        return float(s)
    except (TypeError, ValueError):
        return None


def _to_int01(v: object) -> int | None:
    if v is True or v == 1 or str(v).strip() in ("1", "true", "True"):
        return 1
    if v is False or v == 0 or str(v).strip() in ("0", "false", "False"):
        return 0
    return None


def _norm_dir(s: object) -> str:
    d = str(s or "").strip().upper()
    if d in ("UNDER", "U", "LOWER"):
        return "UNDER"
    if d in ("OVER", "O", "HIGHER"):
        return "OVER"
    return d


def _norm_pick(s: object) -> str:
    p = str(s or "standard").strip().lower()
    if p in ("u", "under"):
        return "standard"
    if "goblin" in p:
        return "goblin"
    if "demon" in p:
        return "demon"
    if "standard" in p or p in ("", "std"):
        return "standard"
    return p or "standard"


def _line_norm(s: object) -> str:
    """Fixed decimal without trailing zeros: 12.5 never 12.50."""
    try:
        return f"{float(s):.4f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return str(s or "").strip()


def _sport_norm(s: object) -> str:
    return _blank(s).upper()


def _player_fold(s: object) -> str:
    return fold_player_name(s)


def _prop_canon(sport: object, prop: object) -> str:
    hit = canon_prop(sport, prop)
    return hit or re.sub(r"\s+", " ", str(prop or "").strip().lower())


def _slate_date_et(raw: object, fallback: object = "") -> str:
    s = _blank(raw)
    if _DATE_ONLY.fullmatch(s):
        return s
    et = eastern_slate_date(raw)
    if et:
        return et
    fb = _blank(fallback)
    if _DATE_ONLY.fullmatch(fb):
        return fb
    return eastern_slate_date(fallback) or fb[:10]


def canon_components(
    *,
    slate_date: str,
    sport: str,
    player: str,
    prop: str,
    line: object,
    direction: str,
    pick_type: str,
) -> dict[str, str]:
    sport_n = _sport_norm(sport)
    return {
        "leg_id_version": LEG_ID_VERSION,
        "slate_date_et": _slate_date_et(slate_date),
        "sport": sport_n,
        "player_fold": _player_fold(player),
        "prop_canon": _prop_canon(sport_n, prop),
        "line_norm": _line_norm(line),
        "direction_norm": _norm_dir(direction),
        "pick_norm": _norm_pick(pick_type),
    }


def leg_id_v1(
    *,
    slate_date: str,
    sport: str,
    player: str,
    prop: str,
    line: object,
    direction: str,
    pick_type: str,
    pp_projection_id: str = "",  # stored on the row, not hashed
) -> str:
    """v1 fingerprint. Projection id is ignored so blank vs filled ids do not split a face."""
    del pp_projection_id
    c = canon_components(
        slate_date=slate_date,
        sport=sport,
        player=player,
        prop=prop,
        line=line,
        direction=direction,
        pick_type=pick_type,
    )
    parts = "|".join(
        [
            LEG_ID_VERSION,
            c["slate_date_et"],
            c["sport"],
            c["player_fold"],
            c["prop_canon"],
            c["line_norm"],
            c["direction_norm"],
            c["pick_norm"],
        ]
    )
    digest = hashlib.sha1(parts.encode("utf-8")).hexdigest()[:16]
    return f"{LEG_ID_VERSION}:{digest}"


leg_id_for = leg_id_v1


def _norm_result(v: object) -> str:
    s = _blank(v).upper()
    if s in ("HIT", "WIN", "W"):
        return "HIT"
    if s in ("MISS", "LOSS", "L"):
        return "MISS"
    if s in ("PUSH", "TIE"):
        return "PUSH"
    if s in ("VOID", "DNP", "CANCEL"):
        return "VOID"
    if not s:
        return "PENDING"
    return s


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def ledger_root(root: Path | None = None) -> Path:
    return Path(root) if root else LEDGER_ROOT


def _part_path(table: str, slate_date: str, sha256: str, root: Path | None = None) -> Path:
    d = ledger_root(root) / table / f"slate_date={slate_date[:10]}"
    d.mkdir(parents=True, exist_ok=True)
    return d / f"sha={sha256[:16]}.parquet"


def _write_part(
    df: pd.DataFrame, table: str, slate_date: str, sha256: str, root: Path | None = None
) -> Path:
    path = _part_path(table, slate_date, sha256, root)
    if df.empty:
        return path
    if path.is_file():
        return path
    if not parquet_available():
        raise RuntimeError("pyarrow/fastparquet required to write the leg ledger")
    write_props_parquet(df, path)
    return path


def _part_exists(table: str, slate_date: str, sha256: str, root: Path | None = None) -> bool:
    return _part_path(table, slate_date, sha256, root).is_file()


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def graded_json_paths(repo: Path | None = None, date: str | None = None) -> list[Path]:
    repo = repo or _REPO
    seen: dict[str, Path] = {}
    for folder in (repo / "ui_runner" / "templates", repo / "ui_runner" / "data"):
        if not folder.is_dir():
            continue
        for path in sorted(folder.glob("graded_props_*.json")):
            stamp = path.stem.replace("graded_props_", "")[:10]
            if date and stamp != date[:10]:
                continue
            seen[stamp] = path
    return [seen[k] for k in sorted(seen)]


def _prop_from_row(row: dict[str, Any]) -> str:
    return _blank(row.get("prop") or row.get("prop_type") or row.get("Prop Type") or row.get("Prop"))


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _source_meta(path: str, sha256: str) -> dict[str, str]:
    return {
        "source_filename": Path(path).name if path else "",
        "source_sha256": sha256,
        "ingested_at": _now_iso(),
        "source_path": path,
    }


def rows_from_graded_payload(
    payload: dict[str, Any],
    *,
    source_path: str = "",
    source_sha256: str = "",
) -> tuple[list[dict], list[dict]]:
    slate_date = _slate_date_et(payload.get("date") or Path(source_path).stem.replace("graded_props_", ""))
    meta = _source_meta(source_path, source_sha256)
    legs: list[dict] = []
    feats: list[dict] = []
    for row in payload.get("props") or []:
        if not isinstance(row, dict):
            continue
        player = _blank(row.get("player") or row.get("Player"))
        prop = _prop_from_row(row)
        if not player or not prop:
            continue
        sport = _sport_norm(row.get("sport"))
        direction = _norm_dir(row.get("direction") or row.get("over_under") or row.get("dir"))
        pick_type = _norm_pick(row.get("pick_type"))
        pid = _blank(row.get("pp_projection_id") or row.get("projection_id"))
        comps = canon_components(
            slate_date=slate_date,
            sport=sport,
            player=player,
            prop=prop,
            line=row.get("line"),
            direction=direction,
            pick_type=pick_type,
        )
        lid = leg_id_v1(
            slate_date=slate_date,
            sport=sport,
            player=player,
            prop=prop,
            line=row.get("line"),
            direction=direction,
            pick_type=pick_type,
        )
        result = _norm_result(row.get("result"))
        hit = _to_int01(row.get("hit"))
        if hit is None and result == "HIT":
            hit = 1
        if hit is None and result == "MISS":
            hit = 0
        legs.append(
            {
                "leg_id": lid,
                **comps,
                "slate_date": comps["slate_date_et"],
                "league": _blank(row.get("league")),
                "player": player,
                "team": _blank(row.get("team")),
                "opp_team": _blank(row.get("opp_team")),
                "prop": prop,
                "line": _to_float(row.get("line")),
                "direction": comps["direction_norm"],
                "pick_type": comps["pick_norm"],
                "pp_projection_id": pid,
                "result": result,
                "hit": hit,
                "actual_value": _to_float(row.get("actual_value")),
                "void_reason": _blank(row.get("void_reason")),
                "on_ticket": int(bool(row.get("on_ticket"))),
                "on_shadow_ticket": int(bool(row.get("on_shadow_ticket"))),
                "ticket_id": _blank(row.get("ticket_id")) or None,
                "source": "graded_props_json",
                **meta,
            }
        )
        extra = {k: v for k, v in row.items() if k not in _LEG_SKIP_EXTRA and _blank(v)}
        feat = {
            "leg_id": lid,
            "slate_date": comps["slate_date_et"],
            "sport": sport,
            "extra_json": json.dumps(extra, ensure_ascii=False) if extra else None,
            **meta,
        }
        for col in _FEATURE_TYPED:
            feat[col] = _to_float(row.get(col))
        for col in _FEATURE_TEXT:
            feat[col] = _blank(row.get(col)) or None
        feats.append(feat)
    return legs, feats


def collision_groups(payload: dict[str, Any]) -> list[dict]:
    """Rows that share a v1 id but are not the same canonical bet."""
    legs, _ = rows_from_graded_payload(payload)
    by_id: dict[str, list[tuple]] = {}
    for r in legs:
        key = (
            r["slate_date_et"],
            r["sport"],
            r["player_fold"],
            r["prop_canon"],
            r["line_norm"],
            r["direction_norm"],
            r["pick_norm"],
        )
        by_id.setdefault(r["leg_id"], []).append(key)
    bad = []
    for lid, keys in by_id.items():
        uniq = set(keys)
        if len(uniq) > 1:
            bad.append({"leg_id": lid, "canonical_keys": sorted(uniq), "n": len(keys)})
    return bad


def ingest_graded_file(path: Path, *, root: Path | None = None) -> dict[str, Any]:
    sha = file_sha256(path)
    payload = _read_json(path)
    if not isinstance(payload, dict):
        return {"legs": 0, "features": 0, "skipped": False}
    slate_date = _slate_date_et(payload.get("date") or path.stem.replace("graded_props_", ""))
    if _part_exists("legs", slate_date, sha, root):
        return {"legs": 0, "features": 0, "slate_date": slate_date, "skipped": True, "source_sha256": sha}
    legs, feats = rows_from_graded_payload(payload, source_path=str(path), source_sha256=sha)
    _write_part(pd.DataFrame(legs), "legs", slate_date, sha, root)
    _write_part(pd.DataFrame(feats), "features", slate_date, sha, root)
    return {"legs": len(legs), "features": len(feats), "slate_date": slate_date, "skipped": False, "source_sha256": sha}


def _walk_ticket_groups(payload: dict[str, Any]) -> Iterable[tuple[str, dict, dict]]:
    groups = payload.get("groups") if isinstance(payload, dict) else payload
    if not isinstance(groups, list):
        return
    for grp in groups:
        if not isinstance(grp, dict):
            continue
        gname = str(grp.get("group_name") or grp.get("web_group_name") or "")
        for ticket in grp.get("tickets") or []:
            if isinstance(ticket, dict):
                yield gname, grp, ticket


def _power_flex(group_name: str, ticket: dict[str, Any]) -> str:
    blob = f"{group_name} {ticket.get('web_group_name') or ''} {ticket.get('mode') or ''}".lower()
    if "flex" in blob:
        return "Flex"
    if "power" in blob:
        return "Power"
    if ticket.get("flex_payout") and not ticket.get("power_payout"):
        return "Flex"
    return "Power"


def rows_from_tickets_payload(
    payload: dict[str, Any],
    *,
    source_path: str = "",
    run_id: str = "",
    published: int = 0,
    source_sha256: str = "",
) -> tuple[list[dict], list[dict]]:
    slate_date = _slate_date_et(payload.get("date") or "")
    meta = _source_meta(source_path, source_sha256)
    tickets: list[dict] = []
    tlegs: list[dict] = []
    seen: set[str] = set()
    for gname, _grp, ticket in _walk_ticket_groups(payload):
        tid = _blank(ticket.get("ticket_id"))
        if not tid:
            no = ticket.get("ticket_no") or ticket.get("n") or ""
            tid = f"{slate_date}|{gname}|{no}"
        if tid in seen:
            continue
        seen.add(tid)
        pay = ticket.get("payout") if isinstance(ticket.get("payout"), dict) else {}
        tickets.append(
            {
                "ticket_id": tid,
                "slate_date": slate_date,
                "run_id": run_id or _blank(payload.get("run_id")),
                "group_name": gname,
                "n_legs": int(ticket.get("n_legs") or len(ticket.get("legs") or []) or 0),
                "power_flex": _power_flex(gname, ticket),
                "published": int(published),
                "quote_power": _to_float(
                    pay.get("power") or ticket.get("power_payout") or ticket.get("display_min_x")
                ),
                "quote_flex": _to_float(pay.get("flex") or ticket.get("flex_payout")),
                "payout_source": _blank(pay.get("payout_source") or ticket.get("payout_source")),
                "ticket_track": _blank(ticket.get("ticket_track") or payload.get("ticket_track")),
                "result": _norm_result(ticket.get("result") or ticket.get("grade"))
                if _blank(ticket.get("result") or ticket.get("grade"))
                else None,
                "source": "ticket_json",
                **meta,
            }
        )
        for i, leg in enumerate(ticket.get("legs") or []):
            if not isinstance(leg, dict):
                continue
            sport = _sport_norm(leg.get("sport"))
            player = _blank(leg.get("player"))
            prop = _prop_from_row(leg) or _blank(leg.get("prop_type"))
            direction = _norm_dir(leg.get("direction") or leg.get("dir") or leg.get("over_under"))
            pick_type = _norm_pick(leg.get("pick_type"))
            comps = canon_components(
                slate_date=slate_date,
                sport=sport,
                player=player,
                prop=prop,
                line=leg.get("line"),
                direction=direction,
                pick_type=pick_type,
            )
            lid = leg_id_v1(
                slate_date=slate_date,
                sport=sport,
                player=player,
                prop=prop,
                line=leg.get("line"),
                direction=direction,
                pick_type=pick_type,
            )
            tlegs.append(
                {
                    "ticket_id": tid,
                    "leg_id": lid,
                    "slate_date": comps["slate_date_et"],
                    "pos": i,
                    "sport": sport,
                    "player": player,
                    **{k: comps[k] for k in ("player_fold", "prop_canon", "line_norm", "direction_norm", "pick_norm", "leg_id_version")},
                    "prop": prop,
                    "line": _to_float(leg.get("line")),
                    "direction": comps["direction_norm"],
                    "pick_type": comps["pick_norm"],
                    **meta,
                }
            )
    return tickets, tlegs


def ingest_tickets_file(
    path: Path,
    *,
    root: Path | None = None,
    run_id: str = "",
    published: int = 0,
) -> dict[str, Any]:
    sha = file_sha256(path)
    payload = _read_json(path)
    if not isinstance(payload, dict):
        return {"tickets": 0, "ticket_legs": 0, "skipped": False}
    slate_date = _slate_date_et(payload.get("date") or "")
    if not slate_date:
        return {"tickets": 0, "ticket_legs": 0, "skipped": False}
    if _part_exists("tickets", slate_date, sha, root):
        return {
            "tickets": 0,
            "ticket_legs": 0,
            "slate_date": slate_date,
            "skipped": True,
            "source_sha256": sha,
        }
    tickets, tlegs = rows_from_tickets_payload(
        payload, source_path=str(path), run_id=run_id, published=published, source_sha256=sha
    )
    _write_part(pd.DataFrame(tickets), "tickets", slate_date, sha, root)
    _write_part(pd.DataFrame(tlegs), "ticket_legs", slate_date, sha, root)
    return {
        "tickets": len(tickets),
        "ticket_legs": len(tlegs),
        "slate_date": slate_date,
        "skipped": False,
        "source_sha256": sha,
    }


def ticket_ingest_repos(repo: Path | None = None) -> list[Path]:
    """Ledger ticket sources: this checkout, sibling live tree, extra env roots."""
    base = Path(repo) if repo else _REPO
    out: list[Path] = [base]
    extra = os.environ.get("PROPORACLE_TICKET_RUN_ROOTS") or ""
    for part in extra.replace(",", ";").split(";"):
        raw = part.strip()
        if not raw:
            continue
        p = Path(raw)
        if p.is_dir():
            out.append(p)
    if base.name.lower().startswith("proporacle"):
        sibling = base.parent / "PropORACLE_main_cp"
        try:
            if sibling.is_dir() and sibling.resolve() != base.resolve():
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


def ingest_ticket_runs(repo: Path | None = None, *, date: str | None = None, root: Path | None = None) -> dict[str, int]:
    n_t = n_l = n_skip = 0
    for src in ticket_ingest_repos(repo):
        runs = src / "ui_runner" / "data" / "ticket_runs"
        if runs.is_dir():
            for day_dir in sorted(runs.iterdir()):
                if not day_dir.is_dir():
                    continue
                if date and day_dir.name != date[:10]:
                    continue
                for run_dir in sorted(day_dir.iterdir()):
                    tickets_path = run_dir / "tickets.json"
                    if tickets_path.is_file():
                        out = ingest_tickets_file(tickets_path, root=root, run_id=run_dir.name)
                        n_t += int(out.get("tickets") or 0)
                        n_l += int(out.get("ticket_legs") or 0)
                        n_skip += int(bool(out.get("skipped")))
        pool_dir = src / "ui_runner" / "data"
        if pool_dir.is_dir():
            for path in sorted(pool_dir.glob(GRADE_POOL_GLOB)):
                stamp = path.stem.replace("combined_slate_tickets_", "")[:10]
                if date and stamp != date[:10]:
                    continue
                if stamp == "latest" or not stamp[:4].isdigit():
                    continue
                out = ingest_tickets_file(path, root=root, run_id="grade_pool")
                n_t += int(out.get("tickets") or 0)
                n_l += int(out.get("ticket_legs") or 0)
                n_skip += int(bool(out.get("skipped")))
    return {"tickets": n_t, "ticket_legs": n_l, "skipped": n_skip}


def ingest_graded(repo: Path | None = None, *, date: str | None = None, root: Path | None = None) -> dict[str, int]:
    n_legs = n_feat = n_files = n_skip = 0
    for path in graded_json_paths(repo, date=date):
        out = ingest_graded_file(path, root=root)
        n_legs += int(out.get("legs") or 0)
        n_feat += int(out.get("features") or 0)
        n_files += 0 if out.get("skipped") else 1
        n_skip += int(bool(out.get("skipped")))
    return {"files": n_files, "legs": n_legs, "features": n_feat, "skipped": n_skip}


def _glob_parquet(table: str, root: Path | None = None) -> list[Path]:
    base = ledger_root(root) / table
    if not base.is_dir():
        return []
    return sorted(base.glob("slate_date=*/*.parquet"))


def read_table_df(table: str, root: Path | None = None) -> pd.DataFrame:
    frames = [pd.read_parquet(p) for p in _glob_parquet(table, root)]
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def current_legs(root: Path | None = None) -> pd.DataFrame:
    """Latest ingest per leg_id (append-only parts may include older hashes)."""
    df = read_table_df("legs", root)
    if df.empty:
        return df
    if "ingested_at" in df.columns:
        df = df.sort_values("ingested_at")
    return df.drop_duplicates(subset=["leg_id"], keep="last")


def connect(root: Path | None = None):
    try:
        import duckdb
    except ImportError:
        return None
    con = duckdb.connect(database=":memory:")
    base = ledger_root(root)
    for table in ("legs", "features", "tickets", "ticket_legs"):
        glob = str(base / table / "slate_date=*/*.parquet").replace("\\", "/")
        paths = _glob_parquet(table, root)
        if not paths:
            con.execute(f"CREATE VIEW {table} AS SELECT * FROM (SELECT 1 AS _empty) WHERE 1=0")
            continue
        con.execute(
            f"CREATE VIEW {table} AS SELECT * FROM read_parquet('{glob}', hive_partitioning=true)"
        )
    sql_path = Path(__file__).with_name("schema") / "leg_ledger.sql"
    if sql_path.is_file():
        text = sql_path.read_text(encoding="utf-8")
        for stmt in text.split(";"):
            stmt = stmt.strip()
            if stmt.upper().startswith("CREATE VIEW"):
                try:
                    con.execute(stmt)
                except Exception:
                    pass
    return con


def _edge_ratio_high(g: pd.DataFrame) -> float:
    if "edge" not in g.columns or "line" not in g.columns:
        return 0.0
    edge = pd.to_numeric(g["edge"], errors="coerce")
    line = pd.to_numeric(g["line"], errors="coerce").abs().clip(lower=1.0)
    ratio = edge.abs() / line
    return float((ratio > EDGE_RATIO_FLAG).fillna(False).mean())


def _fill_note(col: str, sport: str, fill: float, *, proj_id_saw_fill: bool) -> str:
    if col == "usage_pct":
        if str(sport or "").upper() in USAGE_REQUIRED_SPORTS:
            return "regression" if fill == 0 else ""
        return EXPECTED_FILL_GAPS["usage_pct"]
    if col == "league":
        return EXPECTED_FILL_GAPS["league"]
    if (col, sport) in EXPECTED_SPORT_GAPS:
        return EXPECTED_SPORT_GAPS[(col, sport)]
    if col == "pp_projection_id" and fill == 0 and proj_id_saw_fill:
        return "regression"
    if col == "pp_projection_id" and fill == 0:
        return "blank"
    return ""


def fill_by_date(root: Path | None = None, *, date: str | None = None) -> pd.DataFrame:
    legs = current_legs(root)
    feats = read_table_df("features", root)
    if legs.empty:
        return pd.DataFrame()
    if date:
        legs = legs[legs["slate_date"].astype(str).str[:10] == date[:10]]
        if not feats.empty:
            feats = feats[feats["slate_date"].astype(str).str[:10] == date[:10]]
    if not feats.empty:
        feats = feats.sort_values("ingested_at") if "ingested_at" in feats.columns else feats
        feats = feats.drop_duplicates(subset=["leg_id"], keep="last")
        merged = legs.merge(feats, on=["leg_id", "slate_date"], how="left", suffixes=("", "_feat"))
    else:
        merged = legs
    if "pp_projection_id" in merged.columns:
        hist = (
            merged.assign(_filled=merged["pp_projection_id"].fillna("").astype(str).str.len() > 0)
            .groupby(merged["slate_date"].astype(str).str[:10])["_filled"]
            .mean()
        )
        saw_fill = {d: v > 0.01 for d, v in hist.items()}
        prior_true = False
        saw_before: dict[str, bool] = {}
        for d in sorted(saw_fill):
            saw_before[d] = prior_true
            prior_true = prior_true or saw_fill[d]
    else:
        saw_before = {}
    rows = []
    for (slate, sport), g in merged.groupby(
        [merged["slate_date"].astype(str).str[:10], merged["sport"].fillna("")]
    ):
        n = len(g)
        proj = float((g["pp_projection_id"].fillna("").astype(str).str.len() > 0).mean()) if "pp_projection_id" in g else 0.0
        league = float((g["league"].fillna("").astype(str).str.len() > 0).mean()) if "league" in g else 0.0
        ml = float(g["ml_prob"].notna().mean()) if "ml_prob" in g else 0.0
        usage = float(g["usage_pct"].notna().mean()) if "usage_pct" in g else 0.0
        notes = []
        if proj < 0.05:
            notes.append(
                _fill_note("pp_projection_id", sport, proj, proj_id_saw_fill=saw_before.get(str(slate), False))
            )
        if league < 0.05:
            notes.append(_fill_note("league", sport, league, proj_id_saw_fill=False))
        if ml < 0.05:
            notes.append(_fill_note("ml_prob", sport, ml, proj_id_saw_fill=False))
        if usage < 0.05:
            notes.append(_fill_note("usage_pct", sport, usage, proj_id_saw_fill=False))
        rows.append(
            {
                "slate_date": str(slate)[:10],
                "sport": sport or "(blank)",
                "n": n,
                "proj_id_fill": proj,
                "league_fill": league,
                "ml_prob_fill": ml,
                "usage_fill": usage,
                "edge_ratio_gt5": _edge_ratio_high(g) if "line" in g else 0.0,
                "notes": ",".join(x for x in notes if x),
            }
        )
    return pd.DataFrame(rows).sort_values(["slate_date", "n"], ascending=[True, False])


def fill_summary(root: Path | None = None, *, date: str | None = None) -> pd.DataFrame:
    by = fill_by_date(root, date=date)
    if by.empty:
        return by
    rows = []
    for sport, g in by.groupby("sport"):
        n = int(g["n"].sum())
        w = g["n"].clip(lower=1)
        rows.append(
            {
                "sport": sport,
                "n": n,
                "proj_id_fill": float((g["proj_id_fill"] * w).sum() / n),
                "league_fill": float((g["league_fill"] * w).sum() / n),
                "ml_prob_fill": float((g["ml_prob_fill"] * w).sum() / n),
                "usage_fill": float((g["usage_fill"] * w).sum() / n),
                "edge_ratio_gt5": float((g["edge_ratio_gt5"] * w).sum() / n),
                "notes": ",".join(sorted({p for s in g["notes"] for p in str(s).split(",") if p})),
            }
        )
    return pd.DataFrame(rows).sort_values("n", ascending=False)


def ticket_join_summary(root: Path | None = None, *, date: str | None = None) -> pd.DataFrame:
    tlegs = read_table_df("ticket_legs", root)
    legs = current_legs(root)
    if tlegs.empty:
        return pd.DataFrame()
    if date:
        tlegs = tlegs[tlegs["slate_date"].astype(str).str[:10] == date[:10]]
        if not legs.empty:
            legs = legs[legs["slate_date"].astype(str).str[:10] == date[:10]]
    if legs.empty:
        tlegs = tlegs.copy()
        tlegs["matched"] = 0
    else:
        key = set(zip(legs["slate_date"].astype(str).str[:10], legs["leg_id"].astype(str)))
        tlegs = tlegs.copy()
        tlegs["matched"] = [
            1 if (str(d)[:10], str(i)) in key else 0
            for d, i in zip(tlegs["slate_date"], tlegs["leg_id"])
        ]
    out = (
        tlegs.groupby(tlegs["slate_date"].astype(str).str[:10], dropna=False)
        .agg(ticket_leg_rows=("leg_id", "size"), matched=("matched", "sum"))
        .reset_index()
        .rename(columns={"slate_date": "slate_date"})
    )
    out["unmatched"] = out["ticket_leg_rows"] - out["matched"]
    out["match_rate"] = out["matched"] / out["ticket_leg_rows"].clip(lower=1)
    return out.sort_values("slate_date")


def unmatched_ticket_legs(root: Path | None = None, *, date: str | None = None) -> pd.DataFrame:
    tlegs = read_table_df("ticket_legs", root)
    legs = current_legs(root)
    if tlegs.empty:
        return pd.DataFrame()
    if date:
        tlegs = tlegs[tlegs["slate_date"].astype(str).str[:10] == date[:10]]
    if legs.empty:
        return tlegs
    key = set(zip(legs["slate_date"].astype(str).str[:10], legs["leg_id"].astype(str)))
    mask = [
        (str(d)[:10], str(i)) not in key
        for d, i in zip(tlegs["slate_date"], tlegs["leg_id"])
    ]
    return tlegs.loc[mask].copy()
