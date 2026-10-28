"""CSV/Excel stay human-facing; Parquet is the fast machine hop.

Writers keep emitting CSV/XLSX. Readers prefer a sibling ``.parquet`` when
pyarrow is installed. Railway/web does not need pyarrow — loaders fall back
to CSV/XLSX automatically.
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any, Iterable

import pandas as pd

_PARQUET_ENGINES = ("pyarrow", "fastparquet")


def _sheet_token(sheet: str | None) -> str:
    raw = str(sheet or "").strip()
    if not raw:
        return ""
    safe = "".join(ch if ch.isalnum() else "_" for ch in raw)
    return safe.strip("_")


def parquet_sidecar_path(path: str | Path, sheet: str | None = None) -> Path:
    """Sibling parquet hop. Key by sheet when one is named (``stem_ALL.parquet``).

    Unkeyed ``stem.parquet`` is the CSV / legacy hop. A reader that asked for a
    sheet must not use that file — it may be ELIGIBLE or another subset.
    """
    p = Path(path)
    token = _sheet_token(sheet)
    if token:
        return p.with_name(f"{p.stem}_{token}.parquet")
    return p.with_suffix(".parquet")


def iter_parquet_sidecars(path: str | Path) -> list[Path]:
    p = Path(path)
    found: list[Path] = []
    unkeyed = p.with_suffix(".parquet")
    if unkeyed.is_file():
        found.append(unkeyed)
    parent = p.parent
    if parent.is_dir():
        for q in sorted(parent.glob(f"{p.stem}_*.parquet")):
            if q not in found:
                found.append(q)
    return found


def _requested_sheet(sheet: str | None, sheet_order: tuple[str, ...] | None) -> str:
    raw = str(sheet or "").strip()
    if raw:
        return raw
    if sheet_order:
        return str(sheet_order[0] or "").strip()
    return ""


def _resolve_parquet_hop(
    path: Path,
    *,
    requested_sheet: str,
    suffix: str,
) -> Path | None:
    """Return a sidecar only when it matches the requested sheet.

    CSV keeps the unkeyed hop. XLSX never uses an unkeyed sidecar when the
    workbook exists — that file may be a different sheet.
    """
    if suffix in {".csv", ".txt"}:
        unkeyed = parquet_sidecar_path(path)
        return unkeyed if unkeyed.is_file() else None
    if suffix == ".parquet":
        return path if path.is_file() else None
    if requested_sheet:
        keyed = parquet_sidecar_path(path, sheet=requested_sheet)
        return keyed if keyed.is_file() else None
    keyed_all = parquet_sidecar_path(path, sheet="ALL")
    if keyed_all.is_file():
        return keyed_all
    if not path.is_file():
        unkeyed = parquet_sidecar_path(path)
        if unkeyed.is_file():
            return unkeyed
    return None


def parquet_available() -> bool:
    try:
        import pyarrow  # noqa: F401

        return True
    except ImportError:
        try:
            import fastparquet  # noqa: F401

            return True
        except ImportError:
            return False


def table_exists(path: str | Path) -> bool:
    p = Path(path)
    return p.is_file() or bool(iter_parquet_sidecars(p))


def _dedupe_columns(df: pd.DataFrame) -> pd.DataFrame:
    return df.loc[:, ~df.columns.duplicated()].copy()


def _prepare_for_parquet(df: pd.DataFrame) -> pd.DataFrame:
    out = _dedupe_columns(df)
    out.columns = [str(c) for c in out.columns]
    for col in list(out.columns):
        s = out[col]
        if s.dtype == object:
            mapped = s.map(_parquet_cell)
            types = {type(v) for v in mapped.dropna()}
            if len(types) > 1:
                out[col] = mapped.map(lambda v: None if v is None else str(v))
            else:
                out[col] = mapped
            continue
        try:
            if s.dropna().map(lambda v: isinstance(v, str)).any():
                out[col] = s.map(_parquet_cell).map(
                    lambda v: None if v is None else str(v)
                )
        except Exception:
            out[col] = s.astype(object).map(_parquet_cell)
    return out


def _stringify_objects(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        out[col] = out[col].map(_parquet_cell).map(lambda v: None if v is None else str(v))
    return out


def _parquet_cell(v: Any) -> Any:
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, (str, int, float, bool)):
        return v
    return str(v)


def write_parquet_sidecar(
    df: pd.DataFrame,
    path: str | Path,
    sheet: str | None = None,
) -> Path | None:
    """Write a sibling parquet hop. XLSX defaults to the ALL sheet key."""
    if df is None or not parquet_available():
        return None
    p = Path(path)
    if not str(p):
        return None
    if sheet is None and p.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
        sheet = "ALL"
    pq = parquet_sidecar_path(p, sheet=sheet)
    pq.parent.mkdir(parents=True, exist_ok=True)
    body = _prepare_for_parquet(df)
    try:
        body.to_parquet(pq, index=False)
    except Exception:
        try:
            _stringify_objects(body).to_parquet(pq, index=False)
        except Exception as exc:
            print(f"[table_io] WARN parquet sidecar skipped ({pq.name}): {exc}")
            return None
    print(f"[table_io] parquet sidecar -> {pq}")
    return pq


def write_parquet_sidecars(
    df: pd.DataFrame,
    *paths: str | Path,
    sheet: str | None = None,
) -> list[Path]:
    written: list[Path] = []
    seen: set[str] = set()
    for path in paths:
        if not path:
            continue
        p = Path(path)
        token_sheet = sheet
        if token_sheet is None and p.suffix.lower() in {".xlsx", ".xlsm", ".xls"}:
            token_sheet = "ALL"
        key = str(parquet_sidecar_path(p, sheet=token_sheet).resolve()).lower()
        if key in seen:
            continue
        seen.add(key)
        out = write_parquet_sidecar(df, path, sheet=sheet)
        if out is not None:
            written.append(out)
    return written


def copy_parquet_sidecar(src: str | Path, dest: str | Path) -> Path | None:
    """Copy src sidecars (keyed and unkeyed) next to dest."""
    src_p = Path(src)
    dest_p = Path(dest)
    dest_p.parent.mkdir(parents=True, exist_ok=True)
    copied: Path | None = None
    for src_pq in iter_parquet_sidecars(src_p):
        suffix = src_pq.name[len(src_p.stem) :]
        dest_pq = dest_p.with_name(f"{dest_p.stem}{suffix}")
        try:
            shutil.copy2(src_pq, dest_pq)
        except OSError as exc:
            print(f"[table_io] WARN parquet copy skipped ({dest_pq.name}): {exc}")
            continue
        copied = dest_pq
    return copied


def _drop_arrow_backend(df: pd.DataFrame) -> pd.DataFrame:
    """Arrow-backed string cols reject numeric assignment (combined L5 truth)."""
    out = df.copy()
    for col in out.columns:
        dtype = out[col].dtype
        name = str(dtype)
        if (
            "pyarrow" in name
            or name.startswith("string")
            or type(dtype).__name__ == "ArrowDtype"
        ):
            out[col] = out[col].astype(object)
    return out


def _read_parquet(path: Path) -> pd.DataFrame:
    try:
        raw = pd.read_parquet(path, dtype_backend="numpy_nullable")
    except TypeError:
        raw = pd.read_parquet(path)
    out = _drop_arrow_backend(_dedupe_columns(raw))
    # Excel-era callers use `x or ""`; pandas NA is not boolean-falsy.
    return out.astype(object).mask(out.isna(), None)


def _pick_excel_sheet(path: Path, sheet: str | None, sheet_order: tuple[str, ...] | None) -> str | int:
    if sheet:
        return sheet
    xl = pd.ExcelFile(path, engine="openpyxl")
    names = [n for n in list(xl.sheet_names) if str(n) != "_META"]
    if sheet_order:
        for name in sheet_order:
            if name in names:
                return name
    return names[0] if names else 0


def excel_data_header_row(path: Path, sheet: str | int) -> int:
    """Skip the optional slate/build STALE banner written as row 0 on older files."""
    try:
        peek = pd.read_excel(path, sheet_name=sheet, header=None, nrows=1, engine="openpyxl")
    except Exception:
        return 0
    if peek is None or peek.empty:
        return 0
    cell = str(peek.iloc[0, 0] if peek.shape[1] else "")
    if cell.startswith("slate_date="):
        return 1
    return 0


def _excel_data_header_row(path: Path, sheet: str | int) -> int:
    return excel_data_header_row(path, sheet)


def repair_banner_headers(df: pd.DataFrame) -> pd.DataFrame:
    """If a reader treated ``slate_date=...`` as column names, promote row 0."""
    if df is None or getattr(df, "empty", True):
        return df
    cols = [str(c) for c in df.columns]
    if not any(c.startswith("slate_date=") for c in cols):
        return df
    new_cols = [str(x).strip() for x in df.iloc[0].tolist()]
    out = df.iloc[1:].copy()
    out.columns = new_cols
    return out.reset_index(drop=True)


def stringify_cells(df: pd.DataFrame) -> pd.DataFrame:
    """Match step8 ``pd.read_excel(..., dtype=str).fillna('')`` without Excel."""
    if df is None:
        return pd.DataFrame()
    out = _dedupe_columns(df)
    if out.empty:
        return out
    for col in out.columns:
        out[col] = out[col].map(_excel_str_cell)
    return out


def _excel_str_cell(v: Any) -> str:
    if v is None:
        return ""
    try:
        if pd.isna(v):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(v)
    if s.lower() in {"nan", "<na>", "none", "nat", "<nat>"}:
        return ""
    return s


def read_table(
    path: str | Path,
    *,
    sheet: str | None = None,
    sheet_order: tuple[str, ...] | None = None,
    prefer_parquet: bool = True,
    **csv_kwargs: Any,
) -> pd.DataFrame:
    """Read a sheet-keyed parquet sidecar when it matches, else CSV/XLSX."""
    p = Path(path)
    suffix = p.suffix.lower()
    requested = _requested_sheet(sheet, sheet_order)
    if prefer_parquet and parquet_available():
        hop = _resolve_parquet_hop(p, requested_sheet=requested, suffix=suffix)
        if hop is not None:
            try:
                return _read_parquet(hop)
            except Exception as exc:
                print(f"[table_io] WARN parquet read failed ({hop.name}), falling back: {exc}")

    if suffix == ".parquet":
        return _read_parquet(p)
    if suffix in {".csv", ".txt"}:
        kwargs = {"encoding": "utf-8-sig", "low_memory": False, **csv_kwargs}
        return _dedupe_columns(pd.read_csv(p, **kwargs))
    if suffix in {".xlsx", ".xlsm", ".xls"}:
        chosen = _pick_excel_sheet(p, sheet, sheet_order)
        header_row = _excel_data_header_row(p, chosen)
        return _dedupe_columns(
            pd.read_excel(p, sheet_name=chosen, engine="openpyxl", header=header_row)
        )
    leftover = _resolve_parquet_hop(p, requested_sheet=requested, suffix=suffix)
    if leftover is not None and parquet_available():
        return _read_parquet(leftover)
    raise FileNotFoundError(f"Unsupported table path: {p}")


def read_table_str(
    path: str | Path,
    *,
    sheet: str | None = None,
    sheet_order: tuple[str, ...] | None = None,
    prefer_parquet: bool = True,
    **csv_kwargs: Any,
) -> pd.DataFrame:
    """``read_table`` then stringify cells (step8 ranked-board hop)."""
    return stringify_cells(
        read_table(
            path,
            sheet=sheet,
            sheet_order=sheet_order,
            prefer_parquet=prefer_parquet,
            **csv_kwargs,
        )
    )


def write_props_parquet(df: pd.DataFrame, path: str | Path) -> Path:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    body = _prepare_for_parquet(df)
    try:
        body.to_parquet(p, index=False)
    except Exception:
        _stringify_objects(body).to_parquet(p, index=False)
    return p


def read_props_parquet(path: str | Path) -> pd.DataFrame:
    return _read_parquet(Path(path))


def iter_engine_names() -> Iterable[str]:
    return _PARQUET_ENGINES


def xlsxwriter_available() -> bool:
    try:
        import xlsxwriter  # noqa: F401

        return True
    except ImportError:
        return False


def _xl_col_name(idx: int) -> str:
    try:
        from xlsxwriter.utility import xl_col_to_name

        return xl_col_to_name(idx)
    except Exception:
        n = idx + 1
        letters = ""
        while n:
            n, rem = divmod(n - 1, 26)
            letters = chr(65 + rem) + letters
        return letters


def _sanitize_excel_df(df: pd.DataFrame) -> pd.DataFrame:
    out = _dedupe_columns(df if df is not None else pd.DataFrame())
    out.columns = [str(c) for c in out.columns]
    try:
        import numpy as np

        return out.replace([np.inf, -np.inf], np.nan)
    except Exception:
        return out


_TIER_FILL = {
    "A": ("1E8449", "FFFFFF"),
    "B": ("2874A6", "FFFFFF"),
    "C": ("D4AC0D", "000000"),
    "D": ("717D7E", "FFFFFF"),
}


def _et_today_iso() -> str:
    try:
        from zoneinfo import ZoneInfo

        from datetime import datetime

        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d")
    except Exception:
        from datetime import datetime

        return datetime.now().strftime("%Y-%m-%d")


def _now_build_stamp() -> str:
    try:
        from zoneinfo import ZoneInfo

        from datetime import datetime

        return datetime.now(ZoneInfo("America/New_York")).strftime("%Y-%m-%d %H:%M ET")
    except Exception:
        from datetime import datetime

        return datetime.now().strftime("%Y-%m-%d %H:%M")


def infer_slate_date_from_path(path: str | Path) -> str:
    """Pull YYYY-MM-DD from outputs/<date>/… (or any path segment)."""
    import re

    parts = Path(path).resolve().parts
    for part in parts:
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", part or ""):
            return part
    return ""


_NBA_PATH_PARTS = frozenset({"nba", "nba1h", "nba1q", "nbap"})
_NBA_REQUIRED_SAFETY_COLS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("l5_current_season_n", "L5 Current RS N"), "L5 Current RS N"),
    (("current_season_rs_n", "Current Season RS N"), "Current Season RS N"),
    (("clean_minutes", "Clean Minutes"), "Clean Minutes"),
    (("minutes_flag", "Minutes Flag"), "Minutes Flag"),
)
_BLANK_CELL = frozenset({"", "nan", "none", "<na>", "<na>", "nat"})


def path_looks_nba_step8(path: str | Path) -> bool:
    p = Path(path)
    if any(str(part).lower() in _NBA_PATH_PARTS for part in p.parts):
        return True
    stem = p.stem.lower()
    # ``wnba`` contains ``nba`` as a substring — match NBA tokens only.
    return (
        stem.startswith("step8_nba")
        or "_nba_" in stem
        or stem.endswith("_nba")
    )


def _series_all_blank(series: pd.Series) -> bool:
    if series is None or len(series) == 0:
        return True
    s = series.astype("string").fillna("").str.strip().str.lower()
    return bool(s.isin(_BLANK_CELL).all())


def nba_required_columns_all_blank(df: pd.DataFrame) -> list[str]:
    """Display names of required NBA safety columns that are 100% blank."""
    missing: list[str] = []
    for names, label in _NBA_REQUIRED_SAFETY_COLS:
        present = [n for n in names if n in df.columns]
        if not present or all(_series_all_blank(df[n]) for n in present):
            missing.append(label)
    return missing


def step8_freshness_label(
    slate_date: str,
    run_date: str = "",
    *,
    column_fail: bool = False,
) -> str:
    """STALE when slate_date is older than run_date; FAIL when NBA safety cols are blank."""
    slate = (slate_date or "").strip()[:10]
    run = (run_date or "").strip()[:10] or _et_today_iso()
    if len(slate) < 10 or len(run) < 10:
        return "FAIL" if column_fail else ""
    if slate < run:
        return "STALE"
    if column_fail:
        return "FAIL"
    return "OK"


def format_step8_meta_banner(
    *,
    slate_date: str = "",
    run_date: str = "",
    build_time: str = "",
    column_fail: bool = False,
) -> tuple[str, str]:
    """Return (banner text, freshness label). freshness is STALE|FAIL|OK|''."""
    slate = (slate_date or "").strip()[:10]
    run = (run_date or "").strip()[:10] or _et_today_iso()
    built = (build_time or "").strip() or _now_build_stamp()
    fresh = step8_freshness_label(slate, run, column_fail=column_fail)
    bits = [
        f"slate_date={slate or 'UNKNOWN'}",
        f"build_time={built}",
        f"run_date={run}",
    ]
    if fresh:
        bits.append(fresh)
    return " | ".join(bits), fresh


def read_step8_meta_freshness(path: str | Path) -> str:
    """Read ``_META.freshness`` from an xlsx. Empty when the sheet is missing."""
    p = Path(path)
    if p.suffix.lower() not in {".xlsx", ".xlsm"} or not p.is_file():
        return ""
    try:
        meta = pd.read_excel(p, sheet_name="_META")
    except Exception:
        return ""
    if meta is None or getattr(meta, "empty", True) or "freshness" not in meta.columns:
        return ""
    return str(meta["freshness"].iloc[0] or "").strip().upper()


def _style_xlsxwriter_sheet(
    writer,
    sheet_name: str,
    df: pd.DataFrame,
    header_color: str,
    header_font: str,
    *,
    header_row: int = 0,
) -> None:
    wb = writer.book
    ws = writer.sheets[sheet_name]
    hdr = wb.add_format(
        {
            "bold": True,
            "font_color": header_font,
            "bg_color": header_color,
            "align": "center",
            "valign": "vcenter",
            "text_wrap": True,
        }
    )
    cols = [str(c) for c in df.columns]
    for i, col in enumerate(cols):
        ws.write(header_row, i, col, hdr)
        ws.set_column(i, i, min(36, max(10, len(col) + 2)))
    ws.freeze_panes(header_row + 1, 0)
    nrows = len(df)
    ncols = max(len(cols), 1)
    data_first = header_row + 1
    data_last = header_row + max(nrows, 1)
    ws.autofilter(header_row, 0, data_last, ncols - 1)
    if nrows == 0 or not cols:
        return
    last_r, last_c = header_row + nrows, ncols - 1
    excel_data_row = data_first + 1  # 1-indexed Excel row of first data cell
    for cand in ("Tier", "tier"):
        if cand not in cols:
            continue
        letter = _xl_col_name(cols.index(cand))
        for grade, (bg, fg) in _TIER_FILL.items():
            fmt = wb.add_format({"bg_color": bg, "font_color": fg})
            ws.conditional_format(
                data_first,
                0,
                last_r,
                last_c,
                {
                    "type": "formula",
                    "criteria": f'=${letter}{excel_data_row}="{grade}"',
                    "format": fmt,
                },
            )
        break
    for cand in ("Direction", "direction"):
        if cand not in cols:
            continue
        cidx = cols.index(cand)
        letter = _xl_col_name(cidx)
        over = wb.add_format({"bg_color": "C8F7C5", "bold": True})
        under = wb.add_format({"bg_color": "F7C5C5", "bold": True})
        ws.conditional_format(
            data_first, cidx, last_r, cidx,
            {"type": "formula", "criteria": f'=${letter}{excel_data_row}="OVER"', "format": over},
        )
        ws.conditional_format(
            data_first, cidx, last_r, cidx,
            {"type": "formula", "criteria": f'=${letter}{excel_data_row}="UNDER"', "format": under},
        )
        break


def write_excel_sheets(
    path: str | Path,
    sheets: dict[str, pd.DataFrame],
    *,
    header_color: str = "1C1C1C",
    header_font: str = "FFFFFF",
    style: bool = True,
    slate_date: str = "",
    run_date: str = "",
    build_time: str = "",
    stamp_meta: bool = True,
) -> Path:
    """Bulk-write an .xlsx. Prefers xlsxwriter (~5x vs openpyxl cell loops).

    When ``stamp_meta`` is true (default):
    - sheet ``_META`` is written first (slate_date / build_time / run_date / freshness)
    - data sheets start at row 0 with real column headers (banner is ``_META`` only)
    - ``read_table`` still skips a leftover row-0 ``slate_date=`` banner on old files
    """
    import os

    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    cleaned: dict[str, pd.DataFrame] = {}
    for name, df in sheets.items():
        key = str(name or "Sheet")[:31]
        if key in cleaned:
            key = f"{key[:28]}_{len(cleaned)}"
        cleaned[key] = _sanitize_excel_df(df)

    slate = (slate_date or "").strip()[:10] or infer_slate_date_from_path(p)
    if not slate:
        slate = (os.environ.get("PROPORACLE_SLATE_DATE") or os.environ.get("PROPORACLE_DATE") or "").strip()[:10]
    run = (run_date or "").strip()[:10] or (os.environ.get("PROPORACLE_RUN_DATE") or "").strip()[:10] or _et_today_iso()
    blank_cols: list[str] = []
    column_fail = False
    if stamp_meta and path_looks_nba_step8(p):
        all_df = cleaned.get("ALL")
        if all_df is None:
            all_df = next(iter(cleaned.values()), None)
        if all_df is not None and not all_df.empty:
            blank_cols = nba_required_columns_all_blank(all_df)
            column_fail = bool(blank_cols)
    banner, fresh = format_step8_meta_banner(
        slate_date=slate,
        run_date=run,
        build_time=build_time,
        column_fail=column_fail,
    )
    if stamp_meta and fresh == "STALE":
        print(f"[table_io] STALE step8 workbook slate_date={slate} run_date={run} -> {p.name}")
    elif stamp_meta and fresh == "FAIL":
        print(
            f"[table_io] FAIL step8 workbook missing safety columns "
            f"{blank_cols} slate_date={slate} -> {p.name}"
        )
    elif stamp_meta:
        print(f"[table_io] step8 meta {banner}")

    engine = "xlsxwriter" if xlsxwriter_available() else "openpyxl"
    kwargs: dict[str, Any] = {}
    if engine == "xlsxwriter":
        kwargs["engine_kwargs"] = {"options": {"strings_to_urls": False}}
    built = (build_time or "").strip() or _now_build_stamp()
    ordered: dict[str, pd.DataFrame] = {}
    if stamp_meta:
        ordered["_META"] = pd.DataFrame(
            [
                {
                    "slate_date": slate or "UNKNOWN",
                    "build_time": built,
                    "run_date": run,
                    "freshness": fresh or "",
                    "banner": banner,
                    "blank_cols": ",".join(blank_cols),
                }
            ]
        )
    ordered.update(cleaned)
    startrow = 0

    with pd.ExcelWriter(p, engine=engine, **kwargs) as writer:
        for name, df in ordered.items():
            body = df if not df.empty else pd.DataFrame(columns=df.columns)
            is_meta = name == "_META"
            row0 = 0 if is_meta else startrow
            body.to_excel(writer, sheet_name=name, index=False, startrow=row0)
            if style and engine == "xlsxwriter" and not is_meta:
                try:
                    _style_xlsxwriter_sheet(
                        writer, name, body, header_color, header_font, header_row=row0
                    )
                except Exception as exc:
                    print(f"[table_io] WARN xlsx style skipped ({name}): {exc}")
    tag = f" meta={fresh or 'n/a'}" if stamp_meta else ""
    print(f"[table_io] excel -> {p.name} engine={engine} sheets={list(ordered)}{tag}")
    return p
