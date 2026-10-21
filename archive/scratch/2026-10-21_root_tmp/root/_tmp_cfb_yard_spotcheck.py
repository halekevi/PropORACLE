"""Spot-check yard props on one overnight grade vs the 9/12 path."""
from __future__ import annotations

import json
from collections import Counter
from pathlib import Path

import pandas as pd

YARD = ("pass yards", "rush yards", "receiving yards", "rec yards", "pass yds", "rush yds", "rec yds")


def norm_prop(s: str) -> str:
    x = str(s or "").strip().lower()
    if x in {"pass yds", "passing yards"}:
        return "pass yards"
    if x in {"rush yds", "rushing yards"}:
        return "rush yards"
    if x in {"rec yds", "receiving yards", "reception yards"}:
        return "rec yards"
    return x


def from_json(path: Path) -> Counter:
    raw = json.loads(path.read_text(encoding="utf-8"))
    c = Counter()
    for r in raw.get("props") or []:
        if str(r.get("sport") or "").upper() != "CFB":
            continue
        prop = norm_prop(r.get("prop") or r.get("prop_type") or "")
        if prop not in {"pass yards", "rush yards", "rec yards"}:
            continue
        res = str(r.get("result") or "").upper() or "BLANK"
        void = str(r.get("void_reason") or r.get("void_reason_grade") or "")
        if "NO_ACTUAL" in void.upper():
            res = "NO_ACTUAL"
        c[res] += 1
    return c


def from_xlsx(path: Path) -> Counter:
    df = pd.read_excel(path, sheet_name="Box Raw")
    cols = {str(c).strip().lower(): c for c in df.columns}
    prop_c = cols.get("prop_type_norm") or cols.get("prop") or cols.get("prop type")
    res_c = cols.get("result")
    void_c = cols.get("void_reason") or cols.get("void reason")
    c = Counter()
    for row in df.to_dict("records"):
        prop = norm_prop(row.get(prop_c) if prop_c else "")
        if prop not in {"pass yards", "rush yards", "rec yards"}:
            continue
        res = str(row.get(res_c) or "").upper() or "BLANK"
        void = str(row.get(void_c) or "") if void_c else ""
        if "NO_ACTUAL" in void.upper() or res in {"", "NAN", "NONE"}:
            if "NO_ACTUAL" in void.upper() or res in {"PENDING", "VOID", "NAN", "NONE", ""}:
                res = "NO_ACTUAL" if "NO_ACTUAL" in void.upper() or res not in {"HIT", "MISS"} else res
        c[res] += 1
    return c


def show(label: str, c: Counter) -> None:
    n = sum(c.values())
    print(f"{label} n={n} {dict(c)}")


pairs = [
    ("9/05 overnight xlsx", Path(r"H:\PropORACLE_main_cp\outputs\2026-09-05\graded_cfb_2026-09-05.xlsx"), "xlsx"),
    ("9/05 main_cp json", Path(r"H:\PropORACLE_main_cp\ui_runner\templates\graded_props_2026-09-05.json"), "json"),
    ("9/05 cursor json", Path(r"H:\PropORACLE\ui_runner\templates\graded_props_2026-09-05.json"), "json"),
    ("9/12 cursor xlsx", Path(r"H:\PropORACLE\outputs\2026-09-12\graded_cfb_2026-09-12.xlsx"), "xlsx"),
    ("9/12 cursor json", Path(r"H:\PropORACLE\ui_runner\templates\graded_props_2026-09-12.json"), "json"),
    ("9/12 main_cp json", Path(r"H:\PropORACLE_main_cp\ui_runner\templates\graded_props_2026-09-12.json"), "json"),
]
for label, path, kind in pairs:
    if not path.is_file():
        print(label, "MISSING")
        continue
    show(label, from_xlsx(path) if kind == "xlsx" else from_json(path))
