#!/usr/bin/env python3
"""Production-model decile table for one book, split by face verification.

Question (2026-10-24): the Standard top decile predicted 0.97 and realized
0.36. Was that thin player history, or Goblin/Demon rungs labeled Standard in
the training window (NHL Feb-Jun, Soccer Mar-Jun, Tennis Apr-Jul, NBA1H)?

Scores ``data/retrain_dataset.csv`` rows of one book with
``models/edge_model_unified.pkl`` and prints pred-vs-realized deciles for
all rows, face-verified sport-dates only, and unverified sport-dates only
(``utils.face_ladder`` status on the leg ledger).

  py -3.14 scripts/standard_decile_face_verified.py --book standard
  py -3.14 scripts/standard_decile_face_verified.py --book goblin --direction OVER
"""
from __future__ import annotations

import argparse
import json
import sys
import warnings
from pathlib import Path

import joblib
import numpy as np
import pandas as pd

_REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_REPO / "scripts"))
sys.path.insert(0, str(_REPO))

from train_edge_model import _load_retrain_csv_as_raw, _prepare_features  # noqa: E402

OUT_DIR = _REPO / "data" / "reports"


def _score(model, feats: list[str], df: pd.DataFrame) -> np.ndarray:
    X = df.copy()
    for c in feats:
        if c not in X.columns:
            X[c] = np.nan
        ser = pd.to_numeric(X[c], errors="coerce")
        med = float(np.nanmedian(ser.to_numpy(dtype=float))) if ser.notna().any() else 0.0
        X[c] = ser.fillna(0.0 if np.isnan(med) else med)
    return np.asarray(model.predict_proba(X[feats].astype(float))[:, 1], dtype=float)


def _deciles(p: np.ndarray, y: np.ndarray, q: int = 10) -> pd.DataFrame:
    if len(p) < q * 5:
        return pd.DataFrame()
    try:
        dec = pd.qcut(pd.Series(p), q=q, labels=False, duplicates="drop")
    except ValueError:
        return pd.DataFrame()
    tab = (
        pd.DataFrame({"decile": dec.to_numpy(), "p": p, "y": y})
        .groupby("decile")
        .agg(n=("y", "size"), pred=("p", "mean"), realized=("y", "mean"))
        .reset_index()
    )
    tab["gap"] = tab["pred"] - tab["realized"]
    return tab


def _print_tab(title: str, tab: pd.DataFrame) -> None:
    print(f"\n{title}")
    if tab.empty:
        print("  (too few rows)")
        return
    print(f"  {'dec':>3} {'n':>7} {'pred':>6} {'real':>6} {'gap':>6}")
    for r in tab.itertuples(index=False):
        print(f"  {int(r.decile):>3} {int(r.n):>7} {r.pred:6.3f} {r.realized:6.3f} {r.gap:+6.3f}")
    top = tab.iloc[-1]
    print(f"  top decile pred={top.pred:.3f} realized={top.realized:.3f} n={int(top.n)}")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--book", default="standard", choices=["standard", "goblin", "demon"])
    ap.add_argument("--direction", default=None, help="OVER / UNDER (optional)")
    ap.add_argument("--csv", default=str(_REPO / "data" / "retrain_dataset.csv"))
    ap.add_argument("--model", default=str(_REPO / "models" / "edge_model_unified.pkl"))
    ap.add_argument("--features", default=str(_REPO / "models" / "edge_model_features.json"))
    ap.add_argument(
        "--prob-col",
        default=None,
        help="Use a stored probability column (e.g. ml_prob) instead of re-scoring. "
        "ml_prob reproduces the May-June top_confidence_walkforward table.",
    )
    ap.add_argument("--from", dest="date_from", default=None, help="YYYY-MM-DD (inclusive)")
    ap.add_argument("--to", dest="date_to", default=None, help="YYYY-MM-DD (inclusive)")
    args = ap.parse_args(argv)

    raw = _load_retrain_csv_as_raw(
        Path(args.csv), None, source_label=args.csv, pick_type_filter=args.book, direction_filter=args.direction
    )
    if raw.empty:
        print("no rows")
        return 1
    if args.prob_col:
        df = raw.copy()
        res = df.get("result", pd.Series("", index=df.index)).astype(str).str.upper()
        df["y"] = np.where(res.eq("HIT"), 1.0, np.where(res.eq("MISS"), 0.0, np.nan))
        df["_p"] = pd.to_numeric(df[args.prob_col], errors="coerce")
        df = df[df["_p"].notna()]
        label_model = f"stored {args.prob_col}"
    else:
        model = joblib.load(args.model)
        feats = json.loads(Path(args.features).read_text(encoding="utf-8"))
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = _prepare_features(raw, skip_median_fill=True)
        if "y" not in df.columns:
            df["y"] = pd.to_numeric(df["_hit_y"], errors="coerce")
        label_model = Path(args.model).name
    df = df[df["y"].isin([0.0, 1.0])].copy()
    date = pd.to_datetime(df.get("file_date"), errors="coerce")
    if "step8_game_date" in df.columns:
        date = date.where(date.notna(), pd.to_datetime(df["step8_game_date"], errors="coerce"))
    if "game_date" in df.columns:
        date = date.where(date.notna(), pd.to_datetime(df["game_date"], errors="coerce"))
    df["_date"] = date.dt.strftime("%Y-%m-%d")
    if args.date_from:
        df = df[df["_date"] >= args.date_from]
    if args.date_to:
        df = df[df["_date"] <= args.date_to]
    df["_sport"] = df["sport"].astype(str).str.upper()
    if not args.prob_col:
        df["_p"] = _score(model, feats, df)

    from proporacle.data.leg_ledger import face_status

    status = face_status()  # stored per-sport-date table (first-class face flag)
    st_map = {(str(r.sport).upper(), str(r.slate_date)[:10]): str(r.status) for r in status.itertuples(index=False)}
    df["_face"] = [st_map.get((s, d), "no_ledger_rows") for s, d in zip(df["_sport"], df["_date"])]
    ver = df["_face"].eq("verified")
    unv = df["_face"].isin(["default_labels", "blank_labels", "laddered_standard"])

    label = (
        f"{args.book.title()} {args.direction or 'both directions'} — {label_model}"
        f" {args.date_from or ''}..{args.date_to or ''}"
    )
    print(f"{label}\nrows={len(df):,}  verified={int(ver.sum()):,}  unverified={int(unv.sum()):,}  "
          f"no_ledger_rows={int((~ver & ~unv).sum()):,}")
    print("\nunverified rows by sport:")
    print(df[unv].groupby("_sport").size().sort_values(ascending=False).to_string())
    print("\nverified rows by sport:")
    print(df[ver].groupby("_sport").size().sort_values(ascending=False).to_string())

    y = df["y"].to_numpy(dtype=float)
    p = df["_p"].to_numpy(dtype=float)
    tabs = {
        "all": _deciles(p, y),
        "verified": _deciles(p[ver.to_numpy()], y[ver.to_numpy()]),
        "unverified": _deciles(p[unv.to_numpy()], y[unv.to_numpy()]),
    }
    for k, t in tabs.items():
        _print_tab(f"[{k}] deciles", t)

    # Where does the over-confident tail come from?
    hi = df["_p"] >= 0.90
    print(f"\nrows with p>=0.90: {int(hi.sum()):,}  realized={df.loc[hi, 'y'].mean():.3f}")
    print(df[hi].groupby("_face").agg(n=("y", "size"), realized=("y", "mean")).to_string())
    print(df[hi].groupby(["_sport", "_face"]).agg(n=("y", "size"), realized=("y", "mean")).sort_values("n", ascending=False).head(12).to_string())

    suffix = ("_" + args.direction.lower()) if args.direction else ""
    if args.prob_col:
        suffix += f"_{args.prob_col}"
    out = OUT_DIR / f"decile_face_split_{args.book}{suffix}.json"
    out.write_text(
        json.dumps(
            {
                "book": args.book,
                "direction": args.direction,
                "rows": int(len(df)),
                "verified": int(ver.sum()),
                "unverified": int(unv.sum()),
                "deciles": {k: t.to_dict(orient="records") for k, t in tabs.items()},
                "p_ge_090_by_face": df[hi].groupby("_face").agg(n=("y", "size"), realized=("y", "mean")).reset_index().to_dict(orient="records"),
            },
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"\nwrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
