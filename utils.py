from __future__ import annotations

from typing import Any, Mapping, Optional
import datetime as dt
import re

import numpy as np
import pandas as pd


_EXCEL_DATE_BASE_1900 = dt.datetime(1899, 12, 30)  # pandas convention for Excel serials


def _parse_excel_serial_date(val: Any, *, tz: Optional[str] = None):
    """
    Convert Excel serial date/time (1900 date system) to pandas Timestamp.
    Handles ints/floats and numeric-looking strings like "47521" or "47521.5".
    Returns pd.NaT on failure.
    """
    if val is None or (isinstance(val, float) and np.isnan(val)):
        return pd.NaT

    # Normalize strings
    if isinstance(val, str):
        s = val.strip()
        if s == "":
            return pd.NaT
        # Accept numeric-ish strings
        if re.fullmatch(r"[-+]?\d+(\.\d+)?", s):
            try:
                val = float(s)
            except Exception:
                return pd.NaT
        else:
            return pd.NaT

    if isinstance(val, (int, np.integer)):
        serial = float(val)
    elif isinstance(val, (float, np.floating)):
        if np.isnan(val):
            return pd.NaT
        serial = float(val)
    else:
        return pd.NaT

    # Guardrails: typical Excel serial dates (roughly years 1900–2100)
    if not (1 <= serial <= 80000):
        return pd.NaT

    ts = pd.Timestamp(_EXCEL_DATE_BASE_1900) + pd.to_timedelta(serial, unit="D")
    if tz:
        # treat as naive local date/time then localize
        ts = ts.tz_localize(tz, nonexistent="NaT", ambiguous="NaT")
    return ts


def _to_date_series(s: pd.Series) -> pd.Series:
    """
    Convert a Series to python `datetime.date` objects (or NaT).
    Handles ISO strings, common date strings, pandas timestamps, AND Excel serials.
    """
    # 1) Try normal datetime parsing
    dt_norm = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)

    # 2) For values that didn't parse, try Excel serial conversion
    mask_bad = dt_norm.isna() & s.notna()

    if mask_bad.any():
        # Apply serial conversion only to the problematic subset
        converted = s.where(mask_bad).map(_parse_excel_serial_date)
        # Combine: prefer normal parse, else excel-serial parse
        dt_norm = dt_norm.where(~mask_bad, converted)

    # 3) Convert to python date
    return dt_norm.dt.date


def coerce_dtypes(
    df: pd.DataFrame,
    dtype_map: Mapping[str, str | type],
    *,
    on_missing: str = "ignore",  # "ignore" | "raise"
) -> pd.DataFrame:
    """
    Change dtypes for columns using a provided mapping.

    dtype_map values can be:
      - "date" / "datetime" / "string" / "int" / "float" / "bool"
      - Python types: int, float, bool, str
      - pandas dtypes: "Int64", "Float64", etc.

    Special behavior:
      - If target is "date": uses pd.to_datetime(...).dt.date and also handles Excel serial dates like 47521.

    Returns a NEW dataframe (does not mutate the original).
    """
    out = df.copy()

    for col, target in dtype_map.items():
        if col not in out.columns:
            if on_missing == "raise":
                raise KeyError(f"Column not found: {col}")
            continue

        s = out[col]

        # Normalize target spec
        t = target
        if isinstance(t, type):
            if t is int:
                t = "int"
            elif t is float:
                t = "float"
            elif t is bool:
                t = "bool"
            elif t is str:
                t = "string"
            else:
                t = str(t)

        if isinstance(t, str):
            t_norm = t.strip().lower()
        else:
            t_norm = str(t).strip().lower()

        if t_norm in {"date"}:
            out[col] = _to_date_series(s)

        elif t_norm in {"datetime", "datetime64", "timestamp"}:
            # For datetime, also support Excel serials when normal parse fails.
            dt_norm = pd.to_datetime(s, errors="coerce", infer_datetime_format=True)
            mask_bad = dt_norm.isna() & s.notna()
            if mask_bad.any():
                converted = s.where(mask_bad).map(_parse_excel_serial_date)
                dt_norm = dt_norm.where(~mask_bad, converted)
            out[col] = dt_norm

        elif t_norm in {"string", "str"}:
            out[col] = s.astype("string")

        elif t_norm in {"int", "int64", "integer"}:
            # Use pandas nullable integer so blanks don't blow up
            out[col] = pd.to_numeric(s, errors="coerce").astype("Int64")

        elif t_norm in {"float", "float64", "double"}:
            out[col] = pd.to_numeric(s, errors="coerce").astype("Float64")

        elif t_norm in {"bool", "boolean"}:
            # Handle common representations
            if s.dtype == "bool":
                out[col] = s
            else:
                mapped = (
                    s.astype("string")
                    .str.strip()
                    .str.lower()
                    .map(
                        {
                            "true": True,
                            "t": True,
                            "yes": True,
                            "y": True,
                            "1": True,
                            "false": False,
                            "f": False,
                            "no": False,
                            "n": False,
                            "0": False,
                        }
                    )
                )
                out[col] = mapped.astype("boolean")

        else:
            # If user passed a pandas dtype like "Int64" / "category" / etc.
            try:
                out[col] = s.astype(target)
            except Exception as e:
                raise TypeError(f"Failed to coerce column '{col}' to {target!r}: {e}") from e

    return out