from __future__ import annotations

import operator
from typing import Any, Dict, Iterable, Optional, Union

import pandas as pd


# -----------------------------
# Supported operators
# -----------------------------
_OPS = {
    "==": operator.eq,
    "!=": operator.ne,
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
}

_STRING_OPS = {"contains", "not_contains", "startswith", "endswith"}
_NULL_OPS = {"isnull", "is_null", "notnull", "not_null"}
_SET_OPS = {"in", "isin", "not_in", "notin"}
_RANGE_OPS = {"between"}


FilterNode = Dict[str, Any]  # group node or leaf node


def filter_df_nested(
    df: pd.DataFrame,
    filters: Optional[FilterNode] = None,
    *,
    case_insensitive: bool = False,
) -> pd.DataFrame:
    """
    Filters DataFrame using a nested boolean tree.

    Node formats:
      - Group:
          {"and": [node1, node2, ...]}
          {"or":  [node1, node2, ...]}
          {"not": node}  OR  {"not": [node1, node2, ...]} (list is treated as AND then negated)
      - Leaf rule:
          {"col": "colname", "op": ">=", "value": 10}

    Returns original df if filters is None/empty.
    """
    if not filters:
        return df

    mask = _eval_node(df, filters, case_insensitive=case_insensitive)
    return df[mask]


def _eval_node(df: pd.DataFrame, node: Any, *, case_insensitive: bool) -> pd.Series:
    if not isinstance(node, dict):
        raise TypeError(f"Each node must be a dict. Got: {type(node)}")

    keys = {k.lower().strip() for k in node.keys()}

    # -----------------------------
    # Group nodes
    # -----------------------------
    if "and" in keys or "or" in keys:
        # enforce exactly one of them
        if ("and" in keys) and ("or" in keys):
            raise ValueError("Node cannot contain both 'and' and 'or' keys.")

        group_key = "and" if "and" in keys else "or"
        children = node.get(group_key) if group_key in node else node.get(group_key.upper())

        if children is None:
            raise ValueError(f"Group node '{group_key}' must have a list of children.")
        if not isinstance(children, list) or len(children) == 0:
            raise ValueError(f"Group node '{group_key}' must be a non-empty list.")

        child_masks = [_eval_node(df, child, case_insensitive=case_insensitive) for child in children]

        out = child_masks[0]
        for m in child_masks[1:]:
            out = out & m if group_key == "and" else out | m
        return out

    if "not" in keys:
        child = node.get("not") if "not" in node else node.get("NOT")
        if child is None:
            raise ValueError("NOT node must have a child node or list of nodes.")

        # Allow {"not": [ ... ]} meaning NOT(AND(children))
        if isinstance(child, list):
            if len(child) == 0:
                raise ValueError("NOT with a list must be non-empty.")
            cm = [_eval_node(df, c, case_insensitive=case_insensitive) for c in child]
            combined = cm[0]
            for m in cm[1:]:
                combined = combined & m
            return ~combined

        return ~_eval_node(df, child, case_insensitive=case_insensitive)

    # -----------------------------
    # Leaf rule node
    # -----------------------------
    # Expected keys: col, op, value (value optional for isnull/notnull)
    col = node.get("col")
    op = node.get("op")
    val = node.get("value", None)

    if col is None or op is None:
        raise ValueError("Leaf rule must include 'col' and 'op' (and usually 'value').")

    if col not in df.columns:
        raise KeyError(f"Column not found: {col}")

    s = df[col]
    op_norm = str(op).lower().strip()

    # NULL ops
    if op_norm in _NULL_OPS:
        if op_norm in ("isnull", "is_null"):
            return s.isna()
        return s.notna()

    # IN / NOT IN
    if op_norm in _SET_OPS:
        if val is None:
            raise ValueError(f"Operator '{op}' requires a list/iterable value for column '{col}'.")
        vals = list(val) if not isinstance(val, (str, bytes)) else [val]
        if op_norm in ("in", "isin"):
            return s.isin(vals)
        return ~s.isin(vals)

    # BETWEEN (inclusive)
    if op_norm in _RANGE_OPS:
        if not (isinstance(val, (list, tuple)) and len(val) == 2):
            raise ValueError(f"Operator 'between' requires value [low, high] for column '{col}'.")
        low, high = val
        return s.between(low, high, inclusive="both")

    # String ops
    if op_norm in _STRING_OPS:
        if val is None:
            raise ValueError(f"Operator '{op}' requires a value for column '{col}'.")
        s_str = s.astype("string")
        needle = str(val)

        if case_insensitive:
            s_str = s_str.str.lower()
            needle = needle.lower()

        if op_norm == "contains":
            return s_str.str.contains(needle, na=False, regex=False)
        if op_norm == "not_contains":
            return ~s_str.str.contains(needle, na=False, regex=False)
        if op_norm == "startswith":
            return s_str.str.startswith(needle, na=False)
        # endswith
        return s_str.str.endswith(needle, na=False)

    # Basic comparisons
    if op_norm in _OPS:
        # Optional: case-insensitive equality/inequality on object-like columns
        if case_insensitive and op_norm in ("==", "!=") and pd.api.types.is_object_dtype(s):
            left = s.astype("string").str.lower()
            right = str(val).lower() if val is not None else val
            return _OPS[op_norm](left, right)

        return _OPS[op_norm](s, val)

    raise ValueError(f"Unsupported operator: {op}")


# -----------------------------
# Example
# -----------------------------
if __name__ == "__main__":
    df = pd.DataFrame(
        {
            "age": [18, 25, 40, 30],
            "state": ["NY", "NJ", "CA", "NY"],
            "name": ["John Smith", "Alice", "Johnny", "Bob"],
            "income": [40000, 80000, 150000, 90000],
            "closed_dt": [None, "2025-01-01", None, None],
        }
    )

    filters = {
        "or": [
            {"and": [
                {"col": "age", "op": ">=", "value": 21},
                {"col": "state", "op": "in", "value": ["NY", "NJ"]},
            ]},
            {"not": {"col": "name", "op": "contains", "value": "bob"}},
        ]
    }

    out = filter_df_nested(df, filters, case_insensitive=True)
    print(out)