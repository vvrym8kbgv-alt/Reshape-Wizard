from __future__ import annotations
import re
import pandas as pd
from typing import List, Tuple, Dict

UNNAMED_PATTERN = re.compile(r"^Unnamed(:\s*\d+)?$", re.IGNORECASE)


def is_invalid(name: str | float | int | None) -> bool:
    if name is None:
        return True
    if isinstance(name, float) and pd.isna(name):
        return True
    s = str(name).strip()
    if s == "":
        return True
    if UNNAMED_PATTERN.match(s):
        return True
    return False


def dedupe(names: List[str]) -> List[str]:
    seen: Dict[str, int] = {}
    result: List[str] = []
    for name in names:
        count = seen.get(name, 0)
        if count == 0:
            result.append(name)
        else:
            result.append(f"{name}.{count+1}")
        seen[name] = count + 1
    return result


def propose_safe_columns(columns: List[str]) -> tuple[List[str], List[Tuple[str, str]], bool]:
    """Return safe col names list, mapping, fixed flag without mutating a df."""
    fixed = False
    safe_cols: List[str] = []
    mapping: List[Tuple[str, str]] = []
    for col in columns:
        new = str(col).strip() if not is_invalid(col) else "col"
        if new == "":
            new = "col"
        safe_cols.append(new)
    safe_cols = dedupe(safe_cols)
    for old, new in zip(columns, safe_cols):
        if old != new:
            fixed = True
            mapping.append((str(old), new))
    return safe_cols, mapping, fixed


def sanitise_columns(df: pd.DataFrame) -> tuple[pd.DataFrame, List[Tuple[str, str]], bool]:
    """Return new df with safe column names, mapping, and whether fixes applied."""
    safe_cols, mapping, fixed = propose_safe_columns(list(df.columns))
    df = df.copy()
    df.columns = safe_cols
    return df, mapping, fixed


def apply_custom_mapping(df: pd.DataFrame, mapping: List[Tuple[str, str]]) -> pd.DataFrame:
    rename_map = {old: new for old, new in mapping}
    return df.rename(columns=rename_map)
