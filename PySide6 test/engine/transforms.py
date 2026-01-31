from __future__ import annotations
import pandas as pd
from typing import List, Dict, Any, Tuple
from .clean_columns import sanitise_columns


class TransformError(Exception):
    pass


def fill_down(df: pd.DataFrame, columns: List[str], treat_empty_as_na: bool = True):
    work = df.copy()
    if treat_empty_as_na:
        work[columns] = work[columns].replace("", pd.NA)
    before = work[columns].isna().sum().to_dict()
    work[columns] = work[columns].ffill()
    after = work[columns].isna().sum().to_dict()
    filled_counts = {col: before[col] - after[col] for col in columns}
    work, mapping, fixed = sanitise_columns(work)
    warnings = []
    if fixed:
        warnings.append("Column names were sanitised after fill-down")
    return work, {"filled_counts": filled_counts, "mapping": mapping}, warnings


def make_longer(df: pd.DataFrame, id_cols: List[str], value_cols: List[str], var_name: str = "variable", value_name: str = "value"):
    try:
        melted = df.melt(id_vars=id_cols, value_vars=value_cols, var_name=var_name, value_name=value_name)
    except KeyError as exc:
        raise TransformError(str(exc))
    melted, mapping, fixed = sanitise_columns(melted)
    warnings = ["Column names were sanitised after melt"] if fixed else []
    return melted, {"mapping": mapping}, warnings


def build_wider_work_df(df: pd.DataFrame, columns_from: List[str]) -> Tuple[pd.DataFrame, str]:
    work = df.copy()
    if len(columns_from) > 1:
        work["_combined_wider"] = work[columns_from].astype(str).agg("_".join, axis=1)
        return work, "_combined_wider"
    return work, columns_from[0]


def make_wider(df: pd.DataFrame, index_cols: List[str], columns_from: List[str], values_from: str, aggfunc: str | None = None):
    if not columns_from:
        raise TransformError("Choose at least one column for Columns zone")
    work, col_key = build_wider_work_df(df, columns_from)
    dupe_mask = work.duplicated(subset=index_cols + [col_key])
    duplicates = bool(dupe_mask.any())
    if duplicates and not aggfunc:
        raise TransformError("Duplicates detected; choose aggregation")
    try:
        pivoted = work.pivot_table(index=index_cols, columns=col_key, values=values_from, aggfunc=aggfunc)
    except KeyError as exc:
        raise TransformError(str(exc))
    pivoted = pivoted.reset_index()
    # flatten columns if MultiIndex
    if isinstance(pivoted.columns, pd.MultiIndex):
        pivoted.columns = ["_".join([str(x) for x in tup if x != ""]).strip("_") for tup in pivoted.columns]
    pivoted, mapping, fixed = sanitise_columns(pivoted)
    warnings = []
    if duplicates:
        warnings.append("Duplicates aggregated using %s" % aggfunc)
    if fixed:
        warnings.append("Column names were sanitised after pivot")
    meta = {"duplicates_detected": duplicates, "mapping": mapping}
    return pivoted, meta, warnings


def transpose_df(df: pd.DataFrame, first_row_header: bool = True, first_col_index: bool = False):
    work = df.copy()
    if first_row_header and len(work) > 0:
        work = work.rename(columns=work.iloc[0]).drop(work.index[0])
    if first_col_index and len(work.columns) > 0:
        work = work.set_index(work.columns[0])
    transposed = work.transpose().reset_index()
    transposed, mapping, fixed = sanitise_columns(transposed)
    warnings = ["Column names were sanitised after transpose"] if fixed else []
    return transposed, {"mapping": mapping}, warnings


def reverse_split(df: pd.DataFrame, columns: List[str], separator: str, new_field_names: List[str], value_name: str = "value"):
    if not columns:
        raise TransformError("Select columns to split")
    if len(new_field_names) < 1:
        raise TransformError("Provide new field names")
    long = df.melt(id_vars=[c for c in df.columns if c not in columns], value_vars=columns, var_name="combined", value_name=value_name)
    parts = long["combined"].astype(str).str.split(separator, expand=True)
    if parts.shape[1] != len(new_field_names):
        raise TransformError("Separator not found consistently in column names")
    parts.columns = new_field_names
    result = pd.concat([long.drop(columns=["combined"]), parts], axis=1)
    result, mapping, fixed = sanitise_columns(result)
    warnings = ["Column names were sanitised after split"] if fixed else []
    return result, {"mapping": mapping}, warnings
