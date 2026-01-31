from __future__ import annotations
from typing import Callable, Dict, Any, Tuple
import pandas as pd


PreviewResult = Tuple[pd.DataFrame, Dict[str, Any], list[str]]


def run_preview(fn: Callable[[], PreviewResult], before_shape: tuple[int, int]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    after_df, meta, warnings = fn()
    meta = meta or {}
    meta.update({
        "before_shape": before_shape,
        "after_shape": after_df.shape,
        "warnings": warnings,
    })
    return after_df, meta

