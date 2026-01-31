from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
import pandas as pd
from .history import HistoryManager, HistoryStep


@dataclass
class AppState:
    original_df: Optional[pd.DataFrame] = None
    current_df: Optional[pd.DataFrame] = None
    preview_df: Optional[pd.DataFrame] = None
    preview_meta: Dict[str, Any] = field(default_factory=dict)
    history: HistoryManager = field(default_factory=lambda: HistoryManager(max_steps=20))
    file_context: Dict[str, Any] = field(default_factory=dict)
    settings: Dict[str, Any] = field(default_factory=lambda: {
        "preview_rows": 100,
        "large_threshold": 200_000,
        "huge_warning": 5_000_000,
    })

    def commit_preview(self, name: str, params: Dict[str, Any]):
        if self.preview_df is None or self.current_df is None:
            return
        step = HistoryStep(
            name=name,
            params=params,
            before_shape=self.current_df.shape if self.current_df is not None else (0, 0),
            after_shape=self.preview_df.shape,
            df_before=self.current_df.copy(),
            df_after=self.preview_df.copy(),
            timestamp=pd.Timestamp.utcnow().to_pydatetime(),
        )
        self.current_df = self.preview_df
        self.preview_df = None
        self.preview_meta = {}
        self.history.push(step)

    def undo(self):
        step = self.history.undo()
        if step:
            self.current_df = step.df_before
        return step

    def redo(self):
        step = self.history.redo()
        if step:
            self.current_df = step.df_after
        return step
