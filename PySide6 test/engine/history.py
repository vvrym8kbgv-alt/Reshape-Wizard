from __future__ import annotations
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import pandas as pd
import datetime as _dt


@dataclass
class HistoryStep:
    name: str
    params: Dict[str, Any]
    before_shape: tuple[int, int]
    after_shape: tuple[int, int]
    df_before: pd.DataFrame
    df_after: pd.DataFrame
    timestamp: _dt.datetime


class HistoryManager:
    def __init__(self, max_steps: int = 20):
        self.max_steps = max_steps
        self.stack: List[HistoryStep] = []
        self.redo_stack: List[HistoryStep] = []

    def push(self, step: HistoryStep) -> None:
        self.stack.append(step)
        if len(self.stack) > self.max_steps:
            self.stack.pop(0)
        self.redo_stack.clear()

    def can_undo(self) -> bool:
        return bool(self.stack)

    def can_redo(self) -> bool:
        return bool(self.redo_stack)

    def undo(self) -> Optional[HistoryStep]:
        if not self.stack:
            return None
        step = self.stack.pop()
        self.redo_stack.append(step)
        return step

    def redo(self) -> Optional[HistoryStep]:
        if not self.redo_stack:
            return None
        step = self.redo_stack.pop()
        self.stack.append(step)
        return step
