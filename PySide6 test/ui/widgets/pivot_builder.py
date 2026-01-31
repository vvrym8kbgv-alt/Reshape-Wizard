from __future__ import annotations
from typing import List
from PySide6.QtWidgets import (
    QWidget, QVBoxLayout, QLabel, QListWidget, QLineEdit, QGridLayout, QMenu
)
from PySide6.QtCore import Qt, Signal


class DropList(QListWidget):
    items_changed = Signal()
    dropped = Signal(str, object)

    def __init__(self, title: str, single: bool = False, parent=None):
        super().__init__(parent)
        self.setAcceptDrops(True)
        self.setDragEnabled(True)
        self.setDragDropMode(QListWidget.InternalMove)
        self.title = title
        self.single = single
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self._open_menu)

    def dragEnterEvent(self, event):
        event.acceptProposedAction()

    def dragMoveEvent(self, event):
        event.acceptProposedAction()

    def dropEvent(self, event):
        super().dropEvent(event)
        if self.single and self.count() > 1:
            while self.count() > 1:
                self.takeItem(0)
        last_text = self.item(self.count() - 1).text() if self.count() else ""
        self.items_changed.emit()
        if last_text:
            self.dropped.emit(last_text, self)
        event.acceptProposedAction()

    def items(self) -> List[str]:
        return [self.item(i).text() for i in range(self.count())]

    def _open_menu(self, pos):
        item = self.itemAt(pos)
        if not item:
            return
        menu = QMenu(self)
        remove = menu.addAction("Remove")
        action = menu.exec(self.mapToGlobal(pos))
        if action == remove:
            row = self.row(item)
            self.takeItem(row)
            self.items_changed.emit()


class PivotBuilder(QWidget):
    layout_changed = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)
        layout = QVBoxLayout(self)
        self.search = QLineEdit()
        self.search.setPlaceholderText("Search variables…")
        self.vars_list = QListWidget()
        self.vars_list.setDragEnabled(True)

        grid = QGridLayout()
        self.rows_drop = DropList("Rows")
        self.columns_drop = DropList("Columns", single=True)
        self.values_drop = DropList("Values", single=True)
        self.combine_drop = DropList("Combine parts")
        grid.addWidget(QLabel("Rows"), 0, 0)
        grid.addWidget(self.rows_drop, 1, 0)
        grid.addWidget(QLabel("Columns (single)"), 0, 1)
        grid.addWidget(self.columns_drop, 1, 1)
        grid.addWidget(QLabel("Values (single)"), 2, 0)
        grid.addWidget(self.values_drop, 3, 0)
        grid.addWidget(QLabel("Combine parts"), 2, 1)
        grid.addWidget(self.combine_drop, 3, 1)
        layout.addWidget(QLabel("Variables"))
        layout.addWidget(self.search)
        layout.addWidget(self.vars_list, 1)
        layout.addLayout(grid)

        for dl in [self.rows_drop, self.columns_drop, self.values_drop, self.combine_drop]:
            dl.items_changed.connect(self._emit_layout_changed)

    def set_variables(self, names: List[str]):
        existing = set(self.rows_drop.items() + self.columns_drop.items() + self.values_drop.items() + self.combine_drop.items())
        self.vars_list.clear()
        for name in names:
            if name not in existing:
                self.vars_list.addItem(name)

    def get_layout(self):
        return {
            "rows": self.rows_drop.items(),
            "columns": self.columns_drop.items(),
            "values": self.values_drop.items(),
            "combine_parts": self.combine_drop.items(),
        }

    def remove_from_all(self, name: str):
        for lst in [self.rows_drop, self.columns_drop, self.values_drop, self.combine_drop]:
            for i in range(lst.count() - 1, -1, -1):
                if lst.item(i).text() == name:
                    lst.takeItem(i)
        self._emit_layout_changed()

    def add_to_list(self, target: DropList, name: str):
        if target.single:
            target.clear()
        target.addItem(name)
        self._emit_layout_changed()

    def _emit_layout_changed(self):
        self.layout_changed.emit()
