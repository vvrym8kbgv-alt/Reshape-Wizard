from __future__ import annotations
import sys
from dataclasses import dataclass
from typing import List, Dict, Any, Callable

import pandas as pd
from PySide6.QtCore import Qt, QRunnable, QThreadPool, QObject, Signal
from PySide6.QtGui import QAction, QKeySequence
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QFileDialog, QTabWidget, QLabel, QDockWidget, QListWidget, QLineEdit,
    QMessageBox, QComboBox, QCheckBox, QFormLayout, QSpinBox, QTextEdit,
    QToolBar, QDialog, QDialogButtonBox, QTableWidget, QTableWidgetItem
)

from engine import io, transforms
from engine.clean_columns import sanitise_columns, apply_custom_mapping, is_invalid
from engine.state import AppState
from engine.preview import run_preview
from ui.widgets.data_table import DataTable
from ui.widgets.pivot_builder import PivotBuilder, DropList


class PreviewSignals(QObject):
    finished = Signal(object, dict)
    failed = Signal(str)


class PreviewWorker(QRunnable):
    def __init__(self, fn: Callable[[], tuple[pd.DataFrame, dict]]):
        super().__init__()
        self.fn = fn
        self.signals = PreviewSignals()

    def run(self):
        try:
            df, meta = self.fn()
            self.signals.finished.emit(df, meta)
        except Exception as exc:  # pragma: no cover - threaded
            self.signals.failed.emit(str(exc))


class OperationTab(QWidget):
    def __init__(self, name: str):
        super().__init__()
        self.setObjectName(name)
        layout = QVBoxLayout(self)
        self.form_box = QVBoxLayout()
        layout.addLayout(self.form_box)

        self.before_table = DataTable()
        self.after_table = DataTable()
        tables = QHBoxLayout()
        tables.addWidget(self.before_table)
        tables.addWidget(self.after_table)
        layout.addLayout(tables, 1)

        btns = QHBoxLayout()
        self.preview_btn = QPushButton("Preview")
        self.apply_btn = QPushButton("Apply")
        self.copy_btn = QPushButton("Copy table")
        self.apply_btn.setEnabled(False)
        btns.addWidget(self.preview_btn)
        btns.addWidget(self.apply_btn)
        btns.addWidget(self.copy_btn)
        layout.addLayout(btns)
        self.meta_label = QLabel()
        layout.addWidget(self.meta_label)

    def set_form(self, widget: QWidget | None):
        if widget:
            self.form_box.addWidget(widget)

    def set_before(self, df: pd.DataFrame | None, limit: int):
        df_display = df.head(limit) if df is not None and len(df) > limit else df
        self.before_table.set_dataframe(df_display)

    def set_after(self, df: pd.DataFrame | None, limit: int):
        df_display = df.head(limit) if df is not None and len(df) > limit else df
        self.after_table.set_dataframe(df_display)

    def set_meta(self, text: str):
        self.meta_label.setText(text)

    def set_busy(self, busy: bool):
        self.preview_btn.setEnabled(not busy)
        self.apply_btn.setEnabled(not busy and self.after_table.model_df.rowCount() > 0)


class ImportDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Load File")
        layout = QVBoxLayout(self)
        form = QFormLayout()
        self.path_edit = QLineEdit()
        browse = QPushButton("Browse…")
        browse.clicked.connect(self._choose)
        hb = QHBoxLayout()
        hb.addWidget(self.path_edit)
        hb.addWidget(browse)
        form.addRow("File", hb)
        self.sheet_combo = QComboBox()
        self.header_spin = QSpinBox()
        self.header_spin.setRange(1, 20)
        self.header_spin.setValue(1)
        self.fill_merged = QCheckBox("Fill merged cells (unmerge)")
        self.ask_fix = QCheckBox("Ask before fixing column names")
        self.ask_fix.setChecked(True)
        form.addRow("Sheet", self.sheet_combo)
        form.addRow("Header row (1-based)", self.header_spin)
        form.addRow(self.fill_merged)
        form.addRow(self.ask_fix)
        layout.addLayout(form)
        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        layout.addWidget(self.buttons)
        self.buttons.accepted.connect(self.accept)
        self.buttons.rejected.connect(self.reject)
        self.sheet_combo.setEnabled(False)
        self.header_spin.setEnabled(False)

    def _choose(self):
        path, _ = QFileDialog.getOpenFileName(self, "Open", filter="Data files (*.csv *.xlsx *.xlsm *.xlsb)")
        if not path:
            return
        self.path_edit.setText(path)
        if path.lower().endswith((".xlsx", ".xlsm", ".xlsb")):
            try:
                sheets = io.get_sheet_names(path)
            except Exception as exc:
                QMessageBox.critical(self, "Error", str(exc))
                return
            self.sheet_combo.clear()
            self.sheet_combo.addItems(sheets)
            self.sheet_combo.setCurrentIndex(0)
            self.sheet_combo.setEnabled(True)
            self.header_spin.setEnabled(True)
        else:
            self.sheet_combo.clear()
            self.sheet_combo.setEnabled(False)
            self.header_spin.setEnabled(False)

    def get_values(self):
        return {
            "path": self.path_edit.text().strip(),
            "sheet": self.sheet_combo.currentText() if self.sheet_combo.isEnabled() else None,
            "header_row": (self.header_spin.value() - 1) if self.header_spin.isEnabled() else 0,
            "fill_merged": self.fill_merged.isChecked(),
            "ask_fix": self.ask_fix.isChecked(),
        }


class RenameDialog(QDialog):
    def __init__(self, mapping: List[tuple[str, str]], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Confirm column names")
        layout = QVBoxLayout(self)
        self.table = QTableWidget(len(mapping), 2)
        self.table.setHorizontalHeaderLabels(["Old", "New"])
        for row, (old, new) in enumerate(mapping):
            self.table.setItem(row, 0, QTableWidgetItem(old))
            self.table.setItem(row, 1, QTableWidgetItem(new))
        layout.addWidget(self.table)
        self.buttons = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        self.buttons.accepted.connect(self._validate)
        self.buttons.rejected.connect(self.reject)
        layout.addWidget(self.buttons)
        self.result_mapping: List[tuple[str, str]] = mapping

    def _validate(self):
        new_names = []
        mapping: List[tuple[str, str]] = []
        for row in range(self.table.rowCount()):
            old = self.table.item(row, 0).text()
            new_item = self.table.item(row, 1)
            new = new_item.text().strip() if new_item else ""
            if not new or is_invalid(new):
                QMessageBox.warning(self, "Invalid", f"Column '{old}' must have a valid name")
                return
            new_names.append(new)
            mapping.append((old, new))
        if len(new_names) != len(set(new_names)):
            QMessageBox.warning(self, "Duplicates", "Column names must be unique")
            return
        self.result_mapping = mapping
        self.accept()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Reshape Wizard")
        self.resize(1400, 900)
        self.state = AppState()
        self.threadpool = QThreadPool.globalInstance()

        self._build_toolbar()
        self._build_tabs()
        self._build_docks()
        self._connect_signals()
        self._refresh_tables()

    # Toolbar & docks
    def _build_toolbar(self):
        tb = QToolBar("Main")
        self.addToolBar(tb)
        load_btn = QAction("Load File…", self)
        load_btn.triggered.connect(self.open_file_dialog)
        tb.addAction(load_btn)
        paste_btn = QAction("Paste from Clipboard", self)
        paste_btn.triggered.connect(self.paste_from_clipboard)
        tb.addAction(paste_btn)
        tb.addSeparator()
        undo_btn = QAction("Undo", self)
        undo_btn.setShortcut(QKeySequence.Undo)
        undo_btn.triggered.connect(self.undo)
        tb.addAction(undo_btn)
        reset_btn = QAction("Reset", self)
        reset_btn.triggered.connect(self.reset_state)
        tb.addAction(reset_btn)
        tb.addSeparator()
        export_csv = QAction("Export CSV", self)
        export_csv.triggered.connect(self.export_csv)
        export_xlsx = QAction("Export XLSX", self)
        export_xlsx.triggered.connect(self.export_xlsx)
        tb.addAction(export_csv)
        tb.addAction(export_xlsx)
        self.undo_act = undo_btn

    def _build_tabs(self):
        self.tabs = QTabWidget()
        self.preview_tab = OperationTab("Preview")
        self.preprocess_tab = OperationTab("Preprocess")
        self.longer_tab = OperationTab("Longer")
        self.wider_tab = OperationTab("Wider")
        self.split_tab = OperationTab("Split")
        self.transpose_tab = OperationTab("Transpose")
        self.code_tab = OperationTab("Recipe")
        self.code_tab.preview_btn.hide(); self.code_tab.apply_btn.hide()

        self.tabs.addTab(self.preview_tab, "Preview")
        self.tabs.addTab(self.preprocess_tab, "Preprocess")
        self.tabs.addTab(self.longer_tab, "Make longer")
        self.tabs.addTab(self.wider_tab, "Make wider")
        self.tabs.addTab(self.split_tab, "Split combined")
        self.tabs.addTab(self.transpose_tab, "Transpose")
        self.tabs.addTab(self.code_tab, "Reproducible code")
        container = QWidget()
        lay = QVBoxLayout(container)
        lay.addWidget(self.tabs)
        self.setCentralWidget(container)

        # Forms
        self._build_preprocess_form()
        self._build_longer_form()
        self._build_wider_form()
        self._build_split_form()
        self._build_transpose_form()

    def _build_docks(self):
        # Variables dock
        self.vars_dock = QDockWidget("Variables", self)
        vwidget = QWidget()
        vlayout = QVBoxLayout(vwidget)
        self.vars_search = QLineEdit()
        self.vars_search.setPlaceholderText("Search…")
        self.vars_list = QListWidget()
        self.vars_list.setDragEnabled(True)
        vlayout.addWidget(self.vars_search)
        vlayout.addWidget(self.vars_list)
        self.vars_dock.setWidget(vwidget)
        self.addDockWidget(Qt.LeftDockWidgetArea, self.vars_dock)

        # Pivot builder dock
        self.builder_dock = QDockWidget("Pivot Builder", self)
        bwidget = QWidget()
        blay = QVBoxLayout(bwidget)
        self.pivot_builder = PivotBuilder()
        blay.addWidget(self.pivot_builder)
        self.separator_edit = QLineEdit("_")
        blay.addWidget(QLabel("Separator for combined columns"))
        blay.addWidget(self.separator_edit)
        reset_layout_btn = QPushButton("Reset layout")
        reset_layout_btn.clicked.connect(self._reset_pivot_layout)
        blay.addWidget(reset_layout_btn)
        self.builder_dock.setWidget(bwidget)
        self.addDockWidget(Qt.RightDockWidgetArea, self.builder_dock)

    # Forms detail
    def _build_preprocess_form(self):
        form = QFormLayout()
        self.fill_columns_edit = QLineEdit()
        self.fill_treat_empty = QCheckBox("Treat empty strings as missing")
        self.fill_treat_empty.setChecked(True)
        self.unmerge_btn = QPushButton("Preview fill merged cells")
        form.addRow("Fill-down columns (comma)", self.fill_columns_edit)
        form.addRow("", self.fill_treat_empty)
        form.addRow(self.unmerge_btn)
        wrapper = QWidget(); wrapper.setLayout(form)
        self.preprocess_tab.set_form(wrapper)

    def _build_longer_form(self):
        form = QFormLayout()
        self.longer_id_cols = QLineEdit()
        self.longer_value_cols = QLineEdit()
        self.longer_var_name = QLineEdit("variable")
        self.longer_value_name = QLineEdit("value")
        form.addRow("ID columns", self.longer_id_cols)
        form.addRow("Value columns", self.longer_value_cols)
        form.addRow("Var name", self.longer_var_name)
        form.addRow("Value name", self.longer_value_name)
        wrapper = QWidget(); wrapper.setLayout(form)
        self.longer_tab.set_form(wrapper)

    def _build_wider_form(self):
        form = QFormLayout()
        self.wider_index_cols = QLineEdit()
        self.wider_columns_from = QLineEdit()
        self.wider_values_from = QLineEdit()
        self.wider_agg = QComboBox(); self.wider_agg.addItems(["", "first", "sum", "mean", "count", "min", "max"])
        form.addRow("Index (Rows)", self.wider_index_cols)
        form.addRow("Columns from (ignored if Combine Parts used)", self.wider_columns_from)
        form.addRow("Values from", self.wider_values_from)
        form.addRow("Aggregation (shown if duplicates)", self.wider_agg)
        wrapper = QWidget(); wrapper.setLayout(form)
        self.wider_tab.set_form(wrapper)

    def _build_split_form(self):
        form = QFormLayout()
        self.split_columns = QLineEdit()
        self.split_separator = QLineEdit("_")
        self.split_new_fields = QLineEdit("ScoreType,Year")
        form.addRow("Columns to split", self.split_columns)
        form.addRow("Separator", self.split_separator)
        form.addRow("New field names", self.split_new_fields)
        wrapper = QWidget(); wrapper.setLayout(form)
        self.split_tab.set_form(wrapper)

    def _build_transpose_form(self):
        form = QFormLayout()
        self.transpose_header = QCheckBox("Use first row as header")
        self.transpose_header.setChecked(True)
        self.transpose_first_col = QCheckBox("Use first column as index")
        form.addRow(self.transpose_header)
        form.addRow(self.transpose_first_col)
        wrapper = QWidget(); wrapper.setLayout(form)
        self.transpose_tab.set_form(wrapper)

    # Signal wiring
    def _connect_signals(self):
        self.preprocess_tab.preview_btn.clicked.connect(self.preview_fill_down)
        self.unmerge_btn.clicked.connect(self.preview_unmerge_fill)
        self.preprocess_tab.apply_btn.clicked.connect(lambda: self.apply_preview("Preprocess"))
        self.longer_tab.preview_btn.clicked.connect(self.preview_longer)
        self.longer_tab.apply_btn.clicked.connect(lambda: self.apply_preview("Make longer"))
        self.wider_tab.preview_btn.clicked.connect(self.preview_wider)
        self.wider_tab.apply_btn.clicked.connect(lambda: self.apply_preview("Make wider"))
        self.split_tab.preview_btn.clicked.connect(self.preview_split)
        self.split_tab.apply_btn.clicked.connect(lambda: self.apply_preview("Split combined"))
        self.transpose_tab.preview_btn.clicked.connect(self.preview_transpose)
        self.transpose_tab.apply_btn.clicked.connect(lambda: self.apply_preview("Transpose"))
        # copy buttons
        for tab in [self.preview_tab, self.preprocess_tab, self.longer_tab, self.wider_tab, self.split_tab, self.transpose_tab, self.code_tab]:
            tab.copy_btn.clicked.connect(self.copy_table)
        self.preview_tab.preview_btn.setEnabled(False)
        self.preview_tab.apply_btn.setEnabled(False)

        # variables search
        self.vars_search.textChanged.connect(self._filter_vars)
        self.vars_list.itemDoubleClicked.connect(self._add_var_to_rows)
        self.pivot_builder.layout_changed.connect(self._dedupe_pivot_layout)
        self.separator_edit.textChanged.connect(lambda _: self._dedupe_pivot_layout())
        for dl in [self.pivot_builder.rows_drop, self.pivot_builder.columns_drop, self.pivot_builder.values_drop, self.pivot_builder.combine_drop]:
            dl.dropped.connect(self._on_zone_drop)

    # Helpers
    def _filter_vars(self, text: str):
        for i in range(self.vars_list.count()):
            item = self.vars_list.item(i)
            item.setHidden(text.lower() not in item.text().lower())

    def _add_var_to_rows(self, item):
        name = item.text()
        self._place_variable(name, self.pivot_builder.rows_drop)

    def _reset_pivot_layout(self):
        for lst in [self.pivot_builder.rows_drop, self.pivot_builder.columns_drop, self.pivot_builder.values_drop, self.pivot_builder.combine_drop]:
            lst.clear()
        self._dedupe_pivot_layout()

    def _on_zone_drop(self, name: str, target: DropList):
        # When an item is dropped into a zone, it should be removed from other zones and kept here.
        for lst in [self.pivot_builder.rows_drop, self.pivot_builder.columns_drop, self.pivot_builder.values_drop, self.pivot_builder.combine_drop]:
            if lst is target:
                continue
            for i in range(lst.count() - 1, -1, -1):
                if lst.item(i).text() == name:
                    lst.takeItem(i)
        if target.single and target.count() > 1:
            # keep latest drop
            while target.count() > 1:
                target.takeItem(0)
        self._dedupe_pivot_layout()

    def _place_variable(self, name: str, target: DropList):
        self.pivot_builder.remove_from_all(name)
        if target.single:
            target.clear()
        target.addItem(name)
        self._dedupe_pivot_layout()

    def _dedupe_pivot_layout(self):
        seen = set()
        for lst in [self.pivot_builder.rows_drop, self.pivot_builder.columns_drop, self.pivot_builder.values_drop, self.pivot_builder.combine_drop]:
            for i in range(lst.count() - 1, -1, -1):
                txt = lst.item(i).text()
                if txt in seen:
                    lst.takeItem(i)
                else:
                    seen.add(txt)
        self._refresh_status()

    # Import / paste
    def open_file_dialog(self):
        dlg = ImportDialog(self)
        if dlg.exec() != QDialog.Accepted:
            return
        vals = dlg.get_values()
        path = vals["path"]
        if not path:
            return
        try:
            df, ctx = io.load_file_via_context(path, sheet_name=vals["sheet"], header_row=vals["header_row"], unmerge_fill=vals["fill_merged"])
        except Exception as exc:
            QMessageBox.critical(self, "Load error", str(exc))
            return
        df = self._maybe_ask_fix(df, vals["ask_fix"])
        self.state.original_df = df.copy()
        self.state.current_df = df
        self.state.preview_df = None
        self.state.preview_meta = {}
        self.state.file_context = ctx.__dict__
        self.state.history.stack.clear()
        self.state.history.redo_stack.clear()
        self._refresh_tables()

    def paste_from_clipboard(self):
        text = QApplication.clipboard().text()
        if not text:
            QMessageBox.information(self, "Clipboard", "Clipboard is empty")
            return
        try:
            df = io.parse_clipboard_tsv(text)
        except Exception as exc:
            QMessageBox.critical(self, "Paste error", str(exc))
            return
        df = self._maybe_ask_fix(df, ask=True)  # prompt by default when pasting
        self.state.original_df = df.copy()
        self.state.current_df = df
        self.state.preview_df = None
        self.state.history.stack.clear(); self.state.history.redo_stack.clear()
        self.state.file_context = {"path": None, "ext": "tsv", "bytes_data": text.encode("utf-8"), "sheet_name": None, "header_row": 0}
        self._refresh_tables()

    def _maybe_ask_fix(self, df: pd.DataFrame, ask: bool) -> pd.DataFrame:
        df_fixed, mapping, fixed = sanitise_columns(df)
        if not fixed or not ask:
            return df_fixed
        dlg = RenameDialog(mapping, self)
        if dlg.exec() == QDialog.Accepted:
            return apply_custom_mapping(df, dlg.result_mapping)
        return df_fixed

    # Export / copy
    def export_csv(self):
        if self.state.current_df is None:
            return
        path, _ = QFileDialog.getSaveFileName(self, "Export CSV", filter="CSV files (*.csv)")
        if not path:
            return
        self.state.current_df.to_csv(path, index=False)
        self.statusBar().showMessage(f"Saved {path}")

    def export_xlsx(self):
        if self.state.current_df is None:
            return
        path, _ = QFileDialog.getSaveFileName(self, "Export XLSX", filter="Excel files (*.xlsx)")
        if not path:
            return
        self.state.current_df.to_excel(path, index=False)
        self.statusBar().showMessage(f"Saved {path}")

    def copy_table(self):
        df = self.state.preview_df if self.state.preview_df is not None else self.state.current_df
        if df is None:
            return
        cells = df.shape[0] * df.shape[1]
        if cells > 200_000:
            QMessageBox.warning(self, "Copy", "Table too large to copy (over 200k cells)")
            return
        QApplication.clipboard().setText(df.to_csv(sep="\t", index=False))
        self.statusBar().showMessage("Copied table to clipboard")

    # History
    def undo(self):
        step = self.state.undo()
        if step:
            self.state.preview_df = None
            self.state.preview_meta = {}
            self._refresh_tables()

    def reset_state(self):
        if self.state.original_df is None:
            return
        self.state.current_df = self.state.original_df.copy()
        self.state.preview_df = None
        self.state.preview_meta = {}
        self.state.history.stack.clear(); self.state.history.redo_stack.clear()
        self._refresh_tables()

    # Preview helpers
    def _run_preview_async(self, fn: Callable[[], tuple[pd.DataFrame, dict]]):
        worker = PreviewWorker(fn)
        for tab in [self.preview_tab, self.preprocess_tab, self.longer_tab, self.wider_tab, self.split_tab, self.transpose_tab]:
            tab.set_busy(True)
        self.statusBar().showMessage("Computing preview…")
        worker.signals.finished.connect(self._on_preview_finished)
        worker.signals.failed.connect(self._on_preview_failed)
        self.threadpool.start(worker)

    def _on_preview_finished(self, df: pd.DataFrame, meta: dict):
        self.state.preview_df = df
        self.state.preview_meta = meta
        self._refresh_tables()
        for tab in [self.preview_tab, self.preprocess_tab, self.longer_tab, self.wider_tab, self.split_tab, self.transpose_tab]:
            tab.set_busy(False)
        self.statusBar().clearMessage()

    def _on_preview_failed(self, message: str):
        for tab in [self.preview_tab, self.preprocess_tab, self.longer_tab, self.wider_tab, self.split_tab, self.transpose_tab]:
            tab.set_busy(False)
        self.statusBar().clearMessage()
        QMessageBox.critical(self, "Preview error", message)

    def apply_preview(self, name: str):
        if self.state.preview_df is None:
            return
        self.state.commit_preview(name=name, params=self.state.preview_meta)
        self._refresh_tables()

    def _ensure_data(self) -> bool:
        if self.state.current_df is None:
            QMessageBox.information(self, "No data", "Load data first")
            return False
        return True

    # Specific previews
    def preview_fill_down(self):
        if not self._ensure_data():
            return
        columns = [c.strip() for c in self.fill_columns_edit.text().split(',') if c.strip()]
        if not columns:
            QMessageBox.information(self, "Fill-down", "Specify columns")
            return
        treat_empty = self.fill_treat_empty.isChecked()
        before_shape = self.state.current_df.shape
        def run():
            df, meta, warnings = transforms.fill_down(self.state.current_df, columns, treat_empty)
            meta = meta or {}
            meta.update({"warnings": warnings, "before_shape": before_shape, "after_shape": df.shape})
            return df, meta
        self._run_preview_async(run)

    def preview_unmerge_fill(self):
        ctx = self.state.file_context
        if not ctx or ctx.get("ext") not in ("xlsx", "xlsm", "xlsb"):
            QMessageBox.information(self, "Unmerge", "Only available for XLSX sources")
            return
        sheet = ctx.get("sheet_name")
        header_row = ctx.get("header_row", 0)
        path_or_bytes = ctx.get("path") or ctx.get("bytes_data")
        before_shape = self.state.current_df.shape if self.state.current_df is not None else (0, 0)
        def run():
            df, meta = io.unmerge_fill_xlsx(path_or_bytes, sheet, header_row)
            meta.update({"before_shape": before_shape, "after_shape": df.shape, "warnings": []})
            return df, meta
        self._run_preview_async(run)

    def preview_longer(self):
        if not self._ensure_data():
            return
        ids = [c.strip() for c in self.longer_id_cols.text().split(',') if c.strip()]
        vals = [c.strip() for c in self.longer_value_cols.text().split(',') if c.strip()]
        var_name = self.longer_var_name.text() or "variable"
        value_name = self.longer_value_name.text() or "value"
        before_shape = self.state.current_df.shape
        def run():
            df, meta, warnings = transforms.make_longer(self.state.current_df, ids, vals, var_name, value_name)
            meta.update({"before_shape": before_shape, "after_shape": df.shape, "warnings": warnings})
            return df, meta
        self._run_preview_async(run)

    def preview_wider(self):
        if not self._ensure_data():
            return
        layout = self.pivot_builder.get_layout()
        combine_parts = layout["combine_parts"]
        columns_zone = layout["columns"]
        cols_from = combine_parts if combine_parts else columns_zone
        if not cols_from:
            # fallback to manual entry
            cols_from = [c.strip() for c in self.wider_columns_from.text().split(',') if c.strip()]
        values_from = layout["values"][:1] if layout["values"] else []
        if not values_from:
            v_manual = self.wider_values_from.text().strip()
            if v_manual:
                values_from = [v_manual]
        if not values_from:
            QMessageBox.information(self, "Make wider", "Choose a Values field")
            return
        idx = layout["rows"] if layout["rows"] else [c.strip() for c in self.wider_index_cols.text().split(',') if c.strip()]
        separator = self.separator_edit.text() or "_"
        agg = self.wider_agg.currentText() or None
        before_shape = self.state.current_df.shape
        def run():
            work_df = self.state.current_df.copy()
            # create combined column if needed
            if combine_parts:
                work_df["_combined_wider"] = work_df[combine_parts].astype(str).agg(separator.join, axis=1)
                cols_actual = ["_combined_wider"]
            else:
                cols_actual = cols_from
            df, meta, warnings = transforms.make_wider(work_df, idx, cols_actual, values_from[0], agg)
            meta.update({"before_shape": before_shape, "after_shape": df.shape, "warnings": warnings, "used_combine_parts": combine_parts})
            return df, meta
        self._run_preview_async(run)

    def preview_split(self):
        if not self._ensure_data():
            return
        columns = [c.strip() for c in self.split_columns.text().split(',') if c.strip()]
        sep = self.split_separator.text()
        new_fields = [c.strip() for c in self.split_new_fields.text().split(',') if c.strip()]
        before_shape = self.state.current_df.shape
        def run():
            df, meta, warnings = transforms.reverse_split(self.state.current_df, columns, sep, new_fields)
            meta.update({"before_shape": before_shape, "after_shape": df.shape, "warnings": warnings})
            return df, meta
        self._run_preview_async(run)

    def preview_transpose(self):
        if not self._ensure_data():
            return
        use_header = self.transpose_header.isChecked()
        use_first_col = self.transpose_first_col.isChecked()
        before_shape = self.state.current_df.shape
        def run():
            df, meta, warnings = transforms.transpose_df(self.state.current_df, use_header, use_first_col)
            meta.update({"before_shape": before_shape, "after_shape": df.shape, "warnings": warnings})
            return df, meta
        self._run_preview_async(run)

    # Refresh
    def _refresh_tables(self):
        limit = self.state.settings["preview_rows"]
        tabs = [self.preview_tab, self.preprocess_tab, self.longer_tab, self.wider_tab, self.split_tab, self.transpose_tab, self.code_tab]
        for tab in tabs:
            tab.set_before(self.state.current_df, limit)
            tab.set_after(self.state.preview_df, limit)
            meta = self.state.preview_meta
            meta_text = ""
            if meta:
                meta_text = f"Before {meta.get('before_shape')} → After {meta.get('after_shape')}"
                warnings = meta.get("warnings")
                if warnings:
                    meta_text += " | " + "; ".join(warnings)
            tab.set_meta(meta_text)
            tab.apply_btn.setEnabled(self.state.preview_df is not None)
        self._refresh_vars_list()
        self._refresh_status()
        self.undo_act.setEnabled(self.state.history.can_undo())

    def _refresh_vars_list(self):
        self.vars_list.clear()
        if self.state.current_df is None:
            return
        names = list(self.state.current_df.columns)
        layout = self.pivot_builder.get_layout()
        used = set(layout["rows"] + layout["columns"] + layout["values"] + layout["combine_parts"])
        for name in names:
            if name not in used:
                self.vars_list.addItem(name)
        self.pivot_builder.set_variables(names)

    def _refresh_status(self):
        if self.state.current_df is None:
            self.statusBar().showMessage("No data loaded")
            return
        msg = f"Current shape: {self.state.current_df.shape}"
        if self.state.preview_df is not None:
            msg += f" | Preview: {self.state.preview_df.shape}"
        self.statusBar().showMessage(msg)


def main():
    app = QApplication(sys.argv)
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
