from __future__ import annotations
from PySide6.QtCore import QAbstractTableModel, Qt, QModelIndex
from PySide6.QtGui import QColor
from PySide6.QtWidgets import QTableView
import pandas as pd


class DataFrameModel(QAbstractTableModel):
    def __init__(self, df: pd.DataFrame | None = None):
        super().__init__()
        self._df = df if df is not None else pd.DataFrame()

    def set_dataframe(self, df: pd.DataFrame | None):
        self.beginResetModel()
        self._df = df if df is not None else pd.DataFrame()
        self.endResetModel()

    # model interface
    def rowCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self._df)

    def columnCount(self, parent=QModelIndex()):
        return 0 if parent.isValid() else len(self._df.columns)

    def data(self, index: QModelIndex, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        if role == Qt.DisplayRole:
            value = self._df.iat[index.row(), index.column()]
            if pd.isna(value):
                return ""
            return str(value)
        if role == Qt.BackgroundRole and index.row() % 2 == 1:
            return QColor(245, 245, 245)
        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None
        if orientation == Qt.Horizontal:
            try:
                return str(self._df.columns[section])
            except Exception:
                return ""
        return str(section + 1)


class DataTable(QTableView):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.model_df = DataFrameModel()
        self.setModel(self.model_df)
        self.setAlternatingRowColors(True)
        self.horizontalHeader().setStretchLastSection(True)

    def set_dataframe(self, df: pd.DataFrame | None):
        self.model_df.set_dataframe(df)

