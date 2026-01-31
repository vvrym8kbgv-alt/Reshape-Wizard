# Reshape Wizard (PySide6)

Quick-start desktop tool for SPSS-like reshaping of tabular data with preview-first workflow.

## Install
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt  # see below
```

Minimal dependencies:
```txt
PySide6
pandas
openpyxl
pyperclip  # optional
```

## Run
```bash
python app.py
```

## Packaging (PyInstaller one-file)
```bash
pyinstaller --onefile --name reshape-wizard app.py
```
Add `--windowed` for GUI-only build. Include the `ui` and `engine` packages via automatic discovery; PySide6 hooks handle Qt libs.

## Current Features
- Load CSV/XLSX via button dialog (sheet + header row selection, optional merged-cell fill) and paste TSV from clipboard.
- Ask-before-fix column names modal (handles empty/Unnamed/duplicates).
- Preview-first + Apply for: preprocess (fill-down, merged-cell fill), make-longer, make-wider (duplicates + aggregation, combined parts), split combined columns, transpose.
- Drag-and-drop Pivot Builder (Rows, Columns, Values, Combine Parts, separator) wired into wider preview.
- Copy-to-clipboard (TSV) on every tab, export CSV/XLSX, undo/reset history (20 steps), status updates.
- Background preview worker to keep UI responsive.

## Limitations
- Reproducible code tab currently shows data previews only (recipe rendering TBD).
- Very large datasets may still feel heavy; previews cap at `preview_rows` in state settings.

## Quick demo workflow
1. Run the app and open a CSV/XLSX.
2. Use the *Make longer* tab: enter `id_col` in ID columns and value fields in Value columns, click **Preview** then **Apply**.
3. Try *Make wider*: set index columns, columns-from and values-from, choose aggregation if duplicates reported.
4. Use **Copy table** (Ctrl+C) to paste preview/current into Excel.

## Next steps
- Hook Pivot Builder selections into make-wider form and duplicate detection preview.
- Move previews to worker threads (QtConcurrent) with busy indicator.
- Add rename-columns dialog and ask-before-fix modal.
- Generate XLSX test data and automated acceptance tests.
