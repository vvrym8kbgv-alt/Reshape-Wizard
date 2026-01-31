# Reshape Wizard (MVP)

Offline Streamlit helper for quick table reshaping. Load CSV/XLSX, tidy merged cells, fill down, reshape longer/wider/transpose, undo steps, and export.

## Install
```bash
python -m venv .venv
source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

## Run
```bash
streamlit run app.py
```

## Supported inputs
- CSV (utf-8 then cp1251 fallback).
- XLSX/XLSM/XLS with sheet selection and header row choice (1–20). Duplicate headers are normalised as `Col`, `Col.2`, `Col.3`, etc.

## Unmerge & fill (XLSX only)
If a sheet has merged cells, enable **“Unmerge cells and fill values”** before loading. The app reads merged ranges, writes the top-left value into every cell of the range in memory, then unmerges so the resulting table is fully rectangular. A message shows how many merged ranges were filled.

## Typical operations
- **Make longer (melt):** Pick ID columns to keep, choose columns to stack (default = others), name the variable/value columns, and apply.
- **Make wider (pivot):** Choose index columns, column names source, and value source. If duplicates exist in the key combo, an aggregation (first/mean/sum/count/min/max) is required; otherwise a simple pivot is used. Optionally reset the index.
- **Transpose:** Optionally treat the first column as row names before transposing and use the first transposed row as headers.

## History & undo
- Every step is logged with shapes. Undo reverts the last step; Reset returns to the original load. History keeps the last 20 steps.

## Export
- Download CSV or XLSX (openpyxl). File names include the timestamp and last operation. Index export is optional.

## Preview & metadata
- Shows first 100 rows (50 for very large files), shape, dtypes, and top missing-value counts.

## Acceptance checks
1. Load XLSX with merged “Region” cells → unmerge+fill → fill-down → rectangular data.
2. Make longer on Jan..Dec columns → output has variable/value plus ID columns.
3. Make wider when duplicates exist → app forces aggregation and produces pivot_table.
4. Undo works across at least 5 steps.
5. Exported XLSX opens in Excel and matches the preview. 
