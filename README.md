# Team_Info_Crunch

## Process the video datasets

Run the standard-library Python script from the repository root:

```powershell
python process_dataset.py
```

To add user metrics to an already-generated `Processed_Data/videos.csv`
without rebuilding it from all video source archives, run:

```powershell
python process_dataset.py --merge-user-info
```

By default, the same full-build command also reads `update.txt` files from
`Updating_Crawl_Data` and applies their newest valid metrics to matching video
records before writing `videos.csv`. This means one run combines the main video
data, optional file-size/bitrate enrichment, optional uploader information, and
crawl updates. Use `--crawl-updates-dir` to select a different crawl-update
folder; when the default folder is absent, crawl updates are skipped.

To apply incremental crawl metrics to an existing `Processed_Data/videos.csv`
without rebuilding it, run:

```powershell
python process_dataset.py --merge-crawl-updates
```

The incremental merge validates and reduces repeated updates to the newest
dated record per video ID. It updates only views, rate, ratings, and comments
for videos already in the CSV; videos without a matching existing row are
reported rather than appended because crawl updates do not contain the other
required video metadata.

The script reads every `.txt` data file and `.zip` archive below `Dataset_Files`
without extracting the archives. When `Data_FileSize_Bitrate` exists, it also
reads its size and bitrate records without extracting them. It ignores `log.txt`
files and processes all four source folders in one run. For large archives,
validated rows are written to SQLite in batches and duplicate-key tables avoid
the extra rowid index. This reduces per-record database overhead and temporary
storage while keeping the processing bounded in memory. Before matching the
millions of uploader records, the processor builds an index on video uploader
names so user-match reporting and output joins do not repeatedly scan all video
rows. It writes:

The command-line run prints progress every million source rows; call
`process_dataset()` directly with `show_progress=True` for the same updates.

- `Processed_Data/videos.csv` — one row per unique video, with the requested
  fields, plus `file_size_bytes`, `bitrate_kbps`, and `bitrate_method`.
  When `Datasets_User_Information` exists, it also joins uploader-level
  `uploader_uploads`, `uploader_watches`, and `uploader_friends` metrics by
  uploader name. The newest valid user snapshot is used; metrics absent from
  that snapshot are left blank.
  `related_ids` is a JSON array in the CSV cell. Invalid VBR bitrate values are
  calculated from file size and video length when both are available and match
  the processed video record.
- `Processed_Data/processing_report.json` — row counts and cleansing details.

To choose different folders, pass `--input-dir`, `--output-dir`,
`--enrichment-dir`, and `--user-info-dir`. The user loader reads `user.txt`
files only, ignores duplicate `userid.txt` lists and logs, validates numeric
metrics, reduces duplicate user rows to the newest dated snapshot, and reports
how many uploader records matched. It detects UTF-8 user files and supports
legacy Windows-1252 rows, with the selected file encodings included in the
report. The script checks required fields and numeric ranges, removes
invalid/duplicate related IDs, skips malformed rows, and keeps the newest
dated record for each video ID. For records with the same snapshot date it
prefers the record with the most related IDs, then the earliest source row.
Bitrates outside 0.1–100,000 kbps, non-finite values, and VBR markers are
treated as invalid; the report records the measurements rejected and derived.