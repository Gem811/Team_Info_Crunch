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

## Spark PageRank (sprint 2)

`spark_pagerank.py` ranks every video by PageRank over the related-video graph.
It uses only Spark's RDD map/reduce methods (`flatMap`, `reduceByKey`, `join`)
and reads the full `Processed_Data/videos.csv`, which is more data than the
Neo4j Aura database holds. `pagerank_neo4j.py` then stores the scores on the
videos that are in Neo4j, so the graph database can query them.

How it works:

- **Graph build.** The rules are the same as `graph_data.prepare_csvs`:
  - invalid or duplicate video IDs, `related_ids` that is not a JSON array of
    at most 20 IDs, and invalid related IDs stop the run;
  - duplicate related IDs are dropped;
  - links to videos outside the file are counted and left out.

  Video IDs are mapped to dense integers with `zipWithIndex`.
- **Each iteration.** Every video's rank is split evenly across its related
  videos (`flatMap`), and the shares are summed per video (`reduceByKey`). The
  sums are joined back to the graph state; both use the same partitioner, so
  the join needs no shuffle. The new rank is
  `(1 - d + d * dangling) / N + d * sum`, where `dangling` is the rank held by
  videos with no links. The run stops when the total (L1) change falls below
  `--tolerance`. Each iteration's state is locally checkpointed and its
  Python-side lineage dropped. Without that, PySpark keeps every older shuffle
  file (about 1.4 GB per iteration on the full dataset), and the disk fills up.
- **Same results as sprint 1.** The math is the same as `query_neo4j.pagerank`,
  and `--check-reference` reruns that pure-Python version and compares the two.
  This works for inputs of up to 500K videos.

### Run it

Spark runs in Docker, so Windows needs no Java or Hadoop setup. Start Docker
Desktop; the first run pulls the Spark image (about 1 GB).

```powershell
docker compose -f compose.spark.yaml run --rm spark spark_pagerank.py
```

It writes:

- `Processed_Data/pagerank.csv` — one row per video, sorted by score:
  `rank, video_id, score, in_degree, out_degree, category, views`.
- `Processed_Data/pagerank_report.json` — graph statistics, iterations, the
  per-iteration change, timings and the top 10.

Options:

- `--videos`, `--output` and `--report` choose the files.
- `--damping` (default 0.85), `--tolerance` (default 1e-9) and
  `--max-iterations` (default 200) match sprint 1's defaults.
- `--partitions` sets the number of partitions. The default is about 16 MiB of
  CSV per partition.
- `--check-reference` compares the scores with sprint 1's `pagerank()`.
- `SPARK_DRIVER_MEMORY` (default `3g`) and `SPARK_MASTER` (default `local[8]`)
  are environment variables that tune the container.

Run time on the full dataset with 8 cores: the graph build takes about 3.5
minutes, each iteration about 45 seconds, and writing the CSV about 2 minutes.
The full run converges in 90 iterations, in about 75 minutes. Evidence from it
is in `docs/evidence/spark_pagerank.json`. A 300K-video `--check-reference`
run is in `docs/evidence/spark_pagerank_reference_check.json`: its largest
score difference from sprint 1's `pagerank()` is 3e-16, with the same top 10.

Memory: the defaults fit Docker Desktop's usual 8 GB VM. The full dataset
(7.78M videos, 81.7M edges) peaks at about 6–7 GB. Cached RDDs are kept on
disk, so the JVM heap stays free for shuffles. More cores need more VM memory:
24 cores with an 8 GB VM ran out of memory.

In Git Bash, set `MSYS_NO_PATHCONV=1` before passing container paths such as
`/scratch/...`.

Run the Spark tests in the container. On the host they are skipped, because
`SPARK_HOME` is not set:

```powershell
docker compose -f compose.spark.yaml run --rm --entrypoint python3 spark -m unittest discover -s tests -p "test_spark*.py"
```

### Store and query the scores in Neo4j

`pagerank_neo4j.py` uses the same `.env` credentials as `ingest_neo4j.py`. It
sets `pagerank` and `pagerank_rank` on every Neo4j video that has a score,
removes old scores from videos that have none, and adds the `video_pagerank`
index from `schema.cypher`. It writes its report to
`Processed_Data/pagerank_neo4j_report.json`.

```powershell
python pagerank_neo4j.py --system-trust
python query_neo4j.py --skip-pagerank   # includes the top_pagerank query
```
