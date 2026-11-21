"""Sprint 2: PageRank over the related-video graph with Spark RDD map/reduce.

The graph is built from Processed_Data/videos.csv with the same rules as
graph_data.prepare_csvs. PageRank then runs with RDD join, flatMap and
reduceByKey, using the same math as query_neo4j.pagerank so both agree.
Run it in Docker: docker compose -f compose.spark.yaml run --rm spark
"""

from __future__ import annotations

import argparse
import csv
import gc
import math
import sys
from collections import namedtuple
from datetime import datetime, timezone
from functools import reduce
from operator import add, itemgetter, or_
from pathlib import Path
from time import perf_counter

from graph_data import VIDEO_ID, fingerprint
from ingest_neo4j import save_report

ROOT = Path(__file__).resolve().parent
VIDEO_COLUMNS = ("video_id", "related_ids", "category", "views")
OUTPUT_COLUMNS = (
    "rank",
    "video_id",
    "score",
    "in_degree",
    "out_degree",
    "category",
    "views",
)
MAX_RELATED = 20
# Java-regex twin of graph_data.VIDEO_ID (Python's \Z is Java's \z).
SPARK_VIDEO_ID = "^" + VIDEO_ID.pattern.removesuffix(r"\Z") + r"\z"
REFERENCE_NODE_LIMIT = 500_000
REFERENCE_TOLERANCE = 1e-9

Graph = namedtuple("Graph", "nodes links in_degree meta stats")


def default_partitions(videos_path, parallelism):
    """About 16 MiB of CSV per partition, and at least two per core."""
    return max(2 * parallelism, math.ceil(Path(videos_path).stat().st_size / 2**24))


def build_graph(spark, videos_path, partitions):
    """Return a Graph whose links map node index -> tuple of target indexes.

    Follows graph_data.prepare_csvs: invalid rows stop the run, duplicate
    related IDs are dropped, and edges to videos outside the file are counted
    and omitted.
    """
    from pyspark import StorageLevel
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, StringType

    raw = spark.read.options(header=True, escape='"').csv(str(videos_path))
    columns = [name.lstrip("\ufeff") for name in raw.columns]
    missing = sorted(set(VIDEO_COLUMNS) - set(columns))
    if missing:
        raise ValueError(f"{videos_path}: missing columns {missing}")
    videos = (
        raw.toDF(*columns)
        .select(*VIDEO_COLUMNS)
        .withColumn("related", F.from_json("related_ids", ArrayType(StringType())))
        .drop("related_ids")
    )  # Not cached: the related-ID arrays are too wide; Spark rescans the CSV.

    def valid_id(column):
        return column.isNotNull() & column.rlike(SPARK_VIDEO_ID)

    problems = {
        "invalid_video_ids": ~valid_id(F.col("video_id")),
        "invalid_related_lists": F.col("related").isNull(),
        "too_many_related_ids": F.size("related") > MAX_RELATED,
        "invalid_related_ids": F.exists("related", lambda item: ~valid_id(item)),
    }
    distinct_related = F.array_distinct("related")
    summary = videos.agg(
        F.count(F.lit(1)).alias("rows"),
        (F.count("video_id") - F.countDistinct("video_id")).alias("duplicate_video_ids"),
        F.sum(F.size("related") - F.size(distinct_related)).alias("duplicate_related"),
        F.sum(F.size(distinct_related)).alias("candidate_edges"),
        *(
            F.sum(F.when(condition, 1).otherwise(0)).alias(name)
            for name, condition in problems.items()
        ),
    ).first()
    if not summary["rows"]:
        raise ValueError(f"{videos_path} contains no video records")
    failures = {
        name: summary[name]
        for name in (*problems, "duplicate_video_ids")
        if summary[name]
    }
    if failures:
        bad = reduce(or_, (F.coalesce(c, F.lit(False)) for c in problems.values()))
        sample = [row[0] for row in videos.filter(bad).select("video_id").take(5)]
        if not sample:
            duplicates = videos.groupBy("video_id").count().filter("count > 1")
            sample = [row[0] for row in duplicates.take(5)]
        raise ValueError(f"{videos_path}: {failures}; sample video IDs {sample}")

    # Dense integer node IDs keep the cached adjacency small. Caches below are
    # disk only: PySpark data is already serialized, the OS page cache still
    # keeps hot blocks in RAM, and the JVM heap stays free for shuffles.
    index = videos.select("video_id").rdd.map(itemgetter(0)).zipWithIndex()
    index_frame = spark.createDataFrame(index, "video_id string, node long").persist(
        StorageLevel.DISK_ONLY
    )
    edges = (
        videos.select(
            F.col("video_id").alias("source_id"),
            F.explode(distinct_related).alias("target_id"),
        )
        .join(
            index_frame.select(
                F.col("video_id").alias("target_id"), F.col("node").alias("dst")
            ),
            "target_id",
        )
        .join(
            index_frame.select(
                F.col("video_id").alias("source_id"), F.col("node").alias("src")
            ),
            "source_id",
        )
        .select("src", "dst")
    )
    # Every node gets a links entry; None marks nodes so dangling ones survive.
    links = (
        index_frame.select("node")
        .rdd.map(lambda row: (row[0], None))
        .union(edges.rdd.map(tuple))
        .groupByKey(partitions)
        .mapValues(lambda targets: tuple(sorted(t for t in targets if t is not None)))
    )
    checkpointed(links)
    in_degree = (
        links.flatMap(lambda item: ((target, 1) for target in item[1]))
        .reduceByKey(add, partitions)
    )
    checkpointed(in_degree)
    meta = (
        videos.join(index_frame, "video_id")
        .select("node", "video_id", "category", "views")
        .rdd.map(lambda row: (row[0], (row[1], row[2], row[3])))
    )
    checkpointed(meta)
    nodes = links.count()
    if nodes != summary["rows"] or meta.count() != nodes:
        raise ValueError("Graph build lost videos; node counts do not match")
    edge_count = in_degree.values().sum()
    dangling = links.filter(lambda item: not item[1])
    stats = {
        "videos": nodes,
        "edges": edge_count,
        "external_edges_omitted": summary["candidate_edges"] - edge_count,
        "duplicate_related_ids_removed": summary["duplicate_related"],
        "dangling_videos": dangling.count(),
        "isolated_videos": dangling.subtractByKey(in_degree, partitions).count(),
    }
    index_frame.unpersist()
    # The build's DataFrame shuffles are large; release them for cleanup.
    graph = Graph(nodes, detached(links), detached(in_degree), detached(meta), stats)
    del videos, index, index_frame, edges, links, in_degree, meta, dangling
    gc.collect()
    return graph


def checkpointed(rdd):
    """Cache on disk and mark for a local checkpoint, which cuts the lineage
    when the next job materializes the RDD. Returns the same RDD."""
    from pyspark import StorageLevel

    rdd.persist(StorageLevel.DISK_ONLY)
    rdd.localCheckpoint()
    return rdd


def detached(rdd):
    """Re-wrap a materialized, locally checkpointed RDD without its parents.

    A PySpark RDD object keeps its parent objects alive, and through them
    every older shuffle, so Spark's ContextCleaner cannot delete those shuffle
    files (about 1 GB per iteration on the full dataset) and the disk fills
    up. The checkpoint already cut the JVM lineage; this drops the Python one
    and keeps the partitioner.
    """
    from pyspark import RDD

    fresh = RDD(rdd._jrdd, rdd.context, rdd._jrdd_deserializer)
    fresh.partitioner = rdd.partitioner
    fresh.is_cached = rdd.is_cached
    return fresh


def spread(item):
    """Map step: send an equal share of a node's rank to each of its targets."""
    node, (targets, rank, _) = item
    yield node, 0.0  # keeps nodes without incoming links in the result
    if targets:
        share = rank / len(targets)
        for target in targets:
            yield target, share


def change_and_dangling(state):
    """Per node: (|new - previous| rank, rank held by a node with no out-links)."""
    targets, rank, previous = state
    return abs(rank - previous), 0.0 if targets else rank


def add_pairs(left, right):
    return left[0] + right[0], left[1] + right[1]


def pagerank_rdd(
    links,
    nodes,
    damping=0.85,
    tolerance=1e-9,
    max_iterations=200,
    partitions=None,
    progress=None,
):
    """Iterate PageRank on an RDD of (node, tuple_of_targets) covering every node.

    Same math as query_neo4j.pagerank: uniform start, dangling mass spread
    evenly, stop when the L1 change drops below tolerance. Returns
    (ranks_rdd, iterations, l1_errors, converged).
    """
    if nodes == 0:
        return links.context.emptyRDD(), 0, [], True
    partitions = partitions or links.getNumPartitions()
    # state: node -> (targets, rank, previous rank). state and the summed
    # contributions share one partitioner, so their join needs no shuffle;
    # only the contributions are shuffled each iteration.
    state = checkpointed(
        links.partitionBy(partitions).mapValues(
            lambda targets: (targets, 1.0 / nodes, 1.0 / nodes)
        )
    )
    dangling = state.filter(lambda item: not item[1][0]).count() / nodes
    state = detached(state)
    errors, converged, iteration = [], False, 0
    while iteration < max_iterations and not converged:
        iteration += 1
        started = perf_counter()
        base = (1 - damping + damping * dangling) / nodes
        updated = checkpointed(
            state.join(
                state.flatMap(spread).reduceByKey(add, partitions), partitions
            ).mapValues(
                lambda pair, base=base: (
                    pair[0][0],
                    base + damping * pair[1],
                    pair[0][1],
                )
            )
        )
        # One action per iteration: the L1 change and the next dangling mass.
        error, dangling = updated.values().map(change_and_dangling).reduce(add_pairs)
        state.unpersist()
        state = detached(updated)
        del updated
        # PySpark RDD objects sit in reference cycles, so only a cyclic GC
        # releases their JVM handles; then Spark's cleaner (periodic JVM GC)
        # deletes the old shuffle files.
        gc.collect()
        errors.append(error)
        converged = error < tolerance
        if progress:
            progress(iteration, error, perf_counter() - started)
    return state.mapValues(itemgetter(1)), iteration, errors, converged


def output_row(item):
    _, (((score, out_degree), in_degree), (video, category, views)) = item
    return score, video, in_degree or 0, out_degree, category, views


def write_scores(path, graph, ranks, partitions, top_k):
    """Write one CSV sorted by score; return output stats and the top-k rows."""
    rows = (
        ranks.join(graph.links.mapValues(len), partitions)
        .leftOuterJoin(graph.in_degree, partitions)
        .join(graph.meta, partitions)
        .map(output_row)
        .sortBy(lambda row: (-row[0], row[1]), numPartitions=partitions)
    )
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    top, total, count = [], 0.0, 0
    with temporary.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(OUTPUT_COLUMNS)
        for count, row in enumerate(rows.toLocalIterator(), 1):
            score, video, in_degree, out_degree, category, views = row
            writer.writerow((count, video, repr(score), *row[2:]))
            total += score
            if count <= top_k:
                top.append(
                    {
                        "rank": count,
                        "video_id": video,
                        "score": score,
                        "in_degree": in_degree,
                        "out_degree": out_degree,
                        "category": category,
                        "views": int(views) if views and views.isdigit() else views,
                    }
                )
    temporary.replace(path)
    return {"file": path.name, "rows": count, "score_sum": total}, top


def check_reference(graph, ranks, args):
    """Rerun the graph through sprint 1's pure-Python pagerank and compare."""
    if graph.nodes > REFERENCE_NODE_LIMIT:
        raise ValueError(
            f"--check-reference supports at most {REFERENCE_NODE_LIMIT:,} videos"
        )
    from query_neo4j import pagerank

    adjacency = sorted(graph.links.collect())
    nodes = [node for node, _ in adjacency]
    edges = [(node, target) for node, targets in adjacency for target in targets]
    expected, iterations = pagerank(
        nodes, edges, args.damping, args.tolerance, args.max_iterations
    )
    actual = ranks.collectAsMap()

    def top(scores):
        order = sorted(scores, key=lambda node: (-round(scores[node], 15), node))
        return order[: args.top_k]

    return {
        "videos": len(nodes),
        "reference_iterations": iterations,
        "max_abs_difference": max(
            (abs(actual[node] - expected[node]) for node in nodes), default=0.0
        ),
        "top_k_match": top(actual) == top(expected),
    }


def run(args, spark=None):
    started = perf_counter()
    report = {
        "status": "running",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "algorithm": "PageRank with Spark RDD join/flatMap/reduceByKey",
        "parameters": {
            "damping": args.damping,
            "tolerance": args.tolerance,
            "max_iterations": args.max_iterations,
        },
    }
    owned = spark is None
    try:
        if owned:
            from pyspark.sql import SparkSession

            spark = (
                SparkSession.builder.appName("info-crunch-pagerank")
                # Collect garbage often so old shuffle files are deleted promptly.
                .config("spark.cleaner.periodicGC.interval", "1min")
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("WARN")
        context = spark.sparkContext
        partitions = args.partitions or default_partitions(
            args.videos, context.defaultParallelism
        )
        report["spark"] = {
            "version": spark.version,
            "master": context.master,
            "default_parallelism": context.defaultParallelism,
            "partitions": partitions,
        }
        report["input"] = fingerprint(args.videos)
        seconds = report["seconds"] = {}

        phase = perf_counter()
        graph = build_graph(spark, args.videos, partitions)
        report["graph"] = graph.stats
        seconds["build_graph"] = perf_counter() - phase
        print(f"graph: {graph.stats}", flush=True)
        save_report(args.report, report)

        def progress(iteration, error, elapsed):
            print(f"iteration {iteration}: L1 change {error:.3e} ({elapsed:.1f} s)", flush=True)

        phase = perf_counter()
        ranks, iterations, errors, converged = pagerank_rdd(
            graph.links,
            graph.nodes,
            args.damping,
            args.tolerance,
            args.max_iterations,
            partitions,
            progress=progress,
        )
        seconds["pagerank"] = perf_counter() - phase
        report["pagerank"] = {
            "iterations": iterations,
            "converged": converged,
            "final_l1_change": errors[-1] if errors else 0.0,
            "seconds_per_iteration": seconds["pagerank"] / max(iterations, 1),
            "l1_changes": errors,
        }
        save_report(args.report, report)

        if args.check_reference:
            phase = perf_counter()
            report["reference_check"] = check_reference(graph, ranks, args)
            seconds["reference_check"] = perf_counter() - phase
            difference = report["reference_check"]["max_abs_difference"]
            if difference > REFERENCE_TOLERANCE:
                raise ValueError(f"Spark and reference scores differ by {difference}")

        phase = perf_counter()
        report["output"], report["top_k"] = write_scores(
            args.output, graph, ranks, partitions, args.top_k
        )
        seconds["write_output"] = perf_counter() - phase
        if abs(report["output"]["score_sum"] - 1) > 1e-6:
            raise ValueError(f"Scores sum to {report['output']['score_sum']}, not 1")
        report["status"] = "passed" if converged else "not_converged"
    except Exception as exc:
        report["status"] = "failed"
        report["error_type"] = type(exc).__name__
        report["error"] = str(exc)[:1000]
        raise
    finally:
        report["total_seconds"] = perf_counter() - started
        save_report(args.report, report)
        if owned and spark is not None:
            spark.stop()
    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--videos", type=Path, default=ROOT / "Processed_Data/videos.csv")
    parser.add_argument("--output", type=Path, default=ROOT / "Processed_Data/pagerank.csv")
    parser.add_argument(
        "--report", type=Path, default=ROOT / "Processed_Data/pagerank_report.json"
    )
    parser.add_argument("--damping", type=float, default=0.85)
    parser.add_argument("--tolerance", type=float, default=1e-9)
    parser.add_argument("--max-iterations", type=int, default=200)
    parser.add_argument(
        "--partitions", type=int, help="Default: twice Spark's default parallelism"
    )
    parser.add_argument("--top-k", type=int, default=10)
    parser.add_argument(
        "--check-reference",
        action="store_true",
        help=f"Also run query_neo4j.pagerank and compare (up to {REFERENCE_NODE_LIMIT:,} videos)",
    )
    args = parser.parse_args(argv)
    if not 0 < args.damping < 1:
        parser.error("--damping must be between 0 and 1")
    if not (math.isfinite(args.tolerance) and args.tolerance > 0):
        parser.error("--tolerance must be a positive number")
    if args.max_iterations < 1 or args.top_k < 0:
        parser.error("--max-iterations must be positive and --top-k nonnegative")
    if args.partitions is not None and args.partitions < 1:
        parser.error("--partitions must be positive")
    paths = {path.resolve() for path in (args.videos, args.output, args.report)}
    if len(paths) != 3:
        parser.error("--videos, --output and --report must be different files")
    return args


def main(argv=None):
    args = parse_args(argv)
    try:
        report = run(args)
    except Exception as exc:
        print(f"Failed: {exc}\nReport: {args.report}", file=sys.stderr)
        return 1
    pagerank = report["pagerank"]
    print(
        f"{report['status']}: {pagerank['iterations']} iterations, "
        f"{report['graph']['videos']:,} videos, {report['graph']['edges']:,} edges, "
        f"{report['total_seconds']:.0f} s -> {args.output}"
    )
    for row in report["top_k"]:
        print(f"  {row['rank']:>3} {row['video_id']} {row['score']:.6e} in-degree {row['in_degree']}")
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
