"""Rank new related videos using directed two-hop paths in the B1/B2 graph."""

import argparse
import json
from pathlib import Path
from time import perf_counter

from pyspark.sql import SparkSession, Window, functions as F

from graph_data import fingerprint

ID_PATTERN = r"^[A-Za-z0-9_-]{11}$"
MAX_TOP_K = 100


def validate_graph(nodes, edges):
    """Check the projected IDs and edge endpoints before computing results."""
    node_stats = nodes.agg(
        F.count("*").alias("rows"),
        F.count_distinct("id").alias("distinct_ids"),
        F.count_if(~F.coalesce(F.col("id").rlike(ID_PATTERN), F.lit(False))).alias("invalid"),
    ).first()
    if not node_stats.rows or node_stats.invalid or node_stats.rows != node_stats.distinct_ids:
        raise ValueError("nodes.csv must contain unique, valid Video IDs")

    edge_stats = edges.agg(
        F.count("*").alias("rows"),
        F.count_if(F.col("source") == F.col("target")).alias("self_loops"),
    ).first()
    endpoints = edges.select(F.col("source").alias("id")).union(edges.select("target")).distinct()
    if endpoints.join(nodes, "id", "left_anti").take(1):
        raise ValueError("edges.csv contains a missing or invalid endpoint")
    return {
        "videos": node_stats.rows,
        "distinct_edges": edge_stats.rows,
        "self_loops_excluded": edge_stats.self_loops,
    }


def rank_candidates(edges, top_k):
    """Return source, target, support and rank from distinct, non-self edges."""
    first = edges.select("source", F.col("target").alias("via"))
    second = edges.select(F.col("source").alias("via"), "target")
    candidates = (
        first.join(second, "via")
        .where(F.col("source") != F.col("target"))
        .groupBy("source", "target")
        .agg(F.count("*").alias("support"))
        .join(edges, ["source", "target"], "left_anti")
    )
    ordering = Window.partitionBy("source").orderBy(F.desc("support"), "target")
    return candidates.withColumn("rank", F.row_number().over(ordering)).where(
        F.col("rank") <= top_k
    )


def pack_results(nodes, ranked):
    """Include every video so a new run can remove stale recommendations."""
    grouped = ranked.groupBy("source").agg(
        F.sort_array(F.collect_list(F.struct("rank", "target", "support"))).alias("recommendations")
    )
    return nodes.select(F.col("id").alias("source")).join(grouped, "source", "left").select(
        "source", F.coalesce("recommendations", F.array()).alias("recommendations")
    )


def describe_input(location):
    if Path(location).is_file():
        return fingerprint(location)
    return {"path": location}


def write_results(results, args):
    if not args.local_output:
        results.write.mode("errorifexists").json(args.output)
        return

    # Hadoop's native Windows file writer is optional for a local run.
    folder = Path(args.output)
    folder.mkdir(parents=True, exist_ok=False)
    encoded = results.select(F.to_json(F.struct("source", "recommendations")).alias("json"))
    temporary = folder / "results.tmp"
    with temporary.open("w", encoding="utf-8", newline="\n") as stream:
        for row in encoded.toLocalIterator():
            stream.write(row.json + "\n")
    temporary.replace(folder / "part-00000.json")
    (folder / "_SUCCESS").touch()


def run(spark, args):
    started = perf_counter()
    options = {"header": True, "mode": "FAILFAST", "escape": '"'}
    nodes = spark.read.options(**options).csv(args.nodes).select("id").persist()
    distinct_edges = (
        spark.read.options(**options)
        .csv(args.edges)
        .select("source", "target")
        .dropDuplicates()
        .persist()
    )
    ranked = None
    try:
        graph = validate_graph(nodes, distinct_edges)
        validated = perf_counter()
        edges = distinct_edges.where(F.col("source") != F.col("target"))
        ranked = rank_candidates(edges, args.top_k).persist()
        result_stats = ranked.agg(
            F.count("*").alias("rows"),
            F.count_distinct("source").alias("videos_with_recommendations"),
            F.max("support").alias("max_support"),
        ).first().asDict()
        preview = [
            row.asDict()
            for row in ranked.orderBy(F.desc("support"), "source", "rank").take(5)
        ]
        ranked_at = perf_counter()
        write_results(pack_results(nodes, ranked), args)
        written = perf_counter()
        report = {
            "status": "passed",
            "input": {
                "nodes": describe_input(args.nodes),
                "edges": describe_input(args.edges),
            },
            "spark": {
                "version": spark.version,
                "master": spark.sparkContext.master,
                "shuffle_partitions": spark.conf.get("spark.sql.shuffle.partitions"),
            },
            "top_k": args.top_k,
            "output_mode": "local_stream" if args.local_output else "spark_json",
            "graph": graph,
            "results": result_stats,
            "sample": preview,
            "seconds": {
                "read_and_validate": round(validated - started, 3),
                "rank_and_summarize": round(ranked_at - validated, 3),
                "write_results": round(written - ranked_at, 3),
                "total": round(written - started, 3),
            },
        }
    finally:
        if ranked is not None:
            ranked.unpersist()
        distinct_edges.unpersist()
        nodes.unpersist()
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", default="Processed_Data/nodes.csv")
    parser.add_argument("--edges", default="Processed_Data/edges.csv")
    parser.add_argument("--output", required=True, help="New Spark output directory")
    parser.add_argument("--report", required=True, type=Path, help="New local JSON report")
    parser.add_argument("--local-output", action="store_true", help="Stream output to a local directory without Hadoop's native file writer")
    parser.add_argument("--top-k", default=5, type=int)
    parser.add_argument("--partitions", type=int, help="Override Spark shuffle partitions")
    parser.add_argument("--master", help="Spark master, set by spark-submit on a cluster")
    args = parser.parse_args(argv)
    if not 1 <= args.top_k <= MAX_TOP_K or (args.partitions is not None and args.partitions < 1):
        parser.error(f"Use top-k in 1 to {MAX_TOP_K} and a positive partition count")
    if args.report.exists() or Path(args.output).resolve() in args.report.resolve().parents:
        parser.error("Use a new report path outside the Spark output directory")
    args.report.parent.mkdir(parents=True, exist_ok=True)

    builder = SparkSession.builder.appName("Info Crunch two-hop recommendations")
    if args.master:
        builder = builder.master(args.master)
    if args.partitions:
        builder = builder.config("spark.sql.shuffle.partitions", args.partitions)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        report = run(spark, args)
    finally:
        spark.stop()
    with args.report.open("x", encoding="utf-8") as stream:
        json.dump(report, stream, indent=2)
        stream.write("\n")
    print(f"Saved {report['results']['rows']:,} recommendations. Report at {args.report}")


if __name__ == "__main__":
    main()
