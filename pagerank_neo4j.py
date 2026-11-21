"""Store Spark PageRank scores (spark_pagerank.py) on the matching Neo4j videos.

Scores come from the full dataset; only videos already in Neo4j are updated.
Videos in Neo4j without a score lose any old score, so after a run exactly the
scored videos carry pagerank and pagerank_rank properties.
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from itertools import islice
from pathlib import Path
from time import perf_counter

from graph_data import fingerprint, integer, number, read_csv, video_id
from ingest_neo4j import ROOT, connection_settings, save_report, setup_schema, write_batch

SCORE_COLUMNS = (
    "rank",
    "video_id",
    "score",
    "in_degree",
    "out_degree",
    "category",
    "views",
)
WRITE_QUERY = """
UNWIND $rows AS row
MATCH (v:Video {id: row.id})
SET v.pagerank = row.score, v.pagerank_rank = row.rank
RETURN count(v) AS processed
"""
CLEAR_QUERY = """
UNWIND $rows AS id
MATCH (v:Video {id: id})
REMOVE v.pagerank, v.pagerank_rank
RETURN count(v) AS processed
"""
SCORED_QUERY = "MATCH (v:Video) WHERE v.pagerank IS NOT NULL RETURN count(v) AS count"


def score_rows(path, known_ids, stats):
    """Yield validated {id, score, rank} rows for videos in known_ids."""
    seen = set()
    stats.setdefault("score_rows_read", 0)
    for line, row in read_csv(path, SCORE_COLUMNS):
        stats["score_rows_read"] += 1
        try:
            identity = video_id(row["video_id"])
            score = number(row["score"], "score", 0, 1)
            rank = integer(row["rank"], "rank")
            if rank < 1:
                raise ValueError("rank: expected a positive integer")
        except ValueError as exc:
            raise ValueError(f"{path}:{line}: {exc}") from exc
        if identity not in known_ids:
            continue
        if identity in seen:
            raise ValueError(f"{path}:{line}: duplicate score for {identity}")
        seen.add(identity)
        yield {"id": identity, "score": score, "rank": rank}


def chunks(rows, size):
    rows = iter(rows)
    while batch := list(islice(rows, size)):
        yield batch


def run(args):
    started = perf_counter()
    report = {
        "status": "running",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "batch_size": args.batch_size,
    }
    try:
        report["scores"] = fingerprint(args.scores)
        from neo4j import GraphDatabase

        uri, auth, database = connection_settings()
        report["database"] = database
        with GraphDatabase.driver(uri, auth=auth, connection_timeout=30) as driver:
            driver.verify_connectivity()
            with driver.session(database=database, fetch_size=10_000) as session:
                report["schema"] = setup_schema(session)
                known = set(session.run("MATCH (v:Video) RETURN v.id").value())
                if not known:
                    raise ValueError("Neo4j has no Video nodes; run ingest_neo4j.py first")
                stats = {"neo4j_videos": len(known)}
                updated = set()
                for batch in chunks(score_rows(args.scores, known, stats), args.batch_size):
                    session.execute_write(write_batch, WRITE_QUERY, batch)
                    updated.update(row["id"] for row in batch)
                    if len(updated) % (args.batch_size * 10) == 0:
                        print(f"{len(updated):,} videos scored", flush=True)
                unscored = sorted(known - updated)
                for batch in chunks(unscored, args.batch_size):
                    session.execute_write(write_batch, CLEAR_QUERY, batch)
                scored = session.run(SCORED_QUERY).single()["count"]
                stats.update(
                    videos_updated=len(updated),
                    neo4j_videos_without_score=len(unscored),
                    videos_with_pagerank=scored,
                )
                report["writeback"] = stats
                if scored != len(updated):
                    raise ValueError(
                        f"{scored} videos carry pagerank but {len(updated)} were updated"
                    )
                report["status"] = "passed"
    except Exception as exc:
        report["status"] = "failed"
        report["error_type"] = type(exc).__name__
        raise
    finally:
        report["total_seconds"] = perf_counter() - started
        save_report(args.report, report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scores", type=Path, default=ROOT / "Processed_Data/pagerank.csv")
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument(
        "--system-trust",
        action="store_true",
        help="Use the operating system's trusted TLS certificates (verification stays enabled)",
    )
    parser.add_argument(
        "--report", type=Path, default=ROOT / "Processed_Data/pagerank_neo4j_report.json"
    )
    args = parser.parse_args(argv)
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    protected = {path.resolve() for path in (args.scores, ROOT / "schema.cypher", ROOT / ".env")}
    if args.report.resolve() in protected:
        parser.error("--report must not overwrite the scores, schema, or credential file")
    try:
        if args.system_trust:
            import truststore

            truststore.inject_into_ssl()
        report = run(args)
    except Exception as exc:
        print(f"Failed: {exc}\nReport: {args.report}", file=sys.stderr)
        return 1
    print(f"{report['status']}: {report['writeback']} -> {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
