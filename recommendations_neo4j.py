"""Validate Spark recommendation output and optionally publish it to Neo4j."""

import argparse
import json
import sqlite3
import tempfile
from contextlib import closing, contextmanager
from pathlib import Path
from time import perf_counter

from graph_data import video_id
from ingest_neo4j import connection_settings, save_report

WRITE = """
UNWIND $rows AS row
MATCH (video:Video {id: row.source})
SET video.m3_recommended_ids = CASE WHEN row.ids = [] THEN null ELSE row.ids END,
    video.m3_recommendation_support = CASE WHEN row.ids = [] THEN null ELSE row.support END
RETURN count(video) AS matched
"""
READ = """
UNWIND $rows AS row
MATCH (video:Video {id: row.source})
RETURN video.id AS source, coalesce(video.m3_recommended_ids, []) AS ids,
       coalesce(video.m3_recommendation_support, []) AS support
"""
MATCH_SOURCES = """
UNWIND $rows AS row
MATCH (video:Video {id: row.source})
RETURN count(DISTINCT video.id) AS matched
"""


def parse_result(line):
    row = json.loads(line)
    source = video_id(row["source"])
    previous = None
    targets, scores = [], []
    items = row["recommendations"]
    if not isinstance(items, list) or len(items) > 100:
        raise ValueError("expected a recommendation array with at most 100 entries")
    for rank, item in enumerate(items, 1):
        target = video_id(item["target"])
        score = item["support"]
        if (
            target == source
            or target in targets
            or type(score) is not int
            or not 0 < score < 2**63
            or type(item["rank"]) is not int
            or item["rank"] != rank
        ):
            raise ValueError("invalid target, support, or rank")
        key = (-score, target)
        if previous is not None and key <= previous:
            raise ValueError("recommendations are not in score and ID order")
        previous = key
        targets.append(target)
        scores.append(score)
    return source, targets, scores


@contextmanager
def stage_results(directory):
    """Check every source and target on disk before a database write."""
    if not (directory / "_SUCCESS").is_file():
        raise ValueError("Spark output is missing _SUCCESS")
    parts = sorted(directory.glob("part-*.json"))
    if not parts:
        raise ValueError("Spark output has no JSON part files")
    with tempfile.TemporaryDirectory(prefix="m3-recommendations-") as tmp:
        with closing(sqlite3.connect(Path(tmp) / "results.sqlite")) as db:
            db.execute("CREATE TABLE videos (id TEXT PRIMARY KEY, ids TEXT, support TEXT) WITHOUT ROWID")
            db.execute("CREATE TABLE targets (id TEXT PRIMARY KEY) WITHOUT ROWID")
            videos = recommendations = 0
            for path in parts:
                with path.open(encoding="utf-8") as stream:
                    for number, line in enumerate(stream, 1):
                        try:
                            source, ids, support = parse_result(line)
                            db.execute("INSERT INTO videos VALUES (?, ?, ?)", (source, json.dumps(ids), json.dumps(support)))
                            db.executemany("INSERT OR IGNORE INTO targets VALUES (?)", ((item,) for item in ids))
                        except (ValueError, TypeError, KeyError, sqlite3.IntegrityError) as exc:
                            raise ValueError(f"{path.name}:{number}: {exc}") from exc
                        videos += 1
                        recommendations += len(ids)
            unknown = db.execute(
                "SELECT id FROM targets WHERE NOT EXISTS (SELECT 1 FROM videos WHERE videos.id = targets.id) LIMIT 1"
            ).fetchone()
            if not videos or unknown:
                raise ValueError("Expected a nonempty result with every target in the source video set")
            db.commit()
            yield db, {"videos": videos, "recommendations": recommendations}


def batches(db, size):
    cursor = db.execute("SELECT id, ids, support FROM videos ORDER BY id")
    while chunk := cursor.fetchmany(size):
        yield [
            {"source": source, "ids": json.loads(ids), "support": json.loads(support)}
            for source, ids, support in chunk
        ]


def require_matches(tx, query, rows):
    count = tx.run(query, rows=rows).single(strict=True)["matched"]
    if count != len(rows):
        raise ValueError(f"Neo4j matched {count} of {len(rows)} source IDs")


def publish(session, db, counts, batch_size, apply):
    started = perf_counter()
    count = session.run("MATCH (v:Video) RETURN count(v) AS n").single(strict=True)["n"]
    if count != counts["videos"]:
        raise ValueError(f"Neo4j has {count} Video nodes, but the Spark output has {counts['videos']}")
    for rows in batches(db, batch_size):
        require_matches(session, MATCH_SOURCES, [{"source": row["source"]} for row in rows])
    checked = perf_counter()
    metrics = {"match_sources": round(checked - started, 3)}
    if apply:
        for rows in batches(db, batch_size):
            session.execute_write(require_matches, WRITE, rows)
        written = perf_counter()
        for rows in batches(db, batch_size):
            keys = [{"source": row["source"]} for row in rows]
            actual = {record["source"]: record for record in session.run(READ, rows=keys).data()}
            if len(actual) != len(rows) or any(actual.get(row["source"]) != row for row in rows):
                raise ValueError("Saved Neo4j recommendations differ from the Spark output")
        metrics["write"] = round(written - checked, 3)
        metrics["read_back"] = round(perf_counter() - written, 3)
    metrics["total"] = round(perf_counter() - started, 3)
    return metrics


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True, help="Downloaded or local Spark output directory")
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--apply", action="store_true", help="Write results after matching the database's Video IDs")
    parser.add_argument("--system-trust", action="store_true")
    args = parser.parse_args()
    if (
        args.batch_size < 1
        or args.report.exists()
        or not args.report.name.endswith(".json")
        or args.input.resolve() in args.report.resolve().parents
    ):
        parser.error("Use a positive batch size and a new JSON report outside the input directory")

    if args.system_trust:
        import truststore

        truststore.inject_into_ssl()

    from neo4j import GraphDatabase

    started = perf_counter()
    with stage_results(args.input) as (db, counts):
        staged = perf_counter()
        uri, auth, database = connection_settings()
        with GraphDatabase.driver(uri, auth=auth) as driver, driver.session(database=database) as session:
            metrics = publish(session, db, counts, args.batch_size, args.apply)
            info = session.run("CALL dbms.components() YIELD versions RETURN versions[0] AS version").single(strict=True)
        save_report(
            args.report,
            {
                "status": "published_and_validated" if args.apply else "source_ids_validated",
                **counts,
                "neo4j_version": info["version"],
                "batch_size": args.batch_size,
                "seconds": {
                    "stage_output": round(staged - started, 3),
                    **metrics,
                    "end_to_end": round(perf_counter() - started, 3),
                },
            },
        )


if __name__ == "__main__":
    main()
