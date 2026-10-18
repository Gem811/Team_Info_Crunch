"""B1/B2: set up Neo4j, ingest validated CSVs, and record measured evidence."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter
from urllib.parse import urlsplit

from graph_data import batches, prepare_csvs, stage_csvs

ROOT = Path(__file__).resolve().parent
NODE_QUERY = """
UNWIND $rows AS row
MERGE (v:Video {id: row.id})
SET v += row.properties
RETURN count(v) AS processed
"""
EDGE_QUERY = """
UNWIND $rows AS row
MATCH (source:Video {id: row.source})
MATCH (target:Video {id: row.target})
MERGE (source)-[:RELATED_TO]->(target)
RETURN count(*) AS processed
"""


def save_report(path, report):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def connection_settings():
    from dotenv import load_dotenv

    load_dotenv(ROOT / ".env", override=False)
    keys = ("NEO4J_URI", "NEO4J_USERNAME", "NEO4J_PASSWORD")
    missing = [key for key in keys if not os.getenv(key)]
    if missing:
        raise ValueError(
            "Set "
            + ", ".join(missing)
            + " in environment variables or the ignored .env file"
        )
    uri = os.environ["NEO4J_URI"]
    parsed = urlsplit(uri)
    if (
        parsed.scheme not in ("bolt", "bolt+s", "neo4j", "neo4j+s")
        or not parsed.hostname
        or parsed.username
        or parsed.password
    ):
        raise ValueError(
            "Use a bolt/neo4j URI without embedded credentials (Aura requires neo4j+s)"
        )
    return (
        uri,
        (os.environ["NEO4J_USERNAME"], os.environ["NEO4J_PASSWORD"]),
        os.getenv("NEO4J_DATABASE", "neo4j"),
    )


def inspect_schema(session):
    constraints = session.run(
        "SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties RETURN name, type, labelsOrTypes, properties"
    ).data()
    if not any(
        row["type"] in ("UNIQUENESS", "NODE_PROPERTY_UNIQUENESS")
        and row["labelsOrTypes"] == ["Video"]
        and row["properties"] == ["id"]
        for row in constraints
    ):
        raise ValueError("Video.id uniqueness constraint is missing")
    indexes = session.run(
        "SHOW INDEXES YIELD name, state, type, entityType, labelsOrTypes, properties RETURN name, state, type, entityType, labelsOrTypes, properties"
    ).data()
    for field in ("id", "category", "views", "file_size_bytes", "length"):
        if not any(
            row["state"] == "ONLINE"
            and row["type"] == "RANGE"
            and row["entityType"] == "NODE"
            and row["labelsOrTypes"] == ["Video"]
            and row["properties"] == [field]
            for row in indexes
        ):
            raise ValueError(f"Online range index on Video.{field} is missing")
    return {"constraints": constraints, "indexes": indexes}


def setup_schema(session):
    source = (ROOT / "schema.cypher").read_text(encoding="utf-8")
    source = "\n".join(
        line for line in source.splitlines() if not line.lstrip().startswith("//")
    )
    for statement in source.split(";"):
        if statement.strip():
            session.run(statement).consume()
    session.run("CALL db.awaitIndexes(300)").consume()
    return inspect_schema(session)


def counts(session):
    nodes = session.run("MATCH (v:Video) RETURN count(v) AS count").single()["count"]
    edges = session.run(
        "MATCH ()-[r:RELATED_TO]->() RETURN count(r) AS count"
    ).single()["count"]
    return {"nodes": nodes, "edges": edges}


def write_batch(tx, query, rows):
    result = tx.run(query, rows=rows)
    processed = result.single(strict=True)["processed"]
    summary = result.consume()
    if processed != len(rows):
        # Raising inside the callback rolls back this entire batch.
        raise ValueError(
            f"Database matched {processed} of {len(rows)} rows; check endpoints and duplicate relationships"
        )
    return summary.counters.nodes_created, summary.counters.relationships_created


def load_phase(session, connection, table, batch_size):
    started = perf_counter()
    processed = created_nodes = created_edges = batch_count = 0
    for batch in batches(connection, table, batch_size):
        nodes, edges = session.execute_write(
            write_batch, NODE_QUERY if table == "nodes" else EDGE_QUERY, batch
        )
        created_nodes += nodes
        created_edges += edges
        processed += len(batch)
        batch_count += 1
        if batch_count % 10 == 0:
            print(f"{table}: {processed:,} records committed", flush=True)
    seconds = perf_counter() - started
    return {
        "records": processed,
        "batches": batch_count,
        "seconds": seconds,
        "records_per_second": processed / seconds if seconds else 0,
        "nodes_created": created_nodes,
        "relationships_created": created_edges,
    }


def validate(session, connection, expected, batch_size):
    actual = counts(session)
    if actual != {key: expected[key] for key in ("nodes", "edges")}:
        raise ValueError(
            f"Graph counts differ: expected {expected['nodes']} nodes/{expected['edges']} edges, found {actual}. Use a dedicated database; no records were deleted."
        )
    checked_nodes = checked_edges = 0
    for batch in batches(connection, "nodes", batch_size):
        records = session.run(
            """
            UNWIND $rows AS row
            MATCH (v:Video {id: row.id})
            RETURN v.id AS id, properties(v) AS properties
        """,
            rows=[{"id": row["id"]} for row in batch],
        ).data()
        by_id = {row["id"]: row["properties"] for row in records}
        for row in batch:
            stored = by_id.get(row["id"])
            if stored is None:
                raise ValueError(f"Missing video {row['id']}")
            for key, value in row["properties"].items():
                if stored.get(key) != value or (
                    value is not None and type(stored.get(key)) is not type(value)
                ):
                    raise ValueError(f"Property/type mismatch for {row['id']}.{key}")
        checked_nodes += len(batch)
    for batch in batches(connection, "edges", batch_size):
        records = session.run(
            """
            UNWIND $rows AS row
            OPTIONAL MATCH (:Video {id: row.source})-[r:RELATED_TO]->(:Video {id: row.target})
            RETURN row.source AS source, row.target AS target, count(r) AS matches
        """,
            rows=batch,
        ).data()
        if len(records) != len(batch) or any(row["matches"] != 1 for row in records):
            raise ValueError("Missing or duplicate RELATED_TO relationship")
        checked_edges += len(batch)
    schema = inspect_schema(session)
    return {
        "passed": True,
        "counts": actual,
        "node_properties_checked": checked_nodes,
        "edges_checked": checked_edges,
        "schema": schema,
    }


def run(args):
    started = perf_counter()
    report = {
        "status": "running",
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "batch_size": args.batch_size,
        "mode": "validate" if args.validate_only else "ingest",
    }
    try:
        if args.prepare_from:
            report["preparation"] = prepare_csvs(args.prepare_from, args.nodes.parent)
        if args.prepare_only:
            report["status"] = "prepared"
            return report
        from neo4j import GraphDatabase

        uri, auth, database = connection_settings()
        report["database"] = database
        # Credentials/URI are deliberately excluded from the report.
        with GraphDatabase.driver(uri, auth=auth, connection_timeout=30) as driver:
            driver.verify_connectivity()
            with driver.session(database=database) as session:
                if args.schema_only:
                    report["schema"] = setup_schema(session)
                    report["status"] = "schema_ready"
                    return report
                preflight = perf_counter()
                with stage_csvs(args.nodes, args.edges) as (connection, expected):
                    report["input"] = expected
                    report["preflight_seconds"] = perf_counter() - preflight
                    report["before"] = counts(session)
                    if not args.validate_only:
                        schema_started = perf_counter()
                        report["schema"] = setup_schema(session)
                        report["schema_seconds"] = perf_counter() - schema_started
                        save_report(args.report, report)
                        for table in ("nodes", "edges"):
                            report[table] = load_phase(
                                session, connection, table, args.batch_size
                            )
                            save_report(args.report, report)
                        seconds = (
                            report["nodes"]["seconds"] + report["edges"]["seconds"]
                        )
                        report["ingestion"] = {
                            "seconds": seconds,
                            "records_per_second": (
                                expected["nodes"] + expected["edges"]
                            )
                            / seconds,
                        }
                    validation_started = perf_counter()
                    report["validation"] = validate(
                        session, connection, expected, args.batch_size
                    )
                    report["validation_seconds"] = perf_counter() - validation_started
                    report["status"] = "passed"
    except Exception as exc:
        report["status"] = "failed"
        report["error_type"] = type(exc).__name__
        raise
    finally:
        report["total_seconds"] = perf_counter() - started
        if "input" in report:
            report["total_records_per_second"] = (
                report["input"]["nodes"] + report["input"]["edges"]
            ) / report["total_seconds"]
        save_report(args.report, report)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--nodes", type=Path, default=ROOT / "Processed_Data/nodes.csv")
    parser.add_argument("--edges", type=Path, default=ROOT / "Processed_Data/edges.csv")
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument(
        "--system-trust",
        action="store_true",
        help="Use the operating system's trusted TLS certificates (verification stays enabled)",
    )
    parser.add_argument(
        "--report", type=Path, default=ROOT / "Processed_Data/ingestion_report.json"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--schema-only", action="store_true")
    mode.add_argument("--validate-only", action="store_true")
    mode.add_argument("--prepare-only", action="store_true")
    parser.add_argument(
        "--prepare-from",
        type=Path,
        help="Convert existing videos.csv to nodes.csv/edges.csv first",
    )
    args = parser.parse_args(argv)
    if args.batch_size <= 0:
        parser.error("--batch-size must be positive")
    if args.prepare_only and not args.prepare_from:
        parser.error("--prepare-only requires --prepare-from videos.csv")
    if args.prepare_from and (
        args.nodes.name != "nodes.csv"
        or args.edges.name != "edges.csv"
        or args.nodes.parent.resolve() != args.edges.parent.resolve()
    ):
        parser.error(
            "--prepare-from requires nodes.csv and edges.csv in the same directory"
        )
    protected = {
        path.resolve()
        for path in (args.nodes, args.edges, ROOT / "schema.cypher", ROOT / ".env")
    }
    if args.prepare_from:
        protected.add(args.prepare_from.resolve())
    if args.report.resolve() in protected:
        parser.error("--report must not overwrite an input, schema, or credential file")
    try:
        if args.system_trust:
            import truststore

            truststore.inject_into_ssl()
        report = run(args)
    except Exception as exc:
        print(
            f"Failed: {exc}\nReport: {args.report}. Earlier committed batches remain; fix the cause and rerun.",
            file=sys.stderr,
        )
        return 1
    print(f"{report['status']}: {args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
