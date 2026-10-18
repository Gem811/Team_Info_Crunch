"""Opt-in test against an EMPTY database reserved for this test."""

import os
import tempfile
import unittest
from pathlib import Path

from test_graph_data import node, write_csv

from graph_data import NODE_COLUMNS, stage_csvs
from ingest_neo4j import counts, load_phase, setup_schema, validate


@unittest.skipUnless(
    os.getenv("NEO4J_TEST_URI"),
    "Set NEO4J_TEST_URI for an empty database reserved for testing",
)
class Neo4jIntegrationTests(unittest.TestCase):
    def test_real_schema_load_rerun_update_and_corruption_detection(self):
        from neo4j import GraphDatabase

        auth = (
            os.getenv("NEO4J_TEST_USERNAME", "neo4j"),
            os.environ["NEO4J_TEST_PASSWORD"],
        )
        with GraphDatabase.driver(os.environ["NEO4J_TEST_URI"], auth=auth) as driver:
            with (
                driver.session(
                    database=os.getenv("NEO4J_TEST_DATABASE", "neo4j")
                ) as session,
                tempfile.TemporaryDirectory() as temporary,
            ):
                self.assertEqual(
                    counts(session),
                    {"nodes": 0, "edges": 0},
                    "Integration test requires an EMPTY disposable database",
                )
                folder = Path(temporary)
                nodes, edges = folder / "nodes.csv", folder / "edges.csv"
                write_csv(
                    nodes,
                    NODE_COLUMNS,
                    [node(file_size_bytes="1024"), node("12345678901", views="0")],
                )
                write_csv(
                    edges,
                    ("source", "target"),
                    [
                        {"source": "abcdefghijk", "target": "12345678901"},
                        {"source": "abcdefghijk", "target": "abcdefghijk"},
                    ],
                )
                try:
                    setup_schema(session)
                    setup_schema(session)  # DDL is idempotent too.
                    with stage_csvs(nodes, edges) as (connection, expected):
                        first = load_phase(session, connection, "nodes", 1)
                        self.assertEqual(first["nodes_created"], 2)
                        self.assertEqual(
                            load_phase(session, connection, "edges", 1)[
                                "relationships_created"
                            ],
                            2,
                        )
                        self.assertTrue(
                            validate(session, connection, expected, 1)["passed"]
                        )
                        self.assertEqual(
                            load_phase(session, connection, "nodes", 1)[
                                "nodes_created"
                            ],
                            0,
                        )
                        self.assertEqual(
                            load_phase(session, connection, "edges", 1)[
                                "relationships_created"
                            ],
                            0,
                        )
                        self.assertTrue(
                            validate(session, connection, expected, 1)["passed"]
                        )
                        session.run(
                            "MATCH (v:Video {id:'abcdefghijk'}) SET v.views=999"
                        ).consume()
                        with self.assertRaisesRegex(
                            ValueError, "Property/type mismatch"
                        ):
                            validate(session, connection, expected, 1)
                        load_phase(session, connection, "nodes", 1)
                        session.run(
                            "MATCH (a:Video {id:'abcdefghijk'})-[r:RELATED_TO]->(b:Video {id:'12345678901'}) DELETE r WITH a,b CREATE (b)-[:RELATED_TO]->(a)"
                        ).consume()
                        with self.assertRaisesRegex(ValueError, "Missing or duplicate"):
                            validate(session, connection, expected, 1)
                    # Blank values remove stale enrichment on a later snapshot.
                    write_csv(
                        nodes,
                        NODE_COLUMNS,
                        [node(views="20"), node("12345678901", views="0")],
                    )
                    with stage_csvs(nodes, edges) as (connection, _):
                        load_phase(session, connection, "nodes", 1)
                    props = session.run(
                        "MATCH (v:Video {id:'abcdefghijk'}) RETURN properties(v) AS p"
                    ).single()["p"]
                    self.assertEqual(props["views"], 20)
                    self.assertNotIn("file_size_bytes", props)
                finally:
                    session.run(
                        "MATCH (v:Video) WHERE v.id IN $ids DETACH DELETE v",
                        ids=["abcdefghijk", "12345678901"],
                    ).consume()


if __name__ == "__main__":
    unittest.main()
