import json
import tempfile
import unittest
from importlib.util import find_spec
from pathlib import Path

from recommendations_neo4j import stage_results

A, B, C, D, E, F, G, H = (letter * 11 for letter in "ABCDEFGH")


@unittest.skipUnless(find_spec("pyspark"), "Install requirements-spark.txt for Spark tests")
class RecommendationAlgorithmTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("recommendation regression test")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.addClassCleanup(cls.spark.stop)

    def values(self, columns, rows):
        # Use the same JVM DataFrame operations as the CSV job.
        values = ", ".join("(" + ", ".join(f"'{i}'" for i in row) + ")" for row in rows)
        return self.spark.sql(f"SELECT * FROM VALUES {values} AS fixture ({columns})")

    def test_ranks_distinct_middles_excludes_existing_and_keeps_isolates(self):
        from pyspark.sql import functions as f

        from spark_recommendations import pack_results, rank_candidates, validate_graph

        nodes = self.values("id", [(node,) for node in (A, B, C, D, E, F, G, H)])
        edges = self.values(
            "source, target",
            [(A, A), (A, B), (A, B), (A, C), (A, F), (B, A),
             (B, D), (B, E), (B, F), (C, D), (C, G)],
        ).distinct()
        self.assertEqual(validate_graph(nodes, edges)["self_loops_excluded"], 1)
        ranked = rank_candidates(edges.where(f.col("source") != f.col("target")), 2)
        rows = {r.source: r.recommendations for r in pack_results(nodes, ranked).collect()}
        self.assertEqual([(r.target, r.support, r.rank) for r in rows[A]], [(D, 2, 1), (E, 1, 2)])
        self.assertEqual(rows[G], [])
        self.assertEqual(rows[H], [])

    def test_rejects_an_edge_to_an_unknown_video(self):
        from spark_recommendations import validate_graph

        nodes = self.values("id", [(A,)])
        edges = self.values("source, target", [(A, B)])
        with self.assertRaisesRegex(ValueError, "endpoint"):
            validate_graph(nodes, edges)


class RecommendationPublishInputTest(unittest.TestCase):
    def test_incomplete_or_dangling_output_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            folder = Path(tmp)
            data = {
                "source": A,
                "recommendations": [{"target": B, "support": 2, "rank": 1}],
            }
            (folder / "part-00000.json").write_text(json.dumps(data) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "_SUCCESS"):
                with stage_results(folder):
                    self.fail("Accepted incomplete output")
            (folder / "_SUCCESS").touch()
            with self.assertRaisesRegex(ValueError, "every target"):
                with stage_results(folder):
                    self.fail("Accepted dangling output")
