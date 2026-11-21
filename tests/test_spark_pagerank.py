"""Spark PageRank tests. They need pyspark, so run them in the Spark container:

docker compose -f compose.spark.yaml run --rm --entrypoint python3 spark -m unittest discover -s tests -p "test_spark*.py"
"""

import csv
import importlib.util
import json
import os
import random
import tempfile
import unittest
from pathlib import Path

from query_neo4j import pagerank
from spark_pagerank import OUTPUT_COLUMNS, build_graph, pagerank_rdd, parse_args, run

COLUMNS = ("video_id", "uploader", "category", "views", "related_ids")
A, B, C, D = (letter * 11 for letter in "abcd")
EXTERNAL = "z" * 11


def write_videos(path, rows, encoding="utf-8"):
    with path.open("w", encoding=encoding, newline="") as stream:
        writer = csv.writer(stream)
        writer.writerow(COLUMNS)
        for identity, related in rows:
            text = related if isinstance(related, str) else json.dumps(related)
            writer.writerow((identity, 'up, "loader"', "Music", "15", text))


# A -> B, C, B (duplicate), EXTERNAL; B -> A; C dangling; D isolated.
SAMPLE = [(A, [B, C, B, EXTERNAL]), (B, [A]), (C, []), (D, [])]
SAMPLE_EDGES = [(A, B), (A, C), (B, A)]


# The container sets SPARK_HOME; a bare pip pyspark on Windows lacks winutils
# and a supported Java, so the host suite skips these.
@unittest.skipUnless(
    os.getenv("SPARK_HOME") and importlib.util.find_spec("pyspark"),
    "Set SPARK_HOME (run in compose.spark.yaml) for Spark tests",
)
class SparkPageRankTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession

        cls.spark = (
            SparkSession.builder.master("local[2]")
            .appName("spark-pagerank-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "4")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.folder = Path(temporary.name)

    def spark_scores(self, nodes, edges, **options):
        index = {node: position for position, node in enumerate(nodes)}
        adjacency = {position: [] for position in index.values()}
        for source, target in edges:
            adjacency[index[source]].append(index[target])
        links = self.spark.sparkContext.parallelize(
            [(node, tuple(targets)) for node, targets in adjacency.items()], 3
        )
        ranks, iterations, _, converged = pagerank_rdd(
            links, len(nodes), partitions=4, **options
        )
        by_index = ranks.collectAsMap()
        return {node: by_index[position] for node, position in index.items()}, converged

    def assert_matches_reference(self, nodes, edges, **options):
        expected, _ = pagerank(nodes, edges)
        actual, converged = self.spark_scores(nodes, edges, **options)
        self.assertTrue(converged)
        self.assertEqual(actual.keys(), expected.keys())
        for node in nodes:
            self.assertAlmostEqual(actual[node], expected[node], delta=1e-9, msg=node)
        self.assertAlmostEqual(sum(actual.values()), 1.0, places=9)
        return actual

    def test_matches_reference_on_sprint1_cases(self):
        cases = {
            "dangling_and_isolated": (["a", "b", "c", "isolated"], [("a", "b"), ("b", "c")]),
            "star": (
                ["hub", *(f"leaf{n}" for n in range(10))],
                [(f"leaf{n}", "hub") for n in range(10)],
            ),
            "cycle": (["a", "b", "c"], [("a", "b"), ("b", "c"), ("c", "a")]),
        }
        scores = {}
        for name, (nodes, edges) in cases.items():
            with self.subTest(name):
                scores[name] = self.assert_matches_reference(nodes, edges)
        star = scores["star"]
        self.assertEqual(max(star, key=star.get), "hub")
        self.assertGreater(scores["dangling_and_isolated"]["isolated"], 0)

    def test_matches_reference_on_random_graph(self):
        generator = random.Random(7)
        nodes = [f"n{number}" for number in range(300)]
        edges = sorted(
            {
                (source, generator.choice(nodes))  # self-loops allowed
                for source in nodes[:280]  # the last 20 have no out-links
                for _ in range(generator.randint(0, 8))
            }
        )
        self.assert_matches_reference(nodes, edges)

    def test_empty_graph_returns_no_scores(self):
        ranks, iterations, errors, converged = pagerank_rdd(
            self.spark.sparkContext.emptyRDD(), 0
        )
        self.assertEqual((ranks.collect(), iterations, errors, converged), ([], 0, [], True))

    def test_iteration_cap_reports_not_converged(self):
        nodes, edges = ["a", "b", "c"], [("a", "b"), ("b", "c")]
        _, converged = self.spark_scores(nodes, edges, max_iterations=2)
        self.assertFalse(converged)

    def test_build_graph_applies_prepare_csvs_rules(self):
        for encoding in ("utf-8", "utf-8-sig"):
            with self.subTest(encoding):
                path = self.folder / f"videos-{encoding}.csv"
                write_videos(path, SAMPLE, encoding)
                graph = build_graph(self.spark, path, 4)
                self.assertEqual(
                    graph.stats,
                    {
                        "videos": 4,
                        "edges": 3,
                        "external_edges_omitted": 1,
                        "duplicate_related_ids_removed": 1,
                        "dangling_videos": 2,
                        "isolated_videos": 1,
                    },
                )
                names = {node: meta[0] for node, meta in graph.meta.collect()}
                edges = sorted(
                    (names[source], names[target])
                    for source, targets in graph.links.collect()
                    for target in targets
                )
                self.assertEqual(edges, sorted(SAMPLE_EDGES))

    def test_build_graph_rejects_invalid_rows(self):
        cases = {
            "bad_video_id": [(A, []), ("short", [])],
            "bad_json": [(A, "not json")],
            "too_many_related": [(A, [B] * 21)],
            "bad_related_id": [(A, ["bad"])],
            "duplicate_video_id": [(A, []), (A, [])],
        }
        for name, rows in cases.items():
            with self.subTest(name):
                path = self.folder / f"{name}.csv"
                write_videos(path, rows)
                with self.assertRaises(ValueError):
                    build_graph(self.spark, path, 4)

    def test_run_writes_sorted_scores_and_report(self):
        videos = self.folder / "videos.csv"
        write_videos(videos, SAMPLE)
        output, report_path = self.folder / "pagerank.csv", self.folder / "report.json"
        args = parse_args(
            [
                "--videos", str(videos),
                "--output", str(output),
                "--report", str(report_path),
                "--partitions", "4",
                "--check-reference",
            ]
        )
        report = run(args, spark=self.spark)
        expected, _ = pagerank([A, B, C, D], SAMPLE_EDGES)
        with output.open(encoding="utf-8", newline="") as stream:
            rows = list(csv.DictReader(stream))
        self.assertEqual(tuple(rows[0]), OUTPUT_COLUMNS)
        self.assertEqual([row["rank"] for row in rows], ["1", "2", "3", "4"])
        self.assertEqual(rows[0]["video_id"], max(expected, key=expected.get))
        for row in rows:
            self.assertAlmostEqual(float(row["score"]), expected[row["video_id"]], delta=1e-9)
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual(saved["status"], "passed")
        self.assertEqual(report["output"]["rows"], 4)
        self.assertAlmostEqual(saved["output"]["score_sum"], 1.0, places=9)
        self.assertLessEqual(saved["reference_check"]["max_abs_difference"], 1e-9)
        self.assertTrue(saved["reference_check"]["top_k_match"])
        self.assertEqual(saved["top_k"][0]["video_id"], rows[0]["video_id"])

    def test_run_records_failure_in_report(self):
        videos = self.folder / "videos.csv"
        write_videos(videos, [("short", [])])
        report_path = self.folder / "report.json"
        args = parse_args(
            ["--videos", str(videos), "--output", str(self.folder / "out.csv"),
             "--report", str(report_path)]
        )
        with self.assertRaises(ValueError):
            run(args, spark=self.spark)
        saved = json.loads(report_path.read_text(encoding="utf-8"))
        self.assertEqual((saved["status"], saved["error_type"]), ("failed", "ValueError"))
        self.assertFalse((self.folder / "out.csv").exists())


class ArgumentTests(unittest.TestCase):
    def test_rejects_bad_parameters_and_shared_paths(self):
        for argv in (
            ["--damping", "1"],
            ["--tolerance", "0"],
            ["--max-iterations", "0"],
            ["--partitions", "0"],
            ["--output", "x.csv", "--report", "x.csv"],
        ):
            with self.subTest(argv), self.assertRaises(SystemExit):
                parse_args(argv)


if __name__ == "__main__":
    unittest.main()
