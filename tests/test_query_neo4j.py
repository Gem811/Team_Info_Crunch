import re
import unittest

from query_neo4j import QUERIES, access_operators, pagerank

WRITE_CLAUSE = re.compile(
    r"\b(CREATE|MERGE|SET|DELETE|REMOVE|DETACH|FOREACH)\b|\bLOAD\s+CSV\b",
    re.IGNORECASE,
)


class PageRankTests(unittest.TestCase):
    def test_dangling_and_isolated_nodes_keep_scores_summing_to_one(self):
        scores, _ = pagerank(["a", "b", "c", "isolated"], [("a", "b"), ("b", "c")])
        self.assertAlmostEqual(sum(scores.values()), 1.0, places=9)
        self.assertGreater(scores["isolated"], 0)

    def test_hub_of_star_ranks_first(self):
        leaves = [f"leaf{number}" for number in range(10)]
        scores, _ = pagerank(["hub", *leaves], [(leaf, "hub") for leaf in leaves])
        self.assertEqual(max(scores, key=scores.get), "hub")

    def test_directed_cycle_is_uniform(self):
        scores, _ = pagerank(["a", "b", "c"], [("a", "b"), ("b", "c"), ("c", "a")])
        for score in scores.values():
            self.assertAlmostEqual(score, 1 / 3)

    def test_empty_graph_returns_no_scores(self):
        self.assertEqual(pagerank([], []), ({}, 0))


class QuerySuiteTests(unittest.TestCase):
    def test_queries_are_read_only_with_unique_names(self):
        for _, name, query in QUERIES:
            self.assertIsNone(WRITE_CLAUSE.search(query), name)
        self.assertEqual(len({name for _, name, _ in QUERIES}), len(QUERIES))

    def test_access_operators_find_nested_index_seek(self):
        plan = {
            "operatorType": "Filter@neo4j",
            "children": [{"operatorType": "NodeIndexSeekByRange@neo4j"}],
        }
        self.assertEqual(access_operators(plan), ["NodeIndexSeekByRange"])


if __name__ == "__main__":
    unittest.main()
