import csv
import tempfile
import unittest
from pathlib import Path

from pagerank_neo4j import SCORE_COLUMNS, chunks, score_rows

A, B, C = (letter * 11 for letter in "abc")


def score(rank, identity, value):
    return {
        "rank": str(rank),
        "video_id": identity,
        "score": value,
        "in_degree": "1",
        "out_degree": "2",
        "category": "Music",
        "views": "15",
    }


class ScoreRowTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.path = Path(temporary.name) / "pagerank.csv"

    def write(self, rows):
        with self.path.open("w", encoding="utf-8", newline="") as stream:
            writer = csv.DictWriter(stream, fieldnames=SCORE_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)

    def test_keeps_only_videos_stored_in_neo4j(self):
        self.write([score(1, A, "0.5"), score(2, B, "0.3"), score(3, C, "0.2")])
        stats = {}
        rows = list(score_rows(self.path, {A, C}, stats))
        self.assertEqual(
            rows,
            [{"id": A, "score": 0.5, "rank": 1}, {"id": C, "score": 0.2, "rank": 3}],
        )
        self.assertEqual(stats, {"score_rows_read": 3})

    def test_rejects_invalid_or_duplicate_rows(self):
        cases = {
            "bad_id": [score(1, "short", "0.5")],
            "bad_score": [score(1, A, "nan")],
            "score_above_one": [score(1, A, "1.5")],
            "zero_rank": [score(0, A, "0.5")],
            "duplicate": [score(1, A, "0.5"), score(2, A, "0.4")],
        }
        for name, rows in cases.items():
            with self.subTest(name):
                self.write(rows)
                with self.assertRaises(ValueError):
                    list(score_rows(self.path, {A}, {}))

    def test_rejects_unexpected_columns(self):
        with self.path.open("w", encoding="utf-8", newline="") as stream:
            stream.write("video_id,score\n" + A + ",0.5\n")
        with self.assertRaises(ValueError):
            list(score_rows(self.path, {A}, {}))

    def test_chunks_split_into_batches(self):
        self.assertEqual(list(chunks(range(5), 2)), [[0, 1], [2, 3], [4]])
        self.assertEqual(list(chunks([], 2)), [])


if __name__ == "__main__":
    unittest.main()
