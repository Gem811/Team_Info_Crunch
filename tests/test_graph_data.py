import csv
import json
import tempfile
import unittest
from pathlib import Path

from graph_data import NODE_COLUMNS, REQUIRED, batches, prepare_csvs, stage_csvs


def node(identity="abcdefghijk", **changes):
    return {
        "id": identity,
        "uploader": "a, uploader",
        "age": "100",
        "category": "Music",
        "length": "30",
        "views": "15",
        "rate": "4.5",
        "ratings": "2",
        "comments": "1",
        **changes,
    }


def write_csv(path, columns, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


class GraphDataTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.folder = Path(self.temporary.name)
        self.nodes = self.folder / "nodes.csv"
        self.edges = self.folder / "edges.csv"
        write_csv(self.nodes, REQUIRED, [node(), node("12345678901", views="0")])
        write_csv(
            self.edges,
            ("source", "target"),
            [{"source": "abcdefghijk", "target": "12345678901"}],
        )

    def test_preflight_roundtrips_csv_quotes_and_typed_batches(self):
        with stage_csvs(self.nodes, self.edges) as (connection, stats):
            self.assertEqual((stats["nodes"], stats["edges"]), (2, 1))
            rows = [row for batch in batches(connection, "nodes", 1) for row in batch]
            by_id = {row["id"]: row["properties"] for row in rows}
            self.assertEqual(by_id["abcdefghijk"]["uploader"], "a, uploader")
            self.assertIs(type(by_id["12345678901"]["views"]), int)
            self.assertIs(type(by_id["abcdefghijk"]["rate"]), float)
            self.assertIsNone(by_id["abcdefghijk"]["file_size_bytes"])
            self.assertEqual(len(stats["files"][0]["sha256"]), 64)

    def test_rejects_duplicate_nodes(self):
        write_csv(self.nodes, REQUIRED, [node(), node()])
        with self.assertRaisesRegex(ValueError, "duplicate node"):
            with stage_csvs(self.nodes, self.edges):
                self.fail("Duplicate node accepted")

    def test_rejects_duplicate_and_dangling_edges(self):
        for rows in (
            [{"source": "abcdefghijk", "target": "12345678901"}] * 2,
            [{"source": "abcdefghijk", "target": "missing0000"}],
        ):
            with self.subTest(rows=rows):
                write_csv(self.edges, ("source", "target"), rows)
                with self.assertRaisesRegex(ValueError, "dangling edge"):
                    with stage_csvs(self.nodes, self.edges):
                        self.fail("Invalid edge accepted")

    def test_rejects_invalid_numeric_values_and_required_text(self):
        for changes in (
            {"rate": "nan"},
            {"views": "1.5"},
            {"views": "-1"},
            {"views": str(2**63)},
            {"category": "  "},
            {"rate": "5.1"},
            {"length": ""},
        ):
            with self.subTest(changes=changes):
                write_csv(self.nodes, REQUIRED, [node(**changes)])
                with self.assertRaises(ValueError):
                    with stage_csvs(self.nodes, self.edges):
                        self.fail("Invalid property accepted")

    def test_rejects_malformed_csv_and_empty_nodes(self):
        for content in (
            "id,id\na,b\n",
            ",".join(REQUIRED) + "\n",
            ",".join(REQUIRED) + "\nabc,uploader\n",
        ):
            with self.subTest(content=content):
                self.nodes.write_text(content, encoding="utf-8")
                with self.assertRaises(ValueError):
                    with stage_csvs(self.nodes, self.edges):
                        self.fail("Malformed CSV accepted")

    def test_optional_properties_and_self_loops(self):
        write_csv(
            self.nodes,
            NODE_COLUMNS,
            [
                node(
                    file_size_bytes="1024",
                    bitrate_kbps="0.1",
                    bitrate_method="measured",
                )
            ],
        )
        write_csv(
            self.edges,
            ("source", "target"),
            [{"source": "abcdefghijk", "target": "abcdefghijk"}],
        )
        with stage_csvs(self.nodes, self.edges) as (connection, stats):
            props = next(batches(connection, "nodes", 10))[0]["properties"]
            self.assertEqual(props["file_size_bytes"], 1024)
            self.assertEqual(stats["edges"], 1)

    def test_prepares_existing_pipeline_format_without_inventing_nodes(self):
        videos = self.folder / "videos.csv"
        columns = ("video_id", *REQUIRED[1:], "related_ids")
        rows = []
        for identity, related in (
            ("abcdefghijk", ["12345678901", "12345678901", "missing0000"]),
            ("12345678901", []),
        ):
            row = node(identity)
            row["video_id"] = row.pop("id")
            row["related_ids"] = json.dumps(related)
            rows.append(row)
        write_csv(videos, columns, rows)
        stats = prepare_csvs(videos, self.folder)
        self.assertEqual(
            (stats["nodes"], stats["edges"], stats["external_edges_omitted"]), (2, 1, 1)
        )
        with stage_csvs(self.nodes, self.edges) as (_, checked):
            self.assertEqual(checked["edges"], 1)

    def test_preparation_failure_preserves_previous_outputs(self):
        before = self.nodes.read_bytes(), self.edges.read_bytes()
        videos = self.folder / "videos.csv"
        row = node()
        row["video_id"] = row.pop("id")
        row["related_ids"] = "not JSON"
        write_csv(videos, ("video_id", *REQUIRED[1:], "related_ids"), [row])
        with self.assertRaises(ValueError):
            prepare_csvs(videos, self.folder)
        self.assertEqual(before, (self.nodes.read_bytes(), self.edges.read_bytes()))


if __name__ == "__main__":
    unittest.main()
