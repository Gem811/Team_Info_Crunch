"""CSV contract and disk-backed preflight for the Neo4j loader."""

from __future__ import annotations

import csv
import hashlib
import json
import math
import re
import sqlite3
import tempfile
from contextlib import contextmanager
from pathlib import Path

REQUIRED = (
    "id",
    "uploader",
    "age",
    "category",
    "length",
    "views",
    "rate",
    "ratings",
    "comments",
)
OPTIONAL_INTS = (
    "file_size_bytes",
    "uploader_uploads",
    "uploader_watches",
    "uploader_friends",
)
OPTIONAL = (*OPTIONAL_INTS, "bitrate_kbps", "bitrate_method")
NODE_COLUMNS = (*REQUIRED, *OPTIONAL)
VIDEO_ID = re.compile(r"[A-Za-z0-9_-]{11}\Z")


def integer(value, field):
    try:
        result = int(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"{field}: expected a nonnegative integer") from exc
    if not 0 <= result <= 2**63 - 1:
        raise ValueError(f"{field}: outside Neo4j's nonnegative 64-bit integer range")
    return result


def number(value, field, low, high):
    try:
        result = float(value)
    except (ValueError, TypeError) as exc:
        raise ValueError(f"{field}: expected a number") from exc
    if not math.isfinite(result) or not low <= result <= high:
        raise ValueError(f"{field}: expected a finite value in [{low}, {high}]")
    return result


def video_id(value):
    if not isinstance(value, str) or not VIDEO_ID.fullmatch(value):
        raise ValueError(f"Invalid video ID: {value!r}")
    return value


def node_properties(row):
    props = {}
    for key in ("uploader", "category"):
        if not row[key] or not row[key].strip():
            raise ValueError(f"{key}: required text is blank")
        props[key] = row[key]
    for key in ("age", "length", "views", "ratings", "comments"):
        props[key] = integer(row[key], key)
    props["rate"] = number(row["rate"], "rate", 0, 5)
    for key in OPTIONAL_INTS:
        props[key] = integer(row[key], key) if row.get(key) else None
    props["bitrate_kbps"] = (
        number(row["bitrate_kbps"], "bitrate_kbps", 0.1, 100_000)
        if row.get("bitrate_kbps")
        else None
    )
    method = row.get("bitrate_method") or None
    if method not in (None, "measured", "calculated_from_file_size"):
        raise ValueError(
            "bitrate_method: expected measured, calculated_from_file_size, or blank"
        )
    if (method is None) != (props["bitrate_kbps"] is None):
        raise ValueError(
            "bitrate_kbps and bitrate_method must both be present or both blank"
        )
    props["bitrate_method"] = method
    return props


def read_csv(path, required, optional=()):
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        headers = reader.fieldnames or []
        if (
            len(headers) != len(set(headers))
            or not set(required) <= set(headers)
            or set(headers) - set(required) - set(optional)
        ):
            raise ValueError(
                f"{path}: expected columns {required}, optional {optional}; got {headers}"
            )
        for row in reader:
            if None in row or any(value is None for value in row.values()):
                raise ValueError(
                    f"{path}:{reader.line_num}: incorrect number of CSV cells"
                )
            yield reader.line_num, row


def fingerprint(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return {
        "name": Path(path).name,
        "bytes": Path(path).stat().st_size,
        "sha256": digest.hexdigest(),
    }


@contextmanager
def stage_csvs(nodes, edges):
    """Validate the complete input before database writes; RAM is batch-bounded."""
    with tempfile.TemporaryDirectory(prefix="info-crunch-") as temporary:
        connection = sqlite3.connect(Path(temporary) / "preflight.sqlite")
        try:
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute(
                "CREATE TABLE nodes (id TEXT PRIMARY KEY, props TEXT NOT NULL) WITHOUT ROWID"
            )
            connection.execute(
                "CREATE TABLE edges (source TEXT REFERENCES nodes(id), target TEXT REFERENCES nodes(id), PRIMARY KEY(source,target)) WITHOUT ROWID"
            )
            for line, row in read_csv(nodes, REQUIRED, OPTIONAL):
                try:
                    connection.execute(
                        "INSERT INTO nodes VALUES (?, ?)",
                        (
                            video_id(row["id"]),
                            json.dumps(node_properties(row), ensure_ascii=False),
                        ),
                    )
                except (ValueError, sqlite3.IntegrityError) as exc:
                    raise ValueError(
                        f"{nodes}:{line}: invalid or duplicate node: {exc}"
                    ) from exc
            node_count = connection.execute("SELECT count(*) FROM nodes").fetchone()[0]
            if not node_count:
                raise ValueError("nodes.csv contains no video records")
            for line, row in read_csv(edges, ("source", "target")):
                try:
                    connection.execute(
                        "INSERT INTO edges VALUES (?, ?)",
                        (video_id(row["source"]), video_id(row["target"])),
                    )
                except (ValueError, sqlite3.IntegrityError) as exc:
                    raise ValueError(
                        f"{edges}:{line}: invalid, duplicate, or dangling edge: {exc}"
                    ) from exc
            connection.commit()
            edge_count = connection.execute("SELECT count(*) FROM edges").fetchone()[0]
            yield (
                connection,
                {
                    "nodes": node_count,
                    "edges": edge_count,
                    "files": [fingerprint(nodes), fingerprint(edges)],
                },
            )
        finally:
            connection.close()


def batches(connection, table, size):
    if table not in ("nodes", "edges") or size <= 0:
        raise ValueError("Invalid batch table or size")
    cursor = connection.execute(f"SELECT * FROM {table}")
    try:
        while rows := cursor.fetchmany(size):
            if table == "nodes":
                yield [{"id": row[0], "properties": json.loads(row[1])} for row in rows]
            else:
                yield [{"source": row[0], "target": row[1]} for row in rows]
    finally:
        cursor.close()


def prepare_csvs(videos, output):
    """Bridge the existing videos.csv output to the B1/B2 two-file contract.

    All videos survive. Edges to IDs outside this input are counted and omitted;
    no synthetic nodes are invented. This is not a connected-subgraph sampler.
    """
    output = Path(output)
    output.mkdir(parents=True, exist_ok=True)
    destinations = (output / "nodes.csv", output / "edges.csv")
    if Path(videos).resolve() in [path.resolve() for path in destinations]:
        raise ValueError("Input videos CSV must not be an output path")
    with tempfile.TemporaryDirectory(prefix=".prepare-graph-", dir=output) as temporary:
        temporary = Path(temporary)
        connection = sqlite3.connect(temporary / "ids.sqlite")
        try:
            connection.execute("CREATE TABLE ids (id TEXT PRIMARY KEY) WITHOUT ROWID")
            connection.execute(
                "CREATE TABLE edges (source TEXT, target TEXT, PRIMARY KEY(source,target)) WITHOUT ROWID"
            )
            with (temporary / "nodes.csv").open(
                "w", encoding="utf-8", newline=""
            ) as stream:
                writer = csv.DictWriter(stream, fieldnames=NODE_COLUMNS)
                writer.writeheader()
                for line, row in read_csv(
                    videos, ("video_id", *REQUIRED[1:], "related_ids"), OPTIONAL
                ):
                    try:
                        identity = video_id(row["video_id"])
                        props = node_properties(row)
                        connection.execute("INSERT INTO ids VALUES (?)", (identity,))
                        related = json.loads(row["related_ids"])
                        if not isinstance(related, list) or len(related) > 20:
                            raise ValueError(
                                "related_ids must be a JSON array of at most 20 IDs"
                            )
                        for target in related:
                            video_id(target)
                        connection.executemany(
                            "INSERT OR IGNORE INTO edges VALUES (?,?)",
                            ((identity, target) for target in related),
                        )
                        writer.writerow({"id": identity, **props})
                    except (ValueError, sqlite3.IntegrityError) as exc:
                        raise ValueError(f"{videos}:{line}: {exc}") from exc
            node_count = connection.execute("SELECT count(*) FROM ids").fetchone()[0]
            if not node_count:
                raise ValueError("videos.csv contains no video records")
            candidate_count = connection.execute(
                "SELECT count(*) FROM edges"
            ).fetchone()[0]
            with (temporary / "edges.csv").open(
                "w", encoding="utf-8", newline=""
            ) as stream:
                writer = csv.writer(stream)
                writer.writerow(("source", "target"))
                edge_count = 0
                for edge in connection.execute(
                    "SELECT source,target FROM edges JOIN ids ON edges.target=ids.id ORDER BY source,target"
                ):
                    writer.writerow(edge)
                    edge_count += 1
            report = {
                "nodes": node_count,
                "edges": edge_count,
                "external_edges_omitted": candidate_count - edge_count,
                "source": fingerprint(videos),
            }
        finally:
            connection.close()
        for path in destinations:
            (temporary / path.name).replace(path)
        (output / "graph_preparation_report.json").write_text(
            json.dumps(report, indent=2) + "\n", encoding="utf-8"
        )
    return report
