"""Clean and consolidate the YouTube video records in Dataset_Files."""

from __future__ import annotations

import argparse
import codecs
import csv
import io
import json
import math
import os
import re
import sqlite3
import tempfile
import zipfile
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import IO, Iterator, TextIO


VIDEO_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{11}$")
SNAPSHOT_PATTERN = re.compile(r"(?<!\d)(\d{6}|\d{4})(?!\d)")
CSV_COLUMNS = (
    "video_id",
    "uploader",
    "age",
    "category",
    "length",
    "views",
    "rate",
    "ratings",
    "comments",
    "related_ids",
    "file_size_bytes",
    "bitrate_kbps",
    "bitrate_method",
    "uploader_uploads",
    "uploader_watches",
    "uploader_friends",
)
MIN_VALID_BITRATE_KBPS = 0.1
MAX_VALID_BITRATE_KBPS = 100_000.0
WRITE_BATCH_SIZE = 10_000

VIDEO_UPSERT_SQL = """
    INSERT INTO videos (
        video_id, uploader, age, category, length, views, rate, ratings,
        comments, related_ids, snapshot_date, related_count, source_order
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(video_id) DO UPDATE SET
        uploader = excluded.uploader,
        age = excluded.age,
        category = excluded.category,
        length = excluded.length,
        views = excluded.views,
        rate = excluded.rate,
        ratings = excluded.ratings,
        comments = excluded.comments,
        related_ids = excluded.related_ids,
        snapshot_date = excluded.snapshot_date,
        related_count = excluded.related_count,
        source_order = excluded.source_order
    WHERE excluded.snapshot_date > videos.snapshot_date
       OR (
           excluded.snapshot_date = videos.snapshot_date
           AND (
               excluded.related_count > videos.related_count
               OR (
                   excluded.related_count = videos.related_count
                   AND excluded.source_order < videos.source_order
               )
           )
       )
"""

USER_UPSERT_SQL = """
    INSERT INTO user_information (
        user_id, uploads, watches, friends, snapshot_date, source_order
    )
    VALUES (?, ?, ?, ?, ?, ?)
    ON CONFLICT(user_id) DO UPDATE SET
        uploads = excluded.uploads,
        watches = excluded.watches,
        friends = excluded.friends,
        snapshot_date = excluded.snapshot_date,
        source_order = excluded.source_order
    WHERE excluded.snapshot_date > user_information.snapshot_date
       OR (
           excluded.snapshot_date = user_information.snapshot_date
           AND excluded.source_order < user_information.source_order
       )
"""

CRAWL_UPSERT_SQL = """
    INSERT INTO crawl_updates (
        video_id, views, rate, ratings, comments, snapshot_date, source_order
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(video_id) DO UPDATE SET
        views = excluded.views,
        rate = excluded.rate,
        ratings = excluded.ratings,
        comments = excluded.comments,
        snapshot_date = excluded.snapshot_date,
        source_order = excluded.source_order
    WHERE excluded.snapshot_date > crawl_updates.snapshot_date
       OR (
           excluded.snapshot_date = crawl_updates.snapshot_date
           AND excluded.source_order < crawl_updates.source_order
       )
"""

FILE_SIZE_UPSERT_SQL = """
    INSERT INTO video_enrichment (
        video_id, file_size_bytes, size_length, size_priority
    )
    VALUES (?, ?, ?, ?)
    ON CONFLICT(video_id) DO UPDATE SET
        file_size_bytes = excluded.file_size_bytes,
        size_length = excluded.size_length,
        size_priority = excluded.size_priority
    WHERE excluded.size_priority > video_enrichment.size_priority
"""

BITRATE_UPSERT_SQL = """
    INSERT INTO video_enrichment (
        video_id, bitrate_kbps, bitrate_length, bitrate_invalid,
        bitrate_priority
    )
    VALUES (?, ?, ?, ?, ?)
    ON CONFLICT(video_id) DO UPDATE SET
        bitrate_kbps = excluded.bitrate_kbps,
        bitrate_length = excluded.bitrate_length,
        bitrate_invalid = excluded.bitrate_invalid,
        bitrate_priority = excluded.bitrate_priority
    WHERE excluded.bitrate_priority > video_enrichment.bitrate_priority
"""


@dataclass(frozen=True)
class Video:
    video_id: str
    uploader: str
    age: int
    category: str
    length: int
    views: int
    rate: float
    ratings: int
    comments: int
    related_ids: tuple[str, ...]


@dataclass(frozen=True)
class UserInformation:
    user_id: str
    uploads: int
    watches: int
    friends: int | None


class InvalidRecord(ValueError):
    """A source row does not contain a valid video record."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _nonnegative_integer(value: str, field: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise InvalidRecord(f"invalid_{field}") from exc
    if number < 0:
        raise InvalidRecord(f"negative_{field}")
    return number


def parse_record(line: str) -> tuple[Video, int, int, int]:
    """Parse and clean one tab-delimited source row."""
    fields = [field.strip() for field in line.rstrip("\r\n").split("\t")]
    if len(fields) < 9:
        raise InvalidRecord("too_few_fields")

    video_id, uploader, age, category, length, views, rate, ratings, comments = (
        fields[:9]
    )
    if not VIDEO_ID_PATTERN.fullmatch(video_id):
        raise InvalidRecord("invalid_video_id")
    if not uploader:
        raise InvalidRecord("empty_uploader")
    if not category:
        raise InvalidRecord("empty_category")

    age_value = _nonnegative_integer(age, "age")
    length_value = _nonnegative_integer(length, "length")
    views_value = _nonnegative_integer(views, "views")
    ratings_value = _nonnegative_integer(ratings, "ratings")
    comments_value = _nonnegative_integer(comments, "comments")
    try:
        rate_value = float(rate)
    except ValueError as exc:
        raise InvalidRecord("invalid_rate") from exc
    if not math.isfinite(rate_value) or not 0 <= rate_value <= 5:
        raise InvalidRecord("rate_out_of_range")

    related_ids: list[str] = []
    seen_related_ids: set[str] = set()
    invalid_related_ids = 0
    duplicate_related_ids = 0
    for related_id in fields[9:29]:
        if not VIDEO_ID_PATTERN.fullmatch(related_id):
            invalid_related_ids += 1
        elif related_id in seen_related_ids:
            duplicate_related_ids += 1
        else:
            related_ids.append(related_id)
            seen_related_ids.add(related_id)

    video = Video(
        video_id=video_id,
        uploader=uploader,
        age=age_value,
        category=category,
        length=length_value,
        views=views_value,
        rate=rate_value,
        ratings=ratings_value,
        comments=comments_value,
        related_ids=tuple(related_ids[:20]),
    )
    excess_related_ids = max(0, len(fields) - 29)
    return (
        video,
        invalid_related_ids,
        duplicate_related_ids,
        excess_related_ids,
    )


def parse_user_record(line: str) -> UserInformation:
    """Parse a user row containing uploads, watches, and optional friends."""
    fields = [field.strip() for field in line.rstrip("\r\n").split("\t")]
    if len(fields) not in {3, 4}:
        raise InvalidRecord("invalid_user_field_count")
    user_id = fields[0]
    if not user_id:
        raise InvalidRecord("empty_user_id")
    uploads = _nonnegative_integer(fields[1], "uploads")
    watches = _nonnegative_integer(fields[2], "watches")
    friends = (
        _nonnegative_integer(fields[3], "friends") if len(fields) == 4 else None
    )
    return UserInformation(user_id, uploads, watches, friends)


def parse_crawl_update(line: str) -> tuple[str, int, float, int, int]:
    """Parse one incremental video metrics row."""
    fields = [field.strip() for field in line.rstrip("\r\n").split("\t")]
    if len(fields) != 5:
        raise InvalidRecord("invalid_crawl_update_field_count")
    video_id, views, rate, ratings, comments = fields
    if not VIDEO_ID_PATTERN.fullmatch(video_id):
        raise InvalidRecord("invalid_video_id")
    views_value = _nonnegative_integer(views, "views")
    ratings_value = _nonnegative_integer(ratings, "ratings")
    comments_value = _nonnegative_integer(comments, "comments")
    try:
        rate_value = float(rate)
    except ValueError as exc:
        raise InvalidRecord("invalid_rate") from exc
    if not math.isfinite(rate_value) or not 0 <= rate_value <= 5:
        raise InvalidRecord("rate_out_of_range")
    return video_id, views_value, rate_value, ratings_value, comments_value


def snapshot_date(source_name: str) -> int:
    """Return a sortable YYYYMMDD snapshot key, or zero if no date is present."""
    matches = SNAPSHOT_PATTERN.findall(source_name)
    for token in reversed(matches):
        if len(token) == 6:
            year, month, day = 2000 + int(token[:2]), int(token[2:4]), int(token[4:])
        else:
            year, month, day = 2007, int(token[:2]), int(token[2:])
        if 1 <= month <= 12 and 1 <= day <= 31:
            return year * 10000 + month * 100 + day
    return 0


def _iter_crawl_update_sources(input_dir: Path) -> Iterator[tuple[str, TextIO]]:
    """Yield update.txt records from loose files and ZIP archives."""
    paths = sorted(
        (
            path
            for path in input_dir.rglob("*")
            if path.is_file() and path.suffix.lower() in {".txt", ".zip"}
        ),
        key=lambda path: path.as_posix().casefold(),
    )
    for path in paths:
        if path.suffix.lower() == ".txt":
            if path.name.lower() != "update.txt":
                continue
            with path.open("r", encoding="utf-8-sig", newline="") as stream:
                yield path.as_posix(), stream
            continue

        with zipfile.ZipFile(path) as archive:
            for member in sorted(
                (
                    info
                    for info in archive.infolist()
                    if not info.is_dir()
                    and Path(info.filename).name.lower() == "update.txt"
                ),
                key=lambda info: info.filename.casefold(),
            ):
                with archive.open(member) as binary_stream:
                    with io.TextIOWrapper(
                        binary_stream, encoding="utf-8-sig", newline=""
                    ) as stream:
                        yield f"{path.as_posix()}!{member.filename}", stream


def _create_crawl_updates_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE crawl_updates (
            video_id TEXT PRIMARY KEY,
            views INTEGER NOT NULL,
            rate REAL NOT NULL,
            ratings INTEGER NOT NULL,
            comments INTEGER NOT NULL,
            snapshot_date INTEGER NOT NULL,
            source_order INTEGER NOT NULL
        ) WITHOUT ROWID
        """
    )


def _load_crawl_updates(
    connection: sqlite3.Connection,
    update_dir: Path,
    *,
    show_progress: bool = False,
) -> dict[str, object]:
    """Load validated updates, retaining the newest dated row per video."""
    counters: Counter[str] = Counter()
    rejected: Counter[str] = Counter()
    source_order = 0
    batch: list[tuple[str, int, float, int, int, int, int]] = []
    for source_name, stream in _iter_crawl_update_sources(update_dir):
        counters["source_files_scanned"] += 1
        snapshot = snapshot_date(source_name)
        for line in stream:
            counters["rows_read"] += 1
            if not line.strip():
                counters["empty_lines_skipped"] += 1
                continue
            try:
                video_id, views, rate, ratings, comments = parse_crawl_update(line)
            except InvalidRecord as exc:
                rejected[exc.reason] += 1
                continue
            counters["valid_rows_before_deduplication"] += 1
            batch.append(
                (video_id, views, rate, ratings, comments, snapshot, source_order)
            )
            source_order += 1
            if len(batch) >= WRITE_BATCH_SIZE:
                connection.executemany(CRAWL_UPSERT_SQL, batch)
                batch.clear()
            if show_progress and counters["rows_read"] % 1_000_000 == 0:
                print(
                    f"Loaded {counters['rows_read']:,} crawl update rows.",
                    flush=True,
                )

    if batch:
        connection.executemany(CRAWL_UPSERT_SQL, batch)
    if counters["source_files_scanned"] == 0:
        raise ValueError(f"No update.txt files found in {update_dir}")
    counters["unique_update_records"] = connection.execute(
        "SELECT COUNT(*) FROM crawl_updates"
    ).fetchone()[0]
    counters["duplicate_rows_reduced"] = (
        counters["valid_rows_before_deduplication"]
        - counters["unique_update_records"]
    )
    counters["rejected_rows"] = sum(rejected.values())
    result: dict[str, object] = dict(sorted(counters.items()))
    result["rejected_by_reason"] = dict(sorted(rejected.items()))
    return result


def _is_log_file(name: str) -> bool:
    return Path(name).name.lower() == "log.txt"


def _iter_text_sources(input_dir: Path) -> Iterator[tuple[str, TextIO]]:
    """Yield records from loose text files and text members inside ZIP archives."""
    paths = sorted(
        (
            path
            for path in input_dir.rglob("*")
            if path.is_file() and path.suffix.lower() in {".txt", ".zip"}
        ),
        key=lambda path: path.as_posix().casefold(),
    )
    for path in paths:
        if path.suffix.lower() == ".txt":
            if _is_log_file(path.name):
                continue
            with path.open("r", encoding="utf-8-sig", newline="") as stream:
                yield path.as_posix(), stream
            continue

        with zipfile.ZipFile(path) as archive:
            for member in sorted(
                (
                    info
                    for info in archive.infolist()
                    if not info.is_dir()
                    and info.filename.lower().endswith(".txt")
                    and not _is_log_file(info.filename)
                ),
                key=lambda info: info.filename.casefold(),
            ):
                with archive.open(member) as binary_stream:
                    with io.TextIOWrapper(
                        binary_stream, encoding="utf-8-sig", newline=""
                    ) as stream:
                        yield f"{path.as_posix()}!{member.filename}", stream


def _detect_user_encoding(stream: IO[bytes]) -> str:
    """Prefer UTF-8, with a strict Windows-1252 fallback for legacy rows."""
    decoder = codecs.getincrementaldecoder("utf-8-sig")()
    try:
        while chunk := stream.read(64 * 1024):
            decoder.decode(chunk)
        decoder.decode(b"", final=True)
    except UnicodeDecodeError:
        encoding = "cp1252"
    else:
        encoding = "utf-8-sig"
    stream.seek(0)
    return encoding


def _iter_user_sources(
    user_info_dir: Path,
) -> Iterator[tuple[str, str, TextIO]]:
    """Yield user.txt files, excluding duplicate ID lists and log files."""
    paths = sorted(
        (
            path
            for path in user_info_dir.rglob("*")
            if path.is_file()
            and (
                path.suffix.lower() == ".zip"
                or (
                    path.suffix.lower() == ".txt"
                    and path.name.lower() == "user.txt"
                )
            )
        ),
        key=lambda path: path.as_posix().casefold(),
    )
    for path in paths:
        if path.suffix.lower() == ".txt":
            if path.name.lower() != "user.txt":
                continue
            with path.open("rb") as binary_stream:
                encoding = _detect_user_encoding(binary_stream)
                with io.TextIOWrapper(
                    binary_stream, encoding=encoding, newline=""
                ) as stream:
                    yield path.as_posix(), encoding, stream
            continue

        with zipfile.ZipFile(path) as archive:
            for member in sorted(
                (
                    info
                    for info in archive.infolist()
                    if not info.is_dir()
                    and Path(info.filename).name.lower() == "user.txt"
                ),
                key=lambda info: info.filename.casefold(),
            ):
                with archive.open(member) as binary_stream:
                    encoding = _detect_user_encoding(binary_stream)
                    with io.TextIOWrapper(
                        binary_stream, encoding=encoding, newline=""
                    ) as stream:
                        yield (
                            f"{path.as_posix()}!{member.filename}",
                            encoding,
                            stream,
                        )


def _create_staging_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE videos (
            video_id TEXT PRIMARY KEY,
            uploader TEXT NOT NULL,
            age INTEGER NOT NULL,
            category TEXT NOT NULL,
            length INTEGER NOT NULL,
            views INTEGER NOT NULL,
            rate REAL NOT NULL,
            ratings INTEGER NOT NULL,
            comments INTEGER NOT NULL,
            related_ids TEXT NOT NULL,
            snapshot_date INTEGER NOT NULL,
            related_count INTEGER NOT NULL,
            source_order INTEGER NOT NULL
        ) WITHOUT ROWID
        """
    )


def _create_user_information_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE user_information (
            user_id TEXT PRIMARY KEY,
            uploads INTEGER NOT NULL,
            watches INTEGER NOT NULL,
            friends INTEGER,
            snapshot_date INTEGER NOT NULL,
            source_order INTEGER NOT NULL
        ) WITHOUT ROWID
        """
    )


def _create_enrichment_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE video_enrichment (
            video_id TEXT PRIMARY KEY,
            file_size_bytes INTEGER,
            size_length INTEGER,
            size_priority INTEGER NOT NULL DEFAULT -1,
            bitrate_kbps REAL,
            bitrate_length INTEGER,
            bitrate_invalid INTEGER NOT NULL DEFAULT 0,
            bitrate_priority INTEGER NOT NULL DEFAULT -1
        ) WITHOUT ROWID
        """
    )


def _iter_enrichment_sources(
    enrichment_dir: Path,
) -> Iterator[tuple[str, str, TextIO]]:
    """Yield supported size and bitrate files from loose files and ZIP archives."""
    kinds = {"size.txt": "size", "rate.txt": "rate", "sizerate.txt": "sizerate"}
    paths = sorted(
        (
            path
            for path in enrichment_dir.rglob("*")
            if path.is_file() and path.suffix.lower() in {".txt", ".zip"}
        ),
        key=lambda path: path.as_posix().casefold(),
    )
    for path in paths:
        if path.suffix.lower() == ".txt":
            kind = kinds.get(path.name.lower())
            if kind is not None:
                with path.open("r", encoding="utf-8-sig", newline="") as stream:
                    yield path.as_posix(), kind, stream
            continue

        with zipfile.ZipFile(path) as archive:
            members = sorted(
                (
                    info
                    for info in archive.infolist()
                    if not info.is_dir()
                    and Path(info.filename).name.lower() in kinds
                ),
                key=lambda info: info.filename.casefold(),
            )
            for member in members:
                kind = kinds[Path(member.filename).name.lower()]
                with archive.open(member) as binary_stream:
                    with io.TextIOWrapper(
                        binary_stream, encoding="utf-8-sig", newline=""
                    ) as stream:
                        yield f"{path.as_posix()}!{member.filename}", kind, stream


def _upsert_file_size(
    video_id: str,
    length: int,
    file_size_bytes: int,
    priority: int,
) -> tuple[str, int, int, int]:
    return video_id, file_size_bytes, length, priority


def _upsert_bitrate(
    video_id: str,
    length: int,
    bitrate_kbps: float | None,
    invalid: bool,
    priority: int,
) -> tuple[str, float | None, int, int, int]:
    return video_id, bitrate_kbps, length, int(invalid), priority


def _load_video_enrichment(
    connection: sqlite3.Connection, enrichment_dir: Path
) -> dict[str, object]:
    """Clean metric observations and retain the newest valid observation per ID."""
    counters: Counter[str] = Counter()
    size_batch: list[tuple[str, int, int, int]] = []
    bitrate_batch: list[tuple[str, float | None, int, int, int]] = []
    for source_name, kind, stream in _iter_enrichment_sources(enrichment_dir):
        counters["source_files_scanned"] += 1
        priority = snapshot_date(source_name)
        for line in stream:
            counters["source_rows_read"] += 1
            fields = [field.strip() for field in line.rstrip("\r\n").split("\t")]
            expected_fields = 4 if kind == "sizerate" else 3
            if (
                len(fields) < 3
                or len(fields) > expected_fields
                or not VIDEO_ID_PATTERN.fullmatch(fields[0])
            ):
                counters["invalid_source_rows"] += 1
                continue
            try:
                length = _nonnegative_integer(fields[1], "length")
            except InvalidRecord:
                counters["invalid_source_rows"] += 1
                continue

            if kind in {"size", "sizerate"}:
                size_value = fields[2]
                if not size_value:
                    counters["missing_size_values"] += 1
                else:
                    try:
                        size_bytes = int(size_value)
                    except ValueError:
                        counters["invalid_size_values"] += 1
                    else:
                        if size_bytes <= 0:
                            counters["invalid_size_values"] += 1
                        else:
                            counters["valid_size_values"] += 1
                            size_batch.append(
                                _upsert_file_size(
                                    fields[0], length, size_bytes, priority
                                )
                            )
                            if len(size_batch) >= WRITE_BATCH_SIZE:
                                connection.executemany(
                                    FILE_SIZE_UPSERT_SQL, size_batch
                                )
                                size_batch.clear()

            if kind in {"rate", "sizerate"}:
                bitrate_value = fields[2] if kind == "rate" else (
                    fields[3] if len(fields) == 4 else ""
                )
                if not bitrate_value:
                    counters["missing_bitrate_values"] += 1
                    continue
                is_vbr = bitrate_value.upper() == "VBR"
                try:
                    bitrate = float(bitrate_value)
                except ValueError:
                    bitrate = math.nan
                if is_vbr:
                    counters["vbr_bitrate_values"] += 1
                if (
                    is_vbr
                    or not math.isfinite(bitrate)
                    or not MIN_VALID_BITRATE_KBPS
                    <= bitrate
                    <= MAX_VALID_BITRATE_KBPS
                ):
                    counters["invalid_bitrate_values"] += 1
                    bitrate_batch.append(
                        _upsert_bitrate(
                            fields[0], length, None, True, priority
                        )
                    )
                else:
                    counters["valid_bitrate_values"] += 1
                    bitrate_batch.append(
                        _upsert_bitrate(
                            fields[0], length, bitrate, False, priority
                        )
                    )
                if len(bitrate_batch) >= WRITE_BATCH_SIZE:
                    connection.executemany(BITRATE_UPSERT_SQL, bitrate_batch)
                    bitrate_batch.clear()

    if size_batch:
        connection.executemany(FILE_SIZE_UPSERT_SQL, size_batch)
    if bitrate_batch:
        connection.executemany(BITRATE_UPSERT_SQL, bitrate_batch)
    if counters["source_files_scanned"] == 0:
        raise ValueError(
            f"No supported size or bitrate files found in {enrichment_dir}"
        )
    counters["unique_video_ids_with_size"] = connection.execute(
        "SELECT COUNT(*) FROM video_enrichment WHERE file_size_bytes IS NOT NULL"
    ).fetchone()[0]
    counters["unique_video_ids_with_bitrate"] = connection.execute(
        "SELECT COUNT(*) FROM video_enrichment WHERE bitrate_priority >= 0"
    ).fetchone()[0]
    return dict(sorted(counters.items()))


def _upsert_user_information(
    user: UserInformation,
    snapshot: int,
    source_order: int,
) -> tuple[str, int, int, int | None, int, int]:
    return (
        user.user_id,
        user.uploads,
        user.watches,
        user.friends,
        snapshot,
        source_order,
    )


def _load_user_information(
    connection: sqlite3.Connection,
    user_info_dir: Path,
    *,
    show_progress: bool = False,
) -> dict[str, object]:
    """Clean user metrics and retain the newest valid row for each user."""
    counters: Counter[str] = Counter()
    rejected: Counter[str] = Counter()
    source_order = 0
    batch: list[tuple[str, int, int, int | None, int, int]] = []
    for source_name, encoding, stream in _iter_user_sources(user_info_dir):
        counters["source_files_scanned"] += 1
        counters[f"source_encoding_{encoding}"] += 1
        snapshot = snapshot_date(source_name)
        for line in stream:
            counters["rows_read"] += 1
            if not line.strip():
                counters["empty_rows_skipped"] += 1
                continue
            try:
                user = parse_user_record(line)
            except InvalidRecord as exc:
                rejected[exc.reason] += 1
                continue
            counters["valid_rows_before_deduplication"] += 1
            batch.append(_upsert_user_information(user, snapshot, source_order))
            source_order += 1
            if len(batch) >= WRITE_BATCH_SIZE:
                connection.executemany(USER_UPSERT_SQL, batch)
                batch.clear()
            if show_progress and counters["rows_read"] % 1_000_000 == 0:
                print(
                    f"Loaded {counters['rows_read']:,} user rows.",
                    flush=True,
                )

    if batch:
        connection.executemany(USER_UPSERT_SQL, batch)
    if counters["source_files_scanned"] == 0:
        raise ValueError(f"No user.txt files found in {user_info_dir}")

    counters["unique_user_records"] = connection.execute(
        "SELECT COUNT(*) FROM user_information"
    ).fetchone()[0]
    counters["duplicate_rows_reduced"] = (
        counters["valid_rows_before_deduplication"]
        - counters["unique_user_records"]
    )
    counters["rejected_rows"] = sum(rejected.values())
    counters["users_with_friends"] = connection.execute(
        "SELECT COUNT(*) FROM user_information WHERE friends IS NOT NULL"
    ).fetchone()[0]
    result: dict[str, object] = dict(sorted(counters.items()))
    result["rejected_by_reason"] = dict(sorted(rejected.items()))
    return result


def merge_user_information(
    videos_csv: Path,
    user_info_dir: Path,
    *,
    show_progress: bool = False,
) -> dict[str, object]:
    """Join cleaned uploader metrics onto an existing processed videos CSV."""
    videos_csv = videos_csv.resolve()
    user_info_dir = user_info_dir.resolve()
    if not videos_csv.is_file():
        raise FileNotFoundError(f"Processed video CSV does not exist: {videos_csv}")
    if not user_info_dir.is_dir():
        raise FileNotFoundError(
            f"User information directory does not exist: {user_info_dir}"
        )

    output_dir = videos_csv.parent
    report_path = output_dir / "processing_report.json"
    with tempfile.TemporaryDirectory(
        prefix=".user-information-merge-", dir=output_dir
    ) as staging_dir:
        staging_path = Path(staging_dir)
        database_path = staging_path / "users.sqlite3"
        with closing(sqlite3.connect(database_path)) as connection:
            connection.execute("PRAGMA journal_mode = OFF")
            connection.execute("PRAGMA synchronous = OFF")
            connection.execute("PRAGMA cache_size = -65536")
            _create_user_information_table(connection)
            user_report = _load_user_information(
                connection, user_info_dir, show_progress=show_progress
            )
            connection.commit()

            with videos_csv.open(encoding="utf-8-sig", newline="") as input_stream:
                reader = csv.DictReader(input_stream)
                if reader.fieldnames is None or not {
                    "video_id",
                    "uploader",
                }.issubset(reader.fieldnames):
                    raise ValueError(
                        f"Processed video CSV must contain video_id and uploader: "
                        f"{videos_csv}"
                    )
                output_columns = list(reader.fieldnames)
                for column in (
                    "uploader_uploads",
                    "uploader_watches",
                    "uploader_friends",
                ):
                    if column not in output_columns:
                        output_columns.append(column)

                staged_csv = staging_path / "videos.csv"
                video_rows = 0
                matched_video_rows = 0
                matched_user_ids: set[str] = set()
                with staged_csv.open(
                    "w", encoding="utf-8", newline=""
                ) as output_stream:
                    writer = csv.DictWriter(
                        output_stream,
                        fieldnames=output_columns,
                        extrasaction="ignore",
                    )
                    writer.writeheader()
                    for row in reader:
                        if None in row:
                            raise ValueError(
                                "Processed video CSV contains a row with more "
                                "columns than its header."
                            )
                        video_rows += 1
                        uploader = row.get("uploader") or ""
                        user = connection.execute(
                            """
                            SELECT uploads, watches, friends
                            FROM user_information WHERE user_id = ?
                            """,
                            (uploader,),
                        ).fetchone()
                        if user is None:
                            row["uploader_uploads"] = ""
                            row["uploader_watches"] = ""
                            row["uploader_friends"] = ""
                        else:
                            row["uploader_uploads"] = user[0]
                            row["uploader_watches"] = user[1]
                            row["uploader_friends"] = (
                                user[2] if user[2] is not None else ""
                            )
                            matched_video_rows += 1
                            matched_user_ids.add(uploader)
                        writer.writerow(row)
                        if show_progress and video_rows % 1_000_000 == 0:
                            print(
                                f"Joined user data to {video_rows:,} video rows.",
                                flush=True,
                            )

            user_report["user_ids_with_video_data"] = len(matched_user_ids)
            unique_user_records = user_report["unique_user_records"]
            if not isinstance(unique_user_records, int):
                raise TypeError("User information report is missing its record count.")
            user_report["user_ids_not_in_video_data"] = (
                unique_user_records - len(matched_user_ids)
            )
            user_report["videos_with_user_information"] = matched_video_rows
            user_report["videos_without_user_information"] = (
                video_rows - matched_video_rows
            )

            report: dict[str, object] = {}
            if report_path.is_file():
                with report_path.open(encoding="utf-8") as report_stream:
                    existing_report = json.load(report_stream)
                if not isinstance(existing_report, dict):
                    raise ValueError(
                        f"Processing report must contain a JSON object: "
                        f"{report_path}"
                    )
                report = existing_report
            report["user_information_directory"] = str(user_info_dir)
            report["user_information"] = user_report
            staged_report = staging_path / "processing_report.json"
            with staged_report.open("w", encoding="utf-8", newline="\n") as stream:
                json.dump(report, stream, indent=2, ensure_ascii=False)
                stream.write("\n")

        os.replace(staged_csv, videos_csv)
        os.replace(staged_report, report_path)

    user_report["video_rows_processed"] = video_rows
    return user_report


def merge_crawl_updates(
    videos_csv: Path,
    update_dir: Path,
    *,
    show_progress: bool = False,
) -> dict[str, object]:
    """Apply the newest valid crawl metrics to matching existing video rows."""
    videos_csv = videos_csv.resolve()
    update_dir = update_dir.resolve()
    if not videos_csv.is_file():
        raise FileNotFoundError(f"Processed video CSV does not exist: {videos_csv}")
    if not update_dir.is_dir():
        raise FileNotFoundError(f"Crawl update directory does not exist: {update_dir}")

    output_dir = videos_csv.parent
    report_path = output_dir / "processing_report.json"
    with tempfile.TemporaryDirectory(
        prefix=".crawl-update-merge-", dir=output_dir
    ) as staging_dir:
        staging_path = Path(staging_dir)
        database_path = staging_path / "updates.sqlite3"
        counters: Counter[str] = Counter()
        rejected: Counter[str] = Counter()
        source_order = 0

        with closing(sqlite3.connect(database_path)) as connection:
            connection.execute("PRAGMA journal_mode = OFF")
            connection.execute("PRAGMA synchronous = OFF")
            connection.execute("PRAGMA cache_size = -65536")
            connection.execute(
                """
                CREATE TABLE crawl_updates (
                    video_id TEXT PRIMARY KEY,
                    views INTEGER NOT NULL,
                    rate REAL NOT NULL,
                    ratings INTEGER NOT NULL,
                    comments INTEGER NOT NULL,
                    snapshot_date INTEGER NOT NULL,
                    source_order INTEGER NOT NULL,
                    matched INTEGER NOT NULL DEFAULT 0
                ) WITHOUT ROWID
                """
            )
            batch: list[tuple[str, int, float, int, int, int, int]] = []
            for source_name, stream in _iter_crawl_update_sources(update_dir):
                counters["source_files_scanned"] += 1
                snapshot = snapshot_date(source_name)
                for line in stream:
                    counters["rows_read"] += 1
                    if not line.strip():
                        counters["empty_lines_skipped"] += 1
                        continue
                    try:
                        video_id, views, rate, ratings, comments = (
                            parse_crawl_update(line)
                        )
                    except InvalidRecord as exc:
                        rejected[exc.reason] += 1
                        continue
                    counters["valid_rows_before_deduplication"] += 1
                    batch.append(
                        (
                            video_id,
                            views,
                            rate,
                            ratings,
                            comments,
                            snapshot,
                            source_order,
                        )
                    )
                    source_order += 1
                    if len(batch) >= WRITE_BATCH_SIZE:
                        connection.executemany(CRAWL_UPSERT_SQL, batch)
                        batch.clear()
                    if show_progress and counters["rows_read"] % 1_000_000 == 0:
                        print(
                            f"Loaded {counters['rows_read']:,} crawl update rows.",
                            flush=True,
                        )

            if batch:
                connection.executemany(CRAWL_UPSERT_SQL, batch)
            if counters["source_files_scanned"] == 0:
                raise ValueError(f"No update.txt files found in {update_dir}")
            connection.commit()
            counters["unique_update_records"] = connection.execute(
                "SELECT COUNT(*) FROM crawl_updates"
            ).fetchone()[0]
            counters["duplicate_rows_reduced"] = (
                counters["valid_rows_before_deduplication"]
                - counters["unique_update_records"]
            )
            counters["rejected_rows"] = sum(rejected.values())

            staged_csv = staging_path / "videos.csv"
            video_rows = 0
            updated_video_rows = 0
            with videos_csv.open(encoding="utf-8-sig", newline="") as input_stream:
                reader = csv.DictReader(input_stream)
                required_columns = {
                    "video_id",
                    "views",
                    "rate",
                    "ratings",
                    "comments",
                }
                if reader.fieldnames is None or not required_columns.issubset(
                    reader.fieldnames
                ):
                    raise ValueError(
                        "Processed video CSV must contain video_id, views, rate, "
                        f"ratings, and comments: {videos_csv}"
                    )
                output_columns = list(reader.fieldnames)
                with staged_csv.open(
                    "w", encoding="utf-8", newline=""
                ) as output_stream:
                    writer = csv.DictWriter(
                        output_stream,
                        fieldnames=output_columns,
                        extrasaction="ignore",
                    )
                    writer.writeheader()
                    for row in reader:
                        if None in row:
                            raise ValueError(
                                "Processed video CSV contains a row with more "
                                "columns than its header."
                            )
                        video_rows += 1
                        update = connection.execute(
                            """
                            SELECT views, rate, ratings, comments
                            FROM crawl_updates WHERE video_id = ?
                            """,
                            (row.get("video_id") or "",),
                        ).fetchone()
                        if update is not None:
                            (
                                row["views"],
                                row["rate"],
                                row["ratings"],
                                row["comments"],
                            ) = update
                            connection.execute(
                                "UPDATE crawl_updates SET matched = 1 "
                                "WHERE video_id = ?",
                                (row["video_id"],),
                            )
                            updated_video_rows += 1
                        writer.writerow(row)
                        if show_progress and video_rows % 1_000_000 == 0:
                            print(
                                f"Applied crawl updates to {video_rows:,} video rows.",
                                flush=True,
                            )

            counters["video_rows_processed"] = video_rows
            counters["video_rows_updated"] = updated_video_rows
            counters["update_records_without_video_data"] = connection.execute(
                "SELECT COUNT(*) FROM crawl_updates WHERE matched = 0"
            ).fetchone()[0]
            crawl_report: dict[str, object] = dict(sorted(counters.items()))
            crawl_report["rejected_by_reason"] = dict(sorted(rejected.items()))
            report: dict[str, object] = {}
            if report_path.is_file():
                with report_path.open(encoding="utf-8") as report_stream:
                    existing_report = json.load(report_stream)
                if not isinstance(existing_report, dict):
                    raise ValueError(
                        f"Processing report must contain a JSON object: "
                        f"{report_path}"
                    )
                report = existing_report
            report["crawl_updates_directory"] = str(update_dir)
            report["crawl_updates"] = crawl_report
            staged_report = staging_path / "processing_report.json"
            with staged_report.open("w", encoding="utf-8", newline="\n") as stream:
                json.dump(report, stream, indent=2, ensure_ascii=False)
                stream.write("\n")

        os.replace(staged_csv, videos_csv)
        os.replace(staged_report, report_path)

    return crawl_report


def _upsert_video(
    video: Video,
    snapshot: int,
    source_order: int,
) -> tuple[str, str, int, str, int, int, float, int, int, str, int, int, int]:
    return (
        video.video_id,
        video.uploader,
        video.age,
        video.category,
        video.length,
        video.views,
        video.rate,
        video.ratings,
        video.comments,
        json.dumps(video.related_ids),
        snapshot,
        len(video.related_ids),
        source_order,
    )


def process_dataset(
    input_dir: Path,
    output_dir: Path,
    enrichment_dir: Path | None = None,
    user_info_dir: Path | None = None,
    crawl_updates_dir: Path | None = None,
    *,
    show_progress: bool = False,
) -> dict[str, object]:
    """Process all source files, deduplicate snapshots, and write clean outputs."""
    input_dir = input_dir.resolve()
    output_dir = output_dir.resolve()
    if enrichment_dir is not None:
        enrichment_dir = enrichment_dir.resolve()
        if not enrichment_dir.is_dir():
            raise FileNotFoundError(
                f"Enrichment directory does not exist: {enrichment_dir}"
            )
    if user_info_dir is not None:
        user_info_dir = user_info_dir.resolve()
        if not user_info_dir.is_dir():
            raise FileNotFoundError(
                f"User information directory does not exist: {user_info_dir}"
            )
    if crawl_updates_dir is not None:
        crawl_updates_dir = crawl_updates_dir.resolve()
        if not crawl_updates_dir.is_dir():
            raise FileNotFoundError(
                f"Crawl updates directory does not exist: {crawl_updates_dir}"
            )
    if not input_dir.is_dir():
        raise FileNotFoundError(f"Input directory does not exist: {input_dir}")
    try:
        output_dir.relative_to(input_dir)
    except ValueError:
        pass
    else:
        raise ValueError("Output directory must not be inside the input directory.")

    output_dir.mkdir(parents=True, exist_ok=True)
    counters: Counter[str] = Counter()
    rejected: Counter[str] = Counter()
    enrichment_report: dict[str, object] = {}
    user_information_report: dict[str, object] = {}
    crawl_updates_report: dict[str, object] = {}
    source_order = 0
    video_batch: list[
        tuple[str, str, int, str, int, int, float, int, int, str, int, int, int]
    ] = []

    with tempfile.TemporaryDirectory(prefix="dataset-reduction-") as temp_dir:
        database_path = Path(temp_dir) / "records.sqlite3"
        with closing(sqlite3.connect(database_path)) as connection:
            connection.execute("PRAGMA journal_mode = OFF")
            connection.execute("PRAGMA synchronous = OFF")
            connection.execute("PRAGMA temp_store = MEMORY")
            connection.execute("PRAGMA cache_size = -262144")
            _create_staging_table(connection)
            _create_enrichment_table(connection)
            _create_user_information_table(connection)
            _create_crawl_updates_table(connection)

            for source_name, stream in _iter_text_sources(input_dir):
                counters["text_files_scanned"] += 1
                snapshot = snapshot_date(source_name)
                for line in stream:
                    counters["rows_read"] += 1
                    if not line.strip():
                        counters["empty_lines_skipped"] += 1
                        continue
                    try:
                        (
                            video,
                            invalid_related,
                            duplicate_related,
                            excess_related,
                        ) = parse_record(line)
                    except InvalidRecord as exc:
                        rejected[exc.reason] += 1
                        continue
                    counters["valid_rows_before_deduplication"] += 1
                    counters["invalid_related_ids_removed"] += invalid_related
                    counters["duplicate_related_ids_removed"] += duplicate_related
                    counters["excess_related_ids_removed"] += excess_related
                    video_batch.append(_upsert_video(video, snapshot, source_order))
                    source_order += 1
                    if len(video_batch) >= WRITE_BATCH_SIZE:
                        connection.executemany(VIDEO_UPSERT_SQL, video_batch)
                        video_batch.clear()
                    if show_progress and counters["rows_read"] % 1_000_000 == 0:
                        print(
                            f"Read {counters['rows_read']:,} video source rows.",
                            flush=True,
                        )
            if video_batch:
                connection.executemany(VIDEO_UPSERT_SQL, video_batch)
                video_batch.clear()
            if enrichment_dir is not None:
                enrichment_report = _load_video_enrichment(
                    connection, enrichment_dir
                )
            if user_info_dir is not None:
                user_information_report = _load_user_information(
                    connection, user_info_dir, show_progress=show_progress
                )
            if crawl_updates_dir is not None:
                crawl_updates_report = _load_crawl_updates(
                    connection, crawl_updates_dir, show_progress=show_progress
                )
            connection.commit()

            connection.execute(
                "CREATE INDEX videos_uploader_idx ON videos (uploader)"
            )
            unique_records = connection.execute(
                "SELECT COUNT(*) FROM videos"
            ).fetchone()[0]
            counters["unique_video_records"] = unique_records
            counters["duplicate_rows_reduced"] = (
                counters["valid_rows_before_deduplication"] - unique_records
            )
            counters["rejected_rows"] = sum(rejected.values())
            enrichment_summary: dict[str, object] = (
                enrichment_report
                if enrichment_dir is not None
                else {
                    "source_files_scanned": 0,
                    "unique_video_ids_with_size": 0,
                    "unique_video_ids_with_bitrate": 0,
                    "note": "No enrichment directory was provided.",
                }
            )
            report: dict[str, object] = {
                "input_directory": str(input_dir),
                "text_files_scanned": counters["text_files_scanned"],
                "rows_read": counters["rows_read"],
                "empty_lines_skipped": counters["empty_lines_skipped"],
                "valid_rows_before_deduplication": counters[
                    "valid_rows_before_deduplication"
                ],
                "unique_video_records": counters["unique_video_records"],
                "duplicate_rows_reduced": counters["duplicate_rows_reduced"],
                "rejected_rows": counters["rejected_rows"],
                "rejected_by_reason": dict(sorted(rejected.items())),
                "invalid_related_ids_removed": counters[
                    "invalid_related_ids_removed"
                ],
                "duplicate_related_ids_removed": counters[
                    "duplicate_related_ids_removed"
                ],
                "excess_related_ids_removed": counters[
                    "excess_related_ids_removed"
                ],
                "snapshot_selection": (
                    "Newest dated snapshot per video ID; on equal dates, prefer "
                    "the row with more related IDs, then the earliest source row."
                ),
            }
            if crawl_updates_dir is not None:
                matched_updates = connection.execute(
                    """
                    SELECT COUNT(*)
                    FROM crawl_updates AS c
                    INNER JOIN videos AS v USING (video_id)
                    """
                ).fetchone()[0]
                crawl_updates_report["video_rows_processed"] = unique_records
                crawl_updates_report["video_rows_updated"] = matched_updates
                crawl_updates_report["update_records_without_video_data"] = (
                    crawl_updates_report["unique_update_records"] - matched_updates
                )
                report["crawl_updates_directory"] = str(crawl_updates_dir)
                report["crawl_updates"] = crawl_updates_report
            else:
                report["crawl_updates"] = {
                    "source_files_scanned": 0,
                    "unique_update_records": 0,
                    "video_rows_updated": 0,
                    "note": "No crawl updates directory was provided.",
                }
            if enrichment_dir is not None:
                report["enrichment_directory"] = str(enrichment_dir)
            report["enrichment"] = enrichment_summary
            if user_info_dir is not None:
                user_information_report["user_ids_with_video_data"] = (
                    connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM user_information AS u
                        WHERE EXISTS (
                            SELECT 1 FROM videos AS v WHERE v.uploader = u.user_id
                        )
                        """
                    ).fetchone()[0]
                )
                user_information_report["user_ids_not_in_video_data"] = (
                    connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM user_information AS u
                        LEFT JOIN videos AS v ON v.uploader = u.user_id
                        WHERE v.video_id IS NULL
                        """
                    ).fetchone()[0]
                )
                videos_with_user_information = (
                    connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM videos AS v
                        JOIN user_information AS u ON u.user_id = v.uploader
                        """
                    ).fetchone()[0]
                )
                user_information_report["videos_with_user_information"] = (
                    videos_with_user_information
                )
                user_information_report["videos_without_user_information"] = (
                    unique_records - videos_with_user_information
                )
                report["user_information_directory"] = str(user_info_dir)
                report["user_information"] = user_information_report
            else:
                report["user_information"] = {
                    "source_files_scanned": 0,
                    "unique_user_records": 0,
                    "videos_with_user_information": 0,
                    "note": "No user information directory was provided.",
                }

            with tempfile.TemporaryDirectory(
                prefix=".dataset-processing-", dir=output_dir
            ) as staging_dir:
                staged_csv = Path(staging_dir) / "videos.csv"
                staged_report = Path(staging_dir) / "processing_report.json"
                enrichment_summary["size_ids_not_in_video_data"] = (
                    connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM video_enrichment AS e
                        LEFT JOIN videos AS v USING (video_id)
                        WHERE e.file_size_bytes IS NOT NULL AND v.video_id IS NULL
                        """
                    ).fetchone()[0]
                )
                enrichment_summary["bitrate_ids_not_in_video_data"] = (
                    connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM video_enrichment AS e
                        LEFT JOIN videos AS v USING (video_id)
                        WHERE e.bitrate_priority >= 0 AND v.video_id IS NULL
                        """
                    ).fetchone()[0]
                )
                with staged_csv.open(
                    "w", encoding="utf-8", newline=""
                ) as csv_stream:
                    writer = csv.writer(csv_stream)
                    writer.writerow(CSV_COLUMNS)
                    for row in connection.execute(
                        """
                        SELECT v.video_id, v.uploader, v.age, v.category, v.length,
                               COALESCE(c.views, v.views),
                               COALESCE(c.rate, v.rate),
                               COALESCE(c.ratings, v.ratings),
                               COALESCE(c.comments, v.comments), v.related_ids,
                               e.file_size_bytes, e.size_length, e.bitrate_kbps,
                               e.bitrate_length, e.bitrate_invalid,
                               e.bitrate_priority, u.uploads, u.watches, u.friends
                        FROM videos AS v
                        LEFT JOIN video_enrichment AS e USING (video_id)
                        LEFT JOIN user_information AS u ON u.user_id = v.uploader
                        LEFT JOIN crawl_updates AS c USING (video_id)
                        ORDER BY v.video_id
                        """
                    ):
                        (
                            video_id,
                            uploader,
                            age,
                            category,
                            length,
                            views,
                            rate,
                            ratings,
                            comments,
                            related_ids,
                            size_bytes,
                            size_length,
                            bitrate,
                            bitrate_length,
                            bitrate_invalid,
                            bitrate_priority,
                            uploader_uploads,
                            uploader_watches,
                            uploader_friends,
                        ) = row
                        if size_bytes is not None and size_length != length:
                            counters["enrichment_size_length_mismatches"] += 1
                            size_bytes = None
                        output_bitrate: float | str = ""
                        bitrate_method = ""
                        if bitrate_priority is not None and bitrate_priority >= 0:
                            if bitrate_length != length:
                                counters["enrichment_bitrate_length_mismatches"] += 1
                            elif bitrate is not None:
                                output_bitrate = bitrate
                                bitrate_method = "measured"
                            elif (
                                bitrate_invalid
                                and size_bytes is not None
                                and length > 0
                            ):
                                output_bitrate = round(
                                    size_bytes * 8 / (length * 1000), 3
                                )
                                bitrate_method = "calculated_from_file_size"
                        if size_bytes is not None:
                            counters["videos_with_file_size"] += 1
                        if output_bitrate != "":
                            counters["videos_with_bitrate"] += 1
                            counters[f"bitrate_{bitrate_method}"] += 1
                        writer.writerow(
                            (
                                video_id,
                                uploader,
                                age,
                                category,
                                length,
                                views,
                                rate,
                                ratings,
                                comments,
                                related_ids,
                                size_bytes if size_bytes is not None else "",
                                output_bitrate,
                                bitrate_method,
                                uploader_uploads
                                if uploader_uploads is not None
                                else "",
                                uploader_watches
                                if uploader_watches is not None
                                else "",
                                uploader_friends
                                if uploader_friends is not None
                                else "",
                            )
                        )
                enrichment_summary["videos_with_file_size"] = counters[
                    "videos_with_file_size"
                ]
                enrichment_summary["videos_with_bitrate"] = counters[
                    "videos_with_bitrate"
                ]
                enrichment_summary["measured_bitrates"] = counters[
                    "bitrate_measured"
                ]
                enrichment_summary["calculated_bitrates"] = counters[
                    "bitrate_calculated_from_file_size"
                ]
                enrichment_summary["size_length_mismatches"] = counters[
                    "enrichment_size_length_mismatches"
                ]
                enrichment_summary["bitrate_length_mismatches"] = counters[
                    "enrichment_bitrate_length_mismatches"
                ]
                enrichment_summary["valid_bitrate_range_kbps"] = [
                    MIN_VALID_BITRATE_KBPS,
                    MAX_VALID_BITRATE_KBPS,
                ]
                enrichment_summary["calculated_bitrate_formula"] = (
                    "file_size_bytes * 8 / (length_seconds * 1000), "
                    "rounded to 3 decimal places"
                )
                with staged_report.open("w", encoding="utf-8", newline="\n") as report_stream:
                    json.dump(report, report_stream, indent=2, ensure_ascii=False)
                    report_stream.write("\n")
                os.replace(staged_csv, output_dir / "videos.csv")
                os.replace(staged_report, output_dir / "processing_report.json")

    return report


def main() -> None:
    repository_dir = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(
        description=(
            "Clean, transform, and deduplicate YouTube video data from loose "
            "text files and ZIP archives."
        )
    )
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=repository_dir / "Dataset_Files",
        help="source directory (default: Dataset_Files)",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=repository_dir / "Processed_Data",
        help="destination directory (default: Processed_Data)",
    )
    default_enrichment_dir = repository_dir / "Data_FileSize_Bitrate"
    parser.add_argument(
        "--enrichment-dir",
        type=Path,
        default=default_enrichment_dir if default_enrichment_dir.is_dir() else None,
        help=(
            "optional source directory for file sizes and bitrates "
            "(default: Data_FileSize_Bitrate when present)"
        ),
    )
    default_user_info_dir = repository_dir / "Datasets_User_Information"
    parser.add_argument(
        "--user-info-dir",
        type=Path,
        default=default_user_info_dir if default_user_info_dir.is_dir() else None,
        help=(
            "optional source directory for uploader uploads, watches, and friends "
            "(default: Datasets_User_Information when present)"
        ),
    )
    default_crawl_updates_dir = repository_dir / "Updating_Crawl_Data"
    parser.add_argument(
        "--crawl-updates-dir",
        type=Path,
        default=(
            default_crawl_updates_dir
            if default_crawl_updates_dir.is_dir()
            else None
        ),
        help=(
            "source directory for incremental update.txt records "
            "(default: Updating_Crawl_Data when present)"
        ),
    )
    merge_group = parser.add_mutually_exclusive_group()
    merge_group.add_argument(
        "--merge-user-info",
        action="store_true",
        help=(
            "join user metrics into an existing videos.csv without rescanning "
            "Dataset_Files"
        ),
    )
    merge_group.add_argument(
        "--merge-crawl-updates",
        action="store_true",
        help=(
            "apply the newest valid incremental crawl metrics to an existing "
            "videos.csv without rescanning Dataset_Files"
        ),
    )
    args = parser.parse_args()

    if args.merge_crawl_updates:
        if args.crawl_updates_dir is None:
            parser.error(
                "--crawl-updates-dir is required for --merge-crawl-updates"
            )
        update_report = merge_crawl_updates(
            args.output_dir / "videos.csv",
            args.crawl_updates_dir,
            show_progress=True,
        )
        print(
            f"Updated {args.output_dir.resolve() / 'videos.csv'}; "
            f"{update_report['video_rows_updated']:,} existing video rows "
            "received crawl metrics."
        )
        return

    if args.merge_user_info:
        if args.user_info_dir is None:
            parser.error("--user-info-dir is required for --merge-user-info")
        user_report = merge_user_information(
            args.output_dir / "videos.csv",
            args.user_info_dir,
            show_progress=True,
        )
        print(
            f"Updated {args.output_dir.resolve() / 'videos.csv'}; "
            f"{user_report['videos_with_user_information']:,} video rows "
            "matched uploader information."
        )
        return

    report = process_dataset(
        args.input_dir,
        args.output_dir,
        args.enrichment_dir,
        args.user_info_dir,
        args.crawl_updates_dir,
        show_progress=True,
    )
    print(
        f"Wrote {report['unique_video_records']} unique videos to "
        f"{args.output_dir.resolve()}"
    )
    print(
        f"Read {report['rows_read']} rows; rejected "
        f"{report['rejected_rows']} malformed rows and reduced "
        f"{report['duplicate_rows_reduced']} duplicate snapshots."
    )


if __name__ == "__main__":
    main()
