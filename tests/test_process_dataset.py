import csv
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from process_dataset import (
    InvalidRecord,
    merge_crawl_updates,
    merge_user_information,
    parse_crawl_update,
    parse_record,
    parse_user_record,
    process_dataset,
    snapshot_date,
)


def record(
    video_id: str,
    *,
    uploader: str = "sample_uploader",
    views: int = 15,
    related_ids: tuple[str, ...] = (),
) -> str:
    fields = [
        video_id,
        uploader,
        "100",
        "Music",
        "30",
        str(views),
        "4.5",
        "2",
        "1",
        *related_ids,
    ]
    return "\t".join(fields)


class ParseRecordTests(unittest.TestCase):
    def test_parses_incremental_crawl_metrics(self) -> None:
        self.assertEqual(
            parse_crawl_update("abcdefghijk\t15\t4.5\t2\t1\n"),
            ("abcdefghijk", 15, 4.5, 2, 1),
        )

    def test_rejects_invalid_incremental_crawl_metrics(self) -> None:
        with self.assertRaisesRegex(InvalidRecord, "invalid_crawl_update_field_count"):
            parse_crawl_update("abcdefghijk\t15\t4.5\n")
        with self.assertRaisesRegex(InvalidRecord, "negative_comments"):
            parse_crawl_update("abcdefghijk\t15\t4.5\t2\t-1")

    def test_parses_typed_fields_and_cleans_related_ids(self) -> None:
        video, invalid_related, duplicate_related, excess_related = parse_record(
            record(
                "abcdefghijk",
                related_ids=("12345678901", "abcdefghijk", "12345678901", "bad"),
            )
        )

        self.assertEqual(video.video_id, "abcdefghijk")
        self.assertEqual(video.age, 100)
        self.assertEqual(video.rate, 4.5)
        self.assertEqual(video.related_ids, ("12345678901", "abcdefghijk"))
        self.assertEqual(invalid_related, 1)
        self.assertEqual(duplicate_related, 1)
        self.assertEqual(excess_related, 0)

    def test_caps_related_ids_at_twenty(self) -> None:
        related_ids = tuple(f"{number:011d}" for number in range(1, 22))
        video, _, _, excess_related = parse_record(
            record("abcdefghijk", related_ids=related_ids)
        )

        self.assertEqual(len(video.related_ids), 20)
        self.assertEqual(excess_related, 1)

    def test_rejects_rows_without_all_required_fields(self) -> None:
        with self.assertRaisesRegex(InvalidRecord, "too_few_fields"):
            parse_record("abcdefghijk\tuser")

    def test_rejects_invalid_rates(self) -> None:
        fields = record("abcdefghijk").split("\t")
        fields[6] = "5.1"
        with self.assertRaisesRegex(InvalidRecord, "rate_out_of_range"):
            parse_record("\t".join(fields))

    def test_parses_user_metrics_with_optional_friends(self) -> None:
        full = parse_user_record("sample_uploader\t5\t11\t3\n")
        partial = parse_user_record("sample_uploader\t8\t13\n")

        self.assertEqual((full.uploads, full.watches, full.friends), (5, 11, 3))
        self.assertEqual(
            (partial.uploads, partial.watches, partial.friends), (8, 13, None)
        )

    def test_rejects_negative_user_metrics(self) -> None:
        with self.assertRaisesRegex(InvalidRecord, "negative_uploads"):
            parse_user_record("sample_uploader\t-1\t11\t3\n")


class ProcessDatasetTests(unittest.TestCase):
    def test_large_input_reduces_duplicates_across_write_batches(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "Dataset_Files" / "080327"
            output_dir = root / "Processed_Data"
            input_dir.mkdir(parents=True)
            source_path = input_dir / "0.txt"
            with source_path.open("w", encoding="utf-8", newline="") as stream:
                for number in range(10_000):
                    stream.write(record(f"{number:011d}") + "\n")
                stream.write(
                    record(
                        "00000000001",
                        related_ids=("ZYXWVUTSRQP",),
                    )
                    + "\n"
                )

            report = process_dataset(input_dir.parent, output_dir)

            with (output_dir / "videos.csv").open(
                encoding="utf-8", newline=""
            ) as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 10_000)
            self.assertEqual(report["duplicate_rows_reduced"], 1)
            selected = next(row for row in rows if row["video_id"] == "00000000001")
            self.assertEqual(selected["related_ids"], '["ZYXWVUTSRQP"]')

    def test_merges_latest_clean_crawl_updates_into_existing_videos(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_dir = root / "Processed_Data"
            update_dir = root / "Updating_Crawl_Data"
            output_dir.mkdir()
            update_dir.mkdir()
            videos_csv = output_dir / "videos.csv"
            with videos_csv.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(
                    ("video_id", "views", "rate", "ratings", "comments", "related_ids")
                )
                writer.writerow(
                    ("abcdefghijk", "10", "3.0", "1", "0", '["ZYXWVUTSRQP"]')
                )
                writer.writerow(("ZYXWVUTSRQP", "5", "2.0", "1", "0", "[]"))
            with zipfile.ZipFile(update_dir / "080827u.zip", "w") as archive:
                archive.writestr(
                    "080827/update.txt",
                    "abcdefghijk\t20\t4.25\t3\t2\n"
                    "abcdefghijk\t21\t4.5\t4\t3\n"
                    "invalid\t1\t3\t1\t0\n"
                    "12345678901\t7\t3.5\t1\t0\n",
                )
                archive.writestr("080827/id.txt", "ignored\n")
                archive.writestr("080827/logu.txt", "ignored\n")
            with zipfile.ZipFile(update_dir / "080903u.zip", "w") as archive:
                archive.writestr(
                    "080903/update.txt",
                    "abcdefghijk\t25\t4.75\t5\t4\n"
                    "ZYXWVUTSRQP\t9\t4\t2\t1\n"
                    "ZYXWVUTSRQP\t99\t1\t99\t99\n"
                    "abcdefghijk\tbad\t4.75\t5\t4\n",
                )
            (output_dir / "processing_report.json").write_text(
                json.dumps({"existing_field": True}), encoding="utf-8"
            )

            report = merge_crawl_updates(videos_csv, update_dir)

            with videos_csv.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 2)
            self.assertEqual(
                (rows[0]["views"], rows[0]["rate"], rows[0]["ratings"], rows[0]["comments"]),
                ("25", "4.75", "5", "4"),
            )
            self.assertEqual(
                rows[0]["related_ids"], '["ZYXWVUTSRQP"]'
            )
            self.assertEqual(
                (rows[1]["views"], rows[1]["rate"], rows[1]["ratings"], rows[1]["comments"]),
                ("9", "4.0", "2", "1"),
            )
            self.assertEqual(report["duplicate_rows_reduced"], 3)
            self.assertEqual(report["video_rows_updated"], 2)
            self.assertEqual(report["update_records_without_video_data"], 1)
            self.assertEqual(report["rejected_by_reason"], {"invalid_video_id": 1, "invalid_views": 1})
            saved_report = json.loads(
                (output_dir / "processing_report.json").read_text(encoding="utf-8")
            )
            self.assertTrue(saved_report["existing_field"])
            self.assertEqual(saved_report["crawl_updates"], report)

    def test_reads_loose_and_zipped_files_and_keeps_latest_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "Dataset_Files"
            output_dir = root / "Processed_Data"
            early_dir = input_dir / "0301" / "0301"
            early_dir.mkdir(parents=True)
            (early_dir / "0.txt").write_text(
                record("abcdefghijk", views=10, related_ids=("12345678901",))
                + "\nmalformed\n",
                encoding="utf-8",
            )
            (early_dir / "log.txt").write_text("log\tfile\n", encoding="utf-8")

            archive_path = input_dir / "080327.zip"
            with zipfile.ZipFile(archive_path, "w") as archive:
                archive.writestr(
                    "080327/0.txt",
                    record(
                        "abcdefghijk",
                        views=99,
                        related_ids=("12345678901", "ZYXWVUTSRQP"),
                    )
                    + "\n"
                    + record("ZYXWVUTSRQP", views=5)
                    + "\n",
                )
                archive.writestr("080327/log.txt", "log\tfile\n")

            report = process_dataset(input_dir, output_dir)

            with (output_dir / "videos.csv").open(
                encoding="utf-8", newline=""
            ) as stream:
                rows = list(csv.DictReader(stream))
            by_id = {row["video_id"]: row for row in rows}
            self.assertEqual(by_id["abcdefghijk"]["views"], "99")
            self.assertEqual(
                json.loads(by_id["abcdefghijk"]["related_ids"]),
                ["12345678901", "ZYXWVUTSRQP"],
            )
            self.assertEqual(report["unique_video_records"], 2)
            self.assertEqual(report["duplicate_rows_reduced"], 1)
            self.assertEqual(report["rejected_by_reason"], {"too_few_fields": 1})
            self.assertTrue((output_dir / "processing_report.json").is_file())

    def test_full_build_merges_video_enrichment_users_and_crawl_updates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "Dataset_Files"
            output_dir = root / "Processed_Data"
            enrichment_dir = root / "Data_FileSize_Bitrate"
            user_info_dir = root / "Datasets_User_Information"
            crawl_updates_dir = root / "Updating_Crawl_Data"
            for directory in (
                input_dir,
                enrichment_dir,
                user_info_dir,
                crawl_updates_dir,
            ):
                directory.mkdir()
            with zipfile.ZipFile(input_dir / "080327.zip", "w") as archive:
                archive.writestr(
                    "080327/0.txt",
                    record("abcdefghijk", uploader="sample_uploader") + "\n",
                )
            (enrichment_dir / "size.txt").write_text(
                "abcdefghijk\t30\t1200000\n", encoding="utf-8"
            )
            (enrichment_dir / "rate.txt").write_text(
                "abcdefghijk\t30\t-1.#QNAN\n", encoding="utf-8"
            )
            (user_info_dir / "user.txt").write_text(
                "sample_uploader\t8\t13\t2\n", encoding="utf-8"
            )
            with zipfile.ZipFile(crawl_updates_dir / "080903u.zip", "w") as archive:
                archive.writestr(
                    "080903/update.txt",
                    "abcdefghijk\t25\t4.75\t5\t4\n",
                )

            report = process_dataset(
                input_dir,
                output_dir,
                enrichment_dir,
                user_info_dir,
                crawl_updates_dir,
            )

            with (output_dir / "videos.csv").open(
                encoding="utf-8", newline=""
            ) as stream:
                row = next(csv.DictReader(stream))
            self.assertEqual(
                (row["views"], row["rate"], row["ratings"], row["comments"]),
                ("25", "4.75", "5", "4"),
            )
            self.assertEqual(row["file_size_bytes"], "1200000")
            self.assertEqual(row["bitrate_kbps"], "320.0")
            self.assertEqual(row["uploader_uploads"], "8")
            self.assertEqual(row["uploader_watches"], "13")
            self.assertEqual(row["uploader_friends"], "2")
            self.assertEqual(report["crawl_updates"]["video_rows_updated"], 1)

    def test_integrates_sizes_and_normalizes_invalid_bitrates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "Dataset_Files"
            output_dir = root / "Processed_Data"
            enrichment_dir = root / "Data_FileSize_Bitrate"
            input_dir.mkdir()
            enrichment_dir.mkdir()
            with zipfile.ZipFile(input_dir / "080327.zip", "w") as archive:
                archive.writestr(
                    "080327/0.txt",
                    "\n".join(
                        record(video_id)
                        for video_id in (
                            "abcdefghijk",
                            "ZYXWVUTSRQP",
                            "12345678901",
                        )
                    )
                    + "\n",
                )

            with zipfile.ZipFile(enrichment_dir / "0523.zip", "w") as archive:
                archive.writestr(
                    "0523/size.txt",
                    "abcdefghijk\t30\t1200000\n"
                    "ZYXWVUTSRQP\t30\t600000\n"
                    "12345678901\t30\t600000\n",
                )
            with zipfile.ZipFile(enrichment_dir / "0628.zip", "w") as archive:
                archive.writestr(
                    "0628/rate.txt",
                    "abcdefghijk\t30\t-1.#QNAN\n"
                    "ZYXWVUTSRQP\t30\t350.5\n"
                    "12345678901\t30\t350.5\n"
                    "invalid\t30\t400\n",
                )
            with zipfile.ZipFile(
                enrichment_dir / "080908sizerate.zip", "w"
            ) as archive:
                archive.writestr(
                    "080908sizerate/sizerate.txt",
                    "ZYXWVUTSRQP\t30\t900000\tVBR\n",
                )

            report = process_dataset(input_dir, output_dir, enrichment_dir)

            with (output_dir / "videos.csv").open(
                encoding="utf-8", newline=""
            ) as stream:
                by_id = {
                    row["video_id"]: row for row in csv.DictReader(stream)
                }
            self.assertEqual(by_id["abcdefghijk"]["file_size_bytes"], "1200000")
            self.assertEqual(by_id["abcdefghijk"]["bitrate_kbps"], "320.0")
            self.assertEqual(
                by_id["abcdefghijk"]["bitrate_method"],
                "calculated_from_file_size",
            )
            self.assertEqual(by_id["ZYXWVUTSRQP"]["file_size_bytes"], "900000")
            self.assertEqual(by_id["ZYXWVUTSRQP"]["bitrate_kbps"], "240.0")
            self.assertEqual(
                by_id["12345678901"]["bitrate_kbps"], "350.5"
            )
            self.assertEqual(
                by_id["12345678901"]["bitrate_method"], "measured"
            )
            enrichment = report["enrichment"]
            self.assertEqual(enrichment["invalid_source_rows"], 1)
            self.assertEqual(enrichment["calculated_bitrates"], 2)
            self.assertEqual(enrichment["measured_bitrates"], 1)

    def test_joins_cleaned_latest_user_metrics_to_videos(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "Dataset_Files"
            output_dir = root / "Processed_Data"
            user_info_dir = root / "Datasets_User_Information"
            input_dir.mkdir()
            user_info_dir.mkdir()
            with zipfile.ZipFile(input_dir / "080903.zip", "w") as archive:
                archive.writestr(
                    "080903/0.txt",
                    record("abcdefghijk", uploader="sample_uploader") + "\n",
                )
            with zipfile.ZipFile(user_info_dir / "0528.zip", "w") as archive:
                archive.writestr(
                    "0528/user.txt",
                    "sample_uploader\t5\t11\t3\n"
                    "older_only\t1\t2\t3\n"
                    "invalid_user\t-1\t0\t4\n",
                )
                archive.writestr("0528/userid.txt", "should_not_be_loaded\n")
            with zipfile.ZipFile(
                user_info_dir / "080903user.zip", "w"
            ) as archive:
                archive.writestr(
                    "080903user/user.txt",
                    "sample_uploader\t8\t13\n"
                    "newer_only\t2\t4\n",
                )

            report = process_dataset(
                input_dir, output_dir, user_info_dir=user_info_dir
            )

            with (output_dir / "videos.csv").open(
                encoding="utf-8", newline=""
            ) as stream:
                row = next(csv.DictReader(stream))
            self.assertEqual(row["uploader_uploads"], "8")
            self.assertEqual(row["uploader_watches"], "13")
            self.assertEqual(row["uploader_friends"], "")
            user_report = report["user_information"]
            self.assertEqual(user_report["unique_user_records"], 3)
            self.assertEqual(user_report["duplicate_rows_reduced"], 1)
            self.assertEqual(
                user_report["rejected_by_reason"], {"negative_uploads": 1}
            )
            self.assertEqual(user_report["videos_with_user_information"], 1)
            self.assertEqual(user_report["videos_without_user_information"], 0)
            self.assertEqual(user_report["user_ids_with_video_data"], 1)
            self.assertEqual(user_report["user_ids_not_in_video_data"], 2)

    def test_reads_legacy_windows_1252_user_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            input_dir = root / "Dataset_Files"
            output_dir = root / "Processed_Data"
            user_info_dir = root / "Datasets_User_Information"
            input_dir.mkdir()
            user_info_dir.mkdir()
            with zipfile.ZipFile(input_dir / "080903.zip", "w") as archive:
                archive.writestr(
                    "080903/0.txt",
                    record("abcdefghijk", uploader="sample_uploader") + "\n",
                )
            (user_info_dir / "user.txt").write_bytes(
                b"sample_uploader\t1\t2\r\ncaf\xe9\t3\t4\r\n"
            )

            report = process_dataset(
                input_dir, output_dir, user_info_dir=user_info_dir
            )

            self.assertEqual(
                report["user_information"]["source_encoding_cp1252"], 1
            )
            with (output_dir / "videos.csv").open(
                encoding="utf-8", newline=""
            ) as stream:
                row = next(csv.DictReader(stream))
            self.assertEqual(row["uploader_uploads"], "1")
            self.assertEqual(row["uploader_watches"], "2")

    def test_merges_user_information_into_an_existing_videos_csv(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            output_dir = root / "Processed_Data"
            user_info_dir = root / "Datasets_User_Information"
            output_dir.mkdir()
            user_info_dir.mkdir()
            videos_csv = output_dir / "videos.csv"
            with videos_csv.open("w", encoding="utf-8", newline="") as stream:
                writer = csv.writer(stream)
                writer.writerow(("video_id", "uploader", "views"))
                writer.writerow(("abcdefghijk", "sample_uploader", "15"))
                writer.writerow(("ZYXWVUTSRQP", "no_user_match", "4"))
            (output_dir / "processing_report.json").write_text(
                json.dumps({"unique_video_records": 2, "preserved": True}),
                encoding="utf-8",
            )
            (user_info_dir / "user.txt").write_text(
                "sample_uploader\t8\t13\t2\n",
                encoding="utf-8",
            )

            report = merge_user_information(videos_csv, user_info_dir)

            with videos_csv.open(encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(rows[0]["uploader_uploads"], "8")
            self.assertEqual(rows[0]["uploader_watches"], "13")
            self.assertEqual(rows[0]["uploader_friends"], "2")
            self.assertEqual(rows[1]["uploader_uploads"], "")
            self.assertEqual(report["videos_with_user_information"], 1)
            self.assertEqual(report["videos_without_user_information"], 1)
            merged_report = json.loads(
                (output_dir / "processing_report.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertTrue(merged_report["preserved"])
            self.assertEqual(
                merged_report["user_information"]["unique_user_records"], 1
            )

            repeated_report = merge_user_information(videos_csv, user_info_dir)
            self.assertEqual(
                repeated_report["videos_with_user_information"], 1
            )
            with videos_csv.open(encoding="utf-8", newline="") as stream:
                self.assertEqual(
                    csv.DictReader(stream).fieldnames.count("uploader_uploads"),
                    1,
                )

    def test_output_cannot_be_nested_under_input(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            input_dir = Path(temp_dir) / "Dataset_Files"
            input_dir.mkdir()
            with self.assertRaisesRegex(ValueError, "must not be inside"):
                process_dataset(input_dir, input_dir / "Processed_Data")

    def test_snapshot_dates_support_old_and_new_file_names(self) -> None:
        self.assertEqual(snapshot_date("Dataset_Files/0301/0.txt"), 20070301)
        self.assertEqual(snapshot_date("Dataset_Files/080327.zip!080327/0.txt"), 20080327)
        self.assertEqual(snapshot_date("unknown/0.txt"), 0)


if __name__ == "__main__":
    unittest.main()
