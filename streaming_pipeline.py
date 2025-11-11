#!/usr/bin/env python3
from __future__ import annotations

"""
Process PX4 ULogs directly from S3 without persisting them locally.

This utility mirrors the behaviour of `main.py` but streams each log through a
temporary file, keeping disk usage minimal. Outputs (summaries, aggregates, and
reports) are still written locally.
"""

import argparse
import tempfile
from pathlib import Path
from typing import Iterable, List, Optional

import boto3

from pipeline.download_from_s3 import iter_s3_objects
from utils.logging_utils import get_logger
from pipeline.process_ulog import process_one_ulog, CorruptULogError, DataQualityError, ProcessedULog
from pipeline.summarize_data import summarize_processed_log
from pipeline.aggregate_reports import aggregate_summaries_by_vehicle
from pipeline.generate_report import generate_final_report
from pipeline.pipeline_utils import resolve_vehicle_filter, key_matches_vehicle, update_processed_metadata


logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream PX4 ULogs from S3 and process them without persisting locally."
    )
    parser.add_argument("--bucket", required=True, help="S3 bucket name (e.g., rm-prophet)")
    parser.add_argument("--prefix", required=True, help="S3 prefix (e.g., ulogs/)")
    parser.add_argument(
        "--vehicles",
        type=str,
        default=None,
        help="Filter to vehicle IDs (comma or space-separated, e.g., 'EL-040,EL-041' or 'EL-040 EL-041'). Case-insensitive.",
    )
    parser.add_argument(
        "--summaries_csv",
        default="output/summaries.csv",
        help="Output per-log summaries CSV path",
    )
    parser.add_argument(
        "--aggregated_csv",
        default="output/aggregated_by_vehicle.csv",
        help="Output aggregated CSV path",
    )
    parser.add_argument(
        "--report_path",
        default="output/report.md",
        help="Final human-readable report path",
    )
    parser.add_argument(
        "--min_duration_min",
        type=float,
        default=10.0,
        help="Minimum flight duration (minutes) – reserved for future use.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Append to existing output files instead of truncating them before processing.",
    )
    return parser.parse_args()


def stream_process_logs(
    bucket: str,
    prefix: str,
    vehicles: Optional[List[str]],
    summaries_csv: Path,
    aggregated_csv: Path,
    report_path: Path,
    min_duration_min: float,
    resume: bool,
) -> None:
    if not resume:
        for path in (summaries_csv, aggregated_csv, report_path):
            if path.exists():
                logger.info("Removing existing output %s (resume disabled)", path)
                path.unlink()

    summaries_csv.parent.mkdir(parents=True, exist_ok=True)

    s3 = boto3.client("s3")

    processed_count = 0
    skipped_count = 0

    for key in iter_s3_objects(bucket, prefix):
        if not key.lower().endswith(".ulg"):
            continue
        if not key_matches_vehicle(key, vehicles):
            continue

        logger.info("Processing s3://%s/%s", bucket, key)
        with tempfile.NamedTemporaryFile(suffix=".ulg") as tmp:
            try:
                s3.download_fileobj(bucket, key, tmp)
            except Exception as exc:  # noqa: BLE001
                skipped_count += 1
                logger.warning("Failed to download %s: %s", key, exc)
                continue

            tmp.flush()
            tmp.seek(0)
            tmp_path = Path(tmp.name)

            try:
                processed = process_one_ulog(tmp_path)
                processed = update_processed_metadata(processed, key)
                summarize_processed_log(processed, summaries_csv, min_duration_min=min_duration_min)
                processed_count += 1
            except (CorruptULogError, DataQualityError) as exc:
                skipped_count += 1
                logger.warning("Skipping corrupt/invalid log s3://%s/%s: %s", bucket, key, exc)
            except Exception as exc:  # noqa: BLE001
                skipped_count += 1
                logger.exception("Failed to process s3://%s/%s: %s", bucket, key, exc)

    logger.info("Processed %d logs, skipped %d logs", processed_count, skipped_count)

    if summaries_csv.exists():
        logger.info("Aggregating summaries -> %s", aggregated_csv)
        aggregate_summaries_by_vehicle(summaries_csv, aggregated_csv)
        logger.info("Generating report -> %s", report_path)
        generate_final_report(aggregated_csv, report_path)
    else:
        logger.warning("No summaries were generated; skipping aggregation and report.")


def main() -> None:
    args = parse_args()
    vehicles = resolve_vehicle_filter(args.vehicles)
    stream_process_logs(
        bucket=args.bucket,
        prefix=args.prefix,
        vehicles=vehicles,
        summaries_csv=Path(args.summaries_csv),
        aggregated_csv=Path(args.aggregated_csv),
        report_path=Path(args.report_path),
        min_duration_min=args.min_duration_min,
        resume=args.resume,
    )


if __name__ == "__main__":
    main()



