#!/usr/bin/env python3
from __future__ import annotations

"""
Parallel streaming pipeline:
Download PX4 ULogs from S3, process them concurrently, and write summaries locally.
"""

import argparse
import concurrent.futures
import os
import tempfile
import threading
from pathlib import Path
from typing import List, Optional

import boto3

from download_from_s3 import iter_s3_objects
from utils.logging_utils import get_logger
from process_ulog import process_one_ulog, CorruptULogError, DataQualityError, ProcessedULog
from summarize_data import summarize_processed_log
from aggregate_reports import aggregate_summaries_by_vehicle
from generate_report import generate_final_report
from pipeline_utils import resolve_vehicle_filter, key_matches_vehicle, update_processed_metadata


logger = get_logger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream PX4 ULogs from S3, processing multiple logs in parallel."
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
    parser.add_argument(
        "--workers",
        type=int,
        default=max(2, os.cpu_count() or 2),
        help="Number of concurrent workers (default: CPU count).",
    )
    parser.add_argument(
        "--prefetch",
        type=int,
        default=0,
        help="Optional limit for number of logs to process (0 = no limit).",
    )
    return parser.parse_args()


def parallel_stream_process_logs(
    bucket: str,
    prefix: str,
    vehicles: Optional[List[str]],
    summaries_csv: Path,
    aggregated_csv: Path,
    report_path: Path,
    min_duration_min: float,
    resume: bool,
    workers: int,
    prefetch: int,
) -> None:
    if workers <= 0:
        workers = 1

    if not resume:
        for path in (summaries_csv, aggregated_csv, report_path):
            if path.exists():
                logger.info("Removing existing output %s (resume disabled)", path)
                path.unlink()

    summaries_csv.parent.mkdir(parents=True, exist_ok=True)

    keys = _collect_matching_keys(bucket, prefix, vehicles, prefetch)
    if not keys:
        logger.warning("No matching .ulg files found under %s/%s", bucket, prefix)
        return

    logger.info("Processing %d logs using %d workers", len(keys), workers)

    processed_count = 0
    skipped_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        future_to_key = {
            executor.submit(_download_and_process_log, bucket, key): key for key in keys
        }

        for future in concurrent.futures.as_completed(future_to_key):
            key = future_to_key[future]
            try:
                processed = future.result()
            except (CorruptULogError, DataQualityError) as exc:
                skipped_count += 1
                logger.warning("Skipping corrupt/invalid log s3://%s/%s: %s", bucket, key, exc)
                continue
            except Exception as exc:  # noqa: BLE001
                skipped_count += 1
                logger.exception("Failed to process s3://%s/%s: %s", bucket, key, exc)
                continue

            processed = update_processed_metadata(processed, key)
            summarize_processed_log(processed, summaries_csv, min_duration_min=min_duration_min)
            processed_count += 1

    logger.info("Processed %d logs, skipped %d logs", processed_count, skipped_count)

    if summaries_csv.exists():
        logger.info("Aggregating summaries -> %s", aggregated_csv)
        aggregate_summaries_by_vehicle(summaries_csv, aggregated_csv)
        logger.info("Generating report -> %s", report_path)
        generate_final_report(aggregated_csv, report_path)
    else:
        logger.warning("No summaries were generated; skipping aggregation and report.")


_THREAD_LOCAL = threading.local()


def _get_s3_client() -> boto3.client:
    client = getattr(_THREAD_LOCAL, "client", None)
    if client is None:
        client = boto3.client("s3")
        _THREAD_LOCAL.client = client
    return client


def _download_and_process_log(bucket: str, key: str) -> ProcessedULog:
    client = _get_s3_client()
    with tempfile.NamedTemporaryFile(suffix=".ulg") as tmp:
        client.download_fileobj(bucket, key, tmp)
        tmp.flush()
        tmp.seek(0)
        tmp_path = Path(tmp.name)
        processed = process_one_ulog(tmp_path)
    return processed


def _collect_matching_keys(
    bucket: str,
    prefix: str,
    vehicles: Optional[List[str]],
    prefetch: int,
) -> List[str]:
    keys: List[str] = []
    for key in iter_s3_objects(bucket, prefix):
        if not key.lower().endswith(".ulg"):
            continue
        if not key_matches_vehicle(key, vehicles):
            continue
        keys.append(key)
        if prefetch > 0 and len(keys) >= prefetch:
            break
    return keys


def main() -> None:
    args = parse_args()
    vehicles = resolve_vehicle_filter(args.vehicles)
    parallel_stream_process_logs(
        bucket=args.bucket,
        prefix=args.prefix,
        vehicles=vehicles,
        summaries_csv=Path(args.summaries_csv),
        aggregated_csv=Path(args.aggregated_csv),
        report_path=Path(args.report_path),
        min_duration_min=args.min_duration_min,
        resume=args.resume,
        workers=args.workers,
        prefetch=args.prefetch,
    )


if __name__ == "__main__":
    main()


