#!/usr/bin/env python3
"""
Main orchestrator for the local PX4 ULog processing pipeline.

Stages:
  1) download_from_s3.py: Download .ulg files to a local folder
  2) process_ulog.py: Extract key topics/metrics per log
  3) summarize_data.py: Compute per-log summary statistics
  4) aggregate_reports.py: Aggregate summaries by vehicle
  5) generate_report.py: Produce final human-readable report

Configuration is passed via CLI flags or environment variables.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from utils.logging_utils import get_logger
from pipeline.download_from_s3 import download_ulog_folder
from pipeline.process_ulog import process_one_ulog, CorruptULogError
from pipeline.summarize_data import summarize_processed_log
from pipeline.aggregate_reports import aggregate_summaries_by_vehicle
from pipeline.generate_report import generate_final_report


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PX4 ULog local pipeline orchestrator")
    parser.add_argument("--bucket", required=True, help="S3 bucket name (e.g., rm-prophet)")
    parser.add_argument("--prefix", required=True, help="S3 prefix (e.g., ulogs/)")
    parser.add_argument("--local_ulogs", default="data/ulogs", help="Local directory for downloaded ulogs")
    parser.add_argument(
        "--vehicles",
        nargs="+",
        default=None,
        help="Filter to one or more vehicle IDs (e.g., EL-040 EL-041). If omitted, you will be prompted.",
    )
    parser.add_argument("--summaries_csv", default="output/summaries.csv", help="Output per-log summaries CSV path")
    parser.add_argument("--aggregated_csv", default="output/aggregated_by_vehicle.csv", help="Output aggregated CSV path")
    parser.add_argument("--report_path", default="output/report.md", help="Final human-readable report path")
    parser.add_argument("--min_duration_min", type=float, default=10.0, help="Minimum flight duration (minutes)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logger = get_logger(__name__)

    vehicles = args.vehicles
    if not vehicles:
        response = input("Enter vehicle IDs (comma-separated, leave blank for all vehicles): ").strip()
        if response:
            vehicles = [v.strip() for v in response.split(",") if v.strip()]
        else:
            vehicles = None

    vehicles_display = ", ".join(vehicles) if vehicles else "ALL"

    # 1) Download all .ulg files under the prefix
    ulog_dir = Path(args.local_ulogs)
    ulog_dir.mkdir(parents=True, exist_ok=True)
    logger.info(
        "Downloading ULog files from S3: s3://%s/%s -> %s (vehicles=%s)",
        args.bucket,
        args.prefix,
        ulog_dir,
        vehicles_display,
    )
    downloaded_paths = download_ulog_folder(
        bucket=args.bucket,
        prefix=args.prefix,
        local_root=ulog_dir,
        include_vehicles=vehicles,
    )
    logger.info("Downloaded %d ULog files", len(downloaded_paths))

    # 2) Process each ULog and write per-log summaries
    summaries_csv = Path(args.summaries_csv)
    summaries_csv.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Processing logs and writing per-log summaries to %s", summaries_csv)
    for ulg_path in downloaded_paths:
        try:
            processed = process_one_ulog(ulg_path)
            # 3) Summarize into a single row and append
            summarize_processed_log(processed, summaries_csv, min_duration_min=args.min_duration_min)
        except CorruptULogError as exc:
            logger.warning("Skipping corrupt log %s: %s", ulg_path, exc)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Failed to process %s: %s", ulg_path, exc)

    # 4) Aggregate summaries by vehicle
    aggregated_csv = Path(args.aggregated_csv)
    logger.info("Aggregating per-log summaries by vehicle -> %s", aggregated_csv)
    aggregate_summaries_by_vehicle(summaries_csv, aggregated_csv)

    # 5) Generate a human-readable report
    report_path = Path(args.report_path)
    logger.info("Generating final report -> %s", report_path)
    generate_final_report(aggregated_csv, report_path)
    logger.info("Pipeline complete.")


if __name__ == "__main__":
    main()


