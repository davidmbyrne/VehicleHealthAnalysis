# Local PX4 ULog Pipeline (Skeleton)

This project downloads PX4 `.ulg` logs from S3 and runs a local, modular pipeline:

- download_from_s3.py → process_ulog.py → summarize_data.py → aggregate_reports.py → generate_report.py

## Quickstart

```bash
python -m pip install -r requirements.txt
python3 main.py --bucket rm-prophet --prefix ulogs/ \
  --local_ulogs data/ulogs --summaries_csv output/summaries.csv \
  --aggregated_csv output/aggregated_by_vehicle.csv --report_path output/report.md
```

Credentials: export your AWS credentials (see `s3priv.md` if you maintain keys there).

When you run `main.py` it will prompt for vehicle IDs (comma-separated). Leave blank to process all vehicles. You can also bypass the prompt with `--vehicles EL-040 EL-041`.

To create a styled PDF from the Markdown report:

```bash
python3 render_pdf.py --report output/report.md --pdf output/report.pdf
```

## Streamed Processing (No Local Log Storage)

If disk space is tight, use the streaming runner to pull each log directly from S3,
process it, and discard it immediately:

```bash
python3 streaming_pipeline.py --bucket rm-prophet --prefix ulogs/ \
  --summaries_csv output/summaries.csv --aggregated_csv output/aggregated_by_vehicle.csv \
  --report_path output/report.md --vehicles EL-045 EL-046
```

Add `--resume` if you want to append to existing outputs instead of starting fresh.
If you omit `--vehicles`, you’ll be prompted for a comma-separated list (press Enter for all vehicles).

## Parallel Streaming (Faster, Still No Local Storage)

To overlap downloads and parsing across multiple workers:

```bash
python3 parallel_streaming_pipeline.py --bucket rm-prophet --prefix ulogs/ \
  --summaries_csv output/summaries.csv --aggregated_csv output/aggregated_by_vehicle.csv \
  --report_path output/report.md --workers 6 --vehicles EL-045 EL-046
```

- `--workers` sets concurrency (default = CPU count).
- `--prefetch N` limits how many logs to process (helpful for testing).
- Same `--resume` and vehicle filtering behaviour as the other runners.

## Notes
- This is a skeleton with placeholders; extend `process_ulog.py` to extract topics/metrics.
- All logs and outputs are local; no Spark/Glue.

