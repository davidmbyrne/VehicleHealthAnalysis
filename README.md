# PX4 ULog Data Pipeline

Process PX4 flight logs from S3 to generate vehicle health and risk analysis reports.

## Quick Start (Recommended: Parallel Streaming)

The fastest way to process logs without storing them locally:

```bash
python3 parallel_streaming_pipeline.py \
  --bucket rm-prophet \
  --prefix ulogs/ \
  --vehicles "EL-045,EL-046" \
  --workers 6
```

**Note:** All scripts are run from the project root directory.

This will:
1. Stream logs directly from S3 (no local storage)
2. Process multiple logs in parallel
3. Generate `output/summaries.csv`, `output/aggregated_by_vehicle.csv`, and `output/report.md`

**Arguments:**
- `--bucket` - S3 bucket name (required)
- `--prefix` - S3 prefix path (required)
- `--vehicles` - Comma or space-separated vehicle IDs (optional, prompts if omitted)
- `--workers` - Number of parallel workers (default: CPU count)
- `--summaries_csv` - Output path for per-log summaries (default: `output/summaries.csv`)
- `--aggregated_csv` - Output path for aggregated data (default: `output/aggregated_by_vehicle.csv`)
- `--report_path` - Output path for markdown report (default: `output/report.md`)
- `--resume` - Append to existing outputs instead of overwriting
- `--prefetch N` - Limit number of logs to process (0 = no limit)

## Generate PDF Reports

### Main Report (Vehicle Statistics)
```bash
python3 reports/render_pdf.py --report output/report.md --pdf output/report.pdf
```

### Risk Analysis Report
```bash
# Generate risk report
python3 reports/risk_analysis.py --output output/risk_report.md

# Convert to PDF (dead vehicles highlighted in red)
python3 reports/render_risk_pdf.py --report output/risk_report.md --pdf output/risk_report.pdf
```

The risk PDF will automatically highlight vehicles marked as dead in `config/isDead.csv` (where `dead=1`) with a light red background.

## Other Pipeline Options

### Single-threaded Streaming (Slower, No Local Storage)

If you prefer sequential processing:

```bash
python3 streaming_pipeline.py \
  --bucket rm-prophet \
  --prefix ulogs/ \
  --vehicles "EL-045,EL-046"
```

### Local Pipeline (Downloads Files First)

If you have disk space and want to keep logs locally:

```bash
python3 main.py \
  --bucket rm-prophet \
  --prefix ulogs/ \
  --local_ulogs data/ulogs \
  --vehicles "EL-045,EL-046"
```

## Metrics Computed

### Accelerometer Vibration Bins
- Time spent in acceleration magnitude ranges: <30, 30-50, 50-70, >70 m/s²

### Motor Output Analysis
- Time each motor (0-3) spends above 0.8, 0.9, and 1.0 (saturation) output levels

### Fatigue Metrics
- **Peak acceleration events**: Count of samples >100 m/s²
- **Accelerometer clipping**: Count of samples where any axis exceeds 150 m/s² or shows saturation

## Risk Analysis

Generate a risk report ranking vehicles by composite risk score:

```bash
python3 reports/risk_analysis.py \
  --aggregated_csv output/aggregated_by_vehicle.csv \
  --output output/risk_report.md
```

**Risk Score Components:**
- **Fatigue (60%)**: Peak events and clipping samples per hour
- **Motor (20%)**: Saturation and high output time percentages
- **Vibration (20%)**: High and medium vibration time percentages

## Setup

```bash
# Install dependencies
python3 -m pip install -r requirements.txt

# Set AWS credentials (if needed)
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
```

## Output Files

- `output/summaries.csv` - Per-log metrics
- `output/aggregated_by_vehicle.csv` - Aggregated metrics by vehicle
- `output/report.md` - Human-readable Markdown report
- `output/report.pdf` - Branded PDF report
- `output/risk_report.md` - Risk analysis report
- `output/risk_report.pdf` - Risk analysis PDF (with dead vehicle highlighting)

## Project Structure

See `STRUCTURE.md` for a detailed overview of the codebase organization.

## Notes

- All pipelines skip corrupt or invalid logs automatically
- The parallel streaming pipeline is recommended for speed and efficiency
- Vehicle filtering is case-insensitive and supports formats like "EL-045" or "el-045"
- Dead vehicle highlighting requires `config/isDead.csv` (copy from `config/isDead.csv.example`)
- Output files are gitignored - see `.gitignore` for details
