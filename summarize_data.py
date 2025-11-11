from __future__ import annotations

"""
Compute per-log summaries and append to a CSV.
"""

import csv
from pathlib import Path
from typing import Dict, List

from utils.logging_utils import get_logger
from process_ulog import ProcessedULog


logger = get_logger(__name__)


SUMMARY_FIELDS = [
    "file",
    "vehicle_id",
    "accel_time_lt_30_s",
    "accel_time_30_50_s",
    "accel_time_50_70_s",
    "accel_time_gt_70_s",
    "accel_total_time_s",
    "accel_pct_lt_30",
    "accel_pct_30_50",
    "accel_pct_50_70",
    "accel_pct_gt_70",
]


def summarize_processed_log(processed: ProcessedULog, out_csv: Path, min_duration_min: float = 10.0) -> None:
    """Append a single summary row for the processed ULog.

    Placeholder: duration filtering is a stub; integrate actual duration when available.
    """
    # TODO: integrate actual duration filter; for now, we write every row
    row = {
        "file": processed.source_path.name,
        "vehicle_id": processed.vehicle_id or "unknown",
        "accel_time_lt_30_s": processed.accel_time_lt_30_s,
        "accel_time_30_50_s": processed.accel_time_30_50_s,
        "accel_time_50_70_s": processed.accel_time_50_70_s,
        "accel_time_gt_70_s": processed.accel_time_gt_70_s,
        "accel_total_time_s": processed.accel_total_time_s,
    }
    tot = processed.accel_total_time_s or 0.0
    if tot > 0:
        row["accel_pct_lt_30"] = processed.accel_time_lt_30_s / tot
        row["accel_pct_30_50"] = processed.accel_time_30_50_s / tot
        row["accel_pct_50_70"] = processed.accel_time_50_70_s / tot
        row["accel_pct_gt_70"] = processed.accel_time_gt_70_s / tot
    else:
        row["accel_pct_lt_30"] = 0.0
        row["accel_pct_30_50"] = 0.0
        row["accel_pct_50_70"] = 0.0
        row["accel_pct_gt_70"] = 0.0

    added_fields = False
    for key, value in (processed.motor_time_above_thresholds or {}).items():
        row[key] = value
        if key not in SUMMARY_FIELDS:
            SUMMARY_FIELDS.append(key)
            added_fields = True

    # Add fatigue metrics
    for key, value in (processed.fatigue_metrics or {}).items():
        row[key] = value
        if key not in SUMMARY_FIELDS:
            SUMMARY_FIELDS.append(key)
            added_fields = True

    if added_fields:
        _ensure_summary_fieldnames(out_csv, SUMMARY_FIELDS)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    _append_dict_row(out_csv, row, SUMMARY_FIELDS)


def _append_dict_row(csv_path: Path, row: Dict[str, object], fieldnames: list[str]) -> None:
    is_new = not csv_path.exists() or csv_path.stat().st_size == 0
    with csv_path.open("a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if is_new:
            writer.writeheader()
        writer.writerow({k: row.get(k, "") for k in fieldnames})


def _ensure_summary_fieldnames(csv_path: Path, fieldnames: List[str]) -> None:
    """Rewrite existing summary CSV to include any newly added fieldnames."""
    if not csv_path.exists() or csv_path.stat().st_size == 0:
        return

    with csv_path.open("r", newline="") as fh:
        reader = csv.DictReader(fh)
        existing_rows = list(reader)
        existing_fieldnames = list(reader.fieldnames or [])

    if not existing_fieldnames:
        return

    updated_fieldnames = existing_fieldnames[:]
    changed = False
    for field in fieldnames:
        if field not in updated_fieldnames:
            updated_fieldnames.append(field)
            changed = True

    if not changed:
        return

    with csv_path.open("w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=updated_fieldnames)
        writer.writeheader()
        for row in existing_rows:
            writer.writerow({k: row.get(k, "") for k in updated_fieldnames})

    # Update caller's list in-place to preserve order parity.
    fieldnames[:] = updated_fieldnames

