from __future__ import annotations

"""
Process a PX4 ULog into a structured intermediate payload.

Responsibilities:
- Read ULog via pyulog
- Extract topics of interest (subset; extend as needed)
- Compute basic derived signals (e.g., quaternion -> RPY), placeholders here
- Return a dictionary suitable for summarization
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

import numpy as np
from pyulog import ULog

from utils.logging_utils import get_logger
from pipeline.motor_output_metrics import compute_motor_output_time_above_thresholds, DEFAULT_MOTOR_OUTPUT_THRESHOLDS
from pipeline.fatigue_metrics import compute_fatigue_metrics


logger = get_logger(__name__)


class CorruptULogError(Exception):
    """Raised when a ULog is unreadable or missing required structure."""


class DataQualityError(CorruptULogError):
    """Raised when computed metrics fail basic data quality checks."""


@dataclass
class ProcessedULog:
    source_path: Path
    vehicle_id: Optional[str]
    # Accelerometer vibration time bins (seconds)
    accel_time_lt_30_s: float = 0.0
    accel_time_30_50_s: float = 0.0
    accel_time_50_70_s: float = 0.0
    accel_time_gt_70_s: float = 0.0
    accel_total_time_s: float = 0.0
    motor_time_above_thresholds: Dict[str, float] = field(default_factory=dict)
    # Fatigue metrics
    fatigue_metrics: Dict[str, float] = field(default_factory=dict)


def process_one_ulog(ulog_path: Path) -> ProcessedULog:
    """Read and minimally parse a ULog file.

    For now we check presence of topics; detailed extraction can be added.
    """
    logger.debug("Processing %s", ulog_path)
    try:
        u = ULog(str(ulog_path))
    except Exception as exc:  # noqa: BLE001
        raise CorruptULogError(f"Failed to parse ULog: {ulog_path}") from exc

    if not getattr(u, "data_list", None):
        raise CorruptULogError(f"ULog contains no datasets: {ulog_path}")

    vehicle_id = _infer_vehicle_from_path(ulog_path)

    # Compute accelerometer vibration time bins
    accel_bins = _compute_accel_time_bins(u)
    motor_bins = compute_motor_output_time_above_thresholds(u, DEFAULT_MOTOR_OUTPUT_THRESHOLDS)
    fatigue_metrics = compute_fatigue_metrics(u)
    _validate_time_metrics(ulog_path, accel_bins, motor_bins, fatigue_metrics)

    processed = ProcessedULog(
        source_path=ulog_path,
        vehicle_id=vehicle_id,
        accel_time_lt_30_s=accel_bins.get("lt_30_s", 0.0),
        accel_time_30_50_s=accel_bins.get("30_50_s", 0.0),
        accel_time_50_70_s=accel_bins.get("50_70_s", 0.0),
        accel_time_gt_70_s=accel_bins.get("gt_70_s", 0.0),
        accel_total_time_s=accel_bins.get("total_s", 0.0),
        motor_time_above_thresholds=motor_bins,
        fatigue_metrics=fatigue_metrics,
    )
    return processed


def _infer_vehicle_from_path(path: Path) -> Optional[str]:
    """Extract a vehicle label from the path (e.g., EL-040)."""
    parts = [p for p in path.parts if p]
    for token in parts:
        if token.lower().startswith("el-") or token.lower().startswith("el_"):
            return token
    return None


def _compute_accel_time_bins(u: ULog) -> Dict[str, float]:
    """
    Compute time spent in accelerometer magnitude bins (m/s^2):
      <30, 30-50, 50-70, >70
    Uses the 'sensor_accel' topic. If multiple instances exist, pick the instance
    with the most samples.
    """
    # Gather all sensor_accel datasets
    accel_msgs = [m for m in u.data_list if m.name == "sensor_accel"]
    if not accel_msgs:
        return {"lt_30_s": 0.0, "30_50_s": 0.0, "50_70_s": 0.0, "gt_70_s": 0.0, "total_s": 0.0}

    # Select the instance with maximum number of samples
    def count_samples(m) -> int:
        ts = m.data.get("timestamp")
        return len(ts) if ts is not None else 0
    accel = max(accel_msgs, key=count_samples)

    ts = accel.data.get("timestamp")
    x = accel.data.get("x")
    y = accel.data.get("y")
    z = accel.data.get("z")
    if ts is None or x is None or y is None or z is None:
        return {"lt_30_s": 0.0, "30_50_s": 0.0, "50_70_s": 0.0, "gt_70_s": 0.0, "total_s": 0.0}

    ts = np.asarray(ts, dtype=np.float64)
    x = np.asarray(x, dtype=np.float64)
    y = np.asarray(y, dtype=np.float64)
    z = np.asarray(z, dtype=np.float64)
    if ts.size < 2:
        return {"lt_30_s": 0.0, "30_50_s": 0.0, "50_70_s": 0.0, "gt_70_s": 0.0, "total_s": 0.0}

    # Compute magnitude and time deltas (assume timestamp in microseconds)
    mag = np.sqrt(x * x + y * y + z * z)
    dt = np.diff(ts) / 1e6  # seconds
    # Align mag with dt by excluding last sample
    mag_dt = mag[:-1]

    # Define bins
    lt_30 = (mag_dt < 30.0)
    b_30_50 = (mag_dt >= 30.0) & (mag_dt < 50.0)
    b_50_70 = (mag_dt >= 50.0) & (mag_dt < 70.0)
    gt_70 = (mag_dt >= 70.0)

    # Sum durations
    t_lt_30 = float(dt[lt_30].sum())
    t_30_50 = float(dt[b_30_50].sum())
    t_50_70 = float(dt[b_50_70].sum())
    t_gt_70 = float(dt[gt_70].sum())
    total = float(dt.sum())

    return {
        "lt_30_s": t_lt_30,
        "30_50_s": t_30_50,
        "50_70_s": t_50_70,
        "gt_70_s": t_gt_70,
        "total_s": total,
    }


def _validate_time_metrics(
    source_path: Path,
    accel_bins: Dict[str, float],
    motor_bins: Dict[str, float],
    fatigue_metrics: Dict[str, float],
) -> None:
    """Ensure all time-based metrics are finite and non-negative."""

    def _is_valid(value: float) -> bool:
        try:
            v = float(value)
        except (TypeError, ValueError):
            return False
        return np.isfinite(v) and v >= 0.0

    invalid_entries = []
    for key, value in accel_bins.items():
        if not _is_valid(value):
            invalid_entries.append((key, value))

    for key, value in motor_bins.items():
        if not _is_valid(value):
            invalid_entries.append((key, value))

    # Fatigue metrics: events and counts can be 0, but should be finite and non-negative
    for key, value in fatigue_metrics.items():
        if not _is_valid(value):
            invalid_entries.append((key, value))

    if invalid_entries:
        detail = ", ".join(f"{k}={v}" for k, v in invalid_entries[:5])
        if len(invalid_entries) > 5:
            detail += ", ..."
        raise DataQualityError(
            f"Negative or invalid time metrics detected ({detail}) for {source_path}"
        )


