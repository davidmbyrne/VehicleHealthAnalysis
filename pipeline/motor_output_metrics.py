from __future__ import annotations

"""
Utilities for extracting motor output statistics from PX4 ULog files.

Currently supports computing the cumulative time each motor spends above
specified actuator output thresholds (normalized 0.0–1.0 scale).
"""

import math
from typing import Dict, Iterable, Mapping, Sequence

import numpy as np
from pyulog import ULog

from utils.logging_utils import get_logger


logger = get_logger(__name__)

# Default thresholds requested by analytics: 80%, 90%, and saturation (100%).
DEFAULT_MOTOR_OUTPUT_THRESHOLDS: Sequence[float] = (0.8, 0.9, 1.0)

MAX_MOTOR_INDEX = 3
SATURATION_ABS_TOL = 1e-4

# Candidate dataset names that commonly carry motor output data.
_MOTOR_DATASET_CANDIDATES = (
    "actuator_outputs",
    "actuator_motors",
    "fmu_outputs",
    "actuator_controls_0",
)


def compute_motor_output_time_above_thresholds(
    ulog: ULog, thresholds: Sequence[float] | None = None
) -> Dict[str, float]:
    """
    Compute cumulative time (seconds) each motor output stays above thresholds.

    Returns a flat dictionary keyed as:
        motor{index}_time_above_{threshold_label}_s -> duration in seconds

    threshold_label is the threshold value with '.' replaced by '_'.
    """
    if thresholds is None:
        thresholds = DEFAULT_MOTOR_OUTPUT_THRESHOLDS

    thresholds = tuple(sorted({float(t) for t in thresholds}))
    threshold_labels: Dict[float, str] = {t: str(t).replace(".", "_") for t in thresholds}

    dataset = _select_motor_dataset(ulog)
    if dataset is None:
        logger.debug("No actuator output dataset found in %s", ulog.url)
        return {}

    timestamps = dataset.data.get("timestamp")
    if timestamps is None:
        logger.debug("Dataset %s missing timestamp field", dataset.name)
        return {}

    ts = np.asarray(timestamps, dtype=np.float64)
    if ts.size < 2:
        return {}

    dt = np.diff(ts) / 1e6  # convert from microseconds to seconds
    if not np.isfinite(dt).any():
        return {}

    channels = _extract_motor_channels(dataset.data)
    if not channels:
        logger.debug("Dataset %s contains no recognizable motor output channels", dataset.name)
        return {}

    results: Dict[str, float] = {}
    for motor_idx, samples in sorted(channels.items()):
        channel_values = np.asarray(samples, dtype=np.float64)
        if channel_values.size == 0:
            continue

        # Align actuator samples with dt intervals.
        length = min(channel_values.size, dt.size)
        if length == 0:
            continue
        values = channel_values[:length]
        dt_aligned = dt[:length]

        valid_mask = np.isfinite(values) & np.isfinite(dt_aligned)
        if not valid_mask.any():
            continue
        values = values[valid_mask]
        dt_valid = dt_aligned[valid_mask]

        if values.size == 0:
            continue

        for threshold in thresholds:
            if math.isclose(threshold, 1.0, rel_tol=0.0, abs_tol=SATURATION_ABS_TOL):
                above_mask = values >= (1.0 - SATURATION_ABS_TOL)
            else:
                above_mask = values >= threshold
            duration = float(dt_valid[above_mask].sum())
            key = f"motor{motor_idx}_time_above_{threshold_labels[threshold]}_s"
            results[key] = duration

    return results


def _select_motor_dataset(ulog: ULog):
    candidates = [
        dataset
        for dataset in ulog.data_list
        if dataset.name in _MOTOR_DATASET_CANDIDATES
    ]
    if not candidates:
        return None

    def _sample_count(dataset) -> int:
        ts = dataset.data.get("timestamp")
        return len(ts) if ts is not None else 0

    return max(candidates, key=_sample_count)


def _extract_motor_channels(
    data: Mapping[str, Iterable[float]],
) -> Dict[int, np.ndarray]:
    """
    Extract per-motor time series from a ULog dataset data dictionary.

    Supports both vector-style fields (e.g. 'output') and indexed fields
    (e.g. 'output[0]', 'control[3]', 'output3').
    """
    channels: Dict[int, np.ndarray] = {}

    vector_field = data.get("output")
    if vector_field is not None:
        arr = np.asarray(vector_field, dtype=np.float64)
        if arr.ndim == 1:
            channels[0] = arr
        elif arr.ndim == 2:
            for idx in range(arr.shape[1]):
                channels[idx] = arr[:, idx]

    prefixes = ("output", "control")
    for name in data.keys():
        idx = None
        for prefix in prefixes:
            if name.startswith(f"{prefix}[") and name.endswith("]"):
                maybe = name[len(prefix) + 1 : -1]
                if maybe.isdigit():
                    idx = int(maybe)
                    break
            elif name.startswith(prefix) and name[len(prefix) :].isdigit():
                idx = int(name[len(prefix) :])
                break
        if idx is None:
            continue
        if idx in channels:
            # Prefer vector extraction over scalar duplicates.
            continue
        values = data.get(name)
        if values is None:
            continue
        arr = np.asarray(values, dtype=np.float64)
        if arr.size == 0:
            continue
        channels[idx] = arr

    return {
        idx: arr
        for idx, arr in channels.items()
        if 0 <= idx <= MAX_MOTOR_INDEX
    }


