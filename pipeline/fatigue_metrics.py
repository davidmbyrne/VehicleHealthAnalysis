from __future__ import annotations

"""
Fatigue metrics for vehicle component health tracking.

Focuses on metrics relevant to motor clamp and structural fatigue:
- Peak acceleration events (transient spikes)
- Motor imbalance (uneven loading)
- Rapid motor output changes (stress-inducing rate changes)
- Landing impact detection (high-stress events)
"""

from typing import Dict

import numpy as np
from pyulog import ULog

from utils.logging_utils import get_logger


logger = get_logger(__name__)

# Thresholds for fatigue metrics
PEAK_ACCEL_THRESHOLD = 100.0  # m/s² - transient spikes above this

# Accelerometer clipping detection
# Common sensor ranges: ±16g (~156 m/s²), ±32g (~313 m/s²)
# Detect clipping when any axis exceeds reasonable maximum or shows saturation
ACCEL_CLIP_THRESHOLD = 150.0  # m/s² - likely clipping for ±16g sensors
ACCEL_CLIP_TOLERANCE = 2.0  # m/s² - tolerance for detecting pinned values


def compute_fatigue_metrics(ulog: ULog) -> Dict[str, float]:
    """
    Compute fatigue-related metrics from ULog data.
    
    Returns dictionary with:
    - peak_accel_events: Count of transient acceleration spikes >100 m/s²
    - accel_clipping_time_s: Time (seconds) when accelerometer is clipping/saturated
    - accel_clipping_events: Count of samples where clipping occurs (total clipping samples)
    """
    results: Dict[str, float] = {
        "peak_accel_events": 0.0,
        "accel_clipping_time_s": 0.0,
        "accel_clipping_events": 0.0,
    }
    
    # Get accelerometer data (need raw axes for clipping detection)
    accel_data = _get_accel_data(ulog)
    if accel_data is None:
        return results
    
    ts_accel, accel_mag = accel_data
    
    # Get raw accelerometer axes for clipping detection
    accel_raw = _get_accel_raw(ulog)
    if accel_raw is not None:
        ts_raw, x_raw, y_raw, z_raw = accel_raw
        clipping_metrics = _compute_clipping(ts_raw, x_raw, y_raw, z_raw)
        results.update(clipping_metrics)
    
    # Compute peak acceleration events
    accel_metrics = _compute_accel_fatigue(ts_accel, accel_mag)
    results.update(accel_metrics)
    
    return results


def _get_accel_data(ulog: ULog) -> tuple[np.ndarray, np.ndarray] | None:
    """Extract accelerometer magnitude time series."""
    accel_msgs = [m for m in ulog.data_list if m.name == "sensor_accel"]
    if not accel_msgs:
        return None
    
    def count_samples(m) -> int:
        ts = m.data.get("timestamp")
        return len(ts) if ts is not None else 0
    
    accel = max(accel_msgs, key=count_samples)
    
    ts = accel.data.get("timestamp")
    x = accel.data.get("x")
    y = accel.data.get("y")
    z = accel.data.get("z")
    
    if ts is None or x is None or y is None or z is None:
        return None
    
    ts_arr = np.asarray(ts, dtype=np.float64)
    x_arr = np.asarray(x, dtype=np.float64)
    y_arr = np.asarray(y, dtype=np.float64)
    z_arr = np.asarray(z, dtype=np.float64)
    
    if ts_arr.size < 2:
        return None
    
    mag = np.sqrt(x_arr * x_arr + y_arr * y_arr + z_arr * z_arr)
    return ts_arr, mag


def _get_accel_raw(ulog: ULog) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
    """Extract raw accelerometer axes for clipping detection."""
    accel_msgs = [m for m in ulog.data_list if m.name == "sensor_accel"]
    if not accel_msgs:
        return None
    
    def count_samples(m) -> int:
        ts = m.data.get("timestamp")
        return len(ts) if ts is not None else 0
    
    accel = max(accel_msgs, key=count_samples)
    
    ts = accel.data.get("timestamp")
    x = accel.data.get("x")
    y = accel.data.get("y")
    z = accel.data.get("z")
    
    if ts is None or x is None or y is None or z is None:
        return None
    
    ts_arr = np.asarray(ts, dtype=np.float64)
    x_arr = np.asarray(x, dtype=np.float64)
    y_arr = np.asarray(y, dtype=np.float64)
    z_arr = np.asarray(z, dtype=np.float64)
    
    if ts_arr.size < 2:
        return None
    
    return ts_arr, x_arr, y_arr, z_arr




def _compute_accel_fatigue(ts: np.ndarray, mag: np.ndarray) -> Dict[str, float]:
    """Compute accelerometer-based fatigue metrics."""
    results: Dict[str, float] = {
        "peak_accel_events": 0.0,
    }
    
    if mag.size == 0:
        return results
    
    # Peak acceleration events: count spikes above threshold
    peak_mask = mag > PEAK_ACCEL_THRESHOLD
    results["peak_accel_events"] = float(np.sum(peak_mask))
    
    return results


def _compute_clipping(
    ts: np.ndarray, x: np.ndarray, y: np.ndarray, z: np.ndarray
) -> Dict[str, float]:
    """Detect accelerometer clipping/saturation events."""
    results: Dict[str, float] = {
        "accel_clipping_time_s": 0.0,
        "accel_clipping_events": 0.0,
    }
    
    if ts.size < 2:
        return results
    
    # Detect clipping: any axis exceeds threshold OR shows saturation (pinned at high value)
    abs_x = np.abs(x)
    abs_y = np.abs(y)
    abs_z = np.abs(z)
    
    # Method 1: Direct threshold clipping (any axis > threshold)
    clip_mask_threshold = (
        (abs_x > ACCEL_CLIP_THRESHOLD) |
        (abs_y > ACCEL_CLIP_THRESHOLD) |
        (abs_z > ACCEL_CLIP_THRESHOLD)
    )
    
    # Method 2: Detect saturation (values pinned near maximum)
    # Look for axes that are consistently at high values with low variance
    if ts.size >= 10:  # Need enough samples for variance calculation
        window_size = min(10, ts.size // 10)  # Small rolling window
        if window_size >= 3:
            # Compute rolling variance for each axis
            x_var = _rolling_variance(abs_x, window_size)
            y_var = _rolling_variance(abs_y, window_size)
            z_var = _rolling_variance(abs_z, window_size)
            
            # Saturation: high magnitude AND low variance (pinned value)
            saturation_mask = (
                ((abs_x > ACCEL_CLIP_THRESHOLD * 0.9) & (x_var < ACCEL_CLIP_TOLERANCE)) |
                ((abs_y > ACCEL_CLIP_THRESHOLD * 0.9) & (y_var < ACCEL_CLIP_TOLERANCE)) |
                ((abs_z > ACCEL_CLIP_THRESHOLD * 0.9) & (z_var < ACCEL_CLIP_TOLERANCE))
            )
        else:
            saturation_mask = np.zeros_like(abs_x, dtype=bool)
    else:
        saturation_mask = np.zeros_like(abs_x, dtype=bool)
    
    # Combined clipping mask
    clip_mask = clip_mask_threshold | saturation_mask
    
    if clip_mask.any():
        # Compute time spent clipping
        dt = np.diff(ts) / 1e6  # seconds
        clip_mask_dt = clip_mask[:-1]  # Align with dt
        if clip_mask_dt.size == dt.size:
            results["accel_clipping_time_s"] = float(dt[clip_mask_dt].sum())
        
        # Count clipping samples: total number of samples where clipping occurs
        # This counts every individual sample that is clipping
        num_clipping_samples = int(np.sum(clip_mask))
        results["accel_clipping_events"] = float(num_clipping_samples)
    
    return results


def _rolling_variance(arr: np.ndarray, window: int) -> np.ndarray:
    """Compute rolling variance with a simple window."""
    if arr.size < window:
        return np.zeros_like(arr)
    
    result = np.zeros_like(arr)
    half_window = window // 2
    
    for i in range(arr.size):
        start = max(0, i - half_window)
        end = min(arr.size, i + half_window + 1)
        window_data = arr[start:end]
        if window_data.size > 1:
            result[i] = np.var(window_data)
        else:
            result[i] = 0.0
    
    return result



