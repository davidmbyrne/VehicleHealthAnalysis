from __future__ import annotations

"""
Fatigue metrics for vehicle component health tracking.

Focuses on metrics relevant to motor clamp and structural fatigue:
- Peak acceleration events (transient spikes)
- Motor imbalance (uneven loading)
- Rapid motor output changes (stress-inducing rate changes)
- Landing impact detection (high-stress events)

Acceleration clipping detection uses PX4's pre-computed clip_counter fields
from sensor_accel topic, with retroactive detection as fallback.
"""

from typing import Dict, Optional

import numpy as np
from pyulog import ULog

from utils.logging_utils import get_logger


logger = get_logger(__name__)

# Constants
CONSTANTS_ONE_G = 9.80665  # m/s²

# Thresholds for fatigue metrics
PEAK_ACCEL_THRESHOLD = 100.0  # m/s² - transient spikes above this

# Accelerometer clipping detection (fallback method)
# Common sensor ranges: ±16g (~156 m/s²), ±32g (~313 m/s²)
# Used only if clip_counter fields are not available in sensor_accel topic
ACCEL_CLIP_THRESHOLD = 150.0  # m/s² - likely clipping for ±16g sensors
ACCEL_CLIP_TOLERANCE = 2.0  # m/s² - tolerance for detecting pinned values
CLIP_THRESHOLD_PERCENT = 0.999  # 99.9% of max range for retroactive detection


def compute_fatigue_metrics(ulog: ULog) -> Dict[str, float]:
    """
    Compute fatigue-related metrics from ULog data.
    
    Uses PX4's pre-computed clip_counter fields from sensor_accel topic (preferred method),
    with retroactive detection as fallback if clip_counter is not available.
    
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
    
    try:
        # Cache sensor_accel messages to avoid multiple iterations
        accel_msgs = [m for m in ulog.data_list if m.name == "sensor_accel"]
        if not accel_msgs:
            return results
        
        # Get accelerometer data for peak detection
        accel_data = _get_accel_data_from_messages(accel_msgs)
        if accel_data is None:
            return results
        
        ts_accel, accel_mag = accel_data
        
        # Compute clipping metrics across all sensor_accel instances
        sensor_clipping = _compute_clipping_across_sensors(accel_msgs)
        imu_status_clipping = _compute_clipping_from_vehicle_imu_status(ulog)
        
        clipping_metrics = _choose_best_clipping_metrics(sensor_clipping, imu_status_clipping)
        
        # As a last resort, fall back to retroactive detection using the sensor with the most samples
        if clipping_metrics is None:
            accel_raw = _get_accel_raw_from_messages(accel_msgs)
            if accel_raw is not None:
                ts_raw, x_raw, y_raw, z_raw = accel_raw
                clipping_metrics = _compute_clipping_retroactive(ts_raw, x_raw, y_raw, z_raw)
            else:
                clipping_metrics = {
                    "accel_clipping_time_s": 0.0,
                    "accel_clipping_events": 0.0,
                }
        
        results.update(clipping_metrics)
        
        # Compute peak acceleration events
        accel_metrics = _compute_accel_fatigue(ts_accel, accel_mag)
        results.update(accel_metrics)
        
    except Exception as e:
        logger.error("Error computing fatigue metrics: %s", e, exc_info=True)
        # Return default results on error
    
    return results


def _get_accel_data_from_messages(accel_msgs) -> tuple[np.ndarray, np.ndarray] | None:
    """Extract accelerometer magnitude time series from sensor_accel messages."""
    if not accel_msgs:
        return None
    
    def count_samples(m) -> int:
        # Prefer timestamp_sample for accurate timing, fallback to timestamp
        try:
            ts = m.data.get("timestamp_sample")
            if ts is None:
                ts = m.data.get("timestamp")
            return len(ts) if ts is not None else 0
        except Exception:
            return 0
    
    accel = max(accel_msgs, key=count_samples)
    
    # Prefer timestamp_sample for accurate timing
    ts = accel.data.get("timestamp_sample")
    if ts is None:
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


def _get_accel_data(ulog: ULog) -> tuple[np.ndarray, np.ndarray] | None:
    """Extract accelerometer magnitude time series (legacy wrapper)."""
    accel_msgs = [m for m in ulog.data_list if m.name == "sensor_accel"]
    return _get_accel_data_from_messages(accel_msgs)


def _get_accel_raw_from_messages(accel_msgs) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
    """Extract raw accelerometer axes for retroactive clipping detection from messages."""
    if not accel_msgs:
        return None
    
    def count_samples(m) -> int:
        # Prefer timestamp_sample for accurate timing, fallback to timestamp
        try:
            ts = m.data.get("timestamp_sample")
            if ts is None:
                ts = m.data.get("timestamp")
            return len(ts) if ts is not None else 0
        except Exception:
            return 0
    
    accel = max(accel_msgs, key=count_samples)
    
    # Prefer timestamp_sample for accurate timing
    ts = accel.data.get("timestamp_sample")
    if ts is None:
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


def _get_accel_raw(ulog: ULog) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
    """Extract raw accelerometer axes for retroactive clipping detection (legacy wrapper)."""
    accel_msgs = [m for m in ulog.data_list if m.name == "sensor_accel"]
    return _get_accel_raw_from_messages(accel_msgs)


def _choose_best_clipping_metrics(
    primary: Optional[Dict[str, float]],
    secondary: Optional[Dict[str, float]],
) -> Optional[Dict[str, float]]:
    """Return whichever clipping metrics report the higher event count (then time)."""
    if primary is None and secondary is None:
        return None
    if primary is None:
        return secondary
    if secondary is None:
        return primary
    
    primary_events = float(primary.get("accel_clipping_events", 0.0) or 0.0)
    secondary_events = float(secondary.get("accel_clipping_events", 0.0) or 0.0)
    if primary_events > secondary_events:
        return primary
    if secondary_events > primary_events:
        return secondary
    
    primary_time = float(primary.get("accel_clipping_time_s", 0.0) or 0.0)
    secondary_time = float(secondary.get("accel_clipping_time_s", 0.0) or 0.0)
    if primary_time >= secondary_time:
        return primary
    return secondary


def _compute_clipping_across_sensors(accel_msgs) -> Optional[Dict[str, float]]:
    """
    Compute clipping metrics across all sensor_accel instances and return the worst case.
    
    Each sensor's clip counters are inspected; we keep the instance reporting the highest
    number of clipped samples (breaking ties using clipping time). This prevents us from
    under-reporting when multiple accelerometers are present in the log.
    """
    if not accel_msgs:
        return None
    
    best_metrics: Optional[Dict[str, float]] = None
    best_events = -1.0
    best_time = -1.0
    
    for msg in accel_msgs:
        metrics = _compute_clipping_for_sensor_msg(msg)
        if metrics is None:
            continue
        
        events = float(metrics.get("accel_clipping_events", 0.0) or 0.0)
        time_s = float(metrics.get("accel_clipping_time_s", 0.0) or 0.0)
        
        if (
            best_metrics is None
            or events > best_events
            or (events == best_events and time_s > best_time)
        ):
            best_metrics = metrics
            best_events = events
            best_time = time_s
    
    if best_metrics is None:
        return None
    
    # Strip metadata keys (prefixed with "_") before returning the result.
    return {k: v for k, v in best_metrics.items() if not k.startswith("_")}


def _compute_clipping_for_sensor_msg(msg) -> Optional[Dict[str, float]]:
    """Compute clipping metrics for a single sensor_accel dataset."""
    data = msg.data
    device_id = _extract_device_id(data)
    multi_id = getattr(msg, "multi_id", None)
    
    metrics = _compute_clipping_from_clip_counter_data(data, device_id, multi_id)
    if metrics is not None:
        return metrics
    
    return _compute_clipping_retroactive_from_sensor_data(data, device_id, multi_id)


def _compute_clipping_from_clip_counter_data(
    data: Dict[str, object],
    device_id: Optional[int],
    multi_id: Optional[int],
) -> Optional[Dict[str, float]]:
    """Compute clipping metrics using clip_counter fields for a single sensor."""
    clip_counter_vector = data.get("clip_counter")
    clip_x_arr: Optional[np.ndarray] = None
    clip_y_arr: Optional[np.ndarray] = None
    clip_z_arr: Optional[np.ndarray] = None
    
    if clip_counter_vector is not None:
        clip_counter_arr = np.asarray(clip_counter_vector)
        if clip_counter_arr.ndim == 2 and clip_counter_arr.shape[1] >= 3:
            clip_x_arr = clip_counter_arr[:, 0].astype(np.int64)
            clip_y_arr = clip_counter_arr[:, 1].astype(np.int64)
            clip_z_arr = clip_counter_arr[:, 2].astype(np.int64)
        elif clip_counter_arr.ndim == 1 and clip_counter_arr.size % 3 == 0:
            reshaped = clip_counter_arr.reshape((-1, 3))
            clip_x_arr = reshaped[:, 0].astype(np.int64)
            clip_y_arr = reshaped[:, 1].astype(np.int64)
            clip_z_arr = reshaped[:, 2].astype(np.int64)
    
    if clip_x_arr is None or clip_y_arr is None or clip_z_arr is None:
        clip_x = _select_first_available_field(
            data,
            ["clip_counter[0]", "clip_counter_0", "clip_counter_x"],
        )
        clip_y = _select_first_available_field(
            data,
            ["clip_counter[1]", "clip_counter_1", "clip_counter_y"],
        )
        clip_z = _select_first_available_field(
            data,
            ["clip_counter[2]", "clip_counter_2", "clip_counter_z"],
        )
        
        if clip_x is None or clip_y is None or clip_z is None:
            return None
        
        clip_x_arr = np.asarray(clip_x, dtype=np.int64)
        clip_y_arr = np.asarray(clip_y, dtype=np.int64)
        clip_z_arr = np.asarray(clip_z, dtype=np.int64)
    
    ts_arr = _extract_timestamp_array(data)
    
    metrics = _compute_clip_metrics_from_arrays(ts_arr, clip_x_arr, clip_y_arr, clip_z_arr)
    metrics["_device_id"] = device_id
    metrics["_source"] = "clip_counter"
    metrics["_multi_id"] = multi_id
    
    log_fn = logger.info if metrics["accel_clipping_events"] > 0 else logger.debug
    log_fn(
        "sensor_accel[%s] device_id=%s clip_counter -> events=%d time=%.3f s",
        multi_id if multi_id is not None else "?",
        device_id if device_id is not None else "?",
        int(metrics["accel_clipping_events"]),
        metrics["accel_clipping_time_s"],
    )
    
    return metrics


def _compute_clip_metrics_from_arrays(
    ts_arr: Optional[np.ndarray],
    clip_x_arr: np.ndarray,
    clip_y_arr: np.ndarray,
    clip_z_arr: np.ndarray,
) -> Dict[str, float]:
    """Helper to compute clipping totals and time from clip counter arrays."""
    results: Dict[str, float] = {
        "accel_clipping_time_s": 0.0,
        "accel_clipping_events": 0.0,
    }
    
    if clip_x_arr.size == 0 or clip_y_arr.size == 0 or clip_z_arr.size == 0:
        return results
    
    sizes = [clip_x_arr.size, clip_y_arr.size, clip_z_arr.size]
    if ts_arr is not None:
        sizes.append(ts_arr.size)
    min_len = min(sizes)
    if min_len == 0:
        return results
    
    clip_x_trim = clip_x_arr[:min_len]
    clip_y_trim = clip_y_arr[:min_len]
    clip_z_trim = clip_z_arr[:min_len]
    
    results["accel_clipping_events"] = float(
        clip_x_trim.sum() + clip_y_trim.sum() + clip_z_trim.sum()
    )
    
    if ts_arr is not None and min_len >= 2:
        ts_trim = ts_arr[:min_len]
        dt = np.diff(ts_trim) / 1e6  # Convert microseconds to seconds
        clip_mask = (clip_x_trim > 0) | (clip_y_trim > 0) | (clip_z_trim > 0)
        clip_mask_dt = clip_mask[:-1]
        if clip_mask_dt.size == dt.size:
            results["accel_clipping_time_s"] = float(dt[clip_mask_dt].sum())
    
    return results


def _compute_clipping_retroactive_from_sensor_data(
    data: Dict[str, object],
    device_id: Optional[int],
    multi_id: Optional[int],
) -> Optional[Dict[str, float]]:
    """Fallback: compute clipping retroactively from scaled acceleration values."""
    ts_arr = _extract_timestamp_array(data)
    if ts_arr is None:
        return None
    
    x = data.get("x")
    y = data.get("y")
    z = data.get("z")
    if x is None or y is None or z is None:
        return None
    
    x_arr = np.asarray(x, dtype=np.float64)
    y_arr = np.asarray(y, dtype=np.float64)
    z_arr = np.asarray(z, dtype=np.float64)
    
    if min(x_arr.size, y_arr.size, z_arr.size, ts_arr.size) < 2:
        return None
    
    metrics = _compute_clipping_retroactive(ts_arr, x_arr, y_arr, z_arr)
    metrics["_device_id"] = device_id
    metrics["_source"] = "retroactive"
    metrics["_multi_id"] = multi_id
    
    log_fn = logger.info if metrics["accel_clipping_events"] > 0 else logger.debug
    log_fn(
        "sensor_accel[%s] device_id=%s retroactive -> events=%d time=%.3f s",
        multi_id if multi_id is not None else "?",
        device_id if device_id is not None else "?",
        int(metrics["accel_clipping_events"]),
        metrics["accel_clipping_time_s"],
    )
    
    return metrics


def _select_first_available_field(data: Dict[str, object], keys: list[str]) -> Optional[object]:
    """Return the first non-None value for the provided keys."""
    for key in keys:
        value = data.get(key)
        if value is not None:
            return value
    return None


def _extract_timestamp_array(data: Dict[str, object]) -> Optional[np.ndarray]:
    """Extract timestamp array (preferring timestamp_sample)."""
    ts = data.get("timestamp_sample")
    if ts is None:
        ts = data.get("timestamp")
    if ts is None:
        return None
    ts_arr = np.asarray(ts, dtype=np.float64)
    if ts_arr.size == 0:
        return None
    return ts_arr


def _extract_device_id(data: Dict[str, object]) -> Optional[int]:
    """Extract a scalar device_id from a sensor dataset."""
    device_id = data.get("device_id")
    if device_id is None:
        return None
    
    try:
        if isinstance(device_id, (list, tuple, np.ndarray)):
            if len(device_id) == 0:
                return None
            return int(device_id[0])
        return int(device_id)
    except (TypeError, ValueError):
        return None


def _compute_clipping_from_clip_counter(ulog: ULog) -> Optional[Dict[str, float]]:
    """Compute clipping from clip_counter (legacy wrapper)."""
    accel_msgs = [m for m in ulog.data_list if m.name == "sensor_accel"]
    return _compute_clipping_across_sensors(accel_msgs)


def _compute_clipping_from_vehicle_imu_status(ulog: ULog) -> Optional[Dict[str, float]]:
    """
    Compute clipping metrics from vehicle_imu_status topic (Method 1b - Alternative).
    
    Uses accel_clipping[0/1/2] from vehicle_imu_status which contains cumulative
    total clipping counts per axis accumulated over the flight.
    
    Returns the final accumulated clipping counts (last value in each array).
    """
    results: Dict[str, float] = {
        "accel_clipping_time_s": 0.0,
        "accel_clipping_events": 0.0,
    }
    
    try:
        imu_status_msgs = [m for m in ulog.data_list if m.name == "vehicle_imu_status"]
        if not imu_status_msgs:
            return None
        
        # Get the first (or most complete) vehicle_imu_status message
        imu_status = imu_status_msgs[0]
        data = imu_status.data
        
        # Check for accel_clipping fields
        clip_x_key = "accel_clipping[0]"
        clip_y_key = "accel_clipping[1]"
        clip_z_key = "accel_clipping[2]"
        
        clip_x = data.get(clip_x_key)
        if clip_x is None:
            clip_x = data.get("accel_clipping_0")
        
        clip_y = data.get(clip_y_key)
        if clip_y is None:
            clip_y = data.get("accel_clipping_1")
        
        clip_z = data.get(clip_z_key)
        if clip_z is None:
            clip_z = data.get("accel_clipping_2")
        
        if clip_x is None or clip_y is None or clip_z is None:
            return None
        
        # Convert to numpy arrays
        clip_x_arr = np.asarray(clip_x, dtype=np.int64)
        clip_y_arr = np.asarray(clip_y, dtype=np.int64)
        clip_z_arr = np.asarray(clip_z, dtype=np.int64)
        
        # Get the last value (final accumulated count) from each array
        # These are cumulative totals, so the last value is the total for the flight
        if clip_x_arr.size > 0 and clip_y_arr.size > 0 and clip_z_arr.size > 0:
            total_clip_x = float(clip_x_arr[-1])
            total_clip_y = float(clip_y_arr[-1])
            total_clip_z = float(clip_z_arr[-1])
            total_clip_events = total_clip_x + total_clip_y + total_clip_z
            
            logger.info("Computed clipping from vehicle_imu_status: X=%d, Y=%d, Z=%d, Total=%d samples",
                       int(total_clip_x), int(total_clip_y), int(total_clip_z), int(total_clip_events))
            
            results["accel_clipping_events"] = total_clip_events
            
            # For time calculation, we'd need timestamps, but vehicle_imu_status
            # only gives us totals, so we can't compute clipping_time_s accurately
            # Leave it at 0.0 or try to estimate from sensor_accel if available
        
        return results
    
    except Exception as e:
        logger.debug("Error computing clipping from vehicle_imu_status: %s", e)
        return None




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


def _compute_clipping_retroactive(
    ts: np.ndarray, x: np.ndarray, y: np.ndarray, z: np.ndarray,
    sensor_range_g: float = 16.0
) -> Dict[str, float]:
    """
    Detect accelerometer clipping retroactively from scaled acceleration values (Method 2 - Fallback).
    
    Used when clip_counter fields are not available in sensor_accel topic.
    Detects clipping by checking if values exceed sensor range limits.
    
    Args:
        ts: Timestamps (microseconds)
        x, y, z: Scaled acceleration values (m/s²)
        sensor_range_g: Sensor range in g (default 16g for common sensors)
    
    Returns:
        Dictionary with accel_clipping_time_s and accel_clipping_events
    """
    results: Dict[str, float] = {
        "accel_clipping_time_s": 0.0,
        "accel_clipping_events": 0.0,
    }
    
    if ts.size < 2:
        return results
    
    # Calculate clip limit based on sensor range
    # Using 99.9% threshold as per specification
    sensor_range_mps2 = sensor_range_g * CONSTANTS_ONE_G
    clip_limit_mps2 = sensor_range_mps2 * CLIP_THRESHOLD_PERCENT
    
    # Detect clipping: any axis exceeds clip limit
    abs_x = np.abs(x)
    abs_y = np.abs(y)
    abs_z = np.abs(z)
    
    # Direct threshold clipping (any axis >= clip limit)
    clip_mask_threshold = (
        (abs_x >= clip_limit_mps2) |
        (abs_y >= clip_limit_mps2) |
        (abs_z >= clip_limit_mps2)
    )
    
    # Additional saturation detection: values pinned near maximum with low variance
    # This helps catch cases where values are consistently at the limit
    if ts.size >= 10:  # Need enough samples for variance calculation
        window_size = min(10, ts.size // 10)  # Small rolling window
        if window_size >= 3:
            # Compute rolling variance for each axis
            x_var = _rolling_variance(abs_x, window_size)
            y_var = _rolling_variance(abs_y, window_size)
            z_var = _rolling_variance(abs_z, window_size)
            
            # Saturation: high magnitude AND low variance (pinned value)
            saturation_mask = (
                ((abs_x > clip_limit_mps2 * 0.9) & (x_var < ACCEL_CLIP_TOLERANCE)) |
                ((abs_y > clip_limit_mps2 * 0.9) & (y_var < ACCEL_CLIP_TOLERANCE)) |
                ((abs_z > clip_limit_mps2 * 0.9) & (z_var < ACCEL_CLIP_TOLERANCE))
            )
        else:
            saturation_mask = np.zeros_like(abs_x, dtype=bool)
    else:
        saturation_mask = np.zeros_like(abs_x, dtype=bool)
    
    # Combined clipping mask
    clip_mask = clip_mask_threshold | saturation_mask
    
    if clip_mask.any():
        # Compute time spent clipping
        dt = np.diff(ts) / 1e6  # Convert microseconds to seconds
        clip_mask_dt = clip_mask[:-1]  # Align with dt
        if clip_mask_dt.size == dt.size:
            results["accel_clipping_time_s"] = float(dt[clip_mask_dt].sum())
        
        # Count clipping samples: total number of samples where clipping occurs
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



