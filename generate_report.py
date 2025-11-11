from __future__ import annotations

"""
Generate a simple Markdown report from the aggregated CSV.
"""

import math
from pathlib import Path
import re
from typing import Dict
import pandas as pd

from utils.logging_utils import get_logger


logger = get_logger(__name__)


def generate_final_report(aggregated_csv: Path, report_path: Path) -> None:
    if not aggregated_csv.exists():
        logger.warning("Aggregated CSV not found: %s", aggregated_csv)
        return

    df = pd.read_csv(aggregated_csv)
    df = _sort_by_vehicle_id(df)
    lines = ["# PX4 ULog Report", ""]

    if df.empty:
        lines.append("_No data available._")
    else:
        total_logs = int(df["num_logs"].sum())
        lines.extend([f"- Total vehicles: {df.shape[0]}", f"- Total logs processed: {total_logs}", ""])

        lines.append("## Vehicles")
        lines.append("")

        vehicle_ids = [str(v) for v in df["vehicle_id"].tolist()]
        lines.append("- Vehicles in report: " + ", ".join(vehicle_ids))
        lines.append("")

        for _, row in df.iterrows():
            vid = row.get("vehicle_id", "unknown")
            num_logs = int(row.get("num_logs", 0))
            lines.append(f"### Vehicle {vid}")
            lines.append(f"- Logs processed: {num_logs}")
            motor_stats: Dict[int, Dict[float, float]] = {}
            threshold_values = set()
            motor_pattern = re.compile(r"motor(\d+)_time_above_(.+)_s")
            for col in row.index:
                if not isinstance(col, str):
                    continue
                match = motor_pattern.fullmatch(col)
                if not match:
                    continue
                motor_idx = int(match.group(1))
                threshold_label = match.group(2).replace("_", ".")
                try:
                    threshold_value = float(threshold_label)
                except ValueError:
                    continue
                try:
                    value = float(row[col])
                except (TypeError, ValueError):
                    value = 0.0
                if pd.isna(value):
                    value = 0.0
                motor_stats.setdefault(motor_idx, {})[threshold_value] = value
                threshold_values.add(threshold_value)

            if motor_stats:
                try:
                    sorted_thresholds = sorted(threshold_values)
                except TypeError:
                    sorted_thresholds = sorted(threshold_values, key=lambda x: str(x))
                header = ["Motor"] + [f">= {thr:g} of max output (min)" for thr in sorted_thresholds]
                align = ["---"] + ["---:" for _ in sorted_thresholds]
                lines.append("| " + " | ".join(header) + " |")
                lines.append("| " + " | ".join(align) + " |")
                for motor_idx in sorted(motor_stats):
                    values = motor_stats[motor_idx]
                    row_entries = [f"Motor {motor_idx}"]
                    for threshold in sorted_thresholds:
                        duration_s = float(values.get(threshold, 0.0))
                        row_entries.append(f"{duration_s / 60.0:.1f}")
                    lines.append("| " + " | ".join(row_entries) + " |")

                # Additional table summarizing motor totals per threshold.
                lines.append("")
                lines.append("| Threshold | Total time (min) |")
                lines.append("| --- | ---: |")
                for threshold in sorted_thresholds:
                    total_duration = sum(values.get(threshold, 0.0) for values in motor_stats.values())
                    lines.append(f"| >= {threshold:g} | {total_duration / 60.0:.1f} |")
            else:
                lines.append("_No motor output data available._")

            lines.append("")

            # Accelerometer stress distribution
            time_map = [
                ("< 30 m/s²", "accel_time_lt_30_s", "accel_pct_lt_30"),
                ("30–50 m/s²", "accel_time_30_50_s", "accel_pct_30_50"),
                ("50–70 m/s²", "accel_time_50_70_s", "accel_pct_50_70"),
                ("> 70 m/s²", "accel_time_gt_70_s", "accel_pct_gt_70"),
            ]
            total_time = float(row.get("accel_total_time_s", 0.0) or 0.0)
            if total_time > 0:
                lines.append("| Accel bin | Time (min) | Share |")
                lines.append("| --- | ---: | ---: |")
                for label, col_time, col_pct in time_map:
                    time_s = float(row.get(col_time, 0.0) or 0.0)
                    share = float(row.get(col_pct, 0.0) or 0.0)
                    lines.append(f"| {label} | {time_s / 60.0:.1f} | {share * 100:.1f}% |")
                lines.append(f"| Total tracked | {total_time / 60.0:.1f} | 100.0% |")
            else:
                lines.append("_No accelerometer data available._")

            lines.append("")

            # Fatigue metrics
            fatigue_metrics = {
                "peak_accel_events": ("Peak accel events (>100 m/s²)", "count"),
                "accel_clipping_time_s": ("Accel clipping time", "s"),
                "accel_clipping_events": ("Accel clipping events", "count"),
            }
            
            has_fatigue_data = False
            fatigue_rows = []
            for key, (label, unit) in fatigue_metrics.items():
                value = row.get(key)
                if value is not None and not pd.isna(value):
                    has_fatigue_data = True
                    val = float(value)
                    if unit == "min":
                        display_val = f"{val / 60.0:.1f} {unit}"
                    elif unit == "count":
                        display_val = f"{int(val)} {unit}"
                    elif unit == "s":
                        display_val = f"{val:.2f} {unit}"
                    else:
                        display_val = f"{val:.1f} {unit}"
                    fatigue_rows.append((label, display_val))
            
            if has_fatigue_data:
                lines.append("| Fatigue Metric | Value |")
                lines.append("| --- | ---: |")
                for label, display_val in fatigue_rows:
                    lines.append(f"| {label} | {display_val} |")
                lines.append("")

    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(lines))
    logger.info("Wrote report to %s", report_path)


def _sort_by_vehicle_id(df: pd.DataFrame) -> pd.DataFrame:
    if "vehicle_id" not in df.columns:
        return df

    def extract_number(value: object) -> float:
        if not isinstance(value, str):
            return math.inf
        match = re.search(r"(\d+)", value)
        if match:
            return float(match.group(1))
        return math.inf

    temp = df.copy()
    temp["_vehicle_num"] = temp["vehicle_id"].map(extract_number)
    temp["_vehicle_id_lower"] = temp["vehicle_id"].astype(str).str.lower()
    temp = temp.sort_values(by=["_vehicle_num", "_vehicle_id_lower", "vehicle_id"])
    return temp.drop(columns=["_vehicle_num", "_vehicle_id_lower"])


