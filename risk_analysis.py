#!/usr/bin/env python3
from __future__ import annotations

"""
Analyze vehicle risk based on accelerometer vibrations and motor output stress.

Identifies vehicles with:
- High vibration exposure (>70 m/s², 50-70 m/s²)
- High motor output stress (saturation at 1.0, high output at >=0.9)
"""

import argparse
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from utils.logging_utils import get_logger


logger = get_logger(__name__)


def calculate_risk_score(row: pd.Series) -> Tuple[float, Dict[str, float]]:
    """
    Calculate a composite risk score for a vehicle.
    
    Returns:
        (total_score, breakdown_dict)
    """
    breakdown = {}
    
    # Vibration risk (accelerometer stress)
    # High risk: >70 m/s²
    # Medium risk: 50-70 m/s²
    accel_gt_70 = float(row.get("accel_time_gt_70_s", 0.0) or 0.0)
    accel_50_70 = float(row.get("accel_time_50_70_s", 0.0) or 0.0)
    total_accel_time = float(row.get("accel_total_time_s", 0.0) or 0.0)
    
    if total_accel_time > 0:
        # High vibration risk: percentage of time >70 m/s²
        vibration_high_pct = accel_gt_70 / total_accel_time
        # Medium vibration risk: percentage of time 50-70 m/s²
        vibration_med_pct = accel_50_70 / total_accel_time
        
        # Weight high vibration more heavily
        vibration_score = (vibration_high_pct * 10.0) + (vibration_med_pct * 3.0)
        breakdown["vibration_score"] = vibration_score
        breakdown["vibration_high_pct"] = vibration_high_pct * 100
        breakdown["vibration_med_pct"] = vibration_med_pct * 100
    else:
        breakdown["vibration_score"] = 0.0
        breakdown["vibration_high_pct"] = 0.0
        breakdown["vibration_med_pct"] = 0.0
    
    # Motor stress risk
    # High risk: saturation (>=1.0)
    # Medium risk: high output (>=0.9)
    motor_saturation_total = 0.0
    motor_high_output_total = 0.0
    
    for motor_idx in range(4):  # Motors 0-3
        saturation_key = f"motor{motor_idx}_time_above_1_0_s"
        high_output_key = f"motor{motor_idx}_time_above_0_9_s"
        
        saturation_time = float(row.get(saturation_key, 0.0) or 0.0)
        high_output_time = float(row.get(high_output_key, 0.0) or 0.0)
        
        motor_saturation_total += saturation_time
        motor_high_output_total += high_output_time
    
    # Normalize motor stress by total flight time
    if total_accel_time > 0:
        motor_saturation_pct = motor_saturation_total / total_accel_time
        motor_high_output_pct = motor_high_output_total / total_accel_time
        
        # Weight saturation more heavily
        motor_score = (motor_saturation_pct * 15.0) + (motor_high_output_pct * 5.0)
        breakdown["motor_score"] = motor_score
        breakdown["motor_saturation_pct"] = motor_saturation_pct * 100
        breakdown["motor_high_output_pct"] = motor_high_output_pct * 100
    else:
        breakdown["motor_score"] = 0.0
        breakdown["motor_saturation_pct"] = 0.0
        breakdown["motor_high_output_pct"] = 0.0
    
    # Fatigue risk (component stress indicators)
    peak_events = float(row.get("peak_accel_events", 0.0) or 0.0)
    clipping_events = float(row.get("accel_clipping_events", 0.0) or 0.0)
    
    # Normalize fatigue metrics by flight time
    if total_accel_time > 0:
        # Peak events per hour of flight
        peak_rate = (peak_events / total_accel_time) * 3600.0
        # Clipping samples per hour
        clipping_rate = (clipping_events / total_accel_time) * 3600.0
        
        # Normalize clipping rate to a 0-1 scale (assuming max reasonable rate ~10000 samples/hr)
        # This prevents clipping from dominating the score
        max_clipping_rate = 10000.0  # Reasonable upper bound for clipping samples/hour
        normalized_clipping = min(clipping_rate / max_clipping_rate, 1.0)
        
        # Normalize peak rate similarly (assuming max ~1000 events/hr)
        max_peak_rate = 1000.0
        normalized_peak = min(peak_rate / max_peak_rate, 1.0)
        
        # Raw fatigue score (0-1 scale)
        raw_fatigue = (normalized_peak * 0.3) + (normalized_clipping * 0.7)  # Clipping weighted more
        
        breakdown["peak_events_per_hour"] = peak_rate
        breakdown["clipping_events_per_hour"] = clipping_rate
    else:
        raw_fatigue = 0.0
        breakdown["peak_events_per_hour"] = 0.0
        breakdown["clipping_events_per_hour"] = 0.0
    
    # Normalize vibration and motor scores to 0-1 scale for fair comparison
    # Vibration score typically ranges 0-13 (100% time >70m/s² = 10, 100% time 50-70 = 3)
    max_vibration = 13.0
    normalized_vibration = min(breakdown["vibration_score"] / max_vibration, 1.0)
    
    # Motor score typically ranges 0-20 (100% saturation = 15, 100% high output = 5)
    max_motor = 20.0
    normalized_motor = min(breakdown["motor_score"] / max_motor, 1.0)
    
    # Apply target weighting: Fatigue 60%, Motor 20%, Vibration 20%
    # Scale to 0-100 range for readability
    fatigue_score = raw_fatigue * 60.0
    motor_score_scaled = normalized_motor * 20.0
    vibration_score_scaled = normalized_vibration * 20.0
    
    breakdown["fatigue_score"] = fatigue_score
    breakdown["motor_score"] = motor_score_scaled
    breakdown["vibration_score"] = vibration_score_scaled
    
    total_score = vibration_score_scaled + motor_score_scaled + fatigue_score
    breakdown["total_score"] = total_score
    
    return total_score, breakdown


def analyze_risk(aggregated_csv: Path, top_n: int | None = None) -> pd.DataFrame:
    """Analyze vehicle risk and return ranked results.
    
    Args:
        aggregated_csv: Path to aggregated vehicle statistics CSV
        top_n: Optional limit on number of vehicles to return (None = all vehicles)
    """
    if not aggregated_csv.exists():
        raise FileNotFoundError(f"Aggregated CSV not found: {aggregated_csv}")
    
    df = pd.read_csv(aggregated_csv)
    
    if df.empty:
        logger.warning("No vehicle data found in %s", aggregated_csv)
        return pd.DataFrame()
    
    # Calculate risk scores
    risk_data = []
    for _, row in df.iterrows():
        vehicle_id = row.get("vehicle_id", "unknown")
        score, breakdown = calculate_risk_score(row)
        
        risk_data.append({
            "vehicle_id": vehicle_id,
            "risk_score": score,
            "vibration_score": breakdown["vibration_score"],
            "motor_score": breakdown["motor_score"],
            "fatigue_score": breakdown.get("fatigue_score", 0.0),
            "vibration_high_pct": breakdown["vibration_high_pct"],
            "vibration_med_pct": breakdown["vibration_med_pct"],
            "motor_saturation_pct": breakdown["motor_saturation_pct"],
            "motor_high_output_pct": breakdown["motor_high_output_pct"],
            "peak_events_per_hour": breakdown.get("peak_events_per_hour", 0.0),
            "clipping_events_per_hour": breakdown.get("clipping_events_per_hour", 0.0),
            # Raw fatigue metric values
            "peak_accel_events": float(row.get("peak_accel_events", 0.0) or 0.0),
            "accel_clipping_events": float(row.get("accel_clipping_events", 0.0) or 0.0),
            "total_flight_time_min": float(row.get("accel_total_time_s", 0.0) or 0.0) / 60.0,
            "num_logs": int(row.get("num_logs", 0) or 0),
        })
    
    risk_df = pd.DataFrame(risk_data)
    risk_df = risk_df.sort_values("risk_score", ascending=False)
    
    # Apply top_n limit if specified
    if top_n is not None and top_n > 0:
        risk_df = risk_df.head(top_n)
    
    return risk_df


def print_risk_report(risk_df: pd.DataFrame, output_file: Path | None = None) -> None:
    """Print or save a formatted risk report."""
    if risk_df.empty:
        print("No vehicles found for risk analysis.")
        return
    
    lines = ["# Vehicle Risk Analysis Report", ""]
    lines.append("Vehicles ranked by composite risk score (vibration + motor stress).")
    lines.append("")
    
    lines.append("## Vehicle Risk Rankings")
    lines.append("")
    lines.append("| Rank | Vehicle | Risk Score | Vib | Motor | Fatigue | High Vib % | Sat % | Peak Events | Clipping Events | Flight Time (min) | Logs |")
    lines.append("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")
    
    for idx, (_, row) in enumerate(risk_df.iterrows(), 1):
        lines.append(
            f"| {idx} | {row['vehicle_id']} | "
            f"{row['risk_score']:.2f} | "
            f"{row['vibration_score']:.2f} | "
            f"{row['motor_score']:.2f} | "
            f"{row.get('fatigue_score', 0.0):.2f} | "
            f"{row['vibration_high_pct']:.1f}% | "
            f"{row['motor_saturation_pct']:.1f}% | "
            f"{int(row.get('peak_accel_events', 0.0))} | "
            f"{int(row.get('accel_clipping_events', 0.0))} | "
            f"{row['total_flight_time_min']:.1f} | "
            f"{row['num_logs']} |"
        )
    
    report_text = "\n".join(lines)
    
    if output_file:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(report_text)
        logger.info("Risk report saved to %s", output_file)
    else:
        print(report_text)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Analyze vehicle risk based on vibrations and motor stress."
    )
    parser.add_argument(
        "--aggregated_csv",
        type=Path,
        default=Path("output/aggregated_by_vehicle.csv"),
        help="Path to aggregated vehicle statistics CSV",
    )
    parser.add_argument(
        "--report_md",
        type=Path,
        default=None,
        help="Optional: Path to markdown report (will use aggregated CSV if not provided)",
    )
    parser.add_argument(
        "--top_n",
        type=int,
        default=None,
        help="Optional: Limit number of vehicles to report (default: all vehicles)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Optional: Save risk report to this file (default: print to stdout)",
    )
    
    args = parser.parse_args()
    
    # Use aggregated CSV (more reliable than parsing markdown)
    csv_path = args.aggregated_csv
    if args.report_md and not csv_path.exists():
        logger.warning("Aggregated CSV not found, attempting to parse markdown...")
        # Could add markdown parsing here if needed, but CSV is preferred
        csv_path = args.aggregated_csv
    
    try:
        risk_df = analyze_risk(csv_path, top_n=args.top_n)
        print_risk_report(risk_df, output_file=args.output)
    except Exception as exc:
        logger.exception("Failed to analyze risk: %s", exc)
        raise


if __name__ == "__main__":
    main()

