from __future__ import annotations

"""
Aggregate per-log summaries by vehicle.
"""

import math
import re
from pathlib import Path
import pandas as pd

from utils.logging_utils import get_logger


logger = get_logger(__name__)


def aggregate_summaries_by_vehicle(summaries_csv: Path, out_csv: Path) -> None:
    if not summaries_csv.exists():
        logger.warning("No summaries found at %s", summaries_csv)
        return
    df = pd.read_csv(summaries_csv)
    if "vehicle_id" not in df.columns:
        logger.warning("summaries.csv missing vehicle_id column: %s", summaries_csv)
        df.to_csv(out_csv, index=False)
        return

    agg_dict = {"file": "count"}
    time_columns = [
        col
        for col in df.columns
        if (
            col in {"accel_time_lt_30_s", "accel_time_30_50_s", "accel_time_50_70_s", "accel_time_gt_70_s", "accel_total_time_s"}
            or col.startswith("motor") and col.endswith("_s")
        )
    ]
    for col in time_columns:
        agg_dict[col] = "sum"
    
    # Fatigue metrics: sum counts/events
    fatigue_columns = [
        col
        for col in df.columns
        if col in {
            "peak_accel_events",
            "accel_clipping_time_s",
            "accel_clipping_events",
        }
    ]
    for col in fatigue_columns:
        agg_dict[col] = "sum"  # Counts/events: sum across logs

    grouped = df.groupby("vehicle_id").agg(agg_dict).reset_index().rename(columns={"file": "num_logs"})

    if "vehicle_id" in grouped.columns:
        grouped = _sort_by_vehicle_id(grouped)

    # Compute aggregated percentages if time columns exist
    if "accel_total_time_s" in grouped.columns:
        tot = grouped["accel_total_time_s"].replace(0, pd.NA)
        for col, out in [
            ("accel_time_lt_30_s", "accel_pct_lt_30"),
            ("accel_time_30_50_s", "accel_pct_30_50"),
            ("accel_time_50_70_s", "accel_pct_50_70"),
            ("accel_time_gt_70_s", "accel_pct_gt_70"),
        ]:
            if col in grouped.columns:
                grouped[out] = (grouped[col] / tot).fillna(0.0)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    grouped.to_csv(out_csv, index=False)


def _sort_by_vehicle_id(grouped: pd.DataFrame) -> pd.DataFrame:
    def extract_number(value: object) -> float:
        if not isinstance(value, str):
            return math.inf
        match = re.search(r"(\d+)", value)
        if match:
            return float(match.group(1))
        return math.inf

    temp = grouped.copy()
    temp["_vehicle_num"] = temp["vehicle_id"].map(extract_number)
    temp["_vehicle_id_lower"] = temp["vehicle_id"].astype(str).str.lower()
    temp = temp.sort_values(by=["_vehicle_num", "_vehicle_id_lower", "vehicle_id"])
    return temp.drop(columns=["_vehicle_num", "_vehicle_id_lower"])


