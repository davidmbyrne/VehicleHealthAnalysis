from __future__ import annotations

"""
Shared helper utilities for pipeline runners.
"""

import re
from dataclasses import replace
from pathlib import Path
from typing import Iterable, List, Optional, Union

from process_ulog import ProcessedULog


def resolve_vehicle_filter(vehicles: Union[None, str, Iterable[str]]) -> Optional[List[str]]:
    """Parse vehicle filter from CLI string, list, or prompt user."""
    parsed = _normalize_vehicle_list(vehicles)
    if parsed:
        return parsed
    response = input(
        "Enter vehicle IDs to include (comma-separated, leave blank for all vehicles): "
    ).strip()
    if not response:
        return None
    parsed = _normalize_vehicle_list(response)
    return parsed if parsed else None


def _normalize_vehicle_list(vehicles: Union[None, str, Iterable[str]]) -> Optional[List[str]]:
    """Normalize vehicle input to a list of strings, handling comma/space separation."""
    if vehicles is None:
        return None
    if isinstance(vehicles, str):
        tokens = [tok.strip() for tok in re.split(r"[\s,]+", vehicles) if tok.strip()]
        return tokens or None
    # Iterable[str]
    collected = [tok.strip() for tok in vehicles if isinstance(tok, str) and tok.strip()]
    return collected or None


def key_matches_vehicle(key: str, vehicles: Optional[List[str]]) -> bool:
    """Check if an S3 key matches any vehicle in the filter list."""
    if not vehicles:
        return True
    key_lower = key.lower()
    for vehicle in vehicles:
        digits = _vehicle_digits(vehicle)
        if digits and re.search(rf"(?i)el[-_]?{digits}\b", key_lower):
            return True
        if vehicle.lower() in key_lower:
            return True
    return False


def update_processed_metadata(processed: ProcessedULog, key: str) -> ProcessedULog:
    """Update ProcessedULog with vehicle ID inferred from S3 key."""
    vehicle_id = processed.vehicle_id or infer_vehicle_from_key(key)
    try:
        return replace(processed, source_path=Path(key), vehicle_id=vehicle_id)
    except Exception:  # pragma: no cover - fallback path
        processed.source_path = Path(key)  # type: ignore[attr-defined]
        if vehicle_id:
            processed.vehicle_id = vehicle_id  # type: ignore[attr-defined]
        return processed


def infer_vehicle_from_key(key: str) -> Optional[str]:
    """Extract vehicle ID from S3 key path."""
    match = re.search(r"(?i)(el[-_]?(\d+))", key or "")
    if not match:
        return None
    raw = match.group(1).upper()
    return raw.replace("_", "-")


def _vehicle_digits(vehicle: str) -> Optional[str]:
    """Extract numeric digits from vehicle ID string."""
    match = re.search(r"(?i)el[-_]?(\d+)", vehicle or "")
    return match.group(1) if match else None

