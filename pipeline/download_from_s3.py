from __future__ import annotations

"""
Download logic for pulling PX4 .ulg files from S3 to a local folder.
Uses boto3 and a paginator to traverse all keys under a given prefix.
"""

from pathlib import Path
from typing import Iterable, List, Optional
import re

import boto3

from utils.logging_utils import get_logger


logger = get_logger(__name__)


def iter_s3_objects(bucket: str, prefix: str) -> Iterable[str]:
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []) or []:
            key = obj.get("Key", "")
            if key and not key.endswith("/"):
                yield key


def _vehicle_digits(vehicle: str) -> Optional[str]:
    m = re.search(r"(?i)el[-_]?(\d{3})", vehicle or "")
    return m.group(1) if m else None


def _key_matches_vehicle(key: str, vehicles: Optional[List[str]]) -> bool:
    if not vehicles:
        return True
    for vehicle in vehicles:
        digits = _vehicle_digits(vehicle)
        if digits and re.search(rf"(?i)el[-_]?{digits}", key):
            return True
    return False


def download_ulog_folder(bucket: str, prefix: str, local_root: Path, include_vehicles: Optional[List[str]] = None) -> List[Path]:
    """Download all .ulg files under prefix to local_root, preserving structure.

    Returns the list of local paths downloaded (or already present).
    """
    s3 = boto3.client("s3")
    local_root.mkdir(parents=True, exist_ok=True)

    downloaded: List[Path] = []
    for key in iter_s3_objects(bucket, prefix):
        if not key.lower().endswith(".ulg"):
            continue
        if not _key_matches_vehicle(key, include_vehicles):
            continue
        dst = local_root / key
        dst.parent.mkdir(parents=True, exist_ok=True)
        if not dst.exists():
            logger.info("Downloading s3://%s/%s -> %s", bucket, key, dst)
            s3.download_file(bucket, key, str(dst))
        else:
            logger.debug("Skipping existing %s", dst)
        downloaded.append(dst)
    return downloaded


