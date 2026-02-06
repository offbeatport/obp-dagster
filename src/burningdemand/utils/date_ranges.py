from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import List, Tuple


def split_day_into_ranges(date: str, splits: int) -> List[Tuple[str, str]]:
    splits = max(1, int(splits))

    total_seconds = 24 * 60 * 60
    base = total_seconds // splits
    remainder = total_seconds % splits

    midnight_utc = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    created_ranges: List[Tuple[str, str]] = []
    offset = 0

    for i in range(splits):
        window_seconds = base + (1 if i < remainder else 0)
        start_dt = midnight_utc + timedelta(seconds=offset)
        end_dt = midnight_utc + timedelta(seconds=offset + window_seconds - 1)
        created_ranges.append(
            (
                start_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
            )
        )
        offset += window_seconds

    return created_ranges
