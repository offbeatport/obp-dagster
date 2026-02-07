# burningdemand_dagster/partitions.py
from dagster import (
    DailyPartitionsDefinition,
)


daily_partitions = DailyPartitionsDefinition(start_date="2026-01-20")
