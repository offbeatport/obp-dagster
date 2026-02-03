# burningdemand_dagster/partitions.py
from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)

source_partitions = StaticPartitionsDefinition(["github"])
# source_partitions = StaticPartitionsDefinition(
#     ["github", "stackoverflow", "reddit", "hackernews"]
# )
daily_partitions = DailyPartitionsDefinition(start_date="2026-01-20")
source_day_partitions = MultiPartitionsDefinition(
    {"source": source_partitions, "date": daily_partitions}
)
