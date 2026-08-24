from dagster import (asset, 
                    MaterializeResult,
                    AssetExecutionContext, 
                    DailyPartitionsDefinition,
                    define_asset_job, build_schedule_from_partitioned_job,
                    Definitions
                    )

# Define the partition
daily_partitions = DailyPartitionsDefinition(
    start_date = "2026-08-01",
    timezone = "America/Chicago",
    hour_of_day =2,                 # Runs at 2:00 AM
    minute_of_hour =0)

# Define the asset, attach partitions to asset and use the context.
@asset(partitions_def=daily_partitions)
def raw_sales(context: AssetExecutionContext):
    current_slice = context.partition_key
    data = [100, 200]
    return MaterializeResult(
        value = data,
        metadata = {
            "partition": current_slice,
            "total": sum(data)
            }
    )

# Schedule creation based on Partitions
## 1st define partition-aware job
sales_partition_job = define_asset_job(
    name = "sales_partition_job",
    selection = ["raw_sales"],
    partitions_def = daily_partitions
)

##2nd you automate the partioned job with a schedule
daily_schedule = build_schedule_from_partitioned_job(
    job = sales_partition_job,
)

# Define Definitions
defs = Definitions (
    assets = [raw_sales],
    schedules = [daily_schedule]
)