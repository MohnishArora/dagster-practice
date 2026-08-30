from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Definitions,
    MaterializeResult,
    asset,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

# A. DEFINE THE BLUEPRINT
daily_partitions = DailyPartitionsDefinition(
    start_date="2026-08-01",
    timezone = "America/Chicago",
    hour_offset = 2,
    minute_offset = 0)


# B. ATTACH TO ASSETS & USE THE CONTEXT
@asset(partitions_def=daily_partitions)
def raw_sales(context: AssetExecutionContext):
    current_slice = context.partition_key
    data = [100, 200]
    return MaterializeResult(
        value=data, metadata={"partition": current_slice, "total": sum(data)}
    )


# C. DEFINE A PARTITION-AWARE JOB
sales_partition_job = define_asset_job(
    name="sales_partition_job",
    selection=["raw_sales"],
    partitions_def=daily_partitions
)

# D. AUTOMATE WITH A SCHEDULE
daily_schedule = build_schedule_from_partitioned_job(
    job=sales_partition_job
)

# E. REGISTER
defs = Definitions(
    assets=[raw_sales], 
    schedules=[daily_schedule])