import os
from dagster import (
    asset, MaterializeResult,
    asset_check, AssetCheckResult,
    define_asset_job, ScheduleDefinition, 
    sensor, RunRequest, SkipReason,
    Definitions
)

file_path = "incoming_data.csv"

@asset
def raw_sales():
    if os.path.exists(file_path):
        with open(file_path) as f:
            lines = f.read().strip().split(",")
            data = [int(x.strip()) for x in lines if x.strip()]
    else:
        data = [100, 250, 75, 400]

    return MaterializeResult(
        value = data,
        metadata = {"total_records": len(data), 
                    "max_sale": max(data)}
    )

@asset
def total_revenue(raw_sales):
    revenue = sum(raw_sales)

    return MaterializeResult(
        value = revenue,
        metadata = {"total_revenue": revenue,
                    "order_count": len(raw_sales)}
    )

@asset_check(asset = total_revenue)
def check_min_revenue(total_revenue):
    min_revenue = total_revenue >= 500

    return AssetCheckResult(
        passed = min_revenue,
        metadata = {
            "threshold": 500,
            "value": total_revenue
        }
    )


# Creating a Job
daily_sales_job = define_asset_job(
    name = "daily_sales_job",
    selection = ["raw_sales", "total_revenue"]
)

# Creating a Schedule on the job
daily_sales_schedule = ScheduleDefinition(
    job = daily_sales_job,
    cron_schedule = "* * * * *",
    execution_timezone = "America/Chicago"
)

# Adding sensor
@sensor(job = daily_sales_job)
def new_file_sensor():
    if os.path.exists(file_path):
        yield RunRequest(run_key = f"file_{os.path.getmtime(file_path)}")
    else:
        yield SkipReason(f"Waiting for {file_path} to arrive.")

defs = Definitions(
    assets = [raw_sales, total_revenue],
    asset_checks = [check_min_revenue],
    schedules = [daily_sales_schedule],
    sensors = [new_file_sensor]
)