import os
from dagster import (
                    asset, MaterializeResult,
                    Definitions,
                    asset_check, AssetCheckResult,
                    define_asset_job, ScheduleDefinition,
                    sensor, RunRequest, SkipReason
                    )

file_path = "incoming_data.csv"
# Step 1: Create assets from the python functions.
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

#Step 2: Add Definitions so that Dagster can differentiate between the assets and load them properly.
# Pushing Definitions at the end of the code to compile all the parts for dagster pipeline.

#Part 3: Add Asset Checks. They are used to check the quality and validity of the data.
@asset_check(asset=total_revenue)
def check_min_revenue(total_revenue):
    min_revenue = total_revenue >= 500

    return AssetCheckResult(
        passed = min_revenue,
        metadata = {
            "threshold": 500,
            "value": total_revenue
        }
    )

# Part 4.a.: Add Schedule. Used to run a data pipeline at a specific time or interval using a cron expression.
#1st you need to create a job.
daily_sales_job = define_asset_job(
    name = "daily_sales_job",
    selection = ["raw_sales", "total_revenue"]
)
#2nd you need to create a schedule for the job using cron expression.
daily_sales_schedule = ScheduleDefinition(
    job = daily_sales_job,
    cron_schedule = "* * * * *",
    execution_timezone = "America/Chicago"
)

# Part 4.b.: Add Sensor. Used to trigger a data pipeline based on an event or condition like adding a new file or data object.
#Define the job you want to run when the Sensor is triggered.
@sensor(job = daily_sales_job)
def new_file_sensor():
#Check if the new file exists to trigger the sensor.
    if os.path.exists(file_path):
        yield RunRequest(run_key = f"file_{os.path.getmtime(file_path)}")
    else:
        yield SkipReason(f"Waiting for {file_path} to arrive.")

############################
defs = Definitions(
    assets = [raw_sales, total_revenue],
    asset_checks = [check_min_revenue],
    schedules = [daily_sales_schedule],
    sensors = [new_file_sensor]
)
