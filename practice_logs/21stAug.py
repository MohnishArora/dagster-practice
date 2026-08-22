# Feature: Adding a Schedule for a job 
import os
from dagster import ( 
                    asset, MaterializeResult, 
                    asset_check, AssetCheckResult, 
                    Definitions, 
                    define_asset_job, ScheduleDefinition,
                    RunRequest, SkipReason, sensor
                    )

FILE_PATH = "incoming_data.csv"

#Step 1: Create Assets 
@asset
def raw_sales():
    if os.path.exists(FILE_PATH):
        with open(FILE_PATH, "r") as f:
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


# Step2: Add check: total revenue should be more than 500
@asset_check(asset=total_revenue)
def check_min_revenue(total_revenue):
    
    min_revenue = total_revenue >= 500

    return AssetCheckResult(
        passed = min_revenue,
        metadata = {"threshold": 500, 
                    "value": total_revenue}
    )

# Step 3: Add Definitions. Bundle assets and checks together for Dagster to differentiate. 
# Pushed after Sensor



# Step 4.a.: Add a Schedule.
daily_sales_job = define_asset_job(
    name = "daily_sales_job",
    selection = ["raw_sales", "total_revenue"]
)

daily_sales_schedule = ScheduleDefinition(
    job = daily_sales_job,
    cron_schedule = "* * * * *",
    execution_timezone = "America/Chicago"
)

# Step 4.b.: Add a Sensor.
@sensor(job=daily_sales_job)
def new_file_sensor():
    file_path = "incoming_data.csv"
    
    if os.path.exists(file_path):
        yield RunRequest(run_key=None)
    else:
        yield SkipReason(f"Waiting for {file_path} to arrive.")


defs = Definitions(
    assets = [raw_sales, total_revenue],
    asset_checks = [check_min_revenue],
    schedules = [daily_sales_schedule],
    sensors = [new_file_sensor]
)