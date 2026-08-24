import os
from dagster import (asset, MaterializeResult,
                     asset_check, AssetCheckResult,
                     Definitions,
                     define_asset_job, ScheduleDefinition,
                    sensor, RunRequest, SkipReason
                     )

FILE_PATH = "incoming_data.csv"
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
                    "max_sales": max(data)}
    )


@asset
def total_revenue(raw_sales):
    revenue = sum(raw_sales)
    return MaterializeResult(
        value = revenue,
        metadata = {"total_revenue": revenue,
                    "Order_count": len(raw_sales)}
    )

# Asset Check Creation
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


# Schedule Creation
daily_sales_job = define_asset_job(
    name = "daily_sales_job",
    selection = ["raw_sales", "total_revenue"]
)

daily_sales_schedule = ScheduleDefinition(
    job = daily_sales_job,
    cron_schedule = "* * * * *",
    execution_timezone = "America/Chicago"
)

# Sensor Creation
@sensor(job=daily_sales_job)
def new_file_sensor():    
    if os.path.exists(FILE_PATH):
        last_modified = os.path.getmtime(FILE_PATH)
        yield RunRequest(
            run_key=f"incoming_data_{last_modified}",
            message=f"Detected updated {FILE_PATH}"
        )
    else:
        yield SkipReason(f"Waiting for {FILE_PATH} to arrive.")


#Definitions Creation
defs = Definitions(
    assets = [raw_sales, total_revenue],
    asset_checks = [check_min_revenue],
    schedules = [daily_sales_schedule],
    sensors = [new_file_sensor]
)