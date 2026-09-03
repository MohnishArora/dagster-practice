from dagster import (
    AssetCheckExecutionContext,
    AssetCheckResult,
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Definitions,
    MaterializeResult,
    asset,
    asset_check,
    build_schedule_from_partitioned_job,
    define_asset_job,
)

# 1. Define the daily timeline
daily_partitions = DailyPartitionsDefinition(
    start_date="2026-08-25",
    timezone="America/Chicago",
    hour_offset=2,  # Runs at 02:00 AM after partition closes
)


# 2. Pipeline Assets
@asset(partitions_def=daily_partitions)
def raw_daily_orders(context: AssetExecutionContext):
  partition_date = context.partition_key
  context.log.info(f"Extracting raw orders for: {partition_date}")

  mock_orders = [
      {"order_id": 101, "amount": 120.0, "status": "COMPLETED"},
      {"order_id": 102, "amount": 80.5, "status": "COMPLETED"},
      {"order_id": 103, "amount": 0.0, "status": "REFUNDED"},
  ]
  return MaterializeResult(value=mock_orders)


@asset(partitions_def=daily_partitions)
def clean_daily_orders(
    context: AssetExecutionContext, raw_daily_orders: list[dict]
):
  partition_date = context.partition_key
  valid_orders = [
      o
      for o in raw_daily_orders
      if o["status"] == "COMPLETED" and o["amount"] > 0
  ]
  context.log.info(
      f"Cleaned orders for {partition_date}: retained {len(valid_orders)}"
  )
  return MaterializeResult(value=valid_orders)


## Creating the 3rd Asset
@asset(partitions_def = daily_partitions)
def daily_revenue_summary(context: AssetExecutionContext, clean_daily_orders: list[dict]):
    partition_date = context.partition_key
    total_revenue = sum(o["amount"] for o in clean_daily_orders)
    context.log.info(f"Computed Revenue for {partition_date}: ${total_revenue:.2f}")

    return MaterializeResult(
       value = total_revenue,
       metadata = {
          "partition": partition_date,
          "total_revenue": total_revenue,
          "total_orders": len(clean_daily_orders)
       }
    )

# 3. Slice-bound Asset Check
@asset_check(asset=clean_daily_orders, partitions_def=daily_partitions)
def check_non_empty_orders(
    context: AssetCheckExecutionContext, clean_daily_orders: list[dict]
):
  has_records = len(clean_daily_orders) > 0
  return AssetCheckResult(
      passed=has_records,
      metadata={
          "partition": context.partition_key,
          "valid_count": len(clean_daily_orders),
      },
  )

@asset_check(asset=daily_revenue_summary, partitions_def = daily_partitions)
def check_min_revenue(context: AssetCheckExecutionContext, daily_revenue_summary: float):
  meets_threshold = daily_revenue_summary >= 100.0

  return AssetCheckResult(
      passed=meets_threshold,
      metadata={
          "partition": context.partition_key,
          "observed_revenue": daily_revenue_summary,
          "threshold": 100.0
      },
  )

# 4. Job & Schedule Construction
daily_pipeline_job = define_asset_job(
    name="daily_orders_pipeline_job",
    selection=[raw_daily_orders, clean_daily_orders, daily_revenue_summary],
    partitions_def=daily_partitions,
)

daily_orders_schedule = build_schedule_from_partitioned_job(
    job=daily_pipeline_job,
)

defs = Definitions(
    assets=[raw_daily_orders, clean_daily_orders, daily_revenue_summary],
    asset_checks=[check_non_empty_orders, check_min_revenue],
    jobs=[daily_pipeline_job],
    schedules=[daily_orders_schedule],
)