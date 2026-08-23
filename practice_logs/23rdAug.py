from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
    Definitions,
    MaterializeResult,
    asset
)

# 1. Define the partition scheme (e.g., daily starting from Aug 1, 2026)
daily_partitions = DailyPartitionsDefinition(start_date="2026-08-01")

# 2. Attach partitions_def to the asset
@asset(partitions_def=daily_partitions)
def raw_sales(context: AssetExecutionContext):
    # Retrieve which specific partition date is executing (e.g. "2026-08-20")
    partition_date = context.partition_key
    
    # Mock data keyed by date
    sales_by_date = {
        "2026-08-20": [100, 250, 75],
        "2026-08-21": [300, 400],
        "2026-08-22": [150, 50, 200, 80],
    }
    
    # Process only the records for this specific partition
    data = sales_by_date.get(partition_date, [50, 100])

    return MaterializeResult(
        value=data,
        metadata={
            "partition_date": partition_date,
            "record_count": len(data),
            "daily_total": sum(data)
        }
    )

@asset(partitions_def=daily_partitions)
def total_revenue(context: AssetExecutionContext, raw_sales):
    partition_date = context.partition_key
    revenue = sum(raw_sales)
    
    return MaterializeResult(
        value=revenue,
        metadata={
            "partition_date": partition_date,
            "total_revenue": revenue
        }
    )

defs = Definitions(
    assets=[raw_sales, total_revenue]
)