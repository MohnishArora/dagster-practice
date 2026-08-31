# Module 5.1 Static Partitions and Slice Checks

from dagster import (
    asset, MaterializeResult,
    StaticPartitionsDefinition, AssetExecutionContext,
    asset_check, AssetCheckResult, AssetCheckExecutionContext,
    Definitions
)

region_partitions = StaticPartitionsDefinition(["US", "EU", "APAC"])

@asset(partitions_def = region_partitions)
def regional_sales(context: AssetExecutionContext):
    selected_region = context.partition_key
    context.log.info(f"Processing sales data for region: {selected_region}")
    sales_data = {
        "US": [500, 700, 300],
        "EU": [200, 450, 150],
        "APAC": [100, 50, 80]
        }
    records = sales_data.get(selected_region, [])

    return MaterializeResult(
        value = records,
        metadata = {
            "region": selected_region,
            "record_count": len(records),
            "total_revenue": sum(records)
        }
    )

# CONNECTING UPSTREAM TO DWONSTREAM (regional_sales -> regional_summary)
@asset(partitions_def = region_partitions)
def regional_summary(context: AssetExecutionContext, regional_sales: list[int]):
    avg_regional_sale = sum(regional_sales)/len(regional_sales)

    return MaterializeResult(
        value = avg_regional_sale,
        metadata = {
            "region": context.partition_key,
            "avg_sale": avg_regional_sale
        }
    )

#Adding Asset check to the partitions
@asset_check(asset=regional_summary)
def check_positive_avg(context: AssetCheckExecutionContext, regional_summary: dict[str, float]):
    all_positive = all(avg > 0 for avg in regional_summary.values())

    return AssetCheckResult(
        passed = all_positive,
        metadata = {
            "all_regional_averages": str(regional_summary),
            "num_regions_checked": len(regional_summary)
        }
    )

defs = Definitions(
    assets = [regional_sales, regional_summary],
    asset_checks = [check_positive_avg]
)