from dagster import(
    asset, MaterializeResult,
    AssetExecutionContext, StaticPartitionsDefinition,
    asset_check, AssetCheckResult, AssetCheckExecutionContext,
    Definitions
)

region_partitions = StaticPartitionsDefinition(["US", "EU", "APAC"])

@asset(partitions_def = region_partitions)
def regional_sales(context: AssetExecutionContext):
    selected_region = context.partition_key
    context.log.info(f"Processing sales data for the region: {selected_region}")

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
            "total_records": len(records),
            "total_revenue": sum(records)
        }
    )

@asset(partitions_def = region_partitions)
def regional_summary(context: AssetExecutionContext, regional_sales):
    avg_regional_sales = sum(regional_sales)/len(regional_sales)

    return MaterializeResult(
        value = avg_regional_sales,
        metadata = {
            "region": context.partition_key,
            "avg_sales": avg_regional_sales,
            "total_sales": sum(regional_sales),
            "total_records": len(regional_sales)
        }
    )

@asset_check(asset=regional_summary, partitions_def=region_partitions)
def check_positive_avg(context: AssetCheckExecutionContext, regional_summary: float):          #Here i did not specify the type of the parameter regional_summary as dict[str, float] because it
    positive_avg = regional_summary > 0                                                 #was giving me an error. I will check with the AI if this is a bug or not.

    return AssetCheckResult(
        passed = positive_avg,
        metadata = {
                    "region": context.partition_key,
                    "avg_sales": regional_summary
        }
    )


defs = Definitions(
    assets = [regional_sales, regional_summary],
    asset_checks = [check_positive_avg]
)