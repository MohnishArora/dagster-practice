from dagster import (
    asset, MaterializeResult,
    AssetExecutionContext, StaticPartitionsDefinition,
    Definitions
)

# Define the categorical slices
region_partitions = StaticPartitionsDefinition(["US", "EU", "APAC"])

# Attach the partions to the asset
@asset(partitions_def=region_partitions)
def regional_sales(context: AssetExecutionContext):
    selected_region = context.partition_key
    context.log.info(f"Processing sales data for region: {selected_region}")

    #Mock sales data per region
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