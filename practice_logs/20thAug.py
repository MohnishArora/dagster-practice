from dagster import asset, MaterializeResult, asset_check, AssetCheckResult, Definitions

@asset
def raw_sales():
    data = [100, 250, 75, 400]
    return MaterializeResult(
        value = data,
        metadata = {
            "Total_Records" : len(data),
            "Max_Sales" : max(data)
        }
    )

@asset
def total_revenue(raw_sales):
    revenue = sum(raw_sales)
    return MaterializeResult(
        value = revenue,
        metadata = {
            "Total_Revenue" : revenue,
            "Order_Count" : len(raw_sales)
        }
    )

# Add a check: total revenue should be more than 500

@asset_check(asset=total_revenue)
def check_min_revenue(total_revenue):
    min_revenue = total_revenue >= 500
    return AssetCheckResult(
        passed = min_revenue,
        metadata = {"Threshold" : 500, "Value" : total_revenue}
    )

# Adding a check does nothing on refresh in Dagster UI. Thuse we define Definitions to bundle our assets and checks together for Dagster
defs = Definitions(
    assets = [raw_sales, total_revenue],
    asset_checks = [check_min_revenue]
)
