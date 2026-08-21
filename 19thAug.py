from dagster import MaterializeResult, asset, asset_check, AssetCheckResult, Definitions

@asset
def raw_sales():
    data = [100, 250, 75, 400]

    return MaterializeResult(
        value = data,
        metadata = {
            "total_records": len(data),
            "max_sale": max(data),
        }
    )

@asset
def total_revenue(raw_sales):
    revenue = sum(raw_sales)

    return MaterializeResult(
        value = revenue,
        metadata = {
            "total_revenue": revenue,
            "order_count": len(raw_sales),
        }
    )

##############################################################################

@asset_check(asset=total_revenue)
def check_min_revenue(total_revenue):
    min_revenue = total_revenue >= 500

    return AssetCheckResult(
        passed=min_revenue,
        metadata={"threshold": 500, "value": total_revenue}
    )

''' On running the above code, we saw that the asset check was not included in the asset box UI.
For this we will add Definitaions below to ensure the code reads where to connect the asset checker.'''

# Bundle our assets and checks together for Dagster
defs = Definitions(
    assets=[raw_sales, total_revenue],
    asset_checks=[check_min_revenue]
)