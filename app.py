from dagster import asset, MaterializeResult

'''@asset
def raw_sales():
    return[100, 250, 75, 400]

@asset
def total_revenue(raw_sales):
    return sum(raw_sales)'''

@asset
def raw_sales():
    data = [100, 250, 75, 400]

    return MaterializeResult(
        value = data,
        metadata = {
            "total_records": len(data),
            "max_sale": max(data),
        })

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