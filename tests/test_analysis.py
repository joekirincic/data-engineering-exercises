from datetime import date

from analysis import highest_revenue_color_per_year, average_lead_time_per_product_category


def test_highest_revenue_color_per_year_returns_one_per_year(spark):
    orders = spark.createDataFrame(
        [
            (1, 1, date(2024, 3, 1), 100.0, 5),
            (2, 1, date(2024, 6, 1), 200.0, 3),
            (3, 2, date(2025, 1, 1), 50.0, 4),
        ],
        ["SalesOrderID", "ProductID", "OrderDate", "TotalLineExtendedPrice", "LeadTimeInBusinessDays"],
    )
    products = spark.createDataFrame(
        [
            (1, "Red", "Bikes"),
            (2, "Blue", "Clothing"),
        ],
        ["ProductID", "Color", "ProductCategoryName"],
    )
    result = highest_revenue_color_per_year(orders, products).collect()

    years = [row["Year"] for row in result]
    assert len(result) == 2
    assert date(2024, 1, 1) in years
    assert date(2025, 1, 1) in years


def test_highest_revenue_color_picks_top_color(spark):
    orders = spark.createDataFrame(
        [
            (1, 1, date(2024, 1, 1), 100.0, 5),
            (2, 2, date(2024, 6, 1), 999.0, 3),
        ],
        ["SalesOrderID", "ProductID", "OrderDate", "TotalLineExtendedPrice", "LeadTimeInBusinessDays"],
    )
    products = spark.createDataFrame(
        [
            (1, "Red", "Bikes"),
            (2, "Blue", "Bikes"),
        ],
        ["ProductID", "Color", "ProductCategoryName"],
    )
    result = highest_revenue_color_per_year(orders, products).collect()

    assert len(result) == 1
    assert result[0]["Color"] == "Blue"


def test_average_lead_time_per_product_category(spark):
    orders = spark.createDataFrame(
        [
            (1, 1, 10),
            (2, 2, 20),
            (3, 2, 30),
        ],
        ["SalesOrderID", "ProductID", "LeadTimeInBusinessDays"],
    )
    products = spark.createDataFrame(
        [
            (1, "Red", "Bikes"),
            (2, "Blue", "Clothing"),
        ],
        ["ProductID", "Color", "ProductCategoryName"],
    )
    result = average_lead_time_per_product_category(orders, products).collect()
    by_category = {row["ProductCategoryName"]: float(row["AvgLeadTimeInBusinessDays"]) for row in result}

    assert by_category["Bikes"] == 10.0
    assert by_category["Clothing"] == 25.0
