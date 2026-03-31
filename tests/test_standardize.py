from standardize import cast_columns, create_publish_product, create_publish_orders
from pyspark.sql import functions as F


def test_cast_columns_casts_matching_columns(spark):
    df = spark.createDataFrame([("1", "100.50")], ["ProductID", "ListPrice"])
    result = cast_columns(df)

    assert result.schema["ProductID"].dataType.simpleString() == "int"
    assert result.schema["ListPrice"].dataType.simpleString() == "decimal(10,2)"


def test_cast_columns_ignores_unknown_columns(spark):
    df = spark.createDataFrame([("hello",)], ["UnknownCol"])
    result = cast_columns(df)

    assert result.schema["UnknownCol"].dataType.simpleString() == "string"


def test_publish_product_fills_null_color(spark):
    df = spark.createDataFrame(
        [(1, None, "Bikes", "Mountain Bikes")],
        "ProductID int, Color string, ProductCategoryName string, ProductSubCategoryName string",
    )
    result = create_publish_product(df).collect()

    assert result[0]["Color"] == "N/A"


def test_publish_product_fills_empty_color(spark):
    df = spark.createDataFrame(
        [(1, "", "Bikes", "Mountain Bikes")],
        ["ProductID", "Color", "ProductCategoryName", "ProductSubCategoryName"],
    )
    result = create_publish_product(df).collect()

    assert result[0]["Color"] == "N/A"


def test_publish_product_preserves_existing_color(spark):
    df = spark.createDataFrame(
        [(1, "Red", "Bikes", "Mountain Bikes")],
        ["ProductID", "Color", "ProductCategoryName", "ProductSubCategoryName"],
    )
    result = create_publish_product(df).collect()

    assert result[0]["Color"] == "Red"


def test_publish_product_assigns_clothing_category(spark):
    df = spark.createDataFrame(
        [(1, "Black", "", "Gloves")],
        ["ProductID", "Color", "ProductCategoryName", "ProductSubCategoryName"],
    )
    result = create_publish_product(df).collect()

    assert result[0]["ProductCategoryName"] == "Clothing"


def test_publish_product_assigns_accessories_category(spark):
    df = spark.createDataFrame(
        [(1, "Black", "", "Helmets")],
        ["ProductID", "Color", "ProductCategoryName", "ProductSubCategoryName"],
    )
    result = create_publish_product(df).collect()

    assert result[0]["ProductCategoryName"] == "Accessories"


def test_publish_product_assigns_components_category_frames(spark):
    df = spark.createDataFrame(
        [(1, "Black", "", "Road Frames")],
        ["ProductID", "Color", "ProductCategoryName", "ProductSubCategoryName"],
    )
    result = create_publish_product(df).collect()

    assert result[0]["ProductCategoryName"] == "Components"


def test_publish_product_assigns_components_category_wheels(spark):
    df = spark.createDataFrame(
        [(1, "Black", "", "Wheels")],
        ["ProductID", "Color", "ProductCategoryName", "ProductSubCategoryName"],
    )
    result = create_publish_product(df).collect()

    assert result[0]["ProductCategoryName"] == "Components"


def test_publish_product_preserves_existing_category(spark):
    df = spark.createDataFrame(
        [(1, "Black", "Bikes", "Gloves")],
        ["ProductID", "Color", "ProductCategoryName", "ProductSubCategoryName"],
    )
    result = create_publish_product(df).collect()

    assert result[0]["ProductCategoryName"] == "Bikes"


def test_publish_orders_calculates_fields(spark):
    from datetime import date

    details = spark.createDataFrame(
        [(1, 10, 100, 2, 50.00, 5.00)],
        ["SalesOrderID", "SalesOrderDetailID", "ProductID", "OrderQty", "UnitPrice", "UnitPriceDiscount"],
    )
    header = spark.createDataFrame(
        [(1, date(2024, 1, 1), date(2024, 1, 8), True, "ACC-001", 100, 200, 10.00)],
        ["SalesOrderID", "OrderDate", "ShipDate", "OnlineOrderFlag", "AccountNumber", "CustomerID", "SalesPersonID", "Freight"],
    )
    result = create_publish_orders(details, header).collect()
    row = result[0]

    # Jan 1 (Mon) to Jan 8 (Mon) = 5 business days
    assert row["LeadTimeInBusinessDays"] == 5
    assert float(row["TotalLineExtendedPrice"]) == 2 * (50.00 - 5.00)
    assert float(row["TotalOrderFreight"]) == 10.00
    assert "Freight" not in result[0].asDict()
    assert "SalesOrderID" not in result[0].asDict()
