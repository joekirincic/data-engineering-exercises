from pyspark.sql import DataFrame
from pyspark.sql import functions as F


INTEGER_COLS = ["ProductID", "SafetyStockLevel", "ReorderPoint", "SalesOrderID", "SalesOrderDetailID", "OrderQty", "CustomerID", "SalesPersonID"]
BOOLEAN_COLS = ["MakeFlag", "OnlineOrderFlag"]
DECIMAL_COLS = ["StandardCost", "ListPrice", "Weight", "UnitPrice", "UnitPriceDiscount", "Freight"]
DATE_COLS = ["OrderDate", "ShipDate"]

ALL_CASTS: dict[str, str] = {
    **{c: "int" for c in INTEGER_COLS},
    **{c: "boolean" for c in BOOLEAN_COLS},
    **{c: "decimal(10,2)" for c in DECIMAL_COLS},
    **{c: "date" for c in DATE_COLS},
}


def cast_columns(df: DataFrame) -> DataFrame:
    casts = {k: F.col(k).cast(v) for k, v in ALL_CASTS.items() if k in df.columns}
    return df.withColumns(casts)


def create_publish_product(df: DataFrame) -> DataFrame:
    missing_category_ind = (
        F.col("ProductCategoryName").isNull()
        | (F.col("ProductCategoryName") == "")
        | (F.col("ProductCategoryName") == "NULL")
    )
    clothing_ind = F.col("ProductSubCategoryName").isin(["Gloves", "Shorts", "Socks", "Tights", "Vests"])
    accessories_ind = F.col("ProductSubCategoryName").isin(["Locks", "Lights", "Headsets", "Helmets", "Pedals", "Pumps"])
    components_ind = F.col("ProductSubCategoryName").isin(["Wheels", "Saddles"]) | F.col("ProductSubCategoryName").rlike("(?i)frames")

    return df.withColumns(
        {
            "Color": F.when(F.col("Color").isNull() | (F.col("Color") == ""), "N/A").otherwise(F.col("Color")),
            "ProductCategoryName": F.when(missing_category_ind & clothing_ind, "Clothing")
            .when(missing_category_ind & accessories_ind, "Accessories")
            .when(missing_category_ind & components_ind, "Components")
            .otherwise(F.col("ProductCategoryName")),
        }
    )


def create_publish_orders(details: DataFrame, header: DataFrame) -> DataFrame:
    lead_time_in_business_days = (
        F.size(
            F.filter(
                F.sequence(F.col("OrderDate"), F.col("ShipDate")),
                lambda x: F.dayofweek(x).isin([2, 3, 4, 5, 6]),
            )
        )
        - 1
    )
    total_line_extended_price = F.col("OrderQty") * (F.col("UnitPrice") - F.col("UnitPriceDiscount"))

    return (
        details.join(header, on="SalesOrderID", how="left")
        .withColumns(
            {
                "LeadTimeInBusinessDays": lead_time_in_business_days,
                "TotalLineExtendedPrice": total_line_extended_price,
                "TotalOrderFreight": F.col("Freight"),
            }
        )
        .drop("Freight", "SalesOrderID")
    )
