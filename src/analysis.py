from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def highest_revenue_color_per_year(orders: DataFrame, products: DataFrame) -> DataFrame:
    analysis_df = orders.join(
        products.select("ProductID", "Color", "ProductCategoryName"),
        on="ProductID",
        how="left",
    )
    w = Window.partitionBy(F.col("Year")).orderBy(F.col("Revenue").desc())

    return (
        analysis_df.groupBy(F.trunc(F.col("OrderDate"), "year").alias("Year"), "Color")
        .agg(
            F.sum(
                F.when(F.col("TotalLineExtendedPrice").isNull(), F.lit(0)).otherwise(F.col("TotalLineExtendedPrice"))
            ).alias("Revenue")
        )
        .withColumns({"MaxYearlyRevenueInd": F.row_number().over(w) == F.lit(1)})
        .filter(F.col("MaxYearlyRevenueInd"))
        .orderBy(F.col("Year"))
        .drop("MaxYearlyRevenueInd")
    )


def average_lead_time_per_product_category(orders: DataFrame, products: DataFrame) -> DataFrame:
    analysis_df = orders.join(
        products.select("ProductID", "Color", "ProductCategoryName"),
        on="ProductID",
        how="left",
    )

    return analysis_df.groupBy(F.col("ProductCategoryName")).agg(
        F.avg(F.col("LeadTimeInBusinessDays")).alias("AvgLeadTimeInBusinessDays")
    )
