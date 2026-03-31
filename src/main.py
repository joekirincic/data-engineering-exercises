from pathlib import Path
from ingest import create_spark_session, load_csv
from standardize import cast_columns, create_publish_product, create_publish_orders
from analysis import highest_revenue_color_per_year, average_lead_time_per_product_category

if __name__ == "__main__":
    spark = create_spark_session()
    data_dir = Path.cwd().joinpath('data')

    # Data ingestion
    raw_product = load_csv(spark, data_dir.joinpath('products.csv'))
    raw_sales_order_detail = load_csv(spark, data_dir.joinpath('sales_order_detail.csv'))
    raw_sales_order_header = load_csv(spark, data_dir.joinpath('sales_order_header.csv'))

    # Data standardization
    store_product = cast_columns(raw_product)
    store_sales_order_detail = cast_columns(raw_sales_order_detail)
    store_sales_order_header = cast_columns(raw_sales_order_header)
    publish_product = create_publish_product(store_product)
    publish_orders = create_publish_orders(store_sales_order_detail, store_sales_order_header)

    # Analysis questions
    highest_revenue_color_per_year(publish_orders, publish_product).show(truncate=False)
    average_lead_time_per_product_category(publish_orders, publish_product).show(truncate=False)

    spark.stop()
