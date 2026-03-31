from pyspark.sql import SparkSession, DataFrame
from pathlib import Path


def create_spark_session() -> SparkSession:
    return SparkSession.builder.master("local[*]").appName("audition").getOrCreate()

def load_csv(spark: SparkSession, file_path: Path) -> DataFrame:
    return spark.read.csv(
        file_path.as_posix(),
        header = True
    )
