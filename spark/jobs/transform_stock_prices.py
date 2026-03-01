"""
Stock Prices Spark Transformation
Bronze JSON -> Silver Parquet com metricas calculadas
"""
import argparse
import logging
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql.window import Window

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
log = logging.getLogger(__name__)


def create_spark_session(app_name: str = "StockPricesTransform") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )


def read_bronze(spark: SparkSession, s3_path: str):
    log.info("Reading bronze data from: %s", s3_path)
    raw = spark.read.option("multiLine", "true").json(s3_path)
    return raw.select(
        F.col("\Meta Data\.\2. Symbol\").alias("symbol"),
        F.explode(F.col("\Time Series (Daily)\")).alias("trade_date", "ohlcv"),
    ).select(
        F.col("symbol"),
        F.to_date(F.col("trade_date")).alias("trade_date"),
        F.col("ohlcv.\1. open\").cast(DoubleType()).alias("open_price"),
        F.col("ohlcv.\2. high\").cast(DoubleType()).alias("high_price"),
        F.col("ohlcv.\3. low\").cast(DoubleType()).alias("low_price"),
        F.col("ohlcv.\4. close\").cast(DoubleType()).alias("close_price"),
        F.col("ohlcv.\5. volume\").cast(LongType()).alias("volume"),
    )


def transform(df):
    window = Window.partitionBy("symbol").orderBy("trade_date")
    window_20 = window.rowsBetween(-19, 0)
    window_5 = window.rowsBetween(-4, 0)

    return (
        df
        .withColumn("price_range", F.col("high_price") - F.col("low_price"))
        .withColumn(
            "daily_return_pct",
            F.round(
                (F.col("close_price") - F.lag("close_price", 1).over(window))
                / F.lag("close_price", 1).over(window) * 100,
                4,
            ),
        )
        .withColumn("ma_5d", F.round(F.avg("close_price").over(window_5), 4))
        .withColumn("ma_20d", F.round(F.avg("close_price").over(window_20), 4))
        .withColumn(
            "dq_flag",
            F.when(F.col("volume") == 0, "zero_volume")
            .when(F.col("close_price") <= 0, "invalid_price")
            .when(F.col("high_price") < F.col("low_price"), "inverted_range")
            .otherwise("ok"),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_system", F.lit("alpha_vantage"))
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True)
    args = parser.parse_args()

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze_path = f"s3a://de-portfolio-raw/stock_prices/date={args.date}/*/*.json"
        df = read_bronze(spark, bronze_path)
        result = transform(df)

        silver_path = f"s3a://de-portfolio-processed/stock_prices/date={args.date}"
        result.write.mode("overwrite").parquet(silver_path)
        log.info("Written to Silver: %s (rows: %d)", silver_path, result.count())

    except Exception as e:
        log.error("Job failed: %s", e, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
