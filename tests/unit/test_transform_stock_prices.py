"""
Unit tests para o Spark job de transformacao.
Roda com SparkSession local - sem precisar do cluster Docker.
"""
import pytest
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, DateType,
)
import sys
sys.path.insert(0, "spark/jobs")


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .appName("test")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )


@pytest.fixture
def sample_df(spark):
    data = [
        ("AAPL", date(2024, 1, 15), 184.0, 186.5, 182.5, 185.2, 55_000_000),
        ("AAPL", date(2024, 1, 16), 185.0, 188.0, 184.0, 187.5, 60_000_000),
        ("MSFT", date(2024, 1, 15), 375.0, 378.0, 373.0, 376.5, 25_000_000),
    ]
    schema = StructType([
        StructField("symbol",      StringType(), False),
        StructField("trade_date",  DateType(),   False),
        StructField("open_price",  DoubleType(), True),
        StructField("high_price",  DoubleType(), True),
        StructField("low_price",   DoubleType(), True),
        StructField("close_price", DoubleType(), True),
        StructField("volume",      LongType(),   True),
    ])
    return spark.createDataFrame(data, schema=schema)


def test_price_range_calculated(spark, sample_df):
    from transform_stock_prices import transform
    result = transform(sample_df)
    row = result.filter("symbol = 'AAPL' AND trade_date = '2024-01-15'").first()
    assert row["price_range"] == pytest.approx(186.5 - 182.5, rel=1e-4)


def test_dq_flag_ok_for_valid_data(spark, sample_df):
    from transform_stock_prices import transform
    result = transform(sample_df)
    flags = [r["dq_flag"] for r in result.select("dq_flag").collect()]
    assert all(f == "ok" for f in flags)


def test_dq_flag_zero_volume(spark):
    from transform_stock_prices import transform
    data = [("TSLA", date(2024, 1, 15), 200.0, 205.0, 198.0, 202.0, 0)]
    df = spark.createDataFrame(
        data,
        ["symbol", "trade_date", "open_price", "high_price",
         "low_price", "close_price", "volume"],
    )
    assert transform(df).first()["dq_flag"] == "zero_volume"


def test_daily_return_first_row_is_null(spark, sample_df):
    from transform_stock_prices import transform
    row = transform(sample_df).filter(
        "symbol = 'AAPL' AND trade_date = '2024-01-15'"
    ).first()
    assert row["daily_return_pct"] is None


def test_metadata_columns_added(spark, sample_df):
    from transform_stock_prices import transform
    result = transform(sample_df)
    assert "ingested_at" in result.columns
    assert result.first()["source_system"] == "alpha_vantage"
