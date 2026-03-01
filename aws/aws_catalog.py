"""
aws/aws_catalog.py
Registers Gold layer tables in AWS Glue Data Catalog
Run: python aws/aws_catalog.py
Free tier: up to 1M metadata objects at no cost
"""
import os, boto3, logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("glue_catalog")

REGION     = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
S3_BUCKET  = os.getenv("S3_BUCKET_CURATED",  "projectalphav")
S3_PREFIX  = os.getenv("S3_PREFIX_CURATED",   "portfolio-curated")
RUN_DATE   = os.getenv("PIPELINE_DATE",        "2026-03-01")
DATABASE   = "equity_research"

TABLES = {
    "equity_ranking": {
        "description": "All equities ranked by composite Quality Score with sector P/E comparison",
        "columns": [
            ("ticker",            "string",  "Stock ticker symbol"),
            ("sector",            "string",  "GICS sector"),
            ("equity_style",      "string",  "Growth / Value / Blend classification"),
            ("quality_score",     "double",  "Composite quality score 0-100"),
            ("trailing_pe",       "double",  "Trailing Price-to-Earnings ratio"),
            ("price_to_book",     "double",  "Price-to-Book ratio"),
            ("roe_pct",           "double",  "Return on Equity percentage"),
            ("net_margin_pct",    "double",  "Net profit margin percentage"),
            ("pe_vs_sector_avg",  "double",  "Ticker P/E vs sector average P/E"),
            ("pe_vs_sector_label","string",  "Cheap / Fair / Expensive vs sector"),
            ("overall_rank",      "int",     "Final rank by quality score"),
            ("market_cap_b",      "double",  "Market capitalization in billions USD"),
        ],
    },
    "sector_valuation": {
        "description": "Aggregated valuation metrics by GICS sector",
        "columns": [
            ("sector",             "string", "GICS sector name"),
            ("ticker_count",       "int",    "Number of tickers in sector"),
            ("avg_pe",             "double", "Average trailing P/E ratio"),
            ("avg_quality_score",  "double", "Average composite quality score"),
            ("avg_rev_growth",     "double", "Average revenue growth percentage"),
            ("avg_roe",            "double", "Average return on equity percentage"),
            ("total_market_cap",   "double", "Total market cap in billions USD"),
        ],
    },
    "performance_annual": {
        "description": "Annual return per ticker per year (2020-2025)",
        "columns": [
            ("ticker",            "string", "Stock ticker symbol"),
            ("year",              "int",    "Calendar year"),
            ("annual_return_pct", "double", "Annual price return percentage"),
            ("sector",            "string", "GICS sector"),
            ("equity_style",      "string", "Growth / Value / Blend"),
            ("avg_volume",        "double", "Average daily trading volume"),
        ],
    },
    "daily_snapshot": {
        "description": "Latest trading day snapshot with full fundamentals",
        "columns": [
            ("ticker",              "string", "Stock ticker symbol"),
            ("date",                "string", "Trading date YYYY-MM-DD"),
            ("open",                "double", "Opening price USD"),
            ("high",                "double", "Intraday high price USD"),
            ("low",                 "double", "Intraday low price USD"),
            ("close",               "double", "Closing price USD"),
            ("volume",              "bigint", "Daily trading volume"),
            ("net_margin_pct",      "double", "Net profit margin percentage"),
            ("roe_pct",             "double", "Return on equity percentage"),
            ("beta",                "double", "Beta vs S&P 500"),
            ("sector",              "string", "GICS sector"),
            ("equity_style",        "string", "Growth / Value / Blend"),
            ("quality_score",       "double", "Composite quality score 0-100"),
            ("trailing_pe",         "double", "Trailing P/E ratio"),
            ("market_cap_b",        "double", "Market cap in billions USD"),
            ("dividend_yield_pct",  "double", "Annual dividend yield percentage"),
        ],
    },
}


def get_or_create_database(glue):
    try:
        glue.get_database(Name=DATABASE)
        log.info("Database '%s' already exists", DATABASE)
    except glue.exceptions.EntityNotFoundException:
        glue.create_database(
            DatabaseInput={
                "Name":        DATABASE,
                "Description": "Equity Research Platform — Gold layer tables (Bronze→Silver→Gold on AWS S3)",
                "LocationUri": f"s3://{S3_BUCKET}/{S3_PREFIX}/",
            }
        )
        log.info("Database '%s' created", DATABASE)


def register_table(glue, table_name, meta):
    s3_location = f"s3://{S3_BUCKET}/{S3_PREFIX}/{table_name}/date={RUN_DATE}/"

    columns = [
        {"Name": col, "Type": dtype, "Comment": comment}
        for col, dtype, comment in meta["columns"]
    ]

    table_input = {
        "Name":        table_name,
        "Description": meta["description"],
        "StorageDescriptor": {
            "Columns":            columns,
            "Location":           s3_location,
            "InputFormat":        "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat":       "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"},
            },
            "Compressed": False,
        },
        "TableType": "EXTERNAL_TABLE",
        "Parameters": {
            "classification":       "parquet",
            "EXTERNAL":             "TRUE",
            "parquet.compression":  "SNAPPY",
            "pipeline_date":        RUN_DATE,
            "registered_at":        datetime.utcnow().isoformat(),
            "source":               "End-to-End Data Engineering Platform",
        },
    }

    try:
        glue.get_table(DatabaseName=DATABASE, Name=table_name)
        glue.update_table(DatabaseName=DATABASE, TableInput=table_input)
        log.info("Table '%s' updated in Glue Catalog", table_name)
    except glue.exceptions.EntityNotFoundException:
        glue.create_table(DatabaseName=DATABASE, TableInput=table_input)
        log.info("Table '%s' registered in Glue Catalog", table_name)


def main():
    log.info("=== AWS GLUE CATALOG REGISTRATION ===")
    log.info("Database : %s", DATABASE)
    log.info("Bucket   : s3://%s/%s/", S3_BUCKET, S3_PREFIX)
    log.info("Date     : %s", RUN_DATE)

    glue = boto3.client("glue", region_name=REGION)

    get_or_create_database(glue)

    for table_name, meta in TABLES.items():
        register_table(glue, table_name, meta)

    log.info("=== REGISTRATION COMPLETE ===")
    log.info("4 tables registered in database '%s'", DATABASE)
    log.info("View in AWS Console:")
    log.info("https://console.aws.amazon.com/glue/home?region=%s#/v2/data-catalog/databases/equity_research", REGION)


if __name__ == "__main__":
    main()