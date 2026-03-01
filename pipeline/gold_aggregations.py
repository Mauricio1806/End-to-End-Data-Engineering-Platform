"""
pipeline/gold_aggregations.py
Gold Layer — equity ranking, valuation comparison, 5yr performance
"""
from __future__ import annotations
import io, logging, os, argparse
from datetime import date, datetime
import boto3
import pandas as pd
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
log = logging.getLogger("gold_aggregations")

SILVER_BUCKET = os.getenv("S3_BUCKET_PROCESSED", "projectalphav")
GOLD_BUCKET   = os.getenv("S3_BUCKET_CURATED",   "projectalphav")
SILVER_PREFIX = os.getenv("S3_PREFIX_PROCESSED",  "portfolio-processed")
GOLD_PREFIX   = os.getenv("S3_PREFIX_CURATED",    "portfolio-curated")

def get_s3_client():
    endpoint = os.getenv("MINIO_ENDPOINT","").strip()
    if endpoint:
        return boto3.client("s3", endpoint_url=endpoint,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID","minio"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY","minio123"),
            region_name="us-east-1")
    return boto3.client("s3", region_name=os.getenv("AWS_DEFAULT_REGION","us-east-1"))

def ensure_bucket(s3, bucket):
    try:
        s3.head_bucket(Bucket=bucket)
    except ClientError:
        s3.create_bucket(Bucket=bucket)

def read_silver(s3, subpath: str) -> pd.DataFrame:
    prefix = f"{SILVER_PREFIX}/{subpath}/"
    resp = s3.list_objects_v2(Bucket=SILVER_BUCKET, Prefix=prefix)
    keys = [o["Key"] for o in resp.get("Contents",[]) if o["Key"].endswith(".parquet")]
    if not keys:
        return pd.DataFrame()
    frames = []
    for key in keys:
        r = s3.get_object(Bucket=SILVER_BUCKET, Key=key)
        frames.append(pd.read_parquet(io.BytesIO(r["Body"].read())))
    return pd.concat(frames, ignore_index=True)

def build_equity_ranking(df_fund: pd.DataFrame) -> pd.DataFrame:
    df = df_fund.copy()
    # Rank within sector by quality score
    df["sector_rank"] = df.groupby("sector")["quality_score"].rank(ascending=False).astype(int)
    # Overall rank
    df["overall_rank"] = df["quality_score"].rank(ascending=False).astype(int)
    # Relative PE (vs sector median)
    sector_median_pe = df.groupby("sector")["trailing_pe"].transform("median")
    df["pe_vs_sector"] = ((df["trailing_pe"] / sector_median_pe - 1) * 100).round(2)
    df["pe_vs_sector_label"] = df["pe_vs_sector"].apply(
        lambda x: "Expensive" if x > 20 else ("Cheap" if x < -20 else "Fair") if pd.notna(x) else "N/A"
    )
    df["gold_created_at"] = datetime.utcnow().isoformat() + "Z"
    return df.sort_values("overall_rank")

def build_5yr_performance(df_price: pd.DataFrame, df_fund: pd.DataFrame) -> pd.DataFrame:
    sector_map = df_fund.set_index("ticker")["sector"].to_dict()
    style_map  = df_fund.set_index("ticker")["equity_style"].to_dict()

    # Annual return per ticker per year
    df_price["trade_date"] = pd.to_datetime(df_price["trade_date"])
    df_price["year"] = df_price["trade_date"].dt.year

    annual = df_price.groupby(["ticker","year"]).agg(
        open_price  = ("close","first"),
        close_price = ("close","last"),
        avg_volume  = ("volume","mean"),
        volatility  = ("volatility_20d","mean"),
        avg_trend   = ("trend_signal", lambda x: x.mode()[0] if len(x) > 0 else "neutral"),
    ).reset_index()

    annual["annual_return_pct"] = ((annual["close_price"] / annual["open_price"]) - 1).mul(100).round(2)
    annual["sector"]       = annual["ticker"].map(sector_map)
    annual["equity_style"] = annual["ticker"].map(style_map)
    annual["gold_created_at"] = datetime.utcnow().isoformat() + "Z"
    return annual

def build_sector_valuation(df_fund: pd.DataFrame) -> pd.DataFrame:
    agg = df_fund.groupby("sector").agg(
        ticker_count      = ("ticker","count"),
        avg_pe            = ("trailing_pe","mean"),
        median_pe         = ("trailing_pe","median"),
        avg_pb            = ("price_to_book","mean"),
        avg_quality_score = ("quality_score","mean"),
        avg_roe           = ("roe_pct","mean"),
        avg_net_margin    = ("net_margin_pct","mean"),
        avg_rev_growth    = ("revenue_growth_pct","mean"),
        total_market_cap  = ("market_cap_b","sum"),
        growth_count      = ("equity_style", lambda x: (x=="Growth").sum()),
        value_count       = ("equity_style", lambda x: (x=="Value").sum()),
    ).reset_index()

    for col in ["avg_pe","median_pe","avg_pb","avg_quality_score",
                "avg_roe","avg_net_margin","avg_rev_growth","total_market_cap"]:
        agg[col] = agg[col].round(2)

    agg["gold_created_at"] = datetime.utcnow().isoformat() + "Z"
    return agg

def build_daily_snapshot(df_price: pd.DataFrame, df_fund: pd.DataFrame) -> pd.DataFrame:
    latest = df_price.sort_values("trade_date").groupby("ticker").last().reset_index()
    merged = latest.merge(
        df_fund[["ticker","short_name","sector","equity_style","quality_score",
                 "trailing_pe","market_cap_b","trailing_eps","dividend_yield_pct",
                 "net_margin_pct","roe_pct","beta"]],
        on="ticker", how="left"
    )
    merged["gold_created_at"] = datetime.utcnow().isoformat() + "Z"
    return merged

def write_gold(s3, df: pd.DataFrame, table: str, run_date: str) -> str:
    key = f"{GOLD_PREFIX}/{table}/date={run_date}/data.parquet"
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3.put_object(Bucket=GOLD_BUCKET, Key=key,
                  Body=buf.getvalue(), ContentType="application/octet-stream")
    log.info("Gold written: s3://%s/%s (%d rows)", GOLD_BUCKET, key, len(df))
    return key

def run(run_date: str) -> dict:
    s3 = get_s3_client()
    ensure_bucket(s3, GOLD_BUCKET)

    df_price = read_silver(s3, "prices")
    df_fund  = read_silver(s3, "fundamentals")

    if df_price.empty or df_fund.empty:
        log.warning("No Silver data found. Run Silver first.")
        return {"error": "No Silver data", "run_date": run_date}

    log.info("Silver loaded: %d price rows | %d tickers with fundamentals",
             len(df_price), len(df_fund))

    ranking     = build_equity_ranking(df_fund)
    performance = build_5yr_performance(df_price, df_fund)
    valuation   = build_sector_valuation(df_fund)
    snapshot    = build_daily_snapshot(df_price, df_fund)

    results = {
        "run_date": run_date,
        "tables": {
            "equity_ranking":     write_gold(s3, ranking,     "equity_ranking",     run_date),
            "performance_annual": write_gold(s3, performance, "performance_annual", run_date),
            "sector_valuation":   write_gold(s3, valuation,   "sector_valuation",   run_date),
            "daily_snapshot":     write_gold(s3, snapshot,    "daily_snapshot",     run_date),
        },
        "stats": {
            "tickers":         len(df_fund),
            "price_rows":      len(df_price),
            "years_covered":   int(df_price["trade_date"].pipe(pd.to_datetime).dt.year.nunique()),
        }
    }

    log.info("=== GOLD SUMMARY ===")
    log.info("Equity Ranking (top 5 by quality):\n%s",
             ranking[["ticker","sector","equity_style","quality_score","trailing_pe",
                       "pe_vs_sector_label"]].head(5).to_string(index=False))
    log.info("\nSector Valuation:\n%s",
             valuation[["sector","avg_pe","avg_quality_score","avg_rev_growth",
                         "total_market_cap"]].to_string(index=False))

    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()
    log.info("=== GOLD START | date=%s ===", args.date)
    run(args.date)
    log.info("=== GOLD END ===")
