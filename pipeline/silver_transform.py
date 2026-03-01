"""
pipeline/silver_transform.py
Silver Layer — price transforms + fundamental analysis + equity classification
"""
from __future__ import annotations
import io, json, logging, os, argparse
from datetime import date, datetime
import boto3
import pandas as pd
import numpy as np
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
log = logging.getLogger("silver_transform")

BRONZE_BUCKET = os.getenv("S3_BUCKET_RAW",       "projectalphav")
SILVER_BUCKET = os.getenv("S3_BUCKET_PROCESSED",  "projectalphav")
BRONZE_PREFIX = os.getenv("S3_PREFIX_RAW",        "portfolio-raw")
SILVER_PREFIX = os.getenv("S3_PREFIX_PROCESSED",  "portfolio-processed")

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

def list_bronze_keys(s3, run_date: str) -> list:
    prefix = f"{BRONZE_PREFIX}/fundamentals/date={run_date}/"
    resp = s3.list_objects_v2(Bucket=BRONZE_BUCKET, Prefix=prefix)
    keys = [o["Key"] for o in resp.get("Contents",[]) if o["Key"].endswith("data.json")]
    log.info("Bronze files found: %d", len(keys))
    return keys

def build_price_df(payload: dict) -> pd.DataFrame:
    ticker = payload["_metadata"]["ticker"]
    records = payload["price_history"]
    df = pd.DataFrame(records)
    df["trade_date"] = pd.to_datetime(df["date"])
    df["ticker"] = ticker
    df = df.sort_values("trade_date").reset_index(drop=True)

    # Returns
    df["daily_return_pct"]      = df["close"].pct_change().mul(100).round(4)
    df["cumulative_return_pct"] = ((1 + df["close"].pct_change()).cumprod() - 1).mul(100).round(4)

    # Annual returns
    df["year"] = df["trade_date"].dt.year
    df["annual_return_pct"] = df.groupby("year")["close"].transform(
        lambda x: ((x / x.iloc[0]) - 1) * 100
    ).round(4)

    # Moving averages
    df["ma_20d"]  = df["close"].rolling(20,  min_periods=1).mean().round(4)
    df["ma_50d"]  = df["close"].rolling(50,  min_periods=1).mean().round(4)
    df["ma_200d"] = df["close"].rolling(200, min_periods=1).mean().round(4)

    # Volatility
    df["volatility_20d"]  = (df["daily_return_pct"].rolling(20,  min_periods=10).std() * (252**0.5)).round(4)
    df["volatility_252d"] = (df["daily_return_pct"].rolling(252, min_periods=60).std() * (252**0.5)).round(4)

    # Bollinger Bands
    rolling_std       = df["close"].rolling(20, min_periods=10).std()
    df["bb_upper"]    = (df["ma_20d"] + 2 * rolling_std).round(4)
    df["bb_lower"]    = (df["ma_20d"] - 2 * rolling_std).round(4)
    df["bb_position"] = ((df["close"] - df["bb_lower"]) / (df["bb_upper"] - df["bb_lower"])).round(4)

    # Trend
    df["trend_signal"] = "neutral"
    df.loc[(df["close"] > df["ma_50d"])  & (df["ma_50d"]  > df["ma_200d"]), "trend_signal"] = "bullish"
    df.loc[(df["close"] < df["ma_50d"])  & (df["ma_50d"]  < df["ma_200d"]), "trend_signal"] = "bearish"

    # Volume
    df["vol_ma_20"] = df["volume"].rolling(20, min_periods=5).mean()
    df["vol_ratio"] = (df["volume"] / df["vol_ma_20"]).round(4)

    # DQ flags
    df["dq_flag"] = "ok"
    df.loc[df["volume"] == 0,                 "dq_flag"] = "zero_volume"
    df.loc[df["close"] <= 0,                  "dq_flag"] = "invalid_price"
    df.loc[df["daily_return_pct"].abs() > 25, "dq_flag"] = "extreme_move"

    df["transformed_at"] = datetime.utcnow().isoformat() + "Z"
    return df

def build_fundamentals_df(payload: dict) -> pd.DataFrame:
    ticker = payload["_metadata"]["ticker"]
    f = payload.get("fundamentals", {})

    # Equity classification
    pe = f.get("trailingPE") or 0
    pb = f.get("priceToBook") or 0
    rev_growth = (f.get("revenueGrowth") or 0) * 100

    if pe > 30 or rev_growth > 20:
        style = "Growth"
    elif pe < 15 and pb < 2:
        style = "Value"
    elif pe == 0:
        style = "N/A"
    else:
        style = "Blend"

    # Quality score (0-100)
    roe   = (f.get("returnOnEquity")  or 0) * 100
    roa   = (f.get("returnOnAssets")  or 0) * 100
    npm   = (f.get("profitMargins")   or 0) * 100
    fcf   = f.get("freeCashflow") or 0
    d2e   = f.get("debtToEquity") or 999

    quality_score = min(100, max(0,
        min(roe, 30) / 30 * 25 +
        min(roa, 15) / 15 * 25 +
        min(npm, 30) / 30 * 25 +
        max(0, (50 - min(d2e, 50)) / 50 * 25)
    ))

    return pd.DataFrame([{
        "ticker":              ticker,
        "short_name":          f.get("shortName"),
        "sector":              f.get("sector"),
        "industry":            f.get("industry"),
        "country":             f.get("country"),
        "currency":            f.get("currency"),
        "market_cap_b":        round((f.get("marketCap") or 0) / 1e9, 2),
        "enterprise_value_b":  round((f.get("enterpriseValue") or 0) / 1e9, 2),
        "trailing_pe":         f.get("trailingPE"),
        "forward_pe":          f.get("forwardPE"),
        "price_to_book":       f.get("priceToBook"),
        "trailing_eps":        f.get("trailingEps"),
        "forward_eps":         f.get("forwardEps"),
        "total_revenue_b":     round((f.get("totalRevenue") or 0) / 1e9, 2),
        "revenue_growth_pct":  round(rev_growth, 2),
        "gross_margin_pct":    round((f.get("grossMargins")     or 0) * 100, 2),
        "operating_margin_pct":round((f.get("operatingMargins") or 0) * 100, 2),
        "net_margin_pct":      round((f.get("profitMargins")    or 0) * 100, 2),
        "roe_pct":             round(roe, 2),
        "roa_pct":             round(roa, 2),
        "debt_to_equity":      f.get("debtToEquity"),
        "free_cashflow_b":     round((f.get("freeCashflow") or 0) / 1e9, 2),
        "dividend_yield_pct":  round((f.get("dividendYield") or 0) * 100, 2),
        "beta":                f.get("beta"),
        "ev_to_ebitda":        f.get("enterpriseToEbitda"),
        "ev_to_revenue":       f.get("enterpriseToRevenue"),
        "equity_style":        style,
        "quality_score":       round(quality_score, 1),
        "transformed_at":      datetime.utcnow().isoformat() + "Z",
    }])

def write_parquet(s3, df: pd.DataFrame, key: str) -> str:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, engine="pyarrow")
    buf.seek(0)
    s3.put_object(Bucket=SILVER_BUCKET, Key=key,
                  Body=buf.getvalue(), ContentType="application/octet-stream")
    log.info("Silver written: s3://%s/%s (%d rows)", SILVER_BUCKET, key, len(df))
    return key

def run(run_date: str) -> dict:
    s3 = get_s3_client()
    ensure_bucket(s3, SILVER_BUCKET)
    keys = list_bronze_keys(s3, run_date)
    if not keys:
        log.warning("No Bronze data for %s", run_date)
        return {"success":[], "failed":[], "run_date": run_date}

    results = {"success":[], "failed":[], "run_date": run_date}

    for key in keys:
        ticker = key.split("symbol=")[1].split("/")[0].replace("_","-")
        log.info("Transforming %s...", ticker)
        try:
            resp    = s3.get_object(Bucket=BRONZE_BUCKET, Key=key)
            payload = json.loads(resp["Body"].read())

            # Price silver
            df_price = build_price_df(payload)
            safe = ticker.replace("-","_")
            price_key = f"{SILVER_PREFIX}/prices/ticker={safe}/data.parquet"
            write_parquet(s3, df_price, price_key)

            # Fundamentals silver
            df_fund = build_fundamentals_df(payload)
            fund_key = f"{SILVER_PREFIX}/fundamentals/ticker={safe}/data.parquet"
            write_parquet(s3, df_fund, fund_key)

            results["success"].append({
                "ticker":        ticker,
                "price_rows":    len(df_price),
                "equity_style":  df_fund["equity_style"].iloc[0],
                "quality_score": df_fund["quality_score"].iloc[0],
                "trailing_pe":   df_fund["trailing_pe"].iloc[0],
            })
        except Exception as e:
            log.error("Failed %s: %s", ticker, e)
            results["failed"].append(ticker)

    log.info("Silver: %d OK | %d failed", len(results["success"]), len(results["failed"]))
    for r in results["success"]:
        log.info("  %-8s style=%-7s quality=%.0f PE=%.1f rows=%d",
                 r["ticker"], r["equity_style"], r["quality_score"] or 0,
                 r["trailing_pe"] or 0, r["price_rows"])
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", default=date.today().isoformat())
    args = parser.parse_args()
    log.info("=== SILVER START | date=%s ===", args.date)
    run(args.date)
    log.info("=== SILVER END ===")
