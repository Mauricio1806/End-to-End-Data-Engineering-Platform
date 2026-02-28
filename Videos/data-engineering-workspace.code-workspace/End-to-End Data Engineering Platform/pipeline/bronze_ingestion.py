"""
pipeline/bronze_ingestion.py
Bronze Layer — yfinance: 5yr price history + fundamentals
"""
from __future__ import annotations
import json, logging, os, argparse
from datetime import date, datetime
import boto3
from botocore.exceptions import ClientError
import yfinance as yf

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s - %(message)s")
log = logging.getLogger("bronze_ingestion")

S3_BUCKET = os.getenv("S3_BUCKET_RAW", "projectalphav")
S3_PREFIX = os.getenv("S3_PREFIX_RAW", "portfolio-raw")

SP500_TICKERS = [
    "AAPL","MSFT","GOOGL","AMZN","NVDA",
    "META","TSLA","BRK-B","UNH","JNJ",
]

FUNDAMENTALS_KEYS = [
    "trailingPE","forwardPE","priceToBook","trailingEps","forwardEps",
    "totalRevenue","revenueGrowth","grossMargins","operatingMargins",
    "profitMargins","returnOnEquity","returnOnAssets","debtToEquity",
    "freeCashflow","dividendYield","marketCap","enterpriseValue",
    "enterpriseToEbitda","enterpriseToRevenue","beta","sector","industry",
    "fullTimeEmployees","country","currency","shortName","longName",
]

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
        log.info("Bucket created: %s", bucket)

def fetch_ticker(symbol: str, period: str = "5y") -> dict | None:
    try:
        tkr = yf.Ticker(symbol)

        # Price history
        hist = tkr.history(period=period, auto_adjust=True)
        if hist.empty:
            log.error("No price data for %s", symbol)
            return None

        history_records = []
        for ts, row in hist.iterrows():
            history_records.append({
                "date":   ts.strftime("%Y-%m-%d"),
                "open":   round(float(row["Open"]),4),
                "high":   round(float(row["High"]),4),
                "low":    round(float(row["Low"]),4),
                "close":  round(float(row["Close"]),4),
                "volume": int(row["Volume"]),
            })

        # Fundamentals
        info = tkr.info or {}
        fundamentals = {k: info.get(k) for k in FUNDAMENTALS_KEYS}

        # Quarterly financials
        try:
            income = tkr.quarterly_income_stmt
            quarterly = {}
            if income is not None and not income.empty:
                for col in income.columns[:8]:  # last 8 quarters
                    col_key = str(col.date()) if hasattr(col, "date") else str(col)
                    quarterly[col_key] = {
                        row: (None if str(income.loc[row, col]) == "nan"
                              else float(income.loc[row, col]))
                        for row in ["Total Revenue","Gross Profit",
                                    "Net Income","Operating Income"]
                        if row in income.index
                    }
        except Exception:
            quarterly = {}

        log.info("Fetched %s: %d trading days | PE=%.1f | Rev=$%.1fB",
                 symbol, len(history_records),
                 fundamentals.get("trailingPE") or 0,
                 (fundamentals.get("totalRevenue") or 0) / 1e9)

        return {
            "_metadata": {
                "source":       "yfinance",
                "ingested_at":  datetime.utcnow().isoformat() + "Z",
                "run_date":     date.today().isoformat(),
                "ticker":       symbol,
                "period":       period,
                "trading_days": len(history_records),
            },
            "price_history": history_records,
            "fundamentals":  fundamentals,
            "quarterly_financials": quarterly,
        }
    except Exception as e:
        log.error("Error fetching %s: %s", symbol, e)
        return None

def upload_to_bronze(s3, symbol: str, data: dict, run_date: str) -> str:
    safe = symbol.replace("-","_")
    s3_key = f"{S3_PREFIX}/fundamentals/date={run_date}/symbol={safe}/data.json"
    s3.put_object(
        Bucket=S3_BUCKET, Key=s3_key,
        Body=json.dumps(data, indent=2, default=str),
        ContentType="application/json",
    )
    log.info("Uploaded: s3://%s/%s", S3_BUCKET, s3_key)
    return s3_key

def run(run_date: str, tickers: list | None = None, period: str = "5y") -> dict:
    if tickers is None:
        tickers = SP500_TICKERS
    s3 = get_s3_client()
    ensure_bucket(s3, S3_BUCKET)
    results = {"success": [], "failed": [], "run_date": run_date}

    for i, ticker in enumerate(tickers):
        log.info("Fetching %s (%d/%d)...", ticker, i+1, len(tickers))
        data = fetch_ticker(ticker, period)
        if data:
            key = upload_to_bronze(s3, ticker, data, run_date)
            results["success"].append({
                "ticker": ticker,
                "s3_key": key,
                "trading_days": data["_metadata"]["trading_days"],
                "pe_ratio": data["fundamentals"].get("trailingPE"),
                "market_cap_b": round((data["fundamentals"].get("marketCap") or 0)/1e9, 1),
            })
        else:
            results["failed"].append(ticker)

    log.info("Bronze done: %d OK | %d failed", len(results["success"]), len(results["failed"]))
    return results

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",    default=date.today().isoformat())
    parser.add_argument("--tickers", nargs="+", default=SP500_TICKERS)
    parser.add_argument("--period",  default="5y")
    args = parser.parse_args()
    log.info("=== BRONZE START | date=%s | period=%s ===", args.date, args.period)
    run(args.date, args.tickers, args.period)
    log.info("=== BRONZE END ===")
