"""
dashboard/app.py
Flask server — reads Gold layer from S3 and serves the Equity Research Desk
Run: python dashboard/app.py
Access: http://localhost:8050
"""
import io, os, boto3, pandas as pd
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

S3_BUCKET = os.getenv("S3_BUCKET_CURATED", "projectalphav")
S3_PREFIX = os.getenv("S3_PREFIX_CURATED",  "portfolio-curated")
RUN_DATE  = os.getenv("PIPELINE_DATE",       "2025-01-15")
REGION    = os.getenv("AWS_DEFAULT_REGION",  "us-east-1")

def s3_client():
    return boto3.client("s3", region_name=REGION)

def read_parquet(table: str) -> pd.DataFrame:
    key = f"{S3_PREFIX}/{table}/date={RUN_DATE}/data.parquet"
    r   = s3_client().get_object(Bucket=S3_BUCKET, Key=key)
    return pd.read_parquet(io.BytesIO(r["Body"].read()))

@app.route("/api/snapshot")
def api_snapshot():
    df = read_parquet("daily_snapshot")
    cols = ["ticker","date","open","high","low","close","volume",
            "net_margin_pct","roe_pct","beta","sector","equity_style",
            "quality_score","trailing_pe","market_cap_b","dividend_yield_pct"]
    cols = [c for c in cols if c in df.columns]
    return jsonify(df[cols].fillna(0).to_dict(orient="records"))

@app.route("/api/ranking")
def api_ranking():
    df = read_parquet("equity_ranking")
    cols = ["ticker","sector","equity_style","quality_score","trailing_pe",
            "price_to_book","roe_pct","net_margin_pct","pe_vs_sector_label",
            "overall_rank","market_cap_b"]
    cols = [c for c in cols if c in df.columns]
    return jsonify(df[cols].fillna(0).sort_values("overall_rank").to_dict(orient="records"))

@app.route("/api/sectors")
def api_sectors():
    df = read_parquet("sector_valuation")
    return jsonify(df.fillna(0).to_dict(orient="records"))

@app.route("/api/performance")
def api_performance():
    df = read_parquet("performance_annual")
    cols = ["ticker","year","annual_return_pct","sector","equity_style","avg_volume"]
    cols = [c for c in cols if c in df.columns]
    return jsonify(df[cols].fillna(0).to_dict(orient="records"))

@app.route("/")
def index():
    return render_template_string(open(
        os.path.join(os.path.dirname(__file__), "index.html"),
        encoding="utf-8"
    ).read())

if __name__ == "__main__":
    print("Starting Equity Research Desk at http://localhost:8050")
    app.run(host="0.0.0.0", port=8050, debug=False)
