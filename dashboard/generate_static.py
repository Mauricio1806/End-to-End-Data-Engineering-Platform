"""
dashboard/generate_static.py
Reads Gold layer from S3, embeds data into HTML and publishes as static website
Run: python dashboard/generate_static.py
URL: http://projectalphav.s3-website-us-east-1.amazonaws.com/dashboard/index.html
"""
import io, os, json, boto3, pandas as pd
from datetime import datetime, date

S3_BUCKET  = os.getenv("S3_BUCKET_CURATED", "projectalphav")
S3_PREFIX  = os.getenv("S3_PREFIX_CURATED",  "portfolio-curated")
RUN_DATE   = os.getenv("PIPELINE_DATE",       "2026-03-01")
REGION     = os.getenv("AWS_DEFAULT_REGION",  "us-east-1")
OUTPUT_KEY = "dashboard/index.html"

class SafeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, (pd.Timestamp, datetime, date)):
            return str(obj)[:10]
        if isinstance(obj, float) and (obj != obj):
            return 0
        return super().default(obj)

def s3():
    return boto3.client("s3", region_name=REGION)

def read_parquet(table):
    key = f"{S3_PREFIX}/{table}/date={RUN_DATE}/data.parquet"
    r   = s3().get_object(Bucket=S3_BUCKET, Key=key)
    df  = pd.read_parquet(io.BytesIO(r["Body"].read()))
    for col in df.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]):
        df[col] = df[col].astype(str).str[:10]
    return df.fillna(0)

def main():
    print("Reading Gold layer from S3...")
    snapshot    = read_parquet("daily_snapshot").to_dict(orient="records")
    ranking     = read_parquet("equity_ranking").sort_values("overall_rank").to_dict(orient="records")
    sectors     = read_parquet("sector_valuation").to_dict(orient="records")

    print("Generating static HTML...")
    template = open(os.path.join(os.path.dirname(__file__), "index.html"), encoding="utf-8").read()

    data_script = f"""
<script>
window.__STATIC_DATA__ = {{
  snapshot:  {json.dumps(snapshot,  cls=SafeEncoder)},
  ranking:   {json.dumps(ranking,   cls=SafeEncoder)},
  sectors:   {json.dumps(sectors,   cls=SafeEncoder)},
  generated: "{datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"
}};
</script>
"""
    html = template.replace("</head>", data_script + "</head>")
    html = html.replace(
        "load();",
        """
async function load() {
  try {
    SNAPSHOT = window.__STATIC_DATA__.snapshot;
    RANKING  = window.__STATIC_DATA__.ranking;
    SECTORS  = window.__STATIC_DATA__.sectors;
    if (SNAPSHOT.length) {
      document.getElementById("runDate").textContent = "Snapshot: " + SNAPSHOT[0].date + " (static)";
      buildTape(); buildWL(); buildMovers(); buildRbar();
      select(SNAPSHOT[0].ticker);
    }
  } catch(e) { console.error(e); }
}
load();
"""
    )

    out_path = os.path.join(os.path.dirname(__file__), "index_static.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Static HTML saved locally: {out_path}")

    print("Publishing to S3...")
    s3().put_bucket_website(
        Bucket=S3_BUCKET,
        WebsiteConfiguration={
            "IndexDocument": {"Suffix": "index.html"},
            "ErrorDocument": {"Key": "index.html"},
        }
    )

    s3().put_object(
        Bucket=S3_BUCKET,
        Key=OUTPUT_KEY,
        Body=html.encode("utf-8"),
        ContentType="text/html"
    )

    url = f"http://{S3_BUCKET}.s3-website-us-east-1.amazonaws.com/dashboard/index.html"
    print(f"\n✅ Dashboard published!")
    print(f"🌐 URL: {url}")
    return url

if __name__ == "__main__":
    main()
