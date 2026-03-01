# 📈 End-to-End Data Engineering Platform

**Airflow + Kafka + AWS S3 — Production-Style Equity Research Pipeline**

End-to-end Data Engineering project using a **Medallion Architecture (Bronze → Silver → Gold)**, orchestrated with **Apache Airflow**, with real-time streaming via **Apache Kafka**, fundamentals analysis, quality scoring, and a live analytical dashboard published to **AWS S3**.

---

## 🚀 What This Project Demonstrates

- Medallion Lakehouse architecture (Bronze → Silver → Gold)
- Real financial data ingestion via yfinance (10 S&P 500 equities, 5 years)
- Fundamentals analysis: Quality Score, P/E, ROE, Net Margin, Beta, Market Cap
- Equity classification: Growth / Value / Blend
- Sector valuation and relative P/E analysis
- Real-time price streaming with Apache Kafka (producer/consumer)
- Apache Airflow DAG scheduled for weekday execution
- Static dashboard published to AWS S3 (no server cost)
- Full Docker Compose stack: Airflow, Kafka, Spark, Grafana, Prometheus, MinIO

---

## 🌐 Live Dashboard

Production-style HTML dashboard deployed to AWS S3:

🔗 [http://projectalphav.s3-website-us-east-1.amazonaws.com/dashboard/index.html](http://projectalphav.s3-website-us-east-1.amazonaws.com/dashboard/index.html)

The dashboard reads directly from the Gold layer on S3 and is updated every time the pipeline runs.

---

## 🏗 Architecture

```
yfinance API
     │
     ▼
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   BRONZE     │────▶│   SILVER     │────▶│    GOLD      │
│  Raw JSON    │     │  Parquet     │     │  Parquet     │
│  S3 / AWS    │     │  S3 / AWS    │     │  S3 / AWS    │
└──────────────┘     └──────────────┘     └──────────────┘
                                                 │
                        ┌────────────────────────┼──────────────────────┐
                        ▼                        ▼                      ▼
                 equity_ranking          sector_valuation        daily_snapshot
                        │
                        ▼
              Static Dashboard → S3 Website

Kafka (Real-time Streaming)
producer.py ──▶ topic: stock-prices (3 partitions) ──▶ consumer.py

Orchestration: Apache Airflow 2.8
Schedule: 0 23 * * 1-5 (weekdays 23:00 UTC)
```

---

## 🗂 Repository Structure

```
├── pipeline/
│   ├── bronze_ingestion.py      # Raw OHLCV + fundamentals via yfinance
│   ├── silver_transform.py      # MA, Bollinger Bands, Quality Score
│   ├── gold_aggregations.py     # Analytical tables
│   └── run_pipeline.py          # CLI runner
├── dags/
│   └── stock_market_ingestion.py   # Airflow DAG
├── dashboard/
│   ├── app.py                   # Flask server (port 8050)
│   ├── index.html               # Equity Research Desk UI
│   └── generate_static.py       # S3 static publisher
├── kafka/
│   ├── producer.py              # Real-time price feed simulation
│   └── consumer.py              # Kafka message consumer
├── dbt/
│   └── models/
│       ├── staging/             # stg_stock_prices_daily
│       └── marts/               # fct_stock_prices
├── spark/jobs/
│   └── transform_stock_prices.py
├── monitoring/
│   └── prometheus/prometheus.yml
├── docker-compose.yml
└── requirements.txt
```

---

## 🥉 Bronze Layer

**Goal:** Ingest raw financial data with full fidelity

- **Source:** yfinance API (free, no rate limits)
- **Coverage:** 10 S&P 500 tickers — AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, BRK-B, UNH, JNJ
- **History:** 5 years of daily OHLCV (~1,260 trading days per ticker)
- **Fundamentals:** P/E, EPS, Revenue, Net Margin, ROE, Debt/Equity, Beta, Market Cap, Dividend Yield
- **Output:** JSON partitioned by `date=` and `symbol=` on S3

```
s3://projectalphav/portfolio-raw/fundamentals/date=YYYY-MM-DD/symbol=TICKER/data.json
```

---

## 🥈 Silver Layer

**Goal:** Clean, enrich and compute indicators

Transformations performed:
- Moving Averages: MA20, MA50, MA200
- Bollinger Bands (20d, 2σ)
- Daily returns, cumulative returns, annual returns, rolling volatility
- Quality Score (0–100): ROE (25%) + ROA (25%) + Net Margin (25%) + Debt/Equity (25%)
- Equity Style classification: Growth / Value / Blend

```
s3://projectalphav/portfolio-processed/prices/ticker=TICKER/data.parquet
```

---

## 🥇 Gold Layer

**Goal:** Build analytical tables ready for consumption

| Table | Description |
|---|---|
| `equity_ranking` | All 10 tickers ranked by Quality Score with sector P/E comparison |
| `sector_valuation` | Aggregated P/E, quality, revenue growth and market cap by sector |
| `performance_annual` | Annual returns per ticker per year (2020–2025) |
| `daily_snapshot` | Latest trading day with full fundamentals |

```
s3://projectalphav/portfolio-curated/equity_ranking/date=YYYY-MM-DD/data.parquet
```

---

## 📊 Dashboard Preview

The Equity Research Desk reads live data from the S3 Gold layer and renders:

- **Ticker tape** with ROE indicators for all 10 equities
- **Coverage Universe** watchlist with equity style tags
- **Stock detail view** — P/E, Beta, dividend yield, quality score bar
- **Equity Ranking** — all tickers ranked by Quality Score with P/E vs sector
- **Sector Valuation** — market cap bars, avg P/E, revenue growth, avg ROE
- **ROE Ranking** sidebar
- **Pipeline Status** panel — Bronze / Silver / Gold / AWS S3

---

## ⚡ Kafka Real-Time Streaming

Real-time stock price simulation publishing to topic `stock-prices` (3 partitions, replication factor 1).

```bash
# Terminal 1 — Start consumer
python kafka/consumer.py

# Terminal 2 — Start producer
python kafka/producer.py
```

Output:
```
NVDA   | price=194.87   | change=+0.3102% | vol=287654 | ts=2026-03-01T...
AAPL   | price=275.32   | change=+0.1354% | vol=165421 | ts=2026-03-01T...
MSFT   | price=401.14   | change=-0.2201% | vol=482105 | ts=2026-03-01T...
```

---

## 🔄 Airflow Orchestration

DAG: `stock_market_ingestion`
Schedule: `0 23 * * 1-5` (weekdays at 23:00 UTC)

```
bronze_ingestion → silver_transform → gold_aggregations → notify_success
```

AWS credentials stored as Airflow Variables (never hardcoded).

---

## 🏭 Production Considerations

This project was designed with production-readiness principles:

- Medallion Lakehouse architecture (Bronze → Silver → Gold)
- Idempotent transformation logic (safe to re-run)
- Explicit schema enforcement via PyArrow
- Quality Score as composite metric — not a single indicator
- Relative P/E analysis (ticker vs sector average)
- AWS credentials managed via Airflow Variables
- GitHub Push Protection — secrets never committed
- Separation between computation and presentation layers
- S3 static hosting as zero-cost deployment

Potential production upgrades:
- CI/CD pipeline with GitHub Actions
- IAM role-based S3 access (no static credentials)
- Cloud-native orchestration (AWS MWAA)
- Delta Lake / Iceberg storage format
- Infrastructure-as-Code (Terraform)
- AWS Glue Catalog + Athena for SQL queries

---

## ☁️ Scalability & Cloud Readiness

Although executed locally via Docker, the architecture is cloud-ready:

- Parquet-based columnar storage for scalable analytics
- S3 as the single source of truth across all layers
- Airflow DAG portable to AWS MWAA
- Kafka portable to Amazon MSK
- Spark jobs portable to AWS Glue / EMR
- Dashboard portable to CloudFront + S3

The pipeline can be migrated to: **AWS Glue · Amazon MWAA · Databricks · Snowflake · BigQuery**

---

## ⚙️ Running the Pipeline

### Prerequisites
- Docker Desktop
- Python 3.11+
- AWS CLI configured (`aws configure`)

### 1. Clone the repository

```bash
git clone https://github.com/Mauricio1806/End-to-End-Data-Engineering-Platform.git
cd End-to-End-Data-Engineering-Platform
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the Docker stack

```bash
docker compose up -d
```

### 4. Set AWS credentials as Airflow Variables

```bash
docker compose exec airflow-webserver airflow variables set AWS_ACCESS_KEY_ID 
docker compose exec airflow-webserver airflow variables set AWS_SECRET_ACCESS_KEY 
```

### 5. Run the pipeline manually

```bash
python pipeline/bronze_ingestion.py --date 2026-03-01
python pipeline/silver_transform.py --date 2026-03-01
python pipeline/gold_aggregations.py --date 2026-03-01
```

### 6. Publish static dashboard to S3

```bash
export AWS_ACCESS_KEY_ID=
export AWS_SECRET_ACCESS_KEY=
export S3_BUCKET_CURATED=projectalphav
export PIPELINE_DATE=2026-03-01
python dashboard/generate_static.py
```

### 7. Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Grafana | http://localhost:3000 | admin / admin |
| MinIO | http://localhost:9001 | minio / minio123 |
| Spark Master | http://localhost:8081 | — |
| Prometheus | http://localhost:9191 | — |
| Dashboard (local) | http://localhost:8050 | — |
| Dashboard (S3) | [Live Link](http://projectalphav.s3-website-us-east-1.amazonaws.com/dashboard/index.html) | — |

---

## 🛠 Tech Stack

| Category | Technology |
|---|---|
| Ingestion | yfinance, Python 3.11 |
| Storage | AWS S3 (Bronze / Silver / Gold) |
| Processing | Pandas, PyArrow |
| Orchestration | Apache Airflow 2.8 |
| Streaming | Apache Kafka + Zookeeper |
| Dashboard | Flask, HTML/CSS/JS |
| Static Hosting | AWS S3 Website |
| Monitoring | Grafana, Prometheus |
| Compute | Apache Spark 3.5 |
| Transformation | dbt-core |
| Infrastructure | Docker Compose |

---

## 📚 Data Source

**Yahoo Finance** via yfinance library — daily OHLCV prices and fundamentals for S&P 500 equities.

---

## 👨‍💻 Author

**Mauricio Esquivel**
Data Engineer | Analytics Engineer
Focus: Lakehouse Architecture, Orchestration, Cloud Data Platforms
