# End-to-End-Data-Engineering-Platform
A production-grade data engineering platform for equity research, built on a Medallion Architecture (Bronze → Silver → Gold) with real-time streaming, orchestration, and a live analytical dashboard.

Architecture Overview
yfinance API
     │
     ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   BRONZE    │────▶│   SILVER    │────▶│    GOLD     │
│  Raw JSON   │     │  Parquet    │     │  Parquet    │
│  S3 / AWS   │     │  S3 / AWS   │     │  S3 / AWS   │
└─────────────┘     └─────────────┘     └─────────────┘
                                               │
                          ┌────────────────────┼────────────────────┐
                          ▼                    ▼                    ▼
                   equity_ranking      sector_valuation     daily_snapshot
                          │
                          ▼
                   Flask Dashboard
                   localhost:8050

Kafka (Real-time)
producer.py ──▶ topic: stock-prices ──▶ consumer.py

Orchestration: Apache Airflow 2.8
Schedule: 0 23 * * 1-5 (weekdays 23:00 UTC)

Tech Stack
LayerTechnologyIngestionyfinance, PythonStorageAWS S3 (Bronze/Silver/Gold)ProcessingPandas, PyArrowOrchestrationApache Airflow 2.8StreamingApache Kafka + ZookeeperDashboardFlask, HTML/CSS/JSMonitoringGrafana, PrometheusComputeApache Spark 3.5Transformationdbt-coreInfrastructureDocker Compose

Project Structure
├── pipeline/
│   ├── bronze_ingestion.py     # Raw data ingestion via yfinance
│   ├── silver_transform.py     # Cleaning, indicators, quality score
│   ├── gold_aggregations.py    # Analytical tables
│   └── run_pipeline.py         # CLI runner
├── dags/
│   └── stock_market_ingestion.py  # Airflow DAG
├── dashboard/
│   ├── app.py                  # Flask server (port 8050)
│   └── index.html              # Equity Research Desk UI
├── kafka/
│   ├── producer.py             # Real-time price feed
│   └── consumer.py             # Message consumer
├── dbt/
│   └── models/
│       ├── staging/            # stg_stock_prices_daily
│       └── marts/              # fct_stock_prices
├── spark/jobs/
│   └── transform_stock_prices.py
├── monitoring/prometheus/
│   └── prometheus.yml
├── docker-compose.yml
└── requirements.txt

Pipeline Details
Bronze Layer

Source: yfinance API (free, no rate limit)
Coverage: 10 S&P 500 tickers — AAPL, MSFT, GOOGL, AMZN, NVDA, META, TSLA, BRK-B, UNH, JNJ
History: 5 years of daily OHLCV data (~1,260 trading days per ticker)
Fundamentals: P/E, EPS, Revenue, Margins, ROE, Debt/Equity, Beta, Market Cap
Output: JSON partitioned by date= and symbol= on S3

Silver Layer

Price indicators: MA(20/50/200d), Bollinger Bands, daily/cumulative/annual returns, volatility
Fundamental metrics: quality_score (0-100), equity_style (Growth/Value/Blend)
Quality Score formula: ROE (25%) + ROA (25%) + Net Margin (25%) + Debt/Equity (25%)
Output: Parquet files on S3

Gold Layer
Four analytical tables:
TableRowsDescriptionequity_ranking10Quality score ranking with sector comparisonsector_valuation5Aggregated P/E, quality, revenue growth by sectorperformance_annual60Annual returns per ticker per year (2020–2025)daily_snapshot10Latest trading day with fundamentals

Equity Research Dashboard
Live dashboard served by Flask at http://localhost:8050, reading directly from AWS S3 Gold layer.
Features:

Real-time ticker tape with ROE indicators
Coverage Universe watchlist with equity style tags (Growth/Value/Blend)
Stock header with P/E, P/B, Beta, dividend yield
Quality Score bar (composite: ROE, margins, leverage)
Equity Ranking table — all 10 tickers ranked by quality
Sector Valuation table with market cap bars
ROE Ranking sidebar
Pipeline status panel


Kafka Streaming
Real-time stock price simulation publishing to topic stock-prices (3 partitions).
bash# Terminal 1 — Consumer
python kafka/consumer.py

# Terminal 2 — Producer
python kafka/producer.py
Output example:
NVDA   | price=194.87   | change=+0.3102% | vol=287654 | ts=2026-03-01T...
AAPL   | price=275.32   | change=+0.1354% | vol=165421 | ts=2026-03-01T...

Getting Started
Prerequisites

Docker Desktop
Python 3.11+
AWS CLI configured (aws configure)

Setup
bash# 1. Clone the repository
git clone https://github.com/Mauricio1806/End-to-End-Data-Engineering-Platform.git
cd End-to-End-Data-Engineering-Platform

# 2. Install dependencies
pip install -r requirements.txt

# 3. Start the Docker stack
docker compose up -d

# 4. Set AWS credentials as Airflow Variables
docker compose exec airflow-webserver airflow variables set AWS_ACCESS_KEY_ID <your-key>
docker compose exec airflow-webserver airflow variables set AWS_SECRET_ACCESS_KEY <your-secret>

# 5. Run the pipeline manually
python -m pipeline.run_pipeline --date 2025-01-15 --step bronze
python -m pipeline.run_pipeline --date 2025-01-15 --step silver
python -m pipeline.run_pipeline --date 2025-01-15 --step gold

# 6. Start the dashboard
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>
export S3_BUCKET_CURATED=projectalphav
export PIPELINE_DATE=2025-01-15
python dashboard/app.py
# Open http://localhost:8050
Service URLs
ServiceURLCredentialsAirflowhttp://localhost:8080admin / adminGrafanahttp://localhost:3000admin / adminKafka UIhttp://localhost:9090—MinIOhttp://localhost:9001minio / minio123Spark Masterhttp://localhost:8081—Prometheushttp://localhost:9191—

AWS S3 Data Layout
s3://projectalphav/
├── portfolio-raw/
│   └── fundamentals/date=YYYY-MM-DD/symbol=TICKER/data.json
├── portfolio-processed/
│   ├── prices/ticker=TICKER/data.parquet
│   └── fundamentals/ticker=TICKER/data.parquet
└── portfolio-curated/
    ├── equity_ranking/date=YYYY-MM-DD/data.parquet
    ├── sector_valuation/date=YYYY-MM-DD/data.parquet
    ├── performance_annual/date=YYYY-MM-DD/data.parquet
    └── daily_snapshot/date=YYYY-MM-DD/data.parquet

Airflow DAG
DAG: stock_market_ingestion
Schedule: 0 23 * * 1-5 (weekdays at 23:00 UTC)
bronze_ingestion → silver_transform → gold_aggregations → notify_success

Author
Mauricio Esquivel
Data Engineer
