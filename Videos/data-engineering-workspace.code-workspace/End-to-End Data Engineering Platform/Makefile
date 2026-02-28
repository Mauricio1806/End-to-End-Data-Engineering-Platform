.PHONY: help up down status logs clean test-unit dbt-run dbt-test trigger-dag lint format

.DEFAULT_GOAL := help

help:
    @echo.
    @echo  Data Engineering Portfolio - Comandos disponiveis:
    @echo.
    @echo  up            Sobe todos os servicos Docker
    @echo  down          Para todos os servicos
    @echo  status        Status dos containers
    @echo  test-unit     Roda testes unitarios
    @echo  dbt-run       Executa modelos dbt
    @echo  dbt-test      Roda testes de qualidade dbt
    @echo  lint          Roda linters
    @echo  format        Formata o codigo

up:
    docker compose up -d --build

down:
    docker compose down

status:
    docker compose ps

logs:
    docker compose logs -f

clean:
    docker compose down -v

test-unit:
    pytest tests/unit/ -v --cov=spark --cov-report=term-missing

dbt-run:
    cd dbt && dbt run --profiles-dir ./profiles

dbt-test:
    cd dbt && dbt test --profiles-dir ./profiles

trigger-dag:
    docker compose exec airflow-webserver airflow dags trigger stock_market_ingestion

list-dags:
    docker compose exec airflow-webserver airflow dags list

lint:
    flake8 . --max-line-length=100

format:
    black .
    isort .
