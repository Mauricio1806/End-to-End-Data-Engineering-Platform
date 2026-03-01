"""
pipeline/run_pipeline.py
Executa Bronze -> Silver -> Gold para uma data
"""
from __future__ import annotations
import argparse
import logging
import sys
from datetime import date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("pipeline")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",  default=date.today().isoformat())
    parser.add_argument("--step",  choices=["bronze", "silver", "gold", "all"],
                        default="all")
    args = parser.parse_args()
    run_date = args.date

    log.info("=" * 60)
    log.info("PIPELINE START | date=%s | step=%s", run_date, args.step)
    log.info("=" * 60)

    try:
        if args.step in ("bronze", "all"):
            log.info(">>> 1/3 BRONZE")
            from pipeline.bronze_ingestion import run as bronze_run
            r = bronze_run(run_date)
            log.info("Bronze: %d OK | %d falhas",
                     len(r["success"]), len(r["failed"]))
            if not r["success"]:
                raise RuntimeError("Bronze sem dados — pipeline abortado")

        if args.step in ("silver", "all"):
            log.info(">>> 2/3 SILVER")
            from pipeline.silver_transform import run as silver_run
            r = silver_run(run_date)
            log.info("Silver: %d OK | %d falhas",
                     len(r["success"]), len(r["failed"]))

        if args.step in ("gold", "all"):
            log.info(">>> 3/3 GOLD")
            from pipeline.gold_aggregations import run as gold_run
            r = gold_run(run_date)
            log.info("Gold: tabelas=%s", list(r.get("tables", {}).keys()))

        log.info("=" * 60)
        log.info("PIPELINE CONCLUIDO | date=%s", run_date)
        log.info("MinIO Console: http://localhost:9001 (minio/minio123)")
        log.info("=" * 60)

    except Exception as e:
        log.error("PIPELINE FALHOU: %s", e, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
