"""
Проект 3 — ETL для регулярной отчётности
==========================================

Production-style pipeline:
   - schema validation
   - DQ assertions
   - idempotent partition overwrite
   - metrics + audit log

Запуск:
    python3 pipeline.py --run-date 2026-05-15
"""
from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType, TimestampType,
)


HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
DATA = ROOT / "datasets"

CLIENTS_CSV = DATA / "clients.csv"
TX_CSV = DATA / "transactions.csv"
OUT_DIR = DATA / "etl_reports"
AUDIT_LOG = HERE / "audit.log"
METRICS_FILE = HERE / "metrics_latest.json"


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(AUDIT_LOG, mode="a", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("etl-pipeline")


# ─── Spark ─────────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Project3_ETL_Reports")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ─── Schema validation ────────────────────────────────────────────────────
TX_SCHEMA_EXPECTED = StructType([
    StructField("client_id", IntegerType(), True),
    StructField("ts",        StringType(),   True),
    StructField("category",  StringType(),   True),
    StructField("amount",    DoubleType(),   True),
])


def validate_schema(df: DataFrame, expected: StructType, name: str) -> None:
    actual = {f.name: f.dataType for f in df.schema.fields}
    exp    = {f.name: f.dataType for f in expected.fields}
    for col, t in exp.items():
        if col not in actual:
            raise ValueError(f"[{name}] missing column: {col}")
        if str(actual[col]) != str(t):
            log.warning(f"[{name}] type mismatch for {col}: "
                        f"expected {t}, got {actual[col]}")
    log.info(f"  ✅ схема {name}: корректна")


# ─── DQ assertions ─────────────────────────────────────────────────────────
def assert_dq(df: DataFrame, name: str) -> int:
    n = df.count()
    if n == 0:
        raise ValueError(f"[{name}] empty")
    bad_amount = df.filter(F.col("amount") < 0).count()
    bad_client = df.filter(F.col("client_id").isNull()).count()
    if bad_amount / n > 0.001:
        raise ValueError(f"[{name}] too many negative amounts: {bad_amount}")
    log.info(f"  ✅ DQ {name}: строк={n:,}, отрицательных={bad_amount}, null_клиентов={bad_client}")
    return n


# ─── Pipeline ───────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default="2026-05-15")
    args = parser.parse_args()
    run_date = args.run_date

    log.info("=" * 60)
    log.info(f"Старт ETL: run_date={run_date}, пользователь={os.getenv('USER', '?')}")
    log.info("=" * 60)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    metrics: dict = {"run_date": run_date, "start": datetime.utcnow().isoformat()}

    # E — extract
    log.info("Этап: EXTRACT (извлечение)")
    if not CLIENTS_CSV.exists() or not TX_CSV.exists():
        raise FileNotFoundError("Сначала запустите модуль 02 для генерации данных.")
    clients = spark.read.csv(str(CLIENTS_CSV), header=True, inferSchema=True)
    tx = spark.read.csv(str(TX_CSV), header=True, inferSchema=True)
    validate_schema(tx, TX_SCHEMA_EXPECTED, "transactions")
    metrics["rows_raw"] = assert_dq(tx, "transactions_raw")

    # T — clean
    log.info("Этап: CLEAN (очистка)")
    tx = (tx
        .dropDuplicates(["client_id", "ts"])
        .filter(F.col("client_id").isNotNull())
        .filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("ts", F.to_timestamp("ts"))
        .cache())
    metrics["rows_clean"] = assert_dq(tx, "transactions_clean")

    # T — enrich
    log.info("Этап: ENRICH (обогащение)")
    enriched = (tx
        .join(F.broadcast(clients.select("client_id", "segment", "city")),
              "client_id", "left")
        .withColumn("date", F.to_date("ts"))
    )

    # T — aggregate 1: segments
    log.info("Этап: AGGREGATE — агрегаты по сегментам")
    seg = (enriched
        .groupBy("segment", "category")
        .agg(
            F.count("*").alias("n_tx"),
            F.countDistinct("client_id").alias("n_clients"),
            F.round(F.sum("amount"), 2).alias("total"),
            F.round(F.avg("amount"), 2).alias("avg"),
        )
    )
    metrics["segments_groups"] = seg.count()

    # T — aggregate 2: top-100 контрагенты по клиенту (имитация)
    log.info("Этап: AGGREGATE — топ-100 клиентов")
    top100 = (enriched
        .groupBy("client_id", "segment", "city")
        .agg(
            F.count("*").alias("n_tx"),
            F.round(F.sum("amount"), 2).alias("total"),
        )
        .orderBy(F.col("total").desc())
        .limit(100)
    )

    # T — aggregate 3: KPI
    log.info("Этап: расчёт KPI")
    total_amount = tx.agg(F.sum("amount")).first()[0] or 0
    n_clients_active = tx.select("client_id").distinct().count()
    avg_check = total_amount / metrics["rows_clean"] if metrics["rows_clean"] else 0
    kpi = {
        "total_amount":   round(total_amount, 2),
        "n_transactions": metrics["rows_clean"],
        "n_clients_active": n_clients_active,
        "avg_check":      round(avg_check, 2),
    }
    metrics["kpi"] = kpi
    log.info(f"  KPI: {kpi}")

    # L — load
    log.info("Этап: LOAD (запись результатов)")
    seg_path = OUT_DIR / "segments"
    top_path = OUT_DIR / "top100"
    kpi_path = OUT_DIR / "kpi"

    seg.withColumn("dt", F.lit(run_date)) \
        .coalesce(1).write.mode("overwrite") \
        .partitionBy("dt").parquet(str(seg_path))
    log.info(f"  💾 {seg_path}/dt={run_date}")

    top100.withColumn("dt", F.lit(run_date)) \
        .coalesce(1).write.mode("overwrite") \
        .partitionBy("dt").parquet(str(top_path))
    log.info(f"  💾 {top_path}/dt={run_date}")

    # KPI как JSON
    kpi_path.mkdir(parents=True, exist_ok=True)
    (kpi_path / f"kpi_{run_date}.json").write_text(
        json.dumps(kpi, ensure_ascii=False, indent=2), encoding="utf-8")
    log.info(f"  💾 KPI JSON: {kpi_path / f'kpi_{run_date}.json'}")

    # Metrics
    metrics["end"] = datetime.utcnow().isoformat()
    METRICS_FILE.write_text(json.dumps(metrics, ensure_ascii=False, indent=2),
                             encoding="utf-8")
    log.info(f"  📊 metrics_latest.json")

    tx.unpersist()
    spark.stop()
    log.info("✅ ETL завершён.")


if __name__ == "__main__":
    main()
