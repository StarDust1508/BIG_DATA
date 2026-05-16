"""
🧩 Практика 1 модуля 05 — Полный ETL-пайплайн

Сценарий: ежедневная обработка транзакций.
  EXTRACT  : читаем сырые CSV (transactions.csv + clients.csv из модуля 02)
  TRANSFORM:
     - чистим (категории, типы, дубликаты)
     - обогащаем (присоединяем clients, считаем доли, флаги аномалий)
     - агрегируем (по сегменту/категории)
  LOAD     : пишем Parquet с партиционированием по dt=run_date

Запуск:
    python3 практика_1_etl.py --run-date 2026-05-15
"""
from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


ROOT = Path(__file__).resolve().parent.parent
CLIENTS_CSV = ROOT / "datasets" / "clients.csv"
TX_CSV = ROOT / "datasets" / "transactions.csv"
OUTPUT_DIR = ROOT / "datasets" / "etl_output"


# ─── ETL functions ────────────────────────────────────────────────────────

def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Module05_ETL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def extract(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    clients = spark.read.csv(str(CLIENTS_CSV), header=True, inferSchema=True)
    tx = spark.read.csv(str(TX_CSV), header=True, inferSchema=True)
    return clients, tx


def clean_tx(df: DataFrame) -> DataFrame:
    """Очистка транзакций."""
    return (df
        .dropDuplicates(["client_id", "ts"])
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("ts", F.to_timestamp("ts"))
        .filter(F.col("amount").isNotNull() & (F.col("amount") >= 0))
        .filter(F.col("client_id").isNotNull())
    )


def enrich(tx: DataFrame, clients: DataFrame) -> DataFrame:
    """Обогащение: присоединить сегмент клиента, посчитать ранг и флаги."""
    base = tx.join(F.broadcast(clients.select("client_id", "segment", "city")),
                   "client_id", "left")

    # P99 для флага «крупная»
    p99 = base.approxQuantile("amount", [0.99], 0.001)[0]

    return (base
        .withColumn("is_big", F.when(F.col("amount") > p99, 1).otherwise(0))
        .withColumn("rn_in_client",
            F.row_number().over(
                Window.partitionBy("client_id").orderBy(F.col("amount").desc())))
    )


def aggregate(df: DataFrame) -> DataFrame:
    """Дневные агрегаты по сегменту и категории."""
    return (df
        .groupBy("segment", "category")
        .agg(
            F.count("*").alias("n_tx"),
            F.countDistinct("client_id").alias("n_clients"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.round(F.avg("amount"), 2).alias("avg_amount"),
            F.sum("is_big").alias("n_big_tx"),
        )
        .orderBy("segment", "category")
    )


def assert_quality(df: DataFrame, name: str) -> int:
    n = df.count()
    if n == 0:
        raise ValueError(f"DQ failed on {name}: empty dataframe")
    print(f"  ✅ DQ {name}: {n:,} rows")
    return n


def load(df: DataFrame, run_date: str) -> None:
    df = df.withColumn("dt", F.lit(run_date))
    (df.coalesce(1).write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(str(OUTPUT_DIR)))
    print(f"  💾 Записано в {OUTPUT_DIR}/dt={run_date}")


# ─── main ──────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default="2026-05-15")
    args = parser.parse_args()

    if not CLIENTS_CSV.exists() or not TX_CSV.exists():
        print("❌ Сначала сгенерируйте датасеты:")
        print("   python3 02_python_для_данных/практика_1_eda.py")
        return

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    print(f"\n▶ ETL pipeline for {args.run_date}\n")

    print("E:")
    clients, tx = extract(spark)
    assert_quality(clients, "clients")
    assert_quality(tx, "tx_raw")

    print("\nT:")
    tx_clean = clean_tx(tx).cache()
    assert_quality(tx_clean, "tx_clean")

    tx_enriched = enrich(tx_clean, clients).cache()
    assert_quality(tx_enriched, "tx_enriched")

    daily = aggregate(tx_enriched)
    assert_quality(daily, "daily_agg")

    print("\nResult (агрегаты):")
    daily.show(30, truncate=False)

    print("\nL:")
    load(daily, args.run_date)

    tx_clean.unpersist()
    tx_enriched.unpersist()
    spark.stop()
    print("\n✅ ETL completed.")


if __name__ == "__main__":
    main()
