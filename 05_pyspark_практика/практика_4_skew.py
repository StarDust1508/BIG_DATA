"""
🧩 Практика 4 модуля 05 — Skew (перекос) и его лечение

Сценарий: в датасете есть несколько «горячих» клиентов с миллионами
транзакций, остальные — десятки. При groupBy/join это убивает Spark.

Что делает скрипт:
  1. Генерирует синтетический skewed-датасет.
  2. Считает агрегацию БЕЗ AQE и видим распределение партиций.
  3. Включает AQE и сравнивает.
  4. Демонстрирует salt-trick вручную.

Запуск:
    python3 практика_4_skew.py
"""
import time
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


ROOT = Path(__file__).resolve().parent.parent
SKEW_PARQUET = ROOT / "datasets" / "skewed_tx.parquet"


def get_spark(adaptive: bool) -> SparkSession:
    return (
        SparkSession.builder
        .appName(f"M05_P4_Skew_AQE={adaptive}")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.adaptive.enabled", str(adaptive).lower())
        .config("spark.sql.adaptive.skewJoin.enabled", str(adaptive).lower())
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def generate(spark: SparkSession) -> None:
    if SKEW_PARQUET.exists():
        return
    print("⚙️  Генерирую skewed-датасет...")
    # 95% записей у клиента "HOT", остальные размазаны
    normal = (spark.range(0, 1_000_000)
        .withColumnRenamed("id", "row")
        .withColumn("client_id",
            F.when(F.rand() < 0.95, F.lit("HOT"))
             .otherwise(F.concat(F.lit("c_"), (F.rand() * 1000).cast("int"))))
        .withColumn("amount", F.rand() * 1000))
    normal.write.mode("overwrite").parquet(str(SKEW_PARQUET))
    print(f"   {SKEW_PARQUET} создан")


def bench(label: str, adaptive: bool) -> float:
    spark = get_spark(adaptive)
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(str(SKEW_PARQUET))

    t0 = time.perf_counter()
    # Стандартная агрегация — без AQE будет «один долгий» task
    df.groupBy("client_id").agg(F.sum("amount").alias("total")).collect()
    dt = time.perf_counter() - t0
    print(f"  {label}: {dt:.2f} с")

    # Размер партиций после shuffle
    sizes = (df.groupBy("client_id").agg(F.count("*").alias("n"))
        .orderBy(F.col("n").desc()).limit(5).collect())
    for r in sizes:
        print(f"     {r['client_id'][:8]:8s}  n={r['n']:,}")

    spark.stop()
    return dt


def salt_trick() -> None:
    print("\n--- 3. Salt-trick (ручной приём) ---")
    spark = get_spark(adaptive=False)
    spark.sparkContext.setLogLevel("ERROR")
    df = spark.read.parquet(str(SKEW_PARQUET))

    SALT_BUCKETS = 16
    salted = df.withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int"))

    t0 = time.perf_counter()
    salted.groupBy("client_id", "salt").agg(F.sum("amount").alias("p")).groupBy("client_id").agg(F.sum("p").alias("total")).collect()
    dt = time.perf_counter() - t0
    print(f"  Salt-trick: {dt:.2f} с — выровняли нагрузку на executor'ы")
    spark.stop()


def main() -> None:
    spark0 = get_spark(adaptive=False)
    generate(spark0)
    spark0.stop()

    print("\n=== 1. БЕЗ AQE ===")
    bench("без AQE", adaptive=False)

    print("\n=== 2. С AQE skewJoin ===")
    bench("с AQE", adaptive=True)

    salt_trick()

    print(
        """
🧠 Что увидели:
  • Без AQE один executor возится с миллионом строк ключа HOT.
  • С AQE Spark сам разбивает «толстую» партицию на несколько.
  • Salt-trick — ручная версия того же приёма (для случаев,
    когда AQE недостаточно или его нет).
        """
    )


if __name__ == "__main__":
    main()
