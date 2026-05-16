"""
🧩 Практика 1 модуля 04 — Первые шаги в Spark DataFrame API

Используем те же датасеты, которые сгенерировали в модуле 02:
   datasets/clients.csv
   datasets/transactions.csv

Если их нет — сначала запустите 02_python_для_данных/практика_1_eda.py

Запуск:
    python3 практика_1_первые_шаги.py
"""
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


ROOT = Path(__file__).resolve().parent.parent
CLIENTS = ROOT / "datasets" / "clients.csv"
TX = ROOT / "datasets" / "transactions.csv"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("M04_P1_FirstSteps")
        .master("local[*]")
        .config("spark.driver.memory", "2g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main() -> None:
    if not CLIENTS.exists() or not TX.exists():
        print("❌ Сначала сгенерируйте датасеты: запустите")
        print("   python3 02_python_для_данных/практика_1_eda.py")
        return

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    print(f"Spark {spark.version}, параллелизм: {spark.sparkContext.defaultParallelism}")

    # 1. Чтение
    clients = spark.read.csv(str(CLIENTS), header=True, inferSchema=True)
    tx = spark.read.csv(str(TX), header=True, inferSchema=True)

    print("\n--- 1. Схема ---")
    clients.printSchema()
    tx.printSchema()
    print(f"clients: {clients.count():,} строк")
    print(f"tx     : {tx.count():,} строк")

    # 2. Фильтр + select
    print("\n--- 2. Premium клиенты ---")
    (clients
        .filter(F.col("segment") == "premium")
        .select("client_id", "city", "monthly_income")
        .show(5))

    # 3. groupBy + agg
    print("\n--- 3. Транзакции по категориям ---")
    (tx.groupBy("category")
        .agg(
            F.count("*").alias("n"),
            F.sum("amount").alias("total"),
            F.avg("amount").alias("avg"),
        )
        .orderBy(F.col("total").desc())
        .show())

    # 4. join + агрегация
    print("\n--- 4. Сумма транзакций по сегментам клиентов ---")
    joined = tx.join(clients, on="client_id", how="inner")
    (joined.groupBy("segment")
        .agg(
            F.count("*").alias("tx_count"),
            F.round(F.sum("amount"), 0).alias("total"),
            F.round(F.avg("amount"), 0).alias("avg_per_tx"),
        )
        .orderBy(F.col("total").desc())
        .show())

    # 5. Топ-10 клиентов по обороту
    print("\n--- 5. Топ-10 клиентов по обороту ---")
    (tx.groupBy("client_id")
        .agg(F.sum("amount").alias("total"))
        .orderBy(F.col("total").desc())
        .limit(10)
        .show())

    spark.stop()


if __name__ == "__main__":
    main()
