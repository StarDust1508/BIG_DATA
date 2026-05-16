"""
🧩 Практика 4 модуля 04 — Читаем план запроса (explain)

Цель: научиться разбирать физический план и понимать, что делает Catalyst.

Запуск:
    python3 практика_4_explain.py
"""
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


ROOT = Path(__file__).resolve().parent.parent
TX_PARQUET = ROOT / "datasets" / "bench_10m.parquet"


def main() -> None:
    if not TX_PARQUET.exists():
        print("❌ Сначала запустите практика_3_pandas_vs_spark.py чтобы создать parquet.")
        return

    spark = (
        SparkSession.builder
        .appName("M04_P4_Explain")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(str(TX_PARQUET))

    # ─── 1. Predicate pushdown ──────────────────────────────────────────────
    print("=" * 70)
    print("1. Predicate pushdown — фильтр уходит в Parquet")
    print("=" * 70)
    q1 = df.filter(F.col("amount") > 1000).select("client_id", "amount")
    q1.explain(False)   # обычный (без полного дерева)
    print("\nИщите в выводе: PushedFilters: [IsNotNull(amount), GreaterThan(amount,1000.0)]")

    # ─── 2. Column pruning ──────────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("2. Column pruning — читаем только нужные колонки")
    print("=" * 70)
    q2 = df.select("client_id", "amount")
    q2.explain(False)
    print("\nИщите ReadSchema: struct<client_id:int,amount:float>")

    # ─── 3. GroupBy → Exchange (shuffle) ───────────────────────────────────
    print("\n" + "=" * 70)
    print("3. GroupBy → Exchange (shuffle)")
    print("=" * 70)
    q3 = df.groupBy("city").agg(F.sum("amount").alias("total"))
    q3.explain(False)
    print("\nИщите 'Exchange hashpartitioning(city, 8)' — это shuffle.")

    # ─── 4. Join: large × small → broadcast ────────────────────────────────
    print("\n" + "=" * 70)
    print("4. Join: большая таблица + маленькая → BroadcastHashJoin")
    print("=" * 70)
    cities = spark.createDataFrame(
        [("Moscow", 1), ("SPb", 2), ("Kazan", 3), ("Novosib", 4), ("Yekat", 5)],
        ["city", "city_id"],
    )
    q4 = df.join(F.broadcast(cities), "city")
    q4.explain(False)
    print("\nИщите 'BroadcastHashJoin'. Catalyst автоматически бы выбрал это и без broadcast(),")
    print("потому что cities < spark.sql.autoBroadcastJoinThreshold (10 МБ по умолчанию).")

    # ─── 5. Полный план — analyzed / logical / physical ────────────────────
    print("\n" + "=" * 70)
    print("5. Полный план (4 уровня) для запроса №3")
    print("=" * 70)
    q3.explain(True)

    print(
        """
🧠 Что вы должны увидеть:
  • PushedFilters: Spark передал фильтр в Parquet → не читает лишние строки.
  • ReadSchema: показывает, что читаются ТОЛЬКО нужные колонки.
  • Exchange: показывает, где идёт shuffle.
  • BroadcastHashJoin: маленькая таблица скопирована на executor'ы.
  • Optimized Logical Plan: видно, как Catalyst переписал ваш запрос.
        """
    )
    spark.stop()


if __name__ == "__main__":
    main()
