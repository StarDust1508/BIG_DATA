"""
🧩 Практика 3 модуля 05 — Window functions

Сценарий: для каждого клиента считаем:
  - накопительный оборот (running total)
  - rolling-среднее по последним 5 транзакциям
  - топ-3 транзакции
  - разницу с предыдущей транзакцией
  - sessionization: новые сессии при разрыве > 1 час

Запуск:
    python3 практика_3_windows.py
"""
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


ROOT = Path(__file__).resolve().parent.parent
TX = ROOT / "datasets" / "transactions.csv"


def main() -> None:
    if not TX.exists():
        print("❌ Сначала сгенерируйте транзакции (модуль 02, практика 1).")
        return

    spark = (
        SparkSession.builder
        .appName("M05_P3_Windows")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    tx = (spark.read.csv(str(TX), header=True, inferSchema=True)
            .withColumn("ts", F.to_timestamp("ts"))
            .filter(F.col("client_id").isNotNull())
         )

    # Берём одного клиента для наглядности
    one = tx.filter(F.col("client_id") == 1).orderBy("ts")
    print(f"\n--- Клиент 1: {one.count()} транзакций ---")

    # ── 1. Running total ────────────────────────────────────────────────
    w_time = Window.partitionBy("client_id").orderBy("ts")
    one_run = one.withColumn("running_total",
        F.round(F.sum("amount").over(w_time), 2))
    print("\n1. Накопительная сумма (первые 7):")
    one_run.select("ts", "amount", "running_total").show(7, truncate=False)

    # ── 2. Rolling avg по последним 5 строкам ───────────────────────────
    w_rolling = (Window.partitionBy("client_id")
                  .orderBy("ts")
                  .rowsBetween(-4, 0))
    one_roll = one.withColumn("avg_last5",
        F.round(F.avg("amount").over(w_rolling), 2))
    print("\n2. Rolling avg за последние 5 транзакций:")
    one_roll.select("ts", "amount", "avg_last5").show(7, truncate=False)

    # ── 3. Топ-3 транзакции по сумме на ВСЁМ датасете ───────────────────
    w_rank = Window.partitionBy("client_id").orderBy(F.col("amount").desc())
    top3 = (tx
        .withColumn("rn", F.row_number().over(w_rank))
        .filter(F.col("rn") <= 3)
        .drop("rn"))
    print(f"\n3. Топ-3 транзакции каждого клиента: {top3.count():,} строк")
    top3.orderBy("client_id", "amount").show(9)

    # ── 4. Разница с предыдущей транзакцией ─────────────────────────────
    one_lag = one.withColumn("prev_amount", F.lag("amount", 1).over(w_time))
    one_lag = one_lag.withColumn("delta",
        F.round(F.col("amount") - F.col("prev_amount"), 2))
    print("\n4. Разница с предыдущей транзакцией:")
    one_lag.select("ts", "amount", "prev_amount", "delta").show(7, truncate=False)

    # ── 5. Sessionization ──────────────────────────────────────────────
    sess = (one
        .withColumn("prev_ts", F.lag("ts", 1).over(w_time))
        .withColumn("gap_sec",
            F.unix_timestamp("ts") - F.unix_timestamp("prev_ts"))
        .withColumn("new_session",
            F.when((F.col("gap_sec") > 3600) | F.col("prev_ts").isNull(), 1)
             .otherwise(0))
        .withColumn("session_id", F.sum("new_session").over(w_time))
    )
    print("\n5. Sessionization (gap > 1 час → новая сессия):")
    print(f"   Найдено сессий: {sess.select('session_id').distinct().count()}")
    sess.select("ts", "amount", "gap_sec", "session_id").show(7, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
