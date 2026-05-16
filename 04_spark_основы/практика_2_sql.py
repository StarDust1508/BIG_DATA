"""
🧩 Практика 2 модуля 04 — Те же задачи через Spark SQL

Цель: убедиться, что DataFrame API и SQL дают одинаковый результат.

Запуск:
    python3 практика_2_sql.py
"""
from pathlib import Path

from pyspark.sql import SparkSession


ROOT = Path(__file__).resolve().parent.parent
CLIENTS = ROOT / "datasets" / "clients.csv"
TX = ROOT / "datasets" / "transactions.csv"


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("M04_P2_SQL")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main() -> None:
    if not CLIENTS.exists() or not TX.exists():
        print("❌ Сначала сгенерируйте датасеты")
        return

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    clients = spark.read.csv(str(CLIENTS), header=True, inferSchema=True)
    tx = spark.read.csv(str(TX), header=True, inferSchema=True)

    clients.createOrReplaceTempView("clients")
    tx.createOrReplaceTempView("tx")

    # 1. Premium клиенты
    print("--- 1. Premium ---")
    spark.sql("""
        SELECT client_id, city, monthly_income
        FROM clients
        WHERE segment = 'premium'
        LIMIT 5
    """).show()

    # 2. По категориям
    print("--- 2. По категориям ---")
    spark.sql("""
        SELECT category,
               COUNT(*) AS n,
               ROUND(SUM(amount), 0) AS total,
               ROUND(AVG(amount), 0) AS avg
        FROM tx
        GROUP BY category
        ORDER BY total DESC
    """).show()

    # 3. По сегментам с join
    print("--- 3. Сегменты + tx ---")
    spark.sql("""
        SELECT c.segment,
               COUNT(*) AS tx_count,
               ROUND(SUM(t.amount), 0) AS total,
               ROUND(AVG(t.amount), 0) AS avg_per_tx
        FROM tx t
        JOIN clients c USING (client_id)
        GROUP BY c.segment
        ORDER BY total DESC
    """).show()

    # 4. Топ-10 клиентов
    print("--- 4. Топ-10 клиентов ---")
    spark.sql("""
        SELECT client_id, ROUND(SUM(amount), 0) AS total
        FROM tx
        GROUP BY client_id
        ORDER BY total DESC
        LIMIT 10
    """).show()

    # 5. Бонус: window-функция
    print("--- 5. Накопленная сумма по клиенту (первые 5 строк одного клиента) ---")
    spark.sql("""
        SELECT client_id, ts, amount,
               SUM(amount) OVER (PARTITION BY client_id ORDER BY ts) AS running_total,
               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY ts) AS rn
        FROM tx
        WHERE client_id = 1
        ORDER BY ts
        LIMIT 5
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
