"""
🧩 Практика 2 модуля 05 — Joins на банковских данных

Делаем 5 join'ов разных типов и сравниваем их по плану и результату.

Запуск:
    python3 практика_2_joins.py
"""
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F


ROOT = Path(__file__).resolve().parent.parent
CLIENTS = ROOT / "datasets" / "clients.csv"
TX = ROOT / "datasets" / "transactions.csv"


def main() -> None:
    if not CLIENTS.exists() or not TX.exists():
        print("❌ Сначала сгенерируйте датасеты в модуле 02.")
        return

    spark = (
        SparkSession.builder
        .appName("M05_P2_Joins")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    clients = spark.read.csv(str(CLIENTS), header=True, inferSchema=True)
    tx = spark.read.csv(str(TX), header=True, inferSchema=True)

    print(f"clients: {clients.count():,}   tx: {tx.count():,}")

    # ── 1. Inner join ────────────────────────────────────────────────────
    print("\n=== 1. INNER join (default) ===")
    inner = tx.join(clients, "client_id")
    print(f"   tx: {tx.count():,}, inner: {inner.count():,}")

    # ── 2. Left join ─────────────────────────────────────────────────────
    print("\n=== 2. LEFT join ===")
    left = tx.join(clients, "client_id", "left")
    print(f"   left: {left.count():,}")
    print(f"   с пустым сегментом (нет в clients): "
          f"{left.filter(F.col('segment').isNull()).count()}")

    # ── 3. Left semi (фильтр) ───────────────────────────────────────────
    print("\n=== 3. LEFT SEMI (фильтр по существованию) ===")
    semi = tx.join(clients, "client_id", "left_semi")
    print(f"   semi: {semi.count():,}  (колонки clients НЕ присоединены)")
    semi.printSchema()

    # ── 4. Left anti (чего нет) ─────────────────────────────────────────
    print("\n=== 4. LEFT ANTI (транзакции «осиротевших» клиентов) ===")
    anti = tx.join(clients, "client_id", "left_anti")
    print(f"   anti: {anti.count():,}")
    anti.show(5)

    # ── 5. Broadcast join + explain ─────────────────────────────────────
    print("\n=== 5. BROADCAST join + explain ===")
    bc = tx.join(F.broadcast(clients), "client_id")
    print(f"   результат: {bc.count():,}")
    print("План:")
    bc.explain(False)

    # ── 6. Самостоятельно: anti-join даёт другой ответ, чем !isin?
    print("\n=== 6. Anti-join vs ~isin ===")
    all_ids = [r.client_id for r in clients.select("client_id").collect()]
    n_via_isin = tx.filter(~F.col("client_id").isin(all_ids)).count()
    n_via_anti = anti.count()
    print(f"   via ~isin:    {n_via_isin}")
    print(f"   via anti-join: {n_via_anti}")
    print("   (должно совпадать; anti-join работает на больших данных,")
    print("    а ~isin перетянет весь список на драйвер)")

    spark.stop()


if __name__ == "__main__":
    main()
