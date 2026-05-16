"""
🧩 Практика 3 модуля 04 — Бенчмарк Pandas vs Spark

Сравнивает, сколько занимает одинаковая агрегация на:
  • Pandas
  • PySpark (local[*])

на синтетическом датасете в 10 миллионов строк.

ВАЖНО: бенчмарк ПЕРВОГО запуска включает старт JVM (5–15 сек).
Лучше запустить пару раз, второй прогон будет «честным».

Запуск:
    python3 практика_3_pandas_vs_spark.py
"""
import time
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "datasets"
DATA.mkdir(exist_ok=True)
CSV_PATH = DATA / "bench_10m.csv"
PARQUET_PATH = DATA / "bench_10m.parquet"
N = 10_000_000


def generate_if_needed() -> None:
    if PARQUET_PATH.exists():
        return
    print(f"⚙️  Генерирую датасет ({N:,} строк)...")
    np.random.seed(0)
    df = pd.DataFrame({
        "client_id": np.random.randint(1, 100_000, N, dtype=np.int32),
        "amount": np.random.exponential(5000, N).astype(np.float32),
        "category": np.random.choice(
            ["payment", "transfer", "withdrawal", "deposit"], N
        ),
        "city": np.random.choice(
            ["Moscow", "SPb", "Kazan", "Novosib", "Yekat"], N
        ),
    })
    df.to_parquet(PARQUET_PATH, compression="snappy")
    df.head(50_000).to_csv(CSV_PATH, index=False)
    print(f"   {PARQUET_PATH} ({PARQUET_PATH.stat().st_size / 1024**2:.0f} МБ)")


def timer(label: str):
    """Decorator-like helper."""
    class T:
        def __enter__(self_):
            self_.t0 = time.perf_counter()
            return self_
        def __exit__(self_, *a):
            dt = time.perf_counter() - self_.t0
            print(f"  ⏱  {label:35s} {dt:7.2f} с")
    return T()


def bench_pandas() -> None:
    print("\n=== Pandas ===")
    with timer("read parquet"):
        df = pd.read_parquet(PARQUET_PATH)
    with timer("filter + count"):
        n = df[df.amount > 1000].shape[0]
        print(f"     rows after filter: {n:,}")
    with timer("groupBy.sum"):
        r = df.groupby("category")["amount"].sum()
        print("     ", r.to_dict())
    with timer("groupBy.agg multiple"):
        r = df.groupby(["city", "category"]).agg(
            n=("amount", "count"),
            total=("amount", "sum"),
        )
        print(f"     groups: {len(r)}")


def bench_spark() -> None:
    print("\n=== PySpark (local[*]) ===")
    with timer("SparkSession startup"):
        spark = (
            SparkSession.builder
            .appName("Bench")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.ui.showConsoleProgress", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
    with timer("read parquet (lazy)"):
        df = spark.read.parquet(str(PARQUET_PATH))
    with timer("filter + count"):
        n = df.filter(F.col("amount") > 1000).count()
        print(f"     rows after filter: {n:,}")
    with timer("groupBy.sum"):
        df.groupBy("category").agg(F.sum("amount").alias("total")).collect()
    with timer("groupBy.agg multiple"):
        df.groupBy("city", "category").agg(
            F.count("*").alias("n"),
            F.sum("amount").alias("total"),
        ).collect()
    spark.stop()


def main() -> None:
    generate_if_needed()
    bench_pandas()
    bench_spark()
    print(
        """
🧠 Что увидите:
  • На 10М строк Pandas обычно быстрее на каждой операции в 2–5×.
  • Spark тратит существенное время на startup JVM (5–15 секунд).
  • Если увеличить датасет до 100М+ — Pandas начнёт «задыхаться» по памяти,
    а Spark будет вести себя стабильно.
  • Это и есть «точка перелома», о которой мы говорили в уроке 4.6.
        """
    )


if __name__ == "__main__":
    main()
