"""
🧩 МИНИ-ПРОЕКТ модуля 05 — Анализ корпуса договоров

Юридический сценарий:
  В корпоративной системе хранятся метаданные договоров (стороны,
  предмет, сумма, срок, статус). Юр.отдел хочет регулярную аналитику:
   • Топ-10 крупнейших контрагентов по сумме.
   • Доля просроченных договоров по типам.
   • Среднее время от подписания до закрытия.
   • Аномалии: договора с подозрительно высокой суммой
     по сравнению с медианой данного типа.

Что делает скрипт:
   • Генерирует синтетический датасет 50 000 договоров.
   • Прогоняет полный pipeline и сохраняет результат в Parquet с
     партициями по году.

Запуск:
    python3 мини_проект_контракты.py
"""
import random
from datetime import date, timedelta
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window


ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "datasets"
DATA.mkdir(exist_ok=True)
CSV_PATH = DATA / "contracts.csv"
OUT_DIR = DATA / "contracts_analytics"


CONTRACT_TYPES = [
    "поставка", "услуги", "аренда", "подряд", "лицензия", "консалтинг",
]
STATUSES = ["активный", "закрытый", "расторгнутый", "просроченный"]
COUNTERPARTIES = [f"ООО Контрагент-{i}" for i in range(1, 201)]


def generate_if_needed() -> None:
    if CSV_PATH.exists():
        return
    print("⚙️  Генерирую корпус договоров...")
    random.seed(11)
    rows = ["contract_id,type,counterparty,amount,signed_at,closed_at,status"]
    for i in range(1, 50_001):
        ctype = random.choice(CONTRACT_TYPES)
        cp = random.choices(
            COUNTERPARTIES,
            weights=[5 if j < 20 else 1 for j in range(len(COUNTERPARTIES))],
        )[0]
        # Сумма зависит от типа
        base = {"поставка": 5_000_000, "услуги": 500_000, "аренда": 200_000,
                "подряд": 2_000_000, "лицензия": 100_000, "консалтинг": 800_000}[ctype]
        amount = round(base * random.lognormvariate(0, 0.6), 2)
        if random.random() < 0.01:
            amount *= 20      # аномалия
        signed = date(2023, 1, 1) + timedelta(days=random.randint(0, 1000))
        if random.random() < 0.4:
            closed = signed + timedelta(days=random.randint(30, 800))
        else:
            closed = ""
        status = random.choice(STATUSES)
        rows.append(f"{i},{ctype},{cp},{amount},{signed},{closed},{status}")
    CSV_PATH.write_text("\n".join(rows), encoding="utf-8")
    print(f"  ✅ {CSV_PATH}")


def main() -> None:
    generate_if_needed()

    spark = (
        SparkSession.builder
        .appName("M05_MiniProject_Contracts")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # E
    df = (spark.read.csv(str(CSV_PATH), header=True, inferSchema=True)
            .withColumn("signed_at", F.to_date("signed_at"))
            .withColumn("closed_at",
                F.when(F.col("closed_at") == "", None)
                 .otherwise(F.to_date("closed_at")))
            .withColumn("year_signed", F.year("signed_at"))
         )
    df = df.cache()
    print(f"Загружено: {df.count():,} договоров")

    # ── 1. Топ-10 контрагентов по сумме
    print("\n=== 1. Топ-10 контрагентов по сумме ===")
    (df.groupBy("counterparty")
        .agg(
            F.count("*").alias("n_contracts"),
            F.round(F.sum("amount"), 0).alias("total_amount"),
        )
        .orderBy(F.col("total_amount").desc())
        .limit(10)
        .show(truncate=False))

    # ── 2. Доля просроченных по типу
    print("\n=== 2. Доля просроченных по типу ===")
    (df.groupBy("type")
        .agg(
            F.count("*").alias("n"),
            F.sum(F.when(F.col("status") == "просроченный", 1).otherwise(0)).alias("n_overdue"),
        )
        .withColumn("overdue_share",
            F.round(F.col("n_overdue") / F.col("n"), 3))
        .orderBy(F.col("overdue_share").desc())
        .show())

    # ── 3. Среднее время от подписания до закрытия (где закрыто)
    print("\n=== 3. Среднее время до закрытия (дней) ===")
    closed = df.filter(F.col("closed_at").isNotNull())
    (closed
        .withColumn("days",
            F.datediff(F.col("closed_at"), F.col("signed_at")))
        .groupBy("type")
        .agg(
            F.count("*").alias("n_closed"),
            F.round(F.avg("days"), 1).alias("avg_days"),
            F.expr("percentile_approx(days, 0.5)").alias("median_days"),
        )
        .orderBy("type")
        .show())

    # ── 4. Аномалии: amount > 10× медианы своего типа (window)
    print("\n=== 4. Аномалии по сумме (> 10× медианы своего типа) ===")
    w = Window.partitionBy("type")
    anomalies = (df
        .withColumn("type_median",
            F.expr("percentile_approx(amount, 0.5)").over(w))
        .filter(F.col("amount") > 10 * F.col("type_median"))
    )
    print(f"   найдено аномалий: {anomalies.count()}")
    anomalies.select("contract_id", "type", "counterparty",
                     "amount", "type_median").show(10, truncate=False)

    # L — финальная таблица в Parquet с партиционированием
    print("\n=== Запись результатов ===")
    final = (df
        .withColumn("days_to_close",
            F.when(F.col("closed_at").isNotNull(),
                   F.datediff("closed_at", "signed_at")))
        .withColumn("is_overdue",
            (F.col("status") == "просроченный").cast("int"))
    )
    (final.coalesce(2).write
        .mode("overwrite")
        .partitionBy("year_signed")
        .parquet(str(OUT_DIR)))
    print(f"   💾 {OUT_DIR}/year_signed=YYYY/")
    df.unpersist()
    spark.stop()
    print("\n✅ Мини-проект готов.")


if __name__ == "__main__":
    main()
