"""
🧩 Практика 3 модуля 06 — Сегментация клиентов через KMeans

Сценарий: «найти естественные сегменты клиентов» по их поведению.
Признаки клиента: возраст, доход, кол-во транзакций, средний чек,
число различных категорий.

Запуск:
    python3 практика_3_clustering.py
"""
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


ROOT = Path(__file__).resolve().parent.parent
CLIENTS = ROOT / "datasets" / "clients.csv"
TX = ROOT / "datasets" / "transactions.csv"


def main() -> None:
    if not CLIENTS.exists() or not TX.exists():
        print("❌ Сначала сгенерируйте датасеты (модуль 02).")
        return

    spark = (
        SparkSession.builder
        .appName("M06_P3_Clustering")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    clients = spark.read.csv(str(CLIENTS), header=True, inferSchema=True)
    tx = spark.read.csv(str(TX), header=True, inferSchema=True)

    # Признаки на уровне клиента
    client_aggr = (tx.groupBy("client_id").agg(
            F.count("*").alias("tx_count"),
            F.round(F.avg("amount"), 2).alias("avg_tx_amount"),
            F.countDistinct("category").alias("n_categories"),
        )
        .join(clients.select("client_id", "age", "monthly_income"), "client_id")
        .na.drop()
    )

    print(f"Клиентов с признаками: {client_aggr.count():,}")

    feature_cols = ["age", "monthly_income", "tx_count", "avg_tx_amount", "n_categories"]
    asm = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    sc  = StandardScaler(inputCol="features_raw", outputCol="features")

    # ── Подбор k через elbow + silhouette ─────────────────────
    print("\nПодбор k:")
    ev = ClusteringEvaluator(featuresCol="features", metricName="silhouette")
    best_k = None
    best_sil = -1
    for k in [2, 3, 4, 5, 6, 7, 8]:
        km = KMeans(featuresCol="features", k=k, seed=42)
        model = Pipeline(stages=[asm, sc, km]).fit(client_aggr)
        pred = model.transform(client_aggr)
        sil = ev.evaluate(pred)
        cost = model.stages[-1].summary.trainingCost
        print(f"  k={k}  silhouette={sil:.4f}  cost={cost:.0f}")
        if sil > best_sil:
            best_sil, best_k, best_model, best_pred = sil, k, model, pred

    print(f"\n🏆 Лучший k = {best_k}  (silhouette = {best_sil:.4f})")

    # Описание сегментов
    print("\n=== Профили сегментов ===")
    profiles = (best_pred.groupBy("prediction").agg(
            F.count("*").alias("n"),
            F.round(F.avg("age"), 1).alias("age"),
            F.round(F.avg("monthly_income"), 0).alias("income"),
            F.round(F.avg("tx_count"), 1).alias("tx_count"),
            F.round(F.avg("avg_tx_amount"), 0).alias("avg_tx"),
            F.round(F.avg("n_categories"), 1).alias("n_cat"),
        )
        .orderBy("prediction")
    )
    profiles.show()

    print(
        """
🧠 Что увидели:
  • Несколько устойчивых поведенческих сегментов.
  • На их основе маркетинг может строить кастомизированные кампании.
  • На основе ML-сегментации можно автоматически (а значит, с учётом
    AI Act / GDPR Art. 22) принимать «значимые» решения — но это
    требует прозрачности и опции human review.
        """
    )
    spark.stop()


if __name__ == "__main__":
    main()
