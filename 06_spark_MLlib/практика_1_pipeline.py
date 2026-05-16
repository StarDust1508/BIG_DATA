"""
🧩 Практика 1 модуля 06 — Pipeline классификации

Сценарий: бинарная классификация «крупная транзакция» (amount > P95)
по признакам клиента и категории. Используется как «hello, ML pipeline».

Используем datasets/clients.csv и datasets/transactions.csv из модуля 02.
Если их нет — запустите 02_python_для_данных/практика_1_eda.py

Запуск:
    python3 практика_1_pipeline.py
"""
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler,
)
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator


ROOT = Path(__file__).resolve().parent.parent
CLIENTS = ROOT / "datasets" / "clients.csv"
TX = ROOT / "datasets" / "transactions.csv"


def main() -> None:
    if not CLIENTS.exists() or not TX.exists():
        print("❌ Сначала сгенерируйте датасеты (модуль 02).")
        return

    spark = (
        SparkSession.builder
        .appName("M06_P1_Pipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ── Данные ─────────────────────────────────────────────────
    clients = spark.read.csv(str(CLIENTS), header=True, inferSchema=True)
    tx = (spark.read.csv(str(TX), header=True, inferSchema=True)
            .withColumn("ts", F.to_timestamp("ts")))

    # Метка: «крупная транзакция» — amount > P95
    p95 = tx.approxQuantile("amount", [0.95], 0.001)[0]
    print(f"P95 of amount: {p95:.2f}")

    joined = (tx.join(clients, "client_id", "inner")
        .withColumn("label", (F.col("amount") > p95).cast("int"))
        .withColumn("hour", F.hour("ts"))
        .select("amount", "hour", "category", "city", "segment",
                "age", "monthly_income", "label"))

    print(f"Total: {joined.count():,}, label=1: {joined.filter('label=1').count():,}")

    train, test = joined.randomSplit([0.8, 0.2], seed=42)
    train = train.cache()

    # ── Pipeline ──────────────────────────────────────────────
    cat_cols = ["category", "city", "segment"]
    num_cols = ["amount", "hour", "age", "monthly_income"]

    indexers = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in cat_cols
    ]
    ohe = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in cat_cols],
        outputCols=[f"{c}_ohe" for c in cat_cols],
    )
    asm = VectorAssembler(
        inputCols=num_cols + [f"{c}_ohe" for c in cat_cols],
        outputCol="features_raw",
    )
    scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                             withMean=False)

    # ── Две модели для сравнения ──────────────────────────────
    models = {
        "LR":
            LogisticRegression(featuresCol="features", labelCol="label",
                                maxIter=100, regParam=0.01),
        "RF":
            RandomForestClassifier(featuresCol="features", labelCol="label",
                                    numTrees=80, maxDepth=8, seed=42),
    }

    ev = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

    for name, estimator in models.items():
        print(f"\n=== Обучаю {name} ===")
        pipeline = Pipeline(stages=[*indexers, ohe, asm, scaler, estimator])
        model = pipeline.fit(train)
        pred = model.transform(test)
        auc = ev.evaluate(pred)
        print(f"  ROC-AUC: {auc:.4f}")

        # Топ-5 признаков для RF
        if name == "RF":
            rf = model.stages[-1]
            print("  Feature importance:")
            feature_meta = pred.schema["features"].metadata.get("ml_attr", {}).get("attrs", {})
            # имена признаков
            names = []
            for grp in ("numeric", "binary"):
                for a in feature_meta.get(grp, []):
                    names.append(a["name"])
            for n, imp in sorted(zip(names, rf.featureImportances.toArray()),
                                 key=lambda x: -x[1])[:5]:
                print(f"    {n:30s}  {imp:.4f}")

    train.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
