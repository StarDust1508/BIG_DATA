"""
🧩 Практика 2 модуля 06 — Cross-validation и подбор гиперпараметров

Запускает CrossValidator на той же задаче (label = amount > P95),
с сеткой по нескольким параметрам RandomForestClassifier.

⚠️ Может занять несколько минут — на маленькой машине уменьшите grid.

Запуск:
    python3 практика_2_grid_search.py
"""
from pathlib import Path

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler,
)
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder


ROOT = Path(__file__).resolve().parent.parent
CLIENTS = ROOT / "datasets" / "clients.csv"
TX = ROOT / "datasets" / "transactions.csv"


def main() -> None:
    if not CLIENTS.exists() or not TX.exists():
        print("❌ Сначала сгенерируйте датасеты (модуль 02).")
        return

    spark = (
        SparkSession.builder
        .appName("M06_P2_Grid")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    clients = spark.read.csv(str(CLIENTS), header=True, inferSchema=True)
    tx = (spark.read.csv(str(TX), header=True, inferSchema=True)
            .withColumn("ts", F.to_timestamp("ts")))

    p95 = tx.approxQuantile("amount", [0.95], 0.001)[0]

    df = (tx.join(clients, "client_id")
        .withColumn("label", (F.col("amount") > p95).cast("int"))
        .withColumn("hour", F.hour("ts"))
        .select("amount", "hour", "category", "city", "segment",
                "age", "monthly_income", "label"))

    train, test = df.randomSplit([0.8, 0.2], seed=42)

    cat_cols = ["category", "city", "segment"]
    num_cols = ["amount", "hour", "age", "monthly_income"]

    indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx",
                                handleInvalid="keep") for c in cat_cols]
    ohe = OneHotEncoder(inputCols=[f"{c}_idx" for c in cat_cols],
                         outputCols=[f"{c}_ohe" for c in cat_cols])
    asm = VectorAssembler(
        inputCols=num_cols + [f"{c}_ohe" for c in cat_cols],
        outputCol="features",
    )
    rf = RandomForestClassifier(featuresCol="features", labelCol="label",
                                  seed=42)

    pipeline = Pipeline(stages=[*indexers, ohe, asm, rf])

    grid = (ParamGridBuilder()
        .addGrid(rf.numTrees,            [50, 100])
        .addGrid(rf.maxDepth,            [5, 8])
        .addGrid(rf.minInstancesPerNode, [1, 10])
        .build())

    ev = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=grid,
        evaluator=ev,
        numFolds=3,
        parallelism=2,
        seed=42,
    )

    print(f"Запускаю CV: {len(grid)} комбинаций × 3 fold = {len(grid)*3} обучений")
    cv_model = cv.fit(train)

    print("\nРезультаты:")
    for params, score in sorted(zip(grid, cv_model.avgMetrics),
                                 key=lambda x: -x[1]):
        readable = {p.name: v for p, v in params.items()}
        print(f"  AUC={score:.4f}  {readable}")

    best = cv_model.bestModel
    test_auc = ev.evaluate(best.transform(test))
    print(f"\n🏆 Best AUC on test: {test_auc:.4f}")

    # Сохранение
    out = ROOT / "datasets" / "models" / "best_rf_v1"
    best.write().overwrite().save(str(out))
    print(f"💾 Модель сохранена: {out}")

    spark.stop()


if __name__ == "__main__":
    main()
