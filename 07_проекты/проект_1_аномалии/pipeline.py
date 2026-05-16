"""
Проект 1 — Детекция аномалий в транзакциях
============================================

Сквозной pipeline: load → clean → features → pseudonymize → train → score.
Производит Model Card.

Использует datasets/clients.csv и datasets/transactions.csv из модуля 02.

Запуск:
    python3 pipeline.py --run-date 2026-05-15
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator


# ─── paths ─────────────────────────────────────────────────────────────────
HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
DATA = ROOT / "datasets"

CLIENTS_CSV = DATA / "clients.csv"
TX_CSV = DATA / "transactions.csv"
OUT_DIR = DATA / "anomalies_output"
MODEL_DIR = DATA / "models" / "anomaly_v1"
MODEL_CARD = HERE / "MODEL_CARD.md"
AUDIT_LOG = HERE / "audit.log"


# ─── logging ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(AUDIT_LOG, mode="a", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("anomaly-pipeline")


# ─── spark ─────────────────────────────────────────────────────────────────
def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Project1_Anomalies")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


# ─── 01 load ───────────────────────────────────────────────────────────────
def load(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    log.info("Шаг 01: загрузка данных")
    if not CLIENTS_CSV.exists() or not TX_CSV.exists():
        raise FileNotFoundError(
            "Нет clients.csv/transactions.csv. Запустите модуль 02:\n"
            "  python3 02_python_для_данных/практика_1_eda.py"
        )
    clients = spark.read.csv(str(CLIENTS_CSV), header=True, inferSchema=True)
    tx = (spark.read.csv(str(TX_CSV), header=True, inferSchema=True)
            .withColumn("ts", F.to_timestamp("ts")))
    log.info(f"  клиентов={clients.count():,}  транзакций={tx.count():,}")
    return clients, tx


# ─── 02 clean ──────────────────────────────────────────────────────────────
def clean(tx: DataFrame) -> DataFrame:
    log.info("Шаг 02: очистка данных")
    out = (tx
        .dropDuplicates(["client_id", "ts"])
        .filter(F.col("client_id").isNotNull())
        .filter(F.col("amount").isNotNull())
        .filter(F.col("amount") >= 0)
        .withColumn("category", F.lower(F.trim(F.col("category"))))
    )
    log.info(f"  после очистки: {out.count():,}")
    return out


# ─── 03 features ────────────────────────────────────────────────────────────
def make_features(tx: DataFrame, clients: DataFrame) -> tuple[DataFrame, list[str]]:
    """Считаем фичи + размечаем 'is_anomaly' эвристикой как «грязную» метку.

    В реальном проекте метку даёт human review / правила Compliance.
    Здесь для учебной задачи: транзакции > 100× медианы клиента считаем аномальными.
    """
    log.info("Шаг 03: формирование признаков")
    # Метка: amount > 100 * медиана клиента → аномалия
    w = Window.partitionBy("client_id")
    tx = tx.withColumn("client_median",
        F.expr("percentile_approx(amount, 0.5)").over(w))
    tx = tx.withColumn("label",
        F.when(F.col("amount") > 100 * F.col("client_median"), 1).otherwise(0))

    # Фичи: характеристики клиента + параметры транзакции
    base = (tx
        .join(F.broadcast(clients.select("client_id", "segment", "city",
                                          "monthly_income", "age")),
              "client_id", "left")
        .withColumn("hour", F.hour("ts"))
        .withColumn("dow", F.dayofweek("ts"))
        .withColumn("amount_vs_income",
            F.col("amount") / F.col("monthly_income"))
        .withColumn("amount_vs_client_median",
            F.col("amount") / (F.col("client_median") + 1))
    )

    feature_cols = ["amount", "hour", "dow", "age",
                    "monthly_income", "amount_vs_income",
                    "amount_vs_client_median"]
    log.info(f"  признаки: {feature_cols}")
    log.info(f"  меток label=1 (аномалий): {base.filter('label=1').count():,}")
    return base, feature_cols


# ─── 04 pseudonymize ───────────────────────────────────────────────────────
def pseudonymize(df: DataFrame) -> DataFrame:
    """Заменяет client_id на токен."""
    log.info("Шаг 04: псевдонимизация ПДн")
    salt = os.environ.get("PSEUDO_SALT", "dev-salt-CHANGE-ME")
    if salt == "dev-salt-CHANGE-ME":
        log.warning("⚠️  PSEUDO_SALT не задана. В production используйте Secret Manager.")
    out = (df
        .withColumn("client_token",
            F.substring(F.sha2(F.concat(F.lit(salt),
                                          F.col("client_id").cast("string")),
                                 256), 1, 16))
        .drop("client_id"))
    return out


# ─── 05 train ──────────────────────────────────────────────────────────────
def train(df: DataFrame, feature_cols: list[str]) -> tuple:
    log.info("Шаг 05: обучение модели")
    train_, test_ = df.randomSplit([0.8, 0.2], seed=42)

    asm = VectorAssembler(inputCols=feature_cols, outputCol="features_raw")
    sc = StandardScaler(inputCol="features_raw", outputCol="features")
    rf = RandomForestClassifier(
        featuresCol="features", labelCol="label",
        numTrees=100, maxDepth=10, seed=42,
    )
    model = Pipeline(stages=[asm, sc, rf]).fit(train_)

    pred = model.transform(test_)
    auc_ev = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")
    pr_ev  = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
    auc = auc_ev.evaluate(pred)
    pr_auc = pr_ev.evaluate(pred)

    log.info(f"  ROC-AUC = {auc:.4f}")
    log.info(f"  PR-AUC  = {pr_auc:.4f}")

    return model, pred, {"roc_auc": auc, "pr_auc": pr_auc,
                          "n_train": train_.count(), "n_test": test_.count()}


# ─── 06 score ──────────────────────────────────────────────────────────────
def score(model, df: DataFrame, run_date: str) -> None:
    log.info("Шаг 06: инференс на свежих данных")
    pred = model.transform(df)
    out = pred.select(
        "client_token", "ts", "amount", "category", "label",
        "prediction",
        F.col("probability").alias("scores"),
    ).withColumn("dt", F.lit(run_date))

    (out.coalesce(2).write
        .mode("overwrite")
        .partitionBy("dt")
        .parquet(str(OUT_DIR)))
    log.info(f"  💾 записано: {OUT_DIR}/dt={run_date}")


# ─── Model Card ─────────────────────────────────────────────────────────────
def write_model_card(metrics: dict, model, feature_cols: list[str], run_date: str):
    rf = model.stages[-1]
    importances = list(zip(feature_cols, rf.featureImportances.toArray()))
    importances.sort(key=lambda x: -x[1])

    md = f"""# MODEL CARD — Anomaly Detection v1.0

## 1. Назначение
Бинарная классификация банковских транзакций на «подозрительные»
(требует ручной проверки) и «нормальные».

## 2. Архитектура
- Алгоритм: RandomForestClassifier (numTrees=100, maxDepth=10)
- Фреймворк: PySpark MLlib 3.5
- Pipeline: VectorAssembler → StandardScaler → RF

## 3. Данные обучения
- Источник: внутренние транзакции (datasets/transactions.csv)
- Псевдонимизация: client_id заменён на SHA-256 токен с солью
- Размер train: {metrics["n_train"]:,}
- Размер test:  {metrics["n_test"]:,}
- Дата запуска: {run_date}
- Время обучения: {datetime.utcnow().isoformat()}

## 4. Признаки
{chr(10).join(f"- {f}" for f in feature_cols)}

## 5. Метки
Учебная эвристика: amount > 100 × медиана клиента.
**В production** метки даёт human review Compliance-отдела.

## 6. Метрики
- ROC-AUC: {metrics["roc_auc"]:.4f}
- PR-AUC:  {metrics["pr_auc"]:.4f}

## 7. Feature importance (топ-5)
| # | Признак | Importance |
|---|---------|-----------|
"""
    for i, (name, imp) in enumerate(importances[:5], 1):
        md += f"| {i} | {name} | {imp:.4f} |\n"

    md += """

## 8. Ограничения
- Учебная модель: метка построена эвристикой, в проде нужны метки от Compliance.
- Не использовалась стратификация при сплите.
- Дисбаланс классов: метрика PR-AUC обычно низкая, это нормально для редких событий.
- Disparate Impact по сегментам клиентов не считался — добавить перед production.

## 9. Юр.основа обработки
- 152-ФЗ: согласие клиента на антифрод (обычная статья банковского договора).
- GDPR (если применимо): legitimate interest (банк имеет право защищать от фрода).
- AI Act категория: **высокий риск** (анти-фрод → значимые последствия для клиента).

## 10. Процедура human review
Подозрительные транзакции **не блокируются автоматически**.
Они уходят аналитику Compliance с метаданными:
- транзакция (псевдонимизированная)
- топ-признаки модели для этого решения
- сравнение со средним по сегменту

Аналитик принимает решение в течение 24 часов.

## 11. Версионирование
- Версия: 1.0.0
- Тренирован: {ts}
- Owner: ml-team@bank.example
- DPO согласовал: __ДА (поставить вручную)__

## 12. Что улучшить в v2
- Заменить эвристическую метку на реальные пометки от Compliance.
- Добавить временные фичи (rolling sum за 7 дней).
- Сделать стратифицированный split.
- Изолированный лес (Isolation Forest) как альтернатива.
- Подобрать threshold через precision/recall trade-off.
""".replace("{ts}", datetime.utcnow().isoformat())

    MODEL_CARD.write_text(md, encoding="utf-8")
    log.info(f"  📄 MODEL_CARD.md обновлён")


# ─── main ──────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-date", default="2026-05-15")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info(f"Старт pipeline: run_date={args.run_date}, пользователь={os.getenv('USER','?')}")
    log.info("=" * 60)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    clients, tx = load(spark)
    tx = clean(tx).cache()

    features, feature_cols = make_features(tx, clients)
    features = pseudonymize(features).cache()

    model, pred_test, metrics = train(features, feature_cols)

    model.write().overwrite().save(str(MODEL_DIR))
    log.info(f"💾 модель: {MODEL_DIR}")

    score(model, features, args.run_date)
    write_model_card(metrics, model, feature_cols, args.run_date)

    tx.unpersist()
    features.unpersist()
    spark.stop()
    log.info("✅ Pipeline завершён.")


if __name__ == "__main__":
    main()
