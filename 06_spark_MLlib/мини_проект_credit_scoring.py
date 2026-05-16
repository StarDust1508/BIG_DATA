"""
🧩 МИНИ-ПРОЕКТ модуля 06 — Credit Scoring

Сценарий: банк хочет автоматическую модель кредитного скоринга.
Мы строим pipeline, оцениваем модель, документируем для AI Act.

Скрипт делает:
   1. Генерирует синтетический датасет заявок и фактических исходов.
   2. Строит RandomForest pipeline с CV.
   3. Считает метрики, feature importance, disparate impact.
   4. Сохраняет модель + рядом MODEL_CARD.md.

Запуск:
    python3 мини_проект_credit_scoring.py
"""
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
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
DATA = ROOT / "datasets"
DATA.mkdir(exist_ok=True)
CSV_PATH = DATA / "credit_applications.csv"
MODEL_DIR = DATA / "models" / "credit_scoring_v1"
MODEL_CARD = MODEL_DIR.parent / "credit_scoring_v1_MODEL_CARD.md"


def generate_if_needed() -> None:
    if CSV_PATH.exists():
        return
    print("⚙️  Генерирую заявки на кредиты...")
    np.random.seed(11)
    N = 30_000
    age = np.clip(np.random.normal(40, 12, N), 21, 70).astype(int)
    income = np.clip(np.random.lognormal(11.0, 0.5, N), 20_000, 1_000_000).astype(int)
    debt_ratio = np.clip(np.random.beta(2, 5, N) * 2, 0, 2).round(3)
    employment_years = np.clip(np.random.exponential(5, N), 0, 40).round(1)
    n_open_loans = np.random.poisson(1.5, N)
    has_property = np.random.binomial(1, 0.4, N)
    region = np.random.choice(
        ["Москва", "СПб", "Регион-A", "Регион-B", "Регион-C"], N,
        p=[0.20, 0.15, 0.30, 0.20, 0.15],
    )
    gender = np.random.choice(["male", "female"], N, p=[0.55, 0.45])
    # Скрытая «правда»: вероятность дефолта зависит от факторов
    logit = (
        -3.0
        - 0.02 * (age - 40)
        - 0.0000015 * income
        + 2.0 * debt_ratio
        - 0.05 * employment_years
        + 0.3 * n_open_loans
        - 0.5 * has_property
        + np.random.normal(0, 0.5, N)
    )
    p_default = 1 / (1 + np.exp(-logit))
    default = (np.random.rand(N) < p_default).astype(int)

    df = pd.DataFrame({
        "age": age, "income": income, "debt_ratio": debt_ratio,
        "employment_years": employment_years, "n_open_loans": n_open_loans,
        "has_property": has_property, "region": region, "gender": gender,
        "default": default,
    })
    df.to_csv(CSV_PATH, index=False)
    print(f"  ✅ {CSV_PATH}  (default rate = {df.default.mean():.3f})")


def main() -> None:
    generate_if_needed()

    spark = (
        SparkSession.builder
        .appName("M06_MiniProject_CreditScoring")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = (spark.read.csv(str(CSV_PATH), header=True, inferSchema=True)
            .withColumnRenamed("default", "label"))
    train, test = df.randomSplit([0.8, 0.2], seed=42)
    print(f"train={train.count():,}  test={test.count():,}  "
          f"default rate train={train.filter('label=1').count()/train.count():.3f}")

    cat_cols = ["region", "gender"]
    num_cols = ["age", "income", "debt_ratio", "employment_years",
                "n_open_loans", "has_property"]

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
        .addGrid(rf.numTrees, [50, 100])
        .addGrid(rf.maxDepth, [6, 10])
        .build())
    ev = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderROC")

    cv = CrossValidator(estimator=pipeline, estimatorParamMaps=grid,
                          evaluator=ev, numFolds=3, parallelism=2, seed=42)
    print("\n⏳ Запускаю CV...")
    cv_model = cv.fit(train)
    best = cv_model.bestModel

    pred = best.transform(test)
    auc = ev.evaluate(pred)

    # PR-AUC
    pr_ev = BinaryClassificationEvaluator(labelCol="label", metricName="areaUnderPR")
    pr_auc = pr_ev.evaluate(pred)

    print(f"\n🏆 ROC-AUC = {auc:.4f}")
    print(f"   PR-AUC  = {pr_auc:.4f}")

    # Confusion matrix
    cm = pred.groupBy("label", "prediction").count().orderBy("label", "prediction")
    print("\nConfusion matrix:")
    cm.show()

    # Feature importance
    rf_best = best.stages[-1]
    importances = rf_best.featureImportances.toArray()
    feat_names = num_cols + [f"{c}_ohe[?]" for c in cat_cols]   # упрощённо
    print("\nTop-5 importance:")
    for name, imp in sorted(zip(feat_names, importances[:len(feat_names)]),
                             key=lambda x: -x[1])[:5]:
        print(f"  {name:30s}  {imp:.4f}")

    # Disparate impact по gender
    def positive_rate(df, gender_value):
        sub = df.filter(F.col("gender") == gender_value)
        n = sub.count()
        if n == 0:
            return 0
        return sub.filter(F.col("prediction") == 1).count() / n

    male_rate = positive_rate(pred, "male")
    female_rate = positive_rate(pred, "female")
    di = min(male_rate, female_rate) / max(male_rate, female_rate) if max(male_rate, female_rate) > 0 else 0
    print(f"\nDisparate Impact (gender): {di:.3f}  (норма 0.8...1.25)")
    print(f"  rate(male={male_rate:.3f})   rate(female={female_rate:.3f})")

    # Сохранение
    MODEL_DIR.parent.mkdir(parents=True, exist_ok=True)
    best.write().overwrite().save(str(MODEL_DIR))
    print(f"\n💾 Модель: {MODEL_DIR}")

    # Model Card
    card = f"""# MODEL CARD — Credit Scoring v1.0

## 1. Назначение
Бинарная классификация: вероятность дефолта по заявке на кредит.

## 2. Архитектура
- Алгоритм: RandomForestClassifier (после CV grid: numTrees, maxDepth).
- Spark MLlib.
- Pipeline: StringIndexer → OHE → VectorAssembler → RF.

## 3. Данные обучения
- Размер: {train.count():,} строк.
- Признаки: age, income, debt_ratio, employment_years, n_open_loans,
  has_property, region (категория), gender (категория).
- Источник: синтетика для учебных целей.
- Дата: {datetime.utcnow().isoformat()}

## 4. Метрики на holdout
- ROC-AUC: {auc:.4f}
- PR-AUC: {pr_auc:.4f}

## 5. Disparate Impact (gender)
- DI = {di:.3f}  (норма 0.8...1.25)
- rate(male) = {male_rate:.3f}
- rate(female) = {female_rate:.3f}

## 6. Ограничения
- НЕ применять к заявителям младше 21 года.
- НЕ применять при отсутствии данных о region/gender.
- При сдвиге распределения дохода (например, инфляция) — переобучение.
- Модель — НЕ единственное решение: обязательная процедура human review
  для всех отказов по GDPR Art. 22.

## 7. Юр.основа обработки
- 152-ФЗ: согласие клиента в форме заявки.
- GDPR (если применимо): legitimate interest (банк) + право на возражение.
- AI Act: категория «высокий риск» (кредитный скоринг).

## 8. Контакты
- Owner: ml-team@company.com
- Data Steward: dpo@company.com
- Last review: {datetime.utcnow().date().isoformat()}
"""
    MODEL_CARD.write_text(card, encoding="utf-8")
    print(f"📄 Model Card: {MODEL_CARD}")

    spark.stop()


if __name__ == "__main__":
    main()
