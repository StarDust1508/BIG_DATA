# Урок 6.7 — Cross-validation и подбор гиперпараметров

> Один train/test split — это «оценка с одной точки». CV даёт более робастную оценку. Плюс — автоматический подбор лучших параметров.

---

## Часть 1. ParamGridBuilder

```python
from pyspark.ml.tuning import ParamGridBuilder
from pyspark.ml.classification import LogisticRegression

lr = LogisticRegression(featuresCol="features", labelCol="label")

grid = (ParamGridBuilder()
    .addGrid(lr.regParam,        [0.0, 0.01, 0.1])
    .addGrid(lr.elasticNetParam, [0.0, 0.5, 1.0])
    .addGrid(lr.maxIter,         [50, 100])
    .build())

print(f"Гиперпараметрических точек: {len(grid)}")   # 3 * 3 * 2 = 18
```

---

## Часть 2. CrossValidator

```python
from pyspark.ml.tuning import CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

cv = CrossValidator(
    estimator=pipeline,           # или одна модель
    estimatorParamMaps=grid,
    evaluator=BinaryClassificationEvaluator(labelCol="label"),
    numFolds=5,
    parallelism=2,                 # сколько комбинаций обучать параллельно
    seed=42,
)

cv_model = cv.fit(train)
print("Best model:", cv_model.bestModel.stages[-1])
print("Avg metric per param map:")
for params, metric in zip(grid, cv_model.avgMetrics):
    print(f"  {metric:.4f}  {params}")
```

`cv_model.bestModel` — это уже PipelineModel с лучшими параметрами.

---

## Часть 3. TrainValidationSplit — быстрее CV

Если CV слишком долго, и точность оценки не критична:

```python
from pyspark.ml.tuning import TrainValidationSplit

tvs = TrainValidationSplit(
    estimator=pipeline,
    estimatorParamMaps=grid,
    evaluator=BinaryClassificationEvaluator(labelCol="label"),
    trainRatio=0.8,
    parallelism=2,
)
```

Делает **один** train/val split вместо K fold'ов. В K раз быстрее.

---

## Часть 4. Полный пример

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator

asm = VectorAssembler(inputCols=["x1","x2","x3"], outputCol="features_raw")
sc  = StandardScaler(inputCol="features_raw", outputCol="features")
rf  = RandomForestClassifier(featuresCol="features", labelCol="label")

pipeline = Pipeline(stages=[asm, sc, rf])

grid = (ParamGridBuilder()
    .addGrid(rf.numTrees,            [50, 100, 200])
    .addGrid(rf.maxDepth,            [5, 8, 12])
    .addGrid(rf.minInstancesPerNode, [1, 10])
    .build())

cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=grid,
    evaluator=BinaryClassificationEvaluator(labelCol="label"),
    numFolds=3,
    seed=42,
)

train, test = df.randomSplit([0.8, 0.2], seed=42)
cv_model = cv.fit(train)
best = cv_model.bestModel

pred = best.transform(test)
auc = BinaryClassificationEvaluator(labelCol="label").evaluate(pred)
print(f"Best ROC-AUC on test: {auc:.4f}")
```

---

## Часть 5. Что подбирать

| Модель | Ключевые параметры |
|--------|--------------------|
| LR | `regParam`, `elasticNetParam`, `maxIter` |
| RF | `numTrees`, `maxDepth`, `minInstancesPerNode`, `featureSubsetStrategy` |
| GBT | `maxIter`, `maxDepth`, `stepSize`, `subsamplingRate` |
| KMeans | `k` (но это не CV — это elbow/silhouette) |

---

## Часть 6. Стоимость

CV с K=5 и сеткой 3×3×2 = 18 точек → **90 обучений**. Это дорого.

Способы ускорить:
- Меньше fold'ов (3).
- Меньшая сетка.
- `parallelism > 1`.
- TVS вместо CV.
- Random Search вместо Grid Search (вне коробки, через цикл).

---

## Часть 7. Подводные камни

1. **Сетки слишком крупные** — экспоненциальный взрыв.
2. **CV на стратифицированных данных** — Spark не стратифицирует сам. Если классы дисбалансированы, делайте свой split.
3. **Использовать test для CV** — нельзя, иначе утечка. Используем только train.
4. **Feature engineering вне Pipeline** — не сохранится в bestModel, не применится к test одинаково. Всё должно быть **внутри** Pipeline.

---

## ✅ Самопроверка

1. Чем CV отличается от TVS?
2. Что попадает в `cv_model.bestModel`?
3. Какие гиперпараметры стоит подбирать для RF?
4. Что произойдёт, если feature engineering сделать вне Pipeline?
5. Как ускорить CV, если он работает 6 часов?

---

## ▶️ Дальше

[Урок 6.8 — Сохранение и загрузка моделей](./урок_8_save_load.md)
