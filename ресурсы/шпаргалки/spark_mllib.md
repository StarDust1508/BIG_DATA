# 🤖 Spark MLlib Cheat Sheet

> Pipeline-ориентированный API. С 2.0+ работаем через `pyspark.ml`, **не** `pyspark.mllib` (старый, RDD-based).

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    VectorAssembler, StringIndexer, OneHotEncoder,
    StandardScaler, MinMaxScaler, Imputer,
    Tokenizer, StopWordsRemover, HashingTF, IDF,
)
from pyspark.ml.classification import (
    LogisticRegression, RandomForestClassifier, GBTClassifier,
)
from pyspark.ml.regression import (
    LinearRegression, RandomForestRegressor, GBTRegressor,
)
from pyspark.ml.clustering import KMeans, BisectingKMeans
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator,
    RegressionEvaluator,
    ClusteringEvaluator,
)
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
```

## Базовый pipeline (классификация)

```python
indexer = StringIndexer(inputCol="category", outputCol="cat_idx")
encoder = OneHotEncoder(inputCol="cat_idx", outputCol="cat_ohe")
assembler = VectorAssembler(
    inputCols=["amount", "age", "cat_ohe"],
    outputCol="features",
)
scaler = StandardScaler(inputCol="features", outputCol="scaled")
lr = LogisticRegression(featuresCol="scaled", labelCol="is_fraud")

pipeline = Pipeline(stages=[indexer, encoder, assembler, scaler, lr])

train, test = df.randomSplit([0.8, 0.2], seed=42)
model = pipeline.fit(train)
pred = model.transform(test)
```

## Оценка качества

```python
# Бинарная классификация
ev = BinaryClassificationEvaluator(labelCol="is_fraud", metricName="areaUnderROC")
print("ROC-AUC =", ev.evaluate(pred))

# Многоклассовая
ev = MulticlassClassificationEvaluator(labelCol="y", metricName="f1")
print("F1 =", ev.evaluate(pred))

# Регрессия
ev = RegressionEvaluator(labelCol="y", metricName="rmse")
print("RMSE =", ev.evaluate(pred))
```

## Cross-validation

```python
grid = (
    ParamGridBuilder()
    .addGrid(lr.regParam,       [0.0, 0.01, 0.1])
    .addGrid(lr.elasticNetParam, [0.0, 0.5])
    .build()
)
cv = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=grid,
    evaluator=ev,
    numFolds=5,
    parallelism=2,
)
cv_model = cv.fit(train)
```

## Сохранение / загрузка

```python
model.write().overwrite().save("model_v1")

from pyspark.ml import PipelineModel
loaded = PipelineModel.load("model_v1")
```

## Text-pipeline (NLP)

```python
tok = Tokenizer(inputCol="text", outputCol="tokens")
stop = StopWordsRemover(inputCol="tokens", outputCol="clean")
hash_tf = HashingTF(inputCol="clean", outputCol="raw_tf", numFeatures=10_000)
idf = IDF(inputCol="raw_tf", outputCol="tfidf")
classifier = LogisticRegression(featuresCol="tfidf", labelCol="label")
pipeline = Pipeline(stages=[tok, stop, hash_tf, idf, classifier])
```

## Кластеризация

```python
asm = VectorAssembler(inputCols=["x1", "x2"], outputCol="features")
sc  = StandardScaler(inputCol="features", outputCol="scaled")
km  = KMeans(featuresCol="scaled", k=5, seed=42)
model = Pipeline(stages=[asm, sc, km]).fit(df)

ev = ClusteringEvaluator(featuresCol="scaled")
print("silhouette =", ev.evaluate(model.transform(df)))
```

## Важности признаков

```python
# Для деревьев и ансамблей
rf_stage = model.stages[-1]
for col, imp in zip(["amount", "age"], rf_stage.featureImportances.toArray()):
    print(col, imp)
```

## Когда MLlib, а когда scikit-learn?

| Сценарий | Лучше |
|---|---|
| Данные помещаются в RAM (< 10 ГБ) | scikit-learn |
| Сложные ансамбли (XGBoost, LightGBM) | scikit-learn / xgboost |
| Распределённое обучение на сотнях ГБ | MLlib |
| Inference поверх Spark DataFrame | MLlib (естественнее) |
| Гипер-оптимизация / NN | sklearn + Optuna / PyTorch |

**Лайфхак:** обучить модель в scikit-learn, потом обернуть инференс в pandas-UDF и применить к Spark DataFrame. Часто оптимально.
