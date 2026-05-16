# Урок 6.1 — Архитектура Spark MLlib

> Spark MLlib — модуль для ML на распределённых данных. После этого урока у вас в голове ясная модель: **Pipeline, Transformer, Estimator, Model**.

📖 Шпаргалка: [ресурсы/шпаргалки/spark_mllib.md](../ресурсы/шпаргалки/spark_mllib.md).

---

## Часть 1. Зачем вообще MLlib

Когда данных мало (< 5 ГБ), удобнее `scikit-learn` + `pandas`: огромная экосистема, тонкий тюнинг, объяснимость.

MLlib нужен, когда:
- Данные не помещаются в RAM одной машины.
- ML-этап встроен в распределённый ETL-pipeline.
- Хочется единого стэка от данных до модели без перегонки.

⚠️ Важно: для **большинства** задач даже на «больших» данных правильнее обучить модель в scikit-learn на сэмпле (или с использованием LightGBM/XGBoost-distributed), а **инференс** делать через Spark с pandas-UDF. Часто это оптимальнее MLlib.

---

## Часть 2. Два API: `pyspark.mllib` и `pyspark.ml`

| API | Каков | Когда |
|-----|-------|-------|
| `pyspark.mllib` | Старый, на RDD | **НЕ использовать** в новом коде |
| `pyspark.ml`  | Новый, на DataFrame, с Pipeline | **Стандарт с 2.0+** |

Всё, что ниже, — про `pyspark.ml`.

---

## Часть 3. Четыре ключевых абстракции

```
        ┌───────────────────────────────────────┐
        │ DataFrame (вход)                       │
        │   "features" | "label"                │
        └───────────────┬───────────────────────┘
                        │
              ┌─────────▼──────────┐
              │ Transformer        │
              │   df → df'         │
              │   (StringIndexer,  │
              │    VectorAssembler,│
              │    Scaler,         │
              │    Tokenizer)      │
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │ Estimator          │
              │   df → fit() →     │
              │   Model            │
              │   (LogisticRegr,   │
              │    RandomForest,   │
              │    KMeans)         │
              └─────────┬──────────┘
                        │
              ┌─────────▼──────────┐
              │ Model (= обученный │
              │  Transformer)      │
              │   df → df' с пред-  │
              │   сказанием        │
              └────────────────────┘
```

### Transformer
Чистая функция «DataFrame → DataFrame». Метод `.transform(df)`.

Примеры: `StringIndexer` (мапит строки в числа), `VectorAssembler` (собирает колонки в вектор), `StandardScaler`.

### Estimator
Имеет метод `.fit(df) → Model`. Сам по себе DataFrame не трансформирует, нужна тренировка.

Примеры: `LogisticRegression`, `RandomForestClassifier`, `KMeans`.

### Model
Это уже обученный Transformer. `.transform(df)` даёт предсказания.

### Pipeline
Цепочка Transformer'ов и Estimator'ов. Сам по себе — Estimator. `pipeline.fit(df) → PipelineModel`.

---

## Часть 4. Минимальный пример

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression

# Данные с двумя признаками и меткой
df = spark.createDataFrame([
    (1.0, 5.0, 0), (2.0, 4.0, 0), (3.0, 6.0, 1), (4.0, 7.0, 1)
], ["x1", "x2", "label"])

# 1. Собрать признаки в один вектор
assembler = VectorAssembler(inputCols=["x1", "x2"], outputCol="features_raw")

# 2. Масштабировать
scaler = StandardScaler(inputCol="features_raw", outputCol="features")

# 3. Модель
lr = LogisticRegression(featuresCol="features", labelCol="label")

# 4. Pipeline
pipeline = Pipeline(stages=[assembler, scaler, lr])

# 5. Обучение
model = pipeline.fit(df)

# 6. Предсказание
predictions = model.transform(df)
predictions.select("label", "prediction", "probability").show()
```

Те же 6 шагов вы будете писать снова и снова — это «hello, world» MLlib.

---

## Часть 5. Зачем именно Pipeline

```python
# ❌ Без Pipeline — легко перепутать порядок и забыть применить на test
df_train = assembler.transform(df_train)
df_train = scaler.fit(df_train).transform(df_train)   # тренируем scaler на train
df_train = lr.fit(df_train)   # вместо вызова метода

# ✅ С Pipeline — один объект, один fit
model = Pipeline(stages=[assembler, scaler, lr]).fit(df_train)
# и применить к новым данным
predictions = model.transform(df_new)
```

Pipeline:
- запоминает порядок шагов;
- запоминает «учёные» параметры (scaler помнит mean/std с train);
- сериализуется и загружается одной командой;
- может быть обёрнут в CrossValidator для тюнинга.

---

## Часть 6. Типы Pipeline'ов

### Категориальные данные → числа
```
[StringIndexer] → [OneHotEncoder] → [VectorAssembler]
```

### Числовые признаки
```
[VectorAssembler] → [Imputer] → [Scaler/Normalizer]
```

### Полный pipeline для смешанных
```
StringIndexer (для каждой cat)
OneHotEncoder (для каждой cat)
VectorAssembler (все cat + num → features_raw)
Scaler (features_raw → features)
Estimator (LogisticRegression / RF / GBT)
```

В уроке 6.2 — конкретно по каждому Transformer'у.

---

## Часть 7. Сравнение со scikit-learn

| scikit-learn | Spark MLlib |
|--------------|-------------|
| `Pipeline` | `Pipeline` |
| `ColumnTransformer` | Несколько Transformer'ов перед VectorAssembler |
| `LogisticRegression` | `LogisticRegression` |
| `RandomForestClassifier` | `RandomForestClassifier` |
| `GridSearchCV` | `CrossValidator` + `ParamGridBuilder` |
| `joblib.dump` | `model.write().save()` |
| `predict(X)` | `model.transform(df)` |

Идеи те же. Синтаксис чуть другой.

---

## Часть 8. Когда MLlib vs scikit-learn

| Сценарий | Лучше |
|----------|-------|
| < 5 ГБ, на ноутбуке | scikit-learn |
| 5–100 ГБ, мощная машина | scikit-learn + Polars / Dask |
| > 100 ГБ или нужна распределённая тренировка | MLlib |
| ML-этап встроен в Spark ETL | MLlib (без перегонки) |
| Нужен XGBoost / LightGBM | scikit-learn (или XGBoost4j-Spark) |
| Глубокое обучение | PyTorch / TensorFlow, не MLlib |

В курсе мы изучаем MLlib, потому что это **тема модуля**. В реальных проектах смотрите на размер задачи.

---

## Часть 9. Юридический угол

ML-модель на ПДн — это **обработка ПДн** в смысле 152-ФЗ/GDPR. Применимо:

- Цель обработки должна быть зафиксирована до тренировки.
- Тренировка на анонимизированных или псевдонимизированных данных предпочтительнее.
- Должна быть **возможность объяснить** отдельное предсказание (Art. 22 GDPR + AI Act).
- Версии модели + версии данных + метрики должны фиксироваться (для воспроизводимости и потенциальных судебных разбирательств).

В уроке 6.9 разберём это детально.

---

## ✅ Самопроверка

1. Чем Transformer отличается от Estimator?
2. Зачем нужен Pipeline?
3. Какой API сейчас стандарт — `pyspark.mllib` или `pyspark.ml`?
4. Что делает `VectorAssembler`?
5. Когда MLlib — не лучший выбор?

---

## ▶️ Дальше

[Урок 6.2 — Feature engineering](./урок_2_feature_engineering.md)
