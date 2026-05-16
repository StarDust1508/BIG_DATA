# Урок 6.2 — Feature engineering в MLlib

> Большинство «магии» хорошей модели — это правильные признаки. MLlib даёт богатый набор Transformer'ов.

---

## Часть 1. Категориальные признаки

### StringIndexer — строка → число

```python
from pyspark.ml.feature import StringIndexer

idx = StringIndexer(inputCol="city", outputCol="city_idx",
                     handleInvalid="keep")   # keep | skip | error
model = idx.fit(df)
df_idx = model.transform(df)
# "Москва" → 0.0, "СПб" → 1.0, ...
```

`handleInvalid="keep"` — присваивает -1 неизвестным значениям (тем, что появятся на test).

### OneHotEncoder — индексы → разреженный вектор

```python
from pyspark.ml.feature import OneHotEncoder

ohe = OneHotEncoder(inputCols=["city_idx"], outputCols=["city_ohe"],
                     dropLast=False)
ohe_model = ohe.fit(df_idx)
df_ohe = ohe_model.transform(df_idx)
```

`dropLast=False` — оставлять все уровни (часто полезно для деревьев; для регрессий ставят `True` чтобы избежать мультиколлинеарности).

⚠️ Несколько колонок сразу — массив `inputCols`/`outputCols`. Для каждой нужен свой `StringIndexer` (или один с массивом).

---

## Часть 2. Сбор признаков в вектор

`VectorAssembler` — превращает несколько колонок в один вектор `features`.

```python
from pyspark.ml.feature import VectorAssembler

asm = VectorAssembler(
    inputCols=["amount", "age", "city_ohe", "segment_ohe"],
    outputCol="features_raw",
    handleInvalid="keep",
)
df_vec = asm.transform(df_ohe)
df_vec.select("features_raw").show(3, truncate=False)
```

Любая модель MLlib хочет колонку `features` (вектор). VectorAssembler — обязательный этап.

---

## Часть 3. Масштабирование числовых

Многие модели (логрег, KMeans, kNN) чувствительны к масштабам. Деревья — нет.

```python
from pyspark.ml.feature import StandardScaler, MinMaxScaler, RobustScaler

# Z-score
scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                         withMean=True, withStd=True)

# В диапазон [0,1]
scaler = MinMaxScaler(inputCol="features_raw", outputCol="features")

# Робастный (по медиане и IQR — устойчив к выбросам)
scaler = RobustScaler(inputCol="features_raw", outputCol="features")
```

⚠️ `withMean=True` для StandardScaler **денсифицирует** разреженные вектора → много памяти. Для one-hot этого избегаем (`withMean=False`).

---

## Часть 4. Заполнение пропусков (Imputer)

```python
from pyspark.ml.feature import Imputer

imp = Imputer(inputCols=["amount", "age"],
               outputCols=["amount_imp", "age_imp"],
               strategy="median")   # median | mean | mode
```

⚠️ Имputer **обучается** на train → запоминает median/mean. На test уже использует те же значения.

---

## Часть 5. Биннинг и дискретизация

### Bucketizer — по заданным границам
```python
from pyspark.ml.feature import Bucketizer

bk = Bucketizer(
    splits=[-float("inf"), 0, 1000, 10000, float("inf")],
    inputCol="amount",
    outputCol="amount_bucket",
)
```

### QuantileDiscretizer — по перцентилям
```python
from pyspark.ml.feature import QuantileDiscretizer

qd = QuantileDiscretizer(numBuckets=10, inputCol="amount", outputCol="amount_q")
```

Полезно когда:
- Признак сильно скошен (доход, цена).
- Нужны категориальные представления для деревьев.
- Хотим интерпретируемые «уровни».

---

## Часть 6. Текст (минимум)

```python
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF

tok  = Tokenizer(inputCol="text", outputCol="tokens")
stop = StopWordsRemover(inputCol="tokens", outputCol="clean",
                         stopWords=StopWordsRemover.loadDefaultStopWords("russian"))
htf  = HashingTF(inputCol="clean", outputCol="raw_tf", numFeatures=10_000)
idf  = IDF(inputCol="raw_tf", outputCol="features")
```

Подробнее в уроке 6.6.

---

## Часть 7. Полиномиальные и взаимодействия

```python
from pyspark.ml.feature import PolynomialExpansion, Interaction

poly = PolynomialExpansion(degree=2,
                            inputCol="features",
                            outputCol="features_poly")

inter = Interaction(inputCols=["features", "is_premium"],
                     outputCol="features_x_premium")
```

Используются в линейных моделях для нелинейности.

---

## Часть 8. VectorIndexer — авто-определение категорий

```python
from pyspark.ml.feature import VectorIndexer

vi = VectorIndexer(inputCol="features_raw",
                    outputCol="features",
                    maxCategories=20)
```

Колонки с числом уникальных значений ≤ 20 трактуются как категориальные, остальные как непрерывные. Удобно для случайного леса.

---

## Часть 9. Selector'ы — выбор фич

```python
from pyspark.ml.feature import ChiSqSelector

sel = ChiSqSelector(numTopFeatures=20,
                     featuresCol="features",
                     outputCol="features_top",
                     labelCol="label")
```

Используется на больших фичевых пространствах.

---

## Часть 10. Полный пример pipeline'а с категориями

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import (
    StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler,
)
from pyspark.ml.classification import LogisticRegression

cat_cols = ["city", "segment", "currency"]
num_cols = ["age", "amount", "monthly_income"]

# Для каждой кат-колонки — индексация
indexers = [
    StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
    for c in cat_cols
]

# Все индексы → one-hot вектор
ohe = OneHotEncoder(
    inputCols=[f"{c}_idx" for c in cat_cols],
    outputCols=[f"{c}_ohe" for c in cat_cols],
)

# Сборка всех признаков
asm = VectorAssembler(
    inputCols=num_cols + [f"{c}_ohe" for c in cat_cols],
    outputCol="features_raw",
)

# Масштаб
scaler = StandardScaler(inputCol="features_raw", outputCol="features",
                         withMean=False)   # из-за one-hot не делаем withMean

lr = LogisticRegression(featuresCol="features", labelCol="label",
                          maxIter=100, regParam=0.01)

pipeline = Pipeline(stages=[*indexers, ohe, asm, scaler, lr])
```

Это **архетипный** ML-pipeline. Запомните структуру: индексаторы → OHE → assembler → scaler → модель.

---

## ✅ Самопроверка

1. Зачем нужен `VectorAssembler`?
2. Что делает `StringIndexer` и зачем `handleInvalid="keep"`?
3. Когда `withMean=False` обязателен для StandardScaler?
4. В чём разница между `Bucketizer` и `QuantileDiscretizer`?
5. Какой Transformer обучается (имеет `fit`), а какой — нет?
   - `OneHotEncoder` — ?
   - `VectorAssembler` — ?
   - `StandardScaler` — ?
   - `Imputer` — ?

---

## ▶️ Дальше

[Урок 6.3 — Классификация: LR, RF, GBT](./урок_3_классификация.md)
