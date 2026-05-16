# Урок 6.6 — Текст: TF-IDF и классификация

> Тексты — отдельный мир в ML. В MLlib базовые инструменты есть, но для серьёзного NLP — pretrained-эмбеддинги через HuggingFace / spaCy. Здесь — основа.

---

## Часть 1. Канонический pipeline

```
Сырой текст
   ↓ Tokenizer (или RegexTokenizer)
[слово, слово, слово, ...]
   ↓ StopWordsRemover
[важные_слова]
   ↓ HashingTF или CountVectorizer
Вектор частот (sparse, large)
   ↓ IDF
TF-IDF вектор (нормированный)
   ↓ Estimator
Предсказание
```

---

## Часть 2. Tokenizer

```python
from pyspark.ml.feature import Tokenizer, RegexTokenizer

# Простой — по пробелам
tok = Tokenizer(inputCol="text", outputCol="words_raw")

# Регулярка — гибче (по умолчанию pattern=\W)
regex_tok = RegexTokenizer(
    inputCol="text",
    outputCol="words_raw",
    pattern=r"\W+",         # разделители
    toLowercase=True,
    minTokenLength=2,
)
```

`RegexTokenizer` обычно лучше — он чистит пунктуацию автоматически.

---

## Часть 3. StopWords

«Шумовые» слова: «и», «но», «что», «в». Их обычно убирают.

```python
from pyspark.ml.feature import StopWordsRemover

stop_ru = StopWordsRemover.loadDefaultStopWords("russian")
stop_en = StopWordsRemover.loadDefaultStopWords("english")

sw = StopWordsRemover(
    inputCol="words_raw",
    outputCol="words",
    stopWords=stop_ru,
)
```

Можете дополнить список своими: профессиональные термины, повторяющиеся артефакты.

---

## Часть 4. TF: частоты слов

### HashingTF
Быстрая, но коллизии возможны.
```python
from pyspark.ml.feature import HashingTF

htf = HashingTF(
    inputCol="words",
    outputCol="raw_tf",
    numFeatures=10_000,    # размер хеш-таблицы
)
```

### CountVectorizer
Точная, но требует прохода по корпусу для словаря.
```python
from pyspark.ml.feature import CountVectorizer

cv = CountVectorizer(
    inputCol="words",
    outputCol="raw_tf",
    vocabSize=10_000,
    minDF=5,           # слово должно встретиться хотя бы в 5 документах
)
```

⚠️ После `fit()` CountVectorizer хранит словарь. Это позволяет интерпретировать веса по индексам.

---

## Часть 5. IDF

Чем чаще слово встречается во всём корпусе — тем меньший вес.

```python
from pyspark.ml.feature import IDF

idf = IDF(inputCol="raw_tf", outputCol="features", minDocFreq=2)
```

После TF-IDF получаем sparse-вектор: размерность 10 000, но ненулевых элементов — десятки.

---

## Часть 6. Полный pipeline классификации

```python
from pyspark.ml import Pipeline
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, CountVectorizer, IDF
from pyspark.ml.classification import LogisticRegression

stages = [
    RegexTokenizer(inputCol="text", outputCol="words_raw",
                    pattern=r"\W+", toLowercase=True, minTokenLength=2),
    StopWordsRemover(inputCol="words_raw", outputCol="words",
                      stopWords=StopWordsRemover.loadDefaultStopWords("russian")),
    CountVectorizer(inputCol="words", outputCol="raw_tf",
                     vocabSize=10_000, minDF=2),
    IDF(inputCol="raw_tf", outputCol="features"),
    LogisticRegression(featuresCol="features", labelCol="label",
                         maxIter=100, regParam=0.01),
]

pipeline = Pipeline(stages=stages)
model = pipeline.fit(train)
```

---

## Часть 7. n-граммы

«Хочу не отдельные слова, а пары/тройки соседних».

```python
from pyspark.ml.feature import NGram

ng = NGram(n=2, inputCol="words", outputCol="bigrams")
# затем CountVectorizer(inputCol="bigrams", ...)
```

Часто bigrams сильно улучшают качество классификации (захватывают «не плохо», «очень дорого»).

Можно использовать **и** unigrams, и bigrams вместе через два параллельных Transformer'а и VectorAssembler.

---

## Часть 8. Word2Vec — эмбеддинги

MLlib имеет встроенную реализацию.

```python
from pyspark.ml.feature import Word2Vec

w2v = Word2Vec(
    inputCol="words",
    outputCol="features",
    vectorSize=100,
    minCount=5,
)
```

⚠️ Качество встроенной Word2Vec так себе. Для production — берите pretrained-модели через PyTorch / spaCy.

---

## Часть 9. LDA — тематическое моделирование

«Нашёл 10 тем — какие из них в каждом документе».

```python
from pyspark.ml.clustering import LDA

lda = LDA(k=10, maxIter=20)
model = pipeline_with_lda.fit(corpus)

# Топ-слова в каждой теме
topics = model.describeTopics(maxTermsPerTopic=10)
topics.show(truncate=False)
```

---

## Часть 10. Юридический кейс: классификация судебных решений

Сценарий: классифицировать решения по типам исхода (удовлетворено / отказано / частично).

```python
# Метка
df = df.withColumn("label",
    F.when(F.col("decision").contains("удовлетворено"), 1)
     .when(F.col("decision").contains("отказано"), 0)
     .otherwise(2))

# Pipeline текстовой классификации — как в части 6
```

Дополнительно: интерпретация через топ-слова с положительным коэффициентом LR — для объяснимости.

---

## ✅ Самопроверка

1. Зачем нужен TF-IDF, а не просто TF?
2. Чем HashingTF отличается от CountVectorizer?
3. Что такое n-граммы и зачем они?
4. Почему MLlib Word2Vec часто хуже pretrained-моделей?
5. Какие шаги в pipeline'е классификации текстов?

---

## ▶️ Дальше

[Урок 6.7 — Cross-validation и тюнинг](./урок_7_cv_tuning.md)
