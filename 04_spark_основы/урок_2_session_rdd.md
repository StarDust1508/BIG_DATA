# Урок 4.2 — SparkSession и RDD

> SparkSession — это «дверь в Spark». Через неё мы делаем всё. RDD — низкоуровневая структура, но иногда без неё не обойтись.

---

## Часть 1. SparkSession

С 2.0+ — единая точка входа. Раньше было два объекта (`SparkContext` + `SQLContext`), сейчас один:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MyApp")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)
```

`getOrCreate()` — если сессия уже есть, вернёт её. Это удобно в notebook'ах.

---

## Часть 2. Полезные настройки

```python
.config("spark.driver.memory", "4g")              # драйверу 4 ГБ
.config("spark.executor.memory", "2g")             # каждому executor'у 2 ГБ
.config("spark.sql.shuffle.partitions", "200")      # сколько партиций после shuffle
.config("spark.sql.autoBroadcastJoinThreshold", "10485760")   # 10 МБ для broadcast join
.config("spark.ui.showConsoleProgress", "false")   # убрать progressbar в логах
```

Логи можно убавить:
```python
spark.sparkContext.setLogLevel("ERROR")     # или "WARN"
```

---

## Часть 3. SparkContext

Под капотом — `spark.sparkContext` (sc). Это объект, через который работают **RDD**.

```python
sc = spark.sparkContext
print(sc.appName, sc.master, sc.defaultParallelism)
```

`defaultParallelism` обычно = число ядер кластера. Это часто становится дефолтным числом партиций.

---

## Часть 4. RDD — Resilient Distributed Dataset

«Распределённая отказоустойчивая коллекция». В отличие от DataFrame — без схемы.

### Создание

```python
# Из Python-объекта
rdd = sc.parallelize([1, 2, 3, 4, 5])
rdd = sc.parallelize([("a", 1), ("b", 2)])

# Из файла
rdd = sc.textFile("file.txt")           # строка → элемент

# Сколько партиций?
print(rdd.getNumPartitions())
rdd = sc.parallelize(range(100), 4)      # явно 4 партиции
```

### Базовые трансформации

```python
rdd = sc.parallelize([1, 2, 3, 4, 5])

rdd.map(lambda x: x * 2)                    # → [2,4,6,8,10]
rdd.filter(lambda x: x % 2 == 0)             # → [2,4]
rdd.flatMap(lambda x: [x, x*10])             # → [1,10,2,20,3,30,...]

# Pair RDD (для key-value операций)
pairs = sc.parallelize([("a",1), ("b",2), ("a",3)])
pairs.reduceByKey(lambda a, b: a + b)        # → [("a",4),("b",2)]
pairs.groupByKey().mapValues(list)            # → [("a",[1,3]),("b",[2])]
```

### Action'ы

```python
rdd.collect()        # → Python list — ОПАСНО на больших данных
rdd.take(5)           # → первые 5
rdd.count()
rdd.first()
rdd.reduce(lambda a, b: a + b)
rdd.foreach(print)    # для побочных эффектов
```

### Кэширование

```python
rdd.cache()           # = .persist(StorageLevel.MEMORY_ONLY)
rdd.unpersist()
```

---

## Часть 5. Когда брать RDD в 2026

Честно: **почти никогда**. В 95% случаев DataFrame:
- Имеет схему → Catalyst оптимизирует.
- Поддерживает SQL.
- Поддерживает Parquet/ORC напрямую.
- Совместим с MLlib.

**Когда без RDD не обойтись:**

- Обработка нестандартных файлов (binary, неструктурированный текст).
- Сложные UDF, которые проще выразить как функцию.
- Работа с graph-структурами через GraphX.
- Поддержка legacy-кода.

Если в туториале вы видите `rdd.map(...)` — спросите себя, нельзя ли это сделать через DataFrame. Обычно можно.

---

## Часть 6. Пример: WordCount на RDD

Каноничный пример (для сравнения с Pandas/SQL подходом):

```python
rdd = sc.textFile("/path/to/text.txt")
result = (
    rdd
    .flatMap(lambda line: line.lower().split())
    .filter(lambda w: w.isalpha())
    .map(lambda w: (w, 1))
    .reduceByKey(lambda a, b: a + b)
    .sortBy(lambda kv: -kv[1])
)
print(result.take(10))
```

Тот же подход через DataFrame:

```python
from pyspark.sql.functions import explode, split, lower, col

df = spark.read.text("/path/to/text.txt")
words = df.select(explode(split(lower(col("value")), r"\s+")).alias("word"))
words.groupBy("word").count().orderBy("count", ascending=False).show(10)
```

Поразмыслите: какой вариант **читаемее**? Какой **быстрее** (благодаря Catalyst)?

---

## Часть 7. Конвертация туда-обратно

```python
# DataFrame → RDD
rdd = df.rdd

# RDD → DataFrame
df = rdd.toDF(["col1", "col2"])
# или с типизацией
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
])
df = spark.createDataFrame(rdd, schema)
```

---

## Часть 8. SparkContext'у много чего ещё умеет

```python
# Broadcast: маленькая переменная, видимая всем executor'ам
big_dict = {"RUB": 1.0, "USD": 90.0, "EUR": 100.0}
br = sc.broadcast(big_dict)
# Теперь br.value доступен внутри UDF без копирования по сети

# Accumulator: counter, обновляемый из executor'ов
err_counter = sc.accumulator(0)
def parse(line):
    try:
        return int(line)
    except ValueError:
        err_counter.add(1)
        return None
```

Это пригодится в модуле 05 при тюнинге.

---

## ✅ Самопроверка

1. Что делает `getOrCreate()` в `SparkSession.builder`?
2. Что такое `spark.sql.shuffle.partitions` и зачем его настраивать?
3. Чем RDD отличается от DataFrame на уровне API и на уровне оптимизации?
4. Почему `collect()` опасен на больших данных?
5. Когда оправдано использовать RDD вместо DataFrame?
6. Зачем нужен `sc.broadcast`?

---

## ▶️ Дальше

[Урок 4.3 — DataFrame API: ядро Spark](./урок_3_dataframe.md)
