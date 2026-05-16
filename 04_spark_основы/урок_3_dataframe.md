# Урок 4.3 — DataFrame API: ядро Spark

> Это **главный** урок модуля. После него вы пишете 80% реальных задач.

📖 Параллельно держите открытым [pyspark.md cheat sheet](../ресурсы/шпаргалки/pyspark.md).

---

## Часть 1. Что такое DataFrame

Это **распределённая таблица** со схемой (имена + типы колонок). Концептуально близок к Pandas DataFrame, но:

| | Pandas | Spark DataFrame |
|---|---|---|
| Размещение | RAM одной машины | RAM + диск всех нод |
| Индекс | есть | нет (просто колонки) |
| Lazy | нет | да |
| Оптимизатор | нет | Catalyst |
| SQL | через query() | полноценный |

---

## Часть 2. Создание

```python
# 1. Из списка кортежей
data = [("Аня", 25), ("Боря", 31), ("Витя", 19)]
df = spark.createDataFrame(data, schema=["name", "age"])

# 2. Из Pandas
import pandas as pd
pdf = pd.DataFrame({"name": ["Аня"], "age": [25]})
df = spark.createDataFrame(pdf)

# 3. Из CSV
df = spark.read.csv("file.csv", header=True, inferSchema=True)

# 4. Из Parquet — стандарт для prod
df = spark.read.parquet("file.parquet")
df = spark.read.parquet("dir/*.parquet")     # папка

# 5. Из JSON
df = spark.read.json("file.json")

# 6. Из БД (через JDBC)
df = (spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://host/db")
      .option("dbtable", "my_table")
      .option("user", "u").option("password", "p")
      .load())
```

⚠️ `inferSchema=True` читает файл **дважды**. На больших файлах это дорого. Лучше задавать схему вручную.

---

## Часть 3. Схема (StructType)

```python
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, DoubleType, TimestampType, DateType,
)

schema = StructType([
    StructField("id",     IntegerType(),  False),
    StructField("name",   StringType(),   True),
    StructField("amount", DoubleType(),   True),
    StructField("ts",     TimestampType(), True),
])

df = spark.read.csv("file.csv", header=True, schema=schema)
df.printSchema()
```

Это **самый правильный** способ. Быстрее, чем `inferSchema`, и гарантирует типы.

---

## Часть 4. Просмотр

```python
df.show(10, truncate=False)
df.printSchema()
df.dtypes
df.columns
df.count()
df.describe().show()
df.summary("count", "min", "max", "mean", "stddev").show()
df.head(5)
```

⚠️ `show` — это action (сразу считает). `df` без вызова `show` — это план.

---

## Часть 5. Выборка колонок и фильтр

```python
import pyspark.sql.functions as F
from pyspark.sql.functions import col

# Колонки
df.select("name", "age").show()
df.select(col("name"), col("age") + 1).show()
df.select(F.col("amount"), F.col("amount") * 2).show()

# Фильтры (4 эквивалентных способа)
df.filter(df.age > 18)
df.filter(F.col("age") > 18)
df.filter("age > 18")              # SQL-выражение строкой
df.where(F.col("age") > 18)

# Сложные условия
df.filter((F.col("age") > 18) & (F.col("city") == "Москва"))
df.filter(F.col("category").isin("payment", "transfer"))
df.filter(F.col("name").like("Иван%"))
df.filter(F.col("email").rlike(r".+@yandex\.ru$"))
df.filter(F.col("amount").between(100, 10000))
df.filter(F.col("city").isNotNull())
```

---

## Часть 6. Создание / удаление / переименование колонок

```python
df = df.withColumn("amount_usd", F.col("amount") / 90)
df = df.withColumn("big", F.when(F.col("amount") > 1000, "yes").otherwise("no"))
df = df.withColumn("ts", F.to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss"))

df = df.withColumnRenamed("amount", "sum")

df = df.drop("tmp_col")

# Изменить тип
df = df.withColumn("age", F.col("age").cast("int"))
```

---

## Часть 7. Агрегации

```python
# Простая
df.groupBy("category").count().show()

# Несколько метрик
df.groupBy("category").agg(
    F.count("*").alias("n"),
    F.sum("amount").alias("total"),
    F.avg("amount").alias("avg"),
    F.min("amount").alias("min"),
    F.max("amount").alias("max"),
    F.expr("percentile_approx(amount, 0.5)").alias("median"),
)

# По нескольким ключам
df.groupBy("city", "category").sum("amount").show()
```

---

## Часть 8. Сортировка

```python
df.orderBy("amount").show()
df.orderBy(F.col("amount").desc()).show()
df.orderBy("city", F.col("amount").desc()).show()
```

⚠️ `orderBy` без `limit` запускает **глобальный shuffle** (тяжёлый!). Часто хочется не `orderBy`, а `sortWithinPartitions` (локальная сортировка) или `orderBy + limit`.

---

## Часть 9. Join

```python
a.join(b, "id")                          # inner по общей колонке
a.join(b, "id", "left")
a.join(b, ["id1", "id2"], "inner")        # несколько ключей
a.join(b, a.id == b.client_id)            # на разных именах
a.join(F.broadcast(b), "id")              # broadcast (b — маленький)
```

Виды join:
- `inner` (по умолчанию)
- `left`, `right`, `outer`
- `left_semi` (только строки A, у которых есть совпадение в B; B-колонки не нужны)
- `left_anti` (только строки A, у которых нет совпадения в B)
- `cross` (декартово)

⚠️ `left_anti` — суперполезно для «найти, чего нет».

---

## Часть 10. Пропуски и дубликаты

```python
df.dropna()                                 # любая NaN
df.dropna(subset=["email"])
df.dropna(thresh=3)                          # минимум 3 non-null

df.fillna(0)
df.fillna({"age": 0, "city": "unknown"})
df.fillna(0, subset=["amount"])

df.dropDuplicates()
df.dropDuplicates(["client_id", "ts"])
```

---

## Часть 11. Запись

```python
df.write.mode("overwrite").parquet("out/")
df.write.mode("append").parquet("out/")
df.write.partitionBy("year", "month").parquet("out/")
df.write.csv("out_csv/", header=True)
df.write.option("compression", "gzip").json("out_json/")
df.write.format("delta").save("out_delta/")   # если Delta Lake подключён
```

Режимы (`mode`):
- `overwrite` — заменить
- `append` — дописать
- `ignore` — если есть, пропустить
- `errorifexists` (default) — упасть, если есть

---

## Часть 12. Python UDF (когда без них никак)

UDF = User Defined Function. Своя функция применяется к каждой строке.

```python
@F.udf("string")
def upper_word(s):
    return s.upper() if s else None

df = df.withColumn("name_upper", upper_word(F.col("name")))
```

⚠️ Python UDF — **медленные** (туда-сюда между JVM и Python). По возможности используйте встроенные `F.*` функции — они в 5–10× быстрее.

### Pandas UDF — быстрее

```python
import pandas as pd

@F.pandas_udf("double")
def discount(amount: pd.Series) -> pd.Series:
    return amount * 0.9

df = df.withColumn("discounted", discount(F.col("amount")))
```

Pandas UDF получает **порциями** (Pandas Series), а не построчно — это в разы быстрее.

---

## Часть 13. Кэширование

```python
df = df.cache()       # MEMORY_AND_DISK
df.count()            # триггерит кэширование
# ... несколько action'ов используют df
df.unpersist()
```

**Когда:** если `df` используется > 1 раза. **Когда не:** если только один action.

---

## ✅ Самопроверка

1. Чем отличается `df.filter(df.x > 5)` от `df.filter("x > 5")`? Что лучше?
2. Что произойдёт при `df.show()` после `df = df.filter(...)`?
3. Почему `inferSchema=True` не всегда хорошая идея?
4. Что такое `broadcast` join и когда его применять?
5. Почему Python UDF медленнее `F.*` функций?
6. Когда стоит делать `df.cache()`?

---

## ▶️ Дальше

[Урок 4.4 — Spark SQL и Catalyst](./урок_4_spark_sql.md)
